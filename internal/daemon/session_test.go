//go:build !windows

package daemon

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/naseer-hussain/tether/internal/proto"
	"github.com/naseer-hussain/tether/internal/version"
)

// helperEnv marks a re-exec of the test binary as a real session supervisor.
// SpawnDetached starts os.Executable() with "serve ...", which under `go test`
// is the test binary, so TestMain turns that invocation into a daemon.
const helperEnv = "TETHERD_TEST_HELPER"

func TestMain(m *testing.M) {
	if os.Getenv(helperEnv) == "1" {
		os.Exit(runServeHelper(os.Args[1:]))
	}
	os.Exit(m.Run())
}

// runServeHelper implements the `tetherd serve` command line for a spawned test
// binary, so EnsureSession/SpawnDetached can be exercised end to end.
func runServeHelper(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	home := fs.String("home", "", "")
	session := fs.String("session", "", "")
	socket := fs.String("socket", "", "")
	shell := fs.String("shell", "", "")
	bufferSize := fs.Int("buffer-size", 0, "")
	rows := fs.Int("rows", 0, "")
	cols := fs.Int("cols", 0, "")
	logPath := fs.String("log", "", "")
	if len(args) > 0 && args[0] == "serve" {
		args = args[1:]
	}
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "helper: %v\n", err)
		return 2
	}

	cfg := Config{
		SessionID:  *session,
		Home:       *home,
		SocketPath: *socket,
		Shell:      *shell,
		BufferSize: *bufferSize,
		Rows:       uint16(*rows),
		Cols:       uint16(*cols),
		LogPath:    *logPath,
	}
	s, err := New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: %v\n", err)
		return 1
	}

	ctx, cancel := context.WithCancel(context.Background())
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		<-sigc
		cancel()
	}()

	_ = s.Serve(ctx)
	return 0
}

// shortHome returns a short, private tether home. Unix socket paths are limited
// to ~108 bytes, which t.TempDir() can approach with nested test names.
func shortHome(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "tether-test-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir + "/home"
}

// startTestSession creates a session whose shell is `sh -c <args>` and runs
// Serve in the background. Everything is torn down on test end.
func startTestSession(t *testing.T, shellArgs []string, bufferSize int) *Session {
	t.Helper()
	cfg := Config{
		SessionID:  "test",
		Home:       shortHome(t),
		Shell:      "/bin/sh",
		ShellArgs:  shellArgs,
		BufferSize: bufferSize,
		Rows:       24,
		Cols:       80,
	}
	s, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- s.Serve(ctx) }()

	t.Cleanup(func() {
		cancel()
		_ = s.Close()
		select {
		case <-served:
		case <-time.After(10 * time.Second):
			t.Error("Serve did not return")
		}
	})
	return s
}

func dialSession(t *testing.T, socketPath string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("unix", socketPath, 3*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", socketPath, err)
	}
	return conn
}

func writeHello(t *testing.T, conn net.Conn, lastAck uint64) {
	t.Helper()
	payload, err := proto.Hello{
		Version:    version.ProtocolVersion,
		LastAckSeq: lastAck,
		Rows:       24,
		Cols:       80,
	}.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal hello: %v", err)
	}
	if err := proto.WriteMessage(conn, proto.Message{Type: proto.TypeHello, Payload: payload}); err != nil {
		t.Fatalf("write hello: %v", err)
	}
}

// readDataUntil reads frames until marker shows up in the accumulated Data
// payloads, asserting that Data sequence numbers are contiguous starting at
// expect. It returns the highest sequence number seen and all data it read.
func readDataUntil(t *testing.T, conn net.Conn, marker string, expect uint64, timeout time.Duration) (uint64, []byte) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	if err := conn.SetReadDeadline(deadline); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}

	var acc []byte
	for {
		m, err := proto.ReadMessage(conn)
		if err != nil {
			t.Fatalf("waiting for %q: %v (read %q)", marker, err, acc)
		}
		switch m.Type {
		case proto.TypeData:
			seq, p, err := proto.DataSeq(m)
			if err != nil {
				t.Fatalf("bad data frame: %v", err)
			}
			if len(p) == 0 {
				continue
			}
			if seq != expect {
				t.Fatalf("non-contiguous data: firstSeq=%d want %d", seq, expect)
			}
			expect = seq + uint64(len(p))
			acc = append(acc, p...)
			if bytes.Contains(acc, []byte(marker)) {
				return expect - 1, acc
			}
		case proto.TypeOverrun:
			from, to, err := proto.OverrunRange(m)
			if err != nil {
				t.Fatalf("bad overrun frame: %v", err)
			}
			t.Fatalf("unexpected overrun [%d,%d) while waiting for %q", from, to, marker)
		case proto.TypeExit:
			code, _ := proto.ExitCode(m)
			t.Fatalf("session exited with %d while waiting for %q", code, marker)
		case proto.TypeError:
			t.Fatalf("daemon error: %s", m.Payload)
		}
	}
}

// TestSessionReplayAndResume covers the three attach shapes the client relies
// on: fresh attach, replay from zero, and resume from an acknowledged cursor.
func TestSessionReplayAndResume(t *testing.T) {
	s := startTestSession(t, []string{"-c", "cat"}, 0)

	marker := fmt.Sprintf("tether-marker-%d", time.Now().UnixNano())

	// 1. Fresh attach: input echoes back with contiguous sequence numbers.
	conn := dialSession(t, s.SocketPath())
	writeHello(t, conn, 0)
	if err := proto.WriteData(conn, 0, []byte(marker+"\n")); err != nil {
		t.Fatalf("write data: %v", err)
	}
	lastAck, seen := readDataUntil(t, conn, marker, 1, 5*time.Second)
	if !bytes.Contains(seen, []byte(marker)) {
		t.Fatalf("marker missing from output %q", seen)
	}
	conn.Close()

	// 2. Replay: a new attach with LastAckSeq 0 must see the marker again; this
	// is what makes a dropped connection lossless.
	replay := dialSession(t, s.SocketPath())
	writeHello(t, replay, 0)
	if _, replayed := readDataUntil(t, replay, marker, 1, 5*time.Second); !bytes.Contains(replayed, []byte(marker)) {
		t.Fatalf("marker not replayed: %q", replayed)
	}
	replay.Close()

	// 3. Resume: an attach at the acked cursor must not replay the marker, and
	// new output must continue exactly at lastAck+1.
	resume := dialSession(t, s.SocketPath())
	defer resume.Close()
	writeHello(t, resume, lastAck)

	marker2 := fmt.Sprintf("tether-resume-%d", time.Now().UnixNano())
	if err := proto.WriteData(resume, 0, []byte(marker2+"\n")); err != nil {
		t.Fatalf("write data: %v", err)
	}
	lastAck2, seen2 := readDataUntil(t, resume, marker2, lastAck+1, 5*time.Second)
	if bytes.Contains(seen2, []byte(marker)) {
		t.Fatalf("resume replayed already-acknowledged bytes: %q", seen2)
	}
	if lastAck2 <= lastAck {
		t.Fatalf("ack did not advance: %d -> %d", lastAck, lastAck2)
	}
}

// TestSessionOverrun attaches with a cursor older than the retention window and
// expects a TypeOverrun frame describing the lost range.
func TestSessionOverrun(t *testing.T) {
	s := startTestSession(t, []string{"-c", "yes tether-overrun"}, 1024)

	deadline := time.Now().Add(5 * time.Second)
	for s.Head() < 4096 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if s.Head() < 4096 {
		t.Fatalf("shell produced only %d bytes", s.Head())
	}

	conn := dialSession(t, s.SocketPath())
	defer conn.Close()
	writeHello(t, conn, 1)
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}

	for {
		m, err := proto.ReadMessage(conn)
		if err != nil {
			t.Fatalf("no overrun frame: %v", err)
		}
		if m.Type != proto.TypeOverrun {
			continue
		}
		from, to, err := proto.OverrunRange(m)
		if err != nil {
			t.Fatalf("bad overrun frame: %v", err)
		}
		if to <= from {
			t.Fatalf("empty overrun range [%d,%d)", from, to)
		}
		if from != 2 {
			t.Fatalf("overrun should start at lastAck+1 = 2, got %d", from)
		}
		return
	}
}

// TestSessionExitFrame checks that a short-lived shell still delivers Exit.
func TestSessionExitFrame(t *testing.T) {
	s := startTestSession(t, []string{"-c", "sleep 1; exit 7"}, 0)
	conn := dialSession(t, s.SocketPath())
	defer conn.Close()
	writeHello(t, conn, 0)

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	for {
		m, err := proto.ReadMessage(conn)
		if err != nil {
			t.Fatalf("waiting for exit frame: %v", err)
		}
		if m.Type != proto.TypeExit {
			continue
		}
		code, err := proto.ExitCode(m)
		if err != nil {
			t.Fatalf("bad exit frame: %v", err)
		}
		if code != 7 {
			t.Fatalf("exit code = %d, want 7", code)
		}
		return
	}
}
