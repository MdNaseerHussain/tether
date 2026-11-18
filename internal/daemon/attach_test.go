//go:build !windows

package daemon

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/MdNaseerHussain/tether/internal/proto"
	"github.com/MdNaseerHussain/tether/internal/version"
)

type frameOrErr struct {
	msg proto.Message
	err error
}

// pumpFrames decodes frames from r in the background so the test can select on
// them with a timeout instead of blocking in ReadMessage.
func pumpFrames(r io.Reader) <-chan frameOrErr {
	ch := make(chan frameOrErr, 16)
	go func() {
		defer close(ch)
		for {
			m, err := proto.ReadMessage(r)
			select {
			case ch <- frameOrErr{msg: m, err: err}:
			case <-time.After(5 * time.Second):
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return ch
}

// readFramesUntilData waits for marker in the Data stream, asserting the
// sequence numbers are contiguous starting at expect.
func readFramesUntilData(t *testing.T, frames <-chan frameOrErr, marker string, expect uint64, timeout time.Duration) (uint64, []byte) {
	t.Helper()
	deadline := time.After(timeout)
	var acc []byte
	for {
		select {
		case f, ok := <-frames:
			if !ok {
				t.Fatalf("stream closed before %q (read %q)", marker, acc)
			}
			if f.err != nil {
				t.Fatalf("read frame while waiting for %q: %v (read %q)", marker, f.err, acc)
			}
			if f.msg.Type != proto.TypeData {
				continue
			}
			seq, p, err := proto.DataSeq(f.msg)
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
		case <-deadline:
			t.Fatalf("timed out waiting for %q, read %q", marker, acc)
		}
	}
}

// writeWithTimeout writes p, failing the test instead of hanging if the bridge
// stops reading.
func writeWithTimeout(t *testing.T, w io.Writer, p []byte, timeout time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := w.Write(p)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write: %v", err)
		}
	case <-time.After(timeout):
		t.Fatalf("write timed out after %s", timeout)
	}
}

func encodeFrame(t *testing.T, m proto.Message) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := proto.WriteMessage(&buf, m); err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	return buf.Bytes()
}

func helloFrame(t *testing.T, lastAck uint64) []byte {
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
	return encodeFrame(t, proto.Message{Type: proto.TypeHello, Payload: payload})
}

// TestBridge drives a session through Bridge exactly as the client would: raw
// framed bytes in, raw framed bytes out. Bridge must not reframe anything.
func TestBridge(t *testing.T) {
	s := startTestSession(t, []string{"-c", "cat"}, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	bridged := make(chan error, 1)
	go func() { bridged <- Bridge(ctx, s.SocketPath(), stdinR, stdoutW) }()

	frames := pumpFrames(stdoutR)
	writeWithTimeout(t, stdinW, helloFrame(t, 0), 5*time.Second)

	marker := fmt.Sprintf("tether-bridge-%d", time.Now().UnixNano())
	writeWithTimeout(t, stdinW, encodeFrame(t, proto.Message{
		Type:    proto.TypeData,
		Payload: append([]byte{0, 0, 0, 0, 0, 0, 0, 0}, []byte(marker+"\n")...),
	}), 5*time.Second)

	lastAck, seen := readFramesUntilData(t, frames, marker, 1, 5*time.Second)
	if !bytes.Contains(seen, []byte(marker)) {
		t.Fatalf("marker missing from bridged output %q", seen)
	}
	if lastAck == 0 {
		t.Fatal("no data acknowledged through the bridge")
	}

	writeWithTimeout(t, stdinW, encodeFrame(t, proto.Message{Type: proto.TypeDetach}), 5*time.Second)
	select {
	case err := <-bridged:
		if err != nil {
			t.Fatalf("Bridge: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Bridge did not return after detach")
	}

	_ = stdinW.Close()
	_ = stdoutW.Close()
}

// TestEnsureSessionSpawns checks that EnsureSession starts a detached daemon
// once, that a second call reuses it, and that Kill removes it.
func TestEnsureSessionSpawns(t *testing.T) {
	home := shortHome(t)
	cfg := Config{
		SessionID:  "spawned",
		Home:       home,
		Shell:      "/bin/cat",
		BufferSize: 64 << 10,
		Rows:       24,
		Cols:       80,
		// The spawned supervisor is this test binary; the marker turns it into
		// a daemon instead of a test run.
		Env: append(os.Environ(), helperEnv+"=1"),
	}
	if err := cfg.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	t.Cleanup(func() { _ = Kill(home, cfg.SessionID) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	start := time.Now()
	if err := EnsureSession(ctx, cfg, 5*time.Second); err != nil {
		t.Fatalf("EnsureSession: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("EnsureSession took %s", elapsed)
	}

	conn, err := net.DialTimeout("unix", cfg.SocketPath, time.Second)
	if err != nil {
		t.Fatalf("spawned session not reachable: %v", err)
	}
	_ = conn.Close()

	pid := readPIDFile(filepath.Join(cfg.Home, "sessions", cfg.SessionID+".pid"))
	if pid <= 0 || !pidAlive(pid) {
		t.Fatalf("spawned supervisor pid %d is not alive", pid)
	}

	start = time.Now()
	if err := EnsureSession(ctx, cfg, 5*time.Second); err != nil {
		t.Fatalf("second EnsureSession: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("second EnsureSession took %s, it should reuse the live session", elapsed)
	}
	if again := readPIDFile(filepath.Join(cfg.Home, "sessions", cfg.SessionID+".pid")); again != pid {
		t.Fatalf("second EnsureSession spawned a second supervisor: %d -> %d", pid, again)
	}

	if err := Kill(home, cfg.SessionID); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	waitGone(t, cfg.SocketPath, 10*time.Second)
	if pidAlive(pid) {
		t.Fatalf("supervisor %d still running after Kill", pid)
	}
}

// waitGone waits until path disappears.
func waitGone(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s still exists after %s", path, timeout)
}
