//go:build !windows

// Package e2e drives the real tetherd binaries over real processes, real Unix
// sockets and the real framed protocol. It is the only test that proves the
// supervisor is decoupled from the bridge: the bridge can be SIGKILLed and the
// next attach still replays the buffered output.
package e2e

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/MdNaseerHussain/tether/internal/proto"
	"github.com/MdNaseerHussain/tether/internal/version"
)

const (
	sessionID = "e2e"
	// stepTimeout bounds every protocol step so a wedged daemon fails the test
	// instead of hanging it.
	stepTimeout = 10 * time.Second
)

// moduleRoot returns the repository root that contains go.mod.
func moduleRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root := filepath.Clean(filepath.Join(wd, "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Skipf("not running inside the module checkout: %v", err)
	}
	return root
}

// buildTetherd compiles cmd/tetherd into a directory that is removed with the
// test, so the test always exercises the current sources rather than a stale
// bin/ artefact.
func buildTetherd(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Unix domain sockets and process groups are required")
	}
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("go tool not found: %v", err)
	}
	if _, err := os.Stat("/bin/cat"); err != nil {
		t.Skipf("/bin/cat is required as the echo shell: %v", err)
	}
	// Probe Unix socket support once; t.TempDir paths are short enough here
	// because the socket lives under our own home, not the build dir.
	probe := filepath.Join(t.TempDir(), "probe.sock")
	ln, err := net.Listen("unix", probe)
	if err != nil {
		t.Skipf("unix sockets unavailable: %v", err)
	}
	_ = ln.Close()

	bin := filepath.Join(t.TempDir(), "tetherd")
	cmd := exec.Command(goTool, "build", "-o", bin, "./cmd/tetherd")
	cmd.Dir = moduleRoot(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build ./cmd/tetherd: %v\n%s", err, out)
	}
	return bin
}

// shortHome returns a private tether home with a short path: sun_path is
// limited to ~108 bytes and t.TempDir() can get close with nested test names.
func shortHome(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "tether-e2e-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "home")
}

// child is a started subprocess whose exit is reaped exactly once.
type child struct {
	t    *testing.T
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

func startChild(t *testing.T, cmd *exec.Cmd) *child {
	t.Helper()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", strings.Join(cmd.Args, " "), err)
	}
	c := &child{t: t, cmd: cmd, done: make(chan struct{})}
	go func() {
		c.err = cmd.Wait()
		close(c.done)
	}()
	return c
}

// exited reports whether the process has been reaped.
func (c *child) exited() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// kill SIGKILLs the process, which is how the test simulates an SSH channel
// dying without the bridge getting a chance to detach cleanly.
func (c *child) kill() {
	c.t.Helper()
	_ = c.cmd.Process.Kill()
	select {
	case <-c.done:
	case <-time.After(stepTimeout):
		c.t.Fatalf("%s did not exit after SIGKILL", strings.Join(c.cmd.Args, " "))
	}
}

// stop asks for a clean shutdown and escalates to SIGKILL.
func (c *child) stop() {
	c.t.Helper()
	if c.exited() {
		return
	}
	_ = c.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-c.done:
	case <-time.After(stepTimeout):
		_ = c.cmd.Process.Kill()
		select {
		case <-c.done:
		case <-time.After(stepTimeout):
			c.t.Errorf("%s did not exit at all", strings.Join(c.cmd.Args, " "))
		}
	}
}

// serveProc is a running `tetherd serve`.
type serveProc struct {
	*child
	logPath string
	sock    string
}

func startServe(t *testing.T, bin, home string) *serveProc {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "serve.out")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create serve log: %v", err)
	}
	t.Cleanup(func() { _ = logFile.Close() })

	cmd := exec.Command(bin, "serve",
		"--home", home,
		"--session", sessionID,
		"--shell", "/bin/cat",
		"--buffer-size", "65536",
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	s := &serveProc{
		child:   startChild(t, cmd),
		logPath: logPath,
		sock:    filepath.Join(home, "sessions", sessionID+".sock"),
	}
	t.Cleanup(s.stop)

	deadline := time.Now().Add(stepTimeout)
	for {
		conn, err := net.DialTimeout("unix", s.sock, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return s
		}
		if s.exited() {
			body, _ := os.ReadFile(logPath)
			t.Fatalf("tetherd serve exited before listening: %v\n%s", s.err, body)
		}
		if time.Now().After(deadline) {
			body, _ := os.ReadFile(logPath)
			t.Fatalf("session socket %s never appeared: %v\n%s", s.sock, err, body)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type frameResult struct {
	msg proto.Message
	err error
}

// attachProc is a running `tetherd attach` speaking the framed protocol on its
// stdin/stdout.
type attachProc struct {
	*child
	stdin  io.WriteCloser
	frames <-chan frameResult
}

func startAttach(t *testing.T, cmd *exec.Cmd) *attachProc {
	t.Helper()
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("attach stdin pipe: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("attach stdout pipe: %v", err)
	}
	stderr, err := os.Create(filepath.Join(t.TempDir(), "attach.err"))
	if err != nil {
		t.Fatalf("attach stderr file: %v", err)
	}
	cmd.Stderr = stderr
	t.Cleanup(func() { _ = stderr.Close() })

	a := &attachProc{child: startChild(t, cmd), stdin: stdin}
	frames := make(chan frameResult, 64)
	a.frames = frames
	t.Cleanup(a.kill)

	go func() {
		defer close(frames)
		for {
			m, err := proto.ReadMessage(stdout)
			select {
			case frames <- frameResult{msg: m, err: err}:
			case <-time.After(stepTimeout):
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return a
}

// startAttachDirect starts `tetherd attach --home H --session ID`.
func startAttachDirect(t *testing.T, bin, home string) *attachProc {
	t.Helper()
	return startAttach(t, exec.Command(bin, "attach", "--home", home, "--session", sessionID))
}

// startAttachViaClientCommand starts the bridge exactly the way the local
// client does: `tetherd findable on PATH, attach --session ID --spawn`, with
// the home directory supplied out-of-band through TETHER_HOME. This is a
// runtime check that the client's remote command line is accepted.
func startAttachViaClientCommand(t *testing.T, bin, home string) *attachProc {
	t.Helper()
	binDir := t.TempDir()
	if err := os.Symlink(bin, filepath.Join(binDir, "tetherd")); err != nil {
		t.Fatalf("symlink tetherd: %v", err)
	}
	cmd := exec.Command("/bin/sh", "-c", "tetherd attach --session "+sessionID+" --spawn --buffer-size 65536")
	cmd.Env = append(os.Environ(),
		"TETHER_HOME="+home,
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
	)
	return startAttach(t, cmd)
}

func (a *attachProc) sendHello(lastAck uint64) error {
	payload, err := proto.Hello{
		Version:    version.ProtocolVersion,
		LastAckSeq: lastAck,
		Rows:       24,
		Cols:       80,
	}.MarshalBinary()
	if err != nil {
		return err
	}
	return proto.WriteMessage(a.stdin, proto.Message{Type: proto.TypeHello, Payload: payload})
}

// sendData writes input with firstSeq 0, as the client does: input bytes have
// no sequence space and the daemon must ignore the field.
func (a *attachProc) sendData(p []byte) error {
	return proto.WriteData(a.stdin, 0, p)
}

// waitForData reads frames until marker appears in the accumulated Data
// payloads. It enforces the sequencing contract: the first Data frame must
// start at wantFirstSeq and every following frame must continue exactly where
// the previous one ended, which is what makes replay lossless.
func (a *attachProc) waitForData(marker string, wantFirstSeq uint64) (lastSeq uint64, seen []byte) {
	a.t.Helper()

	expect := wantFirstSeq
	deadline := time.After(stepTimeout)
	var acc []byte
	for {
		select {
		case f, ok := <-a.frames:
			if !ok {
				a.t.Fatalf("attach stream closed before %q (read %q)", marker, acc)
			}
			if f.err != nil {
				a.t.Fatalf("read frame while waiting for %q: %v (read %q)", marker, f.err, acc)
			}
			switch f.msg.Type {
			case proto.TypeData:
				seq, p, err := proto.DataSeq(f.msg)
				if err != nil {
					a.t.Fatalf("bad data frame: %v", err)
				}
				if len(p) == 0 {
					continue
				}
				if seq != expect {
					a.t.Fatalf("non-contiguous data: firstSeq=%d want %d", seq, expect)
				}
				expect = seq + uint64(len(p))
				acc = append(acc, p...)
				if bytes.Contains(acc, []byte(marker)) {
					return expect - 1, acc
				}
			case proto.TypeOverrun:
				from, to, _ := proto.OverrunRange(f.msg)
				a.t.Fatalf("unexpected overrun [%d,%d) while waiting for %q", from, to, marker)
			case proto.TypeError:
				a.t.Fatalf("daemon rejected the attach: %q", f.msg.Payload)
			case proto.TypeExit:
				code, _ := proto.ExitCode(f.msg)
				a.t.Fatalf("session exited with %d while waiting for %q (read %q)", code, marker, acc)
			}
		case <-deadline:
			a.t.Fatalf("timed out waiting for %q, read %q", marker, acc)
		}
	}
}

// waitForExit reads frames until the daemon sends Exit, and then requires the
// channel to be closed by the daemon, as docs/PROTOCOL.md §3.5 specifies.
func (a *attachProc) waitForExit() int32 {
	a.t.Helper()
	deadline := time.After(stepTimeout)
	for {
		select {
		case f, ok := <-a.frames:
			if !ok {
				a.t.Fatalf("attach stream closed before an Exit frame")
			}
			if f.err != nil {
				a.t.Fatalf("read frame while waiting for Exit: %v", f.err)
			}
			if f.msg.Type != proto.TypeExit {
				continue
			}
			code, err := proto.ExitCode(f.msg)
			if err != nil {
				a.t.Fatalf("bad exit frame: %v", err)
			}
			// Exit must be the last frame before the daemon closes.
			select {
			case next, ok := <-a.frames:
				if ok && next.err == nil && next.msg.Type != proto.TypeExit {
					a.t.Fatalf("daemon sent %s after Exit over the same channel", next.msg.Type)
				}
			case <-time.After(stepTimeout):
				a.t.Fatalf("daemon did not close the channel after Exit")
			}
			return code
		case <-deadline:
			a.t.Fatalf("timed out waiting for the Exit frame")
		}
	}
}

// pidFromFile reads <home>/sessions/<id>.pid.
func pidFromFile(t *testing.T, home string) int {
	t.Helper()
	buf, err := os.ReadFile(filepath.Join(home, "sessions", sessionID+".pid"))
	if err != nil {
		t.Fatalf("read pid file: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(buf)))
	if err != nil || pid <= 0 {
		t.Fatalf("pid file %q is not a pid", buf)
	}
	return pid
}

func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func modeOf(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Mode().Perm()
}

type listEntry struct {
	ID         string `json:"id"`
	SocketPath string `json:"socket"`
	PID        int    `json:"pid"`
	Alive      bool   `json:"alive"`
	BufferSize int    `json:"buffer_size"`
}

func runJSON(t *testing.T, bin string, args ...string) []listEntry {
	t.Helper()
	out, err := exec.Command(bin, args...).Output()
	if err != nil {
		t.Fatalf("%s %s: %v", bin, strings.Join(args, " "), err)
	}
	var entries []listEntry
	if err := json.Unmarshal(out, &entries); err != nil {
		t.Fatalf("decode list json %q: %v", out, err)
	}
	return entries
}

func TestDaemonSurvivesBridgeKillAndReplays(t *testing.T) {
	bin := buildTetherd(t)
	home := shortHome(t)
	serve := startServe(t, bin, home)

	// Fresh sessions are created with a private directory, a private socket and
	// a private log.
	if got := modeOf(t, filepath.Join(home, "sessions")); got != 0o700 {
		t.Errorf("sessions dir mode = %o, want 700", got)
	}
	if got := modeOf(t, serve.sock); got != 0o600 {
		t.Errorf("socket mode = %o, want 600", got)
	}
	if got := modeOf(t, filepath.Join(home, "sessions", sessionID+".log")); got != 0o600 {
		t.Errorf("log mode = %o, want 600", got)
	}

	shellPID := pidFromFile(t, home)
	if !processAlive(shellPID) {
		t.Fatalf("shell pid %d is not running", shellPID)
	}

	marker := fmt.Sprintf("tether-e2e-%d-%d", os.Getpid(), time.Now().UnixNano())

	// 1. First bridge, direct flags: the marker echoes back from the PTY
	// starting at sequence 1.
	b1 := startAttachDirect(t, bin, home)
	if err := b1.sendHello(0); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	if err := b1.sendData([]byte(marker + "\n")); err != nil {
		t.Fatalf("send data: %v", err)
	}
	if _, seen := b1.waitForData(marker, 1); !bytes.Contains(seen, []byte(marker)) {
		t.Fatalf("marker %q missing from first attach output %q", marker, seen)
	}

	// Simulate the SSH channel dying: no Detach, no clean close.
	b1.kill()

	if !processAlive(shellPID) {
		t.Fatalf("shell pid %d died with the bridge; the daemon is not decoupled", shellPID)
	}

	// 2. Re-attach through the exact command line the local client builds, from
	// an empty cursor: the daemon must replay everything it retained.
	b2 := startAttachViaClientCommand(t, bin, home)
	if err := b2.sendHello(0); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	if _, replay := b2.waitForData(marker, 1); !bytes.Contains(replay, []byte(marker)) {
		t.Fatalf("marker %q was not replayed from the surviving daemon: %q", marker, replay)
	}

	// 3. The session is still advertised as alive.
	entries := runJSON(t, bin, "list", "--home", home, "--json")
	if len(entries) != 1 {
		t.Fatalf("list returned %d sessions, want 1: %+v", len(entries), entries)
	}
	if entries[0].ID != sessionID || !entries[0].Alive {
		t.Fatalf("session not alive in list: %+v", entries[0])
	}

	// 4. Kill the second bridge too, then observe the marker once more. This is
	// the durability claim: the supervisor, not the bridge, owns the buffer.
	b2.kill()
	marker2 := marker + "-2"
	b3 := startAttachDirect(t, bin, home)
	if err := b3.sendHello(0); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	lastReplayed, replay := b3.waitForData(marker, 1)
	if !bytes.Contains(replay, []byte(marker)) {
		t.Fatalf("marker %q missing after the second bridge kill: %q", marker, replay)
	}
	if err := b3.sendData([]byte(marker2 + "\n")); err != nil {
		t.Fatalf("send data: %v", err)
	}
	// Sequence numbers are session-wide, so the new output continues after the
	// replayed bytes rather than restarting.
	if _, seen := b3.waitForData(marker2, lastReplayed+1); !bytes.Contains(seen, []byte(marker2)) {
		t.Fatalf("new marker %q missing from attach output %q", marker2, seen)
	}

	// 5. Kill tears the session down: the attached bridge gets Exit, the
	// supervisor removes its socket, and the shell goes away.
	out, err := exec.Command(bin, "kill", "--home", home, "--session", sessionID).CombinedOutput()
	if err != nil {
		t.Fatalf("tetherd kill: %v\n%s", err, out)
	}
	code := b3.waitForExit()
	t.Logf("killed shell reported exit code %d", code)

	if _, err := os.Stat(serve.sock); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket still exists after kill: %v", err)
	}
	if entries := runJSON(t, bin, "list", "--home", home, "--json"); len(entries) != 0 {
		t.Fatalf("list still reports sessions after kill: %+v", entries)
	}
	if processAlive(shellPID) {
		t.Fatalf("shell pid %d survived tetherd kill", shellPID)
	}

	// The supervisor exits on its own once the shell is gone; it must not be
	// left behind as an orphan.
	select {
	case <-serve.done:
	case <-time.After(stepTimeout):
		t.Fatalf("tetherd serve kept running after the session was killed")
	}
}

func TestListEmptyHome(t *testing.T) {
	bin := buildTetherd(t)
	home := shortHome(t)

	entries := runJSON(t, bin, "list", "--home", home, "--json")
	if len(entries) != 0 {
		t.Fatalf("list on a fresh home returned %+v", entries)
	}

	// JSON output must always be an array, even when empty, so scripts can
	// decode it unconditionally.
	out, err := exec.Command(bin, "list", "--home", home, "--json").Output()
	if err != nil {
		t.Fatalf("list --json: %v", err)
	}
	if strings.TrimSpace(string(out)) != "[]" {
		t.Fatalf("empty list --json = %q, want []", out)
	}
}

// TestHelloRequired checks that the intentional "Kill before Hello" shortcut did
// not weaken the handshake: any other first frame is rejected with TypeError.
func TestHelloRequired(t *testing.T) {
	bin := buildTetherd(t)
	home := shortHome(t)
	serve := startServe(t, bin, home)

	conn, err := net.DialTimeout("unix", serve.sock, 3*time.Second)
	if err != nil {
		t.Fatalf("dial session: %v", err)
	}
	defer conn.Close()
	if err := proto.WriteData(conn, 0, []byte("not a hello")); err != nil {
		t.Fatalf("write data before hello: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(stepTimeout)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	m, err := proto.ReadMessage(conn)
	if err != nil {
		t.Fatalf("read rejection: %v", err)
	}
	if m.Type != proto.TypeError {
		t.Fatalf("first non-hello frame was answered with %s, want error", m.Type)
	}
	if !strings.Contains(string(m.Payload), "hello") {
		t.Fatalf("rejection text %q does not explain the missing hello", m.Payload)
	}
	if _, err := proto.ReadMessage(conn); err == nil {
		t.Fatalf("daemon kept the rejected connection open")
	}
}

// TestSIGTERMCleanup checks that a supervisor shut down with SIGTERM removes its
// socket, pid file and metadata while keeping the log for post-mortem reading.
func TestSIGTERMCleanup(t *testing.T) {
	bin := buildTetherd(t)
	home := shortHome(t)
	serve := startServe(t, bin, home)

	pidFile := filepath.Join(home, "sessions", sessionID+".pid")
	metaFile := filepath.Join(home, "sessions", sessionID+".json")
	logFile := filepath.Join(home, "sessions", sessionID+".log")
	for _, path := range []string{serve.sock, pidFile, metaFile, logFile} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected %s before SIGTERM: %v", path, err)
		}
	}

	if err := serve.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal serve: %v", err)
	}
	select {
	case <-serve.done:
	case <-time.After(stepTimeout):
		t.Fatalf("serve did not exit after SIGTERM")
	}

	for _, path := range []string{serve.sock, pidFile, metaFile} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s was not removed on SIGTERM: %v", path, err)
		}
	}
	if _, err := os.Stat(logFile); err != nil {
		t.Errorf("log should survive for post-mortem use: %v", err)
	}
}

// TestShellExitCleanup checks the normal-exit path: the shell ends, the
// supervisor removes its files and exits 0.
func TestShellExitCleanup(t *testing.T) {
	bin := buildTetherd(t)
	home := shortHome(t)

	cmd := exec.Command(bin, "serve", "--home", home, "--session", "short", "--shell", "/bin/true")
	c := startChild(t, cmd)
	t.Cleanup(c.stop)
	select {
	case <-c.done:
	case <-time.After(stepTimeout):
		t.Fatalf("serve did not exit when its shell did")
	}
	if c.err != nil {
		t.Fatalf("serve exited with %v, want 0", c.err)
	}
	for _, name := range []string{"short.sock", "short.pid", "short.json"} {
		path := filepath.Join(home, "sessions", name)
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s was not removed on normal exit: %v", path, err)
		}
	}
}

func TestVersionOutput(t *testing.T) {
	bin := buildTetherd(t)
	out, err := exec.Command(bin, "version").Output()
	if err != nil {
		t.Fatalf("tetherd version: %v", err)
	}
	want := fmt.Sprintf("tetherd %s (protocol %d)", version.Version, version.ProtocolVersion)
	if !strings.Contains(string(out), want) {
		t.Fatalf("version output %q does not contain %q", out, want)
	}
}

// TestSpawnCreatesSession exercises the --spawn path: no session exists, and
// the attach command starts a detached supervisor by itself.
func TestSpawnCreatesSession(t *testing.T) {
	bin := buildTetherd(t)
	home := shortHome(t)

	a := startAttach(t, exec.Command(bin, "attach", "--home", home, "--session", "spawned", "--spawn", "--shell", "/bin/cat"))
	marker := fmt.Sprintf("tether-spawn-%d", time.Now().UnixNano())
	if err := a.sendHello(0); err != nil {
		t.Fatalf("send hello: %v", err)
	}
	if err := a.sendData([]byte(marker + "\n")); err != nil {
		t.Fatalf("send data: %v", err)
	}
	if _, seen := a.waitForData(marker, 1); !bytes.Contains(seen, []byte(marker)) {
		t.Fatalf("marker %q missing from spawned session output %q", marker, seen)
	}

	// The supervisor is detached (setsid), so killing the bridge leaves it
	// running; clean it up through the protocol.
	if out, err := exec.Command(bin, "kill", "--home", home, "--session", "spawned").CombinedOutput(); err != nil {
		t.Fatalf("kill spawned session: %v\n%s", err, out)
	}
	_ = a.waitForExit()
}
