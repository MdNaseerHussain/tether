//go:build !windows

package e2e

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/creack/pty"
	"golang.org/x/crypto/ssh"
)

const (
	// sshUser is the login name the harness client presents. The server accepts
	// any key for any user, but the name still reaches the SSH server as real
	// connection metadata.
	sshUser = "tester"
	// sshConnectTimeout bounds one dial plus handshake. It is passed to the
	// real client, so a stalled blackholed handshake fails on this deadline.
	sshConnectTimeout = "3s"
	// blackholeWindow is exactly how long the link stays dead. The client must
	// notice the dead peer at nine silent seconds (three missed 3 s heartbeats)
	// and dial again before the heal, so ten seconds is both the scenario the
	// heartbeat budget promises and a hard upper bound on detection latency.
	blackholeWindow = 10 * time.Second
)

// errNoGoTool marks a build failure that should skip rather than fail.
var errNoGoTool = errors.New("go tool not found")

var (
	buildOnce  sync.Once
	buildErr   error
	buildDir   string
	tetherBin  string
	tetherdBin string
)

// TestMain builds the two real commands once for the whole package.
func TestMain(m *testing.M) {
	code := m.Run()
	if buildDir != "" {
		_ = os.RemoveAll(buildDir)
	}
	os.Exit(code)
}

func sourceRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("getwd: %w", err)
	}
	root := filepath.Clean(filepath.Join(wd, "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		return "", fmt.Errorf("not running inside the module checkout: %w", err)
	}
	return root, nil
}

// sharedBinaries compiles ./cmd/tether and ./cmd/tetherd once per test binary.
// The SSH tests always run the compiled programs, never the source in-process.
func sharedBinaries(t *testing.T) (string, string) {
	t.Helper()
	buildOnce.Do(func() {
		goTool, err := exec.LookPath("go")
		if err != nil {
			buildErr = fmt.Errorf("%w: %v", errNoGoTool, err)
			return
		}
		root, err := sourceRoot()
		if err != nil {
			buildErr = err
			return
		}
		dir, err := os.MkdirTemp("", "tether-e2e-bin-")
		if err != nil {
			buildErr = err
			return
		}
		for _, name := range []string{"tether", "tetherd"} {
			bin := filepath.Join(dir, name)
			cmd := exec.Command(goTool, "build", "-o", bin, "./cmd/"+name)
			cmd.Dir = root
			if out, err := cmd.CombinedOutput(); err != nil {
				buildErr = fmt.Errorf("go build ./cmd/%s: %v\n%s", name, err, out)
				_ = os.RemoveAll(dir)
				return
			}
		}
		buildDir = dir
		tetherBin = filepath.Join(dir, "tether")
		tetherdBin = filepath.Join(dir, "tetherd")
	})
	if buildErr != nil {
		if errors.Is(buildErr, errNoGoTool) {
			t.Skipf("cannot build tether: %v", buildErr)
		}
		t.Fatalf("build tether binaries: %v", buildErr)
	}
	return tetherBin, tetherdBin
}

// syncBuffer is a goroutine-safe sink for a subprocess stream.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}

func (b *syncBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

// eventually polls cond until it holds or the deadline expires, then fails with
// dump's output. Every wait in this harness goes through it: no unbounded
// sleeps and no sleep-as-synchronisation.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool, dump func() string) time.Duration {
	t.Helper()
	start := time.Now()
	deadline := start.Add(timeout)
	for {
		if cond() {
			return time.Since(start)
		}
		if time.Now().After(deadline) {
			detail := ""
			if dump != nil {
				detail = "\n" + dump()
			}
			t.Fatalf("timed out after %s waiting for %s%s", timeout, what, detail)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

var (
	// lineMarker matches the numbered lines of the replay workload.
	lineMarker = regexp.MustCompile(`LINE ([0-9]{5})`)
	// tickMarker matches the once-a-second ticks of the heartbeat workload.
	tickMarker = regexp.MustCompile(`TICK ([0-9]{5})`)
)

// scanStream walks the client's stdout as a state machine and reports which
// number is expected next plus every duplicate or gap it saw. It tolerates
// arbitrary bytes around the marker (ANSI/OSC escapes, CR from the remote PTY)
// but requires the numbers themselves to be strictly consecutive, which is the
// exactly-once claim: no duplicates and no gaps.
func scanStream(data []byte, marker *regexp.Regexp) (next int, problems []string) {
	next = 1
	rest := data
	for {
		i := bytes.IndexByte(rest, '\n')
		if i < 0 {
			break
		}
		line := rest[:i]
		rest = rest[i+1:]
		m := marker.FindSubmatch(line)
		if m == nil {
			continue
		}
		n, err := strconv.Atoi(string(m[1]))
		if err != nil {
			continue
		}
		switch {
		case n == next:
			next = n + 1
		case n < next:
			if len(problems) < 8 {
				problems = append(problems, fmt.Sprintf("duplicate %s %05d (expected %05d)", marker.String(), n, next))
			}
		default:
			if len(problems) < 8 {
				problems = append(problems, fmt.Sprintf("gap: saw %s %05d, expected %05d", marker.String(), n, next))
			}
			next = n + 1
		}
	}
	return next, problems
}

// clientProc is a running `tether` client with piped stdio.
type clientProc struct {
	cmd    *exec.Cmd
	stdout *syncBuffer
	stderr *syncBuffer

	done    chan struct{}
	waitErr error

	stopOnce sync.Once
}

func (c *clientProc) waitCode(t *testing.T, timeout time.Duration) int {
	t.Helper()
	select {
	case <-c.done:
	case <-time.After(timeout):
		t.Fatalf("client did not exit within %s", timeout)
	}
	if c.waitErr == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(c.waitErr, &exitErr) {
		return exitErr.ExitCode()
	}
	return -1
}

func (c *clientProc) running() bool {
	select {
	case <-c.done:
		return false
	default:
		return true
	}
}

func (c *clientProc) stop(t *testing.T) {
	c.stopOnce.Do(func() {
		if !c.running() {
			return
		}
		_ = c.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-c.done:
		case <-time.After(5 * time.Second):
			_ = c.cmd.Process.Kill()
			select {
			case <-c.done:
			case <-time.After(5 * time.Second):
				t.Errorf("client %d did not exit after SIGKILL", c.cmd.Process.Pid)
			}
		}
	})
}

// ptyProc is a running `tether` client whose stdio is a local pseudo-terminal,
// so the real SIGWINCH -> TIOCGWINSZ -> Resize path is exercised.
type ptyProc struct {
	cmd    *exec.Cmd
	master *os.File
	out    *syncBuffer

	done     chan struct{}
	readDone chan struct{}
	waitErr  error

	stopOnce sync.Once
}

func (p *ptyProc) output() []byte { return p.out.Bytes() }

func (p *ptyProc) stop(t *testing.T) {
	p.stopOnce.Do(func() {
		select {
		case <-p.done:
		default:
			_ = p.cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-p.done:
			case <-time.After(5 * time.Second):
				_ = p.cmd.Process.Kill()
				select {
				case <-p.done:
				case <-time.After(5 * time.Second):
					t.Errorf("pty client %d did not exit", p.cmd.Process.Pid)
				}
			}
		}
		// Closing the master unblocks the reader; the slave is already gone.
		_ = p.master.Close()
		select {
		case <-p.readDone:
		case <-time.After(5 * time.Second):
			t.Errorf("pty reader did not stop")
		}
		_ = p.cmd.Wait()
	})
}

// sshFixture owns one isolated deployment: a private tether home, a generated
// client key, a known_hosts entry for the proxy port, an in-process SSH server
// and the proxy in front of it.
type sshFixture struct {
	root       string
	home       string
	tetherBin  string
	tetherdBin string
	keyPath    string
	knownHosts string
	script     string
	session    string

	srv *sshServer
	prx *proxy

	// marker selects the numbered workload output scanStream validates.
	marker *regexp.Regexp

	client *clientProc
	pty    *ptyProc
}

func newSSHFixture(t *testing.T, scriptBody, session string) *sshFixture {
	t.Helper()

	tether, tetherd := sharedBinaries(t)
	home := shortHome(t)
	root := filepath.Dir(home)

	script := filepath.Join(root, "workload.sh")
	if err := os.WriteFile(script, []byte(scriptBody), 0o755); err != nil {
		t.Fatalf("write workload script: %v", err)
	}

	keyPath := filepath.Join(root, "id_ed25519")
	writeClientKey(t, keyPath)

	srv, err := newSSHServer(home, script, filepath.Dir(tetherd))
	if err != nil {
		t.Fatalf("start ssh server: %v", err)
	}
	t.Cleanup(srv.Close)

	prx, err := newProxy(srv.Addr())
	if err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	t.Cleanup(prx.Close)

	knownHosts := filepath.Join(root, "known_hosts")
	writeKnownHosts(t, knownHosts, prx.Port(), srv.HostKey())

	f := &sshFixture{
		root:       root,
		home:       home,
		tetherBin:  tether,
		tetherdBin: tetherd,
		keyPath:    keyPath,
		knownHosts: knownHosts,
		script:     script,
		session:    session,
		srv:        srv,
		prx:        prx,
		marker:     lineMarker,
	}
	t.Cleanup(func() { f.forceCleanup(t) })
	return f
}

// writeClientKey writes an unencrypted OpenSSH ed25519 identity, the format the
// real client's loadSigner parses with ssh.ParsePrivateKey.
func writeClientKey(t *testing.T, path string) {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "tether-e2e")
	if err != nil {
		t.Fatalf("marshal client key: %v", err)
	}
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatalf("write client key: %v", err)
	}
}

// writeKnownHosts records exactly one entry for the proxy endpoint, so host key
// verification is really exercised: a wrong key would abort the client.
func writeKnownHosts(t *testing.T, path string, port int, key ssh.PublicKey) {
	t.Helper()
	line := fmt.Sprintf("[127.0.0.1]:%d %s %s\n", port, key.Type(),
		base64.StdEncoding.EncodeToString(key.Marshal()))
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatalf("write known_hosts: %v", err)
	}
}

// clientEnv is deliberately minimal: no SSH_AUTH_SOCK and no TETHER_* from the
// developer's shell, so the identity file and the explicit --known-hosts are
// the only paths that can authenticate or verify.
func (f *sshFixture) clientEnv() []string {
	return []string{
		"HOME=" + f.root,
		"PATH=" + os.Getenv("PATH"),
		"TERM=xterm-256color",
		"LANG=C",
	}
}

func (f *sshFixture) clientArgs() []string {
	return []string{
		"-i", f.keyPath,
		"-p", strconv.Itoa(f.prx.Port()),
		"-l", sshUser,
		"--known-hosts", f.knownHosts,
		"--remote-bin", f.tetherdBin,
		// The harness counts remote execs; bootstrap would add a probe.
		"--no-bootstrap",
		"--session", f.session,
		"--connect-timeout", sshConnectTimeout,
		"127.0.0.1",
	}
}

func (f *sshFixture) startClient(t *testing.T) *clientProc {
	t.Helper()
	cmd := exec.Command(f.tetherBin, f.clientArgs()...)
	cmd.Env = f.clientEnv()

	c := &clientProc{
		cmd:    cmd,
		stdout: &syncBuffer{},
		stderr: &syncBuffer{},
		done:   make(chan struct{}),
	}
	cmd.Stdout = c.stdout
	cmd.Stderr = c.stderr
	// stdin stays /dev/null: the client's stdin pump sees EOF and the abort
	// watcher cannot fire, which keeps the tests independent of the terminal.
	if err := cmd.Start(); err != nil {
		t.Fatalf("start client %s: %v", strings.Join(cmd.Args, " "), err)
	}
	go func() {
		c.waitErr = cmd.Wait()
		close(c.done)
	}()
	f.client = c
	t.Cleanup(func() { c.stop(t) })
	return c
}

func (f *sshFixture) startPTYClient(t *testing.T, rows, cols uint16) *ptyProc {
	t.Helper()
	cmd := exec.Command(f.tetherBin, f.clientArgs()...)
	cmd.Env = f.clientEnv()

	master, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: rows, Cols: cols})
	if err != nil {
		t.Fatalf("start client under a pty: %v", err)
	}
	p := &ptyProc{
		cmd:      cmd,
		master:   master,
		out:      &syncBuffer{},
		done:     make(chan struct{}),
		readDone: make(chan struct{}),
	}
	go func() {
		defer close(p.readDone)
		buf := make([]byte, 32<<10)
		for {
			n, err := master.Read(buf)
			if n > 0 {
				_, _ = p.out.Write(buf[:n])
			}
			if err != nil {
				return
			}
		}
	}()
	go func() {
		p.waitErr = cmd.Wait()
		close(p.done)
	}()
	f.pty = p
	t.Cleanup(func() { p.stop(t) })
	return p
}

// sessionInfo reads the supervisor metadata through the real `tetherd list`.
func (f *sshFixture) sessionInfo(t *testing.T) listEntry {
	t.Helper()
	entries := runJSON(t, f.tetherdBin, "list", "--home", f.home, "--json")
	if len(entries) != 1 {
		t.Fatalf("list reported %d sessions, want 1: %+v\n%s", len(entries), entries, f.dumpAll())
	}
	return entries[0]
}

// dump renders everything a CI failure needs: both client streams, the parsed
// stream state and the SSH server's audit log.
func (f *sshFixture) dump(stdout, stderr []byte) string {
	var b strings.Builder
	fmt.Fprintf(&b, "fixture: session=%q home=%s script=%s\n", f.session, f.home, f.script)
	fmt.Fprintf(&b, "proxy: accepted=%d  ssh execs=%d  auths=%d\n", f.prx.Accepted(), f.srv.ExecCount(), f.srv.AuthCount())
	next, problems := scanStream(stdout, f.marker)
	fmt.Fprintf(&b, "stream: next %s (consumed %d), problems=%v\n", f.marker.String(), next-1, problems)
	fmt.Fprintf(&b, "--- client stdout (%d bytes) ---\n%s\n", len(stdout), stdout)
	fmt.Fprintf(&b, "--- client stderr (%d bytes) ---\n%s\n", len(stderr), stderr)
	fmt.Fprintf(&b, "--- ssh exec log ---\n%s\n", f.srv.ExecLog())
	fmt.Fprintf(&b, "--- ssh events ---\n%s\n", f.srv.EventLog())
	return b.String()
}

func (f *sshFixture) dumpClient(c *clientProc) string {
	return f.dump(c.stdout.Bytes(), c.stderr.Bytes())
}

func (f *sshFixture) dumpPTY(p *ptyProc) string {
	out := p.output()
	return f.dump(out, []byte("(merged into stdout: stderr shares the pty)"))
}

func (f *sshFixture) dumpAll() string {
	if f.pty != nil {
		return f.dumpPTY(f.pty)
	}
	if f.client != nil {
		return f.dumpClient(f.client)
	}
	return f.dump(nil, nil)
}

// stop shuts the fixture down in the order a user would: terminate the client,
// kill the remote session through the real CLI, then verify nothing survived.
func (f *sshFixture) stop(t *testing.T) {
	t.Helper()
	started := time.Now()
	defer func() { t.Logf("teardown: %s", time.Since(started).Round(time.Millisecond)) }()

	sock := filepath.Join(f.home, "sessions", f.session+".sock")
	pidFile := filepath.Join(f.home, "sessions", f.session+".pid")
	supPID := readPIDFile(pidFile)

	// Terminate the session through the real CLI first: an attached client is
	// supposed to learn about the exit and close itself.
	out, err := exec.Command(f.tetherdBin, "kill", "--home", f.home, "--session", f.session).CombinedOutput()
	if err != nil && !strings.Contains(string(out), "no such session") {
		t.Logf("tetherd kill --session %s: %v (%s)", f.session, err, strings.TrimSpace(string(out)))
	}
	if f.client != nil {
		f.client.stop(t)
	}
	if f.pty != nil {
		f.pty.stop(t)
	}

	// The supervisor removes its socket once the shell is reaped. Poll rather
	// than sleep, and force-kill by pid if it did not.
	if !waitPathGone(sock, 5*time.Second) {
		if supPID > 0 {
			_ = syscall.Kill(supPID, syscall.SIGKILL)
		}
		if !waitPathGone(sock, 5*time.Second) {
			t.Errorf("session socket %s survived tetherd kill\n%s", sock, f.dumpAll())
		}
	}

	if supPID > 0 {
		deadline := time.Now().Add(5 * time.Second)
		for processAlive(supPID) && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if processAlive(supPID) {
			_ = syscall.Kill(supPID, syscall.SIGKILL)
			t.Errorf("supervisor pid %d survived tetherd kill", supPID)
		}
	}

	f.srv.Close()
	f.prx.Close()

	if running := f.srv.Running(); running != 0 {
		t.Errorf("%d remote command(s) still running after shutdown\n%s", running, f.srv.ExecLog())
	}
	if left, _ := filepath.Glob(filepath.Join(f.home, "sessions", "*.sock")); len(left) != 0 {
		t.Errorf("session sockets left behind: %v", left)
	}
}

// forceCleanup is the belt-and-braces path run by t.Cleanup; it never fails a
// test that already finished, it only removes anything still alive.
func (f *sshFixture) forceCleanup(t *testing.T) {
	if f.client != nil {
		f.client.stop(t)
	}
	if f.pty != nil {
		f.pty.stop(t)
	}
	pidFile := filepath.Join(f.home, "sessions", f.session+".pid")
	if pid := readPIDFile(pidFile); pid > 0 {
		_ = exec.Command(f.tetherdBin, "kill", "--home", f.home, "--session", f.session).Run()
		if processAlive(pid) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	}
	f.srv.Close()
	f.prx.Close()
}

// waitPathGone polls until path no longer exists.
func waitPathGone(path string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// readPIDFile returns the pid stored in path, or 0 when it is absent or
// corrupt.
func readPIDFile(path string) int {
	buf, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(buf)))
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}
