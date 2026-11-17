//go:build !windows

package e2e

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/crypto/ssh"
)

// execRecord is one accepted `exec` request and the process it started. A
// record only exists once the process was started, so the count is the number
// of *successful* remote commands, which is the signal a reconnect test needs:
// a TCP accept or a completed handshake alone does not prove that the client
// got as far as running `tetherd attach`.
type execRecord struct {
	Command string
	Start   time.Time
	End     time.Time
	Err     string
}

// sshEvent is a timestamped line in the server's audit log.
type sshEvent struct {
	At   time.Time
	Text string
}

// sshServer is an in-process SSH server that behaves like the remote half of a
// real deployment: it authenticates public keys, accepts session channels and
// runs every `exec` request through /bin/sh in a fixed environment that points
// TETHER_HOME and SHELL at the fixture. Nothing about the tether protocol is
// simulated: the child processes are the real binaries.
type sshServer struct {
	home    string
	env     []string
	ln      net.Listener
	cfg     *ssh.ServerConfig
	hostKey ssh.PublicKey

	closeOnce sync.Once

	mu     sync.Mutex
	execs  []execRecord
	events []sshEvent
	auths  []string
	conns  map[*ssh.ServerConn]struct{}
	closed bool

	acceptWG sync.WaitGroup
	connWG   sync.WaitGroup
}

// newSSHServer listens on a loopback port and starts accepting immediately.
// binDir is prepended to PATH so a remote lookup of `tetherd` also works.
func newSSHServer(home, shell, binDir string) (*sshServer, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate host key: %w", err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		return nil, fmt.Errorf("host key signer: %w", err)
	}

	s := &sshServer{
		home: home,
		env: []string{
			"TETHER_HOME=" + home,
			"HOME=" + home,
			"PATH=" + binDir + ":/usr/local/bin:/usr/bin:/bin",
			"TERM=xterm-256color",
			"SHELL=" + shell,
			"LANG=C",
		},
		hostKey: signer.PublicKey(),
		conns:   make(map[*ssh.ServerConn]struct{}),
	}
	s.cfg = &ssh.ServerConfig{
		// Authentication is real: the client has to present a private key. Any
		// key is authorised because the harness owns both ends of the link, and
		// the tests assert that a key really was offered.
		PublicKeyCallback: func(meta ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			s.mu.Lock()
			s.auths = append(s.auths, key.Type())
			s.eventfLocked("auth user=%q key=%s", meta.User(), key.Type())
			s.mu.Unlock()
			return nil, nil
		},
	}
	s.cfg.AddHostKey(signer)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("ssh listen: %w", err)
	}
	s.ln = ln
	s.eventf("listening on %s", ln.Addr())
	s.acceptWG.Add(1)
	go s.acceptLoop()
	return s, nil
}

// Addr is the listener address the proxy dials.
func (s *sshServer) Addr() string { return s.ln.Addr().String() }

// HostKey is the key a known_hosts entry has to record.
func (s *sshServer) HostKey() ssh.PublicKey { return s.hostKey }

func (s *sshServer) eventf(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eventfLocked(format, args...)
}

func (s *sshServer) eventfLocked(format string, args ...any) {
	s.events = append(s.events, sshEvent{At: time.Now(), Text: fmt.Sprintf(format, args...)})
	if len(s.events) > 512 {
		s.events = append(s.events[:0], s.events[len(s.events)-512:]...)
	}
}

func (s *sshServer) acceptLoop() {
	defer s.acceptWG.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			s.eventf("listener stopped: %v", err)
			return
		}
		s.connWG.Add(1)
		go func() {
			defer s.connWG.Done()
			s.handleConn(conn)
		}()
	}
}

func (s *sshServer) handleConn(raw net.Conn) {
	sconn, chans, reqs, err := ssh.NewServerConn(raw, s.cfg)
	if err != nil {
		_ = raw.Close()
		s.eventf("handshake from %s failed: %v", raw.RemoteAddr(), err)
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = sconn.Close()
		return
	}
	s.conns[sconn] = struct{}{}
	s.mu.Unlock()
	s.eventf("connection established from %s", raw.RemoteAddr())

	defer func() {
		s.mu.Lock()
		delete(s.conns, sconn)
		s.mu.Unlock()
		_ = sconn.Close()
		s.eventf("connection from %s closed", raw.RemoteAddr())
	}()

	go ssh.DiscardRequests(reqs)

	cs := &connState{srv: s, procs: make(map[*exec.Cmd]struct{})}
	var wg sync.WaitGroup
	for newChan := range chans {
		if newChan.ChannelType() != "session" {
			_ = newChan.Reject(ssh.UnknownChannelType, "only session channels are supported")
			continue
		}
		ch, creqs, err := newChan.Accept()
		if err != nil {
			s.eventf("accept session channel: %v", err)
			continue
		}
		s.eventf("session channel open")
		wg.Add(1)
		go func() {
			defer wg.Done()
			cs.serve(ch, creqs)
			s.eventf("session channel closed")
		}()
	}

	// The transport is gone, which is what a dropped Wi-Fi link looks like to
	// sshd: the remote commands lose their channel and are hung up.
	cs.killAll()
	wg.Wait()
}

// connState tracks the remote commands belonging to one SSH transport so they
// can be hung up when the transport dies.
type connState struct {
	srv *sshServer

	mu    sync.Mutex
	procs map[*exec.Cmd]struct{}
}

func (c *connState) addProc(cmd *exec.Cmd) {
	c.mu.Lock()
	c.procs[cmd] = struct{}{}
	c.mu.Unlock()
}

func (c *connState) removeProc(cmd *exec.Cmd) {
	c.mu.Lock()
	delete(c.procs, cmd)
	c.mu.Unlock()
}

func (c *connState) livePIDs() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	pids := make([]int, 0, len(c.procs))
	for cmd := range c.procs {
		if cmd.Process != nil {
			pids = append(pids, cmd.Process.Pid)
		}
	}
	return pids
}

// killAll hangs up every command still running for this transport: SIGTERM to
// the process group, then SIGKILL after a grace period. The supervisor is
// detached (setsid), so it is never in these groups and keeps running.
func (c *connState) killAll() {
	pids := c.livePIDs()
	if len(pids) == 0 {
		return
	}
	for _, pid := range pids {
		_ = syscall.Kill(-pid, syscall.SIGTERM)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(c.livePIDs()) == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	for _, pid := range c.livePIDs() {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	}
}

// serve answers channel requests the way an OpenSSH server would for a command
// invocation: pty and window-change are accepted, exec runs the command, and
// shell/subsystem are refused because the client never asks for them.
func (c *connState) serve(ch ssh.Channel, reqs <-chan *ssh.Request) {
	defer ch.Close()
	for req := range reqs {
		switch req.Type {
		case "pty-req":
			_ = req.Reply(true, nil)
		case "window-change":
			_ = req.Reply(true, nil)
		case "env":
			// The harness environment is authoritative; the client does not
			// send env requests, so refusing keeps it honest.
			_ = req.Reply(false, nil)
		case "exec":
			var payload struct{ Command string }
			if err := ssh.Unmarshal(req.Payload, &payload); err != nil || payload.Command == "" {
				_ = req.Reply(false, nil)
				return
			}
			_ = req.Reply(true, nil)
			c.srv.runCommand(c, ch, payload.Command)
			return
		case "shell", "subsystem":
			_ = req.Reply(false, nil)
		default:
			_ = req.Reply(false, nil)
		}
	}
}

// runCommand starts /bin/sh -c <command> and wires it to the channel the way
// sshd does: stdout and stderr are the channel's two streams, stdin is the
// channel's read side, and the exit status is reported after the process and
// all of its output have finished.
func (s *sshServer) runCommand(c *connState, ch ssh.Channel, command string) {
	cmd := exec.Command("/bin/sh", "-c", command)
	cmd.Env = s.env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		s.eventf("exec %q: stdin pipe: %v", command, err)
		return
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		s.eventf("exec %q: stdout pipe: %v", command, err)
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		s.eventf("exec %q: stderr pipe: %v", command, err)
		return
	}
	if err := cmd.Start(); err != nil {
		s.eventf("exec %q: start: %v", command, err)
		return
	}

	s.mu.Lock()
	s.execs = append(s.execs, execRecord{Command: command, Start: time.Now()})
	idx := len(s.execs) - 1
	s.mu.Unlock()
	s.eventf("exec #%d started: %s", idx+1, command)

	c.addProc(cmd)
	defer c.removeProc(cmd)

	// The stdin copy is deliberately not waited for: it can only finish when
	// the channel closes, which happens after the command has exited.
	go func() {
		_, _ = io.Copy(stdin, ch)
		_ = stdin.Close()
	}()

	var outWG sync.WaitGroup
	outWG.Add(2)
	go func() {
		defer outWG.Done()
		_, _ = io.Copy(ch, stdout)
	}()
	go func() {
		defer outWG.Done()
		_, _ = io.Copy(ch.Stderr(), stderr)
	}()

	waitErr := cmd.Wait()
	status := 0
	if waitErr != nil {
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			status = exitErr.ExitCode()
		}
		errText := ""
		if waitErr != nil {
			errText = waitErr.Error()
		}
		// Wait closes the pipes, so the copies end here.
		outWG.Wait()
		s.mu.Lock()
		s.execs[idx].End = time.Now()
		s.execs[idx].Err = errText
		s.mu.Unlock()
	} else {
		outWG.Wait()
		s.mu.Lock()
		s.execs[idx].End = time.Now()
		s.mu.Unlock()
	}
	s.eventf("exec #%d exited with status %d", idx+1, status)

	_, _ = ch.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{uint32(status)}))
}

// ExecCount is the number of remote commands that were actually started.
func (s *sshServer) ExecCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.execs)
}

// AuthCount is the number of successful public-key authentications.
func (s *sshServer) AuthCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.auths)
}

// Running is the number of remote commands that have not been reaped yet.
func (s *sshServer) Running() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, e := range s.execs {
		if e.End.IsZero() {
			n++
		}
	}
	return n
}

// ExecLog renders the exec history for failure output.
func (s *sshServer) ExecLog() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.execs) == 0 {
		return "(no exec requests)"
	}
	var b strings.Builder
	for i, e := range s.execs {
		end := "-"
		if !e.End.IsZero() {
			end = e.End.Format("15:04:05.000")
		}
		fmt.Fprintf(&b, "#%d start=%s end=%s command=%s err=%s\n",
			i+1, e.Start.Format("15:04:05.000"), end, e.Command, e.Err)
	}
	return b.String()
}

// EventLog renders the connection/exec audit trail for failure output.
func (s *sshServer) EventLog() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b strings.Builder
	for _, e := range s.events {
		fmt.Fprintf(&b, "%s %s\n", e.At.Format("15:04:05.000"), e.Text)
	}
	return b.String()
}

// Close stops the listener, tears down every live transport and waits for the
// per-connection goroutines, so no sshd-like goroutine outlives the test.
func (s *sshServer) Close() {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		conns := make([]*ssh.ServerConn, 0, len(s.conns))
		for c := range s.conns {
			conns = append(conns, c)
		}
		s.mu.Unlock()

		_ = s.ln.Close()
		for _, c := range conns {
			_ = c.Close()
		}

		done := make(chan struct{})
		go func() {
			s.acceptWG.Wait()
			s.connWG.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
	})
}
