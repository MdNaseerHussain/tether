package client

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/MdNaseerHussain/tether/internal/sshconfig"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/crypto/ssh/knownhosts"
	"golang.org/x/term"
)

// channel is one attach: the bidirectional byte stream that carries framed
// protocol messages to and from the remote `tetherd attach` process, plus the
// connection operations the reconnect logic needs.
type channel interface {
	io.Reader
	io.Writer
	// SetDeadline aborts in-flight reads and writes when t is in the past. It
	// is how a wedged transport (suspended laptop, frozen peer) is forced to
	// fail so the reader and writer loops can return.
	SetDeadline(t time.Time) error
	// Wait blocks until the remote command exits and reports its status.
	Wait() error
	Close() error
}

// dialFunc opens one attach channel. It is a function rather than a method so
// tests can substitute an in-memory pipe for SSH.
type dialFunc func(ctx context.Context) (channel, error)

// transport is one SSH connection with the remote attach command running.
type transport struct {
	client *ssh.Client
	sess   *ssh.Session
	raw    net.Conn
	in     io.WriteCloser
	out    io.Reader
	agent  io.Closer

	waitOnce sync.Once
	waitErr  error

	closeOnce sync.Once
	closeErr  error
}

// newTransport dials opts.Host, authenticates, optionally bootstraps a matching
// tetherd, and starts the remote attach command. The raw net.Conn is retained so
// a dead connection can be forced to fail with SetDeadline.
func newTransport(ctx context.Context, opts Options, bc *bootstrapState) (*transport, error) {
	addr := net.JoinHostPort(opts.Host, strconv.Itoa(opts.Port))

	hostKey, err := hostKeyCallback(opts)
	if err != nil {
		return nil, err
	}
	auths, agentConn, err := authMethods(opts)
	if err != nil {
		return nil, err
	}

	cfg := &ssh.ClientConfig{
		User:            opts.User,
		Auth:            auths,
		HostKeyCallback: hostKey,
	}

	raw, err := dialTransport(ctx, opts, addr)
	if err != nil {
		closeIfSet(agentConn)
		return nil, fmt.Errorf("tether: cannot connect to %s: %w", addr, err)
	}

	// The connect timeout does not cover the SSH handshake, so bound the
	// whole exchange with a deadline on the raw socket. Proxy transports may
	// ignore it, in which case the handshake's own read deadline applies.
	if opts.ConnectTimeout > 0 {
		_ = raw.SetDeadline(time.Now().Add(opts.ConnectTimeout))
	}
	conn, chans, reqs, err := ssh.NewClientConn(raw, addr, cfg)
	if err != nil {
		if pc, ok := raw.(*proxyConn); ok {
			err = proxyError(err, pc.stderr)
		}
		raw.Close()
		closeIfSet(agentConn)
		return nil, fmt.Errorf("tether: SSH handshake with %s failed: %w", addr, err)
	}
	_ = raw.SetDeadline(time.Time{})
	client := ssh.NewClient(conn, chans, reqs)

	fail := func(stage string, err error) (*transport, error) {
		client.Close()
		raw.Close()
		closeIfSet(agentConn)
		return nil, fmt.Errorf("tether: %s: %w", stage, err)
	}

	remoteBin := opts.RemoteBin
	if !opts.NoBootstrap {
		remoteBin, err = bc.ensure(ctx, client, opts)
		if err != nil {
			return fail("bootstrap tetherd", err)
		}
	}
	opts.RemoteBin = remoteBin

	sess, err := client.NewSession()
	if err != nil {
		return fail("cannot open SSH session", err)
	}
	in, err := sess.StdinPipe()
	if err != nil {
		return fail("cannot open remote stdin", err)
	}
	out, err := sess.StdoutPipe()
	if err != nil {
		return fail("cannot open remote stdout", err)
	}
	sess.Stderr = newStderrSink(opts)

	if err := sess.Start(remoteCommand(opts)); err != nil {
		return fail("cannot start "+opts.RemoteBin+" on "+opts.Host, err)
	}

	opts.logf("attached transport to %s as %s", addr, opts.User)
	return &transport{
		client: client,
		sess:   sess,
		raw:    raw,
		in:     in,
		out:    out,
		agent:  agentConn,
	}, nil
}

// dialTransport opens the TCP connection to addr, or runs the configured
// ProxyCommand and speaks SSH over its stdio.
func dialTransport(ctx context.Context, opts Options, addr string) (net.Conn, error) {
	proxy := strings.TrimSpace(opts.ProxyCommand)
	if proxy == "" || strings.EqualFold(proxy, "none") {
		dialer := net.Dialer{Timeout: opts.ConnectTimeout}
		return dialer.DialContext(ctx, "tcp", addr)
	}

	alias := opts.Alias
	if alias == "" {
		alias = opts.Host
	}
	command := sshconfig.Substitute(proxy, alias, opts.Host, opts.User, opts.Port)
	return newProxyConn(ctx, command, opts)
}

// Read implements io.Reader over the remote command's stdout.
func (t *transport) Read(p []byte) (int, error) { return t.out.Read(p) }

// Write implements io.Writer over the remote command's stdin.
func (t *transport) Write(p []byte) (int, error) { return t.in.Write(p) }

// SetDeadline forces pending and future I/O to fail when d is in the past.
func (t *transport) SetDeadline(d time.Time) error {
	if t.raw == nil {
		return nil
	}
	return t.raw.SetDeadline(d)
}

// Wait reports the remote command's exit status once it has exited.
func (t *transport) Wait() error {
	t.waitOnce.Do(func() { t.waitErr = t.sess.Wait() })
	return t.waitErr
}

// Close tears down the session, the client and the raw connection. It is safe to
// call more than once.
func (t *transport) Close() error {
	t.closeOnce.Do(func() {
		if t.in != nil {
			t.in.Close()
		}
		if t.sess != nil {
			t.sess.Close()
		}
		if t.client != nil {
			t.client.Close()
		}
		if t.raw != nil {
			t.raw.Close()
		}
		closeIfSet(t.agent)
	})
	return t.closeErr
}

// remoteCommand builds the shell command the client runs on the remote host.
// ssh.Session.Command joins its arguments with spaces and does no quoting, so
// the whole command is assembled here and passed as a single string.
func remoteCommand(opts Options) string {
	args := []string{opts.RemoteBin, "attach", "--session", opts.Session, "--spawn"}
	if opts.BufferSize > 0 {
		args = append(args, "--buffer-size", strconv.Itoa(opts.BufferSize))
	}
	quoted := make([]string, len(args))
	for i, a := range args {
		quoted[i] = shellQuote(a)
	}
	return strings.Join(quoted, " ")
}

// shellQuote quotes s for /bin/sh only when it contains characters the shell
// would interpret. Embedded single quotes use the usual '\” idiom.
func shellQuote(s string) string {
	safe := s != "" && strings.IndexFunc(s, func(r rune) bool { return !shellSafeRune(r) }) < 0
	if safe {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func shellSafeRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		return true
	}
	return strings.ContainsRune("._-+/:@%,=", r)
}

// HostKeyError reports an unrecoverable host key problem. The client never
// auto-accepts a changed key, so the reconnect loop must not retry it.
type HostKeyError struct {
	Host   string
	Reason string
}

func (e *HostKeyError) Error() string {
	return fmt.Sprintf("tether: host key verification failed for %s: %s", e.Host, e.Reason)
}

// hostKeyCallback verifies the server against known_hosts, or disables
// verification when opts.Insecure is set.
func hostKeyCallback(opts Options) (ssh.HostKeyCallback, error) {
	if opts.Insecure {
		return ssh.InsecureIgnoreHostKey(), nil
	}
	path := expandHome(opts.KnownHostsFile)
	cb, err := knownhosts.New(path)
	if err != nil {
		return nil, fmt.Errorf("tether: cannot load known_hosts file %s: %w (pass --known-hosts or use --insecure)", path, err)
	}
	return func(hostname string, remote net.Addr, key ssh.PublicKey) error {
		err := cb(hostname, remote, key)
		if err == nil {
			return nil
		}
		var keyErr *knownhosts.KeyError
		if errors.As(err, &keyErr) && len(keyErr.Want) > 0 {
			return &HostKeyError{
				Host: hostname,
				Reason: fmt.Sprintf("the server presented a different key than %s records; "+
					"remove the stale entry only if the host really changed", path),
			}
		}
		return &HostKeyError{
			Host:   hostname,
			Reason: fmt.Sprintf("%v (known_hosts: %s)", err, path),
		}
	}, nil
}

// authMethods builds the authentication chain: ssh-agent first, then the
// configured or default identity files, then interactive password prompts on
// /dev/tty. The returned closer, when non-nil, owns the agent socket.
func authMethods(opts Options) ([]ssh.AuthMethod, io.Closer, error) {
	var methods []ssh.AuthMethod
	var agentConn io.Closer

	sock := opts.IdentityAgent
	if sock == "" {
		sock = os.Getenv("SSH_AUTH_SOCK")
	}
	if sock != "" && !strings.EqualFold(sock, "none") {
		sock = expandHome(sock)
		conn, err := net.Dial("unix", sock)
		if err != nil {
			opts.logf("ssh-agent %s unavailable: %v", sock, err)
		} else {
			methods = append(methods, ssh.PublicKeysCallback(agent.NewClient(conn).Signers))
			agentConn = conn
		}
	}

	files := explicitIdentityFiles(opts)
	if len(files) > 0 {
		for _, file := range files {
			path := expandHome(file)
			signer, err := loadSigner(path)
			if err != nil {
				closeIfSet(agentConn)
				return nil, nil, fmt.Errorf("tether: cannot use identity file %s: %w", path, err)
			}
			methods = append(methods, ssh.PublicKeys(signer))
		}
	} else {
		for _, path := range defaultIdentityFiles() {
			signer, err := loadSigner(path)
			if err != nil {
				// A default key that cannot be used (passphrase prompt
				// declined, unreadable) is not fatal; another key or the agent
				// may still work.
				opts.logf("skipping %s: %v", path, err)
				continue
			}
			methods = append(methods, ssh.PublicKeys(signer))
		}
	}

	if ttyAvailable() {
		methods = append(methods,
			ssh.KeyboardInteractive(func(name, instruction string, questions []string, echos []bool) ([]string, error) {
				if instruction != "" {
					fmt.Fprintln(os.Stderr, instruction)
				}
				answers := make([]string, len(questions))
				for i, q := range questions {
					a, err := promptTTY(q, !echos[i])
					if err != nil {
						return nil, err
					}
					answers[i] = a
				}
				return answers, nil
			}),
			ssh.PasswordCallback(func() (string, error) {
				return promptTTY(fmt.Sprintf("%s@%s's password: ", opts.User, opts.Host), true)
			}),
		)
	}
	return methods, agentConn, nil
}

// loadSigner reads a private key, prompting for a passphrase on /dev/tty when
// the key is encrypted.
func loadSigner(path string) (ssh.Signer, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	signer, err := ssh.ParsePrivateKey(pem)
	if err == nil {
		return signer, nil
	}
	var missing *ssh.PassphraseMissingError
	if !errors.As(err, &missing) {
		return nil, err
	}
	passphrase, perr := promptTTY(fmt.Sprintf("Enter passphrase for key '%s': ", path), true)
	if perr != nil {
		return nil, perr
	}
	return ssh.ParsePrivateKeyWithPassphrase(pem, []byte(passphrase))
}

// explicitIdentityFiles merges the ssh_config list with the legacy single
// flag. Order is preserved: configuration order first, then -i/--identity when
// it is not already listed.
func explicitIdentityFiles(opts Options) []string {
	files := make([]string, 0, len(opts.IdentityFiles)+1)
	seen := make(map[string]bool, len(opts.IdentityFiles)+1)
	for _, path := range opts.IdentityFiles {
		if path == "" || seen[path] {
			continue
		}
		seen[path] = true
		files = append(files, path)
	}
	if opts.IdentityFile != "" && !seen[opts.IdentityFile] {
		files = append(files, opts.IdentityFile)
	}
	return files
}

// defaultIdentityFiles lists the standard key locations that exist.
func defaultIdentityFiles() []string {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil
	}
	var files []string
	for _, name := range []string{"id_ed25519", "id_rsa", "id_ecdsa"} {
		path := filepath.Join(home, ".ssh", name)
		if _, err := os.Stat(path); err == nil {
			files = append(files, path)
		}
	}
	return files
}

// promptTTY reads a line (or a password) from the controlling terminal, not from
// stdin, so piped input never feeds prompts.
func promptTTY(prompt string, secret bool) (string, error) {
	f, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return "", fmt.Errorf("tether: no terminal available for a prompt: %w", err)
	}
	defer f.Close()
	if !term.IsTerminal(int(f.Fd())) {
		return "", errors.New("tether: controlling terminal is not a TTY")
	}

	fmt.Fprint(f, prompt)
	if secret {
		p, err := term.ReadPassword(int(f.Fd()))
		fmt.Fprintln(f)
		return string(p), err
	}
	line, err := bufio.NewReader(f).ReadString('\n')
	return strings.TrimRight(line, "\r\n"), err
}

// ttyAvailable reports whether prompts can be shown on /dev/tty.
func ttyAvailable() bool {
	f, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return false
	}
	defer f.Close()
	return term.IsTerminal(int(f.Fd()))
}

// stderrSink captures the remote command's stderr. It is echoed locally while
// verbose and otherwise retained so diagnostics are not silently lost.
type stderrSink struct {
	mu   sync.Mutex
	out  io.Writer
	echo bool
	tail []byte
}

const stderrTailLimit = 4096

func newStderrSink(opts Options) *stderrSink {
	return &stderrSink{out: opts.Stderr, echo: opts.Verbose}
}

func (s *stderrSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tail = append(s.tail, p...)
	if len(s.tail) > stderrTailLimit {
		s.tail = append(s.tail[:0], s.tail[len(s.tail)-stderrTailLimit:]...)
	}
	if s.echo && s.out != nil {
		_, _ = s.out.Write(p)
	}
	return len(p), nil
}

// String returns the retained tail of the remote stderr stream.
func (s *stderrSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return string(s.tail)
}

// exitStatus reports the remote command's exit status once it has exited. The
// wait is bounded so a still-running bridge cannot stall the reconnect decision.
func exitStatus(ch channel) (code int, ok bool) {
	done := make(chan error, 1)
	go func() { done <- ch.Wait() }()
	select {
	case err := <-done:
		var exitErr *ssh.ExitError
		if errors.As(err, &exitErr) {
			return exitErr.Waitmsg.ExitStatus(), true
		}
		return 0, err == nil
	case <-time.After(2 * time.Second):
		return 0, false
	}
}

// expandHome resolves a leading ~ in a path.
func expandHome(path string) string {
	if path != "~" && !strings.HasPrefix(path, "~/") {
		return path
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return path
	}
	if path == "~" {
		return home
	}
	return filepath.Join(home, path[2:])
}

func closeIfSet(c io.Closer) {
	if c != nil {
		c.Close()
	}
}
