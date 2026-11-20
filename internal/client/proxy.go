package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// proxyConn adapts a ProxyCommand's stdio to net.Conn so the SSH handshake and
// the session run over it unchanged. stdin feeds the child, stdout is what the
// child writes, and stderr is retained (bounded) for diagnostics.
//
// Close is idempotent and always reaps the child: SIGTERM first, SIGKILL after
// proxyKillGrace, so a wedged proxy cannot outlive the client.
type proxyConn struct {
	cmd    *exec.Cmd
	stdin  *os.File
	stdout *os.File
	stderr *tailBuffer

	localAddr  net.Addr
	remoteAddr net.Addr

	closeOnce sync.Once
	closeErr  error
}

const (
	// proxyKillGrace is how long a ProxyCommand gets to exit after SIGTERM.
	proxyKillGrace = 2 * time.Second
	// proxyStderrLimit bounds the stderr retained for error messages.
	proxyStderrLimit = 8 << 10
)

// newProxyConn starts `/bin/sh -c command` and returns a net.Conn over its
// stdio. The child is tied to ctx so cancellation tears it down as well.
func newProxyConn(ctx context.Context, command string, opts Options) (net.Conn, error) {
	inR, inW, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("proxy command: create stdin pipe: %w", err)
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		inR.Close()
		inW.Close()
		return nil, fmt.Errorf("proxy command: create stdout pipe: %w", err)
	}

	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", command)
	cmd.Stdin = inR
	cmd.Stdout = outW
	stderr := &tailBuffer{limit: proxyStderrLimit}
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		inR.Close()
		inW.Close()
		outR.Close()
		outW.Close()
		return nil, fmt.Errorf("proxy command %q: %w", command, err)
	}
	// The child owns the other ends now; keeping them open in the parent would
	// prevent EOF on the pipes when it exits.
	inR.Close()
	outW.Close()

	opts.logf("using ProxyCommand %q", command)
	return &proxyConn{
		cmd:        cmd,
		stdin:      inW,
		stdout:     outR,
		stderr:     stderr,
		localAddr:  proxyAddr("proxy:" + command),
		remoteAddr: proxyAddr(net.JoinHostPort(opts.Host, fmt.Sprintf("%d", opts.Port))),
	}, nil
}

// Read reads the proxy's stdout.
func (c *proxyConn) Read(p []byte) (int, error) { return c.stdout.Read(p) }

// Write writes to the proxy's stdin.
func (c *proxyConn) Write(p []byte) (int, error) { return c.stdin.Write(p) }

// Close closes both pipes, terminates the child and reaps it.
func (c *proxyConn) Close() error {
	c.closeOnce.Do(func() {
		_ = c.stdin.Close()
		_ = c.stdout.Close()

		if c.cmd.Process == nil {
			return
		}
		_ = c.cmd.Process.Signal(syscall.SIGTERM)

		done := make(chan error, 1)
		go func() { done <- c.cmd.Wait() }()
		select {
		case err := <-done:
			// A proxy killed by our SIGTERM exits with a signal status; that
			// is a normal shutdown, not an error worth surfacing.
			var exitErr *exec.ExitError
			if err != nil && !errors.As(err, &exitErr) {
				c.closeErr = err
			}
		case <-time.After(proxyKillGrace):
			_ = c.cmd.Process.Kill()
			<-done
		}
	})
	return c.closeErr
}

// SetDeadline applies d to the pipes. Both ends are pollable os.Files on Unix,
// so this is a real deadline rather than a no-op; a transport that cannot
// honour it is still unblocked by Close on the heartbeat path.
func (c *proxyConn) SetDeadline(d time.Time) error {
	var err error
	if e := c.stdout.SetDeadline(d); e != nil {
		err = e
	}
	if e := c.stdin.SetDeadline(d); e != nil && err == nil {
		err = e
	}
	return err
}

// SetReadDeadline applies d to the proxy's stdout pipe.
func (c *proxyConn) SetReadDeadline(d time.Time) error { return c.stdout.SetDeadline(d) }

// SetWriteDeadline applies d to the proxy's stdin pipe.
func (c *proxyConn) SetWriteDeadline(d time.Time) error { return c.stdin.SetDeadline(d) }

// LocalAddr reports a synthetic address for the proxy process.
func (c *proxyConn) LocalAddr() net.Addr { return c.localAddr }

// RemoteAddr reports the host:port the proxy was asked to reach.
func (c *proxyConn) RemoteAddr() net.Addr { return c.remoteAddr }

// proxyAddr is a minimal net.Addr for transports that have no socket of their
// own.
type proxyAddr string

func (a proxyAddr) Network() string { return "proxy" }
func (a proxyAddr) String() string  { return string(a) }

// tailBuffer keeps the last limit bytes written to it. Remote proxies tend to
// print a diagnostic line just before failing, which is exactly the part a
// fixed-size head buffer would lose.
type tailBuffer struct {
	mu    sync.Mutex
	limit int
	buf   []byte
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	if len(b.buf) > b.limit {
		b.buf = append(b.buf[:0], b.buf[len(b.buf)-b.limit:]...)
	}
	return len(p), nil
}

// String returns the retained tail, trimmed for one-line errors.
func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

// proxyError decorates an error with the proxy's stderr tail.
func proxyError(err error, stderr *tailBuffer) error {
	if msg := stderr.String(); msg != "" {
		return fmt.Errorf("%w: %s", err, msg)
	}
	return err
}

var _ io.ReadWriteCloser = (*proxyConn)(nil)
