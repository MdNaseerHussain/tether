package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/MdNaseerHussain/tether/internal/proto"
)

const (
	// bridgeChunk is the copy buffer size used in both directions.
	bridgeChunk = 16 << 10
	// probeTimeout bounds the dial used to discover a live socket.
	probeTimeout = 300 * time.Millisecond
	// pollInterval is how often EnsureSession retries the socket.
	pollInterval = 50 * time.Millisecond
	// defaultSpawnTimeout is used when EnsureSession gets a non-positive timeout.
	defaultSpawnTimeout = 5 * time.Second
)

// Bridge is a dumb byte bridge between stdio and a session socket. It copies
// bytes verbatim in both directions and never parses or reframes the protocol,
// so the caller owns framing. On stdin EOF it half-closes the connection and
// keeps reading until the supervisor closes, returning nil on a clean EOF.
func Bridge(ctx context.Context, socketPath string, stdin io.Reader, stdout io.Writer) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", socketPath)
	if err != nil {
		return fmt.Errorf("daemon: dial %s: %w", socketPath, err)
	}
	defer conn.Close()

	readErr := make(chan error, 1)
	go func() {
		_, err := io.CopyBuffer(stdout, conn, make([]byte, bridgeChunk))
		readErr <- err
	}()

	go func() {
		if _, err := io.CopyBuffer(conn, stdin, make([]byte, bridgeChunk)); err != nil {
			// The read side cannot make progress once the socket is broken.
			_ = conn.Close()
			return
		}
		// stdin is done: tell the supervisor we sent our last byte without
		// killing the read direction.
		if cw, ok := conn.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		} else {
			_ = conn.Close()
		}
	}()

	select {
	case err := <-readErr:
		return err
	case <-ctx.Done():
		_ = conn.Close()
		<-readErr
		return ctx.Err()
	}
}

// EnsureSession makes sure a live supervisor is listening on cfg.SocketPath,
// spawning a detached one when nothing answers. It polls until timeout.
func EnsureSession(ctx context.Context, cfg Config, timeout time.Duration) error {
	if err := cfg.Normalize(); err != nil {
		return err
	}
	if conn, err := net.DialTimeout("unix", cfg.SocketPath, probeTimeout); err == nil {
		_ = conn.Close()
		return nil
	}
	// Nothing answered: the socket file is stale at best.
	_ = os.Remove(cfg.SocketPath)

	if _, err := SpawnDetached(cfg); err != nil {
		return err
	}

	if timeout <= 0 {
		timeout = defaultSpawnTimeout
	}
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		conn, err := net.DialTimeout("unix", cfg.SocketPath, probeTimeout)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return fmt.Errorf("daemon: session %s did not start within %s: %w", cfg.SessionID, timeout, lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// dialAlive reports whether something accepts connections on the socket.
func dialAlive(socketPath string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("unix", socketPath, timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// sendKill delivers a single TypeKill frame without attaching.
func sendKill(socketPath string) error {
	conn, err := net.DialTimeout("unix", socketPath, probeTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return proto.WriteMessage(conn, proto.Message{Type: proto.TypeKill})
}

var errNoSession = errors.New("daemon: no such session")
