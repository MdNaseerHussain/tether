package bootstrap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// sshRemote implements Remote over an already established SSH connection plus
// an SFTP subsystem channel.
//
// Ownership: the caller owns the *ssh.Client and must keep it open for as long
// as the Remote is used and close it itself afterwards. CloseRemote only closes
// the SFTP channel opened by NewSSHRemote.
type sshRemote struct {
	client *ssh.Client
	sftp   *sftp.Client
}

// NewSSHRemote opens an SFTP subsystem on client. The returned Remote can be
// released with CloseRemote (or by type-asserting to io.Closer).
func NewSSHRemote(client *ssh.Client) (Remote, error) {
	if client == nil {
		return nil, errors.New("bootstrap: nil ssh client")
	}
	sc, err := sftp.NewClient(client)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: open sftp subsystem: %w", err)
	}
	return &sshRemote{client: client, sftp: sc}, nil
}

// Close releases the SFTP channel. It never closes the underlying ssh.Client.
func (r *sshRemote) Close() error {
	if r.sftp == nil {
		return nil
	}
	err := r.sftp.Close()
	r.sftp = nil
	if err != nil {
		return fmt.Errorf("bootstrap: close sftp: %w", err)
	}
	return nil
}

// CloseRemote closes r when its implementation owns closable resources. It
// never closes the ssh.Client the remote was built from.
func CloseRemote(r Remote) error {
	if c, ok := r.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// Home asks the remote login shell for $HOME rather than assuming a layout;
// some sites run sshd with a home that differs from /etc/passwd.
func (r *sshRemote) Home(ctx context.Context) (string, error) {
	out, err := r.Exec(ctx, `printf %s "$HOME"`)
	if err != nil {
		return "", fmt.Errorf("bootstrap: read remote $HOME: %w", err)
	}
	home := strings.TrimSpace(out)
	if home == "" {
		return "", errors.New("bootstrap: remote $HOME is empty")
	}
	return home, nil
}

// Exec runs cmd through the remote login shell. stdout is returned; stderr is
// discarded except for a short excerpt in the error. Context cancellation
// closes the session, which kills the remote command.
func (r *sshRemote) Exec(ctx context.Context, cmd string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", fmt.Errorf("bootstrap: exec %q: %w", cmd, err)
	}
	sess, err := r.client.NewSession()
	if err != nil {
		return "", fmt.Errorf("bootstrap: exec %q: new session: %w", cmd, err)
	}
	defer sess.Close()

	var stdout, stderr bytes.Buffer
	sess.Stdout = &stdout
	sess.Stderr = &stderr

	if err := sess.Start(cmd); err != nil {
		return "", fmt.Errorf("bootstrap: exec %q: start: %w", cmd, err)
	}

	done := make(chan error, 1)
	go func() { done <- sess.Wait() }()

	select {
	case <-ctx.Done():
		_ = sess.Close()
		<-done
		return "", fmt.Errorf("bootstrap: exec %q: %w", cmd, ctx.Err())
	case err := <-done:
		if err != nil {
			if msg := strings.TrimSpace(stderr.String()); msg != "" {
				return stdout.String(), fmt.Errorf("bootstrap: exec %q: %w: %s", cmd, err, truncate(msg, maxStderr))
			}
			return stdout.String(), fmt.Errorf("bootstrap: exec %q: %w", cmd, err)
		}
	}
	return stdout.String(), nil
}

// Upload streams localPath to remotePath, creating or truncating it, then
// applies mode. A failed upload removes the partial remote file.
func (r *sshRemote) Upload(ctx context.Context, localPath, remotePath string, mode os.FileMode) (err error) {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("bootstrap: upload %s: %w", remotePath, err)
	}
	src, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("bootstrap: open local asset %s: %w", localPath, err)
	}
	defer src.Close()

	dst, err := r.sftp.Create(remotePath)
	if err != nil {
		return fmt.Errorf("bootstrap: create remote %s: %w", remotePath, err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = dst.Close()
			_ = r.sftp.Remove(remotePath)
		}
	}()

	if _, err := io.Copy(dst, ctxReader{ctx: ctx, r: src}); err != nil {
		return fmt.Errorf("bootstrap: copy to %s: %w", remotePath, err)
	}
	if err := dst.Chmod(mode); err != nil {
		return fmt.Errorf("bootstrap: chmod %s: %w", remotePath, err)
	}
	if err := dst.Close(); err != nil {
		return fmt.Errorf("bootstrap: close %s: %w", remotePath, err)
	}
	cleanup = false
	return nil
}

// MkdirAll creates path and its parents on the remote.
func (r *sshRemote) MkdirAll(ctx context.Context, p string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("bootstrap: mkdir %s: %w", p, err)
	}
	if err := r.sftp.MkdirAll(p); err != nil {
		return fmt.Errorf("bootstrap: mkdir %s: %w", p, err)
	}
	return nil
}

// Rename moves oldPath onto newPath. The OpenSSH posix-rename extension is
// preferred because it atomically replaces an existing target; the plain SFTP
// rename is the fallback for servers without the extension.
func (r *sshRemote) Rename(ctx context.Context, oldPath, newPath string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("bootstrap: rename %s -> %s: %w", oldPath, newPath, err)
	}
	posixErr := r.sftp.PosixRename(oldPath, newPath)
	if posixErr == nil {
		return nil
	}
	if err := r.sftp.Rename(oldPath, newPath); err != nil {
		return fmt.Errorf("bootstrap: rename %s -> %s: %w (posix-rename: %v)", oldPath, newPath, err, posixErr)
	}
	return nil
}

// Stat reports whether path exists. A missing file is (false, 0, nil), not an
// error.
func (r *sshRemote) Stat(ctx context.Context, p string) (bool, os.FileMode, error) {
	if err := ctx.Err(); err != nil {
		return false, 0, fmt.Errorf("bootstrap: stat %s: %w", p, err)
	}
	info, err := r.sftp.Stat(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || os.IsNotExist(err) {
			return false, 0, nil
		}
		return false, 0, fmt.Errorf("bootstrap: stat %s: %w", p, err)
	}
	return true, info.Mode(), nil
}

// ctxReader aborts a copy as soon as ctx is cancelled; io.Copy alone would keep
// reading a large asset after the user pressed Ctrl+C.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}
