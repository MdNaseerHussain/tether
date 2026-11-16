package daemon

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/naseer-hussain/tether/internal/ringbuf"
)

// Config describes one session supervisor: which shell to run, where its socket
// and metadata live, and how much PTY output to retain.
type Config struct {
	// SessionID names the session. It is restricted to a conservative character
	// set because it is used as a file name stem.
	SessionID string
	// Home is the tether root directory (TETHER_HOME, default $HOME/.tether).
	Home string
	// SocketPath is the Unix socket the supervisor listens on. When empty,
	// Normalize derives it from Home and SessionID.
	SocketPath string
	// Shell is the program started on the PTY.
	Shell string
	// ShellArgs are extra arguments passed to Shell.
	ShellArgs []string
	// Env, when non-empty, replaces the environment of the shell (and of a
	// detached supervisor started by SpawnDetached). When empty the supervisor
	// and the shell inherit os.Environ.
	Env []string
	// BufferSize is the retention window in bytes.
	BufferSize int
	// Rows and Cols are the initial PTY geometry.
	Rows, Cols uint16
	// LogPath receives supervisor diagnostics. The child shell never writes
	// there; it owns the PTY.
	LogPath string
}

// DefaultConfig returns the configuration used when no flags are given.
func DefaultConfig() Config {
	home := os.Getenv("TETHER_HOME")
	if home == "" {
		if h, err := os.UserHomeDir(); err == nil {
			home = filepath.Join(h, ".tether")
		} else {
			home = ".tether"
		}
	}
	cfg := Config{
		SessionID:  "default",
		Home:       home,
		Shell:      defaultShell(),
		BufferSize: defaultBufferSize(),
		Rows:       24,
		Cols:       80,
	}
	cfg.SocketPath = SocketPath(cfg.Home, cfg.SessionID)
	cfg.LogPath = LogPath(cfg.Home, cfg.SessionID)
	return cfg
}

// Normalize fills in every empty field, resolves Home to an absolute path,
// validates the session id and makes sure <home>/sessions exists with mode 0700.
// It is idempotent.
func (c *Config) Normalize() error {
	if c.SessionID == "" {
		c.SessionID = "default"
	}
	if err := ValidateSessionID(c.SessionID); err != nil {
		return err
	}
	if c.Home == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("daemon: resolve home: %w", err)
		}
		c.Home = filepath.Join(home, ".tether")
	}
	abs, err := filepath.Abs(c.Home)
	if err != nil {
		return fmt.Errorf("daemon: resolve home %q: %w", c.Home, err)
	}
	c.Home = abs
	if c.Shell == "" {
		c.Shell = defaultShell()
	}
	if c.BufferSize <= 0 {
		c.BufferSize = defaultBufferSize()
	}
	if c.Rows == 0 {
		c.Rows = 24
	}
	if c.Cols == 0 {
		c.Cols = 80
	}
	if c.SocketPath == "" {
		c.SocketPath = SocketPath(c.Home, c.SessionID)
	}
	if c.LogPath == "" {
		c.LogPath = LogPath(c.Home, c.SessionID)
	}

	dir := filepath.Join(c.Home, "sessions")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("daemon: create %s: %w", dir, err)
	}
	// MkdirAll keeps the mode of an existing directory; tighten it explicitly
	// because everything in this package relies on 0700 being authoritative.
	if err := os.Chmod(dir, 0700); err != nil {
		return fmt.Errorf("daemon: chmod %s: %w", dir, err)
	}
	return nil
}

// SocketPath returns the session socket path for home and sessionID.
func SocketPath(home, sessionID string) string {
	return filepath.Join(home, "sessions", sessionID+".sock")
}

// LogPath returns the supervisor log path for home and sessionID.
func LogPath(home, sessionID string) string {
	return filepath.Join(home, "sessions", sessionID+".log")
}

var sessionIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// ValidateSessionID rejects ids that would escape the sessions directory or
// produce unreadable file names.
func ValidateSessionID(id string) error {
	if id == "" {
		return errors.New("daemon: empty session id")
	}
	if id == "." || id == ".." {
		return fmt.Errorf("daemon: invalid session id %q", id)
	}
	if !sessionIDPattern.MatchString(id) {
		return fmt.Errorf("daemon: invalid session id %q (want %s)", id, sessionIDPattern)
	}
	return nil
}

func defaultShell() string {
	if s := os.Getenv("TETHER_DEFAULT_SHELL"); s != "" {
		return s
	}
	if s := os.Getenv("SHELL"); s != "" {
		return s
	}
	return "/bin/bash"
}

func defaultBufferSize() int {
	if v := os.Getenv("TETHER_RINGBUF_BYTES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return ringbuf.DefaultCapacity
}

func pidPath(cfg Config) string {
	return filepath.Join(cfg.Home, "sessions", cfg.SessionID+".pid")
}

func metaPath(cfg Config) string {
	return filepath.Join(cfg.Home, "sessions", cfg.SessionID+".json")
}
