//go:build !windows

package daemon

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
)

// SpawnDetached starts a supervisor for cfg in its own session (setsid), with
// stdio redirected to the session log, and returns its pid. The supervisor
// outlives the process that spawned it, which is what makes sessions survive an
// SSH channel going away.
func SpawnDetached(cfg Config) (int, error) {
	if err := cfg.Normalize(); err != nil {
		return 0, err
	}

	exe, err := os.Executable()
	if err != nil {
		return 0, fmt.Errorf("daemon: locate tetherd: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(cfg.LogPath), 0700); err != nil {
		return 0, fmt.Errorf("daemon: create log directory: %w", err)
	}
	logFile, err := os.OpenFile(cfg.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return 0, fmt.Errorf("daemon: open log %s: %w", cfg.LogPath, err)
	}
	defer logFile.Close()

	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return 0, fmt.Errorf("daemon: open %s: %w", os.DevNull, err)
	}
	defer devNull.Close()

	args := []string{
		"serve",
		"--home", cfg.Home,
		"--session", cfg.SessionID,
		"--socket", cfg.SocketPath,
		"--shell", cfg.Shell,
		"--buffer-size", strconv.Itoa(cfg.BufferSize),
		"--rows", strconv.Itoa(int(cfg.Rows)),
		"--cols", strconv.Itoa(int(cfg.Cols)),
		"--log", cfg.LogPath,
	}

	cmd := exec.Command(exe, args...)
	cmd.Stdin = devNull
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if len(cfg.Env) > 0 {
		cmd.Env = cfg.Env
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("daemon: start supervisor: %w", err)
	}
	pid := cmd.Process.Pid
	_ = cmd.Process.Release()
	return pid, nil
}
