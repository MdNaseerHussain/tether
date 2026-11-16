//go:build !windows

package daemon

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	// listProbeTimeout bounds the dial List performs per socket.
	listProbeTimeout = 200 * time.Millisecond
	// killWait is how long Kill waits for the supervisor to remove the socket.
	killWait = 3 * time.Second
)

// SessionInfo describes one session found under <home>/sessions.
type SessionInfo struct {
	ID         string    `json:"id"`
	SocketPath string    `json:"socket"`
	PID        int       `json:"pid"`
	Alive      bool      `json:"alive"`
	Head       uint64    `json:"head"`
	Attached   int       `json:"attached"`
	BufferSize int       `json:"buffer_size"`
	Started    time.Time `json:"started"`
}

// List enumerates the session sockets under home and reports whether each one
// still answers. Metadata is read from <id>.json; corrupt or missing metadata is
// ignored rather than fatal. Results are sorted by id.
func List(home string) ([]SessionInfo, error) {
	if home == "" {
		home = DefaultConfig().Home
	}
	dir := filepath.Join(home, "sessions")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []SessionInfo{}, nil
		}
		return nil, fmt.Errorf("daemon: list %s: %w", dir, err)
	}

	out := make([]SessionInfo, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sock") {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".sock")
		info := SessionInfo{
			ID:         id,
			SocketPath: filepath.Join(dir, entry.Name()),
		}
		if buf, err := os.ReadFile(filepath.Join(dir, id+".json")); err == nil {
			var meta sessionMeta
			if json.Unmarshal(buf, &meta) == nil {
				info.PID = meta.PID
				info.BufferSize = meta.BufferSize
				if started, err := time.Parse(time.RFC3339, meta.Started); err == nil {
					info.Started = started
				}
			}
		}
		if info.PID == 0 {
			info.PID = readPIDFile(filepath.Join(dir, id+".pid"))
		}
		info.Alive = dialAlive(info.SocketPath, listProbeTimeout)
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Kill terminates a session. It prefers the protocol (TypeKill over the socket)
// so the supervisor delivers Exit and cleans up after itself, and falls back to
// the recorded pid when the socket is unreachable. Whole-session removal is
// idempotent: killing an already-dead session reports an error only when there
// is nothing on disk at all.
func Kill(home, sessionID string) error {
	if err := ValidateSessionID(sessionID); err != nil {
		return err
	}
	if home == "" {
		home = DefaultConfig().Home
	}
	sock := SocketPath(home, sessionID)
	pidFile := filepath.Join(home, "sessions", sessionID+".pid")

	if err := sendKill(sock); err == nil {
		// The supervisor removes the socket once the shell is reaped.
		deadline := time.Now().Add(killWait)
		for time.Now().Before(deadline) {
			if _, statErr := os.Stat(sock); errors.Is(statErr, os.ErrNotExist) {
				return nil
			}
			time.Sleep(20 * time.Millisecond)
		}
		return nil
	}

	pid := readPIDFile(pidFile)
	if pid <= 0 {
		// Nothing to signal. If files are lying around, clear them so the next
		// attach does not trip over them.
		if _, statErr := os.Stat(sock); statErr == nil {
			removeSessionFiles(home, sessionID, pidAlive(pid))
			return nil
		}
		return fmt.Errorf("%w: %s", errNoSession, sessionID)
	}

	_ = syscall.Kill(pid, syscall.SIGTERM)
	if !waitProcessGone(pid, 3*time.Second) {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		_ = waitProcessGone(pid, time.Second)
	}
	if pidAlive(pid) {
		return fmt.Errorf("daemon: session %s (pid %d) did not exit", sessionID, pid)
	}
	removeSessionFiles(home, sessionID, false)
	return nil
}

// readPIDFile returns the pid stored in path, or 0 when it is missing/corrupt.
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

// pidAlive reports whether pid names a running process.
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// waitProcessGone polls until pid exits or timeout elapses.
func waitProcessGone(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !pidAlive(pid) {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return !pidAlive(pid)
}

// removeSessionFiles unlinks a dead session's socket, pid file and metadata.
// The log is kept for post-mortem inspection.
func removeSessionFiles(home, sessionID string, alive bool) {
	if alive {
		return
	}
	_ = os.Remove(SocketPath(home, sessionID))
	_ = os.Remove(filepath.Join(home, "sessions", sessionID+".pid"))
	_ = os.Remove(filepath.Join(home, "sessions", sessionID+".json"))
}
