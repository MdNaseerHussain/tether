// Package bootstrap stages a matching tetherd binary on a remote host over an
// already authenticated SSH connection.
//
// The client probes a handful of well-known locations first and only uploads
// when the remote is missing a binary or runs a different version/protocol. The
// daemon is installed to $HOME/.local/bin/tetherd with mode 0755, which needs no
// privileges and is on the default PATH of most login shells.
//
// Everything here assumes a POSIX remote: a /bin/sh or bash login shell for
// system(3)-style exec, a $HOME environment variable, and a uname(1) that
// accepts -s and -m. No key material is ever stored or logged.
package bootstrap

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/MdNaseerHussain/tether/internal/version"
)

const (
	// installMode is the mode of the staged remote binary. 0755 makes it
	// executable for the owner and readable by everyone, which keeps
	// multi-device and container use working without a per-host chmod.
	installMode os.FileMode = 0755
	// installDir is the remote directory, relative to $HOME, that receives the
	// binary. ~/.local/bin is the XDG user executable directory.
	installDir = ".local/bin"
	// remoteBinaryName is the file name under installDir.
	remoteBinaryName = "tetherd"
	// maxStderr bounds how much captured stderr is echoed in an error.
	maxStderr = 200
)

// Target is the remote platform derived from uname output.
type Target struct {
	OS      string // "linux" | "darwin"
	Arch    string // "amd64" | "arm64" | "arm" | "386"
	UnameOS string // raw uname -s, trimmed
	Machine string // raw uname -m, trimmed
}

// Remote is the smallest set of operations bootstrap needs from a connection.
// It is implemented by NewSSHRemote and faked in tests.
type Remote interface {
	// Home returns the remote user's $HOME directory.
	Home(ctx context.Context) (string, error)
	// Exec runs cmd through the remote login shell and returns its stdout.
	Exec(ctx context.Context, cmd string) (stdout string, err error)
	// Upload copies localPath to remotePath and applies mode.
	Upload(ctx context.Context, localPath, remotePath string, mode os.FileMode) error
	// MkdirAll creates path and any missing parents (mode 0755 by convention).
	MkdirAll(ctx context.Context, path string) error
	// Rename renames oldPath to newPath, replacing newPath when the server
	// supports it.
	Rename(ctx context.Context, oldPath, newPath string) error
	// Stat reports whether path exists and its mode.
	Stat(ctx context.Context, path string) (exists bool, mode os.FileMode, err error)
}

// Options configures Ensure.
type Options struct {
	// Version is the local version.Version. Empty means the package default.
	Version string
	// ProtocolVersion is the local version.ProtocolVersion. Zero means the
	// package default.
	ProtocolVersion int
	// RemoteBin is the user's --remote-bin value. "tetherd" or "" selects auto
	// discovery and possible installation; anything containing "/" is treated
	// as an explicit path that is probed but never written.
	RemoteBin string
	// AssetsDir is an optional explicit directory searched first for the
	// staged tetherd binary.
	AssetsDir string
	// LocalBin is the path of the running tether executable, used to find a
	// sibling binary or ../dist next to it.
	LocalBin string
}

// Result describes what Ensure found or installed.
type Result struct {
	// Path is the argv[0] to use for the remote attach command.
	Path string
	// Installed reports whether this call uploaded a binary.
	Installed bool
	// OS and Arch identify the remote platform of an installed binary, so the
	// client can report what it staged.
	OS   string
	Arch string
	// Version is the remote version detected after Ensure succeeded.
	Version string
}

// Ensure locates a usable tetherd on the remote, installing one when needed.
//
// With an explicit remote path (Options.RemoteBin contains a separator) it only
// probes: an existing binary must match the local version and protocol exactly,
// otherwise the user gets an error telling them how to fix it. In auto mode it
// probes `tetherd` via PATH, then $HOME/.local/bin/tetherd, then uploads a new
// binary to $HOME/.local/bin/tetherd and verifies it.
func Ensure(ctx context.Context, r Remote, opts Options) (Result, error) {
	if r == nil {
		return Result{}, errors.New("bootstrap: nil remote")
	}
	if err := ctx.Err(); err != nil {
		return Result{}, fmt.Errorf("bootstrap: %w", err)
	}

	localVersion := opts.Version
	if localVersion == "" {
		localVersion = version.Version
	}
	localProtocol := opts.ProtocolVersion
	if localProtocol == 0 {
		localProtocol = version.ProtocolVersion
	}

	name := strings.TrimSpace(opts.RemoteBin)
	if strings.ContainsRune(name, '/') {
		return ensureExplicit(ctx, r, name, localVersion, localProtocol)
	}
	return ensureAuto(ctx, r, opts, localVersion, localProtocol)
}

// ensureExplicit probes a user-supplied path and never writes to it.
func ensureExplicit(ctx context.Context, r Remote, remotePath, localVersion string, localProtocol int) (Result, error) {
	out, err := r.Exec(ctx, shellQuote(remotePath)+" version")
	if err != nil {
		return Result{}, fmt.Errorf(
			"bootstrap: probe %s: %w; set --remote-bin to a valid tetherd path or remove --remote-bin to let tether install one",
			remotePath, err)
	}
	ver, proto, ok := ParseVersionOutput(out)
	if !ok {
		return Result{}, fmt.Errorf(
			"bootstrap: probe %s: unrecognized version output %q; set --remote-bin to a valid tetherd path or remove --remote-bin to let tether install one",
			remotePath, truncate(strings.TrimSpace(out), maxStderr))
	}
	if ver != localVersion || proto != localProtocol {
		return Result{}, fmt.Errorf(
			"bootstrap: remote %s is tetherd %s (protocol %d), want %s (protocol %d); set --remote-bin to a matching tetherd path or remove --remote-bin to let tether install one",
			remotePath, ver, proto, localVersion, localProtocol)
	}
	return Result{Path: remotePath, Version: ver}, nil
}

// ensureAuto probes PATH and ~/.local/bin before installing.
func ensureAuto(ctx context.Context, r Remote, opts Options, localVersion string, localProtocol int) (Result, error) {
	if out, err := r.Exec(ctx, remoteBinaryName+" version"); err == nil {
		if ver, proto, ok := ParseVersionOutput(out); ok && ver == localVersion && proto == localProtocol {
			return Result{Path: remoteBinaryName, Version: ver}, nil
		}
	}

	home, err := r.Home(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("bootstrap: resolve remote home: %w", err)
	}
	if home == "" {
		return Result{}, errors.New("bootstrap: remote $HOME is empty")
	}
	dir := path.Join(home, installDir)
	installed := path.Join(dir, remoteBinaryName)

	if out, err := r.Exec(ctx, shellQuote(installed)+" version"); err == nil {
		if ver, proto, ok := ParseVersionOutput(out); ok && ver == localVersion && proto == localProtocol {
			return Result{Path: installed, Version: ver}, nil
		}
	}
	return install(ctx, r, opts, dir, installed, localVersion, localProtocol)
}

// install uploads the matching asset next to the target, renames it into place
// and verifies the result.
func install(ctx context.Context, r Remote, opts Options, dir, installed, localVersion string, localProtocol int) (Result, error) {
	if err := r.MkdirAll(ctx, dir); err != nil {
		return Result{}, fmt.Errorf("bootstrap: create remote directory %s: %w", dir, err)
	}

	unameOut, err := r.Exec(ctx, "uname -s; uname -m")
	if err != nil {
		return Result{}, fmt.Errorf("bootstrap: detect remote platform: %w", err)
	}
	unameOS, machine, err := parseUname(unameOut)
	if err != nil {
		return Result{}, err
	}
	target, err := ParseTarget(unameOS, machine)
	if err != nil {
		return Result{}, err
	}
	asset, err := ResolveAsset(target, opts.AssetsDir, opts.LocalBin)
	if err != nil {
		return Result{}, err
	}

	tmp, err := tempPath(installed)
	if err != nil {
		return Result{}, err
	}
	if err := r.Upload(ctx, asset, tmp, installMode); err != nil {
		removeTemp(ctx, r, tmp)
		return Result{}, fmt.Errorf("bootstrap: upload %s for %s/%s: %w",
			filepath.Base(asset), target.OS, target.Arch, err)
	}
	if err := r.Rename(ctx, tmp, installed); err != nil {
		removeTemp(ctx, r, tmp)
		return Result{}, fmt.Errorf("bootstrap: install %s: %w", installed, err)
	}

	out, err := r.Exec(ctx, shellQuote(installed)+" version")
	if err != nil {
		return Result{}, fmt.Errorf("bootstrap: verify %s: %w", installed, err)
	}
	ver, proto, ok := ParseVersionOutput(out)
	if !ok || ver != localVersion || proto != localProtocol {
		return Result{}, fmt.Errorf(
			"bootstrap: verify %s: installed binary reported %q, want tetherd %s (protocol %d)",
			installed, truncate(strings.TrimSpace(out), maxStderr), localVersion, localProtocol)
	}
	return Result{Path: installed, Installed: true, OS: target.OS, Arch: target.Arch, Version: ver}, nil
}

// removeTemp deletes a staged temp file. Failure is tolerated: the connection
// may already be gone, in which case there is nothing left to clean up from
// this process.
func removeTemp(ctx context.Context, r Remote, tmp string) {
	_, _ = r.Exec(ctx, "rm -f "+shellQuote(tmp))
}

// tempPath returns a unique sibling path for the atomic rename.
func tempPath(installed string) (string, error) {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("bootstrap: generate temp name: %w", err)
	}
	return installed + ".tmp-" + hex.EncodeToString(b[:]), nil
}

// parseUname splits the two-line `uname -s; uname -m` output. A trailing
// carriage return from a pty is tolerated.
func parseUname(out string) (unameOS, machine string, err error) {
	lines := make([]string, 0, 2)
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if line != "" {
			lines = append(lines, line)
		}
	}
	if len(lines) < 2 {
		return "", "", fmt.Errorf("bootstrap: unexpected uname output %q", truncate(strings.TrimSpace(out), maxStderr))
	}
	return lines[0], lines[1], nil
}

// ParseTarget maps uname output to a supported platform. Matching is
// case-insensitive and tolerates surrounding whitespace.
func ParseTarget(unameOS, machine string) (Target, error) {
	osName := strings.ToLower(strings.TrimSpace(unameOS))
	m := strings.ToLower(strings.TrimSpace(machine))
	t := Target{UnameOS: unameOS, Machine: machine}

	switch osName {
	case "linux":
		t.OS = "linux"
		switch m {
		case "x86_64", "amd64":
			t.Arch = "amd64"
		case "aarch64", "arm64":
			t.Arch = "arm64"
		case "armv7l", "armv8l", "arm":
			t.Arch = "arm"
		case "i386", "i686":
			t.Arch = "386"
		default:
			return Target{}, fmt.Errorf("bootstrap: unsupported remote platform %s/%s", osName, m)
		}
	case "darwin":
		t.OS = "darwin"
		switch m {
		case "x86_64":
			t.Arch = "amd64"
		case "aarch64", "arm64":
			t.Arch = "arm64"
		default:
			return Target{}, fmt.Errorf("bootstrap: unsupported remote platform %s/%s", osName, m)
		}
	default:
		return Target{}, fmt.Errorf("bootstrap: unsupported remote platform %s/%s", osName, m)
	}
	return t, nil
}

// versionPattern matches the `tetherd <version> (protocol <n>)` line that
// `tetherd version` prints, optionally prefixed by an absolute path.
var versionPattern = regexp.MustCompile(`(?m)(?:^|[\s/])tetherd[ \t]+(\S+)[ \t]+\(protocol[ \t]+(\d+)\)[ \t]*$`)

// ParseVersionOutput extracts the version and protocol from `tetherd version`
// output. Leading/trailing whitespace, CRLF and an absolute path prefix are
// tolerated; anything else must match exactly.
func ParseVersionOutput(s string) (ver string, protocol int, ok bool) {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	m := versionPattern.FindStringSubmatch(s)
	if m == nil {
		return "", 0, false
	}
	n, err := strconv.Atoi(m[2])
	if err != nil {
		return "", 0, false
	}
	return m[1], n, true
}

// AssetName is the canonical file name of a tetherd build for goos/goarch.
func AssetName(goos, goarch string) string {
	return "tetherd-" + goos + "-" + goarch
}

// ResolveAsset finds a local tetherd binary for target. It searches, in order:
// assetsDir, the directory of localBin, <localBin dir>/../dist, and
// ~/.tether/bin. In each directory it tries "tetherd-<os>-<arch>" then
// "tetherd_<os>_<arch>". The file must be a non-empty regular file with the
// owner execute bit set; the returned path is absolute.
func ResolveAsset(target Target, assetsDir, localBin string) (string, error) {
	names := []string{
		AssetName(target.OS, target.Arch),
		fmt.Sprintf("tetherd_%s_%s", target.OS, target.Arch),
	}
	dirs := assetDirs(assetsDir, localBin)

	var tried []string
	seen := make(map[string]bool, len(dirs))
	for _, dir := range dirs {
		abs, err := filepath.Abs(dir)
		if err != nil || seen[abs] {
			continue
		}
		seen[abs] = true
		for _, name := range names {
			candidate := filepath.Join(abs, name)
			tried = append(tried, candidate)
			info, err := os.Stat(candidate)
			if err != nil {
				continue
			}
			if !info.Mode().IsRegular() || info.Size() == 0 || info.Mode().Perm()&0100 == 0 {
				continue
			}
			return candidate, nil
		}
	}

	var b strings.Builder
	fmt.Fprintf(&b, "bootstrap: no tetherd asset for %s/%s; tried:", target.OS, target.Arch)
	for _, p := range tried {
		fmt.Fprintf(&b, "\n  %s", p)
	}
	return "", errors.New(b.String())
}

// assetDirs returns the deterministic search roots, in priority order. Empty
// and CWD-relative entries are skipped: a bare localBin name must not make
// resolution depend on where tether happened to be launched.
func assetDirs(assetsDir, localBin string) []string {
	dirs := make([]string, 0, 4)
	if assetsDir != "" {
		dirs = append(dirs, assetsDir)
	}
	if localBin != "" {
		if binDir := filepath.Dir(localBin); binDir != "." && binDir != "" {
			dirs = append(dirs, binDir, filepath.Join(binDir, "..", "dist"))
		}
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		dirs = append(dirs, filepath.Join(home, ".tether", "bin"))
	}
	return dirs
}

// safeShellWord matches strings that need no quoting for a POSIX shell.
var safeShellWord = regexp.MustCompile(`^[A-Za-z0-9_@%+=:,./-]+$`)

// shellQuote renders s as a single POSIX shell word. Remote paths are joined
// with path.Join, so only "/" separators reach this function.
func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	if safeShellWord.MatchString(s) {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// truncate shortens s to at most n bytes, keeping the local logs readable when
// a remote prints something huge.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
