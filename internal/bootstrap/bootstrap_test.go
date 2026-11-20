package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

const (
	testVersion  = "1.2.3"
	testProtocol = 7
)

// uploadCall records one Remote.Upload invocation.
type uploadCall struct {
	Local  string
	Remote string
	Mode   os.FileMode
}

// renameCall records one Remote.Rename invocation.
type renameCall struct {
	Old, New string
}

// fakeRemote is a scripted, in-memory Remote. It never touches the network.
type fakeRemote struct {
	mu sync.Mutex

	home    string
	homeErr error

	execFn func(cmd string) (string, error)
	execs  []string

	uploads   []uploadCall
	uploadErr error

	mkdirs []string

	renames    []renameCall
	renameHook func(oldPath, newPath string)
	renameErr  error
}

func (f *fakeRemote) Home(context.Context) (string, error) {
	if f.homeErr != nil {
		return "", f.homeErr
	}
	return f.home, nil
}

func (f *fakeRemote) Exec(_ context.Context, cmd string) (string, error) {
	f.mu.Lock()
	f.execs = append(f.execs, cmd)
	fn := f.execFn
	f.mu.Unlock()
	if fn == nil {
		return "", fmt.Errorf("fake: unexpected command %q", cmd)
	}
	return fn(cmd)
}

func (f *fakeRemote) Upload(_ context.Context, localPath, remotePath string, mode os.FileMode) error {
	f.mu.Lock()
	f.uploads = append(f.uploads, uploadCall{Local: localPath, Remote: remotePath, Mode: mode})
	err := f.uploadErr
	f.mu.Unlock()
	if err != nil {
		return err
	}
	return nil
}

func (f *fakeRemote) MkdirAll(_ context.Context, p string) error {
	f.mu.Lock()
	f.mkdirs = append(f.mkdirs, p)
	f.mu.Unlock()
	return nil
}

func (f *fakeRemote) Rename(_ context.Context, oldPath, newPath string) error {
	f.mu.Lock()
	f.renames = append(f.renames, renameCall{Old: oldPath, New: newPath})
	hook := f.renameHook
	err := f.renameErr
	f.mu.Unlock()
	if hook != nil {
		hook(oldPath, newPath)
	}
	return err
}

func (f *fakeRemote) Stat(context.Context, string) (bool, os.FileMode, error) {
	return false, 0, nil
}

func (f *fakeRemote) commands() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.execs...)
}

func (f *fakeRemote) uploadCalls() []uploadCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uploadCall(nil), f.uploads...)
}

func (f *fakeRemote) renameCalls() []renameCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]renameCall(nil), f.renames...)
}

func (f *fakeRemote) mkdirCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.mkdirs...)
}

// writeAsset creates an asset file and forces the mode, so umask cannot make an
// "executable" fixture non-executable.
func writeAsset(t *testing.T, dir, name string, mode os.FileMode, content string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), mode); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	if err := os.Chmod(p, mode); err != nil {
		t.Fatalf("chmod %s: %v", p, err)
	}
	return p
}

// skipIfStagedAsset skips a negative resolution test when the developer's own
// ~/.tether/bin already holds a usable asset, which would legitimately win.
func skipIfStagedAsset(t *testing.T, name string) {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		return
	}
	p := filepath.Join(home, ".tether", "bin", name)
	if info, err := os.Stat(p); err == nil && info.Mode().Perm()&0100 != 0 {
		t.Skipf("local %s exists and would be resolved", p)
	}
}

// installScript wires a fakeRemote that behaves like a host with no tetherd,
// then reports the expected version once Rename has installed it.
func installScript(t *testing.T, home, unameOut, pathOut, installedOut string) *fakeRemote {
	t.Helper()
	installed := false
	f := &fakeRemote{home: home}
	f.execFn = func(cmd string) (string, error) {
		switch {
		case cmd == "tetherd version":
			if pathOut != "" {
				return pathOut, nil
			}
			return "", errors.New("sh: tetherd: not found")
		case cmd == "uname -s; uname -m":
			return unameOut, nil
		case strings.Contains(cmd, "/.local/bin/tetherd version"):
			if installed {
				return fmt.Sprintf("tetherd %s (protocol %d)\n", testVersion, testProtocol), nil
			}
			if installedOut != "" {
				return installedOut, nil
			}
			return "", errors.New("no such file or directory")
		case strings.HasPrefix(cmd, "rm -f "):
			return "", nil
		}
		return "", fmt.Errorf("fake: unexpected command %q", cmd)
	}
	f.renameHook = func(_, _ string) { installed = true }
	return f
}

func TestEnsurePATHMatchSkipsUpload(t *testing.T) {
	f := &fakeRemote{home: "/home/dev"}
	f.execFn = func(cmd string) (string, error) {
		if cmd == "tetherd version" {
			return fmt.Sprintf("tetherd %s (protocol %d)\n", testVersion, testProtocol), nil
		}
		return "", fmt.Errorf("fake: unexpected command %q", cmd)
	}

	res, err := Ensure(context.Background(), f, Options{Version: testVersion, ProtocolVersion: testProtocol})
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if res.Path != "tetherd" {
		t.Errorf("Path = %q, want tetherd", res.Path)
	}
	if res.Installed {
		t.Error("Installed = true, want false")
	}
	if res.Version != testVersion {
		t.Errorf("Version = %q, want %q", res.Version, testVersion)
	}
	if got := f.uploadCalls(); len(got) != 0 {
		t.Errorf("uploads = %v, want none", got)
	}
	if cmds := f.commands(); len(cmds) != 1 {
		t.Errorf("commands = %v, want only the PATH probe", cmds)
	}
}

func TestEnsureInstallsWhenMissing(t *testing.T) {
	assetDir := t.TempDir()
	asset := writeAsset(t, assetDir, "tetherd-linux-amd64", 0o755, "binary")

	f := installScript(t, "/home/dev", "Linux\r\nx86_64\r\n", "", "")

	res, err := Ensure(context.Background(), f, Options{
		Version:         testVersion,
		ProtocolVersion: testProtocol,
		AssetsDir:       assetDir,
	})
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	wantInstalled := "/home/dev/.local/bin/tetherd"
	if res.Path != wantInstalled {
		t.Errorf("Path = %q, want %q", res.Path, wantInstalled)
	}
	if !res.Installed {
		t.Error("Installed = false, want true")
	}
	if res.Version != testVersion {
		t.Errorf("Version = %q, want %q", res.Version, testVersion)
	}

	uploads := f.uploadCalls()
	if len(uploads) != 1 {
		t.Fatalf("uploads = %v, want exactly one", uploads)
	}
	up := uploads[0]
	if up.Local != asset {
		t.Errorf("upload source = %q, want asset %q", up.Local, asset)
	}
	if up.Mode != 0o755 {
		t.Errorf("upload mode = %04o, want 0755", up.Mode)
	}
	if dir := path.Dir(up.Remote); dir != "/home/dev/.local/bin" {
		t.Errorf("upload directory = %q, want /home/dev/.local/bin", dir)
	}
	if base := path.Base(up.Remote); !strings.HasPrefix(base, "tetherd.tmp-") {
		t.Errorf("upload name = %q, want a tetherd.tmp-* staging name", base)
	}

	renames := f.renameCalls()
	if len(renames) != 1 {
		t.Fatalf("renames = %v, want exactly one", renames)
	}
	if renames[0].Old != up.Remote {
		t.Errorf("rename source = %q, want upload path %q", renames[0].Old, up.Remote)
	}
	if renames[0].New != wantInstalled {
		t.Errorf("rename destination = %q, want %q", renames[0].New, wantInstalled)
	}

	mkdirs := f.mkdirCalls()
	if len(mkdirs) != 1 || mkdirs[0] != "/home/dev/.local/bin" {
		t.Errorf("mkdirs = %v, want [/home/dev/.local/bin]", mkdirs)
	}
	for _, cmd := range f.commands() {
		if strings.HasPrefix(cmd, "rm -f ") {
			t.Errorf("unexpected cleanup command on success: %q", cmd)
		}
	}
}

func TestEnsureReinstallsOnVersionMismatch(t *testing.T) {
	assetDir := t.TempDir()
	writeAsset(t, assetDir, "tetherd-linux-amd64", 0o755, "binary")

	// An older tetherd already sits at ~/.local/bin/tetherd.
	f := installScript(t, "/home/dev", "Linux\nx86_64\n", "",
		"tetherd 0.9.0 (protocol 7)\n")

	res, err := Ensure(context.Background(), f, Options{
		Version:         testVersion,
		ProtocolVersion: testProtocol,
		AssetsDir:       assetDir,
	})
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if !res.Installed {
		t.Error("Installed = false, want true after a version mismatch")
	}
	if got := f.uploadCalls(); len(got) != 1 {
		t.Errorf("uploads = %d, want 1", len(got))
	}
	if got := f.renameCalls(); len(got) != 1 || got[0].New != "/home/dev/.local/bin/tetherd" {
		t.Errorf("renames = %v, want one install rename", got)
	}
}

func TestEnsureReinstallsOnProtocolMismatch(t *testing.T) {
	assetDir := t.TempDir()
	writeAsset(t, assetDir, "tetherd-linux-amd64", 0o755, "binary")

	// Same version on PATH, different wire protocol: not usable.
	f := installScript(t, "/home/dev", "Linux\nx86_64\n",
		fmt.Sprintf("tetherd %s (protocol %d)\n", testVersion, testProtocol-1), "")

	res, err := Ensure(context.Background(), f, Options{
		Version:         testVersion,
		ProtocolVersion: testProtocol,
		AssetsDir:       assetDir,
	})
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if !res.Installed {
		t.Error("Installed = false, want true after a protocol mismatch")
	}
	if got := f.uploadCalls(); len(got) != 1 {
		t.Errorf("uploads = %d, want 1", len(got))
	}
}

func TestEnsureExplicitPathMissingIsError(t *testing.T) {
	f := &fakeRemote{home: "/home/dev"}
	f.execFn = func(cmd string) (string, error) {
		return "", errors.New("exit status 127")
	}

	_, err := Ensure(context.Background(), f, Options{
		Version:         testVersion,
		ProtocolVersion: testProtocol,
		RemoteBin:       "/opt/x/tetherd",
	})
	if err == nil {
		t.Fatal("Ensure: want error for a missing explicit remote binary")
	}
	if !strings.Contains(err.Error(), "probe /opt/x/tetherd") {
		t.Errorf("error = %q, want it to name the probed path", err)
	}
	if !strings.Contains(err.Error(), "--remote-bin") {
		t.Errorf("error = %q, want it to mention --remote-bin", err)
	}
	if got := f.uploadCalls(); len(got) != 0 {
		t.Errorf("uploads = %v, want none for an explicit path", got)
	}
	if got := f.renameCalls(); len(got) != 0 {
		t.Errorf("renames = %v, want none for an explicit path", got)
	}
}

func TestEnsureExplicitPathMatch(t *testing.T) {
	f := &fakeRemote{home: "/home/dev"}
	f.execFn = func(cmd string) (string, error) {
		if cmd == "/opt/x/tetherd version" {
			return fmt.Sprintf("tetherd %s (protocol %d)\n", testVersion, testProtocol), nil
		}
		return "", fmt.Errorf("fake: unexpected command %q", cmd)
	}

	res, err := Ensure(context.Background(), f, Options{
		Version:         testVersion,
		ProtocolVersion: testProtocol,
		RemoteBin:       "/opt/x/tetherd",
	})
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	if res.Path != "/opt/x/tetherd" || res.Installed || res.Version != testVersion {
		t.Errorf("Result = %+v, want explicit path, no install, %s", res, testVersion)
	}
	if got := f.uploadCalls(); len(got) != 0 {
		t.Errorf("uploads = %v, want none", got)
	}
}

func TestEnsureExplicitPathMismatchIsError(t *testing.T) {
	f := &fakeRemote{home: "/home/dev"}
	f.execFn = func(cmd string) (string, error) {
		return "tetherd 0.9.0 (protocol 7)\n", nil
	}

	_, err := Ensure(context.Background(), f, Options{
		Version:         testVersion,
		ProtocolVersion: testProtocol,
		RemoteBin:       "/opt/x/tetherd",
	})
	if err == nil {
		t.Fatal("Ensure: want error for a mismatched explicit remote binary")
	}
	if !strings.Contains(err.Error(), "0.9.0") || !strings.Contains(err.Error(), "--remote-bin") {
		t.Errorf("error = %q, want mismatch details and --remote-bin advice", err)
	}
	if got := f.uploadCalls(); len(got) != 0 {
		t.Errorf("uploads = %v, want none", got)
	}
}

func TestEnsureUploadFailureCleansUpTemp(t *testing.T) {
	assetDir := t.TempDir()
	writeAsset(t, assetDir, "tetherd-linux-amd64", 0o755, "binary")

	f := installScript(t, "/home/dev", "Linux\nx86_64\n", "", "")
	f.uploadErr = errors.New("disk full")

	res, err := Ensure(context.Background(), f, Options{
		Version:         testVersion,
		ProtocolVersion: testProtocol,
		AssetsDir:       assetDir,
	})
	if err == nil {
		t.Fatal("Ensure: want an upload error")
	}
	if !strings.Contains(err.Error(), "upload") {
		t.Errorf("error = %q, want it to mention the upload", err)
	}
	if res.Installed || res.Path != "" {
		t.Errorf("Result = %+v, want the zero value on failure", res)
	}
	uploads := f.uploadCalls()
	if len(uploads) != 1 {
		t.Fatalf("uploads = %v, want the attempted upload", uploads)
	}
	if got := f.renameCalls(); len(got) != 0 {
		t.Errorf("renames = %v, want none after a failed upload", got)
	}

	var cleanup string
	for _, cmd := range f.commands() {
		if strings.HasPrefix(cmd, "rm -f ") {
			cleanup = cmd
		}
	}
	if cleanup == "" {
		t.Fatal("no cleanup command issued after a failed upload")
	}
	if !strings.Contains(cleanup, uploads[0].Remote) {
		t.Errorf("cleanup = %q, want it to remove %q", cleanup, uploads[0].Remote)
	}
}

func TestEnsureUnsupportedPlatform(t *testing.T) {
	f := installScript(t, "/home/dev", "Linux\nmips64\n", "", "")

	_, err := Ensure(context.Background(), f, Options{
		Version:         testVersion,
		ProtocolVersion: testProtocol,
	})
	if err == nil {
		t.Fatal("Ensure: want an error for an unsupported platform")
	}
	if !strings.Contains(err.Error(), "unsupported remote platform linux/mips64") {
		t.Errorf("error = %q, want it to name linux/mips64", err)
	}
	if got := f.uploadCalls(); len(got) != 0 {
		t.Errorf("uploads = %v, want none", got)
	}
}

func TestEnsureRenameFailureCleansUpTemp(t *testing.T) {
	assetDir := t.TempDir()
	writeAsset(t, assetDir, "tetherd-linux-amd64", 0o755, "binary")

	f := installScript(t, "/home/dev", "Linux\nx86_64\n", "", "")
	f.renameErr = errors.New("rename failed")

	if _, err := Ensure(context.Background(), f, Options{
		Version:         testVersion,
		ProtocolVersion: testProtocol,
		AssetsDir:       assetDir,
	}); err == nil {
		t.Fatal("Ensure: want a rename error")
	}

	uploads := f.uploadCalls()
	if len(uploads) != 1 {
		t.Fatalf("uploads = %v, want one", uploads)
	}
	var cleaned bool
	for _, cmd := range f.commands() {
		if strings.HasPrefix(cmd, "rm -f ") && strings.Contains(cmd, uploads[0].Remote) {
			cleaned = true
		}
	}
	if !cleaned {
		t.Error("temp file was not removed after a failed rename")
	}
}

func TestParseTarget(t *testing.T) {
	tests := []struct {
		name    string
		unameOS string
		machine string
		wantOS  string
		wantArc string
		wantErr string
	}{
		{name: "linux amd64", unameOS: "Linux", machine: "x86_64", wantOS: "linux", wantArc: "amd64"},
		{name: "linux amd64 alias", unameOS: "linux", machine: "amd64", wantOS: "linux", wantArc: "amd64"},
		{name: "linux arm64", unameOS: "Linux", machine: "aarch64", wantOS: "linux", wantArc: "arm64"},
		{name: "linux arm64 alias", unameOS: "Linux", machine: "arm64", wantOS: "linux", wantArc: "arm64"},
		{name: "linux armv7l", unameOS: "Linux", machine: "armv7l", wantOS: "linux", wantArc: "arm"},
		{name: "linux armv8l", unameOS: "Linux", machine: "armv8l", wantOS: "linux", wantArc: "arm"},
		{name: "linux arm", unameOS: "Linux", machine: "arm", wantOS: "linux", wantArc: "arm"},
		{name: "linux 386", unameOS: "Linux", machine: "i386", wantOS: "linux", wantArc: "386"},
		{name: "linux 686", unameOS: "Linux", machine: "i686", wantOS: "linux", wantArc: "386"},
		{name: "darwin amd64", unameOS: "Darwin", machine: "x86_64", wantOS: "darwin", wantArc: "amd64"},
		{name: "darwin arm64", unameOS: "Darwin", machine: "arm64", wantOS: "darwin", wantArc: "arm64"},
		{name: "darwin aarch64", unameOS: "Darwin", machine: "aarch64", wantOS: "darwin", wantArc: "arm64"},
		{name: "case and spaces", unameOS: "  LINUX ", machine: " X86_64\n", wantOS: "linux", wantArc: "amd64"},
		{name: "unknown os", unameOS: "FreeBSD", machine: "x86_64", wantErr: "unsupported remote platform freebsd/x86_64"},
		{name: "unknown arch", unameOS: "Linux", machine: "riscv64", wantErr: "unsupported remote platform linux/riscv64"},
		{name: "darwin 386", unameOS: "Darwin", machine: "i386", wantErr: "unsupported remote platform darwin/i386"},
		{name: "empty", unameOS: "", machine: "", wantErr: "unsupported remote platform /"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseTarget(tc.unameOS, tc.machine)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("ParseTarget(%q, %q): want error %q", tc.unameOS, tc.machine, tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %q, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseTarget(%q, %q): %v", tc.unameOS, tc.machine, err)
			}
			if got.OS != tc.wantOS || got.Arch != tc.wantArc {
				t.Errorf("Target = %+v, want %s/%s", got, tc.wantOS, tc.wantArc)
			}
			if got.UnameOS != tc.unameOS || got.Machine != tc.machine {
				t.Errorf("raw fields = %q/%q, want %q/%q", got.UnameOS, got.Machine, tc.unameOS, tc.machine)
			}
		})
	}
}

func TestParseVersionOutput(t *testing.T) {
	tests := []struct {
		name     string
		in       string
		wantVer  string
		wantProt int
		wantOK   bool
	}{
		{name: "canonical", in: "tetherd 1.2.3 (protocol 7)\n", wantVer: "1.2.3", wantProt: 7, wantOK: true},
		{name: "surrounding whitespace", in: "  tetherd 1.2.3 (protocol 7)  \n", wantVer: "1.2.3", wantProt: 7, wantOK: true},
		{name: "crlf", in: "tetherd 0.1.0-dev (protocol 1)\r\n", wantVer: "0.1.0-dev", wantProt: 1, wantOK: true},
		{name: "path prefix", in: "/home/dev/.local/bin/tetherd 1.2.3 (protocol 7)\n", wantVer: "1.2.3", wantProt: 7, wantOK: true},
		{name: "preamble line", in: "warning: something\ntetherd 1.2.3 (protocol 7)\n", wantVer: "1.2.3", wantProt: 7, wantOK: true},
		{name: "no protocol", in: "tetherd 1.2.3\n", wantOK: false},
		{name: "missing version", in: "tetherd (protocol 7)\n", wantOK: false},
		{name: "garbage", in: "command not found\n", wantOK: false},
		{name: "empty", in: "", wantOK: false},
		{name: "trailing garbage", in: "tetherd 1.2.3 (protocol 7) extra\n", wantOK: false},
		{name: "wrong word", in: "tether 1.2.3 (protocol 7)\n", wantOK: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ver, proto, ok := ParseVersionOutput(tc.in)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (ver=%q proto=%d)", ok, tc.wantOK, ver, proto)
			}
			if ver != tc.wantVer || proto != tc.wantProt {
				t.Errorf("got (%q, %d), want (%q, %d)", ver, proto, tc.wantVer, tc.wantProt)
			}
		})
	}
}

func TestAssetName(t *testing.T) {
	tests := []struct {
		goos, goarch, want string
	}{
		{"linux", "amd64", "tetherd-linux-amd64"},
		{"linux", "arm64", "tetherd-linux-arm64"},
		{"darwin", "arm64", "tetherd-darwin-arm64"},
		{"linux", "386", "tetherd-linux-386"},
	}
	for _, tc := range tests {
		if got := AssetName(tc.goos, tc.goarch); got != tc.want {
			t.Errorf("AssetName(%q, %q) = %q, want %q", tc.goos, tc.goarch, got, tc.want)
		}
	}
}

func TestResolveAssetPrefersAssetsDir(t *testing.T) {
	first := t.TempDir()
	second := t.TempDir()
	name := AssetName("linux", "arm64")
	want := writeAsset(t, first, name, 0o755, "first")
	writeAsset(t, second, name, 0o755, "second")

	got, err := ResolveAsset(Target{OS: "linux", Arch: "arm64"}, first, filepath.Join(second, "tether"))
	if err != nil {
		t.Fatalf("ResolveAsset: %v", err)
	}
	if got != want {
		t.Errorf("path = %q, want %q", got, want)
	}
}

func TestResolveAssetLocalBinSibling(t *testing.T) {
	dir := t.TempDir()
	name := AssetName("linux", "amd64")
	want := writeAsset(t, dir, name, 0o755, "binary")

	got, err := ResolveAsset(Target{OS: "linux", Arch: "amd64"}, "", filepath.Join(dir, "tether"))
	if err != nil {
		t.Fatalf("ResolveAsset: %v", err)
	}
	if got != want {
		t.Errorf("path = %q, want %q", got, want)
	}
}

func TestResolveAssetDistSibling(t *testing.T) {
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	dist := filepath.Join(root, "dist")
	name := AssetName("darwin", "arm64")
	want := writeAsset(t, dist, name, 0o700, "binary")

	got, err := ResolveAsset(Target{OS: "darwin", Arch: "arm64"}, "", filepath.Join(bin, "tether"))
	if err != nil {
		t.Fatalf("ResolveAsset: %v", err)
	}
	if got != want {
		t.Errorf("path = %q, want %q", got, want)
	}
}

func TestResolveAssetUnderscoreVariant(t *testing.T) {
	dir := t.TempDir()
	want := writeAsset(t, dir, "tetherd_darwin_arm64", 0o755, "binary")

	got, err := ResolveAsset(Target{OS: "darwin", Arch: "arm64"}, dir, "")
	if err != nil {
		t.Fatalf("ResolveAsset: %v", err)
	}
	if got != want {
		t.Errorf("path = %q, want %q", got, want)
	}
}

func TestResolveAssetRejectsNonExecutable(t *testing.T) {
	target := Target{OS: "linux", Arch: "amd64"}
	skipIfStagedAsset(t, AssetName(target.OS, target.Arch))
	skipIfStagedAsset(t, "tetherd_linux_amd64")

	dir := t.TempDir()
	p := writeAsset(t, dir, AssetName(target.OS, target.Arch), 0o644, "binary")

	_, err := ResolveAsset(target, dir, "")
	if err == nil {
		t.Fatal("ResolveAsset: want an error for a non-executable asset")
	}
	if !strings.Contains(err.Error(), p) {
		t.Errorf("error = %q, want it to list %q", err, p)
	}
}

func TestResolveAssetRejectsEmptyFile(t *testing.T) {
	target := Target{OS: "linux", Arch: "amd64"}
	skipIfStagedAsset(t, AssetName(target.OS, target.Arch))
	skipIfStagedAsset(t, "tetherd_linux_amd64")

	dir := t.TempDir()
	p := writeAsset(t, dir, AssetName(target.OS, target.Arch), 0o755, "")

	_, err := ResolveAsset(target, dir, "")
	if err == nil {
		t.Fatal("ResolveAsset: want an error for an empty asset")
	}
	if !strings.Contains(err.Error(), p) {
		t.Errorf("error = %q, want it to list %q", err, p)
	}
}

func TestResolveAssetErrorListsCandidates(t *testing.T) {
	target := Target{OS: "linux", Arch: "amd64"}
	skipIfStagedAsset(t, AssetName(target.OS, target.Arch))
	skipIfStagedAsset(t, "tetherd_linux_amd64")

	dir := t.TempDir()
	_, err := ResolveAsset(target, dir, "")
	if err == nil {
		t.Fatal("ResolveAsset: want an error when no asset exists")
	}
	msg := err.Error()
	for _, want := range []string{
		filepath.Join(dir, "tetherd-linux-amd64"),
		filepath.Join(dir, "tetherd_linux_amd64"),
		".tether",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("error = %q, want it to contain %q", msg, want)
		}
	}
}

func TestParseUnameMatchesEnsureProbe(t *testing.T) {
	unameOS, machine, err := parseUname("Linux\r\nx86_64\r\n")
	if err != nil {
		t.Fatalf("parseUname: %v", err)
	}
	if unameOS != "Linux" || machine != "x86_64" {
		t.Errorf("parseUname = %q/%q, want Linux/x86_64", unameOS, machine)
	}
	if _, _, err := parseUname("Linux\n"); err == nil {
		t.Error("parseUname: want an error for a single line")
	}
}

func TestShellQuote(t *testing.T) {
	tests := []struct{ in, want string }{
		{"tetherd", "tetherd"},
		{"/home/dev/.local/bin/tetherd", "/home/dev/.local/bin/tetherd"},
		{"", "''"},
		{"/tmp/a b", "'/tmp/a b'"},
		{"/tmp/it's", `'/tmp/it'\''s'`},
	}
	for _, tc := range tests {
		if got := shellQuote(tc.in); got != tc.want {
			t.Errorf("shellQuote(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
