package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MdNaseerHussain/tether/internal/client"
)

const testSSHConfig = `
Host dev
	HostName devbox.internal
	User deploy
	Port 2222
	IdentityFile ~/.ssh/dev
	IdentityAgent ~/.ssh/agent.sock
	ProxyCommand nc -X 5 -x proxy.internal:1080 %h %p

Host plain
	HostName plain.example.com
`

// resolveTest runs the real flag parser and resolver, as main does.
func resolveTest(t *testing.T, args ...string) (client.Options, error) {
	t.Helper()
	fs, set, flags, err := newFlagSet(args)
	if err != nil {
		return client.Options{}, err
	}
	return resolveOptions(flags, fs, set)
}

func writeConfig(t *testing.T) (dir, path string) {
	t.Helper()
	dir = t.TempDir()
	path = filepath.Join(dir, "config")
	if err := os.WriteFile(path, []byte(testSSHConfig), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return dir, path
}

func TestResolveAliasFromConfig(t *testing.T) {
	home, path := writeConfig(t)
	t.Setenv("HOME", home)

	opts, err := resolveTest(t, "--ssh-config", path, "dev")
	if err != nil {
		t.Fatalf("resolveOptions: %v", err)
	}
	if opts.Host != "devbox.internal" {
		t.Errorf("Host = %q, want devbox.internal", opts.Host)
	}
	if opts.User != "deploy" {
		t.Errorf("User = %q, want deploy", opts.User)
	}
	if opts.Port != 2222 {
		t.Errorf("Port = %d, want 2222", opts.Port)
	}
	if opts.Alias != "dev" {
		t.Errorf("Alias = %q, want dev", opts.Alias)
	}
	if len(opts.IdentityFiles) != 1 || opts.IdentityFiles[0] != filepath.Join(home, ".ssh", "dev") {
		t.Errorf("IdentityFiles = %v, want [%s]", opts.IdentityFiles, filepath.Join(home, ".ssh", "dev"))
	}
	if opts.IdentityAgent != filepath.Join(home, ".ssh", "agent.sock") {
		t.Errorf("IdentityAgent = %q", opts.IdentityAgent)
	}
	if !strings.Contains(opts.ProxyCommand, "%h %p") {
		t.Errorf("ProxyCommand = %q, want raw template with tokens", opts.ProxyCommand)
	}
}

func TestResolveUserAtAlias(t *testing.T) {
	home, path := writeConfig(t)
	t.Setenv("HOME", home)

	opts, err := resolveTest(t, "--ssh-config", path, "someone@dev")
	if err != nil {
		t.Fatalf("resolveOptions: %v", err)
	}
	if opts.User != "someone" {
		t.Errorf("User = %q, want someone", opts.User)
	}
	if opts.Host != "devbox.internal" {
		t.Errorf("Host = %q, want devbox.internal", opts.Host)
	}
}

func TestResolveFlagOverridesConfig(t *testing.T) {
	home, path := writeConfig(t)
	t.Setenv("HOME", home)

	opts, err := resolveTest(t, "--ssh-config", path,
		"-p", "2200", "-l", "ops", "-i", "/keys/ops", "dev")
	if err != nil {
		t.Fatalf("resolveOptions: %v", err)
	}
	if opts.Port != 2200 {
		t.Errorf("Port = %d, want 2200", opts.Port)
	}
	if opts.User != "ops" {
		t.Errorf("User = %q, want ops", opts.User)
	}
	if opts.IdentityFile != "/keys/ops" {
		t.Errorf("IdentityFile = %q, want /keys/ops", opts.IdentityFile)
	}
	if len(opts.IdentityFiles) != 0 {
		t.Errorf("IdentityFiles = %v, want none when -i wins", opts.IdentityFiles)
	}
	if opts.IdentityAgent != "" {
		t.Errorf("IdentityAgent = %q, want empty when -i wins", opts.IdentityAgent)
	}
	if opts.Host != "devbox.internal" {
		t.Errorf("Host = %q, want config HostName", opts.Host)
	}
}

func TestResolveNoSSHConfig(t *testing.T) {
	home, path := writeConfig(t)
	t.Setenv("HOME", home)

	opts, err := resolveTest(t, "--ssh-config", path, "--no-ssh-config", "dev")
	if err != nil {
		t.Fatalf("resolveOptions: %v", err)
	}
	if opts.Host != "dev" {
		t.Errorf("Host = %q, want the literal alias dev", opts.Host)
	}
	if opts.Port != 22 {
		t.Errorf("Port = %d, want 22", opts.Port)
	}
	if opts.User != "" {
		t.Errorf("User = %q, want empty (client fills the default)", opts.User)
	}
	if opts.ProxyCommand != "" || opts.Alias != "" || len(opts.IdentityFiles) != 0 {
		t.Errorf("config leaked through --no-ssh-config: %+v", opts)
	}
}

func TestResolveMissingExplicitConfig(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "nope")
	_, err := resolveTest(t, "--ssh-config", missing, "dev")
	if err == nil {
		t.Fatal("resolveOptions succeeded, want an error for a missing explicit config")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("error = %q, want it to name the missing file", err)
	}
}

func TestResolveUnknownAliasIsLiteralHost(t *testing.T) {
	home, path := writeConfig(t)
	t.Setenv("HOME", home)

	opts, err := resolveTest(t, "--ssh-config", path, "other.example.com")
	if err != nil {
		t.Fatalf("resolveOptions: %v", err)
	}
	if opts.Host != "other.example.com" {
		t.Errorf("Host = %q, want the literal host", opts.Host)
	}
	if opts.Port != 22 {
		t.Errorf("Port = %d, want 22", opts.Port)
	}
}

func TestResolveDefaultPortWithoutConfigFile(t *testing.T) {
	// An absent ~/.ssh/config is not an error: the alias is literal.
	t.Setenv("HOME", t.TempDir())
	opts, err := resolveTest(t, "host.example.com")
	if err != nil {
		t.Fatalf("resolveOptions: %v", err)
	}
	if opts.Port != 22 || opts.Host != "host.example.com" {
		t.Errorf("opts = %+v, want host.example.com:22", opts)
	}
	if opts.RemoteBin != "tetherd" {
		t.Errorf("RemoteBin = %q, want tetherd (auto) after normalisation", opts.RemoteBin)
	}
}

func TestResolveConflictingUser(t *testing.T) {
	home, path := writeConfig(t)
	t.Setenv("HOME", home)

	if _, err := resolveTest(t, "--ssh-config", path, "-l", "a", "b@dev"); err == nil {
		t.Fatal("resolveOptions accepted conflicting users")
	}
}

func TestResolveInvalidPort(t *testing.T) {
	if _, err := resolveTest(t, "-p", "70000", "host"); err == nil {
		t.Fatal("resolveOptions accepted port 70000")
	}
}
