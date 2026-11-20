package sshconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testHome points HOME at a fresh directory so "~" expansion and DefaultPath
// are independent of the machine running the tests.
func testHome(t *testing.T) string {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	return home
}

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

// broadConfig exercises concrete blocks, wildcards, a "none" agent, and Host *
// defaults that are partly overridden and partly inherited.
const broadConfig = `
Host dev
	HostName dev.internal
	User alice
	Port 2222
	IdentityFile ~/.ssh/id_ed25519
	IdentityFile ~/.ssh/id_rsa
	IdentityFile ~/.ssh/id_ed25519
	IdentityAgent ~/.ssh/agent.sock

Host prod-*
	HostName prod.example.com
	User deploy
	ProxyCommand ssh -W %h:%p %r@bastion.example.com

Host *.corp
	User corpuser

Host noagent
	IdentityAgent none

Host *
	User defaultuser
	Port 2200
	IdentityAgent ~/.ssh/other.sock
	ProxyCommand ssh -W %h:%p bastion
	ServerAliveInterval 30
`

func TestLookupConcreteAlias(t *testing.T) {
	home := testHome(t)
	dir := t.TempDir()
	path := writeFile(t, dir, "config", broadConfig)

	r, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got, err := r.Lookup("dev")
	if err != nil {
		t.Fatalf("Lookup(dev): %v", err)
	}

	if got.Alias != "dev" {
		t.Errorf("Alias = %q, want %q", got.Alias, "dev")
	}
	if got.HostName != "dev.internal" {
		t.Errorf("HostName = %q, want %q", got.HostName, "dev.internal")
	}
	if got.User != "alice" {
		t.Errorf("User = %q, want %q", got.User, "alice")
	}
	if got.Port != 2222 {
		t.Errorf("Port = %d, want 2222", got.Port)
	}
	wantFiles := []string{
		filepath.Join(home, ".ssh", "id_ed25519"),
		filepath.Join(home, ".ssh", "id_rsa"),
	}
	if len(got.IdentityFiles) != len(wantFiles) {
		t.Fatalf("IdentityFiles = %q, want %q", got.IdentityFiles, wantFiles)
	}
	for i := range wantFiles {
		if got.IdentityFiles[i] != wantFiles[i] {
			t.Errorf("IdentityFiles[%d] = %q, want %q", i, got.IdentityFiles[i], wantFiles[i])
		}
	}
	if want := filepath.Join(home, ".ssh", "agent.sock"); got.IdentityAgent != want {
		t.Errorf("IdentityAgent = %q, want %q", got.IdentityAgent, want)
	}
	// ProxyCommand is only set in Host *, so the concrete block inherits it.
	if want := "ssh -W %h:%p bastion"; got.ProxyCommand != want {
		t.Errorf("ProxyCommand = %q, want %q", got.ProxyCommand, want)
	}
}

func TestLookupHostStarDefaults(t *testing.T) {
	home := testHome(t)
	dir := t.TempDir()
	path := writeFile(t, dir, "config", broadConfig)

	r, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got, err := r.Lookup("plain")
	if err != nil {
		t.Fatalf("Lookup(plain): %v", err)
	}

	// HostName is unset: the alias itself is the hostname.
	if got.HostName != "plain" {
		t.Errorf("HostName = %q, want %q", got.HostName, "plain")
	}
	// Inherited from Host *.
	if got.User != "defaultuser" {
		t.Errorf("User = %q, want %q", got.User, "defaultuser")
	}
	if got.Port != 2200 {
		t.Errorf("Port = %d, want 2200", got.Port)
	}
	if want := filepath.Join(home, ".ssh", "other.sock"); got.IdentityAgent != want {
		t.Errorf("IdentityAgent = %q, want %q", got.IdentityAgent, want)
	}
	if want := "ssh -W %h:%p bastion"; got.ProxyCommand != want {
		t.Errorf("ProxyCommand = %q, want %q", got.ProxyCommand, want)
	}
	if len(got.IdentityFiles) != 0 {
		t.Errorf("IdentityFiles = %q, want none", got.IdentityFiles)
	}
}

func TestLookupFirstValueWins(t *testing.T) {
	testHome(t)
	dir := t.TempDir()
	path := writeFile(t, dir, "config", broadConfig)

	r, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got, err := r.Lookup("dev")
	if err != nil {
		t.Fatalf("Lookup(dev): %v", err)
	}
	// Both Host dev and Host * set User, Port and IdentityAgent; the specific
	// block appears first and must win.
	if got.User != "alice" {
		t.Errorf("User = %q, want alice from Host dev", got.User)
	}
	if got.Port != 2222 {
		t.Errorf("Port = %d, want 2222 from Host dev", got.Port)
	}
	if strings.HasSuffix(got.IdentityAgent, "other.sock") {
		t.Errorf("IdentityAgent = %q, want Host dev value, not Host *", got.IdentityAgent)
	}
}

func TestLookupWildcardsAndCase(t *testing.T) {
	testHome(t)
	dir := t.TempDir()
	path := writeFile(t, dir, "config", broadConfig)

	r, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	tests := []struct {
		alias    string
		hostname string
		user     string
	}{
		// Host prod-* matches.
		{alias: "prod-1", hostname: "prod.example.com", user: "deploy"},
		// Host *.corp matches; HostName stays the alias.
		{alias: "web.corp", hostname: "web.corp", user: "corpuser"},
		// OpenSSH lower-cases the hostname before matching.
		{alias: "DEV", hostname: "dev.internal", user: "alice"},
		{alias: "Dev", hostname: "dev.internal", user: "alice"},
	}
	for _, tt := range tests {
		got, err := r.Lookup(tt.alias)
		if err != nil {
			t.Fatalf("Lookup(%s): %v", tt.alias, err)
		}
		if got.HostName != tt.hostname {
			t.Errorf("Lookup(%s).HostName = %q, want %q", tt.alias, got.HostName, tt.hostname)
		}
		if got.User != tt.user {
			t.Errorf("Lookup(%s).User = %q, want %q", tt.alias, got.User, tt.user)
		}
	}

	// Wildcard ProxyCommand keeps its tokens unresolved.
	got, err := r.Lookup("prod-1")
	if err != nil {
		t.Fatalf("Lookup(prod-1): %v", err)
	}
	if want := "ssh -W %h:%p %r@bastion.example.com"; got.ProxyCommand != want {
		t.Errorf("ProxyCommand = %q, want %q", got.ProxyCommand, want)
	}
}

func TestLookupUnsetHostNameAndPort(t *testing.T) {
	testHome(t)
	dir := t.TempDir()
	path := writeFile(t, dir, "config", "Host plainhost\n\tUser bob\n")

	r, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got, err := r.Lookup("plainhost")
	if err != nil {
		t.Fatalf("Lookup(plainhost): %v", err)
	}
	if got.HostName != "plainhost" {
		t.Errorf("HostName = %q, want the alias", got.HostName)
	}
	if got.Port != 0 {
		t.Errorf("Port = %d, want 0", got.Port)
	}
	if got.EffectivePort() != 22 {
		t.Errorf("EffectivePort() = %d, want 22", got.EffectivePort())
	}
	if got.User != "bob" {
		t.Errorf("User = %q, want bob", got.User)
	}
}

func TestLoadMissingFile(t *testing.T) {
	home := testHome(t)
	path := filepath.Join(t.TempDir(), "does-not-exist")

	r, err := Load(path)
	if err != nil {
		t.Fatalf("Load(missing) = %v, want nil error", err)
	}
	got, err := r.Lookup("ghost")
	if err != nil {
		t.Fatalf("Lookup(ghost): %v", err)
	}
	if got.Alias != "ghost" || got.HostName != "ghost" {
		t.Errorf("Host = %+v, want Alias and HostName = ghost", got)
	}
	if got.User != "" || got.IdentityAgent != "" || got.ProxyCommand != "" {
		t.Errorf("Host = %+v, want empty options", got)
	}
	if got.Port != 0 || got.EffectivePort() != 22 {
		t.Errorf("Port = %d, EffectivePort = %d, want 0 and 22", got.Port, got.EffectivePort())
	}
	if len(got.IdentityFiles) != 0 {
		t.Errorf("IdentityFiles = %q, want none", got.IdentityFiles)
	}
	if p := DefaultPath(); p != filepath.Join(home, ".ssh", "config") {
		t.Errorf("DefaultPath() = %q, want %q", p, filepath.Join(home, ".ssh", "config"))
	}
}

func TestLookupIdentityAgentNone(t *testing.T) {
	testHome(t)
	dir := t.TempDir()
	path := writeFile(t, dir, "config", broadConfig)

	r, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got, err := r.Lookup("noagent")
	if err != nil {
		t.Fatalf("Lookup(noagent): %v", err)
	}
	if got.IdentityAgent != "none" {
		t.Errorf("IdentityAgent = %q, want %q", got.IdentityAgent, "none")
	}
}

func TestSubstitute(t *testing.T) {
	tests := []struct {
		name     string
		template string
		port     int
		want     string
	}{
		{
			name:     "all known tokens",
			template: "ssh -W %h:%p %r@bastion",
			port:     2222,
			want:     "ssh -W dev.internal:2222 alice@bastion",
		},
		{
			name:     "alias token",
			template: "proxy %n",
			port:     2222,
			want:     "proxy dev",
		},
		{
			name:     "percent escape",
			template: "printf 100%%",
			port:     2222,
			want:     "printf 100%",
		},
		{
			name:     "unknown tokens preserved",
			template: "%d %C %L %z",
			port:     2222,
			want:     "%d %C %L %z",
		},
		{
			name:     "trailing percent",
			template: "echo 100%",
			port:     2222,
			want:     "echo 100%",
		},
		{
			name:     "unset port still substitutes",
			template: "%h:%p",
			port:     0,
			want:     "dev.internal:0",
		},
		{
			name:     "no tokens",
			template: "ssh bastion",
			port:     2222,
			want:     "ssh bastion",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Substitute(tt.template, "dev", "dev.internal", "alice", tt.port)
			if got != tt.want {
				t.Errorf("Substitute(%q) = %q, want %q", tt.template, got, tt.want)
			}
		})
	}
}

func TestLookupInvalidPort(t *testing.T) {
	testHome(t)
	dir := t.TempDir()
	path := writeFile(t, dir, "config", `
Host badport
	Port 22x

Host bigport
	Port 70000

Host zeroport
	Port 0
`)

	r, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	for _, alias := range []string{"badport", "bigport", "zeroport"} {
		_, err := r.Lookup(alias)
		if err == nil {
			t.Errorf("Lookup(%s) = nil error, want an invalid Port error", alias)
			continue
		}
		if !strings.Contains(err.Error(), "invalid Port") {
			t.Errorf("Lookup(%s) error = %q, want it to mention invalid Port", alias, err)
		}
	}
}

// TestLookupInclude covers the Include directive. kevinburke/ssh_config expands
// Include greedily while parsing: absolute paths are used as-is, while relative
// paths are resolved against $HOME/.ssh (not the including file's directory),
// and a glob matching nothing is silently ignored. The test therefore uses an
// absolute path inside a temp directory.
func TestLookupInclude(t *testing.T) {
	testHome(t)
	dir := t.TempDir()
	extra := writeFile(t, dir, "extra.conf", `
Host included
	HostName included.internal
	User incuser
	IdentityFile ~/.ssh/id_inc
`)

	path := writeFile(t, dir, "config", "Include "+extra+"\n\nHost after\n\tUser afteruser\n")

	r, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	got, err := r.Lookup("included")
	if err != nil {
		t.Fatalf("Lookup(included): %v", err)
	}
	if got.HostName != "included.internal" {
		t.Errorf("HostName = %q, want included.internal", got.HostName)
	}
	if got.User != "incuser" {
		t.Errorf("User = %q, want incuser", got.User)
	}

	got, err = r.Lookup("after")
	if err != nil {
		t.Fatalf("Lookup(after): %v", err)
	}
	if got.User != "afteruser" {
		t.Errorf("User = %q, want afteruser", got.User)
	}
}

func TestLookupEmptyAliasAndEmptyPath(t *testing.T) {
	testHome(t)

	r, err := Load("")
	if err != nil {
		t.Fatalf("Load(\"\"): %v", err)
	}
	if _, err := r.Lookup(""); err == nil {
		t.Error("Lookup(\"\") = nil error, want an empty-alias error")
	}

	var nilResolver *Resolver
	if _, err := nilResolver.Lookup("dev"); err == nil {
		t.Error("nil resolver Lookup = nil error, want an error")
	}
}

func TestDefaultPathWithoutHome(t *testing.T) {
	t.Setenv("HOME", "")
	if got := DefaultPath(); got != "" {
		t.Errorf("DefaultPath() = %q with HOME unset, want %q", got, "")
	}
}
