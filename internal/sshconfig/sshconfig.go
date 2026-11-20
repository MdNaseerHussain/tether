// Package sshconfig resolves OpenSSH configuration aliases so that
// `tether <alias>` behaves like `ssh <alias>`.
//
// It is a thin, deliberately literal layer over github.com/kevinburke/ssh_config:
// no ProxyCommand execution, no authentication and no host-key policy live here.
// The client receives the effective values and owns all of that.
//
// Values are returned as they appear in the file. Tilde prefixes are expanded
// for paths because callers pass them to the filesystem; environment variables
// ("$VAR", "${VAR}") are deliberately left alone. OpenSSH expands them at
// connection time against its own environment, and expanding them here would
// silently bake values from the tether process into the configuration.
package sshconfig

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kevinburke/ssh_config"
)

// defaultPort is the port SSH uses when the configuration does not set one.
const defaultPort = 22

// Host is the effective configuration for one alias.
type Host struct {
	Alias string
	// HostName is the literal HostName value, or Alias when the option is
	// unset. Tilde and token expansion are not applied.
	HostName string
	User     string
	// Port is 0 when the option is unset; see EffectivePort.
	Port int
	// IdentityFiles lists every IdentityFile for the alias in file order, with
	// duplicates removed and "~" expanded.
	IdentityFiles []string
	// IdentityAgent is "~"-expanded, except that "none" is preserved verbatim
	// so callers can distinguish it from an unset value ("").
	IdentityAgent string
	// ProxyCommand is the raw value with no token substitution. Pass it to
	// Substitute before executing it.
	ProxyCommand string
}

// EffectivePort returns the port to connect to: Port when set, otherwise 22.
func (h Host) EffectivePort() int {
	if h.Port == 0 {
		return defaultPort
	}
	return h.Port
}

// Resolver answers alias lookups against one parsed configuration file.
type Resolver struct {
	cfg *ssh_config.Config
}

// DefaultPath returns $HOME/.ssh/config, or "" when HOME cannot be determined.
func DefaultPath() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Join(home, ".ssh", "config")
}

// Load reads path. A missing file is not an error: the returned resolver is
// empty, so Lookup yields defaults for every alias. Other read and parse errors
// are returned.
func Load(path string) (*Resolver, error) {
	if path == "" {
		return emptyResolver()
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// OpenSSH treats a missing user config as "no options set", not as
			// a failure, and so do we.
			return emptyResolver()
		}
		return nil, fmt.Errorf("sshconfig: read %s: %w", path, err)
	}
	cfg, err := ssh_config.DecodeBytes(data)
	if err != nil {
		return nil, fmt.Errorf("sshconfig: parse %s: %w", path, err)
	}
	return &Resolver{cfg: cfg}, nil
}

// emptyResolver returns a resolver whose configuration contains no options.
func emptyResolver() (*Resolver, error) {
	cfg, err := ssh_config.DecodeBytes(nil)
	if err != nil {
		return nil, fmt.Errorf("sshconfig: empty config: %w", err)
	}
	return &Resolver{cfg: cfg}, nil
}

// Lookup resolves alias with OpenSSH semantics: the alias is matched
// case-insensitively against Host and Match blocks (and Include'd files), and
// the first value obtained from the first matching block wins, exactly as
// implemented by kevinburke/ssh_config. Alias is matched in lower case because
// OpenSSH lower-cases the hostname before matching; pattern text itself is
// matched case-sensitively by the library, so write patterns in lower case.
//
// IdentityFile is the exception to first-value-wins: as in OpenSSH the values
// from every matching block accumulate.
func (r *Resolver) Lookup(alias string) (Host, error) {
	if alias == "" {
		return Host{}, errors.New("sshconfig: empty host alias")
	}
	if r == nil || r.cfg == nil {
		return Host{}, errors.New("sshconfig: nil resolver")
	}

	key := strings.ToLower(alias)
	h := Host{Alias: alias}

	hostname, err := r.get(key, "HostName")
	if err != nil {
		return Host{}, lookupError(alias, err)
	}
	if hostname == "" {
		hostname = alias
	}
	h.HostName = hostname

	if h.User, err = r.get(key, "User"); err != nil {
		return Host{}, lookupError(alias, err)
	}

	portValue, err := r.get(key, "Port")
	if err != nil {
		return Host{}, lookupError(alias, err)
	}
	if port := strings.TrimSpace(portValue); port != "" {
		n, convErr := strconv.Atoi(port)
		if convErr != nil || n < 1 || n > 65535 {
			return Host{}, fmt.Errorf("sshconfig: host %q: invalid Port %q: want an integer in 1..65535",
				alias, portValue)
		}
		h.Port = n
	}

	identityFiles, err := r.cfg.GetAll(key, "IdentityFile")
	if err != nil {
		return Host{}, lookupError(alias, err)
	}
	seen := make(map[string]struct{}, len(identityFiles))
	for _, value := range identityFiles {
		if value == "" {
			continue
		}
		path := expandHome(value)
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		h.IdentityFiles = append(h.IdentityFiles, path)
	}

	if h.IdentityAgent, err = r.get(key, "IdentityAgent"); err != nil {
		return Host{}, lookupError(alias, err)
	}
	if h.IdentityAgent != "" && !strings.EqualFold(h.IdentityAgent, "none") {
		h.IdentityAgent = expandHome(h.IdentityAgent)
	}

	if h.ProxyCommand, err = r.get(key, "ProxyCommand"); err != nil {
		return Host{}, lookupError(alias, err)
	}

	return h, nil
}

// get is Config.Get with the key spelled out at the call site.
func (r *Resolver) get(alias, key string) (string, error) {
	return r.cfg.Get(alias, key)
}

func lookupError(alias string, err error) error {
	return fmt.Errorf("sshconfig: host %q: %w", alias, err)
}

// expandHome resolves a leading "~" or "~/" using os.UserHomeDir. A "~user"
// prefix is returned unchanged: expanding it needs the passwd database, which
// the client does not consult.
func expandHome(path string) string {
	if path != "~" && !strings.HasPrefix(path, "~/") {
		return path
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return path
	}
	if path == "~" {
		return home
	}
	return filepath.Join(home, path[2:])
}

// Substitute replaces the OpenSSH tokens the client can resolve locally:
//
//	%h  resolved hostname      %n  original alias
//	%p  port                   %r  remote user
//	%%  a literal percent
//
// Unknown tokens (%d, %C, %L, ...) and a trailing "%" are preserved verbatim so
// a ProxyCommand reaches the program that can actually interpret them.
func Substitute(template, alias, hostname, user string, port int) string {
	if !strings.ContainsRune(template, '%') {
		return template
	}
	var b strings.Builder
	b.Grow(len(template))
	for i := 0; i < len(template); i++ {
		c := template[i]
		if c != '%' || i+1 >= len(template) {
			// Plain byte, or a trailing '%' with no token to consume.
			b.WriteByte(c)
			continue
		}
		switch template[i+1] {
		case 'h':
			b.WriteString(hostname)
			i++
		case 'n':
			b.WriteString(alias)
			i++
		case 'p':
			b.WriteString(strconv.Itoa(port))
			i++
		case 'r':
			b.WriteString(user)
			i++
		case '%':
			b.WriteByte('%')
			i++
		default:
			b.WriteByte('%')
		}
	}
	return b.String()
}
