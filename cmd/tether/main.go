// Command tether is the local client for the tether persistent terminal proxy.
// It puts the terminal into raw mode, dials the remote host over SSH, runs
// `tetherd attach --spawn`, and speaks the framed protocol from internal/proto
// through that byte stream.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/MdNaseerHussain/tether/internal/client"
	"github.com/MdNaseerHussain/tether/internal/sshconfig"
	"github.com/MdNaseerHussain/tether/internal/version"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// cliFlags holds the parsed flag values. Zero values are "not set" for the
// options that ssh_config may supply (port, user, identity); resolveOptions
// decides the effective value.
type cliFlags struct {
	identity       string
	port           int
	user           string
	session        string
	remoteBin      string
	bufferSize     int
	escape         string
	knownHosts     string
	insecure       bool
	connectTimeout time.Duration
	verbose        bool
	showVersion    bool
	sshConfig      string
	noSSHConfig    bool
	noBootstrap    bool
	assetsDir      string
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "version" {
		printVersion(stdout)
		return 0
	}

	fs, set, flags, err := newFlagSet(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			usage(stdout)
			return 0
		}
		fmt.Fprintf(stderr, "tether: %v\n\n", err)
		usage(stderr)
		return 2
	}

	if flags.showVersion {
		printVersion(stdout)
		return 0
	}

	opts, err := resolveOptions(flags, fs, set)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	opts.Stdin = os.Stdin
	opts.Stdout = stdout
	opts.Stderr = stderr

	switch {
	case opts.Session == "":
		fmt.Fprintln(stderr, "tether: --session must not be empty")
		return 2
	case opts.BufferSize < 0:
		fmt.Fprintf(stderr, "tether: invalid --buffer-size %d\n", opts.BufferSize)
		return 2
	case opts.ConnectTimeout <= 0:
		fmt.Fprintf(stderr, "tether: invalid --connect-timeout %s\n", opts.ConnectTimeout)
		return 2
	}

	err = client.Run(context.Background(), opts)
	if err == nil {
		return 0
	}

	var exitErr *client.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}

	// Run's errors are already user-facing and prefixed with "tether: ".
	fmt.Fprintln(stderr, err)
	return 1
}

// newFlagSet registers every flag and reports which ones the user actually
// passed. resolveOptions needs that set to let explicit flags win over
// ssh_config without guessing from default values.
func newFlagSet(args []string) (*flag.FlagSet, map[string]bool, *cliFlags, error) {
	f := &cliFlags{}

	fs := flag.NewFlagSet("tether", flag.ContinueOnError)
	fs.SetOutput(io.Discard) // usage is printed by the caller, not by flag
	fs.Usage = func() {}

	fs.StringVar(&f.identity, "identity", "", "private key file")
	fs.StringVar(&f.identity, "i", "", "private key file")
	fs.IntVar(&f.port, "port", 0, "SSH port (default: ssh_config, then 22)")
	fs.IntVar(&f.port, "p", 0, "SSH port (default: ssh_config, then 22)")
	fs.StringVar(&f.user, "user", "", "SSH user")
	fs.StringVar(&f.user, "l", "", "SSH user")
	fs.StringVar(&f.session, "session", "default", "session id")
	fs.StringVar(&f.remoteBin, "remote-bin", "auto", `remote tetherd binary ("auto" bootstraps when missing)`)
	fs.IntVar(&f.bufferSize, "buffer-size", 0, "ring buffer bytes for a new session")
	fs.StringVar(&f.escape, "escape", "~.", "detach sequence")
	fs.StringVar(&f.knownHosts, "known-hosts", "~/.ssh/known_hosts", "known_hosts file")
	fs.BoolVar(&f.insecure, "insecure", false, "skip host key verification")
	fs.DurationVar(&f.connectTimeout, "connect-timeout", 10*time.Second, "SSH dial timeout")
	fs.BoolVar(&f.verbose, "verbose", false, "verbose diagnostics")
	fs.BoolVar(&f.verbose, "v", false, "verbose diagnostics")
	fs.BoolVar(&f.showVersion, "version", false, "print version")
	fs.StringVar(&f.sshConfig, "ssh-config", sshconfig.DefaultPath(), "ssh_config file (empty disables)")
	fs.BoolVar(&f.noSSHConfig, "no-ssh-config", false, "ignore ~/.ssh/config")
	fs.BoolVar(&f.noBootstrap, "no-bootstrap", false, "never upload tetherd to the remote host")
	fs.StringVar(&f.assetsDir, "assets-dir", "", "local directory with tetherd-<os>-<arch> assets")

	if err := fs.Parse(args); err != nil {
		return nil, nil, nil, err
	}

	set := make(map[string]bool)
	fs.Visit(func(fl *flag.Flag) { set[fl.Name] = true })
	return fs, set, f, nil
}

// resolveOptions turns the positional [user@]alias plus the parsed flags into
// client options, applying ssh_config between the two: explicit flags win, then
// ssh_config, then built-in defaults.
func resolveOptions(f *cliFlags, fs *flag.FlagSet, set map[string]bool) (client.Options, error) {
	rest := fs.Args()
	switch len(rest) {
	case 0:
		return client.Options{}, errors.New("tether: missing host")
	case 1:
	default:
		return client.Options{}, fmt.Errorf("tether: unexpected extra arguments: %s", strings.Join(rest[1:], " "))
	}

	target := rest[0]
	hostUser := ""
	if i := strings.LastIndex(target, "@"); i >= 0 {
		hostUser = target[:i]
		target = target[i+1:]
	}
	if target == "" {
		return client.Options{}, errors.New("tether: missing host")
	}
	if hostUser != "" && f.user != "" && hostUser != f.user {
		return client.Options{}, fmt.Errorf("tether: conflicting user: %q in %q and --user %q", hostUser, rest[0], f.user)
	}

	user := hostUser
	if user == "" {
		user = f.user
	}

	var cfgHost sshconfig.Host
	useConfig := !f.noSSHConfig && f.sshConfig != ""
	if useConfig {
		if set["ssh-config"] {
			if _, err := os.Stat(f.sshConfig); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return client.Options{}, fmt.Errorf("tether: ssh config %s does not exist", f.sshConfig)
				}
				return client.Options{}, fmt.Errorf("tether: cannot read ssh config %s: %w", f.sshConfig, err)
			}
		}
		resolver, err := sshconfig.Load(f.sshConfig)
		if err != nil {
			return client.Options{}, fmt.Errorf("tether: %w", err)
		}
		cfgHost, err = resolver.Lookup(target)
		if err != nil {
			return client.Options{}, fmt.Errorf("tether: %w", err)
		}
	}

	host := target
	if useConfig && cfgHost.HostName != "" {
		host = cfgHost.HostName
	}

	port := 22
	if useConfig {
		port = cfgHost.EffectivePort()
	}
	if set["port"] || set["p"] {
		port = f.port
	}
	if port < 1 || port > 65535 {
		return client.Options{}, fmt.Errorf("tether: invalid port %d", port)
	}

	if user == "" && useConfig {
		user = cfgHost.User
	}

	var (
		identityFile  string
		identityFiles []string
		identityAgent string
		alias         string
		proxyCommand  string
	)
	if set["identity"] || set["i"] {
		identityFile = f.identity
	} else if useConfig {
		identityFiles = cfgHost.IdentityFiles
		identityAgent = cfgHost.IdentityAgent
	}
	if useConfig {
		alias = cfgHost.Alias
		proxyCommand = cfgHost.ProxyCommand
	}

	remoteBin := f.remoteBin
	if remoteBin == "" || remoteBin == "auto" {
		remoteBin = "tetherd"
	}

	return client.Options{
		User:           user,
		Host:           host,
		Port:           port,
		IdentityFile:   identityFile,
		IdentityFiles:  identityFiles,
		IdentityAgent:  identityAgent,
		ProxyCommand:   proxyCommand,
		Alias:          alias,
		KnownHostsFile: f.knownHosts,
		Insecure:       f.insecure,
		Session:        f.session,
		RemoteBin:      remoteBin,
		BufferSize:     f.bufferSize,
		Escape:         f.escape,
		ConnectTimeout: f.connectTimeout,
		Verbose:        f.verbose,
		NoBootstrap:    f.noBootstrap,
		AssetsDir:      f.assetsDir,
	}, nil
}

func printVersion(w io.Writer) {
	fmt.Fprintf(w, "tether %s (protocol %d)\n", version.Version, version.ProtocolVersion)
}

func usage(w io.Writer) {
	fmt.Fprint(w, `usage: tether [flags] [user@]alias
       tether version

Attach to (or create) a persistent remote shell session over SSH. The alias is
resolved through ~/.ssh/config exactly like ssh(1); a literal host works too.

Flags:
  -i, --identity PATH      private key file (default: ssh_config, then
                           ssh-agent and ~/.ssh/id_*)
  -p, --port PORT          SSH port (default: ssh_config, then 22)
  -l, --user USER          SSH user (default: ssh_config, then current user)
      --ssh-config PATH    ssh_config file (default ~/.ssh/config; "" disables)
      --no-ssh-config      ignore ~/.ssh/config
      --session ID         session to attach to (default "default")
      --remote-bin PATH    remote tetherd binary ("auto" installs when missing;
                           a path containing "/" is probed only)
      --no-bootstrap       never upload tetherd to the remote host
      --assets-dir DIR     local directory holding tetherd-<os>-<arch> assets
      --buffer-size N      ring buffer bytes for a new session (default: daemon 4 MiB)
      --escape SEQ         detach sequence after Enter (default "~."; "none" disables)
      --known-hosts PATH   known_hosts file (default ~/.ssh/known_hosts)
      --insecure           skip host key verification (insecure)
      --connect-timeout D  SSH dial timeout (default 10s)
  -v, --verbose            verbose diagnostics on stderr
      --version            print version and exit
`)
}
