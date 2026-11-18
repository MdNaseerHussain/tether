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
	"github.com/MdNaseerHussain/tether/internal/version"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "version" {
		printVersion(stdout)
		return 0
	}

	var (
		identity       string
		port           int
		userFlag       string
		session        string
		remoteBin      string
		bufferSize     int
		escape         string
		knownHosts     string
		insecure       bool
		connectTimeout time.Duration
		verbose        bool
		showVersion    bool
	)

	fs := flag.NewFlagSet("tether", flag.ContinueOnError)
	fs.SetOutput(io.Discard) // usage is printed by the caller, not by flag
	fs.Usage = func() {}

	fs.StringVar(&identity, "identity", "", "private key file")
	fs.StringVar(&identity, "i", "", "private key file")
	fs.IntVar(&port, "port", 22, "SSH port")
	fs.IntVar(&port, "p", 22, "SSH port")
	fs.StringVar(&userFlag, "user", "", "SSH user")
	fs.StringVar(&userFlag, "l", "", "SSH user")
	fs.StringVar(&session, "session", "default", "session id")
	fs.StringVar(&remoteBin, "remote-bin", "tetherd", "remote tetherd binary")
	fs.IntVar(&bufferSize, "buffer-size", 0, "ring buffer bytes for a new session")
	fs.StringVar(&escape, "escape", "~.", "detach sequence")
	fs.StringVar(&knownHosts, "known-hosts", "~/.ssh/known_hosts", "known_hosts file")
	fs.BoolVar(&insecure, "insecure", false, "skip host key verification")
	fs.DurationVar(&connectTimeout, "connect-timeout", 10*time.Second, "SSH dial timeout")
	fs.BoolVar(&verbose, "verbose", false, "verbose diagnostics")
	fs.BoolVar(&verbose, "v", false, "verbose diagnostics")
	fs.BoolVar(&showVersion, "version", false, "print version")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			usage(stdout)
			return 0
		}
		fmt.Fprintf(stderr, "tether: %v\n\n", err)
		usage(stderr)
		return 2
	}

	if showVersion {
		printVersion(stdout)
		return 0
	}

	rest := fs.Args()
	switch len(rest) {
	case 0:
		fmt.Fprintln(stderr, "tether: missing host")
		usage(stderr)
		return 2
	case 1:
	default:
		fmt.Fprintf(stderr, "tether: unexpected extra arguments: %s\n", strings.Join(rest[1:], " "))
		usage(stderr)
		return 2
	}

	target := rest[0]
	hostUser := ""
	if i := strings.LastIndex(target, "@"); i >= 0 {
		hostUser = target[:i]
		target = target[i+1:]
	}
	if target == "" {
		fmt.Fprintln(stderr, "tether: missing host")
		usage(stderr)
		return 2
	}
	if hostUser != "" && userFlag != "" && hostUser != userFlag {
		fmt.Fprintf(stderr, "tether: conflicting user: %q in %q and --user %q\n", hostUser, rest[0], userFlag)
		return 2
	}
	if hostUser == "" {
		hostUser = userFlag
	}

	switch {
	case port < 1 || port > 65535:
		fmt.Fprintf(stderr, "tether: invalid port %d\n", port)
		return 2
	case bufferSize < 0:
		fmt.Fprintf(stderr, "tether: invalid --buffer-size %d\n", bufferSize)
		return 2
	case session == "":
		fmt.Fprintln(stderr, "tether: --session must not be empty")
		return 2
	case connectTimeout <= 0:
		fmt.Fprintf(stderr, "tether: invalid --connect-timeout %s\n", connectTimeout)
		return 2
	}

	opts := client.Options{
		User:           hostUser,
		Host:           target,
		Port:           port,
		IdentityFile:   identity,
		KnownHostsFile: knownHosts,
		Insecure:       insecure,
		Session:        session,
		RemoteBin:      remoteBin,
		BufferSize:     bufferSize,
		Escape:         escape,
		ConnectTimeout: connectTimeout,
		Verbose:        verbose,
		Stdin:          os.Stdin,
		Stdout:         stdout,
		Stderr:         stderr,
	}

	err := client.Run(context.Background(), opts)
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

func printVersion(w io.Writer) {
	fmt.Fprintf(w, "tether %s (protocol %d)\n", version.Version, version.ProtocolVersion)
}

func usage(w io.Writer) {
	fmt.Fprint(w, `usage: tether [flags] [user@]host
       tether version

Attach to (or create) a persistent remote shell session over SSH.

Flags:
  -i, --identity PATH      private key file (default: ssh-agent, then
                           ~/.ssh/id_ed25519, ~/.ssh/id_rsa, ~/.ssh/id_ecdsa)
  -p, --port PORT          SSH port (default 22)
  -l, --user USER          SSH user (default: current user)
      --session ID         session to attach to (default "default")
      --remote-bin PATH    remote tetherd binary (default "tetherd")
      --buffer-size N      ring buffer bytes for a new session (default: daemon 4 MiB)
      --escape SEQ         detach sequence after Enter (default "~."; "none" disables)
      --known-hosts PATH   known_hosts file (default ~/.ssh/known_hosts)
      --insecure           skip host key verification (insecure)
      --connect-timeout D  SSH dial timeout (default 10s)
  -v, --verbose            verbose diagnostics on stderr
      --version            print version and exit
`)
}
