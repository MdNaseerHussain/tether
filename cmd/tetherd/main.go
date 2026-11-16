// Command tetherd is the remote half of tether. It runs one supervisor process
// per session (serve), bridges a channel to an existing session (attach), and
// enumerates or terminates sessions (list, kill).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/naseer-hussain/tether/internal/daemon"
	"github.com/naseer-hussain/tether/internal/version"
)

const usage = `usage: tetherd <command> [flags]

commands:
  serve    run a session supervisor in the foreground
  attach   bridge stdin/stdout to a session socket
  list     list sessions
  kill     terminate a session
  version  print version information
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "serve":
		err = runServe(os.Args[2:])
	case "attach":
		err = runAttach(os.Args[2:])
	case "list":
		err = runList(os.Args[2:])
	case "kill":
		err = runKill(os.Args[2:])
	case "version":
		err = runVersion(os.Args[2:])
	case "-h", "--help", "help":
		fmt.Fprint(os.Stdout, usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "tetherd: unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}

	if err != nil {
		if errors.Is(err, flag.ErrHelp) || errors.Is(err, context.Canceled) {
			return
		}
		fmt.Fprintf(os.Stderr, "tetherd: %v\n", err)
		os.Exit(1)
	}
}

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	home := fs.String("home", "", "tether home directory (default $TETHER_HOME or ~/.tether)")
	session := fs.String("session", "", `session id (default "default")`)
	socket := fs.String("socket", "", "unix socket path (default <home>/sessions/<id>.sock)")
	shell := fs.String("shell", "", "shell to run (default $TETHER_DEFAULT_SHELL or $SHELL)")
	bufferSize := fs.Int("buffer-size", 0, "ring buffer capacity in bytes")
	rows := fs.Int("rows", 0, "initial terminal rows")
	cols := fs.Int("cols", 0, "initial terminal columns")
	logPath := fs.String("log", "", "supervisor log path (default <home>/sessions/<id>.log)")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: tetherd serve [flags]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := daemon.DefaultConfig()
	if *home != "" {
		cfg.Home = *home
	}
	if *session != "" {
		cfg.SessionID = *session
	}
	if *socket != "" {
		cfg.SocketPath = *socket
	} else {
		cfg.SocketPath = daemon.SocketPath(cfg.Home, cfg.SessionID)
	}
	if *logPath != "" {
		cfg.LogPath = *logPath
	} else {
		cfg.LogPath = daemon.LogPath(cfg.Home, cfg.SessionID)
	}
	if *shell != "" {
		cfg.Shell = *shell
	}
	if *bufferSize > 0 {
		cfg.BufferSize = *bufferSize
	}
	if *rows > 0 {
		cfg.Rows = uint16(*rows)
	}
	if *cols > 0 {
		cfg.Cols = uint16(*cols)
	}

	s, err := daemon.New(cfg)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(sigc)
	go func() {
		select {
		case <-sigc:
			// Serve closes the session and removes the socket on cancellation.
			cancel()
		case <-ctx.Done():
		}
	}()

	err = s.Serve(ctx)
	if closeErr := s.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	return err
}

func runAttach(args []string) error {
	fs := flag.NewFlagSet("attach", flag.ContinueOnError)
	home := fs.String("home", "", "tether home directory (default $TETHER_HOME or ~/.tether)")
	session := fs.String("session", "", `session id (default "default")`)
	socket := fs.String("socket", "", "unix socket path (default <home>/sessions/<id>.sock)")
	spawn := fs.Bool("spawn", false, "start a detached session if none is listening")
	shell := fs.String("shell", "", "shell for a spawned session (default $SHELL)")
	bufferSize := fs.Int("buffer-size", 0, "ring buffer capacity for a spawned session")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: tetherd attach [flags]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := daemon.DefaultConfig()
	if *home != "" {
		cfg.Home = *home
	}
	if *session != "" {
		cfg.SessionID = *session
	}
	if *socket != "" {
		cfg.SocketPath = *socket
	} else {
		cfg.SocketPath = daemon.SocketPath(cfg.Home, cfg.SessionID)
	}
	if *shell != "" {
		cfg.Shell = *shell
	}
	if *bufferSize > 0 {
		cfg.BufferSize = *bufferSize
	}
	if err := cfg.Normalize(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(sigc)
	go func() {
		select {
		case <-sigc:
			cancel()
		case <-ctx.Done():
		}
	}()

	if *spawn {
		if err := daemon.EnsureSession(ctx, cfg, 5*time.Second); err != nil {
			return err
		}
	}
	return daemon.Bridge(ctx, cfg.SocketPath, os.Stdin, os.Stdout)
}

func runList(args []string) error {
	fs := flag.NewFlagSet("list", flag.ContinueOnError)
	home := fs.String("home", "", "tether home directory (default $TETHER_HOME or ~/.tether)")
	asJSON := fs.Bool("json", false, "emit JSON instead of a table")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: tetherd list [flags]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	infos, err := daemon.List(*home)
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(infos)
	}
	if len(infos) == 0 {
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
	fmt.Fprintln(w, "SESSION\tPID\tSTATUS\tATTACHED\tBUFFER\tSTARTED")
	for _, info := range infos {
		status := "dead"
		if info.Alive {
			status = "alive"
		}
		started := "unknown"
		if !info.Started.IsZero() {
			started = info.Started.Local().Format(time.RFC3339)
		}
		fmt.Fprintf(w, "%s\t%d\t%s\t%d\t%d\t%s\n",
			info.ID, info.PID, status, info.Attached, info.BufferSize, started)
	}
	return w.Flush()
}

func runKill(args []string) error {
	fs := flag.NewFlagSet("kill", flag.ContinueOnError)
	home := fs.String("home", "", "tether home directory (default $TETHER_HOME or ~/.tether)")
	session := fs.String("session", "", `session id (default "default")`)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: tetherd kill [flags]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := daemon.DefaultConfig()
	if *home != "" {
		cfg.Home = *home
	}
	if *session != "" {
		cfg.SessionID = *session
	}
	return daemon.Kill(cfg.Home, cfg.SessionID)
}

func runVersion(args []string) error {
	fs := flag.NewFlagSet("version", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	fmt.Printf("tetherd %s (protocol %d)\n", version.Version, version.ProtocolVersion)
	return nil
}
