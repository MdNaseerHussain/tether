package client

import (
	"context"
	"fmt"
	"os"

	"github.com/MdNaseerHussain/tether/internal/bootstrap"
	"github.com/MdNaseerHussain/tether/internal/version"
	"golang.org/x/crypto/ssh"
)

// ensure resolves the remote tetherd for one connection. The first successful
// probe or install is cached for the client lifetime so a reconnect does not
// upload again; see bootstrapState.
func (b *bootstrapState) ensure(ctx context.Context, client *ssh.Client, opts Options) (string, error) {
	if path := b.resolved(); path != "" {
		return path, nil
	}

	remote, err := bootstrap.NewSSHRemote(client)
	if err != nil {
		return "", err
	}
	defer bootstrap.CloseRemote(remote)

	localBin, err := os.Executable()
	if err != nil {
		localBin = ""
	}
	res, err := bootstrap.Ensure(ctx, remote, bootstrap.Options{
		Version:         version.Version,
		ProtocolVersion: version.ProtocolVersion,
		RemoteBin:       opts.RemoteBin,
		AssetsDir:       opts.AssetsDir,
		LocalBin:        localBin,
	})
	if err != nil {
		return "", err
	}
	if b != nil {
		b.path = res.Path
	}
	if res.Installed {
		b.notice(opts, res)
	}
	return res.Path, nil
}

// notice reports a one-time install on stderr. It never writes to stdout, which
// carries the session bytes, and it stays quiet when stderr is redirected so
// scripts see only real diagnostics.
func (b *bootstrapState) notice(opts Options, res bootstrap.Result) {
	if !isTTY(opts.Stderr) && !opts.Verbose {
		return
	}
	line := fmt.Sprintf("tether: installed tetherd %s (%s/%s) on %s",
		res.Version, res.OS, res.Arch, opts.Host)
	if isTTY(opts.Stderr) {
		fmt.Fprintf(opts.Stderr, "\r\n%s\r\n", line)
		return
	}
	fmt.Fprintln(opts.Stderr, line)
}
