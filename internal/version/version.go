// Package version records the build identity of the tether binaries.
package version

// Version is the human-readable release identifier. It is overridden at build
// time with:
//
//	-ldflags "-X github.com/MdNaseerHussain/tether/internal/version.Version=1.2.3"
var Version = "0.1.0-dev"

// ProtocolVersion is the wire protocol revision implemented by this build.
// The daemon rejects hello frames whose version does not match exactly.
const ProtocolVersion = 1
