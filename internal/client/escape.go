package client

import (
	"bytes"
	"fmt"
)

// Detacher recognises the local detach sequence while forwarding terminal input.
//
// Like OpenSSH's escape character, the sequence is only honoured at the start of
// a line: a byte-for-byte match is looked for immediately after an Enter (\r or
// \n) has been forwarded. Every other byte passes through untouched, so ordinary
// typing (including a leading "^") never needs escaping. On a partial match the
// buffered prefix is held until the next byte decides whether the sequence
// matched; a mismatch flushes the prefix as normal input.
//
// Filter is not safe for concurrent use: exactly one goroutine owns it.
type Detacher struct {
	spec     []byte
	buf      []byte
	armed    bool
	disabled bool
}

// NewDetacher returns a Detacher for spec, a 1-8 byte sequence such as "~.".
// The literal "none" (or "off") disables detection. An empty or oversized spec
// is an error.
func NewDetacher(spec string) (*Detacher, error) {
	if spec == "none" || spec == "off" {
		return &Detacher{disabled: true}, nil
	}
	n := len(spec)
	if n == 0 {
		return nil, fmt.Errorf(`tether: invalid escape sequence ""; use 1-8 bytes or "none"`)
	}
	if n > 8 {
		return nil, fmt.Errorf("tether: invalid escape sequence %q: longer than 8 bytes", spec)
	}
	return &Detacher{spec: []byte(spec)}, nil
}

// Filter feeds p through the detach parser. It returns the bytes that must be
// forwarded to the session and whether the detach sequence was completed. When
// detach is true the matched bytes are swallowed and any bytes after the match
// in p are dropped, because the connection is about to end.
func (d *Detacher) Filter(p []byte) (out []byte, detach bool) {
	if d.disabled || len(p) == 0 {
		return p, false
	}

	out = make([]byte, 0, len(p)+len(d.buf))
	for _, b := range p {
		if !d.armed {
			out = append(out, b)
			if b == '\r' || b == '\n' {
				d.armed = true
				d.buf = d.buf[:0]
			}
			continue
		}

		if b == d.spec[len(d.buf)] {
			d.buf = append(d.buf, b)
			if len(d.buf) == len(d.spec) {
				d.buf = d.buf[:0]
				d.armed = false
				return out, true
			}
			continue
		}

		// Mismatch: the held prefix plus b is ordinary input after all. Enter
		// still starts a fresh line, so it re-arms the parser.
		out = append(out, d.buf...)
		out = append(out, b)
		d.buf = d.buf[:0]
		d.armed = b == '\r' || b == '\n'
	}
	return out, false
}

// Spec returns the configured sequence, or nil when detection is disabled.
func (d *Detacher) Spec() []byte { return bytes.Clone(d.spec) }
