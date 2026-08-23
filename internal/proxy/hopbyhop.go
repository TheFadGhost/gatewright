package proxy

import (
	"net/http"
	"strings"
)

// hopByHopNames are the connection-scoped header fields stripped on both the
// request leg and the response leg of the proxy (RFC 7230 section 6.1).
// "Connection" itself plus every field named within it are removed by
// StripHopByHop; "Upgrade"/"Connection" are additionally preserved together
// for protocol-upgrade requests (see StripHopByHop).
var hopByHopNames = [...]string{
	"Connection",
	"Proxy-Connection",
	"Keep-Alive",
	"TE",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
	"Proxy-Authorization",
	"Proxy-Authenticate",
}

// IsUpgradeRequested reports whether h constitutes an HTTP/1.1 protocol
// upgrade: a Connection header containing the "Upgrade" token together with
// a non-empty Upgrade header (RFC 7230 section 6.7).
func IsUpgradeRequested(h http.Header) bool {
	if h.Get("Upgrade") == "" {
		return false
	}
	for _, v := range h.Values("Connection") {
		for _, tok := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(tok), "Upgrade") {
				return true
			}
		}
	}
	return false
}

// StripHopByHop removes all hop-by-hop headers from h in place: the
// Connection header itself, every header named inside Connection, and the
// fixed connection-scoped set (Proxy-Connection, Keep-Alive, TE, Trailer,
// Transfer-Encoding, Upgrade, Proxy-Authorization, Proxy-Authenticate).
//
// TE is kept only when every one of its values is exactly "trailers"
// (RFC 7230 section 3.3.1: a sender MAY keep TE: trailers to advertise
// trailer support; any other TE value is hop-by-hop negotiation).
//
// When keepUpgradePair is true an HTTP/1.1 protocol upgrade is in flight;
// Connection and Upgrade are then preserved as a pair so the reverse-proxy
// layer can detect and tunnel the upgraded connection. Every other
// Connection-named header is still stripped.
func StripHopByHop(h http.Header, keepUpgradePair bool) {
	for _, name := range connectionNamed(h) {
		if keepUpgradePair && strings.EqualFold(name, "Upgrade") {
			continue
		}
		h.Del(name)
	}
	for _, name := range hopByHopNames {
		switch name {
		case "TE":
			if teTrailersOnly(h) {
				continue
			}
		case "Connection", "Upgrade":
			if keepUpgradePair {
				continue
			}
		}
		h.Del(name)
	}
}

// connectionNamed returns the header names listed inside Connection values.
func connectionNamed(h http.Header) []string {
	var names []string
	for _, v := range h.Values("Connection") {
		for _, tok := range strings.Split(v, ",") {
			tok = strings.TrimSpace(tok)
			if tok != "" {
				names = append(names, tok)
			}
		}
	}
	return names
}

// teTrailersOnly reports whether the TE header exists and every value is
// exactly "trailers".
func teTrailersOnly(h http.Header) bool {
	vals := h.Values("TE")
	if len(vals) == 0 {
		return false
	}
	for _, v := range vals {
		if !strings.EqualFold(strings.TrimSpace(v), "trailers") {
			return false
		}
	}
	return true
}
