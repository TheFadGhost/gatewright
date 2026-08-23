package middleware

import (
	"errors"
	"net"
	"net/http"
	"strings"

	"gatewright/internal/config"
)

// KeyExtractor derives the rate-limiter bucket key from the request and (when
// auth ran) its identity.
type KeyExtractor func(r *http.Request, id *Identity) string

// keySep separates composite parts; NUL cannot occur in URL paths or valid
// header values, so the encoding is injective.
const keySep = "\x00"

// BuildKeyExtractor compiles a validated KeySpec into a key function. Parts
// are rendered in declared order as "kind\x00value", joined by \x00 — stable,
// collision-safe, and greppable in logs/metrics labels.
func BuildKeyExtractor(spec config.KeySpec) (KeyExtractor, error) {
	if len(spec.Parts) == 0 {
		return nil, errors.New("middleware: key selector has no parts")
	}
	parts := make([]config.KeyPart, len(spec.Parts))
	copy(parts, spec.Parts)
	return func(r *http.Request, id *Identity) string {
		var b strings.Builder
		for i, p := range parts {
			if i > 0 {
				b.WriteString(keySep)
			}
			b.WriteString(p.Kind)
			b.WriteString(keySep)
			b.WriteString(extractKeyPart(r, id, p))
		}
		return b.String()
	}, nil
}

func extractKeyPart(r *http.Request, id *Identity, p config.KeyPart) string {
	switch p.Kind {
	case "ip":
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			return r.RemoteAddr // no port (e.g. unix socket): use verbatim
		}
		return host
	case "path":
		return r.URL.Path
	case "api_key":
		if id != nil {
			return id.APIKey
		}
		return ""
	case "header":
		return r.Header.Get(p.Header)
	default:
		return ""
	}
}
