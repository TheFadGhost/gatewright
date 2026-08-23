package admin

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"

	"gatewright/internal/errs"
)

const bearerPrefix = "Bearer "

// withAuth gates every admin endpoint. When no token is configured the caller
// has guaranteed a loopback-only bind and requests pass through untouched;
// when a token is configured it is always required.
func (s *Server) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.opts.AuthToken == "" {
			next.ServeHTTP(w, r)
			return
		}
		if !bearerMatches(r.Header.Get("Authorization"), s.opts.AuthToken) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="gatewright-admin"`)
			errs.Write(w, errs.New(errs.CodeUnauthorized, "missing or invalid bearer token"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// bearerMatches compares the presented bearer token in constant time. Both
// sides are hashed first so comparison cost does not leak length differences.
func bearerMatches(header, want string) bool {
	if len(header) <= len(bearerPrefix) || !strings.EqualFold(header[:len(bearerPrefix)], bearerPrefix) {
		return false
	}
	got := sha256.Sum256([]byte(header[len(bearerPrefix):]))
	expected := sha256.Sum256([]byte(want))
	return subtle.ConstantTimeCompare(got[:], expected[:]) == 1
}
