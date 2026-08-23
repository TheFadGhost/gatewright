package middleware

import (
	"net/http"
	"time"

	"gatewright/internal/config"
)

// NewRequestHeaders applies inbound request-header manipulation: Set
// overwrites any existing values, Add appends, Del removes all values.
func NewRequestHeaders(m config.HeaderManip) Middleware {
	if len(m.Set) == 0 && len(m.Add) == 0 && len(m.Del) == 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			for name, val := range m.Set {
				r.Header.Set(name, val)
			}
			for name, val := range m.Add {
				r.Header.Add(name, val)
			}
			for _, name := range m.Del {
				r.Header.Del(name)
			}
			next.ServeHTTP(w, r)
			RecordStage(r.Context(), OrderNames[PosRequestHeaders], time.Since(start))
		})
	}
}
