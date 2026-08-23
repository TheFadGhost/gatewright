package middleware

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"gatewright/internal/config"
)

func containsString(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}
	return false
}

func joinNotEmpty(list []string) string {
	out := make([]string, 0, len(list))
	for _, s := range list {
		if strings.TrimSpace(s) != "" {
			out = append(out, strings.TrimSpace(s))
		}
	}
	return strings.Join(out, ", ")
}

// NewCORS implements route-level CORS. Preflight OPTIONS requests are answered
// with 204 and never reach auth or the proxy. A "*" origin entry only ever
// produces Access-Control-Allow-Origin: * without credentials — credentialed
// CORS requires exact origins, so "*" matches nothing in that mode.
func NewCORS(cfg *config.CORS) Middleware {
	if cfg == nil || len(cfg.AllowedOrigins) == 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	creds := cfg.AllowCredentials
	wildcard := containsString(cfg.AllowedOrigins, "*") && !creds
	methods := joinNotEmpty(cfg.AllowedMethods)
	expose := joinNotEmpty(cfg.ExposeHeaders)
	allowHeaders := joinNotEmpty(cfg.AllowedHeaders)
	var maxAge int64
	if cfg.MaxAge.IsSet() && cfg.MaxAge.D > 0 {
		maxAge = int64(cfg.MaxAge.D / time.Second)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			defer func() { RecordStage(r.Context(), OrderNames[PosCORS], time.Since(start)) }()
			origin := r.Header.Get("Origin")
			if origin == "" {
				next.ServeHTTP(w, r)
				return
			}
			matched := wildcard || containsString(cfg.AllowedOrigins, origin)
			// The response varies by Origin whether it was allowed or denied.
			w.Header().Add("Vary", "Origin")
			preflight := r.Method == http.MethodOptions &&
				r.Header.Get("Access-Control-Request-Method") != ""
			if preflight {
				// Always short-circuit preflight (never reaches auth/proxy);
				// unmatched origins receive no Allow-* headers, which makes
				// the browser reject it.
				if matched {
					if wildcard {
						w.Header().Set("Access-Control-Allow-Origin", "*")
					} else {
						w.Header().Set("Access-Control-Allow-Origin", origin)
						if creds {
							w.Header().Set("Access-Control-Allow-Credentials", "true")
						}
					}
					if methods != "" {
						w.Header().Set("Access-Control-Allow-Methods", methods)
					} else {
						w.Header().Set("Access-Control-Allow-Methods",
							r.Header.Get("Access-Control-Request-Method"))
					}
					hdrs := allowHeaders
					if hdrs == "" {
						hdrs = r.Header.Get("Access-Control-Request-Headers")
					}
					if hdrs != "" {
						w.Header().Set("Access-Control-Allow-Headers", hdrs)
					}
					if maxAge > 0 {
						w.Header().Set("Access-Control-Max-Age", strconv.FormatInt(maxAge, 10))
					}
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}
			if !matched {
				next.ServeHTTP(w, r)
				return
			}
			if wildcard {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				if creds {
					w.Header().Set("Access-Control-Allow-Credentials", "true")
				}
			}
			if expose != "" {
				w.Header().Set("Access-Control-Expose-Headers", expose)
			}
			next.ServeHTTP(w, r)
		})
	}
}
