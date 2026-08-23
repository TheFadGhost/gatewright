package middleware

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"net/http"
	"strconv"
	"time"
)

// RequestIDHeader is the gateway-owned correlation header (DESIGN.md §5).
const RequestIDHeader = "X-Gatewright-Request-Id"

const (
	reqIDMaxLen    = 64
	reqIDRandChars = 26 // 16 random bytes -> exactly 26 raw-base32 chars
)

type requestIDCtxKey struct{}

// WithRequestID installs the request id into ctx.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDCtxKey{}, id)
}

// RequestIDFrom returns the request id, or "" when unset.
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDCtxKey{}).(string)
	return id
}

// ValidRequestID reports whether s is an acceptable inbound id: 1..64
// printable ASCII bytes, no spaces or control characters, so it survives
// headers, log lines and grep unchanged.
func ValidRequestID(s string) bool {
	if len(s) < 1 || len(s) > reqIDMaxLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x21 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// generateRequestID returns "gw-" + 26 chars of unpadded base32 (160 bits).
func generateRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure means platform entropy is broken; keep a
		// printable, unique-enough fallback rather than dropping correlation.
		return "gw-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return "gw-" + base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:])
}

// NewRequestID accepts a valid inbound X-Gatewright-Request-Id or generates
// one, then publishes it on both the request header and the context so every
// later stage (envelopes, access logs) can correlate.
func NewRequestID() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			id := r.Header.Get(RequestIDHeader)
			if !ValidRequestID(id) {
				id = generateRequestID()
			}
			r.Header.Set(RequestIDHeader, id)
			ctx := WithRequestID(r.Context(), id)
			next.ServeHTTP(w, r.WithContext(ctx))
			RecordStage(ctx, OrderNames[PosRequestID], time.Since(start))
		})
	}
}
