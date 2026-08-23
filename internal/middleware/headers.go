package middleware

import (
	"bufio"
	"errors"
	"net"
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

// responseHeadersWriter intercepts the inner handler's first WriteHeader or
// Write (plus Flush/Hijack, which bypass them), applies the configured
// Set/Add/Del manipulations to the response header map BEFORE anything
// reaches the network, then delegates. Nothing is buffered: only the Header()
// map is mutated in place, once.
type responseHeadersWriter struct {
	http.ResponseWriter
	m       config.HeaderManip
	applied bool
}

func (w *responseHeadersWriter) applyOnce() {
	if w.applied {
		return
	}
	w.applied = true
	for name, val := range w.m.Set {
		w.Header().Set(name, val)
	}
	for name, val := range w.m.Add {
		w.Header().Add(name, val)
	}
	for _, name := range w.m.Del {
		w.Header().Del(name)
	}
}

func (w *responseHeadersWriter) WriteHeader(code int) {
	w.applyOnce()
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseHeadersWriter) Write(b []byte) (int, error) {
	w.applyOnce()
	return w.ResponseWriter.Write(b)
}

func (w *responseHeadersWriter) Flush() {
	w.applyOnce()
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (w *responseHeadersWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.applyOnce() // headers must be final before the connection is handed over
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("middleware: ResponseWriter does not support Hijack")
	}
	return h.Hijack()
}

// NewResponseHeaders applies outbound response-header manipulation: Set
// overwrites any existing values, Add appends, Del removes all values —
// including headers an upstream already set. The manipulation lands on the
// header map just before the first write of the response body/status, so it
// composes with streamed responses without buffering a byte.
func NewResponseHeaders(m config.HeaderManip) Middleware {
	if len(m.Set) == 0 && len(m.Add) == 0 && len(m.Del) == 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(&responseHeadersWriter{ResponseWriter: w, m: m}, r)
		})
	}
}
