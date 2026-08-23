package middleware

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"gatewright/internal/errs"
)

// errBodyTooLarge is returned by the limited body reader once the configured
// byte budget is exhausted.
var errBodyTooLarge = errors.New("gatewright: request body exceeds the route limit")

// limitBody wraps http.MaxBytesReader and tracks exhaustion so the stage can
// answer 413 deterministically even when a handler swallows read errors.
type limitBody struct {
	rc        io.ReadCloser
	remaining int64
	over      bool
}

func (l *limitBody) Read(p []byte) (int, error) {
	if l.over {
		return 0, errBodyTooLarge
	}
	n, err := l.rc.Read(p)
	if n > 0 {
		l.remaining -= int64(n)
		if l.remaining < 0 {
			l.over = true
		}
	}
	if err != nil && !l.over {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			l.over = true
		}
	}
	if l.over {
		return n, errBodyTooLarge
	}
	return n, err
}

func (l *limitBody) Close() error { return l.rc.Close() }

// limitWriter converts an over-limit condition into a BODY001 envelope if it
// happens before any response byte is written; afterwards the inner handler's
// response stands (the connection-level abort belongs to the server).
type limitWriter struct {
	http.ResponseWriter
	body        *limitBody
	deny        func()
	handled     bool
	wroteHeader bool
}

func (l *limitWriter) rejectIfOver() bool {
	if l.body.over && !l.handled && !l.wroteHeader {
		l.handled, l.wroteHeader = true, true
		l.deny()
		return true
	}
	return false
}

func (l *limitWriter) WriteHeader(code int) {
	if l.rejectIfOver() || l.wroteHeader {
		return
	}
	l.wroteHeader = true
	l.ResponseWriter.WriteHeader(code)
}

func (l *limitWriter) Write(b []byte) (int, error) {
	if l.rejectIfOver() {
		return 0, errBodyTooLarge
	}
	if !l.wroteHeader {
		l.wroteHeader = true
		l.ResponseWriter.WriteHeader(http.StatusOK)
	}
	return l.ResponseWriter.Write(b)
}

// finish covers handlers that noticed the read error but never responded.
func (l *limitWriter) finish() {
	if l.body.over && !l.handled && !l.wroteHeader {
		l.handled = true
		l.deny()
	}
}

// Hijack passes protocol upgrades through; a hijacked connection carries no
// further body traffic, so the limit accounting simply ends with the stage.
func (l *limitWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := l.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("middleware: ResponseWriter does not support Hijack")
	}
	return hj.Hijack()
}

// NewBodyLimit enforces the route body limit: -1 means unlimited. Oversize
// Content-Length requests are rejected before any work; streamed bodies are
// enforced via http.MaxBytesReader with the same BODY001 outcome.
func NewBodyLimit(maxBytes int64) Middleware {
	if maxBytes < 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			reqID := RequestIDFrom(r.Context())
			deny := func() {
				recordCode(r.Context(), errs.CodePayloadTooLarge)
				errs.WriteWithID(w,
					errs.New(errs.CodePayloadTooLarge,
						fmt.Sprintf("request body exceeds the %d byte limit", maxBytes)),
					reqID)
			}
			if r.ContentLength > maxBytes {
				deny() // early rejection before auth/proxy work is spent
				RecordStage(r.Context(), OrderNames[PosBodyLimit], time.Since(start))
				return
			}
			lb := &limitBody{rc: http.MaxBytesReader(w, r.Body, maxBytes), remaining: maxBytes}
			r.Body = lb
			lw := &limitWriter{ResponseWriter: w, body: lb, deny: deny}
			defer lw.finish()
			next.ServeHTTP(lw, r)
			RecordStage(r.Context(), OrderNames[PosBodyLimit], time.Since(start))
		})
	}
}
