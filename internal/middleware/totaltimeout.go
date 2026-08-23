package middleware

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"gatewright/internal/errs"
)

// timeoutWriter serialises header state between the inner handler and the
// deadline watcher. Once the deadline fires before any byte was written, an
// UP004 envelope is emitted and later inner writes are discarded; if the
// response already started, only context cancellation remains (the deadline
// surfaces through inner writes failing on their own).
type timeoutWriter struct {
	http.ResponseWriter
	mu          sync.Mutex
	ctx         context.Context // pre-deadline ctx, for record enrichment
	reqID       string
	wroteHeader bool
	finished    bool
	timedOut    bool
}

func (t *timeoutWriter) expire() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished || t.wroteHeader {
		return
	}
	t.timedOut, t.wroteHeader = true, true
	recordCode(t.ctx, errs.CodeTotalTimeout)
	errs.WriteWithID(t.ResponseWriter,
		errs.New(errs.CodeTotalTimeout, "total route timeout exceeded"), t.reqID)
}

func (t *timeoutWriter) markFinished() {
	t.mu.Lock()
	t.finished = true
	t.mu.Unlock()
}

func (t *timeoutWriter) WriteHeader(code int) {
	t.mu.Lock()
	if t.timedOut || t.wroteHeader {
		t.mu.Unlock()
		return
	}
	t.wroteHeader = true
	t.mu.Unlock()
	t.ResponseWriter.WriteHeader(code)
}

func (t *timeoutWriter) Write(b []byte) (int, error) {
	t.mu.Lock()
	if t.timedOut {
		t.mu.Unlock()
		return len(b), nil // discard: envelope already sent
	}
	first := !t.wroteHeader
	t.wroteHeader = true
	t.mu.Unlock()
	if first {
		t.ResponseWriter.WriteHeader(http.StatusOK)
	}
	return t.ResponseWriter.Write(b)
}

// Hijack passes protocol upgrades through to the server connection. Once the
// connection is hijacked the deadline watcher can no longer write an envelope,
// which markFinished/expire already tolerate.
func (t *timeoutWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := t.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("middleware: ResponseWriter does not support Hijack")
	}
	return hj.Hijack()
}

// NewTotalTimeout bounds the whole request lifetime with a context deadline.
// d <= 0 disables bounding.
func NewTotalTimeout(d time.Duration) Middleware {
	if d <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			tw := &timeoutWriter{ResponseWriter: w, ctx: r.Context(), reqID: RequestIDFrom(ctx)}
			done := make(chan struct{})
			go func() {
				select {
				case <-ctx.Done():
					if errors.Is(ctx.Err(), context.DeadlineExceeded) {
						tw.expire()
					}
				case <-done:
				}
			}()
			defer func() {
				tw.markFinished() // no spurious envelope after normal completion
				close(done)
			}()
			next.ServeHTTP(tw, r.WithContext(ctx))
			RecordStage(r.Context(), OrderNames[PosTotalTimeout], time.Since(start))
		})
	}
}
