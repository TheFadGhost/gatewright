package middleware

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"gatewright/internal/errs"
	"gatewright/internal/limiter"
	"gatewright/internal/obs"
)

// RateLimitEntry is one configured limiter bound to a key extractor.
type RateLimitEntry struct {
	Limiter  limiter.Limiter
	KeyFn    KeyExtractor
	Name     string
	Strategy string
}

// pendingRelease holds an admitted concurrency slot to hand back on response
// completion.
type pendingRelease struct {
	rel  limiter.Releaser
	key  string
	cost int64
}

// ceilSeconds converts a duration into whole seconds, rounding up.
func ceilSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Second - 1) / time.Second)
}

func retryAfterSeconds(d time.Duration) int64 {
	s := ceilSeconds(d)
	if s < 1 {
		return 1 // Retry-After granularity is whole seconds; never zero
	}
	return s
}

// moreRestrictive reports whether a constrains traffic at least as hard as b:
// denials dominate, then fewer remaining units, then longer retry waits.
func moreRestrictive(a, b limiter.Decision) bool {
	if a.Allowed != b.Allowed {
		return !a.Allowed
	}
	if a.Remaining != b.Remaining {
		return a.Remaining < b.Remaining
	}
	return a.RetryAfter > b.RetryAfter
}

func setRateHeaders(h http.Header, d limiter.Decision) {
	limit := strconv.FormatInt(d.Limit, 10)
	rem := strconv.FormatInt(d.Remaining, 10)
	reset := strconv.FormatInt(ceilSeconds(d.ResetIn), 10)
	h.Set("RateLimit-Limit", limit)
	h.Set("RateLimit-Remaining", rem)
	h.Set("RateLimit-Reset", reset)
	h.Set("X-RateLimit-Limit", limit)
	h.Set("X-RateLimit-Remaining", rem)
	h.Set("X-RateLimit-Reset", reset)
}

// completionWriter detects response completion (first WriteHeader/Write,
// Flush, Hijack, or end of ServeHTTP) so concurrency slots are released
// exactly once.
type completionWriter struct {
	http.ResponseWriter
	once     sync.Once
	onDone   func()
	complete bool
}

func (c *completionWriter) markComplete() {
	c.once.Do(c.onDone)
}

func (c *completionWriter) WriteHeader(code int) {
	if !c.complete {
		c.complete = true
		c.markComplete()
	}
	c.ResponseWriter.WriteHeader(code)
}

func (c *completionWriter) Write(b []byte) (int, error) {
	if !c.complete {
		c.complete = true
		c.markComplete()
	}
	return c.ResponseWriter.Write(b)
}

func (c *completionWriter) Flush() {
	c.markComplete()
	if f, ok := c.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (c *completionWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	c.markComplete()
	h, ok := c.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("middleware: ResponseWriter does not support Hijack")
	}
	return h.Hijack()
}

// NewRateLimit ANDs all entries: every configured limiter must admit. The
// first denial short-circuits evaluation and answers RATE001 with Retry-After
// plus RateLimit-* / X-RateLimit-* headers from the most restrictive decision
// among the evaluated limiters. Allowed requests still report those headers.
// Concurrency-style limiters (limiter.AsReleaser) have their admitted units
// released exactly once when the response completes.
func NewRateLimit(entries []RateLimitEntry, sink *obs.Metrics) Middleware {
	if len(entries) == 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	report := func(e *RateLimitEntry, allowed bool) {
		if sink == nil {
			return
		}
		outcome := "allowed"
		if !allowed {
			outcome = "limited"
		}
		sink.IncCounter("gatewright_limiter_decisions_total",
			"rate limiter admission decisions",
			map[string]string{"name": e.Name, "strategy": e.Strategy, "outcome": outcome})
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			id, _ := IdentityFrom(r.Context())
			var (
				most      limiter.Decision
				mostName  string
				have      bool
				denied    *RateLimitEntry
				deniedKey string
				pending   []pendingRelease
			)
			for i := range entries {
				e := &entries[i]
				key := ""
				if e.KeyFn != nil {
					key = e.KeyFn(r, id)
				}
				d := e.Limiter.Allow(key, time.Now(), 1)
				report(e, d.Allowed)
				if rel, ok := limiter.AsReleaser(e.Limiter); ok && d.Allowed {
					pending = append(pending, pendingRelease{rel: rel, key: key, cost: 1})
				}
				if !have || moreRestrictive(d, most) {
					most, mostName, have = d, e.Name, true
				}
				if !d.Allowed {
					denied, deniedKey = e, key
					break // first denial short-circuits remaining limiters
				}
			}
			var relOnce sync.Once
			releaseAll := func() {
				relOnce.Do(func() {
					for _, p := range pending {
						p.rel.Release(p.key, time.Now(), p.cost)
					}
				})
			}
			if rec := RecordFrom(r.Context()); rec != nil {
				rec.Fields.LimiterName = mostName
				rec.Fields.LimiterOutcome = "allowed"
				if denied != nil {
					rec.Fields.LimiterOutcome = "limited"
				}
			}
			if denied != nil {
				releaseAll() // slots admitted by earlier limiters end here too
				recordCode(r.Context(), errs.CodeRateLimited)
				h := w.Header()
				setRateHeaders(h, most)
				h.Set("Retry-After", strconv.FormatInt(retryAfterSeconds(most.RetryAfter), 10))
				errs.WriteWithID(w,
					errs.New(errs.CodeRateLimited,
						fmt.Sprintf("quota exceeded for key %q (%s)", deniedKey, denied.Name)),
					RequestIDFrom(r.Context()))
				RecordStage(r.Context(), OrderNames[PosRateLimit], time.Since(start))
				return
			}
			setRateHeaders(w.Header(), most)
			cw := &completionWriter{ResponseWriter: w, onDone: releaseAll}
			defer cw.markComplete()
			next.ServeHTTP(cw, r)
			RecordStage(r.Context(), OrderNames[PosRateLimit], time.Since(start))
		})
	}
}
