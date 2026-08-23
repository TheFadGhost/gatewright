package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gatewright/internal/config"
	"gatewright/internal/errs"
	"gatewright/internal/limiter"
)

func allowDecision(limit, remaining int64) limiter.Decision {
	return limiter.Decision{Allowed: true, Limit: limit, Remaining: remaining, ResetIn: time.Minute}
}

func denyDecision(limit int64, retryAfter, resetIn time.Duration) limiter.Decision {
	return limiter.Decision{Allowed: false, Limit: limit, Remaining: 0, RetryAfter: retryAfter, ResetIn: resetIn}
}

// fakeLimiter is an inline stub of limiter.Limiter recording every call.
type fakeLimiter struct {
	mu       sync.Mutex
	dec      limiter.Decision
	calls    []string
	lastCost int64
}

func (f *fakeLimiter) Allow(key string, now time.Time, cost int64) limiter.Decision {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, key)
	f.lastCost = cost
	return f.dec
}

func (f *fakeLimiter) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// fakeConcurrent additionally implements limiter.Releaser.
type fakeConcurrent struct {
	fakeLimiter
	mu       sync.Mutex
	releases []string
}

func (f *fakeConcurrent) Release(key string, now time.Time, cost int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases = append(f.releases, key)
}

func (f *fakeConcurrent) releaseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.releases)
}

func TestRateLimitAllAllowedMostRestrictiveHeaders(t *testing.T) {
	a := &fakeLimiter{dec: allowDecision(100, 9)}
	b := &fakeLimiter{dec: allowDecision(10, 3)}
	h := NewRateLimit([]RateLimitEntry{
		{Limiter: a, KeyFn: mustExtractor(t, "ip"), Name: "wide", Strategy: "token_bucket"},
		{Limiter: b, KeyFn: mustExtractor(t, "path"), Name: "tight", Strategy: "fixed_window"},
	}, nil)(okHandler("ok"))
	req := httptest.NewRequest(http.MethodGet, "/v1/x", nil)
	req.RemoteAddr = "1.2.3.4:1111"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	switch {
	case rec.Code != http.StatusOK:
		t.Errorf("status = %d", rec.Code)
	case rec.Header().Get("RateLimit-Limit") != "10":
		t.Errorf("most restrictive limit = %q", rec.Header().Get("RateLimit-Limit"))
	case rec.Header().Get("RateLimit-Remaining") != "3":
		t.Errorf("most restrictive remaining = %q", rec.Header().Get("RateLimit-Remaining"))
	case rec.Header().Get("X-RateLimit-Remaining") != "3":
		t.Errorf("legacy remaining = %q", rec.Header().Get("X-RateLimit-Remaining"))
	case rec.Header().Get("RateLimit-Reset") == "":
		t.Error("reset header missing")
	}
	if a.callCount() != 1 || b.callCount() != 1 {
		t.Fatalf("calls a=%d b=%d, want both evaluated once", a.callCount(), b.callCount())
	}
}

func TestRateLimitDenialShortCircuits(t *testing.T) {
	deny := &fakeLimiter{dec: denyDecision(5, 2500*time.Millisecond, 3*time.Second)}
	never := &fakeLimiter{dec: allowDecision(99, 99)}
	h := NewRateLimit([]RateLimitEntry{
		{Limiter: deny, KeyFn: mustExtractor(t, "ip"), Name: "burst", Strategy: "fixed_window"},
		{Limiter: never, KeyFn: mustExtractor(t, "ip"), Name: "never-run", Strategy: "fixed_window"},
	}, nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("inner reached despite denial")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	env := decodeEnvelope(t, rec)
	hd := rec.Header()
	switch {
	case rec.Code != http.StatusTooManyRequests:
		t.Errorf("status = %d, want 429", rec.Code)
	case env.Error.Code != errs.CodeRateLimited:
		t.Errorf("code = %q, want RATE001", env.Error.Code)
	case hd.Get("Retry-After") != "3": // ceil(2500ms) = 3s
		t.Errorf("Retry-After = %q, want 3", hd.Get("Retry-After"))
	case hd.Get("RateLimit-Limit") != "5":
		t.Errorf("limit = %q", hd.Get("RateLimit-Limit"))
	case hd.Get("RateLimit-Remaining") != "0":
		t.Errorf("remaining = %q", hd.Get("RateLimit-Remaining"))
	}
	if never.callCount() != 0 {
		t.Fatalf("denial did not short-circuit: second limiter called %d times", never.callCount())
	}
}

func TestRateLimitDenialMessageNeverEchoesKey(t *testing.T) {
	const secretKey = "sk-live-super-secret-42"
	l := &fakeLimiter{dec: denyDecision(1, time.Second, time.Second)}
	injector := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(WithIdentity(r.Context(), &Identity{APIKey: secretKey})))
		})
	}
	rec := httptest.NewRecorder()
	Chain(okHandler(""), injector, NewRequestID(),
		NewRateLimit([]RateLimitEntry{{Limiter: l, KeyFn: mustExtractor(t, "api_key"), Name: "ip-burst"}}, nil)).
		ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	body := rec.Body.String()
	env := decodeEnvelope(t, rec)
	if env.Error.Code != errs.CodeRateLimited {
		t.Fatalf("code = %q", env.Error.Code)
	}
	if strings.Contains(body, secretKey) {
		t.Errorf("denial body %q echoes the bucket key; it must never be surfaced to clients", body)
	}
	want := "quota exceeded (limiter ip-burst)"
	if !strings.Contains(env.Error.Message, want) {
		t.Errorf("message = %q, want it to name the limiter via %q", env.Error.Message, want)
	}
}

func TestRateLimitRetryAfterMinimumOneSecond(t *testing.T) {
	for _, tc := range []struct {
		retry time.Duration
		want  string
	}{{400 * time.Millisecond, "1"}, {time.Millisecond, "1"}, {2100 * time.Millisecond, "3"}} {
		l := &fakeLimiter{dec: denyDecision(1, tc.retry, time.Second)}
		h := NewRateLimit([]RateLimitEntry{{Limiter: l, KeyFn: mustExtractor(t, "ip"), Name: "x"}}, nil)(okHandler(""))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if got := rec.Header().Get("Retry-After"); got != tc.want {
			t.Fatalf("retry=%v: Retry-After = %q, want %q", tc.retry, got, tc.want)
		}
	}
}

func TestRateLimitReleaseExactlyOnceOnCompletion(t *testing.T) {
	fc := &fakeConcurrent{}
	fc.dec = allowDecision(4, 3)
	writes := 0
	h := NewRateLimit([]RateLimitEntry{{Limiter: fc, KeyFn: mustExtractor(t, "ip"), Name: "conc", Strategy: "concurrency"}},
		nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("one"))
		_, _ = w.Write([]byte("two"))
		writes++
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	time.Sleep(10 * time.Millisecond)
	if fc.releaseCount() != 1 {
		t.Fatalf("releases after multi-write response = %d, want exactly 1", fc.releaseCount())
	}
	if writes != 1 || rec.Body.String() != "onetwo" {
		t.Fatalf("inner handler misbehaved")
	}
}

func TestRateLimitReleasedWhenHandlerNeverWrites(t *testing.T) {
	fc := &fakeConcurrent{}
	fc.dec = allowDecision(2, 2)
	h := NewRateLimit([]RateLimitEntry{{Limiter: fc, KeyFn: mustExtractor(t, "ip"), Name: "conc", Strategy: "concurrency"}},
		nil)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	if fc.releaseCount() != 1 {
		t.Fatalf("end-of-ServeHTTP release missing: count=%d", fc.releaseCount())
	}
}

func TestRateLimitAdmittedConcurrencyReleasedWhenLaterLimiterDenies(t *testing.T) {
	fc := &fakeConcurrent{}
	fc.dec = allowDecision(8, 7)
	deny := &fakeLimiter{dec: denyDecision(1, time.Second, time.Second)}
	logger := &captureLogger{}
	h := Chain(
		okHandler(""),
		NewRequestID(),
		NewAccessLog(logger, "r"),
		NewRateLimit([]RateLimitEntry{
			{Limiter: fc, KeyFn: mustExtractor(t, "ip"), Name: "conc", Strategy: "concurrency"},
			{Limiter: deny, KeyFn: mustExtractor(t, "ip"), Name: "quota", Strategy: "fixed_window"},
		}, nil),
	)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d", rec.Code)
	}
	if n := fc.releaseCount(); n != 1 {
		t.Fatalf("admitted slot not released on denial: %d releases", n)
	}
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(logger.fields) != 1 {
		t.Fatalf("access lines = %d", len(logger.fields))
	}
	f := logger.fields[0]
	switch {
	case f.Code != "RATE001":
		t.Errorf("code field = %q", f.Code)
	case f.LimiterOutcome != "limited":
		t.Errorf("limiter_outcome = %q", f.LimiterOutcome)
	case f.LimiterName != "quota" && f.LimiterName != "conc":
		t.Errorf("limiter_name = %q", f.LimiterName)
	}
}

func TestRateLimitAllowedOutcomeRecorded(t *testing.T) {
	l := &fakeLimiter{dec: allowDecision(6, 6)}
	logger := &captureLogger{}
	h := Chain(
		okHandler("fine"),
		NewRequestID(),
		NewAccessLog(logger, "r"),
		NewRateLimit([]RateLimitEntry{{Limiter: l, KeyFn: mustExtractor(t, "ip"), Name: "ip-burst"}}, nil),
	)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	logger.mu.Lock()
	defer logger.mu.Unlock()
	f := logger.fields[0]
	switch {
	case f.LimiterName != "ip-burst":
		t.Errorf("limiter_name = %q", f.LimiterName)
	case f.LimiterOutcome != "allowed":
		t.Errorf("limiter_outcome = %q", f.LimiterOutcome)
	case f.Code != "":
		t.Errorf("code set on success: %q", f.Code)
	}
}

func TestRateLimitKeyUsesIdentityWhenPresent(t *testing.T) {
	l := &fakeLimiter{dec: allowDecision(1, 1)}
	ks, err := config.ParseKeySpec("api_key")
	if err != nil {
		t.Fatal(err)
	}
	fn, err := BuildKeyExtractor(*ks)
	if err != nil {
		t.Fatal(err)
	}
	injector := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(WithIdentity(r.Context(), &Identity{APIKey: "sk-42"})))
		})
	}
	h := Chain(okHandler("ok"), injector,
		NewRateLimit([]RateLimitEntry{{Limiter: l, KeyFn: fn, Name: "k"}}, nil))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.calls) != 1 || l.calls[0] != "api_key\x00sk-42" {
		t.Fatalf("limiter keys = %q, want api_key\\x00sk-42", l.calls)
	}
}
