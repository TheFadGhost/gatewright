package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gatewright/internal/config"
)

func okHandler(body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(body))
	})
}

type wireEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		ReqID   string `json:"req_id"`
	} `json:"error"`
}

func decodeEnvelope(t *testing.T, rec *httptest.ResponseRecorder) wireEnvelope {
	t.Helper()
	var env wireEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("response body is not an envelope: %v\nbody: %q", err, rec.Body.String())
	}
	return env
}

// syncRecorder serialises recorder access for middlewares that respond from a
// watcher goroutine (total-timeout).
type syncRecorder struct {
	mu  sync.Mutex
	rec *httptest.ResponseRecorder
}

func newSyncRecorder() *syncRecorder { return &syncRecorder{rec: httptest.NewRecorder()} }

func (s *syncRecorder) Header() http.Header { return s.rec.Header() }

func (s *syncRecorder) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec.Write(b)
}

func (s *syncRecorder) WriteHeader(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rec.WriteHeader(code)
}

func (s *syncRecorder) snapshot() (code int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec.Code, s.rec.Body.String()
}

// awaitStatus polls until the recorded status equals want or the deadline
// passes; returns the last seen code/body.
func awaitStatus(s *syncRecorder, want int, timeout time.Duration) (int, string) {
	deadline := time.Now().Add(timeout)
	for {
		code, body := s.snapshot()
		if code == want || time.Now().After(deadline) {
			return code, body
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestChainOrderFirstArgOutermost(t *testing.T) {
	var mu sync.Mutex
	var order []string
	mk := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				order = append(order, "pre-"+name)
				mu.Unlock()
				next.ServeHTTP(w, r)
				mu.Lock()
				order = append(order, "post-"+name)
				mu.Unlock()
			})
		}
	}
	got := Chain(okHandler("x"), mk("a"), mk("b"), mk("c"))
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	got.ServeHTTP(w, r)
	want := []string{"pre-a", "pre-b", "pre-c", "post-c", "post-b", "post-a"}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order[%d] = %q, want %q (full: %v)", i, order[i], want[i], order)
		}
	}
}

func TestChainNoMiddlewaresPassthrough(t *testing.T) {
	h := Chain(okHandler("plain"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "plain" {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
}

func TestChainNilMiddlewareSkipped(t *testing.T) {
	h := Chain(okHandler("ok"), nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("got %d %q", rec.Code, rec.Body.String())
	}
}

// TestChainFullPipeline exercises the documented stage order end to end with
// every middleware in this package.
func TestChainFullPipeline(t *testing.T) {
	t.Setenv("GW_PIPELINE_KEYS", "secret-key")
	authCfg := &config.Auth{
		Type:   "api_key",
		APIKey: &config.APIKeyAuth{KeysEnv: "GW_PIPELINE_KEYS"},
	}
	corsCfg := &config.CORS{
		AllowedOrigins: []string{"https://app.example.com"},
		AllowedMethods: []string{"GET", "POST"},
	}
	lim1 := &fakeLimiter{dec: allowDecision(10, 4)}
	entries := []RateLimitEntry{{Limiter: lim1, KeyFn: mustExtractor(t, "ip"), Name: "ip-burst", Strategy: "fixed_window"}}
	logger := &captureLogger{}
	terminal := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, _ := IdentityFrom(r.Context())
		if id == nil || id.APIKey != "secret-key" {
			t.Error("identity did not reach terminal handler")
		}
		if got := r.Header.Get("X-Forwarded-Tenant"); got != "acme" {
			t.Errorf("manipulated header = %q, want acme", got)
		}
		if RequestIDFrom(r.Context()) == "" {
			t.Error("request id missing at terminal")
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("done"))
	})
	h := Chain(terminal,
		NewRequestID(),
		NewAccessLog(logger, "api-v1"),
		NewBodyLimit(1024),
		NewTotalTimeout(time.Second),
		NewCORS(corsCfg),
		NewAuth(authCfg),
		NewRequestHeaders(config.HeaderManip{Set: map[string]string{"X-Forwarded-Tenant": "acme"}}),
		NewRateLimit(entries, nil),
	)
	req := httptest.NewRequest(http.MethodPost, "/v1/things?x=1", strings.NewReader(`hi`))
	req.Header.Set("Origin", "https://app.example.com")
	req.Header.Set("X-API-Key", "secret-key")
	req.ContentLength = 2
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK || rec.Body.String() != "done" {
		t.Fatalf("status/body = %d %q", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Errorf("ACAO = %q", got)
	}
	if got := rec.Header().Get("RateLimit-Remaining"); got != "4" {
		t.Errorf("RateLimit-Remaining = %q, want 4", got)
	}
	logger.mu.Lock()
	defer logger.mu.Unlock()
	if len(logger.fields) != 1 {
		t.Fatalf("access lines emitted = %d, want 1", len(logger.fields))
	}
	f := logger.fields[0]
	switch {
	case f.Route != "api-v1":
		t.Errorf("route = %q", f.Route)
	case f.ReqID == "":
		t.Errorf("req_id empty in access line")
	case f.Status != 200:
		t.Errorf("status = %d", f.Status)
	case f.LimiterName != "ip-burst" || f.LimiterOutcome != "allowed":
		t.Errorf("limiter fields = %q/%q", f.LimiterName, f.LimiterOutcome)
	case f.DurationMS < 0:
		t.Errorf("duration_ms negative")
	}
}
