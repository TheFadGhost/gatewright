package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"gatewright/internal/config"
	"gopkg.in/yaml.v3"
)

func preflightRequest(origin, method string) *http.Request {
	req := httptest.NewRequest(http.MethodOptions, "/v1/x", nil)
	req.Header.Set("Origin", origin)
	req.Header.Set("Access-Control-Request-Method", method)
	return req
}

// mustDuration builds a config.Duration the way the YAML loader would, since
// its "set" flag is unexported.
func mustDuration(t *testing.T, s string) config.Duration {
	t.Helper()
	var d config.Duration
	if err := yaml.Unmarshal([]byte(s), &d); err != nil {
		t.Fatalf("yaml duration %q: %v", s, err)
	}
	return d
}

func TestCORSPreflightExactOriginShortCircuit(t *testing.T) {
	reached := false
	cfg := &config.CORS{
		AllowedOrigins: []string{"https://app.example.com"},
		AllowedMethods: []string{"GET", "POST"},
		AllowedHeaders: []string{"X-API-Key", "Content-Type"},
		MaxAge:         mustDuration(t, "10m"),
	}
	h := NewCORS(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, preflightRequest("https://app.example.com", "POST"))
	if reached {
		t.Fatal("preflight reached inner handler")
	}
	hd := rec.Header()
	switch {
	case rec.Code != http.StatusNoContent:
		t.Errorf("status = %d, want 204", rec.Code)
	case hd.Get("Access-Control-Allow-Origin") != "https://app.example.com":
		t.Errorf("ACAO = %q", hd.Get("Access-Control-Allow-Origin"))
	case hd.Get("Access-Control-Allow-Methods") != "GET, POST":
		t.Errorf("methods = %q", hd.Get("Access-Control-Allow-Methods"))
	case hd.Get("Access-Control-Allow-Headers") != "X-API-Key, Content-Type":
		t.Errorf("headers = %q", hd.Get("Access-Control-Allow-Headers"))
	case hd.Get("Access-Control-Max-Age") != "600":
		t.Errorf("max-age = %q", hd.Get("Access-Control-Max-Age"))
	case hd.Get("Vary") != "Origin":
		t.Errorf("vary = %q", hd.Get("Vary"))
	case hd.Get("Access-Control-Allow-Credentials") != "":
		t.Error("credentials header present without config")
	}
}

func TestCORSPreflightWildcardWithoutCredentials(t *testing.T) {
	cfg := &config.CORS{AllowedOrigins: []string{"*"}, AllowedMethods: []string{"GET"}}
	h := NewCORS(cfg)(okHandler("never"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, preflightRequest("https://anywhere.dev", "GET"))
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("ACAO = %q, want *", got)
	}
}

// The forbidden combination: wildcard + credentials must never emit "*".
func TestCORSCredentialsWildcardForbidden(t *testing.T) {
	reached := false
	cfg := &config.CORS{
		AllowedOrigins:   []string{"*"},
		AllowCredentials: true,
		AllowedMethods:   []string{"GET"},
	}
	h := NewCORS(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached = true }))
	for _, tc := range []struct {
		name string
		req  *http.Request
	}{
		{"preflight", preflightRequest("https://evil.example", "GET")},
		{"actual", func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Header.Set("Origin", "https://evil.example")
			return r
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reached = false
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, tc.req)
			hd := rec.Header()
			if got := hd.Get("Access-Control-Allow-Origin"); got == "*" {
				t.Fatal("emitted ACAO:* together with credentials mode")
			}
			if got := hd.Get("Access-Control-Allow-Credentials"); got == "true" && hd.Get("Access-Control-Allow-Origin") == "*" {
				t.Fatal("credentials granted to wildcard origin")
			}
			if tc.name == "preflight" && !reached {
				t.Log("preflight answered without allow headers (browser will reject)")
			}
			if tc.name == "actual" && !reached {
				t.Fatal("non-preflight request with unmatched origin must still be forwarded")
			}
		})
	}
}

func TestCORSCredentialsExactOrigin(t *testing.T) {
	cfg := &config.CORS{
		AllowedOrigins:   []string{"https://trusted.example"},
		AllowCredentials: true,
		AllowedMethods:   []string{"GET", "DELETE"},
	}
	h := NewCORS(cfg)(okHandler("body"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, preflightRequest("https://trusted.example", "DELETE"))
	hd := rec.Header()
	switch {
	case hd.Get("Access-Control-Allow-Origin") != "https://trusted.example":
		t.Errorf("ACAO must reflect the exact origin, got %q", hd.Get("Access-Control-Allow-Origin"))
	case hd.Get("Access-Control-Allow-Credentials") != "true":
		t.Error("Allow-Credentials missing")
	}
}

func TestCORSPreflightUnmatchedOriginNoAllowHeaders(t *testing.T) {
	cfg := &config.CORS{AllowedOrigins: []string{"https://good.example"}, AllowedMethods: []string{"GET"}}
	h := NewCORS(cfg)(okHandler("never"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, preflightRequest("https://bad.example", "GET"))
	hd := rec.Header()
	switch {
	case rec.Code != http.StatusNoContent:
		t.Errorf("status = %d, want 204 short-circuit", rec.Code)
	case hd.Get("Access-Control-Allow-Origin") != "":
		t.Errorf("ACAO leaked for unmatched origin: %q", hd.Get("Access-Control-Allow-Origin"))
	}
}

func TestCORSNonPreflightAddsExposeAndProceeds(t *testing.T) {
	reached := false
	cfg := &config.CORS{
		AllowedOrigins: []string{"https://app.example.com"},
		ExposeHeaders:  []string{"RateLimit-Remaining", "X-Request-Id"},
	}
	h := NewCORS(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusTeapot)
	}))
	req := httptest.NewRequest(http.MethodGet, "/v1/x", nil)
	req.Header.Set("Origin", "https://app.example.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if !reached || rec.Code != http.StatusTeapot {
		t.Fatalf("inner not reached / wrong status: reached=%v code=%d", reached, rec.Code)
	}
	hd := rec.Header()
	switch {
	case hd.Get("Access-Control-Allow-Origin") != "https://app.example.com":
		t.Errorf("ACAO = %q", hd.Get("Access-Control-Allow-Origin"))
	case hd.Get("Access-Control-Expose-Headers") != "RateLimit-Remaining, X-Request-Id":
		t.Errorf("expose = %q", hd.Get("Access-Control-Expose-Headers"))
	}
}

func TestCORSNoOriginHeaderUntouched(t *testing.T) {
	reached := false
	cfg := &config.CORS{AllowedOrigins: []string{"*"}}
	h := NewCORS(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if !reached || rec.Header().Get("Vary") != "" ||
		rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("CORS headers emitted for a same-origin (no Origin header) request")
	}
}

func TestCORSNonPreflightOptionsFallsThrough(t *testing.T) {
	reached := false
	cfg := &config.CORS{AllowedOrigins: []string{"https://app.example.com"}, AllowedMethods: []string{"OPTIONS"}}
	h := NewCORS(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodOptions, "/", nil) // no Access-Control-Request-Method
	req.Header.Set("Origin", "https://app.example.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if !reached {
		t.Fatal("plain OPTIONS (health-check style) must reach the inner handler")
	}
}
