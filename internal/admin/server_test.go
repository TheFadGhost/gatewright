package admin

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gatewright/internal/pool"
)

type fakeProvider struct {
	mu        sync.Mutex
	routes    []RouteView
	pools     []PoolView
	limiters  []LimiterView
	rates     map[string]float64
	metrics   string
	p50       float64
	p95       float64
	p99       float64
	latOK     bool
	reloadErr error
	version   string
	uptime    time.Duration

	reloadCalls int
}

func newFake() *fakeProvider {
	return &fakeProvider{
		routes: []RouteView{
			{Name: "api-v1", Match: "api.example.com/v1/*", PoolName: "catalog",
				LimiterName: "ip-burst", HasLimiter: true},
		},
		pools: []PoolView{{
			Name: "catalog",
			Targets: []pool.TargetStatus{{
				Target:      pool.Target{Name: "catalog[0]", URL: "http://127.0.0.1:9001", Weight: 1},
				State:       pool.StateHealthy,
				Inflight:    3,
				TotalReq:    120,
				CircuitOpen: false,
			}},
		}},
		limiters: []LimiterView{{
			Route:         "api-v1",
			Name:          "ip-burst",
			Strategy:      "token_bucket",
			KeyType:       "ip",
			AllowedPerSec: 140.2,
			LimitedPerSec: 1.9,
			UsageFraction: -1,
			Evictions:     7,
		}},
		rates:   map[string]float64{"api-v1": 142.1},
		metrics: "# HELP gatewright_build_info Build information.\n# TYPE gatewright_build_info gauge\ngatewright_build_info 1\n",
		p50:     12.2,
		p95:     23,
		p99:     41.7,
		latOK:   true,
		version: "test-1",
		uptime:  90 * time.Second,
	}
}

func (f *fakeProvider) RoutesView() []RouteView { return f.routes }

func (f *fakeProvider) PoolsView() []PoolView { return f.pools }

func (f *fakeProvider) MetricsText() string { return f.metrics }

func (f *fakeProvider) LatencyPercentiles(route string, window time.Duration) (float64, float64, float64, bool) {
	return f.p50, f.p95, f.p99, f.latOK
}

func (f *fakeProvider) RequestRates() map[string]float64 { return f.rates }

func (f *fakeProvider) StatusCounts() map[string][3]uint64 {
	return map[string][3]uint64{"api-v1": {120, 2, 0}}
}

func (f *fakeProvider) LimiterViews() []LimiterView { return f.limiters }

func (f *fakeProvider) Reload() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reloadCalls++
	return f.reloadErr
}

func (f *fakeProvider) Version() string       { return f.version }
func (f *fakeProvider) Uptime() time.Duration { return f.uptime }

func get(t *testing.T, h http.Handler, path, token string) *httptest.ResponseRecorder {
	t.Helper()
	return do(t, h, http.MethodGet, path, token, "127.0.0.1:8080")
}

// getFrom issues a request with an explicit Host header (spoofing included).
func getFrom(h http.Handler, path, token, host string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Host = host
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func do(t *testing.T, h http.Handler, method, path, token, host string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	req.Host = host
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	return m
}

func TestStateShape(t *testing.T) {
	srv := New(newFake(), Options{Dashboard: true})
	rec := get(t, srv, "/admin/api/state", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type = %q", ct)
	}
	m := decodeBody(t, rec)
	if m["version"] != "test-1" {
		t.Errorf("version = %v", m["version"])
	}
	up, ok := m["uptime_seconds"].(float64)
	if !ok || up != 90 {
		t.Errorf("uptime_seconds = %v, want 90", m["uptime_seconds"])
	}
	routes, ok := m["routes"].([]any)
	if !ok || len(routes) != 1 {
		t.Fatalf("routes = %#v, want 1 entry", m["routes"])
	}
	r := routes[0].(map[string]any)
	if r["name"] != "api-v1" || r["match"] != "api.example.com/v1/*" || r["upstream"] != "catalog" {
		t.Errorf("route row = %#v", r)
	}
	if r["has_limiter"] != true || r["limiter_name"] != "ip-burst" {
		t.Errorf("route limiter fields = %#v", r)
	}
	if rps, ok := r["rps"].(float64); !ok || rps != 142.1 {
		t.Errorf("rps = %v", r["rps"])
	}
	pct := r["percentiles"].(map[string]any)
	if pct["ok"] != true {
		t.Errorf("percentiles.ok = %v", pct["ok"])
	}
	for key, want := range map[string]float64{"p50_ms": 12.2, "p95_ms": 23, "p99_ms": 41.7} {
		if got, ok := pct[key].(float64); !ok || got != want {
			t.Errorf("percentiles[%s] = %v, want %v", key, pct[key], want)
		}
	}
	pools := m["pools"].([]any)
	p0 := pools[0].(map[string]any)
	if p0["name"] != "catalog" {
		t.Errorf("pool name = %v", p0["name"])
	}
	targets := p0["targets"].([]any)
	t0 := targets[0].(map[string]any)
	if t0["state"] != "healthy" || t0["circuit"] != "closed" {
		t.Errorf("target state/circuit = %v/%v", t0["state"], t0["circuit"])
	}
	if _, hasEjected := t0["ejected_until"]; hasEjected {
		t.Errorf("healthy target must not carry ejected_until")
	}
	limiters := m["limiters"].([]any)
	l0 := limiters[0].(map[string]any)
	if l0["strategy"] != "token_bucket" || l0["key_type"] != "ip" {
		t.Errorf("limiter fields = %#v", l0)
	}
	if u, ok := l0["usage_fraction"].(float64); !ok || u != -1 {
		t.Errorf("usage_fraction = %v, want -1 (unknown)", l0["usage_fraction"])
	}
}

func TestStateEmDashWhenNoLatencyData(t *testing.T) {
	fp := newFake()
	fp.latOK = false
	srv := New(fp, Options{})
	req := httptest.NewRequest(http.MethodGet, "/admin/api/state", nil)
	req.Host = "127.0.0.1:8080"
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	r := decodeBody(t, rec)["routes"].([]any)[0].(map[string]any)
	pct := r["percentiles"].(map[string]any)
	if pct["ok"] != false {
		t.Fatalf("percentiles.ok = %v, want false", pct["ok"])
	}
	for _, key := range []string{"p50_ms", "p95_ms", "p99_ms"} {
		if _, present := pct[key]; present {
			t.Errorf("%s present without data; values must be omitted so UI shows an em dash", key)
		}
	}
}

func TestAuthEnforcedWhenTokenConfigured(t *testing.T) {
	const tok = "sekret"
	srv := New(newFake(), Options{AuthToken: tok, Dashboard: true})

	cases := []struct {
		path  string
		token string
		want  int
	}{
		{"/admin/api/state", "", http.StatusUnauthorized},
		{"/admin/api/state", "wrong", http.StatusUnauthorized},
		{"/admin/api/state", tok, http.StatusOK},
		{"/admin/api/metrics", "", http.StatusUnauthorized},
		{"/admin/api/metrics", tok, http.StatusOK},
		{"/admin/events", "", http.StatusUnauthorized},
		{"/admin/", "", http.StatusUnauthorized},
		{"/admin/", tok, http.StatusOK},
	}
	for _, c := range cases {
		got := get(t, srv, c.path, c.token).Code
		if got != c.want {
			t.Errorf("GET %s token=%q status = %d, want %d", c.path, c.token, got, c.want)
		}
	}

	rec := get(t, srv, "/admin/api/state", "")
	if www := rec.Header().Get("WWW-Authenticate"); www != `Bearer realm="gatewright-admin"` {
		t.Errorf("WWW-Authenticate = %q", www)
	}
	env := decodeBody(t, rec)
	errObj := env["error"].(map[string]any)
	if errObj["code"] != "AUTH001" {
		t.Errorf("error code = %v, want AUTH001", errObj["code"])
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("401 content-type = %q", ct)
	}
}

func TestNoTokenMeansOpenOnTrustedBind(t *testing.T) {
	srv := New(newFake(), Options{})
	for _, host := range []string{"127.0.0.1:8080", "localhost", "localhost:9000", "[::1]:80", "::1", "127.0.0.1"} {
		if code := getFrom(srv, "/admin/api/state", "", host).Code; code != http.StatusOK {
			t.Errorf("host %q: status = %d, want 200 without any header when no token configured", host, code)
		}
	}
}

func TestTokenlessAdminRejectsNonLoopbackHost(t *testing.T) {
	srv := New(newFake(), Options{})
	for _, host := range []string{
		"evil.example.com",
		"192.168.1.5:8080", // private but not loopback
		"[fe80::1]:8080",
		"127.0.0.1.evil.com",
		"2130706433", // decimal IP form of 127.0.0.1 must NOT pass the textual allowlist
		"",
	} {
		rec := getFrom(srv, "/admin/api/state", "", host)
		if rec.Code != http.StatusForbidden {
			t.Errorf("host %q: status = %d, want 403", host, rec.Code)
			continue
		}
		env := decodeBody(t, rec)
		errObj := env["error"].(map[string]any)
		if errObj["code"] != "AUTH002" {
			t.Errorf("host %q: error code = %v, want AUTH002", host, errObj["code"])
		}
		if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Errorf("host %q: 403 content-type = %q", host, ct)
		}
	}
}

func TestTokenConfiguredKeepsBearerCheckRegardlessOfHost(t *testing.T) {
	const tok = "sekret"
	srv := New(newFake(), Options{AuthToken: tok})

	// Spoofed Host with a valid token: allowed — bearer auth is the gate.
	if code := getFrom(srv, "/admin/api/state", tok, "evil.example.com").Code; code != http.StatusOK {
		t.Errorf("spoofed host + valid token: status = %d, want 200", code)
	}
	// Spoofed Host without a token: still the bearer failure (401 AUTH001),
	// never the loopback guard.
	rec := getFrom(srv, "/admin/api/state", "", "evil.example.com")
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("spoofed host + no token: status = %d, want 401", rec.Code)
	}
	env := decodeBody(t, rec)
	if code := env["error"].(map[string]any)["code"]; code != "AUTH001" {
		t.Errorf("error code = %v, want AUTH001 (bearer check owns token mode)", code)
	}
}

func post(h http.Handler, path, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, nil)
	req.Host = "127.0.0.1:8080"
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestReloadSuccessAndFailure(t *testing.T) {
	fp := newFake()
	srv := New(fp, Options{})

	rec := post(srv, "/admin/api/reload", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode reload response: %v", err)
	}
	if !body.OK {
		t.Errorf("ok = %v, want true", body.OK)
	}

	fp.mu.Lock()
	calls := fp.reloadCalls
	fp.mu.Unlock()
	if calls != 1 {
		t.Errorf("reload calls = %d, want 1", calls)
	}

	fp.reloadErr = errors.New("boom")
	rec = post(srv, "/admin/api/reload", "")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("failure status = %d, want 500", rec.Code)
	}
	env := decodeBody(t, rec)
	errObj := env["error"].(map[string]any)
	if errObj["code"] != "INT500" || errObj["message"] != "boom" {
		t.Errorf("error envelope = %#v", errObj)
	}
}

func TestReloadWrongMethodRejected(t *testing.T) {
	srv := New(newFake(), Options{})
	rec := get(t, srv, "/admin/api/reload", "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET reload status = %d, want 405", rec.Code)
	}
	if allow := rec.Header().Get("Allow"); !strings.Contains(allow, "POST") {
		t.Errorf("Allow = %q, want POST listed", allow)
	}
}

func TestMetricsPassthrough(t *testing.T) {
	fp := newFake()
	srv := New(fp, Options{})
	rec := get(t, srv, "/admin/api/metrics", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("content-type = %q", ct)
	}
	if rec.Body.String() != fp.metrics {
		t.Errorf("metrics body mismatch:\n%q\nwant\n%q", rec.Body.String(), fp.metrics)
	}
}

func TestDashboardAssetsServed(t *testing.T) {
	srv := New(newFake(), Options{Dashboard: true})

	rec := get(t, srv, "/admin/", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("/admin/ status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("index content-type = %q", ct)
	}
	if !strings.Contains(rec.Body.String(), "GATEWRIGHT") {
		t.Error("index.html missing GATEWRIGHT brand")
	}

	for name, wantCT := range map[string]string{
		"app.js":     "text/javascript",
		"styles.css": "text/css",
	} {
		rec := get(t, srv, "/admin/assets/"+name, "")
		if rec.Code != http.StatusOK {
			t.Errorf("%s status = %d", name, rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, wantCT) {
			t.Errorf("%s content-type = %q, want prefix %q", name, ct, wantCT)
		}
	}
	if len(get(t, srv, "/admin/assets/app.js", "").Body.Bytes()) == 0 {
		t.Error("app.js served empty")
	}
	if code := get(t, srv, "/admin/assets/nope.txt", "").Code; code != http.StatusNotFound {
		t.Errorf("unknown asset status = %d, want 404", code)
	}
	if code := get(t, srv, "/admin", "").Code; code != http.StatusMovedPermanently {
		t.Errorf("/admin redirect status = %d, want 301", code)
	}
}

func TestDashboardDisabledHidesUI(t *testing.T) {
	srv := New(newFake(), Options{})
	if code := get(t, srv, "/admin/", "").Code; code != http.StatusNotFound {
		t.Errorf("/admin/ status = %d, want 404 when Dashboard=false", code)
	}
	if code := get(t, srv, "/admin/assets/app.js", "").Code; code != http.StatusNotFound {
		t.Errorf("assets status = %d, want 404 when Dashboard=false", code)
	}
}

// readEvent scans one SSE frame ("event:"/"data:" lines then a blank line),
// skipping any earlier frames such as the initial retry hint.
func readEvent(r *bufio.Reader) (event, data string, err error) {
	for {
		line, rerr := r.ReadString('\n')
		if rerr != nil {
			return "", "", rerr
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			data += strings.TrimPrefix(line, "data:")
		case line == "":
			if event != "" || data != "" {
				return event, data, nil
			}
		}
	}
}

func TestSSEHelloThenState(t *testing.T) {
	srv := New(newFake(), Options{})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/admin/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q, want text/event-stream", ct)
	}

	type frame struct {
		event string
		data  string
		err   error
	}
	frames := make(chan frame, 4)
	reader := bufio.NewReader(resp.Body)
	go func() {
		for i := 0; i < 2; i++ {
			ev, data, err := readEvent(reader)
			frames <- frame{ev, data, err}
		}
	}()

	receive := func() frame {
		select {
		case f := <-frames:
			return f
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for SSE frame")
			return frame{}
		}
	}

	first := receive()
	if first.err != nil {
		t.Fatalf("read hello: %v", first.err)
	}
	if first.event != "hello" {
		t.Fatalf("first event = %q, want hello", first.event)
	}

	second := receive()
	if second.err != nil {
		t.Fatalf("read initial state: %v", second.err)
	}
	if second.event != "state" {
		t.Fatalf("second event = %q, want state", second.event)
	}
	var snap map[string]any
	if err := json.Unmarshal([]byte(second.data), &snap); err != nil {
		t.Fatalf("state frame not JSON (%q): %v", second.data, err)
	}
	if snap["version"] != "test-1" {
		t.Errorf("sse version = %v, want test-1", snap["version"])
	}
	if _, ok := snap["uptime_seconds"]; !ok {
		t.Error("sse snapshot missing uptime_seconds")
	}
}

func TestSSEStreamsUpdatesOverTime(t *testing.T) {
	srv := New(newFake(), Options{})
	ts := httptest.NewServer(srv)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/admin/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	if _, _, err := readEvent(reader); err != nil { // hello
		t.Fatal(err)
	}
	if ev, _, err := readEvent(reader); err != nil || ev != "state" { // immediate state
		t.Fatalf("immediate state event: %q %v", ev, err)
	}

	done := make(chan error, 1)
	go func() {
		for {
			ev, _, err := readEvent(reader)
			if err != nil {
				done <- err
				return
			}
			if ev == "state" {
				done <- nil
				return
			}
		}
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waiting for periodic state: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no periodic state event within 3s")
	}
}
