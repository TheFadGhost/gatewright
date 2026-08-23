// Package e2e drives the fully assembled gateway end to end: a real
// runtime.Supervisor built from real configuration files on disk, real HTTP
// upstreams, and clients connecting over TCP -- the same wiring cmd/gatewright
// performs, minus process supervision.
//
// Synchronization policy: no blind sleeps where an observable condition
// exists. Every wait polls observable state under a deadline; wall-clock
// waits appear only where the feature itself is time-based (the reload
// watcher poll interval, in-flight request durations).
package e2e

import (
	"bufio"
	"context"
	crand "crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gatewright/internal/config"
	_ "gatewright/internal/limiter/builtin" // registers limiter strategies for validation & engines
	"gatewright/internal/obs"
	"gatewright/internal/runtime"
	"gatewright/internal/store"
	"gatewright/internal/tlsutil"
)

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

const cfgHeader = `version: 1
server:
  listen: "127.0.0.1:0"
admin:
  listen: "127.0.0.1:0"
observability:
  metrics:
    enabled: false
`

type gateway struct {
	sup      *runtime.Supervisor
	srv      *http.Server
	url      string // http(s)://host:port
	hostPort string
	logPath  string
	cfgPath  string
	dir      string
	cancel   context.CancelFunc
	drained  bool
}

// startGateway builds a real Supervisor from cfgContent written to a temp
// file and serves sup.Handler() on 127.0.0.1:0. The structured logger writes
// JSON lines to a per-instance file the tests can inspect.
func startGateway(t *testing.T, cfgContent string) *gateway {
	t.Helper()
	return newGatewayOn(t, cfgContent, nil)
}

func newGatewayOn(t *testing.T, cfgContent string, wrapListener func(net.Listener) net.Listener) *gateway {
	t.Helper()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "gateway.yaml")
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	logPath := filepath.Join(dir, "access.log")
	logger, err := obs.New(obs.Options{Format: "json", Output: logPath})
	if err != nil {
		t.Fatalf("obs.New: %v", err)
	}
	// Release the logger's file handle so TempDir cleanup works on Windows,
	// even when NewSupervisor below fails validation.
	if f, ok := logger.Writer().(*os.File); ok {
		t.Cleanup(func() { _ = f.Close() })
	}

	sup, err := runtime.NewSupervisor(cfgPath, logger, obs.NewMetrics(), "e2e")
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serving := ln
	if wrapListener != nil {
		serving = wrapListener(ln)
	}

	g := &gateway{
		sup:      sup,
		srv:      &http.Server{Handler: sup.Handler(), ReadHeaderTimeout: 10 * time.Second},
		hostPort: ln.Addr().String(),
		logPath:  logPath,
		cfgPath:  cfgPath,
		dir:      dir,
	}
	g.url = "http://" + g.hostPort
	if wrapListener != nil {
		g.url = "https://" + g.hostPort
	}
	go func() { _ = g.srv.Serve(serving) }()

	t.Cleanup(g.stop)
	return g
}

func (g *gateway) stop() {
	if g.cancel != nil {
		g.cancel() // halt the reload watcher before tearing anything else down
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = g.srv.Shutdown(ctx)
	if !g.drained {
		g.sup.Drain(2 * time.Second)
		g.drained = true
	}
}

// watch enables the config-file poller exactly like `gatewright run`.
func (g *gateway) watch(interval time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	g.cancel = cancel
	g.sup.Watch(ctx, interval)
}

func (g *gateway) client() *http.Client {
	return &http.Client{Timeout: 20 * time.Second}
}

// rewriteConfig replaces the config file contents; callers must leave a gap
// between writes so the mtime the watcher polls actually changes.
func (g *gateway) rewriteConfig(content string) {
	if err := os.WriteFile(g.cfgPath, []byte(content), 0o600); err != nil {
		panic("rewrite config: " + err.Error())
	}
}

func startUpstream(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func upstreamTarget(url string) string {
	return fmt.Sprintf(`
upstreams:
  app:
    targets:
      - url: "%s"
        weight: 1`, url)
}

// waitFor polls cond until true or the deadline expires.
func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, desc)
}

func accessLines(t *testing.T, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []map[string]any
	for _, ln := range strings.Split(string(data), "\n") {
		ln = strings.TrimSpace(ln)
		if !strings.Contains(ln, `"msg":"access"`) {
			continue
		}
		var m map[string]any
		if json.Unmarshal([]byte(ln), &m) == nil {
			out = append(out, m)
		}
	}
	return out
}

func waitForAccessLine(t *testing.T, path string, desc string, pred func(map[string]any) bool) map[string]any {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range accessLines(t, path) {
			if pred(m) {
				return m
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no access log line matching %s", desc)
	return nil
}

func decodeJSONBody(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
	return m
}

func errorEnvelopeOf(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	m := decodeJSONBody(t, resp)
	env, _ := m["error"].(map[string]any)
	if env == nil {
		t.Fatalf("expected error envelope, got %v", m)
	}
	return env
}

// firstErrorEnvelope decodes only the FIRST JSON object of the response.
// Used where two stages can legitimately write sequentially (the body-limit
// stage denies with BODY001 before the forwarder appends its own transport
// outcome): the first envelope is the authoritative client-facing answer.
func firstErrorEnvelope(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()
	var outer struct {
		Error map[string]any `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&outer); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if outer.Error == nil {
		t.Fatal("expected error envelope as first JSON value")
	}
	return outer.Error
}

// hammer fires n requests concurrently and reports each response status.
func hammer(n int, do func(i int) int) []int {
	codes := make([]int, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i] = do(i)
		}(i)
	}
	wg.Wait()
	return codes
}

func countStatus(codes []int, status int) int {
	n := 0
	for _, c := range codes {
		if c == status {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// 1. Proxy end to end: forwarding, X-Forwarded-For append, request id,
//    JSON access log emission.
// ---------------------------------------------------------------------------

func TestProxyEndToEndForwardedHeadersAndAccessLog(t *testing.T) {
	up := startUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"xff":    r.Header.Get("X-Forwarded-For"),
			"req_id": r.Header.Get("X-Gatewright-Request-Id"),
			"fwd":    r.Header.Get("Forwarded"),
			"proto":  r.Header.Get("X-Forwarded-Proto"),
		})
	}))

	g := startGateway(t, cfgHeader+upstreamTarget(up.URL)+`
routes:
  - name: echo-route
    path_prefix: /v1
    upstreams: app
    timeout: 10s
`)

	req, err := http.NewRequest(http.MethodGet, g.url+"/v1/echo?q=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Forwarded-For", "203.0.113.7")
	resp, err := g.client().Do(req)
	if err != nil {
		t.Fatalf("through gateway: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body := decodeJSONBody(t, resp)

	// X-Forwarded-For: prior value kept, client IP appended, never replaced.
	if xff, _ := body["xff"].(string); !strings.HasPrefix(xff, "203.0.113.7, ") || !strings.Contains(xff, "127.0.0.1") {
		t.Errorf("X-Forwarded-For = %q, want 203.0.113.7 appended with client IP", xff)
	}
	// Request id generated by the gateway reached the upstream.
	reqID, _ := body["req_id"].(string)
	if !strings.HasPrefix(reqID, "gw-") || len(reqID) < 10 {
		t.Errorf("upstream saw request id %q, want gw-* correlation id", reqID)
	}
	// Forwarded (RFC 7239) element present.
	if fwd, _ := body["fwd"].(string); !strings.Contains(fwd, "for=") || !strings.Contains(fwd, "by=_gatewright") {
		t.Errorf("Forwarded = %q, want gatewright element", fwd)
	}

	line := waitForAccessLine(t, g.logPath, "status 200 /v1/echo", func(m map[string]any) bool {
		p, _ := m["path"].(string)
		s, _ := m["status"].(float64)
		return p == "/v1/echo" && int(s) == 200
	})
	if m, _ := line["method"].(string); m != "GET" {
		t.Errorf("access log method = %v, want GET", line["method"])
	}
	if id, _ := line["req_id"].(string); id != reqID {
		t.Errorf("access log req_id = %q, want %q (must correlate with upstream)", id, reqID)
	}
	if _, ok := line["duration_ms"]; !ok {
		t.Error("access log missing duration_ms field")
	}
	if q, _ := line["query"].(string); q != "q=1" {
		t.Errorf("access log query = %v, want q=1", line["query"])
	}
	if code, _ := line["code"].(string); code != "" {
		t.Errorf("access log code = %q, want empty on success", code)
	}
}

// ---------------------------------------------------------------------------
// 2. Rate limiting end to end: token_bucket 5/5 keyed by ip.
// ---------------------------------------------------------------------------

func TestRateLimitTokenBucketEndToEnd(t *testing.T) {
	up := startUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))

	g := startGateway(t, cfgHeader+upstreamTarget(up.URL)+`
routes:
  - name: ltd
    path_prefix: /ltd
    upstreams: app
    timeout: 10s
    rate_limits:
      - name: quota
        strategy: token_bucket
        key: ip
        limit: 5
        window: 60s
        burst: 5
`)

	client := g.client()
	wantRemaining := []string{"4", "3", "2", "1", "0"}
	for i := 0; i < 5; i++ {
		resp, err := client.Get(g.url + "/ltd/x")
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200 (bucket starts full)", i, resp.StatusCode)
		}
		resp.Body.Close()
		if got := resp.Header.Get("RateLimit-Remaining"); got != wantRemaining[i] {
			t.Errorf("request %d: RateLimit-Remaining = %q, want %q", i, got, wantRemaining[i])
		}
	}

	// Bucket empty: every further request is denied with RATE001.
	for i := 0; i < 3; i++ {
		resp, err := client.Get(g.url + "/ltd/x")
		if err != nil {
			t.Fatalf("denied request %d: %v", i, err)
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			t.Fatalf("denied request %d: status = %d, want 429", i, resp.StatusCode)
		}
		retryAfter := resp.Header.Get("Retry-After")
		n, perr := strconv.Atoi(retryAfter)
		if perr != nil || n < 1 {
			t.Errorf("Retry-After = %q (%v), want integer >= 1", retryAfter, perr)
		}
		if got := resp.Header.Get("RateLimit-Remaining"); got != "0" {
			t.Errorf("RateLimit-Remaining = %q, want 0 on denial", got)
		}
		env := errorEnvelopeOf(t, resp)
		if code, _ := env["code"].(string); code != "RATE001" {
			t.Errorf("envelope code = %v, want RATE001", env["code"])
		}
	}
}

// ---------------------------------------------------------------------------
// 3. Concurrency limiter end to end: capacity 2 with a blocked upstream;
//    peak occupancy tracked under a mutex.
// ---------------------------------------------------------------------------

type peakGauge struct {
	mu       sync.Mutex
	cur      int
	peak     int
	release  chan struct{}
	released bool
}

func newPeakGauge() *peakGauge { return &peakGauge{release: make(chan struct{})} }

func (p *peakGauge) enter() {
	p.mu.Lock()
	p.cur++
	if p.cur > p.peak {
		p.peak = p.cur
	}
	p.mu.Unlock()
	select {
	case <-p.release:
	case <-time.After(15 * time.Second):
	}
	p.mu.Lock()
	p.cur--
	p.mu.Unlock()
}

func (p *peakGauge) current() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cur
}

func (p *peakGauge) maxSeen() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.peak
}

func (p *peakGauge) letGo() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.released {
		p.released = true
		close(p.release)
	}
}

func TestConcurrencyLimiterEndToEnd(t *testing.T) {
	gauge := newPeakGauge()
	up := startUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gauge.enter()
		_, _ = w.Write([]byte("done"))
	}))

	g := startGateway(t, cfgHeader+upstreamTarget(up.URL)+`
routes:
  - name: capped
    path_prefix: /cap
    upstreams: app
    timeout: 30s
    rate_limits:
      - name: slots
        strategy: concurrency
        key: ip
        limit: 2
`)

	blocked := make(chan int, 4)
	// Warm the route chain with a single held request first: the runtime
	// lazily caches per-route middleware chains on first contact, so the
	// genuinely-parallel phase below must not race that initialization.
	go func() {
		resp, err := g.client().Get(g.url + "/cap/work")
		if err != nil {
			blocked <- -1
			return
		}
		resp.Body.Close()
		blocked <- resp.StatusCode
	}()
	waitFor(t, 10*time.Second, "first request held inside upstream", func() bool {
		return gauge.current() == 1
	})

	go func() {
		resp, err := g.client().Get(g.url + "/cap/work")
		if err != nil {
			blocked <- -1
			return
		}
		resp.Body.Close()
		blocked <- resp.StatusCode
	}()

	waitFor(t, 10*time.Second, "2 requests held inside upstream concurrently", func() bool {
		return gauge.current() == 2
	})

	// Capacity full: further requests must be denied without queueing.
	for i := 0; i < 2; i++ {
		resp, err := g.client().Get(g.url + "/cap/work")
		if err != nil {
			t.Fatalf("overflow request %d: %v", i, err)
		}
		env := errorEnvelopeOf(t, resp)
		resp.Body.Close()
		if resp.StatusCode != http.StatusTooManyRequests {
			t.Errorf("overflow request %d: status = %d, want 429", i, resp.StatusCode)
		}
		if code, _ := env["code"].(string); code != "RATE001" {
			t.Errorf("overflow envelope code = %v, want RATE001", env["code"])
		}
	}

	if peak := gauge.maxSeen(); peak > 2 {
		t.Errorf("peak concurrent requests in upstream = %d, want <= capacity 2", peak)
	}

	gauge.letGo()
	for i := 0; i < 2; i++ {
		select {
		case code := <-blocked:
			if code != http.StatusOK {
				t.Errorf("admitted request %d: status = %d, want 200", i, code)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("admitted request %d never completed", i)
		}
	}
	if peak := gauge.maxSeen(); peak != 2 {
		t.Errorf("final peak = %d, want exactly 2", peak)
	}
}

// ---------------------------------------------------------------------------
// 4. Routing precedence through the full stack: static beats param even when
//    the param route is declared first.
// ---------------------------------------------------------------------------

func TestRoutingPrecedenceStaticBeatsParamEndToEnd(t *testing.T) {
	paramUp := startUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("param"))
	}))
	staticUp := startUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("static"))
	}))

	cfg := cfgHeader + fmt.Sprintf(`
upstreams:
  pparam:
    targets:
      - url: "%s"
        weight: 1
  pstatic:
    targets:
      - url: "%s"
        weight: 1
routes:
  - name: wildcard
    path_pattern: /svc/{id}
    upstreams: pparam
    timeout: 10s
  - name: exact
    path_pattern: /svc/static
    upstreams: pstatic
    timeout: 10s
`, paramUp.URL, staticUp.URL)

	g := startGateway(t, cfg)
	client := g.client()

	get := func(path string) string {
		resp, err := client.Get(g.url + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status = %d body %q", path, resp.StatusCode, body)
		}
		return string(body)
	}

	// Deterministic winner despite config order favouring the param route.
	for i := 0; i < 3; i++ {
		if got := get("/svc/static"); got != "static" {
			t.Errorf("iteration %d: /svc/static routed to %q winner, want static route", i, got)
		}
		if got := get("/svc/other"); got != "param" {
			t.Errorf("iteration %d: /svc/other routed to %q winner, want param route", i, got)
		}
	}
}

// ---------------------------------------------------------------------------
// 5. API key auth end to end: keys_env, 401 AUTH001 envelope on
//    missing/wrong credentials, passthrough on correct key.
// ---------------------------------------------------------------------------

func TestAPIKeyAuthEndToEnd(t *testing.T) {
	const (
		keyEnvName = "GW_E2E_API_KEY"
		keyValue   = "e2e-secret-key-123"
	)
	t.Setenv(keyEnvName, keyValue)

	up := startUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("secured-payload"))
	}))

	g := startGateway(t, cfgHeader+upstreamTarget(up.URL)+`
routes:
  - name: secured
    path_prefix: /sec
    upstreams: app
    timeout: 10s
    auth:
      type: api_key
      api_key:
        header: X-API-Key
        keys_env: `+keyEnvName)

	client := g.client()

	resp, err := client.Get(g.url + "/sec/res")
	if err != nil {
		t.Fatalf("missing key request: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("missing key: status = %d, want 401", resp.StatusCode)
	}
	env := errorEnvelopeOf(t, resp)
	if code, _ := env["code"].(string); code != "AUTH001" {
		t.Errorf("missing key: envelope code = %v, want AUTH001", env["code"])
	}
	if ch := resp.Header.Get("WWW-Authenticate"); !strings.HasPrefix(ch, "ApiKey") {
		t.Errorf("WWW-Authenticate = %q, want ApiKey challenge", ch)
	}

	reqWrong, _ := http.NewRequest(http.MethodGet, g.url+"/sec/res", nil)
	reqWrong.Header.Set("X-API-Key", "totally-wrong")
	respWrong, err := client.Do(reqWrong)
	if err != nil {
		t.Fatalf("wrong key request: %v", err)
	}
	if respWrong.StatusCode != http.StatusUnauthorized {
		t.Errorf("wrong key: status = %d, want 401", respWrong.StatusCode)
	}
	envWrong := errorEnvelopeOf(t, respWrong)
	if code, _ := envWrong["code"].(string); code != "AUTH001" {
		t.Errorf("wrong key: envelope code = %v, want AUTH001", envWrong["code"])
	}

	reqOK, _ := http.NewRequest(http.MethodGet, g.url+"/sec/res", nil)
	reqOK.Header.Set("X-API-Key", keyValue)
	respOK, err := client.Do(reqOK)
	if err != nil {
		t.Fatalf("valid key request: %v", err)
	}
	defer respOK.Body.Close()
	body, _ := io.ReadAll(respOK.Body)
	if respOK.StatusCode != http.StatusOK || string(body) != "secured-payload" {
		t.Errorf("valid key: status = %d body = %q, want 200 secured-payload", respOK.StatusCode, body)
	}
}

// ---------------------------------------------------------------------------
// 6. CORS preflight through the full stack: answered before auth even when
//    the route requires an API key.
// ---------------------------------------------------------------------------

func TestCORSPreflightBypassesAuthEndToEnd(t *testing.T) {
	const keyEnvName = "GW_E2E_CORS_KEY"
	t.Setenv(keyEnvName, "cors-secret")

	up := startUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("cors-data"))
	}))

	g := startGateway(t, cfgHeader+upstreamTarget(up.URL)+`
routes:
  - name: corssed
    path_prefix: /cors
    upstreams: app
    timeout: 10s
    cors:
      allowed_origins: ["https://app.example.com"]
      allowed_methods: [GET, POST]
    auth:
      type: api_key
      api_key:
        header: X-API-Key
        keys_env: `+keyEnvName)

	client := g.client()

	preflight, err := http.NewRequest(http.MethodOptions, g.url+"/cors/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	preflight.Header.Set("Origin", "https://app.example.com")
	preflight.Header.Set("Access-Control-Request-Method", "POST")
	resp, err := client.Do(preflight)
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("preflight status = %d, want 204", resp.StatusCode)
	}
	if ao := resp.Header.Get("Access-Control-Allow-Origin"); ao != "https://app.example.com" {
		t.Errorf("Access-Control-Allow-Origin = %q, want echoed origin", ao)
	}
	if am := resp.Header.Get("Access-Control-Allow-Methods"); !strings.Contains(am, "POST") {
		t.Errorf("Access-Control-Allow-Methods = %q, want to include POST", am)
	}
	if ch := resp.Header.Get("WWW-Authenticate"); ch != "" {
		t.Errorf("preflight hit auth stage: WWW-Authenticate = %q", ch)
	}

	// Contrast: a real request on the same route still needs the key, so the
	// preflight short-circuit above is meaningful rather than vacuous.
	bare, err := client.Get(g.url + "/cors/x")
	if err != nil {
		t.Fatalf("bare GET: %v", err)
	}
	if bare.StatusCode != http.StatusUnauthorized {
		t.Errorf("bare GET without key: status = %d, want 401 (auth still enforced)", bare.StatusCode)
	} else {
		bare.Body.Close()
	}
}

// ---------------------------------------------------------------------------
// 7. Body limit end to end: streamed (chunked) body over the limit answers
//    413 BODY001.
// ---------------------------------------------------------------------------

func TestBodyLimitStreamedBodyEndToEnd(t *testing.T) {
	up := startUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // drain whatever arrives, then 200
		_, _ = w.Write([]byte("received"))
	}))

	g := startGateway(t, cfgHeader+upstreamTarget(up.URL)+`
routes:
  - name: upload
    path_prefix: /up
    upstreams: app
    timeout: 10s
    body_limit: "64B"
`)

	payload := strings.Repeat("a", 200)
	req, err := http.NewRequest(http.MethodPost, g.url+"/up/blob", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = -1 // force chunked transfer: no declared length

	resp, err := g.client().Do(req)
	if err != nil {
		t.Fatalf("streamed upload: %v", err)
	}
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", resp.StatusCode)
	}
	env := firstErrorEnvelope(t, resp)
	if code, _ := env["code"].(string); code != "BODY001" {
		t.Errorf("envelope code = %v, want BODY001", env["code"])
	}
}

// ---------------------------------------------------------------------------
// 8. WebSocket passthrough end to end: raw TCP client, handshake through the
//    gateway, one masked text frame echoed back by an RFC6455 upstream.
// ---------------------------------------------------------------------------

// wsEchoHandler replicates cmd/gatewright demo.go's /ws endpoint (that file
// is package main, so the tiny logic is copied here): minimal RFC6455 accept
// plus frame echo.
func wsEchoHandler(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Sec-WebSocket-Key")
	if upgrade := strings.ToLower(r.Header.Get("Upgrade")); upgrade != "websocket" || key == "" {
		http.Error(w, "expected websocket upgrade", http.StatusBadRequest)
		return
	}
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijack unsupported", http.StatusInternalServerError)
		return
	}
	conn, buf, err := hj.Hijack()
	if err != nil {
		return
	}
	defer conn.Close()

	h := sha1.New()
	_, _ = h.Write([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	accept := base64.StdEncoding.EncodeToString(h.Sum(nil))
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := conn.Write([]byte(resp)); err != nil {
		return
	}
	for {
		frame, op, err := wsReadFrame(buf.Reader)
		if err != nil {
			return
		}
		if op == 8 { // close: echo it and hang up
			_ = wsWriteFrame(conn, op, frame, false)
			return
		}
		if err := wsWriteFrame(conn, op, frame, false); err != nil {
			return
		}
	}
}

func wsReadFrame(r io.Reader) ([]byte, byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, 0, err
	}
	op := header[0] & 0x0F
	masked := header[1]&0x80 != 0
	plen := uint64(header[1] & 0x7F)
	switch plen {
	case 126:
		ext := make([]byte, 2)
		if _, err := io.ReadFull(r, ext); err != nil {
			return nil, 0, err
		}
		plen = uint64(binary.BigEndian.Uint16(ext))
	case 127:
		ext := make([]byte, 8)
		if _, err := io.ReadFull(r, ext); err != nil {
			return nil, 0, err
		}
		plen = binary.BigEndian.Uint64(ext)
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(r, mask[:]); err != nil {
			return nil, 0, err
		}
	}
	payload := make([]byte, plen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, 0, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return payload, op, nil
}

func wsWriteFrame(w io.Writer, op byte, payload []byte, masked bool) error {
	var mask [4]byte
	if masked {
		if _, err := crand.Read(mask[:]); err != nil {
			return err
		}
	}
	out := make([]byte, 0, len(payload)+14)
	out = append(out, 0x80|op)
	l := len(payload)
	maskBit := byte(0)
	if masked {
		maskBit = 0x80
	}
	switch {
	case l < 126:
		out = append(out, byte(l)|maskBit)
	case l < 65536:
		out = append(out, 126|maskBit, byte(l>>8), byte(l))
	default:
		ext := make([]byte, 8)
		binary.BigEndian.PutUint64(ext, uint64(l))
		out = append(out, 127|maskBit)
		out = append(out, ext...)
	}
	if masked {
		out = append(out, mask[:]...)
		maskedPayload := make([]byte, len(payload))
		for i := range payload {
			maskedPayload[i] = payload[i] ^ mask[i%4]
		}
		out = append(out, maskedPayload...)
	} else {
		out = append(out, payload...)
	}
	_, err := w.Write(out)
	return err
}

func TestWebsocketPassthroughEndToEnd(t *testing.T) {
	up := startUpstream(t, http.HandlerFunc(wsEchoHandler))

	g := startGateway(t, cfgHeader+upstreamTarget(up.URL)+`
routes:
  - name: wsroute
    path_prefix: /ws
    upstreams: app
    timeout: 30s
`)

	conn, err := net.DialTimeout("tcp", g.hostPort, 5*time.Second)
	if err != nil {
		t.Fatalf("dial gateway: %v", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}

	nonce := make([]byte, 16)
	if _, err := crand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	wsKey := base64.StdEncoding.EncodeToString(nonce)
	handshake := "GET /ws/room HTTP/1.1\r\n" +
		fmt.Sprintf("Host: %s\r\n", g.hostPort) +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + wsKey + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(handshake)); err != nil {
		t.Fatalf("send handshake through gateway: %v", err)
	}

	br := bufio.NewReader(conn)
	statusLine, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("read upgrade response: %v", err)
	}
	if !strings.Contains(statusLine, "101") {
		t.Fatalf("upgrade status line = %q, want 101 Switching Protocols", strings.TrimSpace(statusLine))
	}

	acceptHeader := ""
	for {
		line, rerr := br.ReadString('\n')
		if rerr != nil || line == "\r\n" {
			break
		}
		name, value, found := strings.Cut(line, ":")
		if found && strings.EqualFold(name, "Sec-WebSocket-Accept") {
			acceptHeader = strings.TrimSpace(value)
		}
	}
	sum := sha1.Sum([]byte(wsKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	wantAccept := base64.StdEncoding.EncodeToString(sum[:])
	if acceptHeader != wantAccept {
		t.Errorf("Sec-WebSocket-Accept = %q, want %q (RFC6455 accept must pass through unmodified)", acceptHeader, wantAccept)
	}

	message := []byte("hello-gatewright-e2e")
	if err := wsWriteFrame(conn, 1, message, true); err != nil {
		t.Fatalf("send masked text frame: %v", err)
	}
	echo, op, err := wsReadFrame(br)
	if err != nil {
		t.Fatalf("read echo frame: %v", err)
	}
	if op != 1 {
		t.Errorf("echo opcode = %d, want 1 (text)", op)
	}
	if string(echo) != string(message) {
		t.Errorf("echo payload = %q, want %q", echo, message)
	}

	_ = wsWriteFrame(conn, 8, nil, true) // close frame; best effort
}

// ---------------------------------------------------------------------------
// 9. Hot reload with in-flight completion plus limiter state carry-over.
//
//    Phase A: a slow in-flight request must complete 200 on the OLD
//    generation while the watcher swaps in a new one that adds a route.
//    Phase B: quota consumed before another reload (limiter settings
//    unchanged) must survive into the new generation -- no fresh quota.
//
//    The bbolt-free memory driver carries state only when the settings hash
//    is unchanged, which is exactly what phase B asserts behaviourally.
// ---------------------------------------------------------------------------

func TestHotReloadInFlightAndLimiterCarryOver(t *testing.T) {
	// Catch-all upstream: the forwarder passes the full incoming path through
	// (no strip_prefix), so every route below must resolve here regardless.
	up := startUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/slow") {
			ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
			if ms <= 0 {
				ms = 800
			}
			time.Sleep(time.Duration(ms) * time.Millisecond)
			_, _ = w.Write([]byte("slow-done"))
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))

	base := cfgHeader + upstreamTarget(up.URL) + `
routes:
  - name: base
    path_prefix: /v1
    upstreams: app
    timeout: 10s
  - name: slower
    path_prefix: /slow
    upstreams: app
    timeout: 10s
  - name: ltd
    path_prefix: /ltd
    upstreams: app
    timeout: 10s
    rate_limits:
      - name: quota
        strategy: token_bucket
        key: ip
        limit: 5
        window: 60s
        burst: 5
`

	g := startGateway(t, base)
	g.watch(40 * time.Millisecond)
	client := g.client()

	// --- Phase A: reload while a request is in flight ----------------------
	slowDone := make(chan int, 1)
	go func() {
		resp, err := client.Get(g.url + "/slow/sleep?ms=800")
		if err != nil {
			slowDone <- -1
			return
		}
		resp.Body.Close()
		slowDone <- resp.StatusCode
	}()

	time.Sleep(150 * time.Millisecond) // let the slow request get in flight

	time.Sleep(75 * time.Millisecond) // guarantee a distinct mtime for the watcher
	v2Config := cfgHeader + upstreamTarget(up.URL) + `
routes:
  - name: base
    path_prefix: /v1
    upstreams: app
    timeout: 10s
  - name: slower
    path_prefix: /slow
    upstreams: app
    timeout: 10s
  - name: ltd
    path_prefix: /ltd
    upstreams: app
    timeout: 10s
    rate_limits:
      - name: quota
        strategy: token_bucket
        key: ip
        limit: 5
        window: 60s
        burst: 5
  - name: fresh
    path_prefix: /new
    upstreams: app
    timeout: 10s
    rate_limits:
      - name: newburst
        strategy: token_bucket
        key: ip
        limit: 3
        window: 60s
        burst: 4
`
	g.rewriteConfig(v2Config)
	waitFor(t, 10*time.Second, "new route /new/base to appear after hot reload", func() bool {
		resp, err := client.Get(g.url + "/new/base")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	select {
	case code := <-slowDone:
		if code != http.StatusOK {
			t.Fatalf("in-flight request across reload: status = %d, want 200 on old generation", code)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("in-flight request never completed after reload")
	}

	// --- Phase B: limiter state carry-over across an unchanged reload ------
	for i := 0; i < 3; i++ {
		resp, err := client.Get(g.url + "/ltd/consume")
		if err != nil {
			t.Fatalf("pre-reload consume %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("pre-reload consume %d: status = %d, want 200", i, resp.StatusCode)
		}
	}

	time.Sleep(75 * time.Millisecond) // distinct mtime again
	v3Config := v2Config + `  - name: marker
    path_prefix: /marker
    upstreams: app
    timeout: 10s
`
	g.rewriteConfig(v3Config)
	waitFor(t, 10*time.Second, "marker route to appear (proving the unchanged-settings reload happened)", func() bool {
		resp, err := client.Get(g.url + "/marker/base")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	for i := 0; i < 2; i++ {
		resp, err := client.Get(g.url + "/ltd/after")
		if err != nil {
			t.Fatalf("post-reload admit %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("post-reload admit %d: status = %d, want 200 (quota carried over)", i, resp.StatusCode)
		}
	}
	respDeny, err := client.Get(g.url + "/ltd/after")
	if err != nil {
		t.Fatalf("post-reload deny: %v", err)
	}
	respDeny.Body.Close()
	if respDeny.StatusCode != http.StatusTooManyRequests {
		t.Errorf("6th request after reload: status = %d, want 429 (no fresh quota granted by reload)", respDeny.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// 10. Graceful shutdown: Drain + server.Shutdown concurrently while a
//     long-poll request is in flight.
// ---------------------------------------------------------------------------

func TestGracefulShutdownDrainsInFlight(t *testing.T) {
	release := make(chan struct{})
	var inUpstream atomic.Int64
	up := startUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inUpstream.Add(1)
		select {
		case <-release:
		case <-time.After(15 * time.Second):
		}
		inUpstream.Add(-1)
		_, _ = w.Write([]byte("long-poll-done"))
	}))

	g := startGateway(t, cfgHeader+upstreamTarget(up.URL)+`
routes:
  - name: poll
    path_prefix: /v1
    upstreams: app
    timeout: 30s
`)

	pollResult := make(chan int, 1)
	go func() {
		resp, err := g.client().Get(g.url + "/v1/wait")
		if err != nil {
			pollResult <- -1
			return
		}
		resp.Body.Close()
		pollResult <- resp.StatusCode
	}()

	waitFor(t, 10*time.Second, "long-poll request to reach the upstream", func() bool {
		return inUpstream.Load() == 1
	})

	drainDone := make(chan struct{})
	go func() {
		g.sup.Drain(2 * time.Second)
		close(drainDone)
	}()
	shutdownErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownErr <- g.srv.Shutdown(ctx)
	}()

	time.Sleep(150 * time.Millisecond) // let both teardown paths settle into waiting

	close(release)

	select {
	case code := <-pollResult:
		if code != http.StatusOK {
			t.Fatalf("in-flight request during drain: status = %d, want 200", code)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("in-flight request never completed during drain")
	}

	select {
	case err := <-shutdownErr:
		if err != nil {
			t.Errorf("server.Shutdown: %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Error("server.Shutdown did not return after the in-flight request completed")
	}
	select {
	case <-drainDone:
	case <-time.After(3 * time.Second):
		t.Error("sup.Drain did not return after the in-flight request completed")
	}

	g.drained = true // cleanup must not run a second Drain

	// The process has stopped serving: new connections must fail.
	respNew, err := (&http.Client{Timeout: 3 * time.Second}).Get(g.url + "/v1/after-shutdown")
	if err == nil {
		respNew.Body.Close()
		t.Fatalf("request after Shutdown unexpectedly succeeded with status %d", respNew.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// 11. TLS termination end to end: generated self-signed cert, TLS 1.2 floor
//     enforced against an old-protocol client.
// ---------------------------------------------------------------------------

func TestTLSTerminationAndMinVersionEndToEnd(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath, err := tlsutil.GenerateSelfSignedCert(dir, []string{"localhost", "127.0.0.1"})
	if err != nil {
		t.Fatalf("GenerateSelfSignedCert: %v", err)
	}
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	serverCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("X509KeyPair: %v", err)
	}
	rootPool := x509.NewCertPool()
	if !rootPool.AppendCertsFromPEM(certPEM) {
		t.Fatal("could not parse generated certificate PEM")
	}

	up := startUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"proto": r.Header.Get("X-Forwarded-Proto"),
		})
	}))

	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS12,
	}
	g := newGatewayOn(t, cfgHeader+upstreamTarget(up.URL)+`
routes:
  - name: echo-route
    path_prefix: /v1
    upstreams: app
    timeout: 10s
`, func(l net.Listener) net.Listener { return tls.NewListener(l, tlsConf) })

	clientOK := &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    rootPool,
				ServerName: "localhost",
				MinVersion: tls.VersionTLS12,
			},
		},
	}
	resp, err := clientOK.Get(g.url + "/v1/echo")
	if err != nil {
		t.Fatalf("TLS request through gateway: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("TLS request status = %d, want 200", resp.StatusCode)
	}
	body := decodeJSONBody(t, resp)
	if proto, _ := body["proto"].(string); proto != "https" {
		t.Errorf("upstream saw X-Forwarded-Proto = %q, want https (TLS termination)", proto)
	}
	if resp.TLS == nil || resp.TLS.Version < tls.VersionTLS12 {
		t.Errorf("negotiated version below TLS 1.2: %v", resp.TLS)
	}

	clientOld := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    rootPool,
				ServerName: "localhost",
				MaxVersion: tls.VersionTLS11,
				MinVersion: tls.VersionTLS10,
			},
		},
	}
	if _, err := clientOld.Get(g.url + "/v1/echo"); err == nil {
		t.Error("client forcing TLS 1.1 completed a handshake; server min_version was not enforced")
	}
}

// ---------------------------------------------------------------------------
// 12. Shared store across two supervisor instances: aggregate quota persists
//     in the bbolt file, so instance B continues exactly where instance A
//     stopped -- no fresh quota.
//
//     The store takes an exclusive writer file lock for the lifetime of a
//     handle by design (internal/store doc), so two live instances cannot
//     hold one file simultaneously; the test therefore hands over A -> B and
//     asserts exact aggregate admission across both.
// ---------------------------------------------------------------------------

func TestSharedStoreAcrossSupervisorInstances(t *testing.T) {
	storeDir := t.TempDir()
	storePath := filepath.Join(storeDir, "shared.bolt")

	up := startUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("shared-ok"))
	}))
	cfg := cfgHeader + upstreamTarget(up.URL) + fmt.Sprintf(`
store:
  path: '%s'
routes:
  - name: pooled
    path_prefix: /pool
    upstreams: app
    timeout: 10s
    rate_limits:
      - name: global
        strategy: token_bucket
        key: ip
        limit: 10
        window: 60s
        burst: 10
        backend: shared
`, storePath)

	doRequest := func(g *gateway, i int) int {
		req, _ := http.NewRequest(http.MethodGet, g.url+fmt.Sprintf("/pool/req%d?instance=hammer", i), nil)
		resp, err := g.client().Do(req)
		if err != nil {
			return -1
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	// --- Instance A: warm the route chain single-flight (the runtime caches
	// per-route chains lazily), then hammer 29 concurrent requests ----------
	gA := startGateway(t, cfg)
	if code := doRequest(gA, 999); code != http.StatusOK {
		t.Fatalf("instance A warm-up request: status = %d, want 200", code)
	}
	warmAdmitted := 1
	codesA := hammer(29, func(i int) int { return doRequest(gA, i) })
	admittedA := countStatus(codesA, http.StatusOK)
	if admittedA != 9 {
		t.Errorf("instance A hammer admitted = %d (+1 warm-up = 10 total), want 9", admittedA)
	}
	if deniedA := countStatus(codesA, http.StatusTooManyRequests); deniedA != 20 {
		t.Errorf("instance A denied = %d, want 20", deniedA)
	}

	// Hand over: close A's store handle, then wait until the exclusive file
	// lock is actually released (observable via a probe open).
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = gA.srv.Shutdown(shutdownCtx)
	cancel()
	gA.sup.Drain(2 * time.Second)
	gA.drained = true

	waitFor(t, 20*time.Second, "bbolt file lock release after draining instance A", func() bool {
		db, err := store.Open(storePath)
		if err != nil {
			return false
		}
		_ = db.Close()
		return true
	})

	// --- Instance B: same shared state, nothing left -----------------------
	gB := startGateway(t, cfg)
	// Warm the chain single-flight as well (quota already spent, so the
	// expected answer is 429 -- the chain caches before the limiter runs).
	if code := doRequest(gB, 999); code != http.StatusTooManyRequests {
		t.Fatalf("instance B warm-up request: status = %d, want 429", code)
	}
	codesB := hammer(30, func(i int) int { return doRequest(gB, i) })
	admittedB := countStatus(codesB, http.StatusOK)
	if admittedB != 0 {
		t.Errorf("instance B admitted = %d, want 0 (aggregate quota already consumed)", admittedB)
	}

	if total := admittedA + admittedB + warmAdmitted; total != 10 {
		t.Errorf("aggregate admitted across instances = %d, want exactly 10", total)
	}
}

// ---------------------------------------------------------------------------
// 13. Configuration validation errors surface the exact path and the unit
//     rule for bare integer durations.
// ---------------------------------------------------------------------------

func TestValidateRejectsBareIntegerDurationWithExactPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	content := cfgHeader + upstreamTarget("http://127.0.0.1:9001") + `
routes:
  - name: broken
    path_prefix: /b
    upstreams: app
    timeout: 10s
    rate_limits:
      - name: quota
        strategy: fixed_window
        key: ip
        limit: 5
        window: 30
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, verr := config.Load(path)
	if verr == nil {
		t.Fatal("config.Load succeeded on a bare integer duration; strict unit validation is broken")
	}
	if cfg != nil {
		t.Error("config.Load returned a Config alongside validation errors, want nil config")
	}
	msg := verr.Error()
	for _, want := range []string{"routes[0]", "window", "duration string with unit suffix", "bare integers"} {
		if !strings.Contains(msg, want) {
			t.Errorf("validation error missing %q:\n%s", want, msg)
		}
	}
	if !strings.Contains(msg, path) && !strings.Contains(msg, filepath.Base(path)) {
		t.Errorf("validation error does not name the offending file %q:\n%s", path, msg)
	}
}
