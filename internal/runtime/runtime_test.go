package runtime

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gatewright/internal/config"
	"gatewright/internal/limiter"
	_ "gatewright/internal/limiter/builtin" // registers strategies for validation & engines
	"gatewright/internal/obs"
)

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

const baseConfigTmpl = `version: 1
server:
  listen: "127.0.0.1:0"
admin:
  listen: "127.0.0.1:0"
upstreams:
  api:
    targets:
      - url: %q
        weight: 1
routes:
  - name: r1
    path_prefix: /api
    upstreams: api
`

// unhealthyConfigTmpl ejects the single target after one passive failure so a
// dead upstream flips pool health deterministically.
const unhealthyConfigTmpl = `version: 1
server:
  listen: "127.0.0.1:0"
admin:
  listen: "127.0.0.1:0"
upstreams:
  api:
    targets:
      - url: %q
        weight: 1
    health_check:
      passive:
        window: 60s
        failures: 1
        ejection_time: 30s
routes:
  - name: r1
    path_prefix: /api
    upstreams: api
`

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func newTestSupervisor(t *testing.T, cfgContent string) (*Supervisor, *obs.Metrics, string) {
	t.Helper()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "gateway.yaml")
	writeFile(t, cfgPath, cfgContent)

	logPath := filepath.Join(dir, "gateway.log")
	logger, err := obs.New(obs.Options{Format: "json", Output: logPath})
	if err != nil {
		t.Fatalf("obs.New: %v", err)
	}
	if f, ok := logger.Writer().(*os.File); ok {
		t.Cleanup(func() { _ = f.Close() })
	}

	metrics := obs.NewMetrics()
	sup, err := NewSupervisor(cfgPath, logger, metrics, "test")
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	return sup, metrics, cfgPath
}

func serveGet(h http.Handler, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	h.ServeHTTP(rec, req)
	return rec
}

// ---------------------------------------------------------------------------
// Reload: invalid candidates never touch the running generation.
// ---------------------------------------------------------------------------

func TestReloadRejectsInvalidCandidateKeepsOldGenerationServing(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Upstream", "v1")
		_, _ = w.Write([]byte("ok"))
	}))
	defer up.Close()

	goodCfg := fmt.Sprintf(baseConfigTmpl, up.URL)
	sup, metrics, cfgPath := newTestSupervisor(t, goodCfg)
	handler := sup.Handler()

	if rec := serveGet(handler, "http://gateway/api/ping"); rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("initial generation status=%d body=%q", rec.Code, rec.Body.String())
	}

	// Corrupt the candidate with an unknown top-level key (CFG001).
	brokenCfg := goodCfg + "totally_bogus_key: true\n"
	writeFile(t, cfgPath, brokenCfg)

	err := sup.Reload()
	if err == nil {
		t.Fatal("Reload must fail for an invalid candidate")
	}
	var verr *config.ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("Reload error type = %T, want *config.ValidationError", err)
	}
	if !strings.Contains(verr.Error(), "totally_bogus_key") {
		t.Errorf("validation detail missing unknown key:\n%s", verr.Error())
	}

	// The old generation must still be serving through Handler().
	if rec := serveGet(handler, "http://gateway/api/ping"); rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("old generation broken after rejected reload: status=%d body=%q", rec.Code, rec.Body.String())
	}

	// The rejection is counted in metrics (startup already counted one
	// successful "applied" sample inside NewSupervisor).
	render := metrics.Render()
	if !strings.Contains(render, `gatewright_reloads_total{outcome="rejected"} 1`) {
		t.Errorf("rejected reload not counted:\n%s", render)
	}
	if !strings.Contains(render, `gatewright_reloads_total{outcome="applied"}`) {
		t.Errorf("applied reload series missing:\n%s", render)
	}

	// Repairing the file makes the very same entry point succeed again.
	writeFile(t, cfgPath, goodCfg)
	if err := sup.Reload(); err != nil {
		t.Fatalf("reload after repair: %v", err)
	}
	if rec := serveGet(handler, "http://gateway/api/ping"); rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("new generation status=%d body=%q", rec.Code, rec.Body.String())
	}
}

func TestReloadRejectsUnparseableYAML(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer up.Close()

	sup, _, cfgPath := newTestSupervisor(t, fmt.Sprintf(baseConfigTmpl, up.URL))
	writeFile(t, cfgPath, "version: 1\nupstreams: [unclosed\n")
	if err := sup.Reload(); err == nil {
		t.Fatal("Reload must fail on syntactically invalid YAML")
	}
	if rec := serveGet(sup.Handler(), "http://gateway/api/ping"); rec.Code != http.StatusOK {
		t.Fatalf("old generation serving = %d after failed reload", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// UnhealthyPools: pools with zero healthy targets are reported for /readyz.
// ---------------------------------------------------------------------------

func TestUnhealthyPoolsReportsPoolWithZeroHealthyTargets(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	deadURL := up.URL
	up.Close() // nothing is listening there anymore

	sup, _, _ := newTestSupervisor(t, fmt.Sprintf(unhealthyConfigTmpl, deadURL))

	if pools := sup.UnhealthyPools(); len(pools) != 0 {
		t.Fatalf("fresh gateway reported unhealthy pools: %v", pools)
	}

	rec := serveGet(sup.Handler(), "http://gateway/api/x")
	if rec.Code < 500 || rec.Code > 599 {
		t.Fatalf("request to dead upstream status = %d, want 5xx", rec.Code)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		pools := sup.UnhealthyPools()
		if len(pools) == 1 && pools[0] == "api" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("UnhealthyPools = %v within deadline, want [api]", pools)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestUnhealthyPoolsEmptyWhileTargetsHealthy(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer up.Close()

	sup, _, _ := newTestSupervisor(t, fmt.Sprintf(unhealthyConfigTmpl, up.URL))

	if rec := serveGet(sup.Handler(), "http://gateway/api/y"); rec.Code != http.StatusOK {
		t.Fatalf("healthy upstream request status = %d, want 200", rec.Code)
	}
	if pools := sup.UnhealthyPools(); len(pools) != 0 {
		t.Errorf("UnhealthyPools = %v, want empty while the only target serves 200", pools)
	}
}

// ---------------------------------------------------------------------------
// settingsHash: identity of limiter settings across reloads.
// ---------------------------------------------------------------------------

func TestSettingsHashStabilityAndSensitivity(t *testing.T) {
	base := limiter.Settings{Limit: 100, Window: 30 * time.Second}
	h1 := settingsHash("fixed_window", base)
	h2 := settingsHash("fixed_window", base)
	if h1 != h2 {
		t.Fatal("identical settings must hash identically")
	}

	diffs := []struct {
		name     string
		strategy string
		set      limiter.Settings
	}{
		{"strategy", "token_bucket", base},
		{"limit", "fixed_window", limiter.Settings{Limit: 101, Window: 30 * time.Second}},
		{"window", "fixed_window", limiter.Settings{Limit: 100, Window: 31 * time.Second}},
		{"burst", "fixed_window", limiter.Settings{Limit: 100, Window: 30 * time.Second, Burst: 20}},
		{"capacity", "concurrency", limiter.Settings{Limit: 100, Capacity: 50}},
		{"cells", "sliding_window_counter", limiter.Settings{Limit: 100, Window: 30 * time.Second, Cells: 12}},
	}
	for _, d := range diffs {
		if got := settingsHash(d.strategy, d.set); got == h1 {
			t.Errorf("%s change did not alter the hash", d.name)
		}
	}
}

// ---------------------------------------------------------------------------
// sinkAgg: allowed/limited counters and per-second rates after ticks.
// ---------------------------------------------------------------------------

type captureSink struct {
	mu        sync.Mutex
	decisions map[string]int
	evictions map[string]int
	last      limiter.Decision
}

func newCaptureSink() *captureSink {
	return &captureSink{
		decisions: map[string]int{},
		evictions: map[string]int{},
	}
}

func (c *captureSink) ObserveDecision(route, name, strategy string, d limiter.Decision) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.decisions[aggKey(route, name, strategy)]++
	c.last = d
}

func (c *captureSink) ObserveEviction(route, name, strategy string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictions[aggKey(route, name, strategy)]++
}

func TestSinkAggregationCountersAndPerSec(t *testing.T) {
	const key = "r1/per-ip/fixed_window"

	inner := newCaptureSink()
	agg := newSinkAgg(inner)

	allowed := limiter.Decision{Allowed: true, Limit: 10, Remaining: 9,
		ResetIn: 10 * time.Second}
	denied := limiter.Decision{Allowed: false, Limit: 10, RetryAfter: time.Second,
		ResetIn: 5 * time.Second}

	for i := 0; i < 3; i++ {
		agg.ObserveDecision("r1", "per-ip", "fixed_window", allowed)
	}
	for i := 0; i < 2; i++ {
		agg.ObserveDecision("r1", "per-ip", "fixed_window", denied)
	}
	agg.ObserveEviction("r1", "per-ip", "fixed_window")

	// Fewer than two samples: no rate yet, but eviction count already visible.
	aRate, lRate, evictions, ok := agg.perSec(key)
	if !ok || aRate != 0 || lRate != 0 || evictions != 1 {
		t.Fatalf("pre-tick perSec = (%v,%v,%v,%v)", aRate, lRate, evictions, ok)
	}

	agg.tick() // baseline snapshot at 3 allowed / 2 limited

	for i := 0; i < 4; i++ {
		agg.ObserveDecision("r1", "per-ip", "fixed_window", allowed)
	}
	agg.ObserveDecision("r1", "per-ip", "fixed_window", denied)
	agg.ObserveEviction("r1", "per-ip", "fixed_window")

	agg.tick() // snapshot at 7 allowed / 3 limited

	aRate, lRate, evictions, ok = agg.perSec(key)
	if !ok {
		t.Fatal("perSec lost track of a known limiter")
	}

	// Two ticks one second apart => deltas over exactly n=1 seconds.
	if aRate != 4 {
		t.Errorf("allowed/sec = %v, want 4", aRate)
	}
	if lRate != 1 {
		t.Errorf("limited/sec = %v, want 1", lRate)
	}
	if evictions != 2 {
		t.Errorf("evictions = %d, want 2", evictions)
	}

	inner.mu.Lock()
	total, fwdEvict := inner.decisions[key], inner.evictions[key]
	inner.mu.Unlock()
	if total != 10 || fwdEvict != 2 {
		t.Errorf("inner sink saw decisions=%d evictions=%d, want 10/2", total, fwdEvict)
	}
}

func TestSinkAggregationForwardsToInnerSink(t *testing.T) {
	inner := newCaptureSink()
	agg := newSinkAgg(inner)

	agg.ObserveDecision("rt", "lim", "token_bucket",
		limiter.Decision{Allowed: false, Limit: 1})
	agg.ObserveEviction("rt", "lim", "token_bucket")

	key := aggKey("rt", "lim", "token_bucket")
	if inner.decisions[key] != 1 || inner.evictions[key] != 1 {
		t.Fatalf("forwarded counts = %+v / %+v", inner.decisions, inner.evictions)
	}
	inner.mu.Lock()
	lastDenied := !inner.last.Allowed
	inner.mu.Unlock()
	if !lastDenied {
		t.Error("decision payload not forwarded intact")
	}
}

func TestSinkPerSecUnknownKeyNotOK(t *testing.T) {
	agg := newSinkAgg(nil)
	if _, _, _, ok := agg.perSec("nope/nope/nope"); ok {
		t.Error("unknown key must report ok=false")
	}
	keys := agg.sortedKeys()
	if len(keys) != 0 {
		t.Errorf("sortedKeys on empty sink = %v", keys)
	}
}

// ---------------------------------------------------------------------------
// M6: shared-store failures fail CLOSED with telemetry and a single warning.
// ---------------------------------------------------------------------------

type brokenStore struct{}

func (brokenStore) Update(string, time.Duration, string,
	func(prev []byte, existed bool) (next []byte)) error {
	return errors.New("bbolt: database not open")
}

func (brokenStore) Close() error { return nil }

type warnRecorder struct {
	mu   sync.Mutex
	msgs []string
}

func (w *warnRecorder) Warn(msg string, kv ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.msgs = append(w.msgs, msg)
}

func (w *warnRecorder) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.msgs)
}

func TestEngineFailsClosedWhenSharedStoreUnavailable(t *testing.T) {
	agg := newSinkAgg(nil)
	logger := &warnRecorder{}
	l, err := limiter.New("fixed_window", limiter.Params{
		Route: "r1", Name: "quota",
		Settings: limiter.Settings{Limit: 7, Window: time.Minute},
		Backend:  brokenStore{},
		Metrics:  agg,
		Logger:   logger,
	})
	if err != nil {
		t.Fatalf("limiter.New: %v", err)
	}

	for i := 1; i <= 2; i++ {
		d := l.Allow("10.0.0.1", time.Now(), 1)
		if d.Allowed {
			t.Fatalf("call %d admitted traffic while the store is down", i)
		}
		if d.RetryAfter != 500*time.Millisecond {
			t.Errorf("call %d RetryAfter = %v, want 500ms", i, d.RetryAfter)
		}
		if d.Limit != 7 {
			t.Errorf("call %d Limit = %d, want the configured 7", i, d.Limit)
		}
	}

	key := aggKey("r1", "quota", "fixed_window")
	if got := agg.storeErrorsFor(key); got != 2 {
		t.Errorf("ObserveStoreError count = %d, want 2", got)
	}
	if logger.count() != 1 {
		t.Errorf("warning emitted %d times, want exactly once per process", logger.count())
	}
}

// ---------------------------------------------------------------------------
// L2: memory-driver capacity pressure surfaces as ObserveEviction.
// ---------------------------------------------------------------------------

func TestMemoryDriverEvictionTelemetryViaPublicAllow(t *testing.T) {
	inner := newCaptureSink()
	agg := newSinkAgg(inner)
	l, err := limiter.New("fixed_window", limiter.Params{
		Route: "r1", Name: "bursty",
		Settings: limiter.Settings{Limit: 1000, Window: time.Hour},
		MaxKeys:  16,
		Metrics:  agg,
	})
	if err != nil {
		t.Fatalf("limiter.New: %v", err)
	}
	now := time.Now()
	for i := 0; i < 1000; i++ {
		l.Allow(fmt.Sprintf("key-%d", i), now, 1)
	}
	key := aggKey("r1", "bursty", "fixed_window")
	if inner.evictions[key] == 0 {
		t.Fatal("1000 keys against MaxKeys=16 produced no ObserveEviction events")
	}
}

// ---------------------------------------------------------------------------
// M14: changing store.path is rejected instead of silently reopening state.
// ---------------------------------------------------------------------------

const sharedBackendConfigTmpl = `version: 1
server:
  listen: "127.0.0.1:0"
admin:
  listen: "127.0.0.1:0"
store:
  path: %q
upstreams:
  api:
    targets:
      - url: %q
        weight: 1
routes:
  - name: r1
    path_prefix: /api
    upstreams: api
    rate_limits:
      - name: quota
        strategy: fixed_window
        key: ip
        limit: 5
        window: 30s
        backend: shared
`

func TestReloadRejectsChangingStorePath(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer up.Close()

	dir := t.TempDir()
	storeA := filepath.Join(dir, "a.bbolt")
	storeB := filepath.Join(dir, "b.bbolt")

	sup, metrics, cfgPath := newTestSupervisor(t,
		fmt.Sprintf(sharedBackendConfigTmpl, storeA, up.URL))
	// Release the exclusive bbolt handle so Windows can delete the temp dir.
	t.Cleanup(func() { sup.Drain(time.Second) })
	handler := sup.Handler()

	writeFile(t, cfgPath, fmt.Sprintf(sharedBackendConfigTmpl, storeB, up.URL))
	err := sup.Reload()
	if err == nil {
		t.Fatal("reload must reject a changed store.path")
	}
	if !strings.Contains(err.Error(), "store.path changes require a restart") {
		t.Errorf("error = %v, want the restart-required message", err)
	}
	if !strings.Contains(err.Error(), storeA) {
		t.Errorf("error = %v, want the current path %q for diagnosis", err, storeA)
	}

	// The rejection is counted and the old generation still serves.
	if rec := serveGet(handler, "http://gateway/api/ping"); rec.Code != http.StatusOK {
		t.Errorf("old generation status = %d after rejected reload", rec.Code)
	}
	render := metrics.Render()
	if !strings.Contains(render, `gatewright_reloads_total{outcome="rejected"} 1`) {
		t.Errorf("rejected reload not counted:\n%s", render)
	}
}

// ---------------------------------------------------------------------------
// M8: generation references keep a swapped-out runtime alive; drain waits.
// ---------------------------------------------------------------------------

func TestReloadKeepsServingInFlightRequestOnOldGeneration(t *testing.T) {
	var entered atomic.Bool
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/hold" {
			entered.Store(true)
			select {
			case <-release:
			case <-time.After(10 * time.Second):
			}
		}
		_, _ = w.Write([]byte("done"))
	}))
	defer up.Close()

	sup, _, cfgPath := newTestSupervisor(t, fmt.Sprintf(baseConfigTmpl, up.URL))
	srv := httptest.NewServer(sup.Handler())
	defer srv.Close()

	done := make(chan int, 1)
	go func() {
		resp, err := (&http.Client{Timeout: 15 * time.Second}).Get(srv.URL + "/api/hold")
		if err != nil {
			done <- -1
			return
		}
		resp.Body.Close()
		done <- resp.StatusCode
	}()

	deadline := time.Now().Add(5 * time.Second)
	for !entered.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !entered.Load() {
		t.Fatal("request never reached the blocking upstream")
	}

	// Swap generations while the request holds a reference on the old one.
	writeFile(t, cfgPath, fmt.Sprintf(baseConfigTmpl, up.URL)+"# reloaded\n")
	if err := sup.Reload(); err != nil {
		t.Fatalf("reload during in-flight request: %v", err)
	}

	close(release)
	select {
	case code := <-done:
		if code != http.StatusOK {
			t.Fatalf("in-flight request across reload: status = %d, want 200", code)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("in-flight request never completed after reload")
	}

	// New requests land on the new generation without issue.
	if rec := serveGet(sup.Handler(), "http://gateway/api/after"); rec.Code != http.StatusOK {
		t.Errorf("post-reload request status = %d, want 200", rec.Code)
	}

	// Drain must return promptly once nothing is in flight (no wg deadlock).
	drainDone := make(chan struct{})
	go func() { sup.Drain(2 * time.Second); close(drainDone) }()
	select {
	case <-drainDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Drain did not return with zero in-flight requests")
	}
	// After Drain the acquire path is removed: Handler answers 503.
	if rec := serveGet(sup.Handler(), "http://gateway/api/post-drain"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("request after Drain status = %d, want 503", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// M10: the process logger is built from configuration, not hardcoded wiring.
// ---------------------------------------------------------------------------

func TestNewLoggerFromConfigRespectsAccessLogSettings(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "access.log")
	cfg := &config.Config{}
	cfg.Observability.AccessLog.Format = "json"
	cfg.Observability.AccessLog.Output = logPath
	cfg.Observability.AccessLog.Fields = []string{"req_id", "status"}

	logger, err := NewLoggerFromConfig(cfg, true, false)
	if err != nil {
		t.Fatalf("NewLoggerFromConfig: %v", err)
	}
	if f, ok := logger.Writer().(*os.File); ok {
		t.Cleanup(func() { _ = f.Close() })
	}
	if logger.Writer() == nil {
		t.Fatal("logger writer is nil")
	}

	// The configured field subset must survive into the emitted JSON.
	logger.Access(obs.AccessFields{ReqID: "r-1", Method: "GET",
		Path: "/x", Route: "hidden", Status: 200})
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	line := strings.TrimSpace(string(data))
	for _, want := range []string{`"req_id":"r-1"`, `"status":200`} {
		if !strings.Contains(line, want) {
			t.Errorf("log line missing %s:\n%s", want, line)
		}
	}
	for _, banned := range []string{`"route"`, `"method"`} {
		if strings.Contains(line, banned) {
			t.Errorf("unselected field %s leaked:\n%s", banned, line)
		}
	}
}

func TestNewLoggerFromConfigDefaultsAndErrors(t *testing.T) {
	// Empty format/output default to json/stdout without error.
	cfg := &config.Config{}
	l, err := NewLoggerFromConfig(cfg, false, false)
	if err != nil {
		t.Fatalf("defaults: %v", err)
	}
	if l.Writer() != os.Stdout {
		t.Errorf("default output = %T, want stdout", l.Writer())
	}

	// An unwritable file output surfaces as an error, not a silent drop.
	bad := filepath.Join(t.TempDir(), "missing-dir", "x.log")
	cfg2 := &config.Config{}
	cfg2.Observability.AccessLog.Format = "json"
	cfg2.Observability.AccessLog.Output = bad
	if _, err := NewLoggerFromConfig(cfg2, false, false); err == nil {
		t.Error("NewLoggerFromConfig must fail for an unwritable output path")
	}
}
