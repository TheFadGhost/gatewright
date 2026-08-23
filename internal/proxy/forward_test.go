package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gatewright/internal/config"
	"gatewright/internal/errs"
	"gatewright/internal/middleware"
	"gatewright/internal/obs"
	"gatewright/internal/pool"
)

// ---------------------------------------------------------------------------
// Fakes and harness
// ---------------------------------------------------------------------------

type recordedDone struct {
	tgt     *pool.Target
	outcome pool.Outcome
}

// fakePool satisfies pool.Pool with scripted targets and recorded outcomes.
type fakePool struct {
	mu      sync.Mutex
	targets []*pool.Target
	next    int
	pickErr bool
	dones   []recordedDone
}

func (p *fakePool) Name() string                { return "fake" }
func (p *fakePool) Status() []pool.TargetStatus { return nil }
func (p *fakePool) Healthy(int) bool            { return !p.pickErr }
func (p *fakePool) Start(context.Context)       {}
func (p *fakePool) Drain(time.Duration)         {}
func (p *fakePool) Close()                      {}

func (p *fakePool) Pick(string) (*pool.Target, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pickErr || len(p.targets) == 0 {
		return nil, pool.ErrNoHealthy
	}
	t := p.targets[p.next%len(p.targets)]
	p.next++
	return t, nil
}

func (p *fakePool) Done(t *pool.Target, o pool.Outcome) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dones = append(p.dones, recordedDone{tgt: t, outcome: o})
}

func (p *fakePool) doneSnapshot() []recordedDone {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]recordedDone(nil), p.dones...)
}

func (p *fakePool) hits() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.next
}

// upstreamLog records every request an upstream received.
type upstreamLog struct {
	mu      sync.Mutex
	hitsN   int
	paths   []string
	queries []string
	methods []string
	hosts   []string
	headers []http.Header
	bodies  [][]byte
	at      []time.Time
}

func (l *upstreamLog) n() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.hitsN
}

func (l *upstreamLog) header(nth int, name string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.headers[nth].Get(name)
}

func (l *upstreamLog) values(nth int, name string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.headers[nth].Values(name)
}

func newRecordingUpstream(t *testing.T, respond func(w http.ResponseWriter, r *http.Request, nth int)) (*httptest.Server, *upstreamLog) {
	t.Helper()
	l := &upstreamLog{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		l.mu.Lock()
		l.hitsN++
		nth := l.hitsN
		l.paths = append(l.paths, r.URL.Path)
		l.queries = append(l.queries, r.URL.RawQuery)
		l.methods = append(l.methods, r.Method)
		l.hosts = append(l.hosts, r.Host)
		l.headers = append(l.headers, r.Header.Clone())
		l.bodies = append(l.bodies, body)
		l.at = append(l.at, time.Now())
		l.mu.Unlock()
		if respond != nil {
			respond(w, r, nth)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, l
}

func poolOf(servers ...*httptest.Server) *fakePool {
	fp := &fakePool{}
	for i, s := range servers {
		fp.targets = append(fp.targets, &pool.Target{
			Name:   fmt.Sprintf("fake[%d]", i),
			URL:    s.URL,
			Weight: 1,
		})
	}
	return fp
}

type gatewayTune func(*ForwarderOpts)

func newGateway(t *testing.T, routes []config.Route, pools map[string]pool.Pool, tune gatewayTune) http.Handler {
	t.Helper()
	rt, err := NewRouter(routes)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	fwds := map[string]*Forwarder{}
	for i := range routes {
		rte := routes[i]
		opts := ForwarderOpts{
			Pool:        pools[rte.Upstreams],
			StripPrefix: rte.StripPrefix,
			Timeout:     rte.Timeout.D,
			Mirror:      rte.Mirror,
			Retry:       RetryPolicy{Attempts: 3, BaseDelay: time.Millisecond, MaxDelay: 5 * time.Millisecond},
		}
		if rte.Mirror != nil {
			mp, ok := pools[rte.Mirror.Upstreams]
			if !ok {
				t.Fatalf("route %q mirror references unknown pool %q", rte.Name, rte.Mirror.Upstreams)
			}
			opts.MirrorPool = mp
		}
		if opts.Pool == nil {
			t.Fatalf("route %q references unknown pool %q", rte.Name, rte.Upstreams)
		}
		if tune != nil {
			tune(&opts)
		}
		if opts.Transport == nil {
			opts.Transport = &http.Transport{}
		}
		fwds[rte.Name] = NewForwarder(opts)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rm, apiErr := rt.Match(r)
		if apiErr != nil {
			if apiErr.Code == errs.CodeMethodNotAllowed && rm != nil {
				w.Header().Set("Allow", rm.AllowMethods())
			}
			errs.WriteWithID(w, apiErr, r.Header.Get("X-Gatewright-Request-Id"))
			return
		}
		fwds[rm.Route.Name].ServeHTTP(w, r, rm)
	})
}

type errEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		ReqID   string `json:"req_id"`
	} `json:"error"`
}

func decodeEnvelope(t *testing.T, body []byte) errEnvelope {
	t.Helper()
	var env errEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("bad error envelope %q: %v", body, err)
	}
	return env
}

// ---------------------------------------------------------------------------
// Unit: hop-by-hop stripping
// ---------------------------------------------------------------------------

func TestStripHopByHopStripsAllIncludingConnectionNamed(t *testing.T) {
	h := http.Header{}
	h.Set("Connection", "X-Custom, Keep-Alive")
	h.Set("X-Custom", "v")
	h.Set("Keep-Alive", "timeout=5")
	h.Set("Proxy-Connection", "keep-alive")
	h.Set("TE", "gzip")
	h.Set("Trailer", "X-Sum")
	h.Set("Transfer-Encoding", "chunked")
	h.Set("Upgrade", "h2c")
	h.Set("Proxy-Authorization", "Basic z")
	h.Set("Proxy-Authenticate", "Basic realm=x")
	h.Set("Content-Type", "text/plain") // survivor control

	StripHopByHop(h, false)

	for _, name := range []string{
		"Connection", "X-Custom", "Keep-Alive", "Proxy-Connection", "TE",
		"Trailer", "Transfer-Encoding", "Upgrade", "Proxy-Authorization", "Proxy-Authenticate",
	} {
		if got := h.Get(name); got != "" {
			t.Errorf("%s survived stripping: %q", name, got)
		}
	}
	if h.Get("Content-Type") != "text/plain" {
		t.Error("end-to-end header was stripped wrongly")
	}
}

func TestStripHopByHopKeepsTrailersTE(t *testing.T) {
	h := http.Header{}
	h.Set("TE", "trailers")
	StripHopByHop(h, false)
	if h.Get("TE") != "trailers" {
		t.Errorf(`TE: trailers must be preserved, got %q`, h.Get("TE"))
	}

	h2 := http.Header{}
	h2.Add("TE", "trailers")
	h2.Add("TE", "gzip") // mixed values => hop-by-hop negotiation, strip all
	StripHopByHop(h2, false)
	if h2.Get("TE") != "" {
		t.Errorf("mixed TE values must be stripped, got %q", h2.Get("TE"))
	}
}

func TestStripHopByHopPreservesUpgradePair(t *testing.T) {
	h := http.Header{}
	h.Set("Connection", "Upgrade, X-Junk")
	h.Set("Upgrade", "websocket")
	h.Set("X-Junk", "z")
	h.Set("Keep-Alive", "timeout=1")

	StripHopByHop(h, true)

	if h.Get("Connection") != "Upgrade, X-Junk" || h.Get("Upgrade") != "websocket" {
		t.Errorf("upgrade pair lost during strip: Connection=%q Upgrade=%q", h.Get("Connection"), h.Get("Upgrade"))
	}
	if h.Get("X-Junk") != "" || h.Get("Keep-Alive") != "" {
		t.Error("other connection-scoped headers must still be stripped during upgrades")
	}
}

func TestIsUpgradeRequested(t *testing.T) {
	cases := []struct {
		connection, upgrade string
		want                bool
	}{
		{"Upgrade", "websocket", true},
		{"keep-alive, Upgrade", "h2c", true},
		{"upgrade", "WebSocket", true}, // tokens are case-insensitive
		{"keep-alive", "websocket", false},
		{"Upgrade", "", false},
	}
	for _, tc := range cases {
		h := http.Header{}
		if tc.connection != "" {
			h.Set("Connection", tc.connection)
		}
		if tc.upgrade != "" {
			h.Set("Upgrade", tc.upgrade)
		}
		if got := IsUpgradeRequested(h); got != tc.want {
			t.Errorf("IsUpgradeRequested(Connection=%q, Upgrade=%q) = %v, want %v",
				tc.connection, tc.upgrade, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// End-to-end forwarding
// ---------------------------------------------------------------------------

func TestForwardBasicSetsForwardedHeaders(t *testing.T) {
	up, log := newRecordingUpstream(t, func(w http.ResponseWriter, r *http.Request, nth int) {
		w.Write([]byte("hello from upstream"))
	})
	pl := poolOf(up)
	gwHandler := newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/api", Upstreams: "u"}},
		map[string]pool.Pool{"u": pl}, nil)
	gw := httptest.NewServer(gwHandler)
	t.Cleanup(gw.Close)

	resp, err := http.Get(gw.URL + "/api/users?q=1")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "hello from upstream" {
		t.Fatalf("status=%d body=%q", resp.StatusCode, body)
	}

	if log.n() != 1 {
		t.Fatalf("upstream hits = %d, want 1", log.n())
	}
	if log.paths[0] != "/api/users" || log.queries[0] != "q=1" {
		t.Errorf("path/query = %q %q, want /api/users q=1", log.paths[0], log.queries[0])
	}
	upstreamHost := strings.TrimPrefix(up.URL, "http://")
	if log.hosts[0] != upstreamHost {
		t.Errorf("upstream saw Host %q, want its own address %q", log.hosts[0], upstreamHost)
	}
	if got := log.header(0, "X-Forwarded-For"); got != "127.0.0.1" {
		t.Errorf("X-Forwarded-For = %q, want 127.0.0.1", got)
	}
	if got := log.header(0, "X-Forwarded-Proto"); got != "http" {
		t.Errorf("X-Forwarded-Proto = %q", got)
	}
	if got := log.header(0, "X-Forwarded-Host"); got == "" {
		t.Error("X-Forwarded-Host not set")
	}
	if got := log.header(0, "X-Forwarded-Port"); got == "" {
		t.Error("X-Forwarded-Port not set")
	}
	fwd := log.header(0, "Forwarded")
	for _, want := range []string{"by=_gatewright", "for=", "host=", "proto=http"} {
		if !strings.Contains(fwd, want) {
			t.Errorf("Forwarded %q missing %q", fwd, want)
		}
	}
	dones := pl.doneSnapshot()
	if len(dones) != 1 || !dones[0].outcome.Success || dones[0].outcome.Status != 200 {
		t.Errorf("Done outcomes = %+v, want one success with status 200", dones)
	}
}

func TestForwardedForAppendedNotReplaced(t *testing.T) {
	up, log := newRecordingUpstream(t, func(w http.ResponseWriter, r *http.Request, nth int) {})
	gw := httptest.NewServer(newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/", Upstreams: "u"}},
		map[string]pool.Pool{"u": poolOf(up)}, nil))
	t.Cleanup(gw.Close)

	req, _ := http.NewRequest("GET", gw.URL+"/deep/path", nil)
	req.Header.Set("X-Forwarded-For", "203.0.113.7, 198.51.100.9")
	req.Header.Set("Forwarded", `for=203.0.113.7; host=client.example`)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if got := log.header(0, "X-Forwarded-For"); got != "203.0.113.7, 198.51.100.9, 127.0.0.1" {
		t.Errorf("X-Forwarded-For = %q, want appended list ending in 127.0.0.1", got)
	}
	fwd := log.header(0, "Forwarded")
	wantPrefix := `for=203.0.113.7; host=client.example, `
	if !strings.HasPrefix(fwd, wantPrefix) {
		t.Errorf("Forwarded = %q, want existing element preserved followed by ours (%q...)", fwd, wantPrefix)
	}
	if !strings.Contains(fwd[len(wantPrefix):], "by=_gatewright") {
		t.Errorf("Forwarded second element missing by=: %q", fwd)
	}
}

func TestHopByHopStrippedEndToEnd(t *testing.T) {
	up, log := newRecordingUpstream(t, func(w http.ResponseWriter, r *http.Request, nth int) {
		w.WriteHeader(200)
	})
	gw := httptest.NewServer(newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/", Upstreams: "u"}},
		map[string]pool.Pool{"u": poolOf(up)}, nil))
	t.Cleanup(gw.Close)

	req, _ := http.NewRequest("GET", gw.URL+"/h", nil)
	req.Header.Set("Connection", "X-Custom")
	req.Header.Set("X-Custom", "hop-junk")
	req.Header.Set("Keep-Alive", "timeout=5")
	req.Header.Set("Proxy-Connection", "keep-alive")
	req.Header.Set("Proxy-Authorization", "Basic zzz")
	req.Header.Set("TE", "gzip")
	req.Header.Set("Trailer", "X-Sum")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	for _, name := range []string{"X-Custom", "Keep-Alive", "Proxy-Connection", "Proxy-Authorization", "TE", "Trailer", "Connection"} {
		if got := log.header(0, name); got != "" {
			t.Errorf("upstream received hop-by-hop %s: %q", name, got)
		}
	}

	// TE: trailers is preserved end-to-end (per-hop trailer negotiation).
	req2, _ := http.NewRequest("GET", gw.URL+"/h", nil)
	req2.Header.Set("TE", "trailers")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if got := log.header(1, "TE"); got != "trailers" {
		t.Errorf(`upstream TE = %q, want "trailers"`, got)
	}
}

func TestStreamingResponseNotBuffered(t *testing.T) {
	const chunk = 64 << 10
	const chunks = 128 // 8 MiB total
	payload := bytes.Repeat([]byte{0xA5}, chunk)
	up, _ := newRecordingUpstream(t, func(w http.ResponseWriter, r *http.Request, nth int) {
		flusher := w.(http.Flusher)
		w.WriteHeader(200)
		for i := 0; i < chunks; i++ {
			w.Write(payload)
			flusher.Flush()
		}
	})
	gw := httptest.NewServer(newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/", Upstreams: "u"}},
		map[string]pool.Pool{"u": poolOf(up)}, nil))
	t.Cleanup(gw.Close)
	client := &http.Client{Transport: &http.Transport{}}

	// Warm up transports/pools so one-time setup does not pollute the sample.
	warm, err := client.Get(gw.URL + "/warm")
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, warm.Body)
	warm.Body.Close()

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	resp, err := client.Get(gw.URL + "/big")
	if err != nil {
		t.Fatal(err)
	}
	received, err := io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)

	if received != chunks*chunk {
		t.Fatalf("received %d bytes, want %d", received, chunks*chunk)
	}
	delta := after.TotalAlloc - before.TotalAlloc
	budget := uint64(chunks*chunk) * 4 // ~4x body size proves no full buffering
	if delta > budget {
		t.Errorf("TotalAlloc delta %d exceeds %d during streaming: body was likely fully buffered", delta, budget)
	}
}

func TestChunkedRequestBodyStreamsPostNeverRetried(t *testing.T) {
	const size = 1 << 20
	up, log := newRecordingUpstream(t, func(w http.ResponseWriter, r *http.Request, nth int) {
		w.WriteHeader(http.StatusServiceUnavailable) // would be retried if POST were eligible
		w.Write([]byte("upstream-down"))
	})
	pl := poolOf(up)
	gw := httptest.NewServer(newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/", Upstreams: "u"}},
		map[string]pool.Pool{"u": pl}, nil))
	t.Cleanup(gw.Close)

	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()
		buf := make([]byte, 32<<10)
		for written := 0; written < size; written += len(buf) {
			if _, err := pw.Write(buf); err != nil {
				return
			}
		}
	}()
	req, _ := http.NewRequest("POST", gw.URL+"/ingest", pr)
	req.ContentLength = -1 // force chunked, unknown length
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if got := pl.hits(); got != 1 {
		t.Errorf("POST picked %d times, want exactly 1 (never retried)", got)
	}
	if log.n() != 1 {
		t.Fatalf("upstream hits = %d, want 1", log.n())
	}
	if len(log.bodies[0]) != size {
		t.Errorf("upstream read %d streamed bytes, want %d", len(log.bodies[0]), size)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("client status = %d, want 503 passed through", resp.StatusCode)
	}
}

func TestRetryGetConnectionResetThenSuccess(t *testing.T) {
	up, log := newRecordingUpstream(t, func(w http.ResponseWriter, r *http.Request, nth int) {
		if nth == 1 {
			hj := w.(http.Hijacker)
			conn, _, err := hj.Hijack()
			if err == nil {
				conn.Close() // abrupt reset before any response byte
			}
			return
		}
		w.WriteHeader(200)
		w.Write([]byte("recovered"))
	})
	pl := poolOf(up)
	gw := httptest.NewServer(newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/", Upstreams: "u"}},
		map[string]pool.Pool{"u": pl},
		func(o *ForwarderOpts) {
			o.Retry = RetryPolicy{Attempts: 3, BaseDelay: time.Millisecond, MaxDelay: 3 * time.Millisecond}
		},
	))
	t.Cleanup(gw.Close)

	resp, err := http.Get(gw.URL + "/flaky")
	if err != nil {
		t.Fatalf("retry did not recover: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "recovered" {
		t.Fatalf("status=%d body=%q", resp.StatusCode, body)
	}
	if log.n() != 2 {
		t.Errorf("upstream hits = %d, want 2", log.n())
	}
	dones := pl.doneSnapshot()
	if len(dones) != 2 {
		t.Fatalf("Done calls = %d, want 2", len(dones))
	}
	if dones[0].outcome.Success || dones[0].outcome.ErrClass == pool.ErrNone {
		t.Errorf("first outcome should be a failure, got %+v", dones[0].outcome)
	}
	if !dones[1].outcome.Success || dones[1].outcome.Status != 200 {
		t.Errorf("second outcome should succeed, got %+v", dones[1].outcome)
	}
}

func TestRetryExhaustedPassesUpstreamStatusThrough(t *testing.T) {
	up, log := newRecordingUpstream(t, func(w http.ResponseWriter, r *http.Request, nth int) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("still-broken-" + fmt.Sprint(nth)))
	})
	gw := httptest.NewServer(newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/", Upstreams: "u"}},
		map[string]pool.Pool{"u": poolOf(up)},
		func(o *ForwarderOpts) {
			o.Retry = RetryPolicy{Attempts: 3, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}
		},
	))
	t.Cleanup(gw.Close)

	resp, err := http.Get(gw.URL + "/dead")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Errorf("status = %d, want upstream 503 passed through", resp.StatusCode)
	}
	if !strings.HasPrefix(string(body), "still-broken-") {
		t.Errorf("body = %q, want upstream response body", body)
	}
	if log.n() != 3 {
		t.Errorf("hits = %d, want 3 (attempts exhausted)", log.n())
	}
}

func TestSmallBodyIdempotentRetryUsesBuffer(t *testing.T) {
	var failPost, failGet atomic.Bool // each method's FIRST request 503s once
	up, log := newRecordingUpstream(t, func(w http.ResponseWriter, r *http.Request, nth int) {
		if r.Method == http.MethodPost && failPost.CompareAndSwap(false, true) {
			w.WriteHeader(503)
			return
		}
		if r.Method == http.MethodGet && failGet.CompareAndSwap(false, true) {
			w.WriteHeader(503)
			return
		}
		w.Write([]byte("ok"))
	})
	gw := httptest.NewServer(newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/", Upstreams: "u"}},
		map[string]pool.Pool{"u": poolOf(up)},
		func(o *ForwarderOpts) {
			o.Retry = RetryPolicy{Attempts: 3, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}
		},
	))
	t.Cleanup(gw.Close)

	resp, err := http.Post(gw.URL+"/small", "text/plain", strings.NewReader("tiny-payload"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	// POST is never retried: the 503 passes straight through.
	if resp.StatusCode != 503 {
		t.Errorf("POST status = %d, want 503 without retry", resp.StatusCode)
	}
	if log.n() != 1 {
		t.Errorf("upstream hits = %d, want exactly 1 (POST is not retryable)", log.n())
	}
	if got := string(log.bodies[0]); got != "tiny-payload" {
		t.Errorf("upstream body = %q, want tiny-payload", got)
	}

	// The same small body on an idempotent method IS replayed after a 503.
	resp2, err := http.Get(gw.URL + "/small")
	if err != nil {
		t.Fatal(err)
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if resp2.StatusCode != 200 || string(body2) != "ok" {
		t.Errorf("GET status=%d body=%q, want 200 ok after one swallowed 503", resp2.StatusCode, body2)
	}
	if log.n() != 3 {
		t.Errorf("total hits = %d, want 3 (1 POST + 2 GET attempts)", log.n())
	}
}

func TestBackoffBoundedByMaxDelay(t *testing.T) {
	f := NewForwarder(ForwarderOpts{
		Pool:      &fakePool{},
		Transport: &http.Transport{},
		Retry:     RetryPolicy{Attempts: 5, BaseDelay: 5 * time.Millisecond, MaxDelay: 6 * time.Millisecond},
	})
	for attempt := 0; attempt < 6; attempt++ {
		for i := 0; i < 200; i++ {
			d := f.backoff(attempt)
			if d < 0 || d > time.Duration(1.5*float64(6*time.Millisecond)) {
				t.Fatalf("backoff(%d) = %v outside jittered max-delay bound", attempt, d)
			}
		}
	}

	// Network-side sanity: repeated 503s with capped backoff finish quickly.
	up, log := newRecordingUpstream(t, func(w http.ResponseWriter, r *http.Request, nth int) {
		w.WriteHeader(503)
	})
	start := time.Now()
	gw := httptest.NewServer(newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/", Upstreams: "u"}},
		map[string]pool.Pool{"u": poolOf(up)},
		func(o *ForwarderOpts) {
			o.Retry = RetryPolicy{Attempts: 4, BaseDelay: 5 * time.Millisecond, MaxDelay: 6 * time.Millisecond}
		},
	))
	t.Cleanup(gw.Close)
	resp, err := http.Get(gw.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	elapsed := time.Since(start)
	if log.n() != 4 {
		t.Errorf("hits = %d, want 4", log.n())
	}
	if elapsed < 4*time.Millisecond || elapsed > 300*time.Millisecond {
		t.Errorf("elapsed %v outside expected bounded-backoff window", elapsed)
	}
}

func TestNoHealthyUpstreamWritesUP012(t *testing.T) {
	up, _ := newRecordingUpstream(t, nil)
	pl := poolOf(up)
	pl.pickErr = true
	gw := httptest.NewServer(newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/", Upstreams: "u"}},
		map[string]pool.Pool{"u": pl}, nil))
	t.Cleanup(gw.Close)

	resp, err := http.Get(gw.URL + "/anywhere")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
	env := decodeEnvelope(t, body)
	if env.Error.Code != errs.CodeNoHealthyUpstream {
		t.Errorf("code = %q, want UP012", env.Error.Code)
	}
}

func TestTotalTimeoutMapsToUP004(t *testing.T) {
	up, _ := newRecordingUpstream(t, func(w http.ResponseWriter, r *http.Request, nth int) {
		time.Sleep(250 * time.Millisecond)
		w.WriteHeader(200)
	})
	pl := poolOf(up)
	gw := httptest.NewServer(newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/", Upstreams: "u"}},
		map[string]pool.Pool{"u": pl},
		func(o *ForwarderOpts) {
			o.Timeout = 30 * time.Millisecond
			o.Retry = RetryPolicy{Attempts: 1}
		},
	))
	t.Cleanup(gw.Close)

	start := time.Now()
	resp, err := http.Get(gw.URL + "/slow")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want 504", resp.StatusCode)
	}
	env := decodeEnvelope(t, body)
	if env.Error.Code != errs.CodeTotalTimeout {
		t.Errorf("code = %q, want UP004", env.Error.Code)
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("timeout took %v; deadline not enforced", elapsed)
	}
	dones := pl.doneSnapshot()
	if len(dones) != 1 || dones[0].outcome.ErrClass != pool.ErrTimeout {
		t.Errorf("outcomes = %+v, want one ErrTimeout", dones)
	}
}

func TestConnectFailureMapsToBadGateway(t *testing.T) {
	// Target a port with no listener => dial refused => UP010 envelope.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	pl := &fakePool{targets: []*pool.Target{{Name: "dead", URL: "http://" + addr, Weight: 1}}}
	gw := httptest.NewServer(newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/", Upstreams: "u"}},
		map[string]pool.Pool{"u": pl},
		func(o *ForwarderOpts) { o.Retry = RetryPolicy{Attempts: 1} },
	))
	t.Cleanup(gw.Close)

	resp, err := http.Get(gw.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", resp.StatusCode)
	}
	env := decodeEnvelope(t, body)
	if env.Error.Code != errs.CodeBadGateway {
		t.Errorf("code = %q, want UP010", env.Error.Code)
	}
	dones := pl.doneSnapshot()
	if len(dones) != 1 || dones[0].outcome.Success || dones[0].outcome.ErrClass != pool.ErrConnect {
		t.Errorf("outcomes = %+v, want one ErrConnect failure", dones)
	}
}

func TestStripPrefixEndToEnd(t *testing.T) {
	up, log := newRecordingUpstream(t, func(w http.ResponseWriter, r *http.Request, nth int) {})
	gw := httptest.NewServer(newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/v1", Upstreams: "u", StripPrefix: true}},
		map[string]pool.Pool{"u": poolOf(up)}, nil))
	t.Cleanup(gw.Close)

	resp, err := http.Get(gw.URL + "/v1/users?full=1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if log.paths[0] != "/users" || log.queries[0] != "full=1" {
		t.Errorf("stripped path/query = %q %q, want /users full=1", log.paths[0], log.queries[0])
	}

	resp2, err := http.Get(gw.URL + "/v1")
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if log.paths[1] != "/" {
		t.Errorf("exact-prefix request forwarded as %q, want \"/\"", log.paths[1])
	}
}

func TestMethodNotAllowedEmitsAllowHeader(t *testing.T) {
	up, _ := newRecordingUpstream(t, nil)
	gw := httptest.NewServer(newGateway(t,
		[]config.Route{{Name: "g", PathPrefix: "/g", Methods: []string{"GET", "HEAD"}, Upstreams: "u"}},
		map[string]pool.Pool{"u": poolOf(up)}, nil))
	t.Cleanup(gw.Close)

	resp, err := http.Post(gw.URL+"/g/item", "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
	if got := resp.Header.Get("Allow"); got != "GET, HEAD" {
		t.Errorf("Allow = %q, want %q", got, "GET, HEAD")
	}
	if env := decodeEnvelope(t, body); env.Error.Code != errs.CodeMethodNotAllowed {
		t.Errorf("code = %q, want RT002", env.Error.Code)
	}
}

// ---------------------------------------------------------------------------
// Mirroring
// ---------------------------------------------------------------------------

func TestMirrorReceivesRequestWithoutAffectingClient(t *testing.T) {
	mirrorPaths := make(chan string, 4)
	mirrorSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mirrorPaths <- r.URL.Path
		w.WriteHeader(204)
	}))
	t.Cleanup(mirrorSrv.Close)
	primary, _ := newRecordingUpstream(t, func(w http.ResponseWriter, r *http.Request, nth int) {
		w.Write([]byte("primary-response"))
	})

	lg, _ := obs.New(obs.Options{Format: "json", Output: "stderr"})
	gw := httptest.NewServer(newGateway(t,
		[]config.Route{{
			Name: "r1", PathPrefix: "/", Upstreams: "p",
			Mirror: &config.Mirror{Upstreams: "shadow", Percentage: 100},
		}},
		map[string]pool.Pool{
			"p":      poolOf(primary),
			"shadow": poolOf(mirrorSrv),
		},
		func(o *ForwarderOpts) { o.Logger = lg }))
	t.Cleanup(gw.Close)

	start := time.Now()
	resp, err := http.Get(gw.URL + "/mirrored/x")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "primary-response" || resp.StatusCode != 200 {
		t.Fatalf("client got status=%d body=%q, want primary response", resp.StatusCode, body)
	}

	select {
	case p := <-mirrorPaths:
		if p != "/mirrored/x" {
			t.Errorf("mirror saw path %q", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("mirror never received the request")
	}
	// Mirror runs detached: client latency stays in the primary's ballpark.
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("client blocked %v on mirroring", d)
	}
}

func TestMirrorFailureDoesNotAffectClient(t *testing.T) {
	primary, _ := newRecordingUpstream(t, func(w http.ResponseWriter, r *http.Request, nth int) {
		w.Write([]byte("fine"))
	})
	brokenShadow := poolOf(httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})))
	brokenShadow.pickErr = true

	gw := httptest.NewServer(newGateway(t,
		[]config.Route{{
			Name: "r1", PathPrefix: "/", Upstreams: "p",
			Mirror: &config.Mirror{Upstreams: "shadow", Percentage: 100},
		}},
		map[string]pool.Pool{
			"p":      poolOf(primary),
			"shadow": brokenShadow,
		}, nil))
	t.Cleanup(gw.Close)

	resp, err := http.Get(gw.URL + "/ok")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "fine" {
		t.Errorf("status=%d body=%q; mirror failure leaked to client", resp.StatusCode, body)
	}
}

// ---------------------------------------------------------------------------
// WebSocket-style upgrade passthrough (raw TCP, no external libs)
// ---------------------------------------------------------------------------

func TestProtocolUpgradePassthrough(t *testing.T) {
	// Upstream: hijacks the connection, answers 101, then echoes lines.
	upLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	upSrv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj := w.(http.Hijacker)
		conn, rw, herr := hj.Hijack()
		if herr != nil {
			return
		}
		defer conn.Close()
		conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\r\n"))
		for {
			line, rerr := rw.Reader.ReadString('\n')
			if rerr != nil {
				return
			}
			if _, werr := conn.Write([]byte("echo:" + line)); werr != nil {
				return
			}
		}
	})}
	go upSrv.Serve(upLn)
	t.Cleanup(func() { _ = upSrv.Close() })

	gwHandler := newGateway(t,
		[]config.Route{{Name: "ws", PathPrefix: "/ws", Upstreams: "u"}},
		map[string]pool.Pool{"u": poolOfFromURL(upLn.Addr().String())},
		nil)
	gwLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gwSrv := &http.Server{Handler: gwHandler}
	go gwSrv.Serve(gwLn)
	t.Cleanup(func() { _ = gwSrv.Close() })

	conn, err := net.DialTimeout("tcp", gwLn.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	fmt.Fprintf(conn, "GET /ws/chat HTTP/1.1\r\nHost: gateway\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")

	br := bufio.NewReader(conn)
	statusLine, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("no upgrade response: %v", err)
	}
	if !strings.Contains(statusLine, "101") {
		t.Fatalf("status line = %q, want 101 Switching Protocols", statusLine)
	}
	sawUpgrade, sawConn := false, false
	for {
		line, err := br.ReadString('\n')
		if err != nil || line == "\r\n" {
			break
		}
		lower := strings.ToLower(line)
		if strings.HasPrefix(lower, "upgrade:") && strings.Contains(lower, "websocket") {
			sawUpgrade = true
		}
		if strings.HasPrefix(lower, "connection:") && strings.Contains(lower, "upgrade") {
			sawConn = true
		}
	}
	if !sawUpgrade || !sawConn {
		t.Errorf("101 headers incomplete: upgrade=%v connection=%v", sawUpgrade, sawConn)
	}

	// Tunnel is live: bidirectional data flows through the hijacked pair.
	if _, err := conn.Write([]byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	echo, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("no echo through tunnel: %v", err)
	}
	if echo != "echo:hello\n" {
		t.Errorf("echo = %q", echo)
	}
}

func poolOfFromURL(raw string) *fakePool {
	return &fakePool{targets: []*pool.Target{{Name: "ws[0]", URL: "http://" + raw, Weight: 1}}}
}

// ---------------------------------------------------------------------------
// Audit fixes: path normalization, hash-key wiring, client-cancel
// classification, ErrAbortHandler propagation, context request ids.
// ---------------------------------------------------------------------------

// waitForCond polls cond until true or the deadline expires (no blind sleeps).
func waitForCond(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestEncodedDotSegmentsRejectedEndToEnd(t *testing.T) {
	up, log := newRecordingUpstream(t, func(w http.ResponseWriter, r *http.Request, nth int) {})
	gw := httptest.NewServer(newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/v1", Upstreams: "u"}},
		map[string]pool.Pool{"u": poolOf(up)}, nil))
	t.Cleanup(gw.Close)

	for _, p := range []string{"/v1/%2e%2e/admin", "/v1/x/%2E%2e/y", "/%2e./v1/z"} {
		resp, err := http.Get(gw.URL + p)
		if err != nil {
			t.Fatalf("GET %s: %v", p, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("GET %s: status = %d, want 400 RT003", p, resp.StatusCode)
		}
		if env := decodeEnvelope(t, body); env.Error.Code != errs.CodeInvalidPath {
			t.Errorf("GET %s: code = %q, want RT003", p, env.Error.Code)
		}
	}
	if log.n() != 0 {
		t.Errorf("upstream hit %d times; traversal must be rejected before forwarding", log.n())
	}

	// Contrast: an encoded slash inside a segment is legitimate and forwarded
	// with its escaping intact ("%2F stays percent-encoded on the wire").
	resp, err := http.Get(gw.URL + "/v1/a%2Fb")
	if err != nil {
		t.Fatalf("GET encoded slash: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || log.n() != 1 {
		t.Fatalf("encoded-slash request: status=%d hits=%d, want 200/1", resp.StatusCode, log.n())
	}
	if !strings.Contains(log.paths[0], "a/b") && !strings.Contains(log.paths[0], "a%2Fb") {
		t.Errorf("upstream path = %q, want the a%%2Fb segment preserved", log.paths[0])
	}
}

func TestWriteErrorPrefersContextRequestID(t *testing.T) {
	up, _ := newRecordingUpstream(t, nil)
	f := NewForwarder(ForwarderOpts{
		Pool:      poolOf(up),
		Transport: &http.Transport{},
		Retry:     RetryPolicy{Attempts: 1},
	})

	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	f.writeError(rec, r.WithContext(middleware.WithRequestID(r.Context(), "ctx-id-123")),
		errs.CodeNoHealthyUpstream, "x")
	if env := decodeEnvelope(t, rec.Body.Bytes()); env.Error.ReqID != "ctx-id-123" {
		t.Errorf("req_id = %q, want ctx value ctx-id-123", env.Error.ReqID)
	}

	rec2 := httptest.NewRecorder()
	r2 := httptest.NewRequest(http.MethodGet, "/", nil)
	r2.Header.Set(middleware.RequestIDHeader, "hdr-id-9")
	f.writeError(rec2, r2, errs.CodeBadGateway, "x")
	if got := decodeEnvelope(t, rec2.Body.Bytes()).Error.ReqID; got != "hdr-id-9" {
		t.Errorf("fallback req_id = %q, want header value hdr-id-9", got)
	}
}

func TestClientCancelClassifiedErrCanceledNotConnect(t *testing.T) {
	release := make(chan struct{})
	up, log := newRecordingUpstream(t, func(w http.ResponseWriter, r *http.Request, nth int) {
		select {
		case <-release:
		case <-time.After(10 * time.Second):
		}
		w.WriteHeader(200)
	})
	pl := poolOf(up)
	gwHandler := newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/", Upstreams: "u"}},
		map[string]pool.Pool{"u": pl},
		func(o *ForwarderOpts) { o.Retry = RetryPolicy{Attempts: 3} },
	)
	gw := httptest.NewServer(gwHandler)
	t.Cleanup(gw.Close)

	req, _ := http.NewRequest(http.MethodGet, gw.URL+"/x", nil)
	ctx, cancel := context.WithCancel(req.Context())
	req = req.WithContext(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := http.DefaultClient.Do(req) // canceled mid-flight by design
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}()
	// Wait until the attempt is parked inside the upstream BEFORE hanging up,
	// so the cancellation lands mid-round-trip deterministically.
	waitForCond(t, 5*time.Second, "attempt to reach the upstream", func() bool {
		return log.n() >= 1
	})
	cancel() // the client hangs up

	var last pool.Outcome
	waitForCond(t, 5*time.Second, "canceled outcome to be reported to the pool", func() bool {
		dones := pl.doneSnapshot()
		if len(dones) == 0 {
			return false
		}
		last = dones[len(dones)-1].outcome
		return true
	})
	close(release) // let the upstream handler finish so teardown is clean
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("client request never returned after cancellation")
	}

	if last.Success {
		t.Errorf("outcome = %+v, want failure classification", last)
	}
	if last.ErrClass != pool.ErrCanceled {
		t.Errorf("ErrClass = %v, want ErrCanceled (client cancels never blame the target)", last.ErrClass)
	}
}

func TestErrAbortHandlerPropagatesUnwrapped(t *testing.T) {
	// Upstream sends headers plus a partial body, then slams the connection
	// shut. ReverseProxy's mid-stream copy fails and it panics with
	// http.ErrAbortHandler; the forwarder must neither swallow nor wrap it.
	upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("partial"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		hj := w.(http.Hijacker)
		if conn, _, herr := hj.Hijack(); herr == nil {
			_ = conn.Close()
		}
	}))
	t.Cleanup(upSrv.Close)

	f := NewForwarder(ForwarderOpts{
		Pool:      poolOf(upSrv),
		Transport: &http.Transport{},
		Retry:     RetryPolicy{Attempts: 1},
	})

	type outcome struct {
		panicked bool
		val      any
	}
	ch := make(chan outcome, 1)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { ch <- outcome{panicked: true, val: recover()} }()
		f.ServeHTTP(w, r, &RouteMatch{Route: &config.Route{Name: "r"}})
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	resp, err := (&http.Client{}).Get(srv.URL + "/abort")
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	select {
	case o := <-ch:
		if !o.panicked {
			t.Fatal("forwarder swallowed the mid-stream abort; ErrAbortHandler must propagate")
		}
		if o.val != http.ErrAbortHandler {
			t.Fatalf("panic value = %v (%T), want unwrapped http.ErrAbortHandler", o.val, o.val)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler never returned after abort")
	}
}

func TestHashKeySpecRoutesConsistentlyThroughRingHash(t *testing.T) {
	up1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("one"))
	}))
	t.Cleanup(up1.Close)
	up2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("two"))
	}))
	t.Cleanup(up2.Close)

	p := pool.New(pool.Config{
		Name:        "rh",
		LoadBalance: "ring_hash",
		Targets: []pool.TargetConfig{
			{URL: up1.URL, Weight: 1},
			{URL: up2.URL, Weight: 1},
		},
		Passive: pool.PassiveConfig{Window: time.Hour, Failures: 1 << 30, EjectionTime: time.Hour},
		Breaker: pool.BreakerConfig{Failures: 1 << 30, Window: time.Hour, Cooldown: time.Hour, HalfOpenProbes: 1},
	})
	defer p.Close()

	// The forwarder keys requests through the middleware extractor; probes
	// must build the identical key ("header\x00<value>") or they would query
	// a different ring position than production traffic.
	spec, err := config.ParseKeySpec("header:X-Session")
	if err != nil {
		t.Fatalf("ParseKeySpec: %v", err)
	}
	keyFn, err := middleware.BuildKeyExtractor(*spec)
	if err != nil {
		t.Fatalf("BuildKeyExtractor: %v", err)
	}
	keyOf := func(v string) string {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-Session", v)
		return keyFn(req, nil)
	}

	// Ask the same pool the forwarder uses which target each key owns, so the
	// consistency assertions below are deterministic for this build.
	pickURL := func(key string) string {
		tgt, err := p.Pick(key)
		if err != nil {
			t.Fatalf("Pick(%q): %v", key, err)
		}
		p.Done(tgt, pool.Outcome{Success: true, Status: 200})
		return tgt.URL
	}
	var keyA, keyB string
	found := false
search:
	for i := 0; i < 256; i++ {
		a := fmt.Sprintf("session-%d", i)
		ua := pickURL(keyOf(a))
		for j := i + 1; j < 256; j++ {
			b := fmt.Sprintf("session-%d", j)
			if ub := pickURL(keyOf(b)); ub != ua {
				keyA, keyB = a, b
				found = true
				break search
			}
		}
	}
	if !found {
		t.Fatal("could not locate two keys mapping to distinct targets")
	}
	wantA, wantB := pickURL(keyOf(keyA)), pickURL(keyOf(keyB))
	if wantA == wantB {
		t.Fatalf("probe targets converged: %q == %q", wantA, wantB)
	}

	gwHandler := newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/", Upstreams: "u"}},
		map[string]pool.Pool{"u": p},
		func(o *ForwarderOpts) {
			o.HashKey = "header:X-Session"
			o.Retry = RetryPolicy{Attempts: 1}
		},
	)
	gw := httptest.NewServer(gwHandler)
	t.Cleanup(gw.Close)

	getWithSession := func(v string) string {
		req, _ := http.NewRequest(http.MethodGet, gw.URL+"/sticky", nil)
		req.Header.Set("X-Session", v)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET with session %q: %v", v, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return string(body)
	}

	wantFor := map[string]string{wantA: bodyOfTarget(wantA, up1.URL, up2.URL), wantB: bodyOfTarget(wantB, up1.URL, up2.URL)}
	for i := 0; i < 5; i++ {
		for key, url := range map[string]string{keyA: wantA, keyB: wantB} {
			if got := getWithSession(key); got != wantFor[url] {
				t.Errorf("key %q attempt %d: body = %q, want %q (consistent routing)", key, i, got, wantFor[url])
			}
		}
	}
}

func bodyOfTarget(url, up1, up2 string) string {
	switch url {
	case up1:
		return "one"
	default:
		return "two"
	}
}

type keyRecordingPool struct {
	pool.Pool
	mu   sync.Mutex
	keys []string
}

func (p *keyRecordingPool) Pick(k string) (*pool.Target, error) {
	p.mu.Lock()
	p.keys = append(p.keys, k)
	p.mu.Unlock()
	return p.Pool.Pick(k)
}

func (p *keyRecordingPool) lastKey() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.keys) == 0 {
		return ""
	}
	return p.keys[len(p.keys)-1]
}

func TestForwarderPickKeyUsesExtractorSemantics(t *testing.T) {
	up, log := newRecordingUpstream(t, func(w http.ResponseWriter, r *http.Request, nth int) {})
	pl := &keyRecordingPool{Pool: poolOf(up)}
	gwHandler := newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/", Upstreams: "u"}},
		map[string]pool.Pool{"u": pl},
		func(o *ForwarderOpts) { o.HashKey = "path" },
	)
	gw := httptest.NewServer(gwHandler)
	t.Cleanup(gw.Close)

	resp, err := http.Get(gw.URL + "/anything")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	const want = "path\x00/anything"
	if got := pl.lastKey(); got != want {
		t.Errorf("Pick key = %q, want middleware extractor form %q", got, want)
	}
	if log.n() != 1 {
		t.Errorf("hits = %d, want 1", log.n())
	}

	// Without HashKey configured, the legacy client-IP key applies.
	pl2 := &keyRecordingPool{Pool: poolOf(up)}
	gw2 := httptest.NewServer(newGateway(t,
		[]config.Route{{Name: "r1", PathPrefix: "/", Upstreams: "u"}},
		map[string]pool.Pool{"u": pl2}, nil))
	t.Cleanup(gw2.Close)
	resp2, err := http.Get(gw2.URL + "/legacy")
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if got := pl2.lastKey(); got != "127.0.0.1" {
		t.Errorf("legacy Pick key = %q, want client IP 127.0.0.1", got)
	}
}
