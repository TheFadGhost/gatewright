package pool

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// MetricsSink receives optional observability events from a pool. All methods
// must be safe for concurrent use. Pools tolerate a nil sink everywhere.
type MetricsSink interface {
	SetTargetHealth(pool, target string, healthy float64)
	SetCircuitState(pool, target string, state float64)
	AddUpstreamRequest(pool, target string, code int)
	ObserveUpstreamLatency(pool, target string, seconds float64)
}

type poolImpl struct {
	name string

	targets []*Target
	states  []*targetState
	peers   []*peer
	byPtr   map[*Target]*targetState

	picker strategy

	cfg       Config
	transport *http.Transport
	client    *http.Client

	sinkMu sync.Mutex
	sink   MetricsSink

	draining  atomic.Bool
	stopCh    chan struct{}
	startOnce sync.Once
	closeOnce sync.Once

	nowFunc func() time.Time
}

// New builds a pool from validated upstream configuration.
func New(cfg Config) *poolImpl {
	p := &poolImpl{
		name:     cfg.Name,
		cfg:      cfg,
		byPtr:    make(map[*Target]*targetState, len(cfg.Targets)),
		stopCh:   make(chan struct{}),
		nowFunc:  func() time.Time { return time.Now() },
		transport: buildTransport(cfg),
	}
	for i, tc := range cfg.Targets {
		w := tc.Weight
		if w < 0 {
			w = 0
		}
		tg := &Target{
			Name:   fmt.Sprintf("%s[%d]", cfg.Name, i),
			URL:    tc.URL,
			Weight: w,
		}
		st, pe := newTargetState(p.name, tg, cfg, p.clock)
		p.targets = append(p.targets, tg)
		p.states = append(p.states, st)
		p.peers = append(p.peers, pe)
		p.byPtr[tg] = st
	}
	p.picker = buildPicker(cfg.LoadBalance, p.peers, p.clock)
	ac := cfg.Active
	if ac.Client != nil {
		p.client = ac.Client
	} else {
		tr := p.transport.Clone()
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: !ac.VerifyTLS}
		c := &http.Client{Transport: tr}
		if ac.Timeout > 0 {
			c.Timeout = ac.Timeout
		}
		p.client = c
	}
	return p
}

func (p *poolImpl) clock() time.Time { return p.nowFunc() }

func buildTransport(cfg Config) *http.Transport {
	dialer := &net.Dialer{Timeout: cfg.ConnectTimeout}
	if cfg.Keepalive > 0 {
		dialer.KeepAlive = cfg.Keepalive
	}
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: !cfg.VerifyTLS},
		TLSHandshakeTimeout:   cfg.ConnectTimeout,
		ResponseHeaderTimeout: cfg.ReadTimeout,
		MaxIdleConnsPerHost:   cfg.MaxIdlePerHost,
		ExpectContinueTimeout: time.Second,
	}
	if cfg.Keepalive > 0 {
		tr.IdleConnTimeout = cfg.Keepalive
	}
	return tr
}

// strategy is the selection algorithm contract; the pool adds Done handling
// and target identity on top.
type strategy interface {
	Pick(hashKey string) (*Target, error)
}

func buildPicker(lb string, peers []*peer, now func() time.Time) strategy {
	switch lb {
	case "least_connections":
		return newLeastConnPicker(peers, now)
	case "ring_hash":
		return newRingHashPicker(peers, now)
	default:
		return newRoundRobinPicker(peers, now)
	}
}

func (p *poolImpl) Name() string { return p.name }

// Transport exposes the shared upstream transport so the forwarder reuses its
// connection pools instead of dialing independently.
func (p *poolImpl) Transport() *http.Transport { return p.transport }

// Draining reports whether Drain has been called.
func (p *poolImpl) Draining() bool { return p.draining.Load() }

// SetMetrics attaches (or replaces, with nil) the metrics sink. The current
// snapshot of every target is emitted immediately so late attachment still
// produces complete series.
func (p *poolImpl) SetMetrics(sink MetricsSink) {
	p.sinkMu.Lock()
	p.sink = sink
	p.sinkMu.Unlock()
	for _, st := range p.states {
		st.mu.Lock()
		st.setSinkLocked(sink)
		st.emitBaselineLocked()
		st.mu.Unlock()
	}
}

func (p *poolImpl) Pick(hashKey string) (*Target, error) {
	return p.picker.Pick(hashKey)
}

func (p *poolImpl) Done(tgt *Target, outcome Outcome) {
	if tgt == nil {
		return
	}
	st, ok := p.byPtr[tgt]
	if !ok {
		return
	}
	st.done(outcome, p.nowFunc())
}

func (p *poolImpl) Status() []TargetStatus {
	now := p.nowFunc()
	out := make([]TargetStatus, 0, len(p.states))
	for _, st := range p.states {
		out = append(out, st.status(now))
	}
	return out
}

func (p *poolImpl) Healthy(minHealthy int) bool {
	now := p.nowFunc()
	count := 0
	for _, pe := range p.peers {
		if pe.available(now) {
			count++
		}
	}
	return count >= minHealthy
}

// Start launches the active health checker. It is a no-op when active checks
// are disabled or already running. Checks for all targets run sequentially on
// one ticker so a given target is never probed concurrently with itself.
func (p *poolImpl) Start(ctx context.Context) {
	ac := p.cfg.Active
	if !ac.Enabled {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	interval := ac.Interval
	if interval <= 0 {
		interval = 10 * time.Second
	}
	path := ac.Path
	if path == "" {
		path = "/healthz"
	}
	method := ac.Method
	if method == "" {
		method = http.MethodGet
	}
	timeout := ac.Timeout
	p.startOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			p.probeAll(ctx, method, path, timeout)
			for {
				select {
				case <-ctx.Done():
					return
				case <-p.stopCh:
					return
				case <-ticker.C:
					p.probeAll(ctx, method, path, timeout)
				}
			}
		}()
	})
}

func (p *poolImpl) probeAll(ctx context.Context, method, path string, timeout time.Duration) {
	for _, st := range p.states {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-p.stopCh:
			return
		default:
		}
		ok := p.probeOne(ctx, st, method, path, timeout)
		st.activeResult(ok, p.nowFunc())
	}
}

func (p *poolImpl) probeOne(ctx context.Context, st *targetState, method, path string, timeout time.Duration) bool {
	if st == nil || st.parsedURL == nil {
		return false
	}
	reqCtx, cancel := ctx, func() {}
	if timeout > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	u := st.parsedURL.JoinPath(path)
	req, err := http.NewRequestWithContext(reqCtx, method, u.String(), nil)
	if err != nil {
		return false
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return false
	}
	code := resp.StatusCode
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 8<<10))
	_ = resp.Body.Close()
	return code >= 200 && code < 500
}

// Drain marks the pool as shutting down. Picks keep working so the supervisor
// decides when to stop admitting requests; idle transport connections are
// closed once the grace period elapses.
func (p *poolImpl) Drain(grace time.Duration) {
	p.draining.Store(true)
	time.AfterFunc(grace, p.transport.CloseIdleConnections)
}

func (p *poolImpl) Close() {
	p.closeOnce.Do(func() {
		close(p.stopCh)
		p.transport.CloseIdleConnections()
	})
}
