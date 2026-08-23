package runtime

import (
	"context"
	"fmt"
	"net/http"

	"gatewright/internal/config"
	"gatewright/internal/limiter"
	"gatewright/internal/middleware"
	"gatewright/internal/obs"
	"gatewright/internal/pool"
	"gatewright/internal/proxy"
	"gatewright/internal/store"
)

func storeOpen(path string) (*store.DB, error) { return store.Open(path) }

// poolTransport reuses the pool-owned transport when available.
func poolTransport(p pool.Pool) *http.Transport {
	if pi, ok := p.(interface{ Transport() *http.Transport }); ok {
		return pi.Transport()
	}
	return nil
}

// buildRuntime constructs every component for one configuration generation.
func (s *Supervisor) buildRuntime(cfg *config.Config) (*Runtime, error) {
	ctx, cancel := context.WithCancel(context.Background())
	rt := &Runtime{
		Cfg:      cfg,
		pools:    pool.NewRegistry(),
		routeFwd: map[string]*proxy.Forwarder{},
		entries:  map[string][]middleware.RateLimitEntry{},
		engines:  map[string]limiter.Limiter{},
		settings: map[string]uint64{},
		chains:   map[string]http.Handler{},
		cancel:   cancel,
		trace:    cfg.Observability.Trace,
		stats:    newRouteStats(),
	}
	prev := s.current.Load()

	var backend limiter.Backend
	if s.needsStore(cfg) {
		db, err := s.openStore(cfg.Store.Path)
		if err != nil {
			cancel()
			return nil, err
		}
		backend = db
	}

	for name, up := range cfg.Upstreams {
		targets := make([]pool.TargetConfig, len(up.Targets))
		for i := range up.Targets {
			targets[i] = pool.TargetConfig{URL: up.Targets[i].URL, Weight: up.Targets[i].Weight}
		}
		verify := up.VerifyTLSOrDefault()
		if !verify {
			s.logger.Warn("upstream TLS verification DISABLED for pool; upstream identity is unauthenticated", "pool", name)
		}
		p := pool.New(pool.Config{
			Name:           name,
			Targets:        targets,
			LoadBalance:    up.LoadBalance,
			HashKey:        up.HashKey,
			ConnectTimeout: up.ConnectTimeout.D,
			ReadTimeout:    up.ReadTimeout.D,
			WriteTimeout:   up.WriteTimeout.D,
			Keepalive:      up.Keepalive.D,
			MaxIdlePerHost: up.MaxIdlePerHost,
			VerifyTLS:      verify,
			Active: pool.ActiveConfig{
				Enabled:            up.HealthCheck.Active.Enabled,
				Interval:           up.HealthCheck.Active.Interval.D,
				Timeout:            up.HealthCheck.Active.Timeout.D,
				Path:               up.HealthCheck.Active.Path,
				Method:             up.HealthCheck.Active.Method,
				HealthyThreshold:   up.HealthCheck.Active.HealthyThreshold,
				UnhealthyThreshold: up.HealthCheck.Active.UnhealthyThreshold,
				VerifyTLS:          up.HealthCheck.Active.VerifyTLSOrDefault(),
			},
			Passive: pool.PassiveConfig{
				Window:       up.HealthCheck.Passive.Window.D,
				Failures:     up.HealthCheck.Passive.Failures,
				EjectionTime: up.HealthCheck.Passive.EjectionTime.D,
			},
			Breaker: pool.BreakerConfig{
				Failures:       up.CircuitBreaker.Failures,
				Window:         up.CircuitBreaker.Window.D,
				Cooldown:       up.CircuitBreaker.Cooldown.D,
				HalfOpenProbes: up.CircuitBreaker.HalfOpenProbes,
			},
		})
		if s.metrics != nil {
			p.SetMetrics(poolMetrics{s.metrics})
		}
		rt.pools.Add(p)
		p.Start(ctx)
	}

	router, err := proxy.NewRouter(cfg.Routes)
	if err != nil {
		cancel()
		return nil, err
	}
	rt.router = router

	for ri := range cfg.Routes {
		route := &cfg.Routes[ri]
		var fwdPool pool.Pool
		if p, ok := rt.pools.Get(route.Upstreams); ok {
			fwdPool = p
		} else {
			cancel()
			return nil, fmt.Errorf("routes[%d].upstreams %q not defined", ri, route.Upstreams)
		}
		var mirrorPool pool.Pool
		if route.Mirror != nil {
			mp, ok := rt.pools.Get(route.Mirror.Upstreams)
			if !ok {
				cancel()
				return nil, fmt.Errorf("routes[%d].mirror.upstreams %q not defined", ri, route.Mirror.Upstreams)
			}
			mirrorPool = mp
		}
		rt.routeFwd[route.Name] = proxy.NewForwarder(proxy.ForwarderOpts{
			Pool:       fwdPool,
			MirrorPool: mirrorPool,
			Timeout:    route.Timeout.D,
			Transport:  poolTransport(fwdPool),
			Logger:     s.logger,
		})

		for li := range route.RateLimits {
			rl := &route.RateLimits[li]
			key := route.Name + "/" + rl.Name
			set := limiter.Settings{
				Limit: rl.Limit, Window: rl.Window.D,
				Burst: rl.Burst, Capacity: rl.Capacity, Cells: rl.Cells,
			}
			spec, kerr := config.ParseKeySpec(rl.Key)
			if kerr != nil {
				cancel()
				return nil, fmt.Errorf("routes[%d].rate_limits[%d].key: %w", ri, li, kerr)
			}
			keyFn, kerr := middleware.BuildKeyExtractor(*spec)
			if kerr != nil {
				cancel()
				return nil, fmt.Errorf("routes[%d].rate_limits[%d].key: %w", ri, li, kerr)
			}
			eng, err := limiter.New(rl.Strategy, limiter.Params{
				Route:    route.Name,
				Name:     rl.Name,
				Settings: set,
				Backend:  backendOrNil(rl.Backend, backend),
				MaxKeys:  rl.MaxKeys,
				Metrics:  s.sink,
			})
			if err != nil {
				cancel()
				return nil, fmt.Errorf("routes[%d].rate_limits[%d]: %w", ri, li, err)
			}
			rt.engines[key] = eng
			rt.settings[key] = settingsHash(rl.Strategy, set)
			rt.entries[route.Name] = append(rt.entries[route.Name], middleware.RateLimitEntry{
				Limiter: eng, KeyFn: keyFn, Name: rl.Name, Strategy: rl.Strategy,
			})
			carryState(prev, key, rt.settings[key], eng, rl.Backend)
		}
	}

	rt.handler = rt.assemble(s.logger, s.metrics)
	return rt, nil
}

// carryState moves memory-driver limiter state across reloads when strategy
// and settings are unchanged, so a reload never resets quota mid-window.
func carryState(prev *Runtime, key string, hash uint64, eng limiter.Limiter, backend string) {
	if prev == nil || backend == "shared" {
		return
	}
	oldEng, ok := prev.engines[key]
	if !ok || prev.settings[key] != hash {
		return
	}
	sfOld, ok1 := limiter.AsStateful(oldEng)
	sfNew, ok2 := limiter.AsStateful(eng)
	if !ok1 || !ok2 {
		return
	}
	if st := sfOld.SnapshotState(); st != nil {
		sfNew.RestoreState(st)
	}
}

func backendOrNil(name string, b limiter.Backend) limiter.Backend {
	if name == "shared" {
		return b
	}
	return nil
}

func (s *Supervisor) needsStore(cfg *config.Config) bool {
	for i := range cfg.Routes {
		for _, rl := range cfg.Routes[i].RateLimits {
			if rl.Backend == "shared" {
				return true
			}
		}
	}
	return false
}

func (s *Supervisor) openStore(path string) (limiter.Backend, error) {
	if s.store != nil && s.storePath == path {
		return s.store, nil
	}
	db, err := storeOpen(path)
	if err != nil {
		return nil, fmt.Errorf("store.path: %w", err)
	}
	s.store, s.storePath = db, path
	return db, nil
}

// poolMetrics adapts pool events into the metrics registry.
type poolMetrics struct{ m *obs.Metrics }

func (p poolMetrics) SetTargetHealth(pl, tgt string, healthy float64) {
	p.m.SetGauge("gatewright_upstream_healthy",
		"Upstream target accepting traffic (1) or not (0)",
		map[string]string{"pool": pl, "target": tgt}, healthy)
}

func (p poolMetrics) SetCircuitState(pl, tgt string, state float64) {
	p.m.SetGauge("gatewright_circuit_state",
		"Circuit breaker state: 0 closed, 0.5 half-open, 1 open",
		map[string]string{"pool": pl, "target": tgt}, state)
}

func (p poolMetrics) AddUpstreamRequest(pl, tgt string, code int) {
	p.m.IncCounter("gatewright_upstream_requests_total",
		"Requests sent to upstream targets",
		map[string]string{"pool": pl, "target": tgt, "code": fmt.Sprintf("%d", code)})
}

func (p poolMetrics) ObserveUpstreamLatency(pl, tgt string, seconds float64) {
	p.m.Observe("gatewright_upstream_request_duration_seconds",
		"Upstream attempt latency", nil,
		map[string]string{"pool": pl, "target": tgt}, seconds)
}
