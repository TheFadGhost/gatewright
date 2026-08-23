// Package runtime assembles a validated configuration into a running gateway:
// pools, router, forwarders, limiter engines, the middleware pipeline, the
// admin surface, hot reload and graceful shutdown.
package runtime

import (
	"context"
	"fmt"
	"hash/fnv"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gatewright/internal/config"
	"gatewright/internal/limiter"
	"gatewright/internal/middleware"
	"gatewright/internal/obs"
	"gatewright/internal/pool"
	"gatewright/internal/proxy"
	"gatewright/internal/store"
)

// Supervisor owns the current Runtime and manages hot reloads and shutdown.
type Supervisor struct {
	cfgPath string
	logger  obs.Logger
	metrics *obs.Metrics
	sink    *sinkAgg

	current   atomic.Pointer[Runtime]
	store     *store.DB
	storePath string

	reloadMu     sync.Mutex   // serialises reload attempts
	genMu        sync.RWMutex // gates generation swap against request acquire
	start        time.Time
	version      string
	drainTimeout time.Duration
}

// Runtime is one immutable configuration generation. In-flight requests hold
// a reference (refs) so the previous generation stays alive until every
// request it admitted has completed or the drain deadline expires.
type Runtime struct {
	Cfg      *config.Config
	router   *proxy.Router
	pools    *pool.Registry
	routeFwd map[string]*proxy.Forwarder            // route name -> forwarder
	entries  map[string][]middleware.RateLimitEntry // route name -> limiter entries
	engines  map[string]limiter.Limiter             // "route/name" -> engine
	settings map[string]uint64                      // carry-over identity
	chains   map[string]http.Handler                // route name -> inner chain (immutable after build)
	handler  http.Handler                           // full assembled pipeline
	cancel   context.CancelFunc
	refs     atomic.Int64 // in-flight requests holding this generation alive
	trace    bool
	stats    *routeStats
}

// NewSupervisor loads the initial configuration and prepares the gateway.
func NewSupervisor(cfgPath string, logger obs.Logger, metrics *obs.Metrics, version string) (*Supervisor, error) {
	s := &Supervisor{
		cfgPath:      cfgPath,
		logger:       logger,
		metrics:      metrics,
		sink:         newSinkAgg(nil),
		start:        time.Now(),
		version:      version,
		drainTimeout: 30 * time.Second,
	}
	if err := s.applyConfig("startup"); err != nil {
		return nil, err
	}
	return s, nil
}

// Handler resolves the current runtime per request. The generation reference
// is taken under genMu's read lock so a concurrent swap can never strand a
// request on a retired runtime without the retirement wait seeing it.
// Older generations serve until drained, so swapping never drops an
// in-flight connection.
func (s *Supervisor) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.genMu.RLock()
		rt := s.current.Load()
		if rt != nil {
			rt.refs.Add(1)
		}
		s.genMu.RUnlock()
		if rt == nil {
			writeJSONStatus(w, http.StatusServiceUnavailable,
				`{"error":{"code":"UP012","message":"gateway starting"}}`)
			return
		}
		rt.serve(w, r)
	})
}

func writeJSONStatus(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

// Reload applies the configuration currently on disk (admin/CLI entry point).
func (s *Supervisor) Reload() error { return s.applyConfig("admin-api") }

// applyConfig builds a new Runtime from disk and swaps it in atomically. A
// failing candidate never touches the running generation.
func (s *Supervisor) applyConfig(reason string) error {
	s.reloadMu.Lock()
	defer s.reloadMu.Unlock()

	cfg, verr := config.Load(s.cfgPath)
	if verr != nil {
		s.logger.Error("reload rejected: invalid config", "reason", reason, "detail", verr.Error())
		s.countReload(false)
		return verr
	}
	rt, err := s.buildRuntime(cfg)
	if err != nil {
		s.logger.Error("reload rejected: build failed", "reason", reason, "detail", err.Error())
		s.countReload(false)
		return err
	}
	old := func() *Runtime {
		s.genMu.Lock()
		defer s.genMu.Unlock()
		return s.current.Swap(rt)
	}()
	s.countReload(true)
	if old != nil {
		old.shutdownAsync(s.drainTimeout, s.logger)
	}
	limiters := 0
	for i := range cfg.Routes {
		limiters += len(cfg.Routes[i].RateLimits)
	}
	s.logger.Info("configuration loaded",
		"reason", reason,
		"routes", len(cfg.Routes),
		"pools", len(cfg.Upstreams),
		"limiters", limiters,
		"config_hash", s.configHash(),
	)
	return nil
}

func (s *Supervisor) countReload(ok bool) {
	if s.metrics == nil {
		return
	}
	outcome := "applied"
	if !ok {
		outcome = "rejected"
	}
	s.metrics.IncCounter("gatewright_reloads_total", "Configuration reload attempts",
		map[string]string{"outcome": outcome})
}

func (s *Supervisor) configHash() string {
	data, err := os.ReadFile(s.cfgPath)
	if err != nil {
		return "unknown"
	}
	h := fnv.New64a()
	_, _ = h.Write(data)
	return fmt.Sprintf("%016x", h.Sum64())
}

// serve runs one request on a generation whose reference was already taken
// by Handler (refs includes this request until it returns).
func (rt *Runtime) serve(w http.ResponseWriter, r *http.Request) {
	defer rt.refs.Add(-1)
	rt.handler.ServeHTTP(w, r)
}

// waitDrained blocks until every in-flight reference on rt is released or the
// grace deadline expires. Callers must have removed rt from the acquire path
// first: either via the genMu-guarded swap in applyConfig or the swap-to-nil
// in Drain.
func (rt *Runtime) waitDrained(grace time.Duration) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for rt.refs.Load() > 0 {
			time.Sleep(time.Millisecond)
		}
	}()
	select {
	case <-done:
		return true
	case <-time.After(grace):
		return false
	}
}

// shutdownAsync stops background work and waits for in-flight requests on the
// retired generation before closing its pools.
func (rt *Runtime) shutdownAsync(grace time.Duration, logger obs.Logger) {
	go func() {
		if rt.cancel != nil {
			rt.cancel()
		}
		if !rt.waitDrained(grace) {
			logger.Warn("drain deadline exceeded; closing remaining connections", "grace", grace.String())
		}
		for _, p := range rt.pools.All() {
			p.Close()
		}
	}()
}

// Drain performs orderly final shutdown when the process is exiting. The
// current generation is swapped out under genMu so no new request can take a
// reference while the drain waits for the existing ones.
func (s *Supervisor) Drain(grace time.Duration) {
	s.genMu.Lock()
	rt := s.current.Swap(nil)
	s.genMu.Unlock()
	if rt == nil {
		return
	}
	if rt.cancel != nil {
		rt.cancel()
	}
	if !rt.waitDrained(grace) {
		s.logger.Warn("shutdown drain deadline exceeded", "grace", grace.String())
	}
	for _, p := range rt.pools.All() {
		p.Drain(grace)
		p.Close()
	}
	if s.store != nil {
		_ = s.store.Close()
	}
}

// StartSampler drives the one-second rolling statistics tick for both route
// and limiter telemetry.
func (s *Supervisor) StartSampler(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if rt := s.current.Load(); rt != nil {
					rt.stats.tick()
				}
				s.sink.tick()
			}
		}
	}()
}

// Watch polls the config file for mtime changes (portable; no fs-event dep).
func (s *Supervisor) Watch(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		last := fileModTime(s.cfgPath)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := fileModTime(s.cfgPath)
				if !now.Equal(last) && !now.IsZero() {
					last = now
					if err := s.applyConfig("file-changed"); err == nil {
						s.logger.Info("hot reload applied")
					}
				}
			}
		}
	}()
}

func fileModTime(path string) time.Time {
	fi, err := os.Stat(path)
	if err != nil {
		return time.Time{}
	}
	return fi.ModTime()
}

// UnhealthyPools lists pools with no healthy target, for /readyz.
func (s *Supervisor) UnhealthyPools() []string {
	rt := s.current.Load()
	if rt == nil {
		return []string{"(starting)"}
	}
	var out []string
	for _, p := range rt.pools.All() {
		if !p.Healthy(1) {
			out = append(out, p.Name())
		}
	}
	sortStrings(out)
	return out
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// settingsHash identifies identical limiter configurations across reloads so
// memory-driver state can be carried over without granting fresh quota.
func settingsHash(strategy string, set limiter.Settings) uint64 {
	h := fnv.New64a()
	fmt.Fprintf(h, "%s|%d|%d|%d|%d|%d", strategy,
		set.Limit, int64(set.Window), set.Burst, set.Capacity, set.Cells)
	return h.Sum64()
}
