package runtime

import (
	"sort"
	"sync"
	"sync/atomic"

	"gatewright/internal/limiter"
)

// sinkAgg aggregates limiter decisions per instance for the admin surface
// while forwarding everything to the metrics registry.
type sinkAgg struct {
	inner limiter.MetricsSink

	mu      sync.Mutex
	aggs    map[string]*limAgg
	samples map[string][]limSample
	seq     uint64
}

type limAgg struct {
	route, name, strategy string
	allowed               atomic.Uint64
	limited               atomic.Uint64
	evictions             atomic.Uint64
	storeErrors           atomic.Uint64
}

type limSample struct {
	seq     uint64
	allowed uint64
	limited uint64
}

func newSinkAgg(inner limiter.MetricsSink) *sinkAgg {
	return &sinkAgg{
		inner:   inner,
		aggs:    map[string]*limAgg{},
		samples: map[string][]limSample{},
	}
}

func aggKey(route, name, strategy string) string {
	return route + "/" + name + "/" + strategy
}

func (a *sinkAgg) ObserveDecision(route, name, strategy string, d limiter.Decision) {
	key := aggKey(route, name, strategy)
	a.mu.Lock()
	g, ok := a.aggs[key]
	if !ok {
		g = &limAgg{route: route, name: name, strategy: strategy}
		a.aggs[key] = g
	}
	a.mu.Unlock()
	if d.Allowed {
		g.allowed.Add(1)
	} else {
		g.limited.Add(1)
	}
	if a.inner != nil {
		a.inner.ObserveDecision(route, name, strategy, d)
	}
}

func (a *sinkAgg) ObserveEviction(route, name, strategy string) {
	key := aggKey(route, name, strategy)
	a.mu.Lock()
	g, ok := a.aggs[key]
	if !ok {
		g = &limAgg{route: route, name: name, strategy: strategy}
		a.aggs[key] = g
	}
	a.mu.Unlock()
	g.evictions.Add(1)
	if a.inner != nil {
		a.inner.ObserveEviction(route, name, strategy)
	}
}

// ObserveStoreError implements limiter.StoreErrorSink: shared-store failures
// are counted per limiter and forwarded to an inner sink that also extends
// the optional interface.
func (a *sinkAgg) ObserveStoreError(route, name, strategy string) {
	key := aggKey(route, name, strategy)
	a.mu.Lock()
	g, ok := a.aggs[key]
	if !ok {
		g = &limAgg{route: route, name: name, strategy: strategy}
		a.aggs[key] = g
	}
	a.mu.Unlock()
	g.storeErrors.Add(1)
	if inner, ok := a.inner.(limiter.StoreErrorSink); ok && a.inner != nil {
		inner.ObserveStoreError(route, name, strategy)
	}
}

func (a *sinkAgg) storeErrorsFor(key string) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if g := a.aggs[key]; g != nil {
		return g.storeErrors.Load()
	}
	return 0
}

// tick snapshots cumulative counters into per-second rings for rate math.
func (a *sinkAgg) tick() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seq++
	for key, g := range a.aggs {
		s := limSample{seq: a.seq, allowed: g.allowed.Load(), limited: g.limited.Load()}
		ring := a.samples[key]
		ring = append(ring, s)
		if len(ring) > 60 {
			ring = ring[len(ring)-60:]
		}
		a.samples[key] = ring
	}
}

// perSec returns per-second deltas over the last minute for one limiter.
func (a *sinkAgg) perSec(key string) (allowed, limited float64, evictions uint64, ok bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	g := a.aggs[key]
	if g == nil {
		return 0, 0, 0, false
	}
	evictions = g.evictions.Load()
	ring := a.samples[key]
	if len(ring) < 2 {
		return 0, 0, evictions, true
	}
	newest, oldest := ring[len(ring)-1], ring[0]
	n := len(ring) - 1 // samples are one second apart (tick cadence)
	if n <= 0 {
		n = 1
	}
	return float64(newest.allowed-oldest.allowed) / float64(n),
		float64(newest.limited-oldest.limited) / float64(n),
		evictions, true
}

// sortedKeys returns all tracked limiter keys deterministically.
func (a *sinkAgg) sortedKeys() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, 0, len(a.aggs))
	for k := range a.aggs {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
