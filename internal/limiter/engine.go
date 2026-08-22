package limiter

import (
	"sort"
	"sync"
	"time"
)

// Backend is a transactional, keyed state store shared across processes.
// Update applies fn atomically to the blob stored under key. The backend owns
// ALL concurrency control for the shared driver; fn must stay pure.
type Backend interface {
	Update(key string, ttl time.Duration, bucket string,
		fn func(prev []byte, existed bool) (next []byte)) error
	Close() error
}

// engine wires a pure Strategy to exactly one of two audited drivers.
// It enforces Decision invariants so a misbehaving strategy fails loudly
// in tests rather than lying in production.
type engine struct {
	strategyName string
	route        string
	name         string

	strat  Strategy
	rel    ReleasableStrategy
	mem    *memStore
	backend Backend
	maxKeys int
	metrics MetricsSink
}

// NewEngine builds a limiter around strat using Params' driver selection.
func NewEngine(strategyName string, strat Strategy, p Params) Limiter {
	e := &engine{
		strategyName: strategyName,
		route:        p.Route,
		name:         p.Name,
		strat:        strat,
		backend:      p.Backend,
		maxKeys:      p.MaxKeys,
		metrics:      p.Metrics,
	}
	if r, ok := strat.(ReleasableStrategy); ok {
		e.rel = r
	}
	if e.maxKeys <= 0 {
		e.maxKeys = DefaultMaxKeys
	}
	if e.backend == nil {
		e.mem = newMemStore()
	}
	return e
}

func (e *engine) Allow(key string, now time.Time, cost int64) Decision {
	if cost <= 0 {
		panic("limiter: cost must be >= 1")
	}
	var d Decision
	if e.backend != nil {
		_ = e.backend.Update(key, e.strat.TTL(), e.bucket(), func(prev []byte, existed bool) []byte {
			next, dec := e.step(prev, existed, now, cost)
			d = dec
			return next
		})
	} else {
		e.mem.update(key, e.strat.TTL(), e.maxKeys, func(prev []byte, existed bool) ([]byte, bool) {
			next, dec := e.step(prev, existed, now, cost)
			d = dec
			return next, false
		}, nil)
	}
	e.report(d)
	return d
}

func (e *engine) Release(key string, now time.Time, cost int64) {
	if e.rel == nil || cost <= 0 {
		return
	}
	if e.backend != nil {
		_ = e.backend.Update(key, e.strat.TTL(), e.bucket(), func(prev []byte, existed bool) []byte {
			return e.rel.ReleaseStep(prev, existed, now, cost)
		})
		return
	}
	e.mem.update(key, e.strat.TTL(), e.maxKeys, func(prev []byte, existed bool) ([]byte, bool) {
		return e.rel.ReleaseStep(prev, existed, now, cost), false
	}, nil)
}

func (e *engine) step(prev []byte, existed bool, now time.Time, cost int64) ([]byte, Decision) {
	next, d := e.strat.Step(prev, existed, now, cost)
	// Invariant enforcement: clamp negatives, floor retry-after.
	if d.Remaining < 0 {
		d.Remaining = 0
	}
	if !d.Allowed && d.RetryAfter < time.Millisecond {
		d.RetryAfter = time.Millisecond
	}
	if d.Allowed {
		d.RetryAfter = 0
	}
	if d.Limit <= 0 {
		// A strategy that cannot state its limit is broken; surface it as a
		// denial rather than silently admitting unbounded traffic.
		d = Decision{Allowed: false, Limit: 1, Remaining: 0, RetryAfter: time.Second, ResetIn: time.Second}
	}
	return next, d
}

func (e *engine) report(d Decision) {
	if e.metrics != nil {
		e.metrics.ObserveDecision(e.route, e.name, e.strategyName, d)
	}
}

func (e *engine) bucket() string { return e.route + "/" + e.name }

// Releaser exposes optional release support to the middleware layer.
func AsReleaser(l Limiter) (Releaser, bool) {
	r, ok := l.(Releaser)
	return r, ok
}

// Stateful exposes reload carry-over for memory-driven engines.
func AsStateful(l Limiter) (Stateful, bool) {
	s, ok := l.(Stateful)
	return s, ok
}

// SnapshotState copies memory-driver state for hot-reload carry-over.
func (e *engine) SnapshotState() map[string][]byte {
	if e.mem == nil {
		return nil // shared-store state lives in the backend already
	}
	return e.mem.snapshot()
}

// RestoreState imports previously snapshotted state.
func (e *engine) RestoreState(state map[string][]byte) {
	if e.mem != nil {
		e.mem.restore(state)
	}
}

var _ Stateful = (*engine)(nil)

// ---------------------------------------------------------------------------
// In-memory driver: sharded map + exact LRU + TTL. Bounded by maxKeys.
// ---------------------------------------------------------------------------

const shardCount = 16

type memEntry struct {
	key     string
	data    []byte
	expires int64 // unix nanos
	touched int64 // unix nanos, LRU clock
}

type memStore struct {
	mu      sync.Mutex
	shards  [shardCount]map[string]*memEntry
	evicted func()
}

func newMemStore() *memStore {
	s := &memStore{}
	for i := range s.shards {
		s.shards[i] = make(map[string]*memEntry)
	}
	return s
}

func shardFor(key string) int { return int(fnv1a(key) % shardCount) }

func fnv1a(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// update runs fn atomically under the global store lock and stores the result.
// Expired entries are treated as absent and dropped.
func (m *memStore) update(key string, ttl time.Duration, maxKeys int,
	fn func(prev []byte, existed bool) ([]byte, bool), onEvict func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	nowNanos := time.Now().UnixNano()
	sh := m.shards[shardFor(key)]

	prev, existed := lookupLocked(sh, key, nowNanos)
	next, del := fn(prev, existed)
	switch {
	case del || next == nil:
		delete(sh, key)
	default:
		sh[key] = &memEntry{key: key, data: next, expires: nowNanos + int64(ttl), touched: nowNanos}
	}
	m.evictLocked(sh, maxKeys, nowNanos)
	if onEvict != nil {
		onEvict()
	}
}

// lookupLocked returns live state, treating expired entries as absent.
func lookupLocked(sh map[string]*memEntry, key string, nowNanos int64) ([]byte, bool) {
	ent, ok := sh[key]
	if !ok {
		return nil, false
	}
	if ent.expires <= nowNanos {
		delete(sh, key)
		return nil, false
	}
	ent.touched = nowNanos
	return ent.data, true
}

// evictLocked keeps the shard under its fair share of maxKeys.
// One amortized pass: drop everything expired, then the oldest-touched
// survivors until within budget. Cost is bounded because each pass removes
// many victims relative to the scan size.
func (m *memStore) evictLocked(sh map[string]*memEntry, maxKeys int, nowNanos int64) {
	budget := maxKeys / shardCount
	if budget < 8 {
		budget = 8
	}
	if len(sh) <= budget {
		return
	}
	type victim struct {
		k    string
		t    int64
		live bool
	}
	victims := make([]victim, 0, len(sh))
	for k, v := range sh {
		expired := v.expires <= nowNanos
		victims = append(victims, victim{k, v.touched, !expired})
		if !expired {
			continue
		}
		delete(sh, k)
	}
	// Still over budget after dropping expired entries?
	if len(sh) <= budget {
		return
	}
	// Sort live survivors oldest-touch first and evict down to budget.
	sort.Slice(victims, func(i, j int) bool { return victims[i].t < victims[j].t })
	for _, v := range victims {
		if len(sh) <= budget {
			break
		}
		if !v.live {
			continue // already removed as expired
		}
		delete(sh, v.k)
	}
}

// snapshot copies all live state (for hot-reload carry-over).
func (m *memStore) snapshot() map[string][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string][]byte)
	nowNanos := time.Now().UnixNano()
	for _, sh := range m.shards {
		for k, v := range sh {
			if v.expires > nowNanos {
				out[k] = append([]byte(nil), v.data...)
			}
		}
	}
	return out
}

// restore replaces all state with the snapshot.
func (m *memStore) restore(state map[string][]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	nowNanos := time.Now().UnixNano()
	for i := range m.shards {
		m.shards[i] = make(map[string]*memEntry)
	}
	for k, v := range state {
		sh := m.shards[shardFor(k)]
		sh[k] = &memEntry{key: k, data: append([]byte(nil), v...), expires: nowNanos + int64(time.Hour), touched: nowNanos}
	}
}
