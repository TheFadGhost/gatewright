package limiter

import (
	"sort"
	"sync"
	"sync/atomic"
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

	strat   Strategy
	rel     ReleasableStrategy
	mem     *memStore
	backend Backend
	maxKeys int
	metrics MetricsSink

	cfgLimit     int64         // configured limit for fail-closed decisions (<=0 => 1)
	logger       WarnLogger    // optional; nil disables warnings
	storeErrOnce sync.Once     // warn once per limiter per process
	evictions    atomic.Uint64 // entries dropped by memory-driver capacity pressure
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
		cfgLimit:     p.Settings.Limit,
		logger:       p.Logger,
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
		err := e.backend.Update(key, e.strat.TTL(), e.bucket(), func(prev []byte, existed bool) []byte {
			next, dec := e.step(prev, existed, now, cost)
			d = dec
			return next
		})
		if err != nil {
			// Policy: shared-store unavailability denies traffic rather than
			// bypassing quota. Failing open would let every process behind a
			// partitioned store admit unlimited traffic exactly when global
			// visibility is lost, so the engine fails CLOSED instead.
			d = Decision{Allowed: false, RetryAfter: 500 * time.Millisecond, ResetIn: 500 * time.Millisecond}
			if e.cfgLimit > 0 {
				d.Limit = e.cfgLimit
			} else {
				d.Limit = 1
			}
			e.noteStoreError(err)
		}
	} else {
		e.mem.update(key, e.strat.TTL(), e.maxKeys, func(prev []byte, existed bool) ([]byte, bool) {
			next, dec := e.step(prev, existed, now, cost)
			d = dec
			return next, false
		}, e.onEvict)
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
	}, e.onEvict)
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

// noteStoreError forwards the failure to an optional StoreErrorSink and logs
// a single warning per limiter per process (the first failure; later ones are
// already counted as limited decisions).
func (e *engine) noteStoreError(err error) {
	if sink, ok := e.metrics.(StoreErrorSink); ok {
		sink.ObserveStoreError(e.route, e.name, e.strategyName)
	}
	e.storeErrOnce.Do(func() {
		if e.logger != nil {
			kv := []any{"route", e.route, "name", e.name, "strategy", e.strategyName}
			if err != nil {
				kv = append(kv, "error", err.Error())
			}
			e.logger.Warn("shared limiter store unavailable; failing closed (denying traffic)", kv...)
		}
	})
}

// onEvict records capacity-pressure evictions from the memory driver and
// reports them to the metrics sink (once per eviction event).
func (e *engine) onEvict(n int) {
	if n <= 0 {
		return
	}
	e.evictions.Add(uint64(n))
	if e.metrics != nil {
		e.metrics.ObserveEviction(e.route, e.name, e.strategyName)
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
	mu     [shardCount]sync.Mutex // one lock per shard, never nested
	shards [shardCount]map[string]*memEntry
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

// update runs fn atomically under the key's shard lock and stores the result.
// Expired entries are treated as absent and dropped. onEvict (when non-nil)
// receives the number of entries the eviction pass removed from the shard.
func (m *memStore) update(key string, ttl time.Duration, maxKeys int,
	fn func(prev []byte, existed bool) ([]byte, bool), onEvict func(n int)) {
	si := shardFor(key)
	m.mu[si].Lock()
	sh := m.shards[si]
	nowNanos := time.Now().UnixNano()

	prev, existed := lookupLocked(sh, key, nowNanos)
	next, del := fn(prev, existed)
	switch {
	case del || next == nil:
		delete(sh, key)
	default:
		sh[key] = &memEntry{key: key, data: next, expires: nowNanos + int64(ttl), touched: nowNanos}
	}
	removed := m.evictLocked(sh, maxKeys, nowNanos)
	m.mu[si].Unlock()
	if onEvict != nil && removed > 0 {
		onEvict(removed)
	}
}

// lookupLocked returns live state, treating expired entries as absent.
// Caller must hold the owning shard's lock.
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
// many victims relative to the scan size. Returns the number of entries
// removed. Caller must hold the owning shard's lock.
func (m *memStore) evictLocked(sh map[string]*memEntry, maxKeys int, nowNanos int64) int {
	budget := maxKeys / shardCount
	if budget < 8 {
		budget = 8
	}
	if len(sh) <= budget {
		return 0
	}
	before := len(sh)
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
		return before - len(sh)
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
	return before - len(sh)
}

// snapshot copies all live state (for hot-reload carry-over). Every shard
// lock is taken in index order and held together so the copy is a consistent
// point-in-time view.
func (m *memStore) snapshot() map[string][]byte {
	for i := range m.shards {
		m.mu[i].Lock()
	}
	defer m.unlockAll()
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

// restore replaces all state with the snapshot. All shard locks are held for
// the swap (index order) so readers observe either the old or the new world.
func (m *memStore) restore(state map[string][]byte) {
	nowNanos := time.Now().UnixNano()
	fresh := make([]map[string]*memEntry, shardCount)
	for i := range fresh {
		fresh[i] = make(map[string]*memEntry)
	}
	for k, v := range state {
		sh := fresh[shardFor(k)]
		sh[k] = &memEntry{key: k, data: append([]byte(nil), v...), expires: nowNanos + int64(time.Hour), touched: nowNanos}
	}
	for i := range m.shards {
		m.mu[i].Lock()
		m.shards[i] = fresh[i]
	}
	m.unlockAll()
}

func (m *memStore) unlockAll() {
	for i := range m.mu {
		m.mu[i].Unlock()
	}
}
