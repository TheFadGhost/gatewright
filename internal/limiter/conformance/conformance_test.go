package conformance

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	bbolt "go.etcd.io/bbolt"

	"gatewright/internal/limiter"

	// Registration side effects: every shipped strategy must face the suite.
	_ "gatewright/internal/limiter/concurrency"
	_ "gatewright/internal/limiter/fixedwindow"
	_ "gatewright/internal/limiter/leakybucket"
	_ "gatewright/internal/limiter/slidingcounter"
	_ "gatewright/internal/limiter/slidinglog"
	_ "gatewright/internal/limiter/tokenbucket"
)

// strategyCases is the authoritative matrix: every registered strategy with
// settings the suite can reason about exactly (integer capacities, windows
// long enough that 1ms slack is negligible).
func strategyCases() []struct {
	name     string
	settings limiter.Settings
} {
	return []struct {
		name     string
		settings limiter.Settings
	}{
		{name: "fixed_window", settings: limiter.Settings{Limit: 40, Window: 200 * time.Millisecond}},
		{name: "token_bucket", settings: limiter.Settings{Limit: 25, Window: 100 * time.Millisecond, Burst: 40}},
		{name: "leaky_bucket", settings: limiter.Settings{Limit: 25, Window: 100 * time.Millisecond, Capacity: 40}},
		{name: "sliding_window_log", settings: limiter.Settings{Limit: 40, Window: 200 * time.Millisecond}},
		{name: "sliding_window_counter", settings: limiter.Settings{Limit: 40, Window: 200 * time.Millisecond, Cells: 10}},
		{name: "concurrency", settings: limiter.Settings{Limit: 40}},
	}
}

func TestConformanceMemoryDriver(t *testing.T) {
	for _, sc := range strategyCases() {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			RunSuite(t, sc.name+"/memory", memoryBuilder(sc.name, sc.settings))
		})
	}
}

// TestConformanceSharedStoreDriver proves both audited drivers behave
// identically (DESIGN.md section 1). gatewright/internal/store does not exist
// yet, so this builds the shared-store path directly over bbolt -- the exact
// engine route any production Backend takes (Params{Backend}); when the store
// package lands it drops in here unchanged. Each instance gets its own
// database file so phases start from fresh state, mirroring the memory
// builder's semantics.
func TestConformanceSharedStoreDriver(t *testing.T) {
	pool := newBoltPool(t)
	for _, sc := range strategyCases() {
		sc := sc
		t.Run(sc.name, func(t *testing.T) {
			RunSuite(t, sc.name+"/shared", pool.builder(sc.name, sc.settings))
		})
	}
}

// memoryBuilder returns a factory handing out independent in-memory limiters.
func memoryBuilder(name string, s limiter.Settings) func() limiter.Limiter {
	return func() limiter.Limiter {
		l, err := limiter.New(name, limiter.Params{Route: "conf", Name: "conf", Settings: s})
		if err != nil {
			panic(err) // static, validated settings; a failure is a programming error
		}
		return l
	}
}

// ---------------------------------------------------------------------------
// Shared-store stand-in: one bbolt file per limiter instance.
// ---------------------------------------------------------------------------

// boltPool owns a temp directory plus every database opened beneath it,
// closing them before testing removes the directory (cleanup order LIFO).
type boltPool struct {
	t   *testing.T
	dir string

	mu  sync.Mutex
	n   int
	dbs []*bbolt.DB
}

func newBoltPool(t *testing.T) *boltPool {
	p := &boltPool{t: t, dir: t.TempDir()}
	t.Cleanup(func() {
		for _, db := range p.dbs {
			_ = db.Close()
		}
	})
	return p
}

// builder returns a factory opening a fresh bbolt-backed limiter per call.
func (p *boltPool) builder(name string, s limiter.Settings) func() limiter.Limiter {
	return func() limiter.Limiter {
		p.mu.Lock()
		p.n++
		path := filepath.Join(p.dir, fmt.Sprintf("inst-%03d.bolt", p.n))
		p.mu.Unlock()

		db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: 10 * time.Second, NoSync: true})
		if err != nil {
			panic(err)
		}
		p.mu.Lock()
		p.dbs = append(p.dbs, db)
		p.mu.Unlock()

		l, err := limiter.New(name, limiter.Params{
			Route: "conf", Name: "conf", Settings: s,
			Backend: &bboltSharedStore{db: db, t: p.t},
		})
		if err != nil {
			_ = db.Close()
			panic(err)
		}
		return l
	}
}

// bboltSharedStore implements limiter.Backend over one bbolt database,
// running each step inside the single-writer transaction -- the same shape as
// the production shared driver. The engine ignores Update's error return, so
// failures are surfaced to the test explicitly rather than silently turning
// into zero-valued Decisions. TTL is accepted but not enforced: bbolt has no
// expiry, suite lifetimes are far shorter than any strategy TTL, and expiry
// belongs to the production store's own tests.
type bboltSharedStore struct {
	db *bbolt.DB
	t  *testing.T

	mu     sync.Mutex
	failed bool
}

func (b *bboltSharedStore) Update(key string, ttl time.Duration, bucket string,
	fn func(prev []byte, existed bool) (next []byte)) error {
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bt, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		var prev []byte
		existed := false
		if cur := bt.Get([]byte(key)); len(cur) > 0 {
			prev = bytes.Clone(cur)
			existed = true
		}
		next := fn(prev, existed)
		if next == nil {
			return bt.Delete([]byte(key))
		}
		return bt.Put([]byte(key), next)
	})
	if err != nil {
		b.mu.Lock()
		first := !b.failed
		b.failed = true
		b.mu.Unlock()
		if first {
			b.t.Errorf("shared-store update failed: %v", err)
		}
	}
	return err
}

func (b *bboltSharedStore) Close() error { return b.db.Close() }

// ---------------------------------------------------------------------------
// Property tests: seeded pseudo-random schedules against global invariants.
// ---------------------------------------------------------------------------

// TestPropertyFixedWindowModelEquivalence replays a seeded random schedule
// and compares every decision against an independent reference model of
// aligned fixed windows, then re-checks the global invariant directly:
// admissions summed over any aligned window never exceed Limit. Skipped
// under -short so default runs stay well inside ten seconds.
func TestPropertyFixedWindowModelEquivalence(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: property loops skipped")
	}
	const (
		limit  = int64(12)
		window = 250 * time.Millisecond
		iters  = 4000
	)
	l := memoryBuilder("fixed_window", limiter.Settings{Limit: limit, Window: window})()
	rng := rand.New(rand.NewSource(42))

	type winState struct{ start, count int64 }
	model := map[string]*winState{}
	windowTotals := map[string]int64{}

	now := BaseTime
	for i := 0; i < iters; i++ {
		now = now.Add(time.Duration(rng.Int63n(int64(2 * window))))
		key := fmt.Sprintf("prop-%d", rng.Intn(5))
		cost := 1 + rng.Int63n(3)

		got := l.Allow(key, now, cost)
		checkContract(t, fmt.Sprintf("fw-property iter %d", i), got)

		start := now.Truncate(window).UnixNano()
		st := model[key]
		if st == nil || st.start != start {
			st = &winState{start: start}
			model[key] = st
		}
		wantAllow := st.count+cost <= limit
		if wantAllow {
			st.count += cost
		}

		if got.Allowed != wantAllow {
			t.Fatalf("iter %d key %s: allowed = %v, model says %v (count %d, cost %d)",
				i, key, got.Allowed, wantAllow, st.count, cost)
		}
		if wantAllow && got.Remaining != limit-st.count {
			t.Fatalf("iter %d key %s: Remaining = %d, model wants %d",
				i, key, got.Remaining, limit-st.count)
		}

		totalKey := key + "|" + fmt.Sprint(start)
		if wantAllow {
			windowTotals[totalKey] += cost
		}
		if windowTotals[totalKey] > limit {
			t.Fatalf("aligned-window invariant violated: %d units admitted within one window of %s (limit %d)",
				windowTotals[totalKey], key, limit)
		}
	}
}

// TestPropertyTokenBucketRefillBound replays a seeded schedule against
// token_bucket (capacity c, sustained rate r = Limit/Window) and asserts the
// refill bound: over any elapsed span a key admits at most c + elapsed*r
// units, with +1 slack absorbing fractional-refill flooring. Two sound
// bookkeeping passes run together -- a whole-history bound from first sight,
// and a rolling bound between consecutive samples -- so neither gross
// over-admission nor refill-scale drift escapes.
func TestPropertyTokenBucketRefillBound(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: property loops skipped")
	}
	const (
		ratePerWindow = int64(10)
		window        = 200 * time.Millisecond
		capacity      = int64(30)
		iters         = 4000
	)
	rate := float64(ratePerWindow) / window.Seconds() // tokens per second
	l := memoryBuilder("token_bucket",
		limiter.Settings{Limit: ratePerWindow, Window: window, Burst: capacity})()
	rng := rand.New(rand.NewSource(42))

	type ledger struct {
		t0       time.Time // history anchor: first sight of the key
		spent    int64     // units admitted since t0
		sampled  time.Time // previous sample point
		atSample int64     // cumulative spend at that point
	}
	ledgerBy := map[string]*ledger{}

	now := BaseTime
	for i := 0; i < iters; i++ {
		now = now.Add(time.Duration(rng.Int63n(int64(2 * window))))
		key := fmt.Sprintf("prop-%d", rng.Intn(5))
		cost := 1 + rng.Int63n(3)

		d := l.Allow(key, now, cost)
		checkContract(t, fmt.Sprintf("tb-property iter %d", i), d)
		if d.Limit != capacity {
			t.Fatalf("iter %d: Limit = %d, want burst capacity %d", i, d.Limit, capacity)
		}

		lg := ledgerBy[key]
		if lg == nil {
			lg = &ledger{t0: now, sampled: now} // bucket starts full at first sight
			ledgerBy[key] = lg
		}
		if d.Allowed {
			lg.spent += cost
		}

		el := now.Sub(lg.t0).Seconds()
		if whole := float64(capacity) + math.Ceil(el*rate) + 1; float64(lg.spent) > whole {
			t.Fatalf("iter %d key %s: %d units admitted within %v; whole-history bound %.1f",
				i, key, lg.spent, now.Sub(lg.t0).Round(time.Millisecond), whole)
		}
		elS := now.Sub(lg.sampled).Seconds()
		delta := lg.spent - lg.atSample
		if rolling := float64(capacity) + math.Ceil(elS*rate) + 1; float64(delta) > rolling {
			t.Fatalf("iter %d key %s: %d units admitted across %v span; rolling bound %.1f",
				i, key, delta, now.Sub(lg.sampled).Round(time.Millisecond), rolling)
		}
		lg.sampled, lg.atSample = now, lg.spent
	}
}
