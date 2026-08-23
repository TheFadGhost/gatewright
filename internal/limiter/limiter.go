// Package limiter defines Gatewright's rate-limiting plugin contract.
//
// Design principle (DESIGN.md §1): the interface must make a correct
// implementation natural and an incorrect one awkward.
//
//   - Time is supplied, never read: strategies receive `now`.
//   - Cost is explicit: weighted accounting cannot silently assume 1.
//   - Decisions are total: limit/remaining/retry-after/reset are all reported,
//     and the conformance suite verifies their accuracy.
//   - Concurrency exists exactly twice: two audited drivers (in-memory,
//     shared-store) wrap pure strategy logic; strategies never lock.
package limiter

import (
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Decision is the complete outcome of one admission check.
type Decision struct {
	Allowed    bool          // true => forward the request
	Limit      int64         // capacity governing this key's current window/bucket
	Remaining  int64         // units left after this call; never negative; 0 when denied
	RetryAfter time.Duration // > 0 iff !Allowed; earliest retry that could succeed
	ResetIn    time.Duration // until window reset / full refill; > 0
}

// Limiter is the entire strategy surface exposed to the gateway.
type Limiter interface {
	// Allow accounts cost units against key at instant now.
	// Implementations must not read wall clocks; now is authoritative.
	Allow(key string, now time.Time, cost int64) Decision
}

// Releaser is implemented by concurrency-style limiters whose admitted units
// are returned when the request finishes.
type Releaser interface {
	Release(key string, now time.Time, cost int64)
}

// Stateful is implemented by limiters whose state can be carried across hot
// reloads so that a config change does not grant fresh quota mid-window.
type Stateful interface {
	SnapshotState() map[string][]byte
	RestoreState(state map[string][]byte)
}

// Strategy is what each algorithm implements: PURE logic over an opaque state
// blob. No locks, no clocks beyond the supplied now, no I/O.
//
// Step receives the previously stored state for the key (nil/existed=false on
// first sight) and returns the state to store along with the decision.
// Returning next == nil deletes the state (e.g. fully drained buckets).
type Strategy interface {
	Step(prev []byte, existed bool, now time.Time, cost int64) (next []byte, d Decision)
	// TTL bounds how long untouched state survives; drives eviction.
	TTL() time.Duration
}

// ReleasableStrategy is optionally implemented for concurrency-style
// strategies where admitted units are handed back after completion.
type ReleasableStrategy interface {
	ReleaseStep(prev []byte, existed bool, now time.Time, cost int64) (next []byte)
}

// MetricsSink receives per-decision telemetry. Implemented by the observability
// package; declared here to avoid an import cycle.
type MetricsSink interface {
	ObserveDecision(route, name, strategy string, d Decision)
	ObserveEviction(route, name, strategy string)
}

// StoreErrorSink is an OPTIONAL extension to MetricsSink: engines check for
// it via type assertion and call it whenever a shared-store update fails, so
// implementations that do not care never see the method.
type StoreErrorSink interface {
	ObserveStoreError(route, name, strategy string)
}

// WarnLogger is the minimal logging surface an engine may use (a subset of
// obs.Logger). Nil loggers disable engine warnings entirely.
type WarnLogger interface {
	Warn(msg string, kv ...any)
}

// Settings are the validated, typed knobs every strategy reads. The config
// layer fills them; factories reject combinations invalid for the strategy.
type Settings struct {
	Limit    int64         // required by all strategies
	Window   time.Duration // required except concurrency
	Burst    int64         // token_bucket; 0 => default = Limit
	Capacity int64         // leaky_bucket/concurrency; 0 => default = Limit
	Cells    int           // sliding_window_counter; 0 => default 10
}

// Checker validates Settings for one strategy, returning human problems.
type Checker func(s Settings) []string

// Params carries everything a Factory needs to build a Limiter.
type Params struct {
	Route    string   // owning route name (metrics label)
	Name     string   // limiter instance name from config
	Settings Settings // validated settings block

	Backend Backend     // nil => in-memory driver
	MaxKeys int         // memory bound per limiter; <=0 => DefaultMaxKeys
	Metrics MetricsSink // optional
	Logger  WarnLogger  // optional; used for once-per-limiter store warnings
}

// DefaultMaxKeys bounds unique keys per limiter unless configured otherwise.
const DefaultMaxKeys = 65536

var (
	regMu     sync.RWMutex
	factories = map[string]Factory{}
	checkers  = map[string]Checker{}
)

// Factory builds a strategy instance from validated settings.
type Factory func(p Params) (Limiter, error)

// Register installs a factory under its config name. Duplicate registration
// panics: it is always a programmer error at init time.
func Register(strategy string, f Factory) {
	regMu.Lock()
	defer regMu.Unlock()
	if _, dup := factories[strategy]; dup {
		panic("limiter: duplicate registration of strategy " + strategy)
	}
	if strategy == "" || f == nil {
		panic("limiter: invalid registration")
	}
	factories[strategy] = f
}

// RegisterChecker installs settings validation for a strategy.
func RegisterChecker(strategy string, c Checker) {
	regMu.Lock()
	defer regMu.Unlock()
	checkers[strategy] = c
}

// CheckSettings validates Settings for a strategy; empty slice = valid.
// Unknown strategies report one problem naming the known set.
func CheckSettings(strategy string, s Settings) []string {
	regMu.RLock()
	c, ok := checkers[strategy]
	regMu.RUnlock()
	if !ok {
		return []string{"unknown strategy " + strconv.Quote(strategy) + " (known: " + strings.Join(Strategies(), ", ") + ")"}
	}
	return c(s)
}

// New instantiates a registered strategy.
func New(strategy string, p Params) (Limiter, error) {
	regMu.RLock()
	f, ok := factories[strategy]
	regMu.RUnlock()
	if !ok {
		return nil, errors.New("limiter: unknown strategy " + strategy)
	}
	return f(p)
}

// Strategies lists registered names, sorted.
func Strategies() []string {
	regMu.RLock()
	defer regMu.RUnlock()
	out := make([]string, 0, len(factories))
	for k := range factories {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Has reports whether a strategy is registered.
func Has(strategy string) bool {
	regMu.RLock()
	defer regMu.RUnlock()
	_, ok := factories[strategy]
	return ok
}
