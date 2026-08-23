// Package pool manages groups of upstream targets: load balancing, active and
// passive health checking, ejection/re-entry and per-target circuit breaking.
package pool

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// Target is one upstream endpoint.
type Target struct {
	Name   string // canonical name, e.g. "catalog[0]"
	URL    string // absolute http(s) URL
	Weight int
}

// Health states reported by pools. Never rely on colour alone in UIs;
// State.String() gives the label word paired everywhere (DESIGN.md §7).
type State int

const (
	StateHealthy State = iota
	StateProbing
	StateEjected
	StateDown
)

func (s State) String() string {
	switch s {
	case StateHealthy:
		return "healthy"
	case StateProbing:
		return "probing"
	case StateEjected:
		return "ejected"
	case StateDown:
		return "down"
	default:
		return "unknown"
	}
}

// TargetStatus is a point-in-time health snapshot for admin/metrics.
type TargetStatus struct {
	Target
	State        State
	Inflight     int64
	Failures     int // consecutive failures within window (passive)
	EjectedUntil time.Time
	CircuitOpen  bool
	TotalReq     uint64
	TotalFail    uint64
}

// Picker selects a live target for a request. Implementations must be safe
// for concurrent use.
type Picker interface {
	// Pick returns a target allowed to receive traffic now.
	// hashKey enables consistent hashing for ring_hash pools ("": no key).
	Pick(hashKey string) (*Target, error)
	// Done reports completion of an attempt on tgt (success or failure),
	// decrementing inflight and feeding passive health + circuit breaker.
	Done(tgt *Target, outcome Outcome)
}

// Outcome summarises one attempt against a target.
type Outcome struct {
	Success  bool
	Status   int      // upstream response status (0 if transport error)
	ErrClass ErrClass // classification when Success == false
	Latency  time.Duration
}

// ErrClass categorises failures for passive health and breaker logic.
type ErrClass int

const (
	ErrNone     ErrClass = iota
	ErrConnect           // dial/TLS handshake failure or timeout
	ErrResponse          // read/write failure mid-response, reset conn, malformed
	ErrHTTP5xx           // upstream answered 5xx
	ErrTimeout           // context deadline exceeded
	ErrCanceled          // client went away: never counts against passive health or the breaker
)

// Pool is one named group of targets behind routes.
type Pool interface {
	Picker
	// Name returns the configured pool name.
	Name() string
	// Status snapshots every target for admin/metrics.
	Status() []TargetStatus
	// Healthy reports whether at least minHealthy targets accept traffic.
	Healthy(minHealthy int) bool
	// Start launches background active health checking (no-op when disabled).
	Start(ctx context.Context)
	// Drain marks the pool shutting down: new picks fail fast once drained,
	// idle connections are closed after the grace period.
	Drain(grace time.Duration)
	Close()
}

// Config carries validated settings from config.Upstream plus its name.
type Config struct {
	Name           string
	Targets        []TargetConfig
	LoadBalance    string
	HashKey        string
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	Keepalive      time.Duration
	MaxIdlePerHost int
	VerifyTLS      bool
	Active         ActiveConfig
	Passive        PassiveConfig
	Breaker        BreakerConfig
}

type TargetConfig struct {
	URL    string
	Weight int
}

type ActiveConfig struct {
	Enabled            bool
	Interval           time.Duration
	Timeout            time.Duration
	Path               string
	Method             string
	HealthyThreshold   int
	UnhealthyThreshold int
	VerifyTLS          bool
	Client             *http.Client // optional override (tests); nil => default
}

type PassiveConfig struct {
	Window       time.Duration
	Failures     int
	EjectionTime time.Duration
}

type BreakerConfig struct {
	Failures       int
	Window         time.Duration
	Cooldown       time.Duration
	HalfOpenProbes int
}

// ErrNoHealthy is returned by Pick when no target may receive traffic.
var ErrNoHealthy = errNoHealthy{}

type errNoHealthy struct{}

func (errNoHealthy) Error() string { return "no healthy upstream available" }

// Shared registry helpers so route configs resolve pools by name exactly once.

type Registry struct {
	mu    sync.RWMutex
	pools map[string]Pool
}

func NewRegistry() *Registry { return &Registry{pools: map[string]Pool{}} }

func (r *Registry) Add(p Pool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pools[p.Name()] = p
}

func (r *Registry) Get(name string) (Pool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.pools[name]
	return p, ok
}

func (r *Registry) All() []Pool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Pool, 0, len(r.pools))
	for _, p := range r.pools {
		out = append(out, p)
	}
	return out
}
