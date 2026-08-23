// Package admin serves the Gatewright operations dashboard and its JSON/SSE
// API under /admin. Views are point-in-time snapshots produced by a
// SnapshotProvider owned by the caller.
package admin

import (
	"time"

	"gatewright/internal/pool"
)

// SnapshotProvider is the data surface the admin server renders. The runtime
// wires its pools, limiters, metrics registry and config store into one
// implementation.
type SnapshotProvider interface {
	// RoutesView summarises configured routes.
	RoutesView() []RouteView
	// PoolsView snapshots every pool with its target statuses.
	PoolsView() []PoolView
	// MetricsText returns the current Prometheus text exposition.
	MetricsText() string
	// LatencyPercentiles returns p50/p95/p99 for route over the window, in
	// milliseconds. ok is false when no observations exist yet; callers must
	// render an em dash rather than a fabricated zero.
	LatencyPercentiles(route string, window time.Duration) (p50, p95, p99 float64, ok bool)
	// RequestRates reports requests per second per route over the last minute.
	RequestRates() map[string]float64
	// StatusCounts reports 2xx/4xx/5xx counts per route over the last minute.
	StatusCounts() map[string][3]uint64
	// LimiterViews snapshots limiter activity.
	LimiterViews() []LimiterView
	// Reload triggers a hot reload of configuration.
	Reload() error
	// Version identifies the running build.
	Version() string
	// Uptime reports process uptime.
	Uptime() time.Duration
}

// RouteView summarises one configured route.
type RouteView struct {
	Name        string `json:"name"`
	Match       string `json:"match"`
	PoolName    string `json:"upstream"`
	LimiterName string `json:"limiter_name,omitempty"`
	HasLimiter  bool   `json:"has_limiter"`
}

// PoolView is one named group of upstream targets.
type PoolView struct {
	Name    string              `json:"name"`
	Targets []pool.TargetStatus `json:"targets"`
}

// LimiterView snapshots one limiter instance's activity.
type LimiterView struct {
	Route         string  `json:"route"`
	Name          string  `json:"name"`
	Strategy      string  `json:"strategy"`
	KeyType       string  `json:"key_type"`
	AllowedPerSec float64 `json:"allowed_per_sec"`
	LimitedPerSec float64 `json:"limited_per_sec"`
	// UsageFraction is 0..1, or -1 when unknown.
	UsageFraction float64 `json:"usage_fraction"`
	Evictions     uint64  `json:"evictions"`
}

// PercentileDTO carries latency percentiles in milliseconds. Values are nil
// when there is no data so the wire never shows a fabricated zero.
type PercentileDTO struct {
	OK    bool     `json:"ok"`
	P50Ms *float64 `json:"p50_ms,omitempty"`
	P95Ms *float64 `json:"p95_ms,omitempty"`
	P99Ms *float64 `json:"p99_ms,omitempty"`
}

// RouteStateDTO is a route row enriched with live rates and latencies.
type RouteStateDTO struct {
	RouteView
	RPS         float64       `json:"rps"`
	Percentiles PercentileDTO `json:"percentiles"`
	// StatusCounts is 2xx/4xx/5xx request counts over the last minute.
	Status2xx uint64 `json:"status_2xx"`
	Status4xx uint64 `json:"status_4xx"`
	Status5xx uint64 `json:"status_5xx"`
}

// TargetDTO is the wire shape of pool.TargetStatus with stable lowercase keys
// and health as a word, never colour alone.
type TargetDTO struct {
	Name         string `json:"name"`
	URL          string `json:"url"`
	Weight       int    `json:"weight"`
	State        string `json:"state"`
	Circuit      string `json:"circuit"`
	Inflight     int64  `json:"inflight"`
	Failures     int    `json:"failures"`
	EjectedUntil string `json:"ejected_until,omitempty"`
	TotalReq     uint64 `json:"total_req"`
	TotalFail    uint64 `json:"total_fail"`
}

// PoolDTO is the wire shape of PoolView.
type PoolDTO struct {
	Name    string      `json:"name"`
	Targets []TargetDTO `json:"targets"`
}

// StateDTO is the full dashboard snapshot served by /admin/api/state and the
// SSE stream.
type StateDTO struct {
	Version       string          `json:"version"`
	UptimeSeconds float64         `json:"uptime_seconds"`
	GeneratedAt   time.Time       `json:"generated_at"`
	Routes        []RouteStateDTO `json:"routes"`
	Pools         []PoolDTO       `json:"pools"`
	Limiters      []LimiterView   `json:"limiters"`
}

func targetToDTO(t pool.TargetStatus) TargetDTO {
	dto := TargetDTO{
		Name:      t.Name,
		URL:       t.URL,
		Weight:    t.Weight,
		State:     t.State.String(),
		Inflight:  t.Inflight,
		Failures:  t.Failures,
		TotalReq:  t.TotalReq,
		TotalFail: t.TotalFail,
	}
	if t.CircuitOpen {
		dto.Circuit = "open"
	} else {
		dto.Circuit = "closed"
	}
	if !t.EjectedUntil.IsZero() {
		dto.EjectedUntil = t.EjectedUntil.UTC().Format(time.RFC3339)
	}
	return dto
}

func poolToDTO(p PoolView) PoolDTO {
	targets := make([]TargetDTO, 0, len(p.Targets))
	for _, t := range p.Targets {
		targets = append(targets, targetToDTO(t))
	}
	return PoolDTO{Name: p.Name, Targets: targets}
}
