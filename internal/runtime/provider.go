package runtime

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"gatewright/internal/admin"
	"gatewright/internal/config"
)

// routeStats maintains rolling per-route telemetry for the dashboard:
// request rates and latency percentiles over the last 60 seconds. Values come
// from in-process atomic counters sampled once per second; percentile math
// uses bucket diffs so numbers are honest windowed values, not lifetime
// averages.
type routeStats struct {
	mu      sync.Mutex
	samples map[string][]sample // route -> ring (60 x 1s)
	cur     map[string]*liveCounters
	seq     uint64
}

type liveCounters struct {
	requests uint64
	buckets  [lenLatBuckets]uint64
}

type sample struct {
	seq      uint64
	requests uint64
	buckets  [lenLatBuckets]uint64
}

// Latency buckets in milliseconds; final bucket is catch-all >=10000ms.
var latBucketsMS = []float64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000}

const lenLatBuckets = 14

func newRouteStats() *routeStats {
	return &routeStats{
		samples: map[string][]sample{},
		cur:     map[string]*liveCounters{},
	}
}

func bucketFor(ms float64) int {
	for i, ub := range latBucketsMS {
		if ms <= ub {
			return i
		}
	}
	return lenLatBuckets - 1
}

func (rs *routeStats) countRequest(route string, start time.Time, status int) {
	rs.mu.Lock()
	lc, ok := rs.cur[route]
	if !ok {
		lc = &liveCounters{}
		rs.cur[route] = lc
	}
	rs.mu.Unlock()
	ms := float64(time.Since(start).Microseconds()) / 1000.0
	// Order matters for the approximate read: bucket then counter.
	b := bucketFor(ms)
	rs.mu.Lock()
	lc.buckets[b]++
	lc.requests++
	rs.mu.Unlock()
}

// tick snapshots current counters into the ring; called once per second.
func (rs *routeStats) tick() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.seq++
	for route, lc := range rs.cur {
		s := sample{seq: rs.seq, requests: lc.requests, buckets: lc.buckets}
		ring := rs.samples[route]
		ring = append(ring, s)
		if len(ring) > 60 {
			ring = ring[len(ring)-60:]
		}
		rs.samples[route] = ring
	}
}

func (rs *routeStats) ratePerSec(route string, window time.Duration) (float64, bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	ring := rs.samples[route]
	if len(ring) < 2 {
		return 0, false
	}
	n := window / time.Second
	if n > time.Duration(len(ring)-1) {
		n = time.Duration(len(ring) - 1)
	}
	if n <= 0 {
		return 0, false
	}
	newest, oldest := ring[len(ring)-1], ring[len(ring)-1-int(n)]
	delta := newest.requests - oldest.requests
	return float64(delta) / float64(n), true
}

// percentiles computes p50/p95/p99 over the last `window` of diffs.
func (rs *routeStats) percentiles(route string, window time.Duration) (p50, p95, p99 float64, ok bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	ring := rs.samples[route]
	if len(ring) < 2 {
		return 0, 0, 0, false
	}
	n := window / time.Second
	if n > time.Duration(len(ring)-1) {
		n = time.Duration(len(ring) - 1)
	}
	if n <= 0 {
		return 0, 0, 0, false
	}
	newest, oldest := ring[len(ring)-1], ring[len(ring)-1-int(n)]
	var counts [lenLatBuckets]uint64
	total := uint64(0)
	for i := range counts {
		counts[i] = newest.buckets[i] - oldest.buckets[i]
		total += counts[i]
	}
	if total == 0 {
		return 0, 0, 0, false
	}
	pick := func(q float64) float64 {
		target := uint64(q * float64(total))
		if target == 0 {
			target = 1
		}
		run := uint64(0)
		for i, c := range counts {
			run += c
			if run >= target {
				if i == lenLatBuckets-1 {
					return latBucketsMS[len(latBucketsMS)-1] * 2 // beyond last bound: report 2x top edge
				}
				return latBucketsMS[i]
			}
		}
		return latBucketsMS[len(latBucketsMS)-1]
	}
	return pick(0.50), pick(0.95), pick(0.99), true
}

// ---------------------------------------------------------------------------
// admin.SnapshotProvider implementation
// ---------------------------------------------------------------------------

var _ admin.SnapshotProvider = (*Supervisor)(nil)

func (s *Supervisor) RoutesView() []admin.RouteView {
	rt := s.current.Load()
	if rt == nil {
		return nil
	}
	out := make([]admin.RouteView, 0, len(rt.Cfg.Routes))
	for i := range rt.Cfg.Routes {
		r := &rt.Cfg.Routes[i]
		match := summarizeMatch(r)
		limiterName := ""
		if len(r.RateLimits) > 0 {
			limiterName = r.RateLimits[0].Name
			if len(r.RateLimits) > 1 {
				limiterName += fmt.Sprintf(" +%d", len(r.RateLimits)-1)
			}
		}
		out = append(out, admin.RouteView{
			Name: r.Name, Match: match, PoolName: r.Upstreams,
			LimiterName: limiterName, HasLimiter: len(r.RateLimits) > 0,
		})
	}
	return out
}

func summarizeMatch(r *config.Route) string {
	host := "*"
	if len(r.Hosts) > 0 {
		host = r.Hosts[0]
		if len(r.Hosts) > 1 {
			host += fmt.Sprintf(" +%d", len(r.Hosts)-1)
		}
	}
	path := "/"
	switch {
	case r.PathPattern != "":
		path = r.PathPattern
	case r.PathPrefix != "":
		path = r.PathPrefix + "*"
	}
	methods := "ANY"
	if len(r.Methods) > 0 {
		methods = joinSort(r.Methods)
	}
	return host + " " + path + " [" + methods + "]"
}

func joinSort(in []string) string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	s := ""
	for i, m := range out {
		if i > 0 {
			s += ","
		}
		s += m
	}
	return s
}

func (s *Supervisor) PoolsView() []admin.PoolView {
	rt := s.current.Load()
	if rt == nil {
		return nil
	}
	pools := rt.pools.All()
	sort.Slice(pools, func(i, j int) bool { return pools[i].Name() < pools[j].Name() })
	out := make([]admin.PoolView, 0, len(pools))
	for _, p := range pools {
		out = append(out, admin.PoolView{Name: p.Name(), Targets: p.Status()})
	}
	return out
}

func (s *Supervisor) MetricsText() string {
	if s.metrics == nil {
		return "# metrics disabled\n"
	}
	return s.metrics.Render()
}

func (s *Supervisor) LatencyPercentiles(route string, window time.Duration) (p50, p95, p99 float64, ok bool) {
	rt := s.current.Load()
	if rt == nil {
		return 0, 0, 0, false
	}
	return rt.stats.percentiles(route, window)
}

func (s *Supervisor) RequestRates() map[string]float64 {
	rt := s.current.Load()
	if rt == nil {
		return nil
	}
	out := map[string]float64{}
	rt.stats.mu.Lock()
	routes := make([]string, 0, len(rt.stats.cur))
	for name := range rt.stats.cur {
		routes = append(routes, name)
	}
	rt.stats.mu.Unlock()
	for _, name := range routes {
		if v, ok := rt.stats.ratePerSec(name, time.Minute); ok {
			out[name] = v
		}
	}
	return out
}

func (s *Supervisor) LimiterViews() []admin.LimiterView {
	var out []admin.LimiterView
	for _, key := range s.sink.sortedKeys() {
		allowed, limited, evictions, _ := s.sink.perSec(key)
		var route, name, strategy string
		if parts := splitKey(key); len(parts) == 3 {
			route, name, strategy = parts[0], parts[1], parts[2]
		}
		out = append(out, admin.LimiterView{
			Route: route, Name: name, Strategy: strategy,
			KeyType:       keyTypeFor(s.current.Load(), route, name),
			AllowedPerSec: allowed,
			LimitedPerSec: limited,
			UsageFraction: -1,
			Evictions:     evictions,
		})
	}
	return out
}

func splitKey(key string) []string {
	out := []string{}
	start := 0
	for i := 0; i < len(key); i++ {
		if key[i] == '/' {
			out = append(out, key[start:i])
			start = i + 1
		}
	}
	return append(out, key[start:])
}

func keyTypeFor(rt *Runtime, route, name string) string {
	if rt == nil {
		return ""
	}
	for i := range rt.Cfg.Routes {
		if rt.Cfg.Routes[i].Name != route {
			continue
		}
		for _, rl := range rt.Cfg.Routes[i].RateLimits {
			if rl.Name == name {
				return rl.Key
			}
		}
	}
	return ""
}

func (s *Supervisor) ReloadNow() error { return s.applyConfig("admin-api") }

func (s *Supervisor) Version() string       { return s.version }
func (s *Supervisor) Uptime() time.Duration { return time.Since(s.start) }
