package pool

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- test infrastructure ---

type fakeClock struct {
	mu  sync.Mutex
	cur time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{cur: time.Unix(1_700_000_000, 0).UTC()}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.cur = c.cur.Add(d)
	c.mu.Unlock()
}

type recSink struct {
	mu     sync.Mutex
	health map[string][]float64
	circ   map[string][]float64
	codes  map[string][]int
	lats   int
}

func newRecSink() *recSink {
	return &recSink{
		health: map[string][]float64{},
		circ:   map[string][]float64{},
		codes:  map[string][]int{},
	}
}

func (r *recSink) SetTargetHealth(poolName, tgt string, healthy float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.health[tgt] = append(r.health[tgt], healthy)
}

func (r *recSink) SetCircuitState(poolName, tgt string, state float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.circ[tgt] = append(r.circ[tgt], state)
}

func (r *recSink) AddUpstreamRequest(poolName, tgt string, code int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.codes[tgt] = append(r.codes[tgt], code)
}

func (r *recSink) ObserveUpstreamLatency(poolName, tgt string, seconds float64) {
	r.mu.Lock()
	r.lats++
	r.mu.Unlock()
}

func (r *recSink) healthSeq(t *testing.T, tgt string) []float64 {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]float64, len(r.health[tgt]))
	copy(out, r.health[tgt])
	return out
}

func (r *recSink) circSeq(t *testing.T, tgt string) []float64 {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]float64, len(r.circ[tgt]))
	copy(out, r.circ[tgt])
	return out
}

func baseCfg(lb string) Config {
	return Config{
		Name:           "pl",
		LoadBalance:    lb,
		ConnectTimeout: 200 * time.Millisecond,
		ReadTimeout:    200 * time.Millisecond,
		WriteTimeout:   200 * time.Millisecond,
		Keepalive:      30 * time.Second,
		MaxIdlePerHost: 8,
		VerifyTLS:      true,
		Passive:        PassiveConfig{Window: time.Hour, Failures: 1 << 30, EjectionTime: 30 * time.Second},
		Breaker:        BreakerConfig{Failures: 1 << 30, Window: time.Hour, Cooldown: time.Second, HalfOpenProbes: 1},
	}
}

func addTarget(cfg *Config, url string, weight int) {
	cfg.Targets = append(cfg.Targets, TargetConfig{URL: url, Weight: weight})
}

func okOutcome() Outcome  { return Outcome{Success: true, Status: 200, Latency: 2 * time.Millisecond} }
func failOutcome() Outcome {
	return Outcome{Success: false, ErrClass: ErrConnect, Latency: time.Millisecond}
}

func statusByName(t *testing.T, p Pool, name string) TargetStatus {
	t.Helper()
	for _, s := range p.Status() {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("target %q not found in status", name)
	return TargetStatus{}
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// --- construction & transport ---

func TestNewBuildsTargetsTransportAndDefaults(t *testing.T) {
	cfg := baseCfg("round_robin")
	addTarget(&cfg, "http://a.test:80", 1)
	addTarget(&cfg, "http://b.test:80", 2)
	p := New(cfg)
	if got := p.Name(); got != "pl" {
		t.Fatalf("name = %q", got)
	}
	if len(p.Status()) != 2 {
		t.Fatalf("status len = %d", len(p.Status()))
	}
	s0 := statusByName(t, p, "pl[0]")
	if s0.URL != "http://a.test:80" || s0.Weight != 1 || s0.State != StateHealthy {
		t.Fatalf("pl[0] snapshot wrong: %+v", s0.Target)
	}
	tr := p.Transport()
	if tr == nil || tr != p.Transport() {
		t.Fatalf("Transport() unstable or nil")
	}
	if tr.MaxIdleConnsPerHost != 8 {
		t.Fatalf("MaxIdleConnsPerHost = %d", tr.MaxIdleConnsPerHost)
	}
	if tr.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("VerifyTLS=true should keep verification on")
	}
}

func TestTransportHonorsVerifyTLSOff(t *testing.T) {
	cfg := baseCfg("round_robin")
	cfg.VerifyTLS = false
	addTarget(&cfg, "http://a.test:80", 1)
	p := New(cfg)
	if !p.Transport().TLSClientConfig.InsecureSkipVerify {
		t.Fatal("VerifyTLS=false should disable verification")
	}
	custom := &http.Client{}
	cfg.Active.Enabled = true
	cfg.Active.Client = custom
	p2 := New(cfg)
	if p2.client != custom {
		t.Fatal("Active.Client override not honored")
	}
}

func TestUnknownLoadBalancerFallsBackToRoundRobin(t *testing.T) {
	cfg := baseCfg("bogus")
	addTarget(&cfg, "http://a.test:80", 1)
	addTarget(&cfg, "http://b.test:80", 1)
	p := New(cfg)
	counts := map[string]int{}
	for i := 0; i < 8; i++ {
		tgt, err := p.Pick("")
		if err != nil {
			t.Fatalf("pick: %v", err)
		}
		counts[tgt.Name]++
		p.Done(tgt, okOutcome())
	}
	if counts["pl[0]"] != 4 || counts["pl[1]"] != 4 {
		t.Fatalf("fallback not round robin: %v", counts)
	}
}

// --- round robin ---

func TestRoundRobinExactWeightDistribution(t *testing.T) {
	cfg := baseCfg("round_robin")
	addTarget(&cfg, "http://a.test:80", 3)
	addTarget(&cfg, "http://b.test:80", 1)
	p := New(cfg)
	counts := map[string]int{}
	for i := 0; i < 40; i++ {
		tgt, err := p.Pick("")
		if err != nil {
			t.Fatalf("pick: %v", err)
		}
		counts[tgt.Name]++
		p.Done(tgt, okOutcome())
	}
	if counts["pl[0]"] != 30 || counts["pl[1]"] != 10 {
		t.Fatalf("weights 3/1 over 40 picks: got %v", counts)
	}
}

func TestRoundRobinEqualWeightsEvenSplit(t *testing.T) {
	cfg := baseCfg("round_robin")
	for _, u := range []string{"http://a:1", "http://b:1", "http://c:1"} {
		addTarget(&cfg, u, 1)
	}
	p := New(cfg)
	counts := map[string]int{}
	for i := 0; i < 30; i++ {
		tgt, _ := p.Pick("")
		counts[tgt.Name]++
		p.Done(tgt, okOutcome())
	}
	for _, n := range []string{"pl[0]", "pl[1]", "pl[2]"} {
		if counts[n] != 10 {
			t.Fatalf("%s = %d, want 10 (%v)", n, counts[n], counts)
		}
	}
}

func TestRoundRobinSkipsEjectedTarget(t *testing.T) {
	fc := newFakeClock()
	cfg := baseCfg("round_robin")
	cfg.Passive = PassiveConfig{Window: time.Minute, Failures: 1, EjectionTime: 30 * time.Second}
	for _, u := range []string{"http://a:1", "http://b:1", "http://c:1"} {
		addTarget(&cfg, u, 1)
	}
	p := New(cfg)
	p.nowFunc = fc.Now

	victim, err := p.Pick("")
	if err != nil {
		t.Fatalf("pick: %v", err)
	}
	p.Done(victim, failOutcome())

	st := statusByName(t, p, victim.Name)
	if st.State != StateEjected {
		t.Fatalf("state = %v, want ejected", st.State)
	}
	for i := 0; i < 60; i++ {
		tgt, err := p.Pick("")
		if err != nil {
			t.Fatalf("pick %d: %v", i, err)
		}
		if tgt.Name == victim.Name {
			t.Fatalf("ejected target %s picked at iteration %d", victim.Name, i)
		}
		p.Done(tgt, okOutcome())
	}
	fc.Advance(31 * time.Second)
	var sawVictim bool
	for i := 0; i < 50 && !sawVictim; i++ {
		tgt, err := p.Pick("")
		if err != nil {
			t.Fatalf("pick: %v", err)
		}
		if tgt.Name == victim.Name {
			sawVictim = true
		}
		p.Done(tgt, okOutcome())
	}
	if !sawVictim {
		t.Fatal("ejected target never became eligible after expiry")
	}
	waitSt := statusByName(t, p, victim.Name)
	if waitSt.State != StateHealthy {
		t.Fatalf("state after probe success = %v, want healthy", waitSt.State)
	}
}

func TestWeightZeroTargetNeverSelected(t *testing.T) {
	cfg := baseCfg("round_robin")
	addTarget(&cfg, "http://zero:1", 0)
	addTarget(&cfg, "http://heavy:1", 5)
	p := New(cfg)
	for i := 0; i < 30; i++ {
		tgt, err := p.Pick("")
		if err != nil {
			t.Fatalf("pick: %v", err)
		}
		if tgt.Name != "pl[1]" {
			t.Fatalf("weight-zero pool returned %s", tgt.Name)
		}
		p.Done(tgt, okOutcome())
	}
	z := statusByName(t, p, "pl[0]")
	if z.TotalReq != 0 {
		t.Fatalf("weight-zero target received traffic: %+v", z)
	}
}

// --- least connections ---

func TestLeastConnPrefersLowerInflight(t *testing.T) {
	cfg := baseCfg("least_connections")
	for _, u := range []string{"http://a:1", "http://b:1", "http://c:1"} {
		addTarget(&cfg, u, 1)
	}
	p := New(cfg)
	first, err := p.Pick("")
	if err != nil {
		t.Fatalf("pick: %v", err)
	}
	second, _ := p.Pick("")
	third, _ := p.Pick("")
	if first.Name == second.Name || second.Name == third.Name || first.Name == third.Name {
		t.Fatalf("initial tie-break not rotating: %s %s %s", first.Name, second.Name, third.Name)
	}
	p.Done(first, okOutcome())
	next, err := p.Pick("")
	if err != nil {
		t.Fatalf("pick: %v", err)
	}
	if next != first {
		t.Fatalf("least-conn ignored lower inflight: got %s want %s", next.Name, first.Name)
	}
	p.Done(second, okOutcome())
	p.Done(third, okOutcome())
	p.Done(next, okOutcome())
	if sum := inflightSum(t, p); sum != 0 {
		t.Fatalf("inflight leak: %d", sum)
	}
}

func TestLeastConnTieRotatesRoundRobin(t *testing.T) {
	cfg := baseCfg("least_connections")
	addTarget(&cfg, "http://a:1", 1)
	addTarget(&cfg, "http://b:1", 1)
	p := New(cfg)
	want := "pl[0]"
	for i := 0; i < 10; i++ {
		tgt, err := p.Pick("")
		if err != nil {
			t.Fatalf("pick %d: %v", i, err)
		}
		if tgt.Name != want {
			t.Fatalf("iteration %d: got %s want %s", i, tgt.Name, want)
		}
		p.Done(tgt, okOutcome())
		if want == "pl[0]" {
			want = "pl[1]"
		} else {
			want = "pl[0]"
		}
	}
}

// --- ring hash ---

func ringMapping(t *testing.T, p Pool, keys []string) map[string]string {
	t.Helper()
	m := make(map[string]string, len(keys))
	for _, k := range keys {
		tgt, err := p.Pick(k)
		if err != nil {
			t.Fatalf("pick %q: %v", k, err)
		}
		m[k] = tgt.Name
		p.Done(tgt, okOutcome())
	}
	return m
}

func TestRingHashStableMapping(t *testing.T) {
	cfg := baseCfg("ring_hash")
	for i := 0; i < 5; i++ {
		addTarget(&cfg, fmt.Sprintf("http://t%d:1", i), 1)
	}
	p := New(cfg)
	for _, key := range []string{"user:42", "session:abc", "tenant:7"} {
		want, err := p.Pick(key)
		if err != nil {
			t.Fatalf("pick: %v", err)
		}
		p.Done(want, okOutcome())
		for i := 0; i < 24; i++ {
			got, err := p.Pick(key)
			if err != nil {
				t.Fatalf("pick: %v", err)
			}
			if got.Name != want.Name {
				t.Fatalf("key %q moved from %s to %s", key, want.Name, got.Name)
			}
			p.Done(got, okOutcome())
		}
	}
}

func TestRingHashBoundedChurnOnRemoval(t *testing.T) {
	full := baseCfg("ring_hash")
	for i := 0; i < 5; i++ {
		addTarget(&full, fmt.Sprintf("http://t%d:1", i), 1)
	}
	reduced := baseCfg("ring_hash")
	for i := 0; i < 4; i++ {
		addTarget(&reduced, fmt.Sprintf("http://t%d:1", i), 1)
	}
	pf, pr := New(full), New(reduced)
	rng := rand.New(rand.NewSource(42))
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", rng.Int63())
	}
	before := ringMapping(t, pf, keys)
	after := ringMapping(t, pr, keys)
	churn := 0
	for _, k := range keys {
		if before[k] != after[k] {
			churn++
		}
	}
	if churn == 0 {
		t.Fatal("removing a target remapped nothing; ring broken")
	}
	if churn >= 35 {
		t.Fatalf("churn %d/100 exceeds bound for losing 1 of 5 targets", churn)
	}
}

func TestRingHashWeightSkew(t *testing.T) {
	cfg := baseCfg("ring_hash")
	for i := 0; i < 4; i++ {
		addTarget(&cfg, fmt.Sprintf("http://light%d:1", i), 1)
	}
	addTarget(&cfg, "http://heavy:1", 8)
	p := New(cfg)
	rng := rand.New(rand.NewSource(7))
	keys := make([]string, 400)
	for i := range keys {
		keys[i] = fmt.Sprintf("sk-%d", rng.Int63())
	}
	counts := map[string]int{}
	for _, k := range keys {
		tgt, err := p.Pick(k)
		if err != nil {
			t.Fatalf("pick: %v", err)
		}
		counts[tgt.Name]++
		p.Done(tgt, okOutcome())
	}
	if counts["pl[4]"] <= len(keys)*45/100 {
		t.Fatalf("heavy target share %d/400 below expectation", counts["pl[4]"])
	}
	for i := 0; i < 4; i++ {
		n := fmt.Sprintf("pl[%d]", i)
		if counts[n] >= len(keys)*25/100 {
			t.Fatalf("light target %s took %d/400", n, counts[n])
		}
	}
}

func TestRingHashEmptyKeyRotates(t *testing.T) {
	cfg := baseCfg("ring_hash")
	for _, u := range []string{"http://a:1", "http://b:1", "http://c:1"} {
		addTarget(&cfg, u, 1)
	}
	p := New(cfg)
	counts := map[string]int{}
	for i := 0; i < 12; i++ {
		tgt, err := p.Pick("")
		if err != nil {
			t.Fatalf("pick: %v", err)
		}
		counts[tgt.Name]++
		p.Done(tgt, okOutcome())
	}
	for _, n := range []string{"pl[0]", "pl[1]", "pl[2]"} {
		if counts[n] != 4 {
			t.Fatalf("empty-key rotation uneven: %v", counts)
		}
	}
}

// --- passive ejection ---

func TestPassiveSlidingWindowIgnoresOldFailures(t *testing.T) {
	fc := newFakeClock()
	cfg := baseCfg("round_robin")
	cfg.Passive = PassiveConfig{Window: 60 * time.Second, Failures: 3, EjectionTime: 30 * time.Second}
	addTarget(&cfg, "http://a:1", 1)
	p := New(cfg)
	p.nowFunc = fc.Now

	tgt, _ := p.Pick("")
	p.Done(tgt, failOutcome())
	fc.Advance(61 * time.Second)
	tgt, _ = p.Pick("")
	p.Done(tgt, failOutcome())
	if !p.Healthy(1) {
		t.Fatalf("two fresh failures should stay under threshold")
	}
	tgt, _ = p.Pick("")
	p.Done(tgt, failOutcome())
	tgt, _ = p.Pick("")
	p.Done(tgt, failOutcome())
	if _, err := p.Pick(""); !errors.Is(err, ErrNoHealthy) {
		t.Fatalf("third fresh failure should eject; got %v", err)
	}
	if p.Healthy(1) {
		t.Fatal("target should be ejected after three fresh failures")
	}
	st := statusByName(t, p, "pl[0]")
	if st.State != StateEjected || st.TotalFail != 4 || st.TotalReq != 4 {
		t.Fatalf("unexpected snapshot: %+v", st)
	}
}

func TestPassiveEjectionLifecycle(t *testing.T) {
	fc := newFakeClock()
	cfg := baseCfg("round_robin")
	cfg.Passive = PassiveConfig{Window: time.Hour, Failures: 3, EjectionTime: 30 * time.Second}
	cfg.Active.HealthyThreshold = 2
	addTarget(&cfg, "http://a:1", 1)
	p := New(cfg)
	p.nowFunc = fc.Now

	name := ""
	for i := 0; i < 2; i++ {
		tgt, err := p.Pick("")
		if err != nil {
			t.Fatalf("pick %d: %v", i, err)
		}
		name = tgt.Name
		p.Done(tgt, failOutcome())
	}
	if !p.Healthy(1) {
		t.Fatal("below threshold target should remain pickable")
	}
	tgt, _ := p.Pick("")
	p.Done(tgt, failOutcome())
	if _, err := p.Pick(""); !errors.Is(err, ErrNoHealthy) {
		t.Fatalf("expected ErrNoHealthy after ejection, got %v", err)
	}
	st := statusByName(t, p, name)
	if st.State != StateEjected || st.CircuitOpen || st.Failures != 0 || st.TotalFail != 3 {
		t.Fatalf("post-ejection snapshot wrong: %+v", st)
	}

	fc.Advance(29 * time.Second)
	if _, err := p.Pick(""); !errors.Is(err, ErrNoHealthy) {
		t.Fatalf("still inside ejection window: %v", err)
	}
	fc.Advance(2 * time.Second)

	tgt, err := p.Pick("")
	if err != nil {
		t.Fatalf("probing pick failed: %v", err)
	}
	if s := statusByName(t, p, name).State; s != StateProbing {
		t.Fatalf("state = %v, want probing", s)
	}
	p.Done(tgt, okOutcome())
	if s := statusByName(t, p, name).State; s != StateProbing {
		t.Fatalf("one success below threshold, state = %v", s)
	}
	tgt, err = p.Pick("")
	if err != nil {
		t.Fatalf("second probe pick failed: %v", err)
	}
	p.Done(tgt, okOutcome())
	st = statusByName(t, p, name)
	if st.State != StateHealthy {
		t.Fatalf("state = %v, want healthy after threshold successes", st.State)
	}
	if _, err := p.Pick(""); err != nil {
		t.Fatalf("healthy target unpickable: %v", err)
	}
}

func TestProbeFailureReEjects(t *testing.T) {
	fc := newFakeClock()
	cfg := baseCfg("round_robin")
	cfg.Passive = PassiveConfig{Window: time.Hour, Failures: 1, EjectionTime: 30 * time.Second}
	addTarget(&cfg, "http://a:1", 1)
	p := New(cfg)
	p.nowFunc = fc.Now

	tgt, _ := p.Pick("")
	p.Done(tgt, failOutcome())
	if _, err := p.Pick(""); !errors.Is(err, ErrNoHealthy) {
		t.Fatalf("want ejected: %v", err)
	}
	fc.Advance(31 * time.Second)
	tgt, err := p.Pick("")
	if err != nil {
		t.Fatalf("probe pick: %v", err)
	}
	p.Done(tgt, failOutcome())
	if _, err := p.Pick(""); !errors.Is(err, ErrNoHealthy) {
		t.Fatalf("failed probe should re-eject: %v", err)
	}
	st := statusByName(t, p, "pl[0]")
	if st.State != StateEjected {
		t.Fatalf("state = %v, want ejected", st.State)
	}
	fc.Advance(31 * time.Second)
	if _, err := p.Pick(""); err != nil {
		t.Fatalf("re-probe after re-ejection failed: %v", err)
	}
}

func TestProbingCapAllowsSingleConcurrentProbe(t *testing.T) {
	fc := newFakeClock()
	cfg := baseCfg("round_robin")
	cfg.Passive = PassiveConfig{Window: time.Hour, Failures: 1, EjectionTime: 30 * time.Second}
	addTarget(&cfg, "http://a:1", 1)
	p := New(cfg)
	p.nowFunc = fc.Now

	tgt, _ := p.Pick("")
	p.Done(tgt, failOutcome())
	fc.Advance(31 * time.Second)
	probe, err := p.Pick("")
	if err != nil {
		t.Fatalf("probe pick: %v", err)
	}
	if _, err := p.Pick(""); !errors.Is(err, ErrNoHealthy) {
		t.Fatalf("second concurrent probe allowed: %v", err)
	}
	p.Done(probe, okOutcome())
	if _, err := p.Pick(""); err != nil {
		t.Fatalf("pick after probe completion failed: %v", err)
	}
}

// --- state machine wording & active-driven transitions ---

func TestStateMachineTransitionsAndLabels(t *testing.T) {
	cfg := baseCfg("round_robin")
	cfg.Active.HealthyThreshold = 2
	cfg.Active.UnhealthyThreshold = 2
	addTarget(&cfg, "http://a:1", 1)
	p := New(cfg)
	st := p.states[0]

	if got := StateHealthy.String(); got != "healthy" {
		t.Fatalf("label = %q", got)
	}
	st.activeResult(false, time.Now())
	st.activeResult(false, time.Now())
	if st.status(time.Now()).State != StateDown {
		t.Fatalf("want down after %d failures", st.unhealthyThreshold)
	}
	if got := StateDown.String(); got != "down" {
		t.Fatalf("label = %q", got)
	}
	if _, err := p.Pick(""); !errors.Is(err, ErrNoHealthy) {
		t.Fatalf("down target pickable: %v", err)
	}
	st.activeResult(true, time.Now())
	now := time.Now()
	if s := st.status(now).State; s != StateProbing {
		t.Fatalf("want probing after first recovery success, got %v", s)
	}
	if got := StateProbing.String(); got != "probing" {
		t.Fatalf("label = %q", got)
	}
	if _, err := p.Pick(""); err != nil {
		t.Fatalf("probing target should accept limited traffic: %v", err)
	}
	st.activeResult(true, now)
	if s := st.status(now).State; s != StateHealthy {
		t.Fatalf("want healthy after threshold successes, got %v", s)
	}
	st.mu.Lock()
	st.ejectLocked(now)
	st.mu.Unlock()
	if got := statusByName(t, p, "pl[0]").State.String(); got != "ejected" {
		t.Fatalf("label = %q", got)
	}
}

// --- circuit breaker ---

func TestCircuitBreakerLifecycle(t *testing.T) {
	fc := newFakeClock()
	cfg := baseCfg("round_robin")
	cfg.Breaker = BreakerConfig{Failures: 2, Window: time.Hour, Cooldown: 10 * time.Second, HalfOpenProbes: 1}
	addTarget(&cfg, "http://a:1", 1)
	p := New(cfg)
	p.nowFunc = fc.Now

	for i := 0; i < 2; i++ {
		tgt, _ := p.Pick("")
		p.Done(tgt, failOutcome())
	}
	st := statusByName(t, p, "pl[0]")
	if !st.CircuitOpen {
		t.Fatal("breaker did not open after consecutive failures")
	}
	if st.State != StateHealthy {
		t.Fatalf("breaker open must not change health state, got %v", st.State)
	}
	if _, err := p.Pick(""); !errors.Is(err, ErrNoHealthy) {
		t.Fatalf("open breaker should block picks: %v", err)
	}

	fc.Advance(9 * time.Second)
	if _, err := p.Pick(""); !errors.Is(err, ErrNoHealthy) {
		t.Fatalf("cooldown not elapsed: %v", err)
	}
	fc.Advance(2 * time.Second)
	tgt, err := p.Pick("")
	if err != nil {
		t.Fatalf("half-open probe pick: %v", err)
	}
	if statusByName(t, p, "pl[0]").CircuitOpen {
		t.Fatal("half-open phase reported as open")
	}
	p.Done(tgt, failOutcome())
	st = statusByName(t, p, "pl[0]")
	if !st.CircuitOpen {
		t.Fatal("failed probe must reopen the circuit")
	}
	if _, err := p.Pick(""); !errors.Is(err, ErrNoHealthy) {
		t.Fatalf("reopened breaker should block picks: %v", err)
	}

	fc.Advance(10 * time.Second)
	tgt, err = p.Pick("")
	if err != nil {
		t.Fatalf("recovery probe pick: %v", err)
	}
	p.Done(tgt, okOutcome())
	st = statusByName(t, p, "pl[0]")
	if st.CircuitOpen {
		t.Fatal("successful probe must close the circuit")
	}
	for i := 0; i < 5; i++ {
		tgt, err := p.Pick("")
		if err != nil {
			t.Fatalf("closed circuit blocking picks: %v", err)
		}
		p.Done(tgt, okOutcome())
	}
}

func TestCircuitBreakerHalfOpenProbeLimits(t *testing.T) {
	fc := newFakeClock()

	t.Run("one_probe", func(t *testing.T) {
		cfg := baseCfg("round_robin")
		cfg.Breaker = BreakerConfig{Failures: 1, Window: time.Hour, Cooldown: 10 * time.Second, HalfOpenProbes: 1}
		addTarget(&cfg, "http://a:1", 1)
		p := New(cfg)
		p.nowFunc = fc.Now
		tgt, _ := p.Pick("")
		p.Done(tgt, failOutcome())
		fc.Advance(10 * time.Second)
		probe, err := p.Pick("")
		if err != nil {
			t.Fatalf("probe pick: %v", err)
		}
		if _, err := p.Pick(""); !errors.Is(err, ErrNoHealthy) {
			t.Fatalf("extra half-open probe allowed beyond limit: %v", err)
		}
		p.Done(probe, okOutcome())
		if _, err := p.Pick(""); err != nil {
			t.Fatalf("closed circuit should admit traffic: %v", err)
		}
	})

	t.Run("two_probes_first_success_closes", func(t *testing.T) {
		cfg := baseCfg("round_robin")
		cfg.Breaker = BreakerConfig{Failures: 1, Window: time.Hour, Cooldown: 10 * time.Second, HalfOpenProbes: 2}
		addTarget(&cfg, "http://a:1", 1)
		p := New(cfg)
		p.nowFunc = fc.Now
		tgt, _ := p.Pick("")
		p.Done(tgt, failOutcome())
		fc.Advance(10 * time.Second)
		a, err := p.Pick("")
		if err != nil {
			t.Fatalf("probe a: %v", err)
		}
		b, err := p.Pick("")
		if err != nil {
			t.Fatalf("probe b: %v", err)
		}
		if _, err := p.Pick(""); !errors.Is(err, ErrNoHealthy) {
			t.Fatalf("third concurrent probe allowed with limit 2: %v", err)
		}
		p.Done(a, okOutcome())
		if statusByName(t, p, "pl[0]").CircuitOpen {
			t.Fatal("first probe success should close the circuit")
		}
		p.Done(b, okOutcome())
		if sum := inflightSum(t, p); sum != 0 {
			t.Fatalf("inflight leak: %d", sum)
		}
	})
}

// --- active health checking ---

func TestActiveHealthDownAndRecovery(t *testing.T) {
	var failing atomic.Bool
	srvBad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failing.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srvBad.Close()
	srvGood := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srvGood.Close()

	cfg := baseCfg("round_robin")
	addTarget(&cfg, srvGood.URL, 1)
	addTarget(&cfg, srvBad.URL, 1)
	cfg.Active = ActiveConfig{
		Enabled:            true,
		Interval:           10 * time.Millisecond,
		Timeout:            500 * time.Millisecond,
		Path:               "/",
		Method:             http.MethodGet,
		HealthyThreshold:   2,
		UnhealthyThreshold: 2,
	}
	p := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)
	t.Cleanup(p.Close)

	waitFor(t, 3*time.Second, "both targets healthy", func() bool {
		ss := p.Status()
		return ss[0].State == StateHealthy && ss[1].State == StateHealthy
	})

	failing.Store(true)
	waitFor(t, 3*time.Second, "bad target down", func() bool {
		return statusByName(t, p, "pl[1]").State == StateDown
	})
	for i := 0; i < 100; i++ {
		tgt, err := p.Pick("")
		if err != nil {
			t.Fatalf("pick while degraded: %v", err)
		}
		if tgt.Name == "pl[1]" {
			t.Fatalf("down target served traffic at iteration %d", i)
		}
		p.Done(tgt, okOutcome())
	}

	failing.Store(false)
	waitFor(t, 3*time.Second, "bad target recovered", func() bool {
		return statusByName(t, p, "pl[1]").State == StateHealthy
	})
	waitFor(t, 3*time.Second, "recovered target receives traffic", func() bool {
		tgt, err := p.Pick("")
		if err != nil {
			return false
		}
		p.Done(tgt, okOutcome())
		return tgt.Name == "pl[1]"
	})
	cancel()
}

func TestActiveChecksSerializedPerTarget(t *testing.T) {
	var live, peak atomic.Int32
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := live.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(25 * time.Millisecond)
		live.Add(-1)
		w.WriteHeader(http.StatusOK)
	}))
	defer slow.Close()

	cfg := baseCfg("round_robin")
	addTarget(&cfg, slow.URL, 1)
	cfg.Active = ActiveConfig{
		Enabled:            true,
		Interval:           5 * time.Millisecond,
		Timeout:            time.Second,
		Path:               "/",
		Method:             http.MethodGet,
		HealthyThreshold:   1,
		UnhealthyThreshold: 2,
	}
	p := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)
	time.Sleep(120 * time.Millisecond)
	p.Close()
	if peak.Load() > 1 {
		t.Fatalf("active checks overlapped on same target (peak=%d)", peak.Load())
	}
}

func TestStartDisabledAndContextCancelSafe(t *testing.T) {
	cfg := baseCfg("round_robin")
	addTarget(&cfg, "http://a:1", 1)
	cfg.Active.Enabled = false
	p := New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)
	p.Start(ctx)
	cancel()
	p.Close()
	p.Close()
	if _, err := p.Pick(""); err != nil {
		t.Fatalf("pick after close: %v", err)
	}
}

// --- drain / close / healthy / misc ---

func TestDrainKeepsPickingAndCloseIsIdempotent(t *testing.T) {
	cfg := baseCfg("round_robin")
	addTarget(&cfg, "http://a:1", 1)
	p := New(cfg)
	if p.Draining() {
		t.Fatal("fresh pool reports draining")
	}
	p.Drain(20 * time.Millisecond)
	if !p.Draining() {
		t.Fatal("drain flag not set")
	}
	tgt, err := p.Pick("")
	if err != nil {
		t.Fatalf("pick must still serve after Drain: %v", err)
	}
	p.Done(tgt, okOutcome())
	time.Sleep(50 * time.Millisecond)
	p.Close()
	p.Close()
}

func TestHealthyAccessor(t *testing.T) {
	fc := newFakeClock()
	cfg := baseCfg("round_robin")
	cfg.Passive = PassiveConfig{Window: time.Hour, Failures: 1, EjectionTime: 30 * time.Second}
	addTarget(&cfg, "http://a:1", 1)
	addTarget(&cfg, "http://b:1", 1)
	p := New(cfg)
	p.nowFunc = fc.Now
	if !p.Healthy(2) || !p.Healthy(1) {
		t.Fatal("both targets should be healthy")
	}
	tgt, _ := p.Pick("")
	p.Done(tgt, failOutcome())
	if !p.Healthy(1) {
		t.Fatal("one healthy target should satisfy min=1")
	}
	if p.Healthy(2) {
		t.Fatal("min=2 should fail with one target ejected")
	}
}

func TestDoneWithUnknownTargetIgnored(t *testing.T) {
	cfg := baseCfg("round_robin")
	addTarget(&cfg, "http://a:1", 1)
	p := New(cfg)
	p.Done(&Target{Name: "ghost", URL: "http://x:1", Weight: 1}, okOutcome())
	p.Done(nil, okOutcome())
	st := statusByName(t, p, "pl[0]")
	if st.TotalReq != 0 || st.TotalFail != 0 {
		t.Fatalf("counters polluted by foreign Done: %+v", st)
	}
}

func TestErrNoHealthyWhenNothingEligible(t *testing.T) {
	cfg := baseCfg("round_robin")
	addTarget(&cfg, "http://a:1", 0)
	p := New(cfg)
	if _, err := p.Pick("k"); !errors.Is(err, ErrNoHealthy) {
		t.Fatalf("all-zero weights: %v", err)
	}
	cfg2 := baseCfg("round_robin")
	cfg2.Targets = nil
	p2 := New(cfg2)
	if _, err := p2.Pick(""); !errors.Is(err, ErrNoHealthy) {
		t.Fatalf("no targets: %v", err)
	}
}

func inflightSum(t *testing.T, p Pool) int64 {
	t.Helper()
	var sum int64
	for _, s := range p.Status() {
		sum += s.Inflight
	}
	return sum
}

// --- metrics sink ---

func TestMetricsSinkEvents(t *testing.T) {
	fc := newFakeClock()
	cfg := baseCfg("round_robin")
	cfg.Passive = PassiveConfig{Window: time.Hour, Failures: 2, EjectionTime: 30 * time.Second}
	cfg.Active.HealthyThreshold = 1
	addTarget(&cfg, "http://a:1", 1)
	p := New(cfg)
	p.nowFunc = fc.Now
	sink := newRecSink()
	p.SetMetrics(sink)

	tgt, _ := p.Pick("")
	p.Done(tgt, Outcome{Success: false, ErrClass: ErrConnect})
	tgt, _ = p.Pick("")
	p.Done(tgt, Outcome{Success: false, Status: 500, ErrClass: ErrHTTP5xx})
	if got := sink.healthSeq(t, "pl[0]"); len(got) != 2 || got[0] != 1 || got[1] != 0 {
		t.Fatalf("health seq = %v", got)
	}
	fc.Advance(31 * time.Second)
	tgt, err := p.Pick("")
	if err != nil {
		t.Fatalf("probe pick: %v", err)
	}
	p.Done(tgt, okOutcome())
	if got := sink.healthSeq(t, "pl[0]"); len(got) != 4 || got[0] != 1 || got[1] != 0 || got[2] != 0 || got[3] != 1 {
		t.Fatalf("health seq = %v", got)
	}
	sink.mu.Lock()
	codes := append([]int(nil), sink.codes["pl[0]"]...)
	lats := sink.lats
	sink.mu.Unlock()
	if len(codes) != 3 || codes[0] != 0 || codes[1] != 500 || codes[2] != 200 {
		t.Fatalf("request codes = %v", codes)
	}
	if lats != 3 {
		t.Fatalf("latency observations = %d", lats)
	}

	bcfg := baseCfg("round_robin")
	bcfg.Breaker = BreakerConfig{Failures: 1, Window: time.Hour, Cooldown: 5 * time.Second, HalfOpenProbes: 1}
	addTarget(&bcfg, "http://b:1", 1)
	bp := New(bcfg)
	bp.nowFunc = fc.Now
	bsink := newRecSink()
	bp.SetMetrics(bsink)
	bt, _ := bp.Pick("")
	bp.Done(bt, failOutcome())
	fc.Advance(6 * time.Second)
	bt, err = bp.Pick("")
	if err != nil {
		t.Fatalf("half-open pick: %v", err)
	}
	bp.Done(bt, okOutcome())
	want := []float64{0, 1, 0.5, 0}
	got := bsink.circSeq(t, "pl[0]")
	if len(got) != len(want) {
		t.Fatalf("circuit seq = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("circuit seq = %v, want %v", got, want)
		}
	}
}

// --- concurrency hammers ---

func hammer(t *testing.T, lb string, workers, iters int, keyed bool) {
	t.Helper()
	cfg := baseCfg(lb)
	weights := []int{3, 1, 2, 1}
	for i, w := range weights {
		addTarget(&cfg, fmt.Sprintf("http://h%d:1", i), w)
	}
	p := New(cfg)

	var picks, fails int64
	var wg sync.WaitGroup
	wg.Add(workers)
	for g := 0; g < workers; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				key := ""
				if keyed {
					key = fmt.Sprintf("k%d", (g*31+i)%97)
				}
				tgt, err := p.Pick(key)
				if err != nil {
					atomic.AddInt64(&fails, -1)
					continue
				}
				atomic.AddInt64(&picks, 1)
				var oc Outcome
				if i%5 == 4 {
					classes := []ErrClass{ErrConnect, ErrResponse, ErrTimeout, ErrHTTP5xx}
					oc = Outcome{Success: false, ErrClass: classes[(g+i)%len(classes)]}
					atomic.AddInt64(&fails, 1)
				} else {
					statuses := []int{200, 201, 301, 404}
					oc = Outcome{Success: true, Status: statuses[i%len(statuses)], Latency: time.Duration((g*7+i)%13) * time.Millisecond}
				}
				p.Done(tgt, oc)
			}
		}(g)
	}
	wg.Wait()

	var sumInflight, sumReq, sumFails int64
	for _, s := range p.Status() {
		sumInflight += s.Inflight
		sumReq += int64(s.TotalReq)
		sumFails += int64(s.TotalFail)
	}
	if sumInflight != 0 {
		t.Fatalf("inflight leak: sum=%d", sumInflight)
	}
	if sumReq != picks {
		t.Fatalf("total requests %d != picks %d", sumReq, picks)
	}
	if sumFails != fails {
		t.Fatalf("total failures %d != injected %d", sumFails, fails)
	}
}

func TestConcurrencyHammerRoundRobin(t *testing.T) {
	hammer(t, "round_robin", 64, 1000, false)
}

func TestConcurrencyHammerLeastConn(t *testing.T) {
	hammer(t, "least_connections", 64, 1000, false)
}

func TestConcurrencyHammerRingHash(t *testing.T) {
	hammer(t, "ring_hash", 32, 1000, true)
}

func TestConcurrencyHammerWithEjections(t *testing.T) {
	cfg := baseCfg("round_robin")
	cfg.Passive = PassiveConfig{Window: time.Hour, Failures: 500, EjectionTime: 50 * time.Millisecond}
	cfg.Active.HealthyThreshold = 1
	for i, w := range []int{2, 1, 1} {
		addTarget(&cfg, fmt.Sprintf("http://e%d:1", i), w)
	}
	p := New(cfg)
	var wg sync.WaitGroup
	wg.Add(16)
	for g := 0; g < 16; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 800; i++ {
				tgt, err := p.Pick("")
				if err != nil {
					continue
				}
				if (g+i)%7 == 0 {
					p.Done(tgt, Outcome{Success: false, ErrClass: ErrTimeout})
				} else {
					p.Done(tgt, okOutcome())
				}
			}
		}(g)
	}
	wg.Wait()
	if sum := inflightSum(t, p); sum != 0 {
		t.Fatalf("inflight leak under ejection churn: %d", sum)
	}
}
