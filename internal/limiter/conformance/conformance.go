// Package conformance defines the behavioural contract every Gatewright
// rate-limiting strategy must satisfy (DESIGN.md section 1: "the decision is
// total", time supplied never read, cost explicit).
//
// The suite drives a Limiter strictly through its public surface with
// explicit, deterministic instants -- never the wall clock -- so any strategy,
// on either audited driver (in-memory or a shared Backend), is verified by
// one call:
//
//	conformance.RunSuite(t, "fixed_window/memory", func() limiter.Limiter { ... })
//
// Strategy packages run it from their own tests; this package's test file
// additionally exercises every registered strategy on both drivers.
package conformance

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gatewright/internal/limiter"
)

// BaseTime anchors every explicit instant the suite uses. Fixed, so results
// are reproducible and independent of the host clock.
var BaseTime = time.Unix(1_700_000_000, 0).UTC()

const (
	// hammerGoroutines is K, the width of the concurrent never-over-admit
	// probe. High enough to interleave real goroutines; the drivers serialise
	// decisions anyway, which is precisely what the probe proves.
	hammerGoroutines = 64

	// recoverySlack widens the reported ResetIn before probing recovery so
	// integer truncation at window boundaries cannot straddle a rollover.
	recoverySlack = time.Millisecond
)

// newGate builds the start barrier for the concurrent hammer. It exists
// because RunSuite's parameter named `make` shadows the builtin below it.
func newGate() chan struct{} { return make(chan struct{}) }

// RunSuite asserts the full invariant set below against limiter instances
// built by make. Every sub-test builds a FRESH limiter, so phases stay
// independent regardless of what earlier phases consumed.
//
// Invariants asserted for any implementation:
//
//	a) never-over-admit: 64 goroutines issuing sequential Allows at one
//	   identical instant admit exactly Limit units (cost 1, fresh quota),
//	   and never more;
//	b) remaining monotonicity: at a frozen instant Remaining never increases,
//	   admitted calls drop by exactly the cost, a denied key stays denied,
//	   and denials never report more Remaining than the prior state;
//	c) deny implies Allowed == false, RetryAfter > 0; allow implies
//	   RetryAfter == 0 -- checked on every Decision the suite observes;
//	d) boundary behaviour: advancing now past a denial's ResetIn restores
//	   admission, unless the strategy reports ResetIn == 0 ("time alone will
//	   not help"), which skips the phase honestly;
//	e) key isolation: two keys never share quota;
//	f) cost accounting: cost = 3 consumes exactly 3 units and admits
//	   floor(Limit/3) times;
//	g) Decision.Limit >= 1 always (folded into the shared contract check).
func RunSuite(t *testing.T, name string, make func() limiter.Limiter) {
	t.Helper()
	t.Run(name+"/decision_contract", func(t *testing.T) { testDecisionContract(t, make) })
	t.Run(name+"/never_over_admit_concurrent", func(t *testing.T) { testNeverOverAdmit(t, make) })
	t.Run(name+"/remaining_monotonic_frozen_now", func(t *testing.T) { testRemainingMonotonic(t, make) })
	t.Run(name+"/boundary_recovery_after_reset", func(t *testing.T) { testBoundaryRecovery(t, make) })
	t.Run(name+"/key_isolation", func(t *testing.T) { testKeyIsolation(t, make) })
	t.Run(name+"/cost_accounting", func(t *testing.T) { testCostAccounting(t, make) })
}

// checkContract enforces the per-decision invariants (c) and (g) plus the
// field ranges DESIGN.md promises. Safe to call from any goroutine.
func checkContract(t *testing.T, where string, d limiter.Decision) {
	t.Helper()
	if d.Limit < 1 {
		t.Errorf("%s: Decision.Limit = %d, want >= 1", where, d.Limit)
	}
	if d.Remaining < 0 || d.Remaining > d.Limit {
		t.Errorf("%s: Remaining = %d outside [0, Limit=%d]", where, d.Remaining, d.Limit)
	}
	if d.ResetIn < 0 {
		t.Errorf("%s: ResetIn = %v, want >= 0", where, d.ResetIn)
	}
	if d.Allowed {
		if d.RetryAfter != 0 {
			t.Errorf("%s: allowed decision carries RetryAfter = %v, want 0", where, d.RetryAfter)
		}
		return
	}
	if d.RetryAfter <= 0 {
		t.Errorf("%s: denied decision carries RetryAfter = %v, want > 0", where, d.RetryAfter)
	}
}

// drainToDenial spends cost-1... rather: repeatedly Allow(key, now, cost) at
// the frozen instant now until the limiter refuses, then returns how many
// calls were admitted plus the denying Decision. Fails the test if refusal
// never comes despite far exceeding the reported limit.
func drainToDenial(t *testing.T, l limiter.Limiter, key string, now time.Time, cost int64, where string) (int64, limiter.Decision) {
	t.Helper()
	admitted := int64(0)
	for {
		d := l.Allow(key, now, cost)
		checkContract(t, where, d)
		if !d.Allowed {
			return admitted, d
		}
		admitted++
		if ceiling := d.Limit + 4; admitted > ceiling {
			t.Fatalf("%s: admitted %d units at a frozen instant without ever denying (reported limit %d)",
				where, admitted, d.Limit)
		}
	}
}

// testDecisionContract pins the very first decision on a fresh key: admitted,
// with Remaining exactly Limit-1 under cost 1 -- proof the quota starts
// untouched and the reported scale matches consumption.
func testDecisionContract(t *testing.T, make func() limiter.Limiter) {
	l := make()
	d := l.Allow("contract", BaseTime, 1)
	checkContract(t, "first-decision", d)
	if !d.Allowed {
		t.Fatalf("fresh key denied on its first admission: %+v", d)
	}
	if d.Remaining != d.Limit-1 {
		t.Errorf("first cost-1 admission reports Remaining = %d, want Limit-1 = %d",
			d.Remaining, d.Limit-1)
	}
}

// testNeverOverAdmit covers invariant (a): sequential drain proves capacity
// equals the reported Limit exactly, then 64 goroutines racing at the same
// instant must admit exactly that many -- not one more.
func testNeverOverAdmit(t *testing.T, make func() limiter.Limiter) {
	probe := make()
	d0 := probe.Allow("hammer", BaseTime, 1)
	checkContract(t, "hammer/probe", d0)
	L := d0.Limit

	seqAdmitted, _ := drainToDenial(t, make(), "hammer", BaseTime, 1, "hammer/sequential")
	if seqAdmitted != L {
		t.Errorf("sequential drain admitted %d units, want exactly Limit = %d (cost 1, fresh quota)",
			seqAdmitted, L)
	}

	ham := make()
	per := L/int64(hammerGoroutines) + 2 // guarantees K*per >= L
	var total atomic.Int64
	start := newGate()
	var wg sync.WaitGroup
	for g := 0; g < hammerGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := int64(0); i < per; i++ {
				d := ham.Allow("hammer", BaseTime, 1)
				checkContract(t, "hammer/concurrent", d)
				if d.Allowed {
					total.Add(1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	got := total.Load()
	if got > L {
		t.Errorf("concurrent hammer admitted %d units, OVER the limit of %d", got, L)
	} else if got != L {
		t.Errorf("concurrent hammer admitted %d units, want exactly %d (cost 1, fresh quota)", got, L)
	}
}

// testRemainingMonotonic covers invariant (b) at a frozen instant: exact
// decrements while admitting, no increase anywhere (denials may report
// residual headroom but never above the prior state), no revival after the
// first denial.
func testRemainingMonotonic(t *testing.T, make func() limiter.Limiter) {
	l := make()
	first := l.Allow("mono", BaseTime, 1)
	checkContract(t, "mono/first", first)

	prevRem, deniedSeen := first.Remaining, false
	for i := int64(1); i <= 2*first.Limit+8; i++ {
		d := l.Allow("mono", BaseTime, 1)
		checkContract(t, "mono", d)
		if d.Allowed {
			if deniedSeen {
				t.Fatalf("call %d admitted although an earlier call at the same instant was denied", i)
			}
			if d.Remaining != prevRem-1 {
				t.Errorf("call %d: Remaining = %d after previous %d; want exact cost-1 decrement at a frozen instant",
					i, d.Remaining, prevRem)
			}
		} else {
			if d.Remaining > prevRem {
				t.Errorf("call %d: denied decision reports Remaining = %d, above prior state %d",
					i, d.Remaining, prevRem)
			}
			deniedSeen = true
		}
		prevRem = d.Remaining
	}
	if !deniedSeen {
		t.Fatalf("limit %d never enforced across %d calls", first.Limit, 2*first.Limit+9)
	}

	again := l.Allow("mono", BaseTime, 1)
	checkContract(t, "mono/post-denial", again)
	if again.Allowed {
		t.Errorf("denied key revived at the same instant without any reset")
	}
}

// testBoundaryRecovery covers invariant (d): past ResetIn at least one more
// admission must succeed. Strategies with no self-imposed horizon (ResetIn 0
// on denial, i.e. concurrency) are skipped: only releases free them.
func testBoundaryRecovery(t *testing.T, make func() limiter.Limiter) {
	l := make()
	first := l.Allow("recovery", BaseTime, 1)
	checkContract(t, "recovery/first", first)

	_, denied := drainToDenial(t, l, "recovery", BaseTime, 1, "recovery/drain")
	if denied.ResetIn <= 0 {
		t.Skipf("strategy reports ResetIn = %v on denial: no time-based recovery to test", denied.ResetIn)
	}

	later := BaseTime.Add(denied.ResetIn + recoverySlack)
	d := l.Allow("recovery", later, 1)
	checkContract(t, "recovery/after-reset", d)
	if !d.Allowed {
		t.Errorf("still denied at now+ResetIn+%v (ResetIn was %v): quota did not recover",
			recoverySlack, denied.ResetIn)
	}
}

// testKeyIsolation covers invariant (e): exhausting iso-a leaves iso-b's
// quota untouched and iso-a still drained.
func testKeyIsolation(t *testing.T, make func() limiter.Limiter) {
	l := make()
	first := l.Allow("iso-a", BaseTime, 1)
	checkContract(t, "isolation/a-first", first)
	L := first.Limit

	drainToDenial(t, l, "iso-a", BaseTime, 1, "isolation/drain-a")

	b := l.Allow("iso-b", BaseTime, 1)
	checkContract(t, "isolation/b", b)
	if !b.Allowed {
		t.Errorf("key iso-b denied although only iso-a consumed quota")
	}
	if b.Remaining != L-1 {
		t.Errorf("iso-b reports Remaining = %d, want untouched %d-1", b.Remaining, L)
	}

	aAgain := l.Allow("iso-a", BaseTime, 1)
	checkContract(t, "isolation/a-again", aAgain)
	if aAgain.Allowed {
		t.Errorf("drained key iso-a revived without time passing")
	}
}

// testCostAccounting covers invariant (f): cost = 3 consumes exactly three
// units, admits floor(Limit/3) times, then stays denied.
func testCostAccounting(t *testing.T, make func() limiter.Limiter) {
	const unit = int64(3)
	l := make()
	admitted, prevRem := int64(0), int64(-1)
	var last limiter.Decision
	for {
		last = l.Allow("cost", BaseTime, unit)
		checkContract(t, "cost", last)
		if !last.Allowed {
			break
		}
		admitted++
		if prevRem >= 0 && last.Remaining != prevRem-unit {
			t.Errorf("cost-%d admission moved Remaining %d -> %d; want exact decrement",
				unit, prevRem, last.Remaining)
		}
		prevRem = last.Remaining
		if ceiling := last.Limit + 4; admitted > ceiling {
			t.Fatalf("runaway admissions under cost %d (limit %d)", unit, last.Limit)
		}
	}

	want := last.Limit / unit
	if admitted != want {
		t.Errorf("cost-%d admitted %d times, want floor(Limit/%d) = %d",
			unit, admitted, unit, want)
	}
}
