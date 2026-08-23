package pool

import (
	"net/url"
	"sync"
	"time"
)

type circuitPhase uint8

const (
	circuitClosed circuitPhase = iota
	circuitOpen
	circuitHalfOpen
)

func (c circuitPhase) metricValue() float64 {
	switch c {
	case circuitOpen:
		return 1
	case circuitHalfOpen:
		return 0.5
	default:
		return 0
	}
}

type probingCause uint8

const (
	causeNone probingCause = iota
	causeEjection
	causeRecovery
)

// targetState is the runtime health record for one upstream target. All
// mutable fields are guarded by mu; time is supplied by now so tests can
// drive every deadline deterministically.
type targetState struct {
	tgt    Target
	weight int

	poolName string
	sink     MetricsSink

	passive PassiveConfig
	breaker BreakerConfig

	healthyThreshold   int
	unhealthyThreshold int

	parsedURL *url.URL

	mu               sync.Mutex
	state            State
	cause            probingCause
	ejectedUntil     time.Time
	probeStreak      int
	inflight         int64
	passiveFails     []time.Time
	cb               circuitPhase
	cbUntil          time.Time
	breakerFails     []time.Time
	halfLive         int64
	activeFailStreak int
	totalReq         uint64
	totalFail        uint64

	now func() time.Time
}

func newTargetState(poolName string, tg *Target, cfg Config, now func() time.Time) (*targetState, *peer) {
	ht := cfg.Active.HealthyThreshold
	if ht < 1 {
		ht = 1
	}
	ut := cfg.Active.UnhealthyThreshold
	if ut < 1 {
		ut = 1
	}
	st := &targetState{
		tgt:                *tg,
		weight:             tg.Weight,
		poolName:           poolName,
		passive:            cfg.Passive,
		breaker:            cfg.Breaker,
		healthyThreshold:   ht,
		unhealthyThreshold: ut,
		state:              StateHealthy,
		now:                now,
	}
	if u, err := url.Parse(tg.URL); err == nil && u.Scheme != "" && u.Host != "" {
		st.parsedURL = u
	}
	return st, &peer{tg: tg, st: st}
}

func (st *targetState) setSinkLocked(sink MetricsSink) {
	st.sink = sink
}

func (st *targetState) emitBaselineLocked() {
	if st.sink == nil {
		return
	}
	st.sink.SetTargetHealth(st.poolName, st.tgt.Name, healthValue(st.state))
	st.sink.SetCircuitState(st.poolName, st.tgt.Name, st.cb.metricValue())
}

func healthValue(s State) float64 {
	if s == StateHealthy {
		return 1
	}
	return 0
}

func (st *targetState) setStateLocked(s State) {
	if st.state == s {
		return
	}
	st.state = s
	if st.sink != nil {
		st.sink.SetTargetHealth(st.poolName, st.tgt.Name, healthValue(s))
	}
}

func (st *targetState) setCircuitLocked(p circuitPhase) {
	if st.cb == p {
		return
	}
	st.cb = p
	if p == circuitClosed {
		st.halfLive = 0
	}
	if st.sink != nil {
		st.sink.SetCircuitState(st.poolName, st.tgt.Name, p.metricValue())
	}
}

// normalizeLocked applies lazy time-based transitions: ejection expiry turns
// into a probing state and breaker cooldown expiry turns into half-open.
func (st *targetState) normalizeLocked(now time.Time) {
	if st.state == StateEjected && !now.Before(st.ejectedUntil) {
		st.cause = causeEjection
		st.probeStreak = 0
		st.setStateLocked(StateProbing)
	}
	if st.cb == circuitOpen && !now.Before(st.cbUntil) {
		st.setCircuitLocked(circuitHalfOpen)
	}
}

// availableLocked reports whether the target may receive traffic right now.
func (st *targetState) availableLocked(now time.Time) bool {
	if st.weight <= 0 {
		return false
	}
	st.normalizeLocked(now)
	if st.state == StateDown || st.state == StateEjected {
		return false
	}
	if st.cb == circuitOpen {
		return false
	}
	return true
}

// begin reserves one in-flight slot. It returns false when the target became
// unpickable between selection and commit, or when a recovery/half-open probe
// budget is exhausted.
func (st *targetState) begin(now time.Time) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.availableLocked(now) {
		return false
	}
	if st.state == StateProbing && st.inflight >= 1 {
		return false
	}
	if st.cb == circuitHalfOpen && st.halfLive >= int64(clampMin1(st.breaker.HalfOpenProbes)) {
		return false
	}
	if st.cb == circuitHalfOpen {
		st.halfLive++
	}
	st.inflight++
	st.totalReq++
	return true
}

func clampMin1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

func (st *targetState) done(o Outcome, now time.Time) {
	st.mu.Lock()
	if st.inflight > 0 {
		st.inflight--
	}
	if o.Success {
		st.passiveFails = st.passiveFails[:0]
		st.breakerFails = st.breakerFails[:0]
	} else {
		st.totalFail++
		st.passiveFails = append(st.passiveFails, now)
		st.passiveFails = pruneWindow(st.passiveFails, now, st.passive.Window)
		st.ejectIfThresholdLocked(now)
		st.recordBreakerFailureLocked(now)
	}
	if st.state == StateProbing && st.cause != causeNone {
		if o.Success {
			st.probeStreak++
			if st.probeStreak >= st.healthyThreshold {
				st.promoteToHealthyLocked()
			}
		} else if st.cause == causeEjection {
			st.ejectLocked(now)
		} else {
			st.demoteToDownLocked()
		}
	}
	if st.cb == circuitHalfOpen && st.halfLive > 0 {
		st.halfLive--
		if o.Success {
			st.setCircuitLocked(circuitClosed)
			st.breakerFails = st.breakerFails[:0]
		} else {
			st.openBreakerLocked(now)
		}
	}
	sink := st.sink
	name := st.tgt.Name
	st.mu.Unlock()
	if sink != nil {
		sink.AddUpstreamRequest(st.poolName, name, o.Status)
		sink.ObserveUpstreamLatency(st.poolName, name, o.Latency.Seconds())
	}
}

func (st *targetState) ejectIfThresholdLocked(now time.Time) {
	if st.passive.Failures <= 0 || len(st.passiveFails) < st.passive.Failures {
		return
	}
	st.passiveFails = st.passiveFails[:0]
	if st.state == StateDown {
		return
	}
	st.ejectLocked(now)
}

func (st *targetState) ejectLocked(now time.Time) {
	ej := st.passive.EjectionTime
	if ej <= 0 {
		ej = time.Nanosecond
	}
	st.ejectedUntil = now.Add(ej)
	st.cause = causeNone
	st.probeStreak = 0
	st.setStateLocked(StateEjected)
}

func (st *targetState) promoteToHealthyLocked() {
	st.cause = causeNone
	st.probeStreak = 0
	st.setStateLocked(StateHealthy)
}

func (st *targetState) demoteToDownLocked() {
	st.cause = causeNone
	st.probeStreak = 0
	st.setStateLocked(StateDown)
}

func (st *targetState) recordBreakerFailureLocked(now time.Time) {
	if st.breaker.Failures <= 0 || st.cb != circuitClosed {
		return
	}
	st.breakerFails = append(st.breakerFails, now)
	st.breakerFails = pruneWindow(st.breakerFails, now, st.breaker.Window)
	if len(st.breakerFails) >= st.breaker.Failures {
		st.breakerFails = st.breakerFails[:0]
		st.openBreakerLocked(now)
	}
}

func (st *targetState) openBreakerLocked(now time.Time) {
	cd := st.breaker.Cooldown
	if cd <= 0 {
		cd = time.Nanosecond
	}
	st.cbUntil = now.Add(cd)
	st.halfLive = 0
	st.setCircuitLocked(circuitOpen)
}

// activeResult folds one active health-check result into the state machine.
func (st *targetState) activeResult(success bool, now time.Time) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if success {
		st.activeFailStreak = 0
		switch st.state {
		case StateDown:
			st.cause = causeRecovery
			st.probeStreak = 1
			st.setStateLocked(StateProbing)
		case StateProbing:
			st.probeStreak++
			if st.probeStreak >= st.healthyThreshold {
				st.promoteToHealthyLocked()
			}
		case StateEjected:
			// Ejection timer governs re-entry; the streak stays armed.
			st.probeStreak = 0
		}
		return
	}
	st.activeFailStreak++
	switch st.state {
	case StateHealthy:
		if st.activeFailStreak >= st.unhealthyThreshold {
			st.demoteToDownLocked()
		}
	case StateProbing:
		if st.cause == causeRecovery || st.activeFailStreak >= st.unhealthyThreshold {
			st.demoteToDownLocked()
		}
	}
}

func (st *targetState) status(now time.Time) TargetStatus {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.normalizeLocked(now)
	return TargetStatus{
		Target:       st.tgt,
		State:        st.state,
		Inflight:     st.inflight,
		Failures:     len(st.passiveFails),
		EjectedUntil: st.ejectedUntil,
		CircuitOpen:  st.cb == circuitOpen,
		TotalReq:     st.totalReq,
		TotalFail:    st.totalFail,
	}
}

func pruneWindow(ts []time.Time, now time.Time, win time.Duration) []time.Time {
	if win <= 0 {
		return ts[:0]
	}
	cut := now.Add(-win)
	keep := ts[:0]
	for _, t := range ts {
		if t.After(cut) {
			keep = append(keep, t)
		}
	}
	return keep
}

// peer couples a target with its runtime state for picker strategies.
type peer struct {
	tg *Target
	st *targetState
}

func (pe *peer) weight() int { return pe.st.weight }

func (pe *peer) available(now time.Time) bool {
	pe.st.mu.Lock()
	defer pe.st.mu.Unlock()
	return pe.st.availableLocked(now)
}

func (pe *peer) begin(now time.Time) bool { return pe.st.begin(now) }

func (pe *peer) inflight() int64 {
	pe.st.mu.Lock()
	defer pe.st.mu.Unlock()
	return pe.st.inflight
}
