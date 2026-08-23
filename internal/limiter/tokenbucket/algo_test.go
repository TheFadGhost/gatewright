package tokenbucket

import (
	"encoding/binary"
	"math"
	"testing"
	"time"

	"gatewright/internal/limiter"
)

var base = time.Unix(1_700_000_000, 0).UTC()

// exactCfg is dyadic-exact: Limit/Window == 1 token per nanosecond, so every
// refill and subtraction below is free of floating point drift.
var exactCfg = Config{Limit: 1_000_000, Window: time.Millisecond, Burst: 1_000_000}

// halfRateCfg refills exactly 0.5 tokens per nanosecond.
var halfRateCfg = Config{Limit: 1_000_000, Window: 2 * time.Millisecond, Burst: 1_000_000}

// bigBurstCfg refills exactly 0.5 tokens per nanosecond into a 2M bucket.
var bigBurstCfg = Config{Limit: 500_000, Window: time.Millisecond, Burst: 2_000_000}

// drainAll empties a fresh bucket in one shot (cost == burst) and returns
// the state blob encoding zero tokens at now.
func drainAll(t *testing.T, a *algo, cfg Config, now time.Time) []byte {
	t.Helper()
	blob, d := a.Step(nil, false, now, cfg.Burst)
	if !d.Allowed {
		t.Fatalf("drainAll: fresh bucket denied cost %d: %+v", cfg.Burst, d)
	}
	if d.Remaining != 0 {
		t.Fatalf("drainAll: Remaining = %d, want 0", d.Remaining)
	}
	return blob
}

func TestFreshBucketStartsFull(t *testing.T) {
	a := newAlgo(exactCfg)
	blob, d := a.Step(nil, false, base, 400_000)
	if !d.Allowed || d.Limit != 1_000_000 || d.Remaining != 600_000 {
		t.Fatalf("fresh admit: allowed=%v limit=%d remaining=%d", d.Allowed, d.Limit, d.Remaining)
	}
	if d.RetryAfter != 0 {
		t.Fatalf("allowed decision must carry RetryAfter=0, got %v", d.RetryAfter)
	}
	// 600k tokens held; filling needs another 400k at exactly 1 token/ns.
	if d.ResetIn != 400*time.Microsecond {
		t.Fatalf("ResetIn = %v, want 400us", d.ResetIn)
	}
	if len(blob) != stateLen || blob[0] != stateVersion {
		t.Fatalf("state blob malformed: %d bytes, version %d", len(blob), blob[0])
	}
}

func TestRefillAccumulatesExactlyAcrossCalls(t *testing.T) {
	a := newAlgo(exactCfg)
	blob := drainAll(t, a, exactCfg, base)

	// +250us => exactly +250k tokens; ask for 200k.
	blob, d := a.Step(blob, true, base.Add(250*time.Microsecond), 200_000)
	if !d.Allowed || d.Remaining != 50_000 {
		t.Fatalf("after 250us refill: allowed=%v remaining=%d, want true/50000", d.Allowed, d.Remaining)
	}
	// Another +350us on top of the stored level => 50k+350k = 400k; ask 350k.
	blob, d = a.Step(blob, true, base.Add(600*time.Microsecond), 350_000)
	if !d.Allowed || d.Remaining != 50_000 {
		t.Fatalf("second refill step: allowed=%v remaining=%d, want true/50000", d.Allowed, d.Remaining)
	}
	// Only 50k held: 60k must be denied; shortfall 10us is below the 1ms floor.
	_, d = a.Step(blob, true, base.Add(600*time.Microsecond), 60_000)
	if d.Allowed || d.Remaining != 0 {
		t.Fatalf("overdraw: allowed=%v remaining=%d, want false/0", d.Allowed, d.Remaining)
	}
	if d.RetryAfter != time.Millisecond {
		t.Fatalf("RetryAfter = %v, want floored 1ms", d.RetryAfter)
	}
}

func TestFractionalTokensPersistAndFloor(t *testing.T) {
	a := newAlgo(halfRateCfg) // exactly 0.5 tokens per ns
	blob := drainAll(t, a, halfRateCfg, base)

	// +1ns => 0.5 tokens: below cost 1, denied, hint floored to 1ms.
	_, d := a.Step(blob, true, base.Add(1), 1)
	if d.Allowed || d.RetryAfter != time.Millisecond {
		t.Fatalf("half-token deny: allowed=%v retry=%v, want false/1ms", d.Allowed, d.RetryAfter)
	}
	// +3ns total => 1.5 tokens: admit 1, leaving a fractional remainder.
	blob, d = a.Step(blob, true, base.Add(3), 1)
	if !d.Allowed || d.Remaining != 0 { // floor(0.5) == 0
		t.Fatalf("admit from 1.5: allowed=%v remaining=%d, want true/0", d.Allowed, d.Remaining)
	}
	// The fractional 0.5 survived encoding: one more nanosecond completes it.
	_, d = a.Step(blob, true, base.Add(4), 1)
	if !d.Allowed {
		t.Fatalf("fractional accumulation lost across calls: %+v", d)
	}
}

func TestBurstCapacityAndSustainedRefill(t *testing.T) {
	a := newAlgo(bigBurstCfg)
	blob := drainAll(t, a, bigBurstCfg, base) // zero tokens at base

	// Sustained rate: +2ms at 0.5 tokens/ns => exactly +1M tokens back.
	blob, d := a.Step(blob, true, base.Add(2*time.Millisecond), 500_000)
	if !d.Allowed || d.Remaining != 500_000 {
		t.Fatalf("sustained draw 1: allowed=%v remaining=%d", d.Allowed, d.Remaining)
	}
	blob, d = a.Step(blob, true, base.Add(2*time.Millisecond), 500_000)
	if !d.Allowed || d.Remaining != 0 {
		t.Fatalf("sustained draw 2: allowed=%v remaining=%d", d.Allowed, d.Remaining)
	}
	if _, d = a.Step(blob, true, base.Add(2*time.Millisecond), 1); d.Allowed {
		t.Fatal("bucket should be exhausted beyond its refill budget")
	}

	// Burst cap: waiting far beyond the fill time (4ms) can never stock more
	// than Burst. After 10ms exactly four 500k draws succeed, then denial.
	for i := 0; i < 4; i++ {
		var d limiter.Decision
		blob, d = a.Step(blob, true, base.Add(12*time.Millisecond), 500_000)
		want := int64(1_500_000 - i*500_000)
		if !d.Allowed || d.Remaining != want {
			t.Fatalf("burst draw %d: allowed=%v remaining=%d, want true/%d", i, d.Allowed, d.Remaining, want)
		}
	}
	if _, d = a.Step(blob, true, base.Add(12*time.Millisecond), 1); d.Allowed || d.RetryAfter <= 0 {
		t.Fatalf("burst cap exceeded: allowed=%v retry=%v", d.Allowed, d.RetryAfter)
	}
}

func TestDenyReportsEarliestRetry(t *testing.T) {
	a := newAlgo(exactCfg)
	blob := drainAll(t, a, exactCfg, base)

	// Shortfall below the floor: 700us of missing tokens reports 1ms.
	_, d := a.Step(blob, true, base, 700_000)
	if d.Allowed || d.Remaining != 0 {
		t.Fatalf("deny: allowed=%v remaining=%d", d.Allowed, d.Remaining)
	}
	if d.RetryAfter != time.Millisecond {
		t.Fatalf("floored shortfall: allowed=%v retry=%v, want false/1ms", d.Allowed, d.RetryAfter)
	}
	// Above the floor the hint is exact: 1.5M tokens at 0.5/ns => 3ms.
	big := newAlgo(bigBurstCfg)
	empty := drainAll(t, big, bigBurstCfg, base)
	_, d = big.Step(empty, true, base, 1_500_000)
	if d.Allowed || d.RetryAfter != 3*time.Millisecond {
		t.Fatalf("exact shortfall: allowed=%v retry=%v, want false/exactly 3ms", d.Allowed, d.RetryAfter)
	}
	// A cost above Burst can never succeed; report the empty-bucket horizon.
	_, d = a.Step(blob, true, base, 3_000_000)
	if d.Allowed || d.RetryAfter != 3*time.Millisecond {
		t.Fatalf("oversized cost: allowed=%v retry=%v, want false/3ms", d.Allowed, d.RetryAfter)
	}
}

func TestCorruptStateRecoversFull(t *testing.T) {
	a := newAlgo(exactCfg)
	good, _ := a.Step(nil, false, base, 100_000) // 900k held

	mutants := map[string][]byte{
		"truncated": good[:4],
		"extended":  append(append([]byte(nil), good...), 0xFF),
		"version":   flipVersion(good),
		"nan":       withTokens(good, math.Float64bits(math.NaN())),
		"inf":       withTokens(good, math.Float64bits(math.Inf(1))),
		"negative":  withTokens(good, math.Float64bits(-2.5)),
		"garbage":   []byte("gatewright-corrupt-state"),
		"nil":       nil,
	}
	for name, blob := range mutants {
		for _, existed := range []bool{true, false} {
			next, d := a.Step(blob, existed, base, 1)
			if !d.Allowed || d.Remaining != 999_999 {
				t.Fatalf("%s (existed=%v): corrupt state must recover to a full bucket, got allowed=%v remaining=%d",
					name, existed, d.Allowed, d.Remaining)
			}
			if len(next) != stateLen {
				t.Fatalf("%s: recovered blob is %d bytes", name, len(next))
			}
		}
	}

	// A corrupted timestamp alone does not break the encoding: decode stays
	// valid and the future clock is frozen rather than trusted (no refund).
	badTime := append([]byte(nil), good...)
	binary.LittleEndian.PutUint64(badTime[9:], uint64(math.MaxInt64))
	_, d := a.Step(badTime, true, base, 1)
	if !d.Allowed || d.Remaining != 899_999 {
		t.Fatalf("future timestamp: allowed=%v remaining=%d, want true/899999", d.Allowed, d.Remaining)
	}
}

func TestBackwardsClockDoesNotRefund(t *testing.T) {
	a := newAlgo(exactCfg)
	blob := drainAll(t, a, exactCfg, base)

	_, d := a.Step(blob, true, base.Add(-time.Microsecond), 1)
	if d.Allowed {
		t.Fatal("a negative elapsed time must not mint tokens")
	}
	if d.RetryAfter < minRetry {
		t.Fatalf("denial RetryAfter = %v, want >= 1ms", d.RetryAfter)
	}
}

func TestEncodeDecodeRoundtrip(t *testing.T) {
	states := []state{
		{Tokens: 999_999.5, Last: 987654321},
		{Tokens: 0, Last: 0},
		{Tokens: 1_000_000, Last: -5},
		{Tokens: 0.000000001953125, Last: 1 << 40}, // tiny fraction survives bit-exact
	}
	for _, st := range states {
		got, ok := decode(encode(st))
		if !ok {
			t.Fatalf("decode(encode(%+v)) rejected", st)
		}
		if got != st {
			t.Fatalf("roundtrip mismatch: got %+v, want %+v", got, st)
		}
	}
	for _, bad := range [][]byte{nil, {}, {stateVersion}, make([]byte, stateLen+1)} {
		if _, ok := decode(bad); ok {
			t.Fatalf("decode accepted %d malformed bytes", len(bad))
		}
	}
}

func TestConfigFromValidatesAndDefaults(t *testing.T) {
	cfg, err := configFrom(limiter.Settings{Limit: 7, Window: time.Second})
	if err != nil || cfg.Burst != 7 {
		t.Fatalf("burst default: cfg=%+v err=%v, want burst 7 nil error", cfg, err)
	}
	cfg, err = configFrom(limiter.Settings{Limit: 7, Window: time.Second, Burst: 20})
	if err != nil || cfg.Burst != 20 {
		t.Fatalf("explicit burst: cfg=%+v err=%v", cfg, err)
	}
	for _, s := range []limiter.Settings{
		{Limit: 0, Window: time.Second},
		{Limit: -3, Window: time.Second},
		{Limit: 5, Window: 0},
		{Limit: 5, Window: time.Millisecond - 1},
		{Limit: 5, Window: time.Second, Burst: -1},
	} {
		if _, err := configFrom(s); err == nil {
			t.Fatalf("configFrom(%+v) accepted invalid settings", s)
		}
	}
}

func TestCheckerRegistered(t *testing.T) {
	if !limiter.Has(strategyName) {
		t.Fatal("strategy not registered")
	}
	if probs := limiter.CheckSettings(strategyName, limiter.Settings{Limit: 10, Window: time.Second}); len(probs) != 0 {
		t.Fatalf("valid settings flagged: %v", probs)
	}
	for _, s := range []limiter.Settings{
		{Limit: 0, Window: time.Second},
		{Limit: 10, Window: 0},
		{Limit: 10, Window: time.Second, Burst: -2},
	} {
		if probs := limiter.CheckSettings(strategyName, s); len(probs) == 0 {
			t.Fatalf("CheckSettings(%+v) reported no problems", s)
		}
	}
}

func TestFactoryBuildsWorkingLimiter(t *testing.T) {
	l, err := limiter.New(strategyName, limiter.Params{
		Name:     "tb",
		Settings: limiter.Settings{Limit: 10, Window: time.Second}, // burst defaults to 10
	})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	if d := l.Allow("k", base, 6); !d.Allowed || d.Remaining != 4 {
		t.Fatalf("first allow: %+v", d)
	}
	if d := l.Allow("k", base, 6); d.Allowed || d.RetryAfter <= 0 {
		t.Fatalf("over-burst through engine: %+v", d)
	}
	if d := l.Allow("other", base, 6); !d.Allowed {
		t.Fatalf("keys must be independent: %+v", d)
	}
	if _, err := limiter.New(strategyName, limiter.Params{Name: "bad"}); err == nil {
		t.Fatal("factory accepted settings without limit/window")
	}
	if ttl := newAlgo(exactCfg).TTL(); ttl <= 0 {
		t.Fatalf("TTL = %v, want > 0", ttl)
	}
}

func flipVersion(b []byte) []byte {
	out := append([]byte(nil), b...)
	if len(out) > 0 {
		out[0] ^= 0xFF
	}
	return out
}

func withTokens(b []byte, bits uint64) []byte {
	out := append([]byte(nil), b...)
	if len(out) == stateLen {
		binary.LittleEndian.PutUint64(out[1:], bits)
	}
	return out
}
