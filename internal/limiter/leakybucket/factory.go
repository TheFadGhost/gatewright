package leakybucket

import (
	"errors"
	"time"

	"gatewright/internal/limiter"
)

// Factory builds a leaky_bucket limiter from validated settings, wiring the
// pure algorithm into the engine's driver selection.
func Factory(p limiter.Params) (limiter.Limiter, error) {
	cfg, err := configFrom(p.Settings)
	if err != nil {
		return nil, err
	}
	return limiter.NewEngine(strategyName, newAlgo(cfg), p), nil
}

// configFrom maps generic settings onto Config, applying the documented
// default (Capacity 0 => Limit) and rejecting combinations the algorithm
// cannot honour.
func configFrom(s limiter.Settings) (Config, error) {
	switch {
	case s.Limit < 1:
		return Config{}, errors.New("leaky_bucket: limit must be >= 1")
	case s.Window < time.Millisecond:
		return Config{}, errors.New("leaky_bucket: window must be >= 1ms")
	case s.Capacity < 0:
		return Config{}, errors.New("leaky_bucket: capacity must be >= 0 (omit for default)")
	}
	capacity := s.Capacity
	if capacity == 0 {
		capacity = s.Limit
	}
	return Config{Limit: s.Limit, Window: s.Window, Capacity: capacity}, nil
}

// CheckSettings reports every problem that would make the settings unusable;
// an empty slice means valid.
func CheckSettings(s limiter.Settings) []string {
	var problems []string
	if s.Limit < 1 {
		problems = append(problems, "limit must be >= 1")
	}
	if s.Window < time.Millisecond {
		problems = append(problems, "window must be >= 1ms")
	}
	if s.Capacity < 0 {
		problems = append(problems, "capacity must be >= 0 (0 defaults to limit)")
	}
	return problems
}

func init() {
	limiter.Register(strategyName, Factory)
	limiter.RegisterChecker(strategyName, CheckSettings)
}
