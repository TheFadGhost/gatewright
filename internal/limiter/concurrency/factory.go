package concurrency

import (
	"errors"

	"gatewright/internal/limiter"
)

// Factory builds a concurrency limiter from validated settings, wiring the
// pure algorithm into the engine's driver selection.
func Factory(p limiter.Params) (limiter.Limiter, error) {
	cfg, err := configFrom(p.Settings)
	if err != nil {
		return nil, err
	}
	return limiter.NewEngine(strategyName, newAlgo(cfg), p), nil
}

// configFrom maps generic settings onto Config, applying the documented
// default (Capacity 0 => Limit).
func configFrom(s limiter.Settings) (Config, error) {
	capacity := s.Capacity
	if capacity == 0 {
		capacity = s.Limit
	}
	if capacity < 1 {
		return Config{}, errors.New("concurrency: capacity must be >= 1 (defaults to limit when omitted)")
	}
	return Config{Capacity: capacity}, nil
}

// CheckSettings reports every problem that would make the settings unusable;
// an empty slice means valid. Window is deliberately ignored: the live
// counter has no time dimension.
func CheckSettings(s limiter.Settings) []string {
	capacity := s.Capacity
	if capacity == 0 {
		capacity = s.Limit
	}
	var problems []string
	if capacity < 1 {
		problems = append(problems, "capacity must be >= 1 (defaults to limit when omitted)")
	}
	return problems
}

func init() {
	limiter.Register(strategyName, Factory)
	limiter.RegisterChecker(strategyName, CheckSettings)
}
