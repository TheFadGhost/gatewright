package slidinglog

import (
	"errors"
	"time"

	"gatewright/internal/limiter"
)

// strategyName is the config-facing name of this strategy.
const strategyName = "sliding_window_log"

// Factory builds a sliding_window_log limiter from validated settings.
func Factory(p limiter.Params) (limiter.Limiter, error) {
	cfg, err := configFrom(p.Settings)
	if err != nil {
		return nil, err
	}
	return limiter.NewEngine(strategyName, algo{cfg: cfg}, p), nil
}

func configFrom(s limiter.Settings) (Config, error) {
	if s.Limit < 1 {
		return Config{}, errors.New("sliding_window_log: limit must be >= 1")
	}
	if s.Window < time.Millisecond {
		return Config{}, errors.New("sliding_window_log: window must be >= 1ms")
	}
	return Config{Limit: s.Limit, Window: s.Window}, nil
}

// Checker validates Settings for sliding_window_log, returning human-readable
// problems (empty slice = valid).
func Checker(s limiter.Settings) []string {
	var probs []string
	if s.Limit < 1 {
		probs = append(probs, "limit must be >= 1")
	}
	if s.Window < time.Millisecond {
		probs = append(probs, `window must be >= 1ms (write it with a unit suffix, e.g. "30s")`)
	}
	return probs
}

func init() {
	limiter.Register(strategyName, Factory)
	limiter.RegisterChecker(strategyName, Checker)
}
