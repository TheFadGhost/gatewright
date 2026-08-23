package fixedwindow

import (
	"errors"
	"time"

	"gatewright/internal/limiter"
)

// StrategyName is the config-facing name of this strategy.
const StrategyName = "fixed_window"

// Factory builds a fixed_window limiter from validated settings.
func Factory(p limiter.Params) (limiter.Limiter, error) {
	cfg, err := configFrom(p.Settings)
	if err != nil {
		return nil, err
	}
	return limiter.NewEngine(StrategyName, algo{cfg: cfg}, p), nil
}

func configFrom(s limiter.Settings) (Config, error) {
	if s.Limit < 1 {
		return Config{}, errors.New("fixed_window: limit must be >= 1")
	}
	if s.Window < time.Millisecond {
		return Config{}, errors.New("fixed_window: window must be >= 1ms")
	}
	return Config{Limit: s.Limit, Window: s.Window}, nil
}

// Checker validates Settings for fixed_window, returning human-readable
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
	limiter.Register(StrategyName, Factory)
	limiter.RegisterChecker(StrategyName, Checker)
}
