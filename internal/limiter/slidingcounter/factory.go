package slidingcounter

import (
	"errors"
	"time"

	"gatewright/internal/limiter"
)

// StrategyName is the config-facing name of this strategy.
const StrategyName = "sliding_window_counter"

// Factory builds a sliding_window_counter limiter from validated settings.
func Factory(p limiter.Params) (limiter.Limiter, error) {
	cfg, err := configFrom(p.Settings)
	if err != nil {
		return nil, err
	}
	return limiter.NewEngine(StrategyName, algo{cfg: cfg}, p), nil
}

func configFrom(s limiter.Settings) (Config, error) {
	if s.Limit < 1 {
		return Config{}, errors.New("sliding_window_counter: limit must be >= 1")
	}
	if s.Window < time.Millisecond {
		return Config{}, errors.New("sliding_window_counter: window must be >= 1ms")
	}
	cells := s.Cells
	if cells == 0 {
		cells = DefaultCells
	}
	if cells < 2 || cells > 1000 {
		return Config{}, errors.New("sliding_window_counter: cells must be in range 2..1000")
	}
	return Config{Limit: s.Limit, Window: s.Window, Cells: int64(cells)}, nil
}

// Checker validates Settings for sliding_window_counter, returning
// human-readable problems (empty slice = valid).
func Checker(s limiter.Settings) []string {
	var probs []string
	if s.Limit < 1 {
		probs = append(probs, "limit must be >= 1")
	}
	if s.Window < time.Millisecond {
		probs = append(probs, `window must be >= 1ms (write it with a unit suffix, e.g. "30s")`)
	}
	cells := s.Cells
	if cells == 0 {
		cells = DefaultCells
	}
	if cells < 2 || cells > 1000 {
		probs = append(probs, "cells must be in range 2..1000 (default 10)")
	}
	return probs
}

func init() {
	limiter.Register(StrategyName, Factory)
	limiter.RegisterChecker(StrategyName, Checker)
}
