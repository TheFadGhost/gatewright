package middleware

import (
	"context"
	"time"
)

// Trace collects per-stage timing when request tracing is enabled
// (X-Gatewright-Trace: 1). It is the observable form of the documented
// middleware order: stages appear in OrderNames sequence.
type Trace struct {
	Route   string
	Stages  []StageTiming // completed in execution order
	Enabled bool
}

// StageTiming is one pipeline stage measurement.
type StageTiming struct {
	Name     string
	Duration time.Duration
}

type traceCtxKey struct{}

// WithTrace installs a fresh trace into the context.
func WithTrace(ctx context.Context, t *Trace) context.Context {
	return context.WithValue(ctx, traceCtxKey{}, t)
}

// TraceFrom returns the request's trace, or nil when tracing is off.
func TraceFrom(ctx context.Context) *Trace {
	t, _ := ctx.Value(traceCtxKey{}).(*Trace)
	return t
}

// RecordStage appends a stage duration. Safe to call with tracing disabled
// (no-op). Renamed from Record so that record.go's required accumulator type
// Record can own the short name.
func RecordStage(ctx context.Context, name string, d time.Duration) {
	if t := TraceFrom(ctx); t != nil {
		t.Stages = append(t.Stages, StageTiming{Name: name, Duration: d})
	}
}
