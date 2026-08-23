package middleware

import (
	"context"

	"gatewright/internal/obs"
)

// Record is the per-request accumulator later stages enrich; the access-log
// stage reads it when emitting the final line. Proxy fills Route/Upstream/
// UpstreamAddr/Code/Limiter fields; earlier stages may set Code when they
// short-circuit before the proxy ever runs (denials are real error codes).
type Record struct {
	Fields obs.AccessFields
}

type recordCtxKey struct{}

// WithRecord installs rec into ctx.
func WithRecord(ctx context.Context, rec *Record) context.Context {
	return context.WithValue(ctx, recordCtxKey{}, rec)
}

// RecordFrom returns the request's record, or nil when none was installed.
func RecordFrom(ctx context.Context) *Record {
	rec, _ := ctx.Value(recordCtxKey{}).(*Record)
	return rec
}

// recordCode tags the access line with an error code when a stage rejects the
// request itself. No-op without a record (access logging disabled upstream).
func recordCode(ctx context.Context, code string) {
	if rec := RecordFrom(ctx); rec != nil {
		rec.Fields.Code = code
	}
}
