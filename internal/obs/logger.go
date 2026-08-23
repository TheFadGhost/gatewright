// Package obs provides structured logging, Prometheus-format metrics,
// colour policy and request-trace support. Field names are fixed in DESIGN.md.
package obs

import (
	"io"
	"os"
)

// Logger writes structured events. Implementations must be safe for
// concurrent use and must never panic.
type Logger interface {
	// Info/Warn/Error attach key-value fields (alternating string, any).
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
	// Access emits one access-log line with the fixed field set.
	Access(fields AccessFields)
	// Writer exposes the underlying sink for tests.
	Writer() io.Writer
}

// AccessFields is the fixed access-log schema (DESIGN.md §4). Zero values are
// omitted per the configured field subset; names never change.
type AccessFields struct {
	TS             string // RFC3339Nano
	ReqID          string
	Method         string
	Path           string
	Query          string
	Route          string
	Upstream       string
	UpstreamAddr   string
	Status         int
	BytesIn        int64
	BytesOut       int64
	DurationMS     float64
	Remote         string
	Code           string // error code ("", "RATE001", ...)
	LimiterName    string
	LimiterOutcome string // "allowed" | "limited"
}

// Options configures logger construction.
type Options struct {
	Format  string   // "json" | "human" — validated by config
	Output  string   // "stdout" | "stderr" | file path
	Fields  []string // subset of known fields; empty = all
	NoColor bool     // resolved colour policy (see ColourPolicy)
}

// ColourPolicy resolves the terminal colour decision ONCE at startup:
// explicit --no-color wins; NO_COLOR env wins next; otherwise colour only
// when the destination is a TTY. Palette is standard 16-colour ANSI.
func ColourPolicy(flagNoColor bool, isTTY bool) bool {
	if flagNoColor {
		return false
	}
	if _, set := os.LookupEnv("NO_COLOR"); set {
		return false
	}
	return isTTY
}

// New builds the configured logger. Errors on unwritable file outputs are
// returned; callers fail fast rather than silently dropping logs.
func New(opts Options) (Logger, error) {
	return newLogger(opts)
}

// AccessLogFields is the fixed access-log vocabulary (DESIGN.md §4). It is
// the single source of truth shared by config validation and the logger's
// field filter; names never change.
var AccessLogFields = []string{
	"ts", "req_id", "method", "path", "query", "route", "upstream",
	"upstream_addr", "status", "bytes_in", "bytes_out", "duration_ms",
	"remote", "code", "limiter_name", "limiter_outcome",
}

// ValidAccessLogField reports whether name is part of the fixed vocabulary.
func ValidAccessLogField(name string) bool {
	for _, f := range AccessLogFields {
		if f == name {
			return true
		}
	}
	return false
}
