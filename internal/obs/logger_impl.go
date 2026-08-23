package obs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type stdLogger struct {
	mu     sync.Mutex
	w      io.Writer
	json   bool
	fields map[string]bool // access-log field filter; empty = all
	colour bool            // human mode ANSI colouring (TTY-gated)
}

func newLogger(opts Options) (Logger, error) {
	var w io.Writer
	switch opts.Output {
	case "", "stdout":
		w = os.Stdout
	case "stderr":
		w = os.Stderr
	default:
		f, err := os.OpenFile(opts.Output, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, fmt.Errorf("obs: cannot open log output %q: %w", opts.Output, err)
		}
		w = f
	}
	fieldFilter := map[string]bool{}
	for _, f := range opts.Fields {
		if ValidAccessLogField(f) {
			fieldFilter[f] = true
		}
	}
	return &stdLogger{
		w:      w,
		json:   opts.Format != "human",
		fields: fieldFilter,
		colour: opts.NoColor == false && opts.Format == "human" && isWriterTTY(w),
	}, nil
}

func (l *stdLogger) Writer() io.Writer { return l.w }

func (l *stdLogger) log(level, msg string, kv []any) {
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	if l.json {
		buf := bytes.Buffer{}
		enc := json.NewEncoder(&buf)
		m := map[string]any{"ts": ts, "level": level, "msg": msg}
		for i := 0; i+1 < len(kv); i += 2 {
			k, ok := kv[i].(string)
			if !ok {
				continue
			}
			m[k] = kv[i+1]
		}
		_ = enc.Encode(m)
		l.write(buf.Bytes())
		return
	}
	var b strings.Builder
	b.WriteString(ts)
	b.WriteString(" ")
	b.WriteString(levelTag(level, l.colour))
	b.WriteString(" ")
	b.WriteString(msg)
	pairs := make([]string, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		k, ok := kv[i].(string)
		if !ok {
			continue
		}
		pairs = append(pairs, fmt.Sprintf("%s=%v", k, kv[i+1]))
	}
	sort.Strings(pairs)
	for _, p := range pairs {
		b.WriteString(" ")
		b.WriteString(p)
	}
	b.WriteString("\n")
	l.write([]byte(b.String()))
}

func levelTag(level string, colour bool) string {
	switch level {
	case "warn":
		if colour {
			return "\x1b[33mWRN\x1b[0m"
		}
		return "WRN"
	case "error":
		if colour {
			return "\x1b[31mERR\x1b[0m"
		}
		return "ERR"
	default:
		return "INF"
	}
}

func (l *stdLogger) Info(msg string, kv ...any)  { l.log("info", msg, kv) }
func (l *stdLogger) Warn(msg string, kv ...any)  { l.log("warn", msg, kv) }
func (l *stdLogger) Error(msg string, kv ...any) { l.log("error", msg, kv) }

func (l *stdLogger) Access(f AccessFields) {
	if l.json {
		m := map[string]any{}
		selected := func(key string) bool { return len(l.fields) == 0 || l.fields[key] }
		// Core envelope keys are never filtered out.
		m["ts"] = orDefault(f.TS, time.Now().UTC().Format(time.RFC3339Nano))
		m["level"] = "info"
		m["msg"] = "access"
		str := func(key, v string) {
			if selected(key) && v != "" { // zero-value strings are omitted
				m[key] = v
			}
		}
		num := func(key string, v any) {
			if selected(key) {
				m[key] = v
			}
		}
		str("req_id", f.ReqID)
		str("method", f.Method)
		str("path", f.Path)
		str("query", f.Query)
		str("route", f.Route)
		str("upstream", f.Upstream)
		str("upstream_addr", f.UpstreamAddr)
		num("status", f.Status)
		num("bytes_in", f.BytesIn)
		num("bytes_out", f.BytesOut)
		num("duration_ms", f.DurationMS)
		str("remote", f.Remote)
		str("code", f.Code)
		str("limiter_name", f.LimiterName)
		str("limiter_outcome", f.LimiterOutcome)
		buf := bytes.Buffer{}
		_ = json.NewEncoder(&buf).Encode(m)
		l.write(buf.Bytes())
		return
	}

	// Human mode: the core four (ts, req_id, status, duration_ms) are always
	// present; every other configured field renders as a sorted key=value
	// pair with empty-string values omitted, mirroring the JSON filter.
	dur := fmt.Sprintf("%.1fms", f.DurationMS)
	var b strings.Builder
	b.WriteString(orDefault(f.TS, time.Now().UTC().Format(time.RFC3339)))
	b.WriteString(" INF req_id=")
	b.WriteString(orDefault(f.ReqID, "-"))
	b.WriteString(" status=")
	b.WriteString(strconv.Itoa(f.Status))
	b.WriteString(" duration_ms=")
	b.WriteString(dur)
	pairs := humanPairs(l.fields, f)
	sort.Strings(pairs)
	for _, p := range pairs {
		b.WriteString(" ")
		b.WriteString(p)
	}
	b.WriteString("\n")
	line := b.String()
	if l.colour {
		switch {
		case f.Status >= 500:
			line = "\x1b[31m" + line[:len(line)-1] + "\x1b[0m\n"
		case f.Status >= 400:
			line = "\x1b[33m" + line[:len(line)-1] + "\x1b[0m\n"
		}
	}
	l.write([]byte(line))
}

// humanPairs renders the optional access fields as key=value pairs honouring
// the configured subset and omitting empty-string values. The core four
// (ts/req_id/status/duration_ms) are handled by Access itself.
func humanPairs(fields map[string]bool, f AccessFields) []string {
	selected := func(key string) bool { return len(fields) == 0 || fields[key] }
	pairs := make([]string, 0, 12)
	add := func(key, v string) {
		if selected(key) && v != "" {
			pairs = append(pairs, key+"="+v)
		}
	}
	add("method", f.Method)
	add("path", f.Path)
	add("query", f.Query)
	add("route", f.Route)
	add("upstream", f.Upstream)
	add("upstream_addr", f.UpstreamAddr)
	if selected("bytes_in") {
		pairs = append(pairs, fmt.Sprintf("bytes_in=%d", f.BytesIn))
	}
	if selected("bytes_out") {
		pairs = append(pairs, fmt.Sprintf("bytes_out=%d", f.BytesOut))
	}
	add("remote", f.Remote)
	add("code", f.Code)
	add("limiter_name", f.LimiterName)
	add("limiter_outcome", f.LimiterOutcome)
	return pairs
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func (l *stdLogger) write(p []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.w.Write(p)
}
