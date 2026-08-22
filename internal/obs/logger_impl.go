package obs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// knownFieldSet is the fixed access-log vocabulary.
var knownFieldSet = func() map[string]int {
	m := map[string]int{}
	fields := []string{
		"ts", "req_id", "method", "path", "query", "route", "upstream",
		"upstream_addr", "status", "bytes_in", "bytes_out", "duration_ms",
		"remote", "code", "limiter_name", "limiter_outcome",
	}
	for i, f := range fields {
		m[f] = i
	}
	return m
}()

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
		if _, ok := knownFieldSet[f]; ok {
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
		put := func(key string, v any) {
			if len(l.fields) > 0 && !l.fields[key] {
				return
			}
			m[key] = v
		}
		put("ts", orDefault(f.TS, time.Now().UTC().Format(time.RFC3339Nano)))
		put("level", "info")
		put("msg", "access")
		put("req_id", f.ReqID)
		put("method", f.Method)
		put("path", f.Path)
		put("query", f.Query)
		put("route", f.Route)
		put("upstream", f.Upstream)
		put("upstream_addr", f.UpstreamAddr)
		put("status", f.Status)
		put("bytes_in", f.BytesIn)
		put("bytes_out", f.BytesOut)
		put("duration_ms", f.DurationMS)
		put("remote", f.Remote)
		put("code", f.Code)
		put("limiter_name", f.LimiterName)
		put("limiter_outcome", f.LimiterOutcome)
		buf := bytes.Buffer{}
		_ = json.NewEncoder(&buf).Encode(m)
		l.write(buf.Bytes())
		return
	}
	dur := fmt.Sprintf("%.1fms", f.DurationMS)
	line := fmt.Sprintf("%s INF req_id=%s %s %s route=%s upstream=%s status=%d dur=%s out=%dB remote=%s\n",
		orDefault(f.TS, time.Now().UTC().Format(time.RFC3339)),
		orDefault(f.ReqID, "-"), f.Method, f.Path,
		orDefault(f.Route, "-"), orDefault(f.Upstream, "-"),
		f.Status, dur, f.BytesOut, orDefault(f.Remote, "-"),
	)
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
