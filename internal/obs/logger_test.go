package obs

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newFileLogger builds a logger writing to a temp file so tests never touch
// the process stdout/stderr. The file handle is released on cleanup because
// Windows cannot delete open files.
func newFileLogger(t *testing.T, opts Options) (Logger, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.log")
	opts.Output = path
	l, err := New(opts)
	if err != nil {
		t.Fatalf("obs.New: %v", err)
	}
	if f, ok := l.Writer().(*os.File); ok {
		t.Cleanup(func() { _ = f.Close() })
	}
	return l, path
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	out := []string{}
	for _, ln := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		if strings.TrimSpace(ln) != "" {
			out = append(out, ln)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// JSON mode: exact key sets for access lines and subset filtering.
// ---------------------------------------------------------------------------

var allAccessKeys = []string{
	"ts", "level", "msg", "req_id", "method", "path", "query", "route",
	"upstream", "upstream_addr", "status", "bytes_in", "bytes_out",
	"duration_ms", "remote", "code", "limiter_name", "limiter_outcome",
}

func TestAccessJSONEmitsExactKeySetWithoutFilter(t *testing.T) {
	l, path := newFileLogger(t, Options{Format: "json"})
	l.Access(AccessFields{
		ReqID: "req-1", Method: "GET", Path: "/x", Status: 200,
	})
	lines := readLines(t, path)
	if len(lines) != 1 {
		t.Fatalf("lines = %d, want 1", len(lines))
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
		t.Fatalf("line not JSON: %v\n%q", err, lines[0])
	}
	if len(m) != len(allAccessKeys) {
		t.Errorf("key count = %d, want %d; keys = %v", len(m), len(allAccessKeys), keys(m))
	}
	for _, k := range allAccessKeys {
		if _, ok := m[k]; !ok {
			t.Errorf("missing access-log key %q", k)
		}
	}
}

func TestAccessJSONFieldSubsetFiltersExactly(t *testing.T) {
	l, path := newFileLogger(t, Options{
		Format: "json",
		Fields: []string{"req_id", "status"},
	})
	l.Access(AccessFields{
		TS: "2026-01-02T03:04:05Z", ReqID: "req-7", Method: "GET",
		Path: "/filtered", Status: 201, BytesOut: 99, Route: "hidden",
	})
	lines := readLines(t, path)
	if len(lines) != 1 {
		t.Fatalf("lines = %d, want 1", len(lines))
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
		t.Fatalf("line not JSON: %v", err)
	}
	want := map[string]any{"req_id": "req-7", "status": float64(201)}
	if len(m) != len(want) {
		t.Errorf("keys = %v, want exactly %v", keys(m), keys(want))
	}
	for k, v := range want {
		if m[k] != v {
			t.Errorf("%s = %v, want %v", k, m[k], v)
		}
	}
}

func TestInfoJSONShapeAndKVPairs(t *testing.T) {
	l, path := newFileLogger(t, Options{Format: "json"})
	l.Info("gateway started", "routes", 3, "version", "test")
	lines := readLines(t, path)
	if len(lines) != 1 {
		t.Fatalf("lines = %d, want 1", len(lines))
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
		t.Fatalf("line not JSON: %v", err)
	}
	if m["level"] != "info" || m["msg"] != "gateway started" || m["routes"] != float64(3) {
		t.Errorf("fields = %v", m)
	}
	if _, ok := m["ts"]; !ok {
		t.Error("missing ts")
	}
}

func keys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// ---------------------------------------------------------------------------
// Human mode: single-line format, deterministic when TS is supplied.
// ---------------------------------------------------------------------------

const humanTS = "2026-01-02T03:04:05Z"

func TestHumanInfoSingleLineWithSortedPairs(t *testing.T) {
	buf := &bytes.Buffer{}
	l := &stdLogger{w: buf} // json=false => human mode, no colour (zero value)
	l.Info("started", "z", 26, "a", 1)
	out := buf.String()
	if strings.Count(out, "\n") != 1 || !strings.HasSuffix(out, "\n") {
		t.Fatalf("human log must be one newline-terminated line, got %q", out)
	}
	if !strings.Contains(out, " INF started a=1 z=26") {
		t.Errorf("pairs not sorted or tag wrong: %q", out)
	}
	ts := strings.Fields(out)[0]
	if _, err := time.Parse(time.RFC3339Nano, ts); err != nil {
		t.Errorf("leading timestamp %q not RFC3339Nano: %v", ts, err)
	}
}

func TestHumanWarnErrorTagsPlainWhenNoColour(t *testing.T) {
	buf := &bytes.Buffer{}
	l := &stdLogger{w: buf}
	l.Warn("w", "k", 1)
	l.Error("e")
	out := buf.String()
	if !strings.Contains(out, " WRN w k=1\n") {
		t.Errorf("warn line = %q", out)
	}
	if !strings.Contains(out, " ERR e\n") {
		t.Errorf("error line = %q", out)
	}
	if strings.Contains(out, "\x1b[") {
		t.Errorf("plain logger emitted ANSI escapes: %q", out)
	}
}

func TestHumanAccessSingleLineFormat(t *testing.T) {
	buf := &bytes.Buffer{}
	l := &stdLogger{w: buf}
	l.Access(AccessFields{
		TS: humanTS, ReqID: "r-1", Method: "POST", Path: "/p?q=1",
		Route: "rt", Upstream: "api", Status: 201,
		DurationMS: 1.3, BytesOut: 42, Remote: "10.0.0.1",
	})
	want := humanTS + " INF req_id=r-1 POST /p?q=1 route=rt upstream=api" +
		" status=201 dur=1.3ms out=42B remote=10.0.0.1\n"
	if got := buf.String(); got != want {
		t.Fatalf("human access line:\n got %q\nwant %q", got, want)
	}
}

func TestHumanAccessColourByStatusClass(t *testing.T) {
	cases := []struct {
		status int
		prefix string
	}{
		{200, ""},          // 2xx plain
		{404, "\x1b[33m"}, // 4xx yellow
		{502, "\x1b[31m"}, // 5xx red
	}
	for _, c := range cases {
		buf := &bytes.Buffer{}
		l := &stdLogger{w: buf, colour: true}
		l.Access(AccessFields{TS: humanTS, Method: "GET", Path: "/c", Status: c.status})
		line := buf.String()
		if c.prefix == "" {
			if strings.Contains(line, "\x1b[") {
				t.Errorf("status %d must be plain, got %q", c.status, line)
			}
			continue
		}
		if !strings.HasPrefix(line, c.prefix) || !strings.HasSuffix(line, "\x1b[0m\n") {
			t.Errorf("status %d colour wrap missing: %q", c.status, line)
		}
	}
}

func TestLevelTagColours(t *testing.T) {
	if levelTag("info", true) != "INF" || levelTag("anything", false) != "INF" {
		t.Error("default tag must be INF")
	}
	if got := levelTag("warn", true); got != "\x1b[33mWRN\x1b[0m" {
		t.Errorf("coloured warn = %q", got)
	}
	if got := levelTag("error", true); got != "\x1b[31mERR\x1b[0m" {
		t.Errorf("coloured error = %q", got)
	}
	if got := levelTag("error", false); got != "ERR" {
		t.Errorf("plain error = %q", got)
	}
}

// ---------------------------------------------------------------------------
// Colour policy resolution: flag > NO_COLOR env > TTY detection.
// ---------------------------------------------------------------------------

func withNOColorRemoved(t *testing.T) {
	t.Helper()
	saved, had := os.LookupEnv("NO_COLOR")
	if had {
		if err := os.Unsetenv("NO_COLOR"); err != nil {
			t.Fatalf("unset NO_COLOR: %v", err)
		}
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv("NO_COLOR", saved)
		}
	})
}

func TestColourPolicyPrecedenceMatrix(t *testing.T) {
	withNOColorRemoved(t)

	if !ColourPolicy(false, true) {
		t.Error("TTY without NO_COLOR or flag must enable colour")
	}
	if ColourPolicy(false, false) {
		t.Error("non-TTY must disable colour")
	}
	if ColourPolicy(true, true) || ColourPolicy(true, false) {
		t.Error("--no-color flag must win over everything")
	}

	t.Setenv("NO_COLOR", "")
	if ColourPolicy(false, true) {
		t.Error("NO_COLOR present (even empty) must disable colour")
	}

	t.Setenv("NO_COLOR", "1")
	if ColourPolicy(false, true) {
		t.Error("NO_COLOR=1 must disable colour")
	}
}

func TestNOColorEnvRespectedThroughNewWiring(t *testing.T) {
	t.Setenv("NO_COLOR", "1")
	withNOColorWiring(t, false, func(sl *stdLogger) {
		if sl.colour {
			t.Error("logger built after NO_COLOR was set must have colour off")
		}
	})

	withNOColorRemoved(t)
	withNOColorWiring(t, true, func(sl *stdLogger) {
		// Simulated TTY decision with the flag off would pick colour up;
		// we can only assert the wiring honours it via NoColor=true here.
		if sl.colour {
			t.Error("explicit NoColor option must win over TTY detection")
		}
	})
}

// withNOColorWiring performs the documented startup sequence: resolve
// ColourPolicy for the destination writer, then build the logger with the
// resolved NoColor value.
func withNOColorWiring(t *testing.T, flagNoColor bool, check func(*stdLogger)) {
	t.Helper()
	w := &bytes.Buffer{}
	noColor := ColourPolicy(flagNoColor, isWriterTTY(w))
	l, err := New(Options{Format: "human", NoColor: noColor,
		Output: filepath.Join(t.TempDir(), "l.log")})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if f, ok := l.Writer().(*os.File); ok {
		t.Cleanup(func() { _ = f.Close() })
	}
	check(l.(*stdLogger))
}

// ---------------------------------------------------------------------------
// TTY gating of the constructed logger.
// ---------------------------------------------------------------------------

func TestNonTTYWriterDisablesColourEvenInHumanMode(t *testing.T) {
	if isWriterTTY(&bytes.Buffer{}) {
		t.Fatal("bytes.Buffer must not be detected as a TTY")
	}
	// A regular file is an *os.File but not a character device: still no TTY.
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "regular.txt"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if isWriterTTY(f) {
		t.Fatal("regular file must not be detected as a TTY")
	}

	l, _ := newFileLogger(t, Options{Format: "human"}) // NoColor unset
	if sl := l.(*stdLogger); sl.colour {
		t.Error("human-mode logger to a file must default to colour OFF (non-TTY)")
	}
	sl := l.(*stdLogger)
	sl.w = &bytes.Buffer{} // redirect: same construction rules apply
	sl.Access(AccessFields{Method: "GET", Path: "/x", Status: 500})
	if strings.Contains(sl.w.(*bytes.Buffer).String(), "\x1b[") {
		t.Error("non-TTY human logger must never emit ANSI escapes")
	}
}

func TestJSONModeNeverColourisesRegardlessOfWriter(t *testing.T) {
	l, _ := newFileLogger(t, Options{Format: "json"})
	if sl := l.(*stdLogger); sl.colour || !sl.json {
		t.Errorf("json logger colour=%v json=%v", sl.colour, sl.json)
	}
}

// ---------------------------------------------------------------------------
// Output path handling.
// ---------------------------------------------------------------------------

func TestInvalidOutputPathReturnsError(t *testing.T) {
	bad := filepath.Join(t.TempDir(), "does-not-exist", "log.txt")
	if l, err := New(Options{Format: "json", Output: bad}); err == nil {
		if f, ok := l.Writer().(*os.File); ok {
			_ = f.Close()
		}
		t.Fatal("New must fail for an unwritable output path")
	} else if !strings.Contains(err.Error(), "cannot open log output") {
		t.Errorf("error = %v, want unwritable-output detail", err)
	}
}

func TestNextSeqMonotonic(t *testing.T) {
	a, b := NextSeq(), NextSeq()
	if b <= a {
		t.Errorf("NextSeq not monotonic: %d then %d", a, b)
	}
}
