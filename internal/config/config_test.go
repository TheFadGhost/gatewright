package config

import (
	"strings"
	"testing"
	"time"

	_ "gatewright/internal/limiter/builtin" // registers strategies so validation sees the known set
	"gatewright/internal/limiter"
	"gopkg.in/yaml.v3"
)

// upstreamBlock is a minimal valid pool referenced by route fixtures below.
const upstreamBlock = `
upstreams:
  api:
    targets:
      - url: "http://127.0.0.1:9001"
        weight: 1
`

// doc prefixes fixture bodies with version and the shared upstream pool.
func doc(body string) string {
	return "version: 1\n" + upstreamBlock + "\n" + body
}

// findErr parses doc and returns the single error reported at exactly path,
// failing the test when parsing succeeds or the path is absent/multi-reported.
func findErr(t *testing.T, docStr, path string) *Error {
	t.Helper()
	_, verr := Parse([]byte(docStr), "test.yaml")
	if verr == nil {
		t.Fatalf("expected a validation error at %q, config parsed clean", path)
	}
	var found []*Error
	for _, e := range verr.Errors {
		if e.Path == path {
			found = append(found, e)
		}
	}
	switch len(found) {
	case 1:
		return found[0]
	case 0:
		t.Fatalf("no error reported at exact path %q.\nErrors found:\n%s", path, verr.Error())
	default:
		t.Fatalf("%d errors share path %q; expected exact-path uniqueness:\n%s",
			len(found), path, verr.Error())
	}
	return nil
}

func wantCode(t *testing.T, e *Error, code string) {
	t.Helper()
	if e.Code != code {
		t.Fatalf("error at %s has code %q, want %q (%+v)", e.Path, e.Code, code, e)
	}
}

// ---------------------------------------------------------------------------
// Validation matrix: every malformed case class with exact-path assertions.
// ---------------------------------------------------------------------------

func TestValidationMatrixExactPaths(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		path string
		code string
		want func(*testing.T, *Error)
	}{
		{
			name: "unknown key at top level",
			doc:  "version: 1\ntotally_unknown: true\n",
			path: "totally_unknown",
			code: CodeUnknownField, // CFG001
			want: func(t *testing.T, e *Error) {
				if e.Line == 0 || e.Column == 0 {
					t.Errorf("unknown key must carry source position, got line=%d col=%d", e.Line, e.Column)
				}
				if !strings.Contains(e.Found, "totally_unknown") {
					t.Errorf("Found = %q", e.Found)
				}
				if !strings.Contains(e.Expected, "documented configuration keys") {
					t.Errorf("Expected = %q", e.Expected)
				}
			},
		},
		{
			name: "unknown key at server depth",
			doc: doc(`
server:
  listen: "127.0.0.1:8080"
  bogus_server_key: 1
`),
			path: "server.bogus_server_key",
			code: CodeUnknownField,
		},
		{
			name: "unknown key at route depth",
			doc: doc(`
routes:
  - name: r1
    path_prefix: /api
    upstreams: api
    bogus_route_key: 1
`),
			path: "routes[0].bogus_route_key",
			code: CodeUnknownField,
		},
		{
			name: "unknown key at ratelimit depth",
			doc: doc(`
routes:
  - name: r1
    path_prefix: /api
    upstreams: api
    rate_limits:
      - name: rl1
        strategy: fixed_window
        limit: 5
        window: 10s
        key: ip
        bogus_rl_key: 1
`),
			path: "routes[0].rate_limits[0].bogus_rl_key",
			code: CodeUnknownField,
		},
		{
			name: "bare integer duration rejected with unit hint",
			doc: doc(`
server:
  read_timeout: 30
`),
			path: "server.read_timeout",
			code: CodeInvalidValue, // CFG002
			want: func(t *testing.T, e *Error) {
				if !strings.Contains(e.Hint, "unit") {
					t.Errorf("Hint = %q, want mention of \"unit\"", e.Hint)
				}
				if !strings.Contains(e.Expected, "unit suffix") {
					t.Errorf("Expected = %q", e.Expected)
				}
				if !strings.Contains(e.Found, `"30"`) {
					t.Errorf("Found = %q", e.Found)
				}
			},
		},
		{
			name: "bad size unit",
			doc: doc(`
server:
  max_header_bytes: "16ZiB"
`),
			path: "server.max_header_bytes",
			code: CodeInvalidValue,
			want: func(t *testing.T, e *Error) {
				if !strings.Contains(e.Found, `"ZIB"`) {
					t.Errorf("Found = %q, want unknown-unit detail", e.Found)
				}
				if !strings.Contains(e.Expected, "16KiB") {
					t.Errorf("Expected = %q", e.Expected)
				}
			},
		},
		{
			name: "missing route name",
			doc: doc(`
routes:
  - path_prefix: /api
    upstreams: api
`),
			path: "routes[0].name",
			code: CodeMissingRequired, // CFG003
		},
		{
			name: "missing route upstreams",
			doc: doc(`
routes:
  - name: r1
    path_prefix: /api
`),
			path: "routes[0].upstreams",
			code: CodeMissingRequired,
		},
		{
			name: "missing ratelimit limit",
			doc: doc(`
routes:
  - name: r1
    upstreams: api
    rate_limits:
      - name: rl1
        strategy: fixed_window
        window: 10s
        key: ip
`),
			path: "routes[0].rate_limits[0].limit",
			code: CodeInvalidValue,
			want: func(t *testing.T, e *Error) {
				if e.Expected != ">= 1" {
					t.Errorf("Expected = %q, want \">= 1\"", e.Expected)
				}
				// The strategy checker also flags the gap on the limiter itself.
				rl := findErr(t, doc(`
routes:
  - name: r1
    upstreams: api
    rate_limits:
      - name: rl1
        strategy: fixed_window
        window: 10s
        key: ip
`), "routes[0].rate_limits[0]")
				if !strings.Contains(rl.Found, "limit must be >= 1") {
					t.Errorf("checker problem = %q", rl.Found)
				}
			},
		},
		{
			name: "duplicate route names",
			doc: doc(`
routes:
  - name: dup
    path_prefix: /a
    upstreams: api
  - name: dup
    path_prefix: /b
    upstreams: api
`),
			path: "routes[1].name",
			code: CodeDuplicateName, // CFG004
			want: func(t *testing.T, e *Error) {
				if !strings.Contains(e.Found, `"dup"`) {
					t.Errorf("Found = %q", e.Found)
				}
			},
		},
		{
			name: "duplicate limiter names within one route",
			doc: doc(`
routes:
  - name: r1
    upstreams: api
    rate_limits:
      - name: rl1
        strategy: fixed_window
        limit: 5
        window: 10s
        key: ip
      - name: rl1
        strategy: fixed_window
        limit: 9
        window: 20s
        key: path
`),
			path: "routes[0].rate_limits[1].name",
			code: CodeDuplicateName,
		},
		{
			name: "unknown strategy names the known alternatives",
			doc: doc(`
routes:
  - name: r1
    upstreams: api
    rate_limits:
      - name: rl1
        strategy: token_buket
        limit: 5
        window: 10s
        key: ip
`),
			path: "routes[0].rate_limits[0].strategy",
			code: CodeInvalidValue,
			want: func(t *testing.T, e *Error) {
				want := "one of: " + strings.Join(limiter.Strategies(), ", ")
				if e.Expected != want {
					t.Errorf("Expected = %q, want %q", e.Expected, want)
				}
			},
		},
		{
			name: "fixed_window without window",
			doc: doc(`
routes:
  - name: r1
    upstreams: api
    rate_limits:
      - name: rl1
        strategy: fixed_window
        limit: 5
        key: ip
`),
			path: "routes[0].rate_limits[0]",
			code: CodeInvalidValue,
			want: func(t *testing.T, e *Error) {
				if !strings.Contains(e.Found, "window must be >= 1ms") {
					t.Errorf("Found = %q", e.Found)
				}
			},
		},
		{
			name: "concurrency without capacity or limit",
			doc: doc(`
routes:
  - name: r1
    upstreams: api
    rate_limits:
      - name: rl1
        strategy: concurrency
        key: ip
`),
			path: "routes[0].rate_limits[0]",
			code: CodeInvalidValue,
			want: func(t *testing.T, e *Error) {
				if !strings.Contains(e.Found, "capacity must be >= 1") {
					t.Errorf("Found = %q", e.Found)
				}
			},
		},
		{
			name: "sliding_window_counter cells out of range",
			doc: doc(`
routes:
  - name: r1
    upstreams: api
    rate_limits:
      - name: rl1
        strategy: sliding_window_counter
        limit: 5
        window: 10s
        key: ip
        cells: 1
`),
			path: "routes[0].rate_limits[0].cells",
			code: CodeInvalidValue,
			want: func(t *testing.T, e *Error) {
				if e.Expected != "between 2 and 1000" {
					t.Errorf("Expected = %q", e.Expected)
				}
			},
		},
		{
			name: "composite key selector with a single part",
			doc: doc(`
routes:
  - name: r1
    upstreams: api
    rate_limits:
      - name: rl1
        strategy: fixed_window
        limit: 5
        window: 10s
        key: composite[one]
`),
			path: "routes[0].rate_limits[0].key",
			code: CodeInvalidValue,
			want: func(t *testing.T, e *Error) {
				if !strings.Contains(e.Expected, "2 to 4") {
					t.Errorf("Expected = %q", e.Expected)
				}
			},
		},
		{
			name: "key selector with invalid header name",
			doc: doc(`
routes:
  - name: r1
    upstreams: api
    rate_limits:
      - name: rl1
        strategy: fixed_window
        limit: 5
        window: 10s
        key: "header:Bad Name"
`),
			path: "routes[0].rate_limits[0].key",
			code: CodeInvalidValue,
			want: func(t *testing.T, e *Error) {
				if !strings.Contains(e.Expected, `invalid header name`) {
					t.Errorf("Expected = %q", e.Expected)
				}
				if !strings.Contains(e.Found, `"header:Bad Name"`) {
					t.Errorf("Found = %q", e.Found)
				}
			},
		},
		{
			name: "ring_hash without hash_key",
			doc: `
version: 1
upstreams:
  api:
    targets:
      - url: "http://127.0.0.1:9001"
    load_balance: ring_hash
`,
			path: "upstreams.api.hash_key",
			code: CodeSemanticConflict, // CFG005
			want: func(t *testing.T, e *Error) {
				want := "ip | path | api_key | header:<name> when load_balance is ring_hash"
				if e.Expected != want {
					t.Errorf("Expected = %q, want %q", e.Expected, want)
				}
			},
		},
		{
			name: "strip_prefix without a path predicate",
			doc: doc(`
routes:
  - name: r1
    hosts: ["api.example.com"]
    upstreams: api
    strip_prefix: true
`),
			path: "routes[0].strip_prefix",
			code: CodeSemanticConflict,
		},
		{
			name: "mirror percentage over 100",
			doc: doc(`
routes:
  - name: r1
    upstreams: api
    mirror:
      upstreams: api
      percentage: 150
`),
			path: "routes[0].mirror.percentage",
			code: CodeInvalidValue,
			want: func(t *testing.T, e *Error) {
				if e.Expected != "> 0 and <= 100" {
					t.Errorf("Expected = %q", e.Expected)
				}
				if e.Found != "150" {
					t.Errorf("Found = %q", e.Found)
				}
			},
		},
		{
			name: "non-loopback admin listen without auth",
			doc: `
version: 1
admin:
  listen: "0.0.0.0:9901"
`,
			path: "admin.auth",
			code: CodeUnsafeCombination, // CFG006
			want: func(t *testing.T, e *Error) {
				if !strings.Contains(e.Hint, "token_env") {
					t.Errorf("Hint = %q", e.Hint)
				}
			},
		},
		{
			name: "tls10 accepted but flagged unsafe",
			doc: `
version: 1
server:
  tls:
    cert_file: cert.pem
    key_file: key.pem
    min_version: tls10
`,
			path: "server.tls.min_version",
			code: CodeUnsafeCombination,
			want: func(t *testing.T, e *Error) {
				if e.Found != "tls10" {
					t.Errorf("Found = %q", e.Found)
				}
				if !strings.Contains(e.Hint, "deprecated") {
					t.Errorf("Hint = %q", e.Hint)
				}
			},
		},
		{
			name: "jwt HS algorithms paired with jwks_url",
			doc: doc(`
routes:
  - name: r1
    upstreams: api
    auth:
      type: jwt
      jwt:
        jwks_url: https://id.example.com/jwks.json
        algorithms: [HS256]
`),
			path: "routes[0].auth.jwt",
			code: CodeSemanticConflict,
			want: func(t *testing.T, e *Error) {
				want := "secret_env when HS* algorithms are listed"
				if e.Expected != want {
					t.Errorf("Expected = %q, want %q", e.Expected, want)
				}
			},
		},
		{
			name: "shared limiter backend without store.path",
			doc: doc(`
routes:
  - name: r1
    upstreams: api
    rate_limits:
      - name: rl1
        strategy: fixed_window
        limit: 5
        window: 10s
        key: ip
        backend: shared
`),
			path: "store.path",
			code: CodeSemanticConflict,
			want: func(t *testing.T, e *Error) {
				if !strings.Contains(e.Hint, "bbolt") {
					t.Errorf("Hint = %q", e.Hint)
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			e := findErr(t, tc.doc, tc.path)
			wantCode(t, e, tc.code)
			if tc.want != nil {
				tc.want(t, e)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Valid minimal config: every documented default applied correctly.
// ---------------------------------------------------------------------------

func TestValidMinimalConfigAppliesEveryDefault(t *testing.T) {
	cfg := mustParse(t, doc(`
routes:
  - name: r1
    path_prefix: /api
    upstreams: api
    rate_limits:
      - name: per-ip
        strategy: fixed_window
        key: ip
        limit: 100
        window: 30s
`))

	if cfg.Version != 1 {
		t.Errorf("Version = %d", cfg.Version)
	}

	// Server defaults.
	if cfg.Server.Listen != ":8080" {
		t.Errorf("Server.Listen = %q", cfg.Server.Listen)
	}
	for _, d := range []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"read_timeout", cfg.Server.ReadTimeout.D, 30 * time.Second},
		{"write_timeout", cfg.Server.WriteTimeout.D, 60 * time.Second},
		{"idle_timeout", cfg.Server.IdleTimeout.D, 120 * time.Second},
	} {
		if d.got != d.want {
			t.Errorf("server.%s = %v, want %v", d.name, d.got, d.want)
		}
	}
	if got := cfg.Server.MaxHeaderBytes.N; got != 1024*1024 {
		t.Errorf("max_header_bytes = %d, want 1MiB", got)
	}
	if cfg.Server.MaxHeaderBytes.IsSet() != true || cfg.Server.ReadTimeout.IsSet() != true {
		t.Error("defaulted scalars must report IsSet")
	}
	if cfg.Server.TLS == nil {
		t.Fatal("Server.TLS must be defaulted non-nil")
	}
	if cfg.Server.TLS.MinVersion != "tls12" {
		t.Errorf("tls.min_version = %q, want tls12", cfg.Server.TLS.MinVersion)
	}

	// Admin defaults.
	if cfg.Admin.Listen != "127.0.0.1:9901" {
		t.Errorf("Admin.Listen = %q", cfg.Admin.Listen)
	}

	// Observability defaults.
	if cfg.Observability.AccessLog.Format != "json" {
		t.Errorf("access_log.format = %q", cfg.Observability.AccessLog.Format)
	}
	if cfg.Observability.AccessLog.Output != "stdout" {
		t.Errorf("access_log.output = %q", cfg.Observability.AccessLog.Output)
	}
	if !cfg.Observability.AccessLog.EnabledOrDefault() {
		t.Error("access_log enabled default = false, want true")
	}
	if cfg.Observability.Metrics.Path != "/metrics" {
		t.Errorf("metrics.path = %q", cfg.Observability.Metrics.Path)
	}
	if !cfg.Observability.Metrics.EnabledOrDefault() {
		t.Error("metrics enabled default = false, want true")
	}

	// Upstream defaults.
	up := cfg.Upstreams["api"]
	if up.LoadBalance != "round_robin" {
		t.Errorf("load_balance = %q", up.LoadBalance)
	}
	if !up.VerifyTLSOrDefault() {
		t.Error("verify_upstream_tls default = false, want true")
	}
	upD := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"connect_timeout", up.ConnectTimeout.D, 5 * time.Second},
		{"read_timeout", up.ReadTimeout.D, 30 * time.Second},
		{"write_timeout", up.WriteTimeout.D, 30 * time.Second},
		{"keepalive", up.Keepalive.D, 30 * time.Second},
	}
	for _, d := range upD {
		if d.got != d.want {
			t.Errorf("upstream.%s = %v, want %v", d.name, d.got, d.want)
		}
	}
	if up.MaxIdlePerHost != 32 {
		t.Errorf("max_idle_conns_per_host = %d", up.MaxIdlePerHost)
	}
	hc := up.HealthCheck
	if hc.Active.Interval.D != 10*time.Second {
		t.Errorf("health_check.active.interval = %v", hc.Active.Interval.D)
	}
	if hc.Active.Timeout.D != 2*time.Second {
		t.Errorf("health_check.active.timeout = %v", hc.Active.Timeout.D)
	}
	if hc.Active.Method != "GET" {
		t.Errorf("health_check.active.method = %q", hc.Active.Method)
	}
	if hc.Active.HealthyThreshold != 2 || hc.Active.UnhealthyThreshold != 3 {
		t.Errorf("thresholds = %d/%d, want 2/3", hc.Active.HealthyThreshold, hc.Active.UnhealthyThreshold)
	}
	if hc.Passive.Window.D != 30*time.Second || hc.Passive.Failures != 5 ||
		hc.Passive.EjectionTime.D != 30*time.Second {
		t.Errorf("passive health defaults = %+v", hc.Passive)
	}
	cb := up.CircuitBreaker
	if cb.Failures != 10 || cb.Window.D != 60*time.Second ||
		cb.Cooldown.D != 30*time.Second || cb.HalfOpenProbes != 3 {
		t.Errorf("circuit breaker defaults = %+v", cb)
	}

	// Route defaults.
	r := cfg.Routes[0]
	if r.Timeout.D != 60*time.Second {
		t.Errorf("route timeout = %v", r.Timeout.D)
	}
	if r.BodyLimit.Bytes != 32*1024*1024 || r.BodyLimit.Unlimited {
		t.Errorf("body_limit = %+v, want 32MiB", r.BodyLimit)
	}
	if got := r.BodyLimit.MaxBytes(); got != 32*1024*1024 {
		t.Errorf("MaxBytes = %d", got)
	}

	// Rate-limit defaults.
	rl := r.RateLimits[0]
	if rl.MaxKeys != limiter.DefaultMaxKeys {
		t.Errorf("max_keys = %d, want %d", rl.MaxKeys, limiter.DefaultMaxKeys)
	}
	if rl.Backend != "memory" {
		t.Errorf("backend = %q", rl.Backend)
	}
	if rl.Cells != 10 {
		t.Errorf("cells = %d, want 10", rl.Cells)
	}
}

func mustParse(t *testing.T, docStr string) *Config {
	t.Helper()
	cfg, verr := Parse([]byte(docStr), "test.yaml")
	if verr != nil {
		t.Fatalf("unexpected validation errors:\n%s", verr.Error())
	}
	return cfg
}

// ---------------------------------------------------------------------------
// Scalar roundtrips: Duration, Size, BodyLimit.
// ---------------------------------------------------------------------------

func TestDurationRoundtrip(t *testing.T) {
	var ok struct {
		D Duration `yaml:"d"`
	}
	if err := yaml.Unmarshal([]byte("d: 1500ms\n"), &ok); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if ok.D.D != 1500*time.Millisecond || !ok.D.IsSet() {
		t.Errorf("1500ms decoded to %v (set=%v)", ok.D.D, ok.D.IsSet())
	}

	var compound struct {
		D Duration `yaml:"d"`
	}
	if err := yaml.Unmarshal([]byte("d: 2h45m\n"), &compound); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if compound.D.D != 2*time.Hour+45*time.Minute {
		t.Errorf("2h45m decoded to %v", compound.D.D)
	}

	var bad struct {
		D Duration `yaml:"d"`
	}
	if err := yaml.Unmarshal([]byte("d: 30\n"), &bad); err != nil {
		t.Fatalf("unmarshal must record the problem, not fail: %v", err)
	}
	msg, has := bad.D.ParseError()
	if !has {
		t.Fatal("bare integer must produce a recorded parse error")
	}
	if !strings.Contains(msg, "unit suffix") {
		t.Errorf("parse error = %q", msg)
	}
	line, col := bad.D.Pos()
	if line != 1 || col != 4 {
		t.Errorf("Pos = (%d,%d), want (1,4)", line, col)
	}
}

func TestSizeRoundtrip(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"512B", 512},
		{"16KiB", 16384},
		{"16KIB", 16384}, // unit match is case-insensitive after uppercase
		{"2MiB", 2097152},
		{"1GiB", 1073741824},
		{"3MB", 3000000}, // decimal units are distinct from binary ones
		{"1.5KiB", 1536},
	}
	for _, c := range cases {
		got, err := ParseSize(c.in)
		if err != nil {
			t.Errorf("ParseSize(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseSize(%q) = %d, want %d", c.in, got, c.want)
		}
	}
	for _, bad := range []string{"16", "KiB", "16ZiB", "-5MiB"} {
		if _, err := ParseSize(bad); err == nil {
			t.Errorf("ParseSize(%q) succeeded, want error", bad)
		}
	}

	var s struct {
		S Size `yaml:"s"`
	}
	if err := yaml.Unmarshal([]byte("s: 4MiB\n"), &s); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if s.S.N != 4194304 || !s.S.IsSet() {
		t.Errorf("4MiB decoded to %d", s.S.N)
	}
}

func TestBodyLimitRoundtrip(t *testing.T) {
	var unlimited struct {
		B BodyLimit `yaml:"b"`
	}
	if err := yaml.Unmarshal([]byte("b: unlimited\n"), &unlimited); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !unlimited.B.Unlimited || unlimited.B.Bytes != -1 || unlimited.B.MaxBytes() != -1 {
		t.Errorf("unlimited decoded to %+v", unlimited.B)
	}

	var capped struct {
		B BodyLimit `yaml:"b"`
	}
	if err := yaml.Unmarshal([]byte("b: 64KiB\n"), &capped); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if capped.B.Bytes != 65536 || capped.B.Unlimited || capped.B.MaxBytes() != 65536 {
		t.Errorf("64KiB decoded to %+v", capped.B)
	}

	var upper struct {
		B BodyLimit `yaml:"b"`
	}
	if err := yaml.Unmarshal([]byte("b: UNLIMITED\n"), &upper); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !upper.B.Unlimited {
		t.Errorf("UNLIMITED must match case-insensitively: %+v", upper.B)
	}

	var bad struct {
		B BodyLimit `yaml:"b"`
	}
	if err := yaml.Unmarshal([]byte("b: 64\n"), &bad); err != nil {
		t.Fatalf("unmarshal must record the problem, not fail: %v", err)
	}
	if _, has := bad.B.ParseError(); !has {
		t.Error("bare integer body_limit must produce a recorded parse error")
	}
}

// ---------------------------------------------------------------------------
// Key specification canonical forms.
// ---------------------------------------------------------------------------

func TestParseKeySpecCanonicalForms(t *testing.T) {
	cases := []struct {
		in       string
		kind     string
		header   string
		canonical string // KeySpec.String()
		parts    int
	}{
		{"ip", "ip", "", "ip", 1},
		{" IP ", "ip", "", "ip", 1},
		{"PATH", "path", "", "path", 1},
		{"Api_Key", "api_key", "", "api_key", 1},
		{"header:x-api-key", "header", "X-Api-Key", "header:X-Api-Key", 1},
		{"HEADER:X_API_KEY", "header", "X_api_key", "header:X_api_key", 1},
		{"composite[ip, api_key]", "ip", "", "composite[ip,api_key]", 2},
		{"composite[path,path,api_key,header:x-trace-id]", "path", "", "composite[path,path,api_key,header:X-Trace-Id]", 4},
	}
	for _, c := range cases {
		spec, err := ParseKeySpec(c.in)
		if err != nil {
			t.Errorf("ParseKeySpec(%q): %v", c.in, err)
			continue
		}
		if len(spec.Parts) != c.parts {
			t.Errorf("ParseKeySpec(%q) parts = %d, want %d", c.in, len(spec.Parts), c.parts)
			continue
		}
		first := spec.Parts[0]
		if first.Kind != c.kind {
			t.Errorf("ParseKeySpec(%q) kind = %q, want %q", c.in, first.Kind, c.kind)
		}
		if c.header != "" && first.Header != c.header {
			t.Errorf("ParseKeySpec(%q) header = %q, want %q", c.in, first.Header, c.header)
		}
		if spec.String() != c.canonical {
			t.Errorf("ParseKeySpec(%q).String() = %q, want %q", c.in, spec.String(), c.canonical)
		}
	}

	composite, err := ParseKeySpec("composite[ip, api_key]")
	if err != nil {
		t.Fatalf("composite parse: %v", err)
	}
	if !composite.IsComposite() {
		t.Error("two-part spec must report IsComposite")
	}
	single, err := ParseKeySpec("ip")
	if err != nil || single.IsComposite() {
		t.Errorf("single spec IsComposite = %v (err=%v)", single.IsComposite(), err)
	}

	for _, bad := range []string{
		"",
		"   ",
		"bogus",
		"composite[one]",
		"composite[ip]",
		"composite[a,b,c,d,e]",
		"composite[ip,bogus]",
		"header:Bad Name",
		"header:",
	} {
		if spec, err := ParseKeySpec(bad); err == nil {
			t.Errorf("ParseKeySpec(%q) = %+v, want error", bad, spec)
		}
	}
}
