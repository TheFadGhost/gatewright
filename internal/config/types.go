// Package config defines Gatewright's declarative configuration schema,
// strict loading with exact-path validation errors, defaults, and hot reload.
package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"gatewright/internal/errs"
	"gopkg.in/yaml.v3"
)

// Local aliases for the stable error codes used by configuration validation.
const (
	CodeInvalidValue      = errs.CodeInvalidValue
	CodeMissingRequired   = errs.CodeMissingRequired
	CodeDuplicateName     = errs.CodeDuplicateName
	CodeSemanticConflict  = errs.CodeSemanticConflict
	CodeUnsafeCombination = errs.CodeUnsafeCombination

	CodeUnknownField = errs.CodeUnknownField
)

// ---------------------------------------------------------------------------
// Scalar types with explicit units. Bare integers are rejected by design:
// units are never guessed (DESIGN.md §2).
// ---------------------------------------------------------------------------

type Duration struct {
	D    time.Duration
	set  bool
	err  *scalarErr
	line int
	col  int
}

type scalarErr struct {
	msg string
}

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	d.line, d.col = node.Line, node.Column
	if node.Kind != yaml.ScalarNode {
		d.err = &scalarErr{msg: fmt.Sprintf("expected duration string, found %v", node.Tag)}
		return nil
	}
	dur, err := time.ParseDuration(strings.TrimSpace(node.Value))
	if err != nil || dur < 0 {
		d.err = &scalarErr{msg: fmt.Sprintf("expected duration string with unit suffix (e.g. \"30s\", \"500ms\"), found %q", node.Value)}
		return nil
	}
	d.D, d.set = dur, true
	return nil
}

func (d *Duration) IsSet() bool { return d.set }

// Pos returns the source position captured during decode.
func (d *Duration) Pos() (line, col int) { return d.line, d.col }

func (d *Duration) ParseError() (string, bool) {
	if d.err == nil {
		return "", false
	}
	return d.err.msg, true
}

type Size struct {
	N    int64 // bytes
	set  bool
	err  *scalarErr
	line int
	col  int
}

var sizeUnits = map[string]int64{
	"B": 1, "KIB": 1024, "MIB": 1024 * 1024, "GIB": 1024 * 1024 * 1024,
	"KB": 1000, "MB": 1000 * 1000, "GB": 1000 * 1000 * 1000,
}

// ParseSize parses "512B", "16KiB", "2MiB", "1GiB".
func ParseSize(s string) (int64, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
		i++
	}
	numStr, unit := s[:i], strings.TrimSpace(s[i:])
	if numStr == "" || unit == "" {
		return 0, fmt.Errorf("expected size with unit suffix (e.g. \"16KiB\", \"2MiB\", \"512B\"), found %q", s)
	}
	mult, ok := sizeUnits[unit]
	if !ok {
		return 0, fmt.Errorf("unknown size unit %q (use B, KiB, MiB, GiB)", unit)
	}
	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil || num < 0 {
		return 0, fmt.Errorf("invalid size value %q", numStr)
	}
	return int64(num * float64(mult)), nil
}

func (s *Size) UnmarshalYAML(node *yaml.Node) error {
	s.line, s.col = node.Line, node.Column
	if node.Kind != yaml.ScalarNode {
		s.err = &scalarErr{msg: fmt.Sprintf("expected size string, found %v", node.Tag)}
		return nil
	}
	n, err := ParseSize(node.Value)
	if err != nil {
		s.err = &scalarErr{msg: err.Error()}
		return nil
	}
	s.N, s.set = n, true
	return nil
}

func (s *Size) IsSet() bool          { return s.set }
func (s *Size) Pos() (line, col int) { return s.line, s.col }
func (s *Size) ParseError() (string, bool) {
	if s.err == nil {
		return "", false
	}
	return s.err.msg, true
}

// BodyLimit is either a byte size or the literal "unlimited".
type BodyLimit struct {
	Bytes     int64 // -1 => unlimited
	Unlimited bool
	set       bool
	err       *scalarErr
	line      int
	col       int
}

func (b *BodyLimit) UnmarshalYAML(node *yaml.Node) error {
	b.line, b.col = node.Line, node.Column
	if node.Kind != yaml.ScalarNode {
		b.err = &scalarErr{msg: fmt.Sprintf("expected size string or \"unlimited\", found %v", node.Tag)}
		return nil
	}
	v := strings.TrimSpace(node.Value)
	if strings.EqualFold(v, "unlimited") {
		b.Bytes, b.Unlimited, b.set = -1, true, true
		return nil
	}
	n, err := ParseSize(v)
	if err != nil {
		b.err = &scalarErr{msg: err.Error()}
		return nil
	}
	b.Bytes, b.set = n, true
	return nil
}

func (b *BodyLimit) IsSet() bool          { return b.set }
func (b *BodyLimit) Pos() (line, col int) { return b.line, b.col }
func (b *BodyLimit) ParseError() (string, bool) {
	if b.err == nil {
		return "", false
	}
	return b.err.msg, true
}

func (b *BodyLimit) MaxBytes() int64 {
	if b.Unlimited {
		return -1
	}
	return b.Bytes
}

// Issue returns a located validation error if this scalar failed to parse.
func (d *Duration) Issue(path string) *Error {
	if d.err == nil {
		return nil
	}
	return &Error{Line: d.line, Column: d.col, Path: path,
		Expected: "duration string with unit suffix (e.g. \"30s\", \"500ms\")",
		Found:    d.err.msg, Code: CodeInvalidValue,
		Hint: "bare integers are rejected so units are never guessed"}
}

// Issue reports size parse problems with position.
func (s *Size) Issue(path string) *Error {
	if s.err == nil {
		return nil
	}
	return &Error{Line: s.line, Column: s.col, Path: path,
		Expected: "size string with unit suffix (e.g. \"16KiB\", \"512B\")",
		Found:    s.err.msg, Code: CodeInvalidValue,
		Hint: "bare integers are rejected so units are never guessed"}
}

// Issue reports body-limit parse problems with position.
func (b *BodyLimit) Issue(path string) *Error {
	if b.err == nil {
		return nil
	}
	return &Error{Line: b.line, Column: b.col, Path: path,
		Expected: "size string with unit suffix or \"unlimited\"",
		Found:    b.err.msg, Code: CodeInvalidValue}
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

type Config struct {
	Version       int                 `yaml:"version"`
	Server        Server              `yaml:"server"`
	Admin         Admin               `yaml:"admin"`
	Observability Observability       `yaml:"observability"`
	Store         Store               `yaml:"store"`
	Upstreams     map[string]Upstream `yaml:"upstreams"`
	Routes        []Route             `yaml:"routes"`

	SourceFile string `yaml:"-"`
}

type Store struct {
	Path string `yaml:"path"` // bbolt database file; required for shared limiter backends
}

type Server struct {
	Listen         string     `yaml:"listen"`
	ReadTimeout    Duration   `yaml:"read_timeout"`
	WriteTimeout   Duration   `yaml:"write_timeout"`
	IdleTimeout    Duration   `yaml:"idle_timeout"`
	MaxHeaderBytes Size       `yaml:"max_header_bytes"`
	TLS            *ServerTLS `yaml:"tls"`
}

type ServerTLS struct {
	CertFile   string `yaml:"cert_file"`
	KeyFile    string `yaml:"key_file"`
	MinVersion string `yaml:"min_version"`
}

type Admin struct {
	Listen    string    `yaml:"listen"`
	Auth      AdminAuth `yaml:"auth"`
	Dashboard *bool     `yaml:"dashboard"`
}

type AdminAuth struct {
	TokenEnv  string `yaml:"token_env"`
	TokenFile string `yaml:"token_file"`
}

type Observability struct {
	AccessLog AccessLog `yaml:"access_log"`
	Metrics   Metrics   `yaml:"metrics"`
	Trace     bool      `yaml:"trace"`
}

type AccessLog struct {
	Enabled *bool    `yaml:"enabled"` // default true
	Format  string   `yaml:"format"`  // json | human
	Output  string   `yaml:"output"`  // stdout | stderr | file path
	Fields  []string `yaml:"fields"`
}

func (a AccessLog) EnabledOrDefault() bool { return a.Enabled == nil || *a.Enabled }

type Metrics struct {
	Enabled *bool  `yaml:"enabled"` // default true
	Path    string `yaml:"path"`
}

func (m Metrics) EnabledOrDefault() bool { return m.Enabled == nil || *m.Enabled }

type Upstream struct {
	Targets           []Target       `yaml:"targets"`
	LoadBalance       string         `yaml:"load_balance"`
	HashKey           string         `yaml:"hash_key"`
	ConnectTimeout    Duration       `yaml:"connect_timeout"`
	ReadTimeout       Duration       `yaml:"read_timeout"`
	WriteTimeout      Duration       `yaml:"write_timeout"`
	Keepalive         Duration       `yaml:"keepalive"`
	MaxIdlePerHost    int            `yaml:"max_idle_conns_per_host"`
	VerifyUpstreamTLS *bool          `yaml:"verify_upstream_tls"` // default true
	HealthCheck       HealthCheck    `yaml:"health_check"`
	CircuitBreaker    CircuitBreaker `yaml:"circuit_breaker"`
}

func (u *Upstream) VerifyTLSOrDefault() bool {
	return u.VerifyUpstreamTLS == nil || *u.VerifyUpstreamTLS
}

type Target struct {
	URL    string `yaml:"url"`
	Weight int    `yaml:"weight"`
}

type HealthCheck struct {
	Active  ActiveHealth  `yaml:"active"`
	Passive PassiveHealth `yaml:"passive"`
}

type ActiveHealth struct {
	Enabled            bool     `yaml:"enabled"`
	Interval           Duration `yaml:"interval"`
	Timeout            Duration `yaml:"timeout"`
	Path               string   `yaml:"path"`
	Method             string   `yaml:"method"`
	HealthyThreshold   int      `yaml:"healthy_threshold"`
	UnhealthyThreshold int      `yaml:"unhealthy_threshold"`
	VerifyTLS          *bool    `yaml:"verify_tls"` // default true
}

func (a ActiveHealth) VerifyTLSOrDefault() bool { return a.VerifyTLS == nil || *a.VerifyTLS }

type PassiveHealth struct {
	Window       Duration `yaml:"window"`
	Failures     int      `yaml:"failures"`
	EjectionTime Duration `yaml:"ejection_time"`
}

type CircuitBreaker struct {
	Failures       int      `yaml:"failures"`
	Window         Duration `yaml:"window"`
	Cooldown       Duration `yaml:"cooldown"`
	HalfOpenProbes int      `yaml:"half_open_probes"`
}

type Route struct {
	Name            string            `yaml:"name"`
	Hosts           []string          `yaml:"hosts"`
	PathPrefix      string            `yaml:"path_prefix"`
	PathPattern     string            `yaml:"path_pattern"`
	Methods         []string          `yaml:"methods"`
	MatchHeaders    []HeaderPredicate `yaml:"match_headers"`
	Upstreams       string            `yaml:"upstreams"`
	StripPrefix     bool              `yaml:"strip_prefix"`
	Timeout         Duration          `yaml:"timeout"`
	BodyLimit       BodyLimit         `yaml:"body_limit"`
	Mirror          *Mirror           `yaml:"mirror"`
	RequestHeaders  HeaderManip       `yaml:"request_headers"`
	ResponseHeaders HeaderManip       `yaml:"response_headers"`
	CORS            *CORS             `yaml:"cors"`
	Auth            *Auth             `yaml:"auth"`
	RateLimits      []RateLimit       `yaml:"rate_limits"`
}

type HeaderPredicate struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"` // regex; empty means presence
}

type HeaderManip struct {
	Set map[string]string `yaml:"set"`
	Add map[string]string `yaml:"add"`
	Del []string          `yaml:"del"`
}

type Mirror struct {
	Upstreams  string  `yaml:"upstreams"`
	Percentage float64 `yaml:"percentage"`
}

type CORS struct {
	AllowedOrigins   []string `yaml:"allowed_origins"`
	AllowedMethods   []string `yaml:"allowed_methods"`
	AllowedHeaders   []string `yaml:"allowed_headers"`
	ExposeHeaders    []string `yaml:"expose_headers"`
	AllowCredentials bool     `yaml:"allow_credentials"`
	MaxAge           Duration `yaml:"max_age"`
}

type Auth struct {
	Type   string      `yaml:"type"` // none | api_key | jwt
	APIKey *APIKeyAuth `yaml:"api_key"`
	JWT    *JWTAuth    `yaml:"jwt"`
}

type APIKeyAuth struct {
	Header   string `yaml:"header"`
	KeysEnv  string `yaml:"keys_env"`
	KeysFile string `yaml:"keys_file"`
}

type JWTAuth struct {
	SecretEnv  string   `yaml:"secret_env"`
	JwksURL    string   `yaml:"jwks_url"`
	Issuer     string   `yaml:"issuer"`
	Audience   string   `yaml:"audience"`
	Algorithms []string `yaml:"algorithms"`
}

type RateLimit struct {
	Name     string   `yaml:"name"`
	Strategy string   `yaml:"strategy"`
	Key      string   `yaml:"key"`
	Limit    int64    `yaml:"limit"`
	Window   Duration `yaml:"window"`
	Burst    int64    `yaml:"burst"`
	Capacity int64    `yaml:"capacity"`
	Cells    int      `yaml:"cells"`
	MaxKeys  int      `yaml:"max_keys"`
	Backend  string   `yaml:"backend"`
}

// Enum vocabularies (single source of truth for validation and docs).

var (
	FormatsAccessLog = []string{"json", "human"}
	LoadBalancers    = []string{"round_robin", "least_connections", "ring_hash"}
	TLSVersions      = []string{"tls12", "tls13"} // TLS < 1.2 unsupported pre-1.0
	JWTAlgorithms    = []string{"HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "ES256", "ES384", "ES512"}
	AuthTypes        = []string{"none", "api_key", "jwt"}
	LimiterBackends  = []string{"memory", "shared"}
)

func enumList(vals []string) string { return strings.Join(vals, ", ") }

// ---------------------------------------------------------------------------
// Validation error
// ---------------------------------------------------------------------------

// Error is a single configuration problem, located precisely.
type Error struct {
	File     string `json:"file"`
	Line     int    `json:"line,omitempty"`
	Column   int    `json:"column,omitempty"`
	Path     string `json:"path"`
	Found    string `json:"found,omitempty"`
	Expected string `json:"expected"`
	Code     string `json:"code"`
	Hint     string `json:"hint,omitempty"`
}

func (e *Error) Error() string {
	pos := ""
	if e.Line > 0 {
		pos = fmt.Sprintf(" (%s:%d:%d)", e.File, e.Line, e.Column)
	}
	msg := fmt.Sprintf("error[%s]: %s: expected %s", e.Code, e.Path, e.Expected)
	if e.Found != "" {
		msg += fmt.Sprintf(", found %s", e.Found)
	}
	return msg + pos
}

// ValidationError wraps every problem found while loading one config.
type ValidationError struct {
	File   string
	Errors []*Error
}

func (v *ValidationError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d configuration error(s)\n", v.File, len(v.Errors))
	for _, e := range v.Errors {
		b.WriteString("  ")
		b.WriteString(e.Error())
		b.WriteString("\n")
		if e.Line > 0 {
			col := e.Column
			if col == 0 {
				col = 1
			}
			fmt.Fprintf(&b, "    --> %s:%d:%d\n", e.File, e.Line, col)
		}
		if e.Hint != "" {
			fmt.Fprintf(&b, "    hint: %s\n", e.Hint)
		}
	}
	return b.String()
}
