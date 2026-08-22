package config

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gatewright/internal/limiter"
)

// Access-log field vocabulary (DESIGN.md §4 — names are fixed).
var KnownLogFields = []string{
	"ts", "req_id", "method", "path", "query", "route", "upstream",
	"upstream_addr", "status", "bytes_in", "bytes_out", "duration_ms",
	"remote", "code", "limiter_name", "limiter_outcome",
}

// normalizeAndValidate applies every documented default, then runs semantic
// validation. Defaults are applied BEFORE validation so validation sees the
// effective configuration.
func (c *Config) normalizeAndValidate() *ValidationError {
	var errs []*Error
	add := func(e *Error) { errs = append(errs, e) }
	fail := func(path, found, expected, hint string) {
		add(&Error{Path: path, Found: found, Expected: expected, Code: CodeInvalidValue, Hint: hint})
	}
	require := func(cond bool, path string) bool {
		if !cond {
			add(&Error{Path: path, Expected: "a value", Code: CodeMissingRequired})
			return false
		}
		return true
	}

	c.applyDefaults()

	// --- version ---
	if c.Version != 1 {
		add(&Error{Path: "version", Found: strconv.Itoa(c.Version), Expected: "1",
			Code: CodeInvalidValue, Hint: "this build understands config version 1 only"})
	}

	// --- server ---
	if errStr := validListen(c.Server.Listen); errStr != "" {
		fail("server.listen", strconv.Quote(c.Server.Listen), "host:port for a TCP listener", errStr)
	}
	if e := c.Server.ReadTimeout.Issue("server.read_timeout"); e != nil {
		add(e)
	}
	if e := c.Server.WriteTimeout.Issue("server.write_timeout"); e != nil {
		add(e)
	}
	if e := c.Server.IdleTimeout.Issue("server.idle_timeout"); e != nil {
		add(e)
	}
	if e := c.Server.MaxHeaderBytes.Issue("server.max_header_bytes"); e != nil {
		add(e)
	}
	tlsEnabled := c.Server.TLS != nil && (c.Server.TLS.CertFile != "" || c.Server.TLS.KeyFile != "")
	if tlsEnabled {
		if c.Server.TLS.CertFile == "" || c.Server.TLS.KeyFile == "" {
			add(&Error{Path: "server.tls", Expected: "cert_file and key_file when TLS is configured",
				Code: CodeSemanticConflict})
		}
		if !contains(TLSVersions, c.Server.TLS.MinVersion) {
			fail("server.tls.min_version", strconv.Quote(c.Server.TLS.MinVersion),
				"one of: "+enumList(TLSVersions), "")
		}
		if c.Server.TLS.MinVersion == "tls10" || c.Server.TLS.MinVersion == "tls11" {
			add(&Error{Path: "server.tls.min_version", Found: c.Server.TLS.MinVersion,
				Expected: "tls12 or tls13", Code: CodeUnsafeCombination,
				Hint: "TLS < 1.2 is deprecated; startup will log a loud warning"})
		}
	}

	// --- admin ---
	if errStr := validListen(c.Admin.Listen); errStr != "" {
		fail("admin.listen", strconv.Quote(c.Admin.Listen), "host:port for a TCP listener", errStr)
	}
	if !loopbackListen(c.Admin.Listen) &&
		c.Admin.Auth.TokenEnv == "" && c.Admin.Auth.TokenFile == "" {
		add(&Error{Path: "admin.auth", Found: "no token source with non-loopback listen",
			Expected: "token_env or token_file whenever admin listens beyond loopback",
			Code:     CodeUnsafeCombination,
			Hint:     "set admin.auth.token_env to an environment variable holding a bearer token"})
	}

	// --- observability ---
	if !contains(FormatsAccessLog, c.Observability.AccessLog.Format) {
		fail("observability.access_log.format", strconv.Quote(c.Observability.AccessLog.Format),
			"one of: "+enumList(FormatsAccessLog), "")
	}
	switch c.Observability.AccessLog.Output {
	case "", "stdout", "stderr":
	default:
		// A file path; nothing further to validate at parse time.
	}
	knownFields := map[string]bool{}
	for _, f := range KnownLogFields {
		knownFields[f] = true
	}
	for _, f := range c.Observability.AccessLog.Fields {
		if !knownFields[f] {
			fail("observability.access_log.fields", strconv.Quote(f),
				"a known field: "+strings.Join(KnownLogFields, ", "), "")
		}
	}
	if c.Observability.Metrics.EnabledOrDefault() && !strings.HasPrefix(c.Observability.Metrics.Path, "/") {
		fail("observability.metrics.path", strconv.Quote(c.Observability.Metrics.Path),
			"a path starting with /", "")
	}

	// --- upstreams ---
	poolNames := make(map[string]bool, len(c.Upstreams))
	for name, up := range c.Upstreams {
		p := "upstreams." + name
		poolNames[name] = true
		require(len(up.Targets) > 0, p+".targets")
		for i, t := range up.Targets {
			tp := fmt.Sprintf("%s.targets[%d]", p, i)
			u, err := url.Parse(t.URL)
			if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
				fail(tp+".url", strconv.Quote(t.URL),
					"an absolute http:// or https:// URL", "")
			}
			if t.Weight < 0 {
				fail(tp+".weight", strconv.Itoa(t.Weight), ">= 0 (0 = disabled target)", "")
			}
		}
		if !contains(LoadBalancers, up.LoadBalance) {
			fail(p+".load_balance", strconv.Quote(up.LoadBalance),
				"one of: "+enumList(LoadBalancers), "")
		}
		if up.LoadBalance == "ring_hash" && up.HashKey == "" {
			add(&Error{Path: p + ".hash_key", Expected: "ip | path | api_key | header:<name> when load_balance is ring_hash",
				Code: CodeSemanticConflict})
		} else if up.HashKey != "" {
			if _, kerr := ParseKeySpec(up.HashKey); kerr != nil {
				fail(p+".hash_key", strconv.Quote(up.HashKey), kerr.Error(), "")
			}
		}
		for _, iss := range []struct {
			d    Duration
			path string
		}{
			{up.ConnectTimeout, p + ".connect_timeout"},
			{up.ReadTimeout, p + ".read_timeout"},
			{up.WriteTimeout, p + ".write_timeout"},
			{up.Keepalive, p + ".keepalive"},
		} {
			if e := iss.d.Issue(iss.path); e != nil {
				add(e)
			}
		}
		if up.MaxIdlePerHost < 0 {
			fail(p+".max_idle_conns_per_host", strconv.Itoa(up.MaxIdlePerHost), ">= 0", "")
		}
		hc := &up.HealthCheck
		if e := hc.Active.Interval.Issue(p + ".health_check.active.interval"); e != nil {
			add(e)
		}
		if e := hc.Active.Timeout.Issue(p + ".health_check.active.timeout"); e != nil {
			add(e)
		}
		if hc.Active.Enabled {
			require(hc.Active.Path != "", p+".health_check.active.path")
			if hc.Active.Path != "" && !strings.HasPrefix(hc.Active.Path, "/") {
				fail(p+".health_check.active.path", strconv.Quote(hc.Active.Path), "a path starting with /", "")
			}
			if hc.Active.Method != "" && !validMethod(hc.Active.Method) {
				fail(p+".health_check.active.method", strconv.Quote(hc.Active.Method), "an HTTP method token", "")
			}
		}
		if hc.Active.HealthyThreshold < 0 || hc.Active.UnhealthyThreshold < 0 {
			fail(p+".health_check.active.thresholds", "negative", ">= 1 thresholds", "")
		}
		cb := &up.CircuitBreaker
		for _, iss := range []struct {
			d    Duration
			path string
		}{
			{hc.Passive.Window, p + ".health_check.passive.window"},
			{hc.Passive.EjectionTime, p + ".health_check.passive.ejection_time"},
			{cb.Window, p + ".circuit_breaker.window"},
			{cb.Cooldown, p + ".circuit_breaker.cooldown"},
		} {
			if e := iss.d.Issue(iss.path); e != nil {
				add(e)
			}
		}
		if cb.Failures < 0 || cb.HalfOpenProbes < 0 {
			fail(p+".circuit_breaker", "negative counts", "failures >= 1 and half_open_probes >= 1", "")
		}
	}

	// --- routes ---
	routeNames := map[string]bool{}
	anySharedBackend := false
	for ri := range c.Routes {
		r := &c.Routes[ri]
		base := fmt.Sprintf("routes[%d]", ri)
		if !require(r.Name != "", base+".name") {
			continue
		}
		if routeNames[r.Name] {
			add(&Error{Path: base + ".name", Found: strconv.Quote(r.Name),
				Expected: "a unique route name", Code: CodeDuplicateName})
		}
		routeNames[r.Name] = true

		for hi, h := range r.Hosts {
			if h == "" {
				fail(fmt.Sprintf("%s.hosts[%d]", base, hi), `""`, "a hostname or *.wildcard", "")
			}
		}
		hasPathPredicate := false
		if r.PathPrefix != "" {
			hasPathPredicate = true
			if !strings.HasPrefix(r.PathPrefix, "/") {
				fail(base+".path_prefix", strconv.Quote(r.PathPrefix), "a path starting with /", "")
			}
		}
		if r.PathPattern != "" {
			hasPathPredicate = true
			if !strings.HasPrefix(r.PathPattern, "/") {
				fail(base+".path_pattern", strconv.Quote(r.PathPattern), "a path starting with /", "")
			}
			if perr := validatePattern(r.PathPattern); perr != "" {
				fail(base+".path_pattern", strconv.Quote(r.PathPattern), perr, "parameters use {name}")
			}
		}
		for mi, m := range r.Methods {
			if !validMethod(m) {
				fail(fmt.Sprintf("%s.methods[%d]", base, mi), strconv.Quote(m), "an HTTP method token", "")
			}
		}
		for xi, x := range r.MatchHeaders {
			xp := fmt.Sprintf("%s.match_headers[%d]", base, xi)
			require(x.Name != "", xp+".name")
			if _, rxErr := regexp.Compile(x.Value); x.Value != "" && rxErr != nil {
				fail(xp+".value", strconv.Quote(x.Value), "a valid regular expression", rxErr.Error())
			}
		}
		require(r.Upstreams != "", base+".upstreams")
		if r.Upstreams != "" && !poolNames[r.Upstreams] {
			add(&Error{Path: base + ".upstreams", Found: strconv.Quote(r.Upstreams),
				Expected: "a pool defined under upstreams:", Code: CodeSemanticConflict})
		}
		if r.StripPrefix && !hasPathPredicate {
			add(&Error{Path: base + ".strip_prefix", Found: "true without path_prefix/path_pattern",
				Expected: "strip_prefix only with a path predicate", Code: CodeSemanticConflict})
		}
		if e := r.Timeout.Issue(base + ".timeout"); e != nil {
			add(e)
		}

		if r.Mirror != nil {
			mp := base + ".mirror"
			require(r.Mirror.Upstreams != "", mp+".upstreams")
			if r.Mirror.Upstreams != "" && !poolNames[r.Mirror.Upstreams] {
				add(&Error{Path: mp + ".upstreams", Found: strconv.Quote(r.Mirror.Upstreams),
					Expected: "a pool defined under upstreams:", Code: CodeSemanticConflict})
			}
			if r.Mirror.Percentage <= 0 || r.Mirror.Percentage > 100 {
				fail(mp+".percentage", strconv.FormatFloat(r.Mirror.Percentage, 'f', -1, 64),
					"> 0 and <= 100", "")
			}
		}

		validateHeaderManip(&errs, base+".request_headers", &r.RequestHeaders)
		validateHeaderManip(&errs, base+".response_headers", &r.ResponseHeaders)

		if r.CORS != nil {
			cp := base + ".cors"
			if len(r.CORS.AllowedOrigins) == 0 {
				add(&Error{Path: cp + ".allowed_origins", Expected: "at least one origin or \"*\"",
					Code: CodeMissingRequired})
			}
			for oi, o := range r.CORS.AllowedOrigins {
				if oerr := validOrigin(o); oerr != "" {
					fail(fmt.Sprintf("%s.allowed_origins[%d]", cp, oi), strconv.Quote(o), oerr, "")
				}
			}
			for mi, m := range r.CORS.AllowedMethods {
				if !validMethod(m) {
					fail(fmt.Sprintf("%s.allowed_methods[%d]", cp, mi), strconv.Quote(m), "an HTTP method token", "")
				}
			}
			if e := r.CORS.MaxAge.Issue(cp + ".max_age"); e != nil {
				add(e)
			}
		}

		if r.Auth != nil {
			ap := base + ".auth"
			if !contains(AuthTypes, r.Auth.Type) {
				fail(ap+".type", strconv.Quote(r.Auth.Type), "one of: "+enumList(AuthTypes), "")
			}
			if r.Auth.Type == "api_key" {
				k := r.Auth.APIKey
				if k == nil {
					add(&Error{Path: ap + ".api_key", Expected: "api_key block when type is api_key",
						Code: CodeMissingRequired})
				} else {
					require(k.Header != "", ap + ".api_key.header")
					if (k.KeysEnv == "") == (k.KeysFile == "") {
						add(&Error{Path: ap + ".api_key",
							Expected: "exactly one of keys_env or keys_file", Code: CodeSemanticConflict})
					}
				}
			}
			if r.Auth.Type == "jwt" {
				j := r.Auth.JWT
				if j == nil {
					add(&Error{Path: ap + ".jwt", Expected: "jwt block when type is jwt",
						Code: CodeMissingRequired})
				} else {
					if len(j.Algorithms) == 0 {
						add(&Error{Path: ap + ".jwt.algorithms", Expected: "at least one algorithm: " + enumList(JWTAlgorithms),
							Code: CodeMissingRequired})
					}
					for ai, alg := range j.Algorithms {
						if !contains(JWTAlgorithms, alg) {
							fail(fmt.Sprintf("%s.jwt.algorithms[%d]", ap, ai), strconv.Quote(alg),
								"one of: "+enumList(JWTAlgorithms), "")
						}
					}
					hasHS, hasOther := false, false
					for _, alg := range j.Algorithms {
						if strings.HasPrefix(alg, "HS") {
							hasHS = true
						} else {
							hasOther = true
						}
					}
					if (j.SecretEnv == "") == (j.JwksURL == "") {
						add(&Error{Path: ap + ".jwt",
							Expected: "exactly one of secret_env (for HS*) or jwks_url (for RS*/ES*)",
							Code:     CodeSemanticConflict})
					} else if j.SecretEnv != "" && hasOther {
						add(&Error{Path: ap + ".jwt",
							Expected: "jwks_url when non-HS algorithms are listed", Code: CodeSemanticConflict})
					} else if j.JwksURL != "" && hasHS {
						add(&Error{Path: ap + ".jwt",
							Expected: "secret_env when HS* algorithms are listed", Code: CodeSemanticConflict})
					}
					if jw := j.JwksURL; jw != "" && !strings.HasPrefix(jw, "http") {
						fail(ap+".jwt.jwks_url", strconv.Quote(jw), "an http(s) URL", "")
					}
				}
			}
		}

		rlNames := map[string]bool{}
		for li := range r.RateLimits {
			rl := &r.RateLimits[li]
			lp := fmt.Sprintf("%s.rate_limits[%d]", base, li)
			require(rl.Name != "", lp+".name")
			if rl.Name != "" && rlNames[rl.Name] {
				add(&Error{Path: lp + ".name", Found: strconv.Quote(rl.Name),
					Expected: "a unique limiter name within the route", Code: CodeDuplicateName})
			}
			rlNames[rl.Name] = true

			if !limiter.Has(rl.Strategy) {
				fail(lp+".strategy", strconv.Quote(rl.Strategy),
					"one of: "+enumList(limiter.Strategies()), "see DESIGN.md §1 trade-off table")
			} else {
				s := limiter.Settings{
					Limit: rl.Limit, Window: rl.Window.D,
					Burst: rl.Burst, Capacity: rl.Capacity, Cells: rl.Cells,
				}
				for _, problem := range limiter.CheckSettings(rl.Strategy, s) {
					add(&Error{Path: lp, Found: problem, Expected: "settings valid for strategy " + rl.Strategy,
						Code: CodeInvalidValue})
				}
			}

			if ks, kerr := ParseKeySpec(rl.Key); kerr != nil {
				fail(lp+".key", strconv.Quote(rl.Key), kerr.Error(), "")
			} else if ks.IsComposite() && len(ks.Parts) > 4 {
				fail(lp+".key", strconv.Quote(rl.Key), "composite of at most 4 parts", "")
			}

			if rl.Limit <= 0 {
				add(&Error{Path: lp + ".limit", Found: strconv.FormatInt(rl.Limit, 10),
					Expected: ">= 1", Code: CodeInvalidValue})
			}
			if e := rl.Window.Issue(lp + ".window"); e != nil {
				add(e)
			}
			if rl.Burst < 0 || rl.Capacity < 0 {
				fail(lp, "negative burst/capacity", ">= 1 where used by the strategy", "")
			}
			if rl.Cells != 0 && (rl.Cells < 2 || rl.Cells > 1000) {
				fail(lp+".cells", strconv.Itoa(rl.Cells), "between 2 and 1000", "")
			}
			if rl.MaxKeys != 0 && rl.MaxKeys < 8 {
				fail(lp+".max_keys", strconv.Itoa(rl.MaxKeys), ">= 8", "")
			}
			if !contains(LimiterBackends, rl.Backend) {
				fail(lp+".backend", strconv.Quote(rl.Backend), "one of: "+enumList(LimiterBackends), "")
			}
			if rl.Backend == "shared" {
				anySharedBackend = true
			}
		}
	}

	if anySharedBackend && c.Store.Path == "" {
		add(&Error{Path: "store.path",
			Expected: "a file path for the shared store when any limiter uses backend: shared",
			Code:     CodeSemanticConflict,
			Hint:     "shared state is backed by a transactional bbolt database shared across gateway processes"})
	}

	if len(errs) == 0 {
		return nil
	}
	return &ValidationError{File: c.SourceFile, Errors: errs}
}

func (c *Config) applyDefaults() {
	setDur := func(d *Duration, v time.Duration) {
		if !d.set {
			d.D, d.set = v, true
		}
	}
	if c.Version == 0 {
		c.Version = 1
	}
	if c.Server.Listen == "" {
		c.Server.Listen = ":8080"
	}
	setDur(&c.Server.ReadTimeout, 30*time.Second)
	setDur(&c.Server.WriteTimeout, 60*time.Second)
	setDur(&c.Server.IdleTimeout, 120*time.Second)
	if !c.Server.MaxHeaderBytes.set {
		c.Server.MaxHeaderBytes.N, c.Server.MaxHeaderBytes.set = 1024*1024, true // 1MiB
	}
	if c.Server.TLS == nil {
		c.Server.TLS = &ServerTLS{}
	}
	if c.Server.TLS.MinVersion == "" {
		c.Server.TLS.MinVersion = "tls12"
	}
	if c.Admin.Listen == "" {
		c.Admin.Listen = "127.0.0.1:9901"
	}
	if c.Observability.AccessLog.Format == "" {
		c.Observability.AccessLog.Format = "json"
	}
	if c.Observability.AccessLog.Output == "" {
		c.Observability.AccessLog.Output = "stdout"
	}
	if c.Observability.Metrics.Path == "" {
		c.Observability.Metrics.Path = "/metrics"
	}
	for name := range c.Upstreams {
		up := c.Upstreams[name]
		if up.LoadBalance == "" {
			up.LoadBalance = "round_robin"
		}
		if up.VerifyUpstreamTLS == nil {
			v := true
			up.VerifyUpstreamTLS = &v
		}
		setDur(&up.ConnectTimeout, 5*time.Second)
		setDur(&up.ReadTimeout, 30*time.Second)
		setDur(&up.WriteTimeout, 30*time.Second)
		setDur(&up.Keepalive, 30*time.Second)
		if up.MaxIdlePerHost == 0 {
			up.MaxIdlePerHost = 32
		}
		if up.HealthCheck.Active.Interval.D == 0 {
			setDur(&up.HealthCheck.Active.Interval, 10*time.Second)
		}
		setDur(&up.HealthCheck.Active.Timeout, 2*time.Second)
		if up.HealthCheck.Active.Method == "" {
			up.HealthCheck.Active.Method = "GET"
		}
		if up.HealthCheck.Active.HealthyThreshold == 0 {
			up.HealthCheck.Active.HealthyThreshold = 2
		}
		if up.HealthCheck.Active.UnhealthyThreshold == 0 {
			up.HealthCheck.Active.UnhealthyThreshold = 3
		}
		setDur(&up.HealthCheck.Passive.Window, 30*time.Second)
		if up.HealthCheck.Passive.Failures == 0 {
			up.HealthCheck.Passive.Failures = 5
		}
		setDur(&up.HealthCheck.Passive.EjectionTime, 30*time.Second)
		if up.CircuitBreaker.Failures == 0 {
			up.CircuitBreaker.Failures = 10
		}
		setDur(&up.CircuitBreaker.Window, 60*time.Second)
		setDur(&up.CircuitBreaker.Cooldown, 30*time.Second)
		if up.CircuitBreaker.HalfOpenProbes == 0 {
			up.CircuitBreaker.HalfOpenProbes = 3
		}
		c.Upstreams[name] = up
	}
	for ri := range c.Routes {
		r := &c.Routes[ri]
		setDur(&r.Timeout, 60*time.Second)
		if !r.BodyLimit.set {
			r.BodyLimit.Bytes, r.BodyLimit.set = 32*1024*1024, true // 32MiB
		}
		for li := range r.RateLimits {
			rl := &r.RateLimits[li]
			if rl.MaxKeys == 0 {
				rl.MaxKeys = limiter.DefaultMaxKeys
			}
			if rl.Backend == "" {
				rl.Backend = "memory"
			}
			if rl.Cells == 0 {
				rl.Cells = 10
			}
		}
	}
}

func validateHeaderManip(errs *[]*Error, base string, m *HeaderManip) {
	for k := range m.Set {
		if !validHeaderName(k) {
			failOn(errs, base+".set", strconv.Quote(k), "a valid header field-name")
		}
	}
	for k := range m.Add {
		if !validHeaderName(k) {
			failOn(errs, base+".add", strconv.Quote(k), "a valid header field-name")
		}
	}
	for i, k := range m.Del {
		if !validHeaderName(k) {
			failOn(errs, fmt.Sprintf("%s.del[%d]", base, i), strconv.Quote(k), "a valid header field-name")
		}
	}
}

func failOn(errs *[]*Error, path, found, expected string) {
	*errs = append(*errs, &Error{Path: path, Found: found, Expected: expected, Code: CodeInvalidValue})
}

// ---------------------------------------------------------------------------
// Key specification parsing: ip | path | api_key | header:<NAME> |
// composite[a,b,...] — max 4 parts.
// ---------------------------------------------------------------------------

type KeyPart struct {
	Kind   string // ip | path | api_key | header
	Header string
}

type KeySpec struct {
	Parts []KeyPart
}

func (k *KeySpec) IsComposite() bool { return len(k.Parts) > 1 }

// String renders the canonical form of the key spec.
func (k *KeySpec) String() string {
	if len(k.Parts) == 1 {
		return k.Parts[0].raw()
	}
	parts := make([]string, len(k.Parts))
	for i, p := range k.Parts {
		parts[i] = p.raw()
	}
	return "composite[" + strings.Join(parts, ",") + "]"
}

func (p KeyPart) raw() string {
	switch p.Kind {
	case "header":
		return "header:" + p.Header
	default:
		return p.Kind
	}
}

// ParseKeySpec parses the documented key syntax.
func ParseKeySpec(s string) (*KeySpec, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("a key selector such as ip, path, api_key, header:X-API-Key, composite[ip,api_key]")
	}
	parseOne := func(tok string) (KeyPart, error) {
		tok = strings.TrimSpace(tok)
		lower := strings.ToLower(tok)
		switch lower {
		case "ip", "path", "api_key":
			return KeyPart{Kind: lower}, nil
		default:
			if rest, found := strings.CutPrefix(lower, "header:"); found {
				name := strings.TrimSpace(rest)
				if !validHeaderName(name) {
					return KeyPart{}, fmt.Errorf("invalid header name %q in key selector", name)
				}
				return KeyPart{Kind: "header", Header: http.CanonicalHeaderKey(name)}, nil
			}
			return KeyPart{}, fmt.Errorf("a key selector such as ip, path, api_key, header:X-API-Key, composite[ip,api_key]")
		}
	}
	if strings.HasPrefix(strings.ToLower(s), "composite[") && strings.HasSuffix(s, "]") {
		inner := s[len("composite[") : len(s)-1]
		rawParts := strings.Split(inner, ",")
		if len(rawParts) < 2 || len(rawParts) > 4 {
			return nil, fmt.Errorf("composite[] of 2 to 4 selectors")
		}
		spec := &KeySpec{}
		for _, rp := range rawParts {
			p, err := parseOne(rp)
			if err != nil {
				return nil, err
			}
			spec.Parts = append(spec.Parts, p)
		}
		return spec, nil
	}
	p, err := parseOne(s)
	if err != nil {
		return nil, err
	}
	return &KeySpec{Parts: []KeyPart{p}}, nil
}

// ---------------------------------------------------------------------------
// Small validators
// ---------------------------------------------------------------------------

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func validListen(s string) string {
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		return "use host:port form"
	}
	if port == "" {
		return "port must not be empty"
	}
	if n, err := strconv.Atoi(port); err != nil || n < 0 || n > 65535 {
		return "port must be numeric 0-65535"
	}
	if host != "" {
		if net.ParseIP(host) == nil && !validHostname(host) {
			return "host must be an IP address or hostname"
		}
	}
	return ""
}

func loopbackListen(s string) bool {
	host, _, err := net.SplitHostPort(s)
	if err != nil {
		return false
	}
	if host == "" || host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validHostname(h string) bool {
	if len(h) == 0 || len(h) > 253 {
		return false
	}
	for _, label := range strings.Split(h, ".") {
		if label == "" || len(label) > 63 {
			return false
		}
		for _, r := range label {
			ok := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-'
			if !ok {
				return false
			}
		}
	}
	return true
}

var tokenChars = func() map[rune]bool {
	m := map[rune]bool{}
	for _, r := range "!#$%&'*+-.^_`|~0123456789" {
		m[r] = true
	}
	for r := 'a'; r <= 'z'; r++ {
		m[r] = true
	}
	for r := 'A'; r <= 'Z'; r++ {
		m[r] = true
	}
	return m
}()

func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if !tokenChars[r] {
			return false
		}
	}
	return true
}

var methodRe = regexp.MustCompile(`^[A-Z][A-Z0-9!#$%&'*+\-.^_` + "`" + `|~]*$`)

func validMethod(m string) bool { return methodRe.MatchString(m) }

func validOrigin(o string) string {
	if o == "*" {
		return ""
	}
	if strings.Contains(o, "*") {
		scheme, rest, ok := strings.Cut(o, "://")
		if !ok || scheme != "https" && scheme != "http" {
			return "wildcard origins use https://*.example.com form"
		}
		if !strings.HasPrefix(rest, "*.") {
			return "wildcard origins use https://*.example.com form"
		}
		host := strings.TrimPrefix(rest, "*.")
		if !validHostname(host) {
			return "wildcard origin host is invalid"
		}
		return ""
	}
	u, err := url.Parse(o)
	if err != nil || u.Scheme == "" || u.Host == "" || u.Path != "" || u.RawQuery != "" {
		return "an origin like https://app.example.com (scheme + host, no path)"
	}
	return ""
}

// validatePattern checks {param} syntax; returns expected-description on error.
func validatePattern(p string) string {
	seen := map[string]bool{}
	for i := 0; i < len(p); i++ {
		if p[i] != '{' {
			continue
		}
		end := strings.IndexByte(p[i:], '}')
		if end < 0 {
			return "every { must close with }"
		}
		name := p[i+1 : i+end]
		if name == "" {
			return "parameter names must be non-empty"
		}
		for _, r := range name {
			ok := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_'
			if !ok {
				return "parameter names use letters, digits, underscore"
			}
		}
		if seen[name] {
			return "unique parameter names within one pattern"
		}
		seen[name] = true
		i += end
	}
	return ""
}
