// Package proxy implements route matching and request forwarding — the
// routing/proxy stage of the documented middleware chain (DESIGN.md §6).
// Route precedence implements DESIGN.md §2 exactly; the forwarder implements
// the retry/backoff and circuit-breaker integration rules of §6.
package proxy

import (
	"fmt"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"path"
	"regexp"
	"strings"

	"gatewright/internal/config"
	"gatewright/internal/errs"
)

// ---------------------------------------------------------------------------
// Public surface
// ---------------------------------------------------------------------------

// RouteMatch is a resolved routing decision: the winning route plus any
// {param} captures extracted from the request path.
type RouteMatch struct {
	Route  *config.Route
	Params map[string]string

	prefix string // normalized path prefix matched by the route predicate
}

// AllowMethods returns the methods of the best host/path-matching route,
// comma-joined for an Allow header on RT002 responses. Empty when that route
// has no method restriction.
func (m *RouteMatch) AllowMethods() string {
	if m == nil || m.Route == nil {
		return ""
	}
	return strings.Join(m.Route.Methods, ", ")
}

// MatchedPrefix returns the path prefix consumed by this route's path
// predicate ("/v1" for a prefix "/v1", "/users" for pattern "/users/{id}");
// the Forwarder strips exactly this prefix when strip_prefix is enabled.
func (m *RouteMatch) MatchedPrefix() string {
	if m == nil {
		return ""
	}
	return m.prefix
}

// Router matches requests against configured routes. It is immutable after
// construction and safe for concurrent use.
type Router struct {
	routes []compiledRoute
}

// Scoring constants for DESIGN.md §2 precedence, kept integral by scoring on
// a x2 scale: static segments score 2 (a "{param}" segment is documented as
// 0.5 static segments => 1), exact hosts beat wildcard hosts beat no host.
const (
	hostExactScore    = 2
	hostWildcardScore = 1
	staticSegScore    = 2
	paramSegScore     = 1
	methodScore       = 1
)

type hostPred struct {
	exact   string // lowercased hostname; empty when wildcard
	wildSuf string // suffix after "*." lowercased; empty when exact
}

type patternSeg struct {
	literal string // exact segment text when param == ""
	param   string // parameter name captured from one segment
}

type headerPred struct {
	name string
	re   *regexp.Regexp // nil => presence check only
}

type compiledRoute struct {
	idx       int
	route     *config.Route
	hosts     []hostPred
	litPrefix string        // normalized path_prefix ("" when unset)
	pattern   []patternSeg  // nil when no path_pattern
	patPrefix string        // literal head of the pattern ("", or "/", ...)
	headers   []headerPred
}

// NewRouter compiles routes for matching. It rejects invalid {param}
// patterns, malformed host wildcards and uncompilable header regexes with a
// descriptive error (config validation already catches these earlier).
func NewRouter(routes []config.Route) (*Router, error) {
	rt := &Router{routes: make([]compiledRoute, len(routes))}
	for i := range routes {
		cr, err := compileRoute(i, &routes[i])
		if err != nil {
			return nil, fmt.Errorf("routes[%d] (%q): %w", i, routes[i].Name, err)
		}
		rt.routes[i] = *cr
	}
	return rt, nil
}

func compileRoute(idx int, r *config.Route) (*compiledRoute, error) {
	cr := &compiledRoute{idx: idx, route: r}
	for _, h := range r.Hosts {
		h = strings.ToLower(strings.TrimSpace(h))
		if h == "" {
			return nil, fmt.Errorf("hosts[%d]: empty host predicate", len(cr.hosts))
		}
		if strings.HasPrefix(h, "*.") {
			suf := h[len("*."):]
			if suf == "" || strings.Contains(suf, "*") {
				return nil, fmt.Errorf("hosts[%d]: wildcard must be \"*.suffix\"", len(cr.hosts))
			}
			cr.hosts = append(cr.hosts, hostPred{wildSuf: suf})
			continue
		}
		if strings.Contains(h, "*") {
			return nil, fmt.Errorf("hosts[%d]: only leading *. wildcards are supported", len(cr.hosts))
		}
		cr.hosts = append(cr.hosts, hostPred{exact: h})
	}
	if r.PathPrefix != "" {
		cr.litPrefix = normalizePrefix(r.PathPrefix)
	}
	if r.PathPattern != "" {
		segs, err := splitPattern(r.PathPattern)
		if err != nil {
			return nil, err
		}
		cr.pattern = segs
		cr.patPrefix = literalHead(segs)
	}
	for _, hp := range r.MatchHeaders {
		p := headerPred{name: textproto.CanonicalMIMEHeaderKey(hp.Name)}
		if hp.Value != "" {
			re, err := regexp.Compile(hp.Value)
			if err != nil {
				return nil, fmt.Errorf("match_headers %q: bad regex: %w", hp.Name, err)
			}
			p.re = re
		}
		cr.headers = append(cr.headers, p)
	}
	return cr, nil
}

// ---------------------------------------------------------------------------
// Matching
// ---------------------------------------------------------------------------

// Match resolves r to the highest-precedence route (DESIGN.md §2):
//
//	1. host specificity      (exact > wildcard > no host predicate)
//	2. static path segments  (x2 integral scale; {param} scores 1 of 2)
//	3. method-restricted beats unrestricted
//	4. more header predicates beat fewer
//	5. ties resolve to config order (earlier wins)
//
// No route matches at all => APIError RT001 (404). A route matches on
// host+path but its method/header predicates exclude the request => RT002
// (405); the returned RouteMatch describes that best host/path route so the
// caller can emit an Allow header via AllowMethods().
func (rt *Router) Match(r *http.Request) (*RouteMatch, *errs.APIError) {
	host := hostFromRequest(r)
	clean := cleanRequestPath(r.URL.EscapedPath())

	bestIdx := -1
	bestHost, bestPath, bestMeth, bestHdr := 0, 0, 0, 0
	var bestParams map[string]string
	bestPrefix := ""

	hpIdx := -1
	hpHost, hpPath := 0, 0
	var hpParams map[string]string
	hpPrefix := ""

	for i := range rt.routes {
		cr := &rt.routes[i]
		hostOK, hostScore := matchHosts(cr.hosts, host)
		if !hostOK {
			continue
		}
		pathOK, pathScore, params, prefix := matchPath(cr, clean)
		if !pathOK {
			continue
		}

		// Track the best host/path candidate independent of method/header
		// predicates: it supplies Route + AllowMethods() for RT002.
		if hpIdx == -1 || betterTuple(hostScore, pathScore, cr.idx, hpHost, hpPath, hpIdx) {
			hpIdx, hpHost, hpPath, hpParams, hpPrefix = cr.idx, hostScore, pathScore, params, prefix
		}

		if len(cr.route.Methods) > 0 && !methodAllowed(cr.route.Methods, r.Method) {
			continue
		}
		if !matchHeaders(cr.headers, r) {
			continue
		}

		meth := 0
		if len(cr.route.Methods) > 0 {
			meth = methodScore
		}
		if bestIdx == -1 ||
			better4(hostScore, pathScore, meth, len(cr.headers), cr.idx,
				bestHost, bestPath, bestMeth, bestHdr, bestIdx) {
			bestIdx = cr.idx
			bestHost, bestPath, bestMeth, bestHdr = hostScore, pathScore, meth, len(cr.headers)
			bestParams, bestPrefix = params, prefix
		}
	}

	if bestIdx >= 0 {
		return &RouteMatch{
			Route:  rt.routes[bestIdx].route,
			Params: bestParams,
			prefix: bestPrefix,
		}, nil
	}
	if hpIdx >= 0 {
		rm := &RouteMatch{
			Route:  rt.routes[hpIdx].route,
			Params: hpParams,
			prefix: hpPrefix,
		}
		return rm, errs.New(errs.CodeMethodNotAllowed,
			fmt.Sprintf("method %s not allowed for route %q", r.Method, rm.Route.Name))
	}
	return nil, errs.New(errs.CodeNoRoute,
		fmt.Sprintf("no route matched %s %s", r.Method, clean))
}

// betterTuple reports whether (h,p,idx) beats the incumbent under host then
// path then config-order precedence.
func betterTuple(h, p, idx, ih, ip, iidx int) bool {
	if h != ih {
		return h > ih
	}
	if p != ip {
		return p > ip
	}
	return idx < iidx
}

// better4 extends betterTuple with method-restriction and header-count
// criteria before the config-order tiebreak.
func better4(h, p, m, x, idx, ih, ip, im, ix, iidx int) bool {
	if h != ih {
		return h > ih
	}
	if p != ip {
		return p > ip
	}
	if m != im {
		return m > im
	}
	if x != ix {
		return x > ix
	}
	return idx < iidx
}

func matchHosts(preds []hostPred, host string) (bool, int) {
	if len(preds) == 0 {
		return true, 0
	}
	best := 0
	for _, p := range preds {
		switch {
		case p.exact != "" && host == p.exact:
			return true, hostExactScore
		case p.wildSuf != "" && len(host) > len(p.wildSuf)+1 &&
			strings.HasSuffix(host, "."+p.wildSuf):
			best = maxInt(best, hostWildcardScore)
		}
	}
	return best > 0, best
}

// matchPath evaluates both optional path predicates (ANDed when both are
// configured). The returned prefix is what strip_prefix should remove.
func matchPath(cr *compiledRoute, clean string) (ok bool, score int, params map[string]string, prefix string) {
	prefix = cr.litPrefix
	if cr.litPrefix != "" {
		if !prefixMatches(clean, cr.litPrefix) {
			return false, 0, nil, ""
		}
		score += staticSegScore * segCount(cr.litPrefix)
	}
	if cr.pattern != nil {
		p, pscore, pok := matchPattern(cr.pattern, clean)
		if !pok {
			return false, 0, nil, ""
		}
		params = p
		score += pscore
		if prefix == "" || prefix == "/" {
			prefix = cr.patPrefix
		}
	}
	return true, score, params, prefix
}

// prefixMatches is segment-aligned: prefix "/v1" matches "/v1" itself and
// everything under "/v1/", but never "/v1x" or "/v10/deep". A root "/"
// prefix matches every path. Prefixes are normalized (trailing slashes
// removed) before this check.
func prefixMatches(path, prefix string) bool {
	if prefix == "/" {
		return true
	}
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

// matchPattern requires the same number of segments as the pattern; literal
// segments compare exactly, {param} segments capture any single non-empty
// segment (so captured values never contain "/").
func matchPattern(pat []patternSeg, clean string) (map[string]string, int, bool) {
	segs := splitSegments(clean)
	if len(segs) != len(pat) {
		return nil, 0, false
	}
	var params map[string]string
	score := 0
	for i, ps := range pat {
		if ps.param == "" {
			if segs[i] != ps.literal {
				return nil, 0, false
			}
			score += staticSegScore
			continue
		}
		raw := segs[i]
		val, err := url.PathUnescape(raw)
		if err != nil || strings.Contains(val, "/") || val == "" {
			val = raw
		}
		if params == nil {
			params = make(map[string]string, len(pat))
		}
		params[ps.param] = val
		score += paramSegScore
	}
	return params, score, true
}

func matchHeaders(preds []headerPred, r *http.Request) bool {
	for _, p := range preds {
		vals := r.Header.Values(p.name)
		if p.re == nil {
			if len(vals) == 0 {
				return false
			}
			continue
		}
		matched := false
		for _, v := range vals {
			if p.re.MatchString(v) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func methodAllowed(methods []string, method string) bool {
	for _, m := range methods {
		if strings.EqualFold(m, method) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Normalization helpers
// ---------------------------------------------------------------------------

// cleanRequestPath canonicalizes the ESCAPED request path with path.Clean
// semantics before predicate evaluation: traversal sequences such as
// "/v1/../etc" resolve lexically to "/etc" and "//a//b" to "/a/b". Cleaning
// operates on the escaped form, so percent-encoded separators (%2F) never
// become real segment boundaries and traversal cannot be smuggled through
// encoding. Note path.Clean drops trailing slashes ("/v1/" -> "/v1").
func cleanRequestPath(escaped string) string {
	if escaped == "" {
		return "/"
	}
	if !strings.HasPrefix(escaped, "/") {
		escaped = "/" + escaped
	}
	c := path.Clean(escaped)
	if c == "." || c == "" {
		return "/"
	}
	return c
}

// normalizePrefix canonicalizes a configured path prefix: leading slash,
// cleaned, trailing slashes removed (root stays "/").
func normalizePrefix(p string) string {
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	p = path.Clean(p)
	if p != "/" {
		p = strings.TrimRight(p, "/")
		if p == "" {
			p = "/"
		}
	}
	return p
}

// literalHead joins the pattern's leading literal segments into a prefix;
// stops at the first {param}. Returns "" when the pattern starts with a
// parameter (nothing safely strippable).
func literalHead(segs []patternSeg) string {
	var b strings.Builder
	for _, s := range segs {
		if s.param != "" {
			break
		}
		b.WriteByte('/')
		b.WriteString(s.literal)
	}
	if b.Len() == 0 {
		return ""
	}
	return normalizePrefix(b.String())
}

// splitPattern validates and splits a path_pattern into segments. A segment
// is either an exact literal or a whole-segment {name} parameter; stray
// braces inside literals are rejected so mistakes surface at construction.
func splitPattern(pattern string) ([]patternSeg, error) {
	if !strings.HasPrefix(pattern, "/") {
		return nil, fmt.Errorf("path_pattern %q must start with /", pattern)
	}
	seen := map[string]bool{}
	var segs []patternSeg
	body := strings.TrimPrefix(pattern, "/")
	if body == "" {
		return segs, nil
	}
	for _, raw := range strings.Split(body, "/") {
		if raw == "" {
			continue
		}
		if name, ok := paramName(raw); ok {
			if !validParamName(name) {
				return nil, fmt.Errorf("path_pattern %q: invalid parameter name %q", pattern, name)
			}
			if seen[name] {
				return nil, fmt.Errorf("path_pattern %q: duplicate parameter {%s}", pattern, name)
			}
			seen[name] = true
			segs = append(segs, patternSeg{param: name})
			continue
		}
		if strings.ContainsAny(raw, "{}") {
			return nil, fmt.Errorf("path_pattern %q: parameters must be whole segments like {id}", pattern)
		}
		segs = append(segs, patternSeg{literal: raw})
	}
	return segs, nil
}

func paramName(seg string) (string, bool) {
	if len(seg) < 3 || seg[0] != '{' || seg[len(seg)-1] != '}' {
		return "", false
	}
	name := seg[1 : len(seg)-1]
	if strings.ContainsAny(name, "{}") {
		return "", false
	}
	return name, true
}

func validParamName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		ok := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_'
		if !ok {
			return false
		}
	}
	return true
}

// splitSegments splits a cleaned absolute path into non-empty segments.
func splitSegments(p string) []string {
	parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
	out := parts[:0]
	for _, s := range parts {
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func segCount(prefix string) int { return len(splitSegments(prefix)) }

// hostFromRequest lowercases the request Host without its port (IPv6-aware).
func hostFromRequest(r *http.Request) string {
	h := strings.TrimSpace(r.Host)
	if h == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(h); err == nil {
		return strings.ToLower(host)
	}
	if strings.HasPrefix(h, "[") && strings.HasSuffix(h, "]") {
		return strings.ToLower(h[1 : len(h)-1])
	}
	return strings.ToLower(h)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
