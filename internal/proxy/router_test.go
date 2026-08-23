package proxy

import (
	"net/http"
	"net/url"
	"testing"

	"gatewright/internal/config"
	"gatewright/internal/errs"
)

// req builds a server-shaped request whose URL keeps the raw (uncleaned)
// path exactly as a net/http server would deliver it.
func req(t *testing.T, method, host, rawPath string, headers map[string]string) *http.Request {
	t.Helper()
	u, err := url.ParseRequestURI(rawPath)
	if err != nil {
		t.Fatalf("bad test path %q: %v", rawPath, err)
	}
	r := &http.Request{
		Method: method,
		URL:    u,
		Host:   host,
		Header: make(http.Header),
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	return r
}

func mustRouter(t *testing.T, routes ...config.Route) *Router {
	t.Helper()
	rt, err := NewRouter(routes)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return rt
}

func matchName(t *testing.T, rt *Router, r *http.Request) string {
	t.Helper()
	rm, apiErr := rt.Match(r)
	if apiErr != nil {
		t.Fatalf("Match(%s %s host=%q): unexpected error %v", r.Method, r.URL.Path, r.Host, apiErr)
	}
	return rm.Route.Name
}

func TestRouterPrecedenceTable(t *testing.T) {
	routes := []config.Route{
		{Name: "catchall", Upstreams: "u"},                                                              // 0
		{Name: "wildcard-host", Hosts: []string{"*.example.com"}, PathPrefix: "/p"},                     // 1
		{Name: "exact-host", Hosts: []string{"api.example.com"}, PathPrefix: "/p"},                      // 2
		{Name: "short-prefix", PathPrefix: "/api"},                                                      // 3
		{Name: "param", PathPattern: "/api/{x}"},                                                        // 4
		{Name: "deep-pattern", PathPattern: "/api/users/{id}"},                                          // 5
		{Name: "any-method", PathPrefix: "/m"},                                                          // 6
		{Name: "delete-only", PathPrefix: "/m", Methods: []string{"DELETE"}},                            // 7
		{Name: "hdr-free", PathPrefix: "/h"},                                                            // 8
		{Name: "hdr-one", PathPrefix: "/h", MatchHeaders: []config.HeaderPredicate{{Name: "X-Tenant"}}}, // 9
		{Name: "dup-a", PathPrefix: "/tie"},                                                             // 10
		{Name: "dup-b", PathPrefix: "/tie"},                                                             // 11
	}
	rt := mustRouter(t, routes...)

	cases := []struct {
		name   string
		method string
		host   string
		path   string
		hdrs   map[string]string
		want   string
	}{
		{"exact host beats wildcard beats none", "GET", "api.example.com", "/p/x", nil, "exact-host"},
		{"host match is case-insensitive with port", "GET", "API.Example.COM:8443", "/p/x", nil, "exact-host"},
		{"wildcard beats no host predicate", "GET", "a.example.com", "/p/x", nil, "wildcard-host"},
		{"wildcard needs at least one label", "GET", "example.com", "/p/x", nil, "catchall"},
		{"no host predicate fallback", "GET", "other.test", "/p/x", nil, "catchall"},
		{"longer pattern wins over param and prefix", "GET", "", "/api/users/42", nil, "deep-pattern"},
		{"param wins over short prefix", "GET", "", "/api/orders", nil, "param"},
		{"short prefix alone", "GET", "", "/api", nil, "short-prefix"},
		{"method-restricted preferred on matching method", "DELETE", "", "/m/x", nil, "delete-only"},
		{"unrestricted preferred on other method", "GET", "", "/m/x", nil, "any-method"},
		{"more header predicates win", "GET", "", "/h/x", map[string]string{"X-Tenant": "acme"}, "hdr-one"},
		{"absent header predicate falls back", "GET", "", "/h/x", nil, "hdr-free"},
		{"config order breaks ties", "GET", "", "/tie/z", nil, "dup-a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := matchName(t, rt, req(t, tc.method, tc.host, tc.path, tc.hdrs))
			if got != tc.want {
				t.Errorf("winner = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRouterHeaderRegexPredicate(t *testing.T) {
	rt := mustRouter(t,
		config.Route{Name: "plain", PathPrefix: "/x"},
		config.Route{Name: "regex", PathPrefix: "/x",
			MatchHeaders: []config.HeaderPredicate{{Name: "X-ID", Value: "^42"}}},
	)
	cases := []struct {
		val  string
		want string
	}{
		{"4242", "regex"},
		{"42", "regex"},
		{"9942", "plain"},
		{"", "plain"}, // absent header fails the regex presence+match check
	}
	for _, tc := range cases {
		hdrs := map[string]string{}
		if tc.val != "" {
			hdrs["X-ID"] = tc.val
		}
		if got := matchName(t, rt, req(t, "GET", "", "/x/y", hdrs)); got != tc.want {
			t.Errorf("X-ID=%q: winner = %q, want %q", tc.val, got, tc.want)
		}
	}
}

func TestRouterMethodNotAllowedAndAllowMethods(t *testing.T) {
	rt := mustRouter(t,
		config.Route{Name: "g", PathPrefix: "/g", Methods: []string{"GET", "HEAD"}},
		config.Route{Name: "x", PathPrefix: "/x"},
	)
	r := req(t, "POST", "", "/g/a", nil)
	rm, apiErr := rt.Match(r)
	if apiErr == nil || apiErr.Code != errs.CodeMethodNotAllowed {
		t.Fatalf("want RT002, got %v", apiErr)
	}
	if rm == nil || rm.Route.Name != "g" {
		t.Fatalf("best host/path route = %+v, want route g", rm)
	}
	if got := rm.AllowMethods(); got != "GET, HEAD" {
		t.Errorf("AllowMethods() = %q, want %q", got, "GET, HEAD")
	}
	if _, apiErr := rt.Match(req(t, "HEAD", "", "/g/a", nil)); apiErr != nil {
		t.Errorf("HEAD should be allowed, got %v", apiErr)
	}
	if _, apiErr := rt.Match(req(t, "POST", "", "/x", nil)); apiErr != nil {
		t.Errorf("unrestricted route should accept POST, got %v", apiErr)
	}
}

func TestRouterNoRoute(t *testing.T) {
	rt := mustRouter(t, config.Route{Name: "only", PathPrefix: "/only"})
	rm, apiErr := rt.Match(req(t, "GET", "", "/nothing/here", nil))
	if apiErr == nil || apiErr.Code != errs.CodeNoRoute {
		t.Fatalf("want RT001, got rm=%v err=%v", rm, apiErr)
	}
	if rm != nil {
		t.Errorf("RouteMatch must be nil on RT001, got %+v", rm)
	}
	if errs.HTTPStatus(apiErr.Code) != http.StatusNotFound {
		t.Errorf("RT001 status = %d, want 404", errs.HTTPStatus(apiErr.Code))
	}
}

func TestRouterPrefixSegmentAlignment(t *testing.T) {
	rt := mustRouter(t, config.Route{Name: "v1", PathPrefix: "/v1"})
	cases := []struct {
		path  string
		match bool
	}{
		{"/v1", true},
		{"/v1/", true}, // cleaned to /v1
		{"/v1/a/b", true},
		{"/v1x", false}, // segment alignment: never prefix-of-longer-token
		{"/v10/deep", false},
		{"/v", false},
	}
	for _, tc := range cases {
		_, apiErr := rt.Match(req(t, "GET", "", tc.path, nil))
		if got := apiErr == nil; got != tc.match {
			t.Errorf("prefix /v1 vs %q: matched=%v want %v (err=%v)", tc.path, got, tc.match, apiErr)
		}
	}
	rt2 := mustRouter(t, config.Route{Name: "v1", PathPrefix: "/v1/"}) // trailing slash normalized
	for _, p := range []string{"/v1", "/v1/a"} {
		if _, apiErr := rt2.Match(req(t, "GET", "", p, nil)); apiErr != nil {
			t.Errorf("normalized prefix should still match %q: %v", p, apiErr)
		}
	}
}

func TestRouterPathTraversalCleaning(t *testing.T) {
	rt := mustRouter(t,
		config.Route{Name: "v1", PathPrefix: "/v1"},
		config.Route{Name: "etc", PathPrefix: "/etc"},
		config.Route{Name: "double", PathPrefix: "/double"},
	)
	cases := []struct {
		rawPath string
		want    string // route name, "" => expect RT001, or an error code
	}{
		{"/v1/../etc/passwd", "etc"}, // traversal resolves before matching
		{"/etc/../v1/x", "v1"},
		{"//double//slash", "double"}, // collapsed
		{"/double/slash/../slash/./x", "double"},
		{"/v1/%2e%2e/etc", errs.CodeInvalidPath}, // decoded dots are a traversal: reject
		{"/v1/%2E%2e/admin", errs.CodeInvalidPath},
		{"/v1/x/%2e.", errs.CodeInvalidPath},
		{"/v1/a%2Fb", "v1"}, // encoded separator never splits segments nor triggers rejection
		{"/etc", "etc"},
		{"/nope", ""},
	}
	for _, tc := range cases {
		rm, apiErr := rt.Match(req(t, "GET", "", tc.rawPath, nil))
		switch {
		case tc.want == "":
			if apiErr == nil || apiErr.Code != errs.CodeNoRoute {
				t.Errorf("%q: want RT001, got rm=%v err=%v", tc.rawPath, rm, apiErr)
			}
		case tc.want == errs.CodeInvalidPath:
			if apiErr == nil || apiErr.Code != errs.CodeInvalidPath {
				t.Errorf("%q: want RT003, got rm=%v err=%v", tc.rawPath, rm, apiErr)
			} else if status := errs.HTTPStatus(apiErr.Code); status != http.StatusBadRequest {
				t.Errorf("%q: RT003 HTTP status = %d, want 400", tc.rawPath, status)
			}
		default:
			if apiErr != nil {
				t.Errorf("%q: unexpected %v", tc.rawPath, apiErr)
				continue
			}
			if rm.Route.Name != tc.want {
				t.Errorf("%q: matched %q, want %q", tc.rawPath, rm.Route.Name, tc.want)
			}
		}
	}
}

func TestRouterParamsExtraction(t *testing.T) {
	rt := mustRouter(t, config.Route{
		Name:        "posts",
		PathPattern: "/users/{id}/posts/{pid}",
	})
	rm, apiErr := rt.Match(req(t, "GET", "", "/users/42/posts/7", nil))
	if apiErr != nil {
		t.Fatalf("unexpected %v", apiErr)
	}
	if rm.Params["id"] != "42" || rm.Params["pid"] != "7" {
		t.Errorf("params = %v, want id=42 pid=7", rm.Params)
	}

	rm, apiErr = rt.Match(req(t, "GET", "", "/users/a%20b/posts/c%2Fd", nil))
	if apiErr != nil {
		t.Fatalf("unexpected %v for escaped params", apiErr)
	}
	if rm.Params["id"] != "a b" {
		t.Errorf("decoded param = %q, want %q", rm.Params["id"], "a b")
	}
	if rm.Params["pid"] != "c%2Fd" { // decoding would introduce "/" -> keep raw
		t.Errorf("slash-safe param = %q, want %q", rm.Params["pid"], "c%2Fd")
	}

	if _, apiErr := rt.Match(req(t, "GET", "", "/users/42/extra/posts/7", nil)); apiErr == nil {
		t.Error("extra segments must not match fixed-segment-count pattern")
	}
	if _, apiErr := rt.Match(req(t, "GET", "", "/users/42", nil)); apiErr == nil {
		t.Error("missing segments must not match")
	}

	if got := rm.MatchedPrefix(); got != "/users" {
		t.Errorf("pattern MatchedPrefix = %q, want /users", got)
	}
}

func TestRouterWildcardHosts(t *testing.T) {
	rt := mustRouter(t, config.Route{
		Name:       "wild",
		Hosts:      []string{"*.Example.com"},
		PathPrefix: "/w",
	})
	for _, host := range []string{"a.example.com", "b.a.example.com", "A.EXAMPLE.COM:80"} {
		if got := matchName(t, rt, req(t, "GET", host, "/w", nil)); got != "wild" {
			t.Errorf("wildcard vs %q: got %q", host, got)
		}
	}
	for _, host := range []string{"example.com", "notexample.com", "other.dev"} {
		if _, apiErr := rt.Match(req(t, "GET", host, "/w", nil)); apiErr == nil {
			t.Errorf("wildcard must not match %q", host)
		}
	}
}

func TestRouterCatchAllLowestPrecedence(t *testing.T) {
	rt := mustRouter(t,
		config.Route{Name: "catchall", Upstreams: "u"},
		config.Route{Name: "specific", PathPrefix: "/s"},
	)
	if got := matchName(t, rt, req(t, "GET", "", "/anything/at/all", nil)); got != "catchall" {
		t.Errorf("got %q, want catchall", got)
	}
	if got := matchName(t, rt, req(t, "GET", "", "/s", nil)); got != "specific" {
		t.Errorf("got %q, want specific", got)
	}
}

func TestNewRouterRejectsInvalidConfig(t *testing.T) {
	cases := []struct {
		name   string
		routes []config.Route
	}{
		{"bad regex", []config.Route{{Name: "r", PathPrefix: "/r",
			MatchHeaders: []config.HeaderPredicate{{Name: "X-A", Value: "(["}}}}},
		{"unclosed brace", []config.Route{{Name: "r", PathPrefix: "/r", PathPattern: "/u/{id"}}},
		{"duplicate param", []config.Route{{Name: "r", PathPrefix: "/r", PathPattern: "/{id}/{id}"}}},
		{"bad param char", []config.Route{{Name: "r", PathPrefix: "/r", PathPattern: "/{i-d}"}}},
		{"stray braces in literal", []config.Route{{Name: "r", PathPrefix: "/r", PathPattern: "/pre{id}"}}},
		{"empty host entry", []config.Route{{Name: "r", PathPrefix: "/r", Hosts: []string{""}}}},
		{"mid-pattern wildcard", []config.Route{{Name: "r", PathPrefix: "/r", Hosts: []string{"a.*.com"}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewRouter(tc.routes); err == nil {
				t.Errorf("expected construction error for %s", tc.name)
			}
		})
	}
}

func TestCleanRequestPath(t *testing.T) {
	cases := map[string]string{
		"":               "/",
		"/":              "/",
		"/a/../b":        "/b",
		"//a//b//":       "/a/b",
		"/v1/%2e%2e/etc": "/v1/%2e%2e/etc", // escaped form is never decoded
		"/a/../../..":    "/",
	}
	for in, want := range cases {
		if got := cleanRequestPath(in); got != want {
			t.Errorf("cleanRequestPath(%q) = %q, want %q", in, got, want)
		}
	}
}
