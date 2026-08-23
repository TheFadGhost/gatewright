package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"gatewright/internal/config"
	"gatewright/internal/errs"
	"gatewright/internal/obs"
	"gatewright/internal/pool"
)

// ---------------------------------------------------------------------------
// Options and construction
// ---------------------------------------------------------------------------

// RetryPolicy configures idempotent-method retries (DESIGN.md §6). Attempts
// includes the first try; 0/negative means no retries (one attempt).
type RetryPolicy struct {
	Attempts  int
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

// ForwarderOpts configures one Forwarder.
type ForwarderOpts struct {
	Pool            pool.Pool       // resolved pool for the route
	StripPrefix     bool            // strip the matched path predicate prefix
	Timeout         time.Duration   // total per-attempt deadline; 0 = none
	Transport       *http.Transport // built by caller from pool config
	Mirror          *config.Mirror  // optional shadow-target spec
	MirrorPool      pool.Pool       // resolved pool named by Mirror.Upstreams
	MirrorTransport *http.Transport // transport for mirror traffic
	Logger          obs.Logger      // optional, may be nil
	Retry           RetryPolicy
	TraceEnabled    bool
}

// Forwarder proxies requests to a route's pool: streaming bodies, hop-by-hop
// hygiene, X-Forwarded-*/Forwarded propagation, bounded retries for
// idempotent methods, pool outcome reporting, and optional traffic mirroring.
// It is safe for concurrent use.
type Forwarder struct {
	opts        ForwarderOpts
	rp          *httputil.ReverseProxy
	targetCache sync.Map // string -> *url.URL
	bufPool     sync.Pool
}

const (
	// maxReplayBodyBytes bounds in-memory request buffering for replayable
	// retries (DESIGN.md §6: "bodies replayable only when buffered <= 8 KiB").
	maxReplayBodyBytes = 8 * 1024

	mirrorTimeout    = 5 * time.Second  // shadow traffic never blocks the client
	mirrorDrainLimit = 1 << 20          // cap drain so mirrors can't stall goroutines
	copyBufSize      = 32 * 1024        // ReverseProxy buffer-pool chunk size
)

// NewForwarder builds a forwarder, normalizing retry defaults
// (attempts=1, base=25ms per DESIGN.md §6).
func NewForwarder(o ForwarderOpts) *Forwarder {
	if o.Retry.Attempts <= 0 {
		o.Retry.Attempts = 1
	}
	if o.Retry.BaseDelay <= 0 {
		o.Retry.BaseDelay = 25 * time.Millisecond
	}
	if o.Retry.MaxDelay < o.Retry.BaseDelay {
		o.Retry.MaxDelay = o.Retry.BaseDelay
	}
	if o.Transport == nil {
		o.Transport = http.DefaultTransport.(*http.Transport).Clone()
	}
	f := &Forwarder{opts: o}
	f.bufPool.New = func() any { return make([]byte, copyBufSize) }

	f.rp = &httputil.ReverseProxy{
		Rewrite:        f.rewrite,
		Transport:      o.Transport,
		ErrorHandler:   f.onError,
		ModifyResponse: f.onResponse,
		BufferPool:     &bufPool{p: &f.bufPool},
		FlushInterval:  0, // auto: immediate flush for streamed/chunked responses
	}
	return f
}

type bufPool struct{ p *sync.Pool }

func (b *bufPool) Get() []byte { return b.p.Get().([]byte) }
func (b *bufPool) Put(x []byte) { b.p.Put(x) }

// ---------------------------------------------------------------------------
// Request lifecycle
// ---------------------------------------------------------------------------

// ServeHTTP performs the full proxy handling for one routed request.
func (f *Forwarder) ServeHTTP(w http.ResponseWriter, r *http.Request, rm *RouteMatch) {
	plan := planBody(r)
	upPath := f.upstreamPath(r, rm)

	f.maybeMirror(r, plan, upPath)

	idempotent := isIdempotentMethod(r.Method)
	attempts := 1
	if idempotent && plan.replayable {
		attempts = f.opts.Retry.Attempts
	}

	for attempt := 0; ; attempt++ {
		tgt, err := f.opts.Pool.Pick(hashKey(r))
		if err != nil {
			f.writeError(w, r, errs.CodeNoHealthyUpstream, "no healthy upstream available")
			return
		}

		st := &attemptState{
			tgt:             tgt,
			upPath:          upPath,
			willRetryStatus: idempotent && plan.replayable && attempt+1 < attempts,
			start:           time.Now(),
		}
		ctx := context.WithValue(r.Context(), attemptKey{}, st)
		var cancel context.CancelFunc
		if f.opts.Timeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, f.opts.Timeout)
		}
		req := r.WithContext(ctx)
		if plan.data != nil {
			req.Body = io.NopCloser(bytes.NewReader(plan.data))
			req.ContentLength = int64(len(plan.data))
			req.GetBody = func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(plan.data)), nil
			}
		}

		f.rp.ServeHTTP(w, req)

		outcome, code, msg := resolveAttempt(st)
		f.opts.Pool.Done(tgt, outcome)
		if cancel != nil {
			cancel()
		}

		if st.err == nil {
			return // response fully committed to the client (any status)
		}
		// Every failure reaching here happened before any byte reached the
		// client (connect errors, pre-response timeouts, swallowed 502/503),
		// so eligibility is exactly the caller-side gates below.
		retryable := idempotent && plan.replayable && attempt+1 < attempts &&
			r.Context().Err() == nil
		if !retryable {
			f.writeError(w, r, code, msg)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-time.After(f.backoff(attempt)):
		}
	}
}

type attemptKey struct{}

// attemptState is shared between the ServeHTTP loop and the ReverseProxy
// hooks via the request context.
type attemptState struct {
	tgt             *pool.Target
	upPath          string
	willRetryStatus bool // next retry will happen; swallow upstream 502/503
	start           time.Time
	err             error // transport-level failure recorded by onError
	status          int   // upstream status recorded by onResponse
}

var errRetryStatus = errors.New("gatewright: retrying bad gateway response")

// runAttempt state plumbing -------------------------------------------------

func attemptFrom(ctx context.Context) *attemptState {
	st, _ := ctx.Value(attemptKey{}).(*attemptState)
	return st
}

// rewrite is the ReverseProxy Rewrite hook: it retargets the outbound
// request at the picked upstream, propagates X-Forwarded-*/Forwarded, and
// strips hop-by-hop headers (preserving the Connection/Upgrade pair while a
// protocol upgrade is in flight so ReverseProxy tunnels it).
func (f *Forwarder) rewrite(pr *httputil.ProxyRequest) {
	st := attemptFrom(pr.In.Context())
	tu := f.targetURL(st.tgt.URL)

	u := *tu
	setJoinedPath(&u, tu.EscapedPath(), st.upPath)
	if q := pr.In.URL.RawQuery; q != "" {
		u.RawQuery = q
	}
	pr.Out.URL = &u
	pr.Out.Host = tu.Host

	writeForwardedHeaders(pr.Out.Header, pr.In)
	StripHopByHop(pr.Out.Header, IsUpgradeRequested(pr.In.Header))
}

func (f *Forwarder) onError(w http.ResponseWriter, req *http.Request, err error) {
	if st := attemptFrom(req.Context()); st != nil {
		st.err = err
	}
	f.warn("upstream_error", "error", err.Error())
}

func (f *Forwarder) onResponse(res *http.Response) error {
	st := attemptFrom(res.Request.Context())
	if st == nil {
		return nil
	}
	st.status = res.StatusCode
	if st.willRetryStatus &&
		(res.StatusCode == http.StatusBadGateway || res.StatusCode == http.StatusServiceUnavailable) {
		return errRetryStatus // swallowed; nothing written to the client yet
	}
	return nil
}

// resolveAttempt maps an attempt outcome onto pool bookkeeping plus the
// client-facing error code/message used when no retry follows.
func resolveAttempt(st *attemptState) (pool.Outcome, string, string) {
	latency := time.Since(st.start)
	switch {
	case errors.Is(st.err, errRetryStatus):
		return pool.Outcome{Success: false, Status: st.status, ErrClass: pool.ErrHTTP5xx, Latency: latency},
			errs.CodeBadGateway, "bad gateway after retries"
	case st.err != nil:
		class, code, msg := classifyTransport(st.err)
		return pool.Outcome{Success: false, ErrClass: class, Latency: latency}, code, msg
	case st.status >= 500:
		return pool.Outcome{Success: false, Status: st.status, ErrClass: pool.ErrHTTP5xx, Latency: latency},
			"", ""
	default:
		return pool.Outcome{Success: true, Status: st.status, Latency: latency}, "", ""
	}
}

// classifyTransport maps transport failures onto pool.ErrClass values and the
// stable client-facing codes: dial/TLS/refused/reset => ErrConnect (UP010),
// malformed/EOF responses => ErrResponse (UP010), deadlines => ErrTimeout
// (UP004 total route timeout).
func classifyTransport(err error) (pool.ErrClass, string, string) {
	if errors.Is(err, context.DeadlineExceeded) {
		return pool.ErrTimeout, errs.CodeTotalTimeout, "total route timeout exceeded"
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return pool.ErrTimeout, errs.CodeTotalTimeout, "upstream timeout"
	}
	if errors.Is(err, context.Canceled) {
		return pool.ErrConnect, errs.CodeBadGateway, "request canceled"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return pool.ErrResponse, errs.CodeBadGateway, "malformed upstream response"
	}
	var oe *net.OpError
	if errors.As(err, &oe) {
		return pool.ErrConnect, errs.CodeBadGateway, "upstream connection failed"
	}
	return pool.ErrResponse, errs.CodeBadGateway, "bad gateway"
}

// backoff returns BaseDelay*2^attempt with +-50% uniform jitter, capped at
// MaxDelay (DESIGN.md §6).
func (f *Forwarder) backoff(attempt int) time.Duration {
	d := f.opts.Retry.BaseDelay
	for i := 0; i < attempt && d < f.opts.Retry.MaxDelay; i++ {
		d *= 2
	}
	if d > f.opts.Retry.MaxDelay {
		d = f.opts.Retry.MaxDelay
	}
	jittered := time.Duration(float64(d) * (0.5 + rand.Float64())) // [50%, 150%)
	if jittered < 0 {
		jittered = 0
	}
	return jittered
}

// hashKey derives the ring-hash key from the client IP ("": no key). Pool
// HashKey resolution stays with the caller-configured pools.
func hashKey(r *http.Request) string {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return ip
}

// ---------------------------------------------------------------------------
// Path building
// ---------------------------------------------------------------------------

// upstreamPath computes the escaped, cleaned incoming path with the route's
// matched prefix stripped when strip_prefix is configured. Prefix stripping
// is segment-aligned (MatchedPrefix never matches "/v1x" for "/v1").
func (f *Forwarder) upstreamPath(r *http.Request, rm *RouteMatch) string {
	incoming := cleanRequestPath(r.URL.EscapedPath())
	if !f.opts.StripPrefix {
		return incoming
	}
	prefix := rm.MatchedPrefix()
	if prefix == "" || prefix == "/" {
		return incoming
	}
	if incoming == prefix {
		return "/"
	}
	if strings.HasPrefix(incoming, prefix+"/") {
		stripped := incoming[len(prefix):] // keeps leading "/"
		if stripped == "" {
			return "/"
		}
		return stripped
	}
	return incoming
}

// joinEscapedPath joins an upstream base path with the request path, both in
// escaped form, keeping exactly one separating slash.
func joinEscapedPath(base, req string) string {
	base = strings.TrimSuffix(base, "/")
	switch {
	case base == "" && req == "":
		return "/"
	case base == "":
		if !strings.HasPrefix(req, "/") {
			req = "/" + req
		}
		return req
	case req == "" || req == "/":
		return base + "/"
	case !strings.HasPrefix(req, "/"):
		req = "/" + req
	}
	return base + req
}

// setJoinedPath sets u's path to joinedEscaped (an escaped path), decoding
// into Path and preserving RawPath when it differs from the canonical
// escaping (url.EscapedPath validates and falls back automatically).
func setJoinedPath(u *url.URL, baseEscaped, reqEscaped string) {
	joined := joinEscapedPath(baseEscaped, reqEscaped)
	decoded, err := url.PathUnescape(joined)
	if err != nil {
		u.Path, u.RawPath = joined, ""
		return
	}
	u.Path = decoded
	if escapePath(decoded) == joined {
		u.RawPath = ""
		return
	}
	u.RawPath = joined
}

// escapePath re-escapes a decoded path using URL path rules (mirrors
// net/url's encodePath mode closely enough that equality checks above only
// decide whether RawPath must be kept).
func escapePath(p string) string {
	var b strings.Builder
	for i := 0; i < len(p); i++ {
		c := p[i]
		switch {
		case 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9':
			b.WriteByte(c)
		case c == '/' || c == '-' || c == '_' || c == '.' || c == '~' || c == '$' ||
			c == '&' || c == '+' || c == ',' || c == ':' || c == '=' || c == '@':
			b.WriteByte(c)
		default:
			const hex = "0123456789ABCDEF"
			b.WriteByte('%')
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&0xF])
		}
	}
	return b.String()
}

func (f *Forwarder) targetURL(raw string) *url.URL {
	if v, ok := f.targetCache.Load(raw); ok {
		return v.(*url.URL)
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		u = &url.URL{} // validated at config load; defensive unroutable fallback
	}
	v, _ := f.targetCache.LoadOrStore(raw, u)
	return v.(*url.URL)
}

// ---------------------------------------------------------------------------
// Body planning and retries
// ---------------------------------------------------------------------------

type bodyPlan struct {
	data       []byte // buffered bytes when replayable and body non-empty
	replayable bool
}

// planBody decides retry replayability per DESIGN.md §6: bodies are buffered
// for replay only when empty or ContentLength in (0, 8KiB]; anything else —
// including unknown-length chunked streams — streams straight through and is
// never retried.
func planBody(r *http.Request) bodyPlan {
	switch {
	case r.Body == nil || r.Body == http.NoBody:
		return bodyPlan{replayable: true}
	case r.ContentLength == 0:
		return bodyPlan{replayable: true}
	case r.ContentLength > 0 && r.ContentLength <= maxReplayBodyBytes:
		data, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			return bodyPlan{}
		}
		r.ContentLength = int64(len(data))
		return bodyPlan{data: data, replayable: true}
	default:
		return bodyPlan{}
	}
}

func isIdempotentMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions,
		http.MethodPut, http.MethodDelete, http.MethodTrace:
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Forwarded headers
// ---------------------------------------------------------------------------

// writeForwardedHeaders propagates proxy identity headers onto out:
//
//   - X-Forwarded-For: the client IP is APPENDED (comma-space separated) to
//     any existing list — never replaced.
//   - X-Forwarded-Host / X-Forwarded-Proto / X-Forwarded-Port are overwritten
//     with this hop's values (proto https when the inbound connection is TLS).
//   - Forwarded (RFC 7239): one combined element by=/for=/host=/proto=;
//     appended as an additional comma-separated list element when an inbound
//     Forwarded header exists.
func writeForwardedHeaders(out http.Header, in *http.Request) {
	clientIP := clientIPOf(in)
	proto := "http"
	if in.TLS != nil {
		proto = "https"
	}

	if clientIP != "" {
		prior := in.Header.Values("X-Forwarded-For")
		xff := make([]string, 0, len(prior)+1)
		xff = append(xff, prior...)
		xff = append(xff, clientIP)
		out.Set("X-Forwarded-For", strings.Join(xff, ", "))
	} else if prior := in.Header.Get("X-Forwarded-For"); prior != "" {
		out.Set("X-Forwarded-For", prior)
	}

	out.Set("X-Forwarded-Host", in.Host)
	out.Set("X-Forwarded-Proto", proto)
	port := explicitPort(in.Host)
	if port == "" {
		port = defaultPort(proto)
	}
	out.Set("X-Forwarded-Port", port)

	var elem strings.Builder
	elem.WriteString("by=_gatewright")
	if clientIP != "" {
		elem.WriteString("; for=")
		elem.WriteString(forwardedToken(clientIP))
	}
	if in.Host != "" {
		elem.WriteString("; host=")
		elem.WriteString(forwardedToken(in.Host))
	}
	elem.WriteString("; proto=")
	elem.WriteString(proto)

	if prior := strings.Join(in.Header.Values("Forwarded"), ", "); prior != "" {
		out.Set("Forwarded", prior+", "+elem.String())
	} else {
		out.Set("Forwarded", elem.String())
	}
}

func clientIPOf(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	return ""
}

func explicitPort(hostport string) string {
	if _, port, err := net.SplitHostPort(hostport); err == nil {
		return port
	}
	return ""
}

func defaultPort(proto string) string {
	if proto == "https" {
		return "443"
	}
	return "80"
}

// forwardedToken renders one RFC 7239 value, quoting/escaping when needed and
// bracketing IPv6 literals.
func forwardedToken(v string) string {
	if strings.Contains(v, ":") && !strings.HasPrefix(v, "[") {
		if net.ParseIP(v) != nil {
			v = "[" + v + "]"
		}
	}
	needsQuoting := v == ""
	for i := 0; i < len(v) && !needsQuoting; i++ {
		c := v[i]
		ok := c == '_' || c == '.' || c == '-' ||
			c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if !ok {
			needsQuoting = true
		}
	}
	if !needsQuoting {
		return v
	}
	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '"' || c == '\\' {
			b.WriteByte('\\')
		}
		b.WriteByte(c)
	}
	b.WriteByte('"')
	return b.String()
}

// ---------------------------------------------------------------------------
// Mirroring
// ---------------------------------------------------------------------------

// maybeMirror clones the request for shadow traffic when configured. The
// clone is built synchronously (so nothing touches r after ServeHTTP
// returns); the round trip runs on a detached context in a fire-and-forget
// goroutine and can never affect the client response or its latency.
func (f *Forwarder) maybeMirror(r *http.Request, plan bodyPlan, upPath string) {
	m := f.opts.Mirror
	mp := f.opts.MirrorPool
	if m == nil || mp == nil {
		return
	}
	if m.Percentage > 0 && m.Percentage < 100 && rand.Float64()*100 >= m.Percentage {
		return
	}
	if !plan.replayable {
		f.info("mirror_skipped_unreplayable_body")
		return
	}
	tgt, err := mp.Pick("")
	if err != nil {
		f.warn("mirror_pick_failed", "error", err.Error())
		return
	}
	mu := *f.targetURL(tgt.URL)
	setJoinedPath(&mu, mu.EscapedPath(), upPath)
	if q := r.URL.RawQuery; q != "" {
		mu.RawQuery = q
	}

	mreq := r.Clone(context.Background())
	mreq.RequestURI = "" // client requests must not carry the server-side field
	mreq.URL = &mu
	mreq.Host = mu.Host
	mreq.Body = nil
	mreq.ContentLength = int64(len(plan.data))
	mreq.TransferEncoding = nil
	if len(plan.data) > 0 {
		mreq.Body = io.NopCloser(bytes.NewReader(plan.data))
		mreq.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(plan.data)), nil
		}
	}
	StripHopByHop(mreq.Header, false)
	writeForwardedHeaders(mreq.Header, r)

	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), mirrorTimeout)
	transport := f.opts.MirrorTransport
	if transport == nil {
		transport = f.opts.Transport
	}
	go func() {
		defer cancel()
		defer func() { _ = recover() }() // mirroring must never take down the gateway
		resp, err := (&http.Client{
			Transport:     transport,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			Timeout:       mirrorTimeout,
		}).Do(mreq.WithContext(ctx))
		if resp != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, mirrorDrainLimit))
			_ = resp.Body.Close()
		}
		if err != nil {
			f.warn("mirror_request_failed", "target", tgt.Name, "error", err.Error())
			return
		}
		f.info("mirrored", "target", tgt.Name, "status", resp.StatusCode)
	}()
}

// ---------------------------------------------------------------------------
// Error writing and logging
// ---------------------------------------------------------------------------

func (f *Forwarder) writeError(w http.ResponseWriter, r *http.Request, code, message string) {
	if code == "" {
		code = errs.CodeBadGateway
		message = "bad gateway"
	}
	errs.WriteWithID(w, errs.New(code, message), r.Header.Get("X-Gatewright-Request-Id"))
	f.warn("proxy_error", "code", code, "message", message)
}

func (f *Forwarder) warn(msg string, kv ...any) {
	if f.opts.Logger != nil {
		f.opts.Logger.Warn(msg, kv...)
	}
}

func (f *Forwarder) info(msg string, kv ...any) {
	if f.opts.Logger != nil {
		f.opts.Logger.Info(msg, kv...)
	}
}
