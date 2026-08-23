package runtime

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"gatewright/internal/config"
	"gatewright/internal/errs"
	"gatewright/internal/middleware"
	"gatewright/internal/obs"
	"gatewright/internal/proxy"
)

// assemble builds the pipeline:
//
//	recover -> request-id -> access-log -> trace-gate -> dispatcher
//	  dispatcher: match -> per-route chain (body-limit, total-timeout,
//	  cors, auth, request-headers, rate-limit) -> forwarder
//
// This is the documented order from internal/middleware/order.go.
func (rt *Runtime) assemble(logger obs.Logger, metrics *obs.Metrics) http.Handler {
	dispatcher := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cw := &countingWriter{ResponseWriter: w}
		start := time.Now()

		rec := middleware.RecordFrom(r.Context())
		rm, apiErr := rt.router.Match(r)
		routeName := "<unmatched>"
		if rm != nil {
			routeName = rm.Route.Name
			if rec != nil {
				rec.Fields.Route = routeName
			}
			if t := middleware.TraceFrom(r.Context()); t != nil {
				t.Route = routeName
			}
		}
		defer func() { rt.stats.countRequest(routeName, start, cw.status) }()
		if apiErr != nil {
			if rec != nil {
				rec.Fields.Code = apiErr.Code
			}
			writeRoutingError(cw, r, apiErr)
			return
		}
		rt.routeChain(rm.Route.Name).ServeHTTP(cw, WithRouteMatch(r, rm))
	})

	h := middleware.Chain(dispatcher,
		middleware.NewRequestID(),
		// An empty constructor route defers to Record.Fields.Route, which the
		// dispatcher sets from the actual route match (never "<gateway>").
		middleware.NewAccessLog(logger, ""),
	)
	return recoverMiddleware(traceGate(rt.trace, logger, h))
}

func routeNameFor(ctx context.Context) string {
	if v, ok := ctx.Value(routeNameKey{}).(string); ok {
		return v
	}
	return "<unknown>"
}

type routeNameKey struct{}

// countingWriter records the response status without altering behaviour.
type countingWriter struct {
	http.ResponseWriter
	status int
}

func (cw *countingWriter) WriteHeader(code int) {
	if cw.status == 0 {
		cw.status = code
	}
	cw.ResponseWriter.WriteHeader(code)
}

func (cw *countingWriter) Write(b []byte) (int, error) {
	if cw.status == 0 {
		cw.status = http.StatusOK
	}
	return cw.ResponseWriter.Write(b)
}

func (cw *countingWriter) Flush() {
	if f, ok := cw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack passes protocol upgrades (RFC6455 et al) through to the server
// connection; mirrors middleware's completionWriter. Additive only.
func (cw *countingWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := cw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("runtime: ResponseWriter does not support Hijack")
	}
	return hj.Hijack()
}

// routeChain returns the pre-built middleware stack for a route. Chains are
// constructed eagerly during buildRuntime (M7), so this is an immutable map
// read: safe for concurrent use without locking because the map is fully
// populated before the generation is published via atomic swap.
func (rt *Runtime) routeChain(routeName string) http.Handler {
	return rt.chains[routeName]
}

// buildChains eagerly constructs every route's middleware chain, recovering
// construction panics (e.g. a misconfigured NewAuth) into reload-rejecting
// errors instead of first-request crashes.
func (rt *Runtime) buildChains(logger obs.Logger, metrics *obs.Metrics) error {
	for i := range rt.Cfg.Routes {
		route := &rt.Cfg.Routes[i]
		h, err := rt.buildRouteChainSafe(logger, metrics, route)
		if err != nil {
			return err
		}
		rt.chains[route.Name] = h
	}
	return nil
}

func (rt *Runtime) buildRouteChainSafe(logger obs.Logger, metrics *obs.Metrics, route *config.Route) (h http.Handler, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("route %q: middleware chain build failed: %v", route.Name, r)
		}
	}()
	return rt.buildRouteChain(logger, metrics, route), nil
}

// namedMiddleware pairs a stage with its documented trace name.
type namedMiddleware struct {
	name string
	mw   middleware.Middleware
}

// buildRouteChain assembles each route's middleware stack ending in its
// forwarder. Response-headers runs outermost so it decorates every response
// the route produces, including error envelopes from inner stages; after it,
// stages follow the documented order of internal/middleware/order.go.
func (rt *Runtime) buildRouteChain(logger obs.Logger, metrics *obs.Metrics, route *config.Route) http.Handler {
	fwd := rt.routeFwd[route.Name]
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rm := RouteMatchFrom(r.Context())
		if rm == nil {
			errs.WriteWithID(w, errs.New(errs.CodeInternal, "route match missing"),
				r.Header.Get(middleware.RequestIDHeader))
			return
		}
		fwd.ServeHTTP(w, r, rm)
	})

	mws := make([]namedMiddleware, 0, 8)
	add := func(name string, mw middleware.Middleware) {
		if mw != nil {
			mws = append(mws, namedMiddleware{name: name, mw: mw})
		}
	}
	if rh := route.ResponseHeaders; len(rh.Set)+len(rh.Add)+len(rh.Del) > 0 {
		add("response-headers", middleware.NewResponseHeaders(rh))
	}
	if max := route.BodyLimit.MaxBytes(); max >= 0 {
		add(middleware.OrderNames[middleware.PosBodyLimit], middleware.NewBodyLimit(max))
	}
	add(middleware.OrderNames[middleware.PosTotalTimeout], middleware.NewTotalTimeout(route.Timeout.D))
	if route.CORS != nil {
		add(middleware.OrderNames[middleware.PosCORS], middleware.NewCORS(route.CORS))
	}
	if route.Auth != nil && route.Auth.Type != "none" && route.Auth.Type != "" {
		add(middleware.OrderNames[middleware.PosAuth], middleware.NewAuth(route.Auth))
	}
	if len(route.RequestHeaders.Set)+len(route.RequestHeaders.Add)+len(route.RequestHeaders.Del) > 0 {
		add(middleware.OrderNames[middleware.PosRequestHeaders], middleware.NewRequestHeaders(route.RequestHeaders))
	}
	if entries := rt.entries[route.Name]; len(entries) > 0 {
		add(middleware.OrderNames[middleware.PosRateLimit], middleware.NewRateLimit(entries, metrics))
	}

	var timed []middleware.Middleware
	for _, nm := range mws {
		timed = append(timed, timeStage(nm.name, nm.mw))
	}
	return middleware.Chain(handler, timed...)
}

func timeStage(name string, next middleware.Middleware) middleware.Middleware {
	return func(inner http.Handler) http.Handler {
		wrapped := next(inner)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t0 := time.Now()
			wrapped.ServeHTTP(w, r)
			middleware.RecordStage(r.Context(), name, time.Since(t0))
		})
	}
}

// ---------------------------------------------------------------------------
// Panic recovery (implicit outermost stage): panics become INT500 envelopes.
// ---------------------------------------------------------------------------

type recoverWriter struct {
	http.ResponseWriter
	wrote bool
}

func (rw *recoverWriter) WriteHeader(code int) {
	rw.wrote = true
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *recoverWriter) Write(b []byte) (int, error) {
	rw.wrote = true
	return rw.ResponseWriter.Write(b)
}

func (rw *recoverWriter) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack passes protocol upgrades through to the server connection.
func (rw *recoverWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := rw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("runtime: ResponseWriter does not support Hijack")
	}
	return hj.Hijack()
}

func recoverMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw := &recoverWriter{ResponseWriter: w}
		defer func() {
			if rec := recover(); rec != nil {
				fmt.Fprintf(os.Stderr, "gatewright panic: %v\n%s\n", rec, debug.Stack())
				if !rw.wrote {
					errs.WriteWithID(rw, errs.New(errs.CodeInternal, "internal gateway error"),
						r.Header.Get("X-Gatewright-Request-Id"))
				}
			}
		}()
		next.ServeHTTP(rw, r)
	})
}

// ---------------------------------------------------------------------------
// Route-match context plumbing
// ---------------------------------------------------------------------------

type rmCtxKey struct{}

func WithRouteMatch(r *http.Request, rm *proxy.RouteMatch) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), rmCtxKey{}, rm))
}

func RouteMatchFrom(ctx context.Context) *proxy.RouteMatch {
	v, _ := ctx.Value(rmCtxKey{}).(*proxy.RouteMatch)
	return v
}

// ---------------------------------------------------------------------------
// Trace gate: requests carrying X-Gatewright-Trace: 1 report their pipeline.
// ---------------------------------------------------------------------------

const TraceHeader = "X-Gatewright-Trace"

func traceGate(enabled bool, logger obs.Logger, next http.Handler) http.Handler {
	if !enabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(TraceHeader) != "1" {
			next.ServeHTTP(w, r)
			return
		}
		tw := &traceWriter{ResponseWriter: w}
		next.ServeHTTP(tw, r.WithContext(middleware.WithTrace(r.Context(), &tw.trace)))
		// The response header can only carry stages completed before the
		// upstream responded; the full pipeline trace goes to the log.
		logger.Info("request-trace",
			"req_id", r.Header.Get("X-Gatewright-Request-Id"),
			"route", tw.trace.Route,
			"status", tw.status,
			"stages", traceSummary(tw.trace.Stages),
		)
	})
}

type traceWriter struct {
	http.ResponseWriter
	trace       middleware.Trace
	wroteHeader bool
	status      int
}

func (tw *traceWriter) WriteHeader(code int) {
	tw.status = code
	tw.emit()
	tw.wroteHeader = true
	tw.ResponseWriter.WriteHeader(code)
}

func (tw *traceWriter) Write(b []byte) (int, error) {
	if !tw.wroteHeader {
		if tw.status == 0 {
			tw.status = http.StatusOK
		}
		tw.emit()
		tw.wroteHeader = true
	}
	return tw.ResponseWriter.Write(b)
}

func (tw *traceWriter) Flush() {
	if f, ok := tw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (tw *traceWriter) emit() {
	route := tw.trace.Route
	if route == "" {
		route = "<unmatched>"
	}
	var b strings.Builder
	b.WriteString("route=" + route)
	for _, st := range tw.trace.Stages {
		fmt.Fprintf(&b, "; %s=%.3fms", st.Name, float64(st.Duration.Nanoseconds())/1e6)
	}
	tw.Header().Set(TraceHeader, b.String())
}

func traceSummary(stages []middleware.StageTiming) string {
	var b strings.Builder
	for i, st := range stages {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s=%.3fms", st.Name, float64(st.Duration.Nanoseconds())/1e6)
	}
	return b.String()
}

func writeRoutingError(w http.ResponseWriter, r *http.Request, apiErr *errs.APIError) {
	reqID := r.Header.Get("X-Gatewright-Request-Id")
	if apiErr.Code == errs.CodeMethodNotAllowed {
		if allow := allowFromMessage(apiErr.Message); allow != "" {
			w.Header().Set("Allow", allow)
		}
	}
	errs.WriteWithID(w, apiErr, reqID)
}

// allowFromMessage extracts an Allow list the router embeds in its message.
func allowFromMessage(msg string) string {
	if i := strings.Index(msg, ":"); i >= 0 && i+2 <= len(msg) {
		return strings.TrimSpace(msg[i+1:])
	}
	return ""
}
