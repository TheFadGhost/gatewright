package runtime

import (
	"context"
	"fmt"
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
		rt.routeChain(logger, metrics, rm.Route).ServeHTTP(cw, WithRouteMatch(r, rm))
	})

	h := middleware.Chain(dispatcher,
		middleware.NewRequestID(),
		middleware.NewAccessLog(logger, "<gateway>"),
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

// routeChain lazily assembles (once per generation) each route's middleware
// stack ending in its forwarder.
func (rt *Runtime) routeChain(logger obs.Logger, metrics *obs.Metrics, route *config.Route) http.Handler {
	if h, ok := rt.chains[route.Name]; ok {
		return h
	}
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

	var mws []middleware.Middleware
	if max := route.BodyLimit.MaxBytes(); max >= 0 {
		mws = append(mws, middleware.NewBodyLimit(max))
	}
	mws = append(mws, middleware.NewTotalTimeout(route.Timeout.D))
	if route.CORS != nil {
		mws = append(mws, middleware.NewCORS(route.CORS))
	}
	if route.Auth != nil && route.Auth.Type != "none" && route.Auth.Type != "" {
		mws = append(mws, middleware.NewAuth(route.Auth))
	}
	if len(route.RequestHeaders.Set)+len(route.RequestHeaders.Add)+len(route.RequestHeaders.Del) > 0 {
		mws = append(mws, middleware.NewRequestHeaders(route.RequestHeaders))
	}
	if entries := rt.entries[route.Name]; len(entries) > 0 {
		mws = append(mws, middleware.NewRateLimit(entries, metrics))
	}

	var timed []middleware.Middleware
	for i, m := range mws {
		timed = append(timed, timeStage(stageNames(i, len(mws)), m))
	}
	h := middleware.Chain(handler, timed...)
	rt.chains[route.Name] = h
	return h
}

// stageNames maps pipeline positions to documented names for trace output.
func stageNames(i, n int) string {
	// Positions 4..9 of OrderNames correspond to body-limit..rate-limit.
	idx := 4 + i
	if idx < len(middleware.OrderNames) && n > 0 {
		return middleware.OrderNames[idx]
	}
	return "stage"
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
