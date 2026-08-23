// Command load is a standalone HTTP load generator for exercising Gatewright
// (or any HTTP endpoint) and reading the results like an operator: honest
// counts, named percentiles, plain aligned columns.
//
// Stdlib only. Build: go build ./test/load
//
// Examples:
//
//	load -url http://127.0.0.1:8080/v1/ -d 30s -c 8
//	load -url http://127.0.0.1:8080/v1/ -qps 200 -header "X-API-Key: k1" -output json
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	histLo       = 100 * time.Microsecond // histogram lower edge
	histHi       = 60 * time.Second       // histogram upper edge
	histBuckets  = 144                    // geometric buckets between the edges
	errorBudget  = 0.05                   // exit 1 above this fraction unless -allow-errors
	maxReasonLen = 48                     // truncate exotic transport errors to this
)

type headerList []string

func (h *headerList) String() string { return strings.Join(*h, ", ") }

func (h *headerList) Set(v string) error {
	if !strings.Contains(v, ":") {
		return fmt.Errorf("want \"Name: value\", got %q", v)
	}
	*h = append(*h, v)
	return nil
}

type config struct {
	url          string
	method       string
	concurrency  int
	duration     time.Duration
	timeout      time.Duration
	bodySize     int
	expectStatus int
	qps          float64
	output       string
	allowErrors  bool
	headers      headerList
}

func parseFlags(cfg *config) {
	flag.StringVar(&cfg.url, "url", "", "target URL, including scheme (required)")
	flag.StringVar(&cfg.method, "method", "GET", "HTTP method")
	flag.IntVar(&cfg.concurrency, "c", 8, "concurrent workers")
	flag.DurationVar(&cfg.duration, "d", 30*time.Second, "test duration")
	flag.DurationVar(&cfg.timeout, "timeout", 10*time.Second, "per-request timeout")
	flag.IntVar(&cfg.bodySize, "body", 0, "request body size in bytes (zero-filled)")
	flag.IntVar(&cfg.expectStatus, "expect-status", 200, "status counted as success; any other is an error")
	flag.Float64Var(&cfg.qps, "qps", 0, "target requests per second (0 = unlimited tight loop per worker)")
	flag.StringVar(&cfg.output, "output", "human", "output format: human|json")
	flag.BoolVar(&cfg.allowErrors, "allow-errors", false, "exit 0 even when the error fraction exceeds 5%")
	flag.Var(&cfg.headers, "header", `request header, repeatable: -header "Name: value"`)
	flag.Parse()
}

func (c *config) validate() error {
	if c.url == "" {
		return errors.New("-url is required")
	}
	u, err := url.Parse(c.url)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("-url %q is not an absolute http(s) URL", c.url)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("-url scheme %q not supported (http/https)", u.Scheme)
	}
	if c.method == "" || strings.ContainsAny(c.method, " \t") {
		return fmt.Errorf("-method %q is not a token", c.method)
	}
	if c.concurrency < 1 {
		return fmt.Errorf("-c must be >= 1, got %d", c.concurrency)
	}
	if c.duration <= 0 {
		return fmt.Errorf("-d must be positive, got %v", c.duration)
	}
	if c.timeout <= 0 {
		return fmt.Errorf("-timeout must be positive, got %v", c.timeout)
	}
	if c.bodySize < 0 {
		return fmt.Errorf("-body must be >= 0, got %d", c.bodySize)
	}
	if c.expectStatus < 100 || c.expectStatus > 599 {
		return fmt.Errorf("-expect-status must be a HTTP status code, got %d", c.expectStatus)
	}
	if c.qps < 0 {
		return fmt.Errorf("-qps must be >= 0, got %v", c.qps)
	}
	if c.output != "human" && c.output != "json" {
		return fmt.Errorf("-output must be human|json, got %q", c.output)
	}
	return nil
}

// shard accumulates one worker's results behind its own mutex: fixed-size
// counters plus the shared-shape histogram, never unbounded slices.
type shard struct {
	mu     sync.Mutex
	total  uint64
	ok     uint64
	errs   uint64
	sumNs  float64
	sumSq  float64
	hist   []uint64
	reason map[string]uint64
}

func newShard() *shard {
	return &shard{hist: make([]uint64, histBuckets), reason: map[string]uint64{}}
}

func (s *shard) record(dur time.Duration, ok bool, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.total++
	if ok {
		s.ok++
	} else {
		s.errs++
		s.reason[reason]++
	}
	ns := float64(dur.Nanoseconds())
	s.sumNs += ns
	s.sumSq += ns * ns
	bucketOf(dur).add(s.hist)
}

type bucketRef int

func bucketOf(d time.Duration) bucketRef {
	if d < histLo {
		return 0
	}
	if d >= histHi {
		return bucketRef(histBuckets - 1)
	}
	ratio := math.Log(float64(histHi)/float64(histLo)) / float64(histBuckets-1)
	i := int(math.Log(float64(d)/float64(histLo)) / ratio)
	if i < 0 {
		i = 0
	}
	if i >= histBuckets {
		i = histBuckets - 1
	}
	return bucketRef(i)
}

func (b bucketRef) add(hist []uint64) { hist[int(b)]++ }

func bucketLowerEdge(i int) float64 {
	ratio := math.Log(float64(histHi)/float64(histLo)) / float64(histBuckets-1)
	return float64(histLo) * math.Exp(ratio*float64(i))
}

// percentiles interpolates each p in ps (fractions of total) from the merged
// log-bucketed histogram. Values are interpolated geometrically inside the
// hit bucket -- honest histogram interpolation, not an exact quantile.
func percentiles(merged []uint64, total uint64, ps ...float64) []time.Duration {
	out := make([]time.Duration, len(ps))
	if total == 0 {
		return out
	}
	cum := uint64(0)
	idx := 0
	for pi, p := range ps {
		target := uint64(math.Round(p * float64(total)))
		if target < 1 {
			target = 1
		}
		for idx < len(merged) && cum+merged[idx] < target {
			cum += merged[idx]
			idx++
		}
		if idx >= len(merged) {
			out[pi] = histHi
			continue
		}
		cnt := merged[idx]
		var frac float64
		if cnt > 0 {
			frac = float64(target-cum) / float64(cnt)
		}
		lower := bucketLowerEdge(idx)
		ratio := math.Log(float64(histHi)/float64(histLo)) / float64(histBuckets-1)
		val := lower * math.Exp(ratio*math.Min(frac, 1))
		if val > float64(histHi) {
			val = float64(histHi)
		}
		out[pi] = time.Duration(val)
	}
	return out
}

type reasonCount struct {
	Reason string `json:"reason"`
	Count  uint64 `json:"count"`
}

type report struct {
	URL             string        `json:"url"`
	Method          string        `json:"method"`
	DurationActualS float64       `json:"duration_actual_s"`
	RequestsTotal   uint64        `json:"requests_total"`
	Ok              uint64        `json:"ok"`
	Errors          uint64        `json:"errors"`
	ErrorFraction   float64       `json:"error_fraction"`
	RequestsPerS    float64       `json:"requests_per_s"`
	P50S            float64       `json:"p50_s"`
	P90S            float64       `json:"p90_s"`
	P95S            float64       `json:"p95_s"`
	P99S            float64       `json:"p99_s"`
	PercentileNote  string        `json:"percentile_note"`
	TopErrors       []reasonCount `json:"top_errors,omitempty"`
	Exit            string        `json:"exit"`
}

func classifyErr(err error) string {
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "context deadline exceeded"):
		return "timeout"
	case strings.Contains(msg, "context canceled"):
		return "canceled"
	case strings.Contains(msg, "connection refused"):
		return "connection_refused"
	case strings.Contains(msg, "no such host"):
		return "dns_failure"
	case strings.Contains(msg, "i/o timeout"):
		return "timeout"
	case strings.Contains(msg, "EOF"):
		return "eof"
	case strings.Contains(msg, "TLS"):
		return "tls_error"
	case strings.Contains(msg, "reset by peer"):
		return "connection_reset"
	}
	if len(msg) > maxReasonLen {
		msg = msg[:maxReasonLen]
	}
	return msg
}

func main() {
	var cfg config
	parseFlags(&cfg)
	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}

	body := make([]byte, cfg.bodySize)

	transport := &http.Transport{
		MaxIdleConns:          cfg.concurrency,
		MaxIdleConnsPerHost:   cfg.concurrency,
		IdleConnTimeout:       90 * time.Second,
		DisableCompression:    true,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: cfg.timeout,
		ForceAttemptHTTP2:     true,
		DialContext: (&net.Dialer{
			Timeout:   cfg.timeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	client := &http.Client{Transport: transport}

	shards := make([]*shard, cfg.concurrency)
	for i := range shards {
		shards[i] = newShard()
	}

	started := time.Now()
	deadline := started.Add(cfg.duration)
	var ticket atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	baseHeaders := make([]string, 0, len(cfg.headers))
	for _, h := range cfg.headers {
		baseHeaders = append(baseHeaders, h)
	}

	for w := 0; w < cfg.concurrency; w++ {
		wg.Add(1)
		go func(sh *shard) {
			defer wg.Done()
			reqCtx := context.Background()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if time.Now().After(deadline) {
					return
				}
				if cfg.qps > 0 { // paced mode: claim slot n, sleep until its due time
					n := ticket.Add(1)
					due := started.Add(time.Duration(float64(n) / cfg.qps * float64(time.Second)))
					if delay := time.Until(due); delay > 0 {
						if time.Now().Add(delay).After(deadline) {
							return
						}
						time.Sleep(delay)
					}
				}

				ctx, cancel := context.WithTimeout(reqCtx, cfg.timeout)
				req, err := http.NewRequestWithContext(ctx, cfg.method, cfg.url, bytesReader(body))
				if err != nil {
					cancel()
					sh.record(0, false, "bad_request: "+err.Error())
					continue
				}
				if cfg.bodySize > 0 {
					req.ContentLength = int64(cfg.bodySize)
				}
				for _, h := range baseHeaders {
					name, val, _ := strings.Cut(h, ":")
					req.Header.Set(strings.TrimSpace(name), strings.TrimSpace(val))
				}

				begin := time.Now()
				resp, err := client.Do(req)
				dur := time.Since(begin)
				if err != nil {
					cancel()
					sh.record(dur, false, classifyErr(err))
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body) // drain for connection reuse
				_ = resp.Body.Close()
				cancel()

				if resp.StatusCode == cfg.expectStatus {
					sh.record(dur, true, "")
				} else {
					sh.record(dur, false, fmt.Sprintf("status=%d", resp.StatusCode))
				}
			}
		}(shards[w])
	}

	wg.Wait()
	actual := time.Since(started)
	close(stop)

	rep := buildReport(&cfg, shards, actual)
	if cfg.output == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			fmt.Fprintln(os.Stderr, "error encoding report:", err)
			os.Exit(2)
		}
	} else {
		printHuman(rep)
	}

	if rep.ErrorFraction > errorBudget && !cfg.allowErrors {
		fmt.Fprintf(os.Stderr, "error fraction %.1f%% exceeds %.0f%% budget\n",
			rep.ErrorFraction*100, errorBudget*100)
		os.Exit(1)
	}
}

func buildReport(cfg *config, shards []*shard, actual time.Duration) report {
	merged := make([]uint64, histBuckets)
	var rep report
	rep.URL, rep.Method = cfg.url, cfg.method
	rep.DurationActualS = actual.Seconds()
	reasons := map[string]uint64{}
	for _, sh := range shards {
		sh.mu.Lock()
		rep.RequestsTotal += sh.total
		rep.Ok += sh.ok
		rep.Errors += sh.errs
		for r, n := range sh.reason {
			reasons[r] += n
		}
		for i, n := range sh.hist {
			merged[i] += n
		}
		sh.mu.Unlock()
	}
	if rep.RequestsTotal > 0 {
		rep.ErrorFraction = float64(rep.Errors) / float64(rep.RequestsTotal)
	}
	rep.RequestsPerS = float64(rep.RequestsTotal) / actual.Seconds()

	pss := percentiles(merged, rep.RequestsTotal, 0.50, 0.90, 0.95, 0.99)
	rep.P50S, rep.P90S, rep.P95S, rep.P99S = pss[0].Seconds(), pss[1].Seconds(), pss[2].Seconds(), pss[3].Seconds()
	rep.PercentileNote = "interpolated from log-binned histogram 100us..60s"

	top := make([]reasonCount, 0, len(reasons))
	for r, n := range reasons {
		top = append(top, reasonCount{Reason: r, Count: n})
	}
	sort.Slice(top, func(i, j int) bool {
		if top[i].Count != top[j].Count {
			return top[i].Count > top[j].Count
		}
		return top[i].Reason < top[j].Reason
	})
	if len(top) > 5 {
		top = top[:5]
	}
	rep.TopErrors = top

	rep.Exit = "ok"
	if rep.ErrorFraction > errorBudget {
		rep.Exit = "error-fraction-exceeded"
	}
	return rep
}

func printHuman(rep report) {
	const labelW = 14
	row := func(label, val string) {
		fmt.Printf("%-*s %s\n", labelW, label, val)
	}
	row("url", rep.URL)
	row("method", rep.Method)
	row("duration", fmt.Sprintf("%.2fs (actual)", rep.DurationActualS))
	row("requests", fmt.Sprintf("%d total  %.1f/s", rep.RequestsTotal, rep.RequestsPerS))
	row("ok", fmt.Sprintf("%d", rep.Ok))
	row("errors", fmt.Sprintf("%d  (%.2f%%)", rep.Errors, rep.ErrorFraction*100))
	row("latency", fmt.Sprintf("p50=%-10s p90=%-10s p95=%-10s p99=%-10s",
		fmtDuration(time.Duration(rep.P50S*float64(time.Second))),
		fmtDuration(time.Duration(rep.P90S*float64(time.Second))),
		fmtDuration(time.Duration(rep.P95S*float64(time.Second))),
		fmtDuration(time.Duration(rep.P99S*float64(time.Second)))))
	row("", "* "+rep.PercentileNote)
	if len(rep.TopErrors) > 0 {
		row("top errors", "")
		w := 0
		for _, rc := range rep.TopErrors {
			if len(rc.Reason) > w {
				w = len(rc.Reason)
			}
		}
		for _, rc := range rep.TopErrors {
			fmt.Printf("%-*s %-*s %d\n", labelW, "", w, rc.Reason, rc.Count)
		}
	}
	row("result", rep.Exit)
}

func fmtDuration(d time.Duration) string {
	switch {
	case d >= 10*time.Second:
		return fmt.Sprintf("%.1fs", d.Seconds())
	case d >= time.Second:
		return fmt.Sprintf("%.2fs", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	case d <= 0:
		return "-"
	default:
		return fmt.Sprintf("%.0fus", float64(d)/float64(time.Microsecond))
	}
}

func bytesReader(b []byte) io.Reader {
	if len(b) == 0 {
		return nil
	}
	return bytes.NewReader(b)
}
