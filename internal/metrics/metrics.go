// Package metrics exposes the rate limiter's operational telemetry in
// Prometheus format.
package metrics

import (
	"net/http"
	"time"

	"github.com/LeezaOraon/distributed-rate-limiter/internal/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics implements middleware.Recorder with Prometheus collectors.
type Metrics struct {
	reg      *prometheus.Registry
	requests *prometheus.CounterVec
	errors   prometheus.Counter
	check    prometheus.Histogram
}

var _ middleware.Recorder = (*Metrics)(nil)

// New creates a Metrics backed by its own registry, so tests and multiple
// instances in one process never collide on global state.
func New() *Metrics {
	m := &Metrics{
		reg: prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ratelimit_requests_total",
			Help: "Requests by limiter outcome: allowed, denied, fail_open (served uncounted because the limiter was unreachable) or fail_closed.",
		}, []string{"outcome"}),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "ratelimit_limiter_errors_total",
			Help: "Limiter checks that failed (e.g. Redis unreachable), whatever the fail policy then did.",
		}),
		check: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "ratelimit_check_duration_seconds",
			Help: "Time spent in the limiter check, including the Redis round trip.",
			// 100µs to ~3s: a healthy check is sub-millisecond, an outage
			// shows up in the upper buckets.
			Buckets: prometheus.ExponentialBuckets(0.0001, 2.5, 12),
		}),
	}
	m.reg.MustRegister(m.requests, m.errors, m.check)
	for _, o := range []middleware.Outcome{
		middleware.OutcomeAllowed, middleware.OutcomeDenied,
		middleware.OutcomeFailOpen, middleware.OutcomeFailClosed,
	} {
		m.requests.WithLabelValues(string(o)) // export zero so rates work from the start
	}
	return m
}

// Observe records one request.
func (m *Metrics) Observe(outcome middleware.Outcome, d time.Duration) {
	m.requests.WithLabelValues(string(outcome)).Inc()
	m.check.Observe(d.Seconds())
	if outcome == middleware.OutcomeFailOpen || outcome == middleware.OutcomeFailClosed {
		m.errors.Inc()
	}
}

// Handler serves the metrics in Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
