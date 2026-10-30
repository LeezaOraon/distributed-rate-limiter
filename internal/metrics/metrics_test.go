package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LeezaOraon/distributed-rate-limiter/internal/middleware"
)

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	return string(body)
}

func TestMetrics_CountsOutcomesAndErrors(t *testing.T) {
	m := New()
	m.Observe(middleware.OutcomeAllowed, time.Millisecond)
	m.Observe(middleware.OutcomeAllowed, time.Millisecond)
	m.Observe(middleware.OutcomeDenied, time.Millisecond)
	m.Observe(middleware.OutcomeFailOpen, time.Millisecond)
	m.Observe(middleware.OutcomeFailClosed, time.Millisecond)

	out := scrape(t, m)
	for _, want := range []string{
		`ratelimit_requests_total{outcome="allowed"} 2`,
		`ratelimit_requests_total{outcome="denied"} 1`,
		`ratelimit_requests_total{outcome="fail_open"} 1`,
		`ratelimit_requests_total{outcome="fail_closed"} 1`,
		`ratelimit_limiter_errors_total 2`,
		`ratelimit_check_duration_seconds_count 5`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}

func TestMetrics_ExportsZeroValuesFromTheStart(t *testing.T) {
	out := scrape(t, New())
	if !strings.Contains(out, `ratelimit_requests_total{outcome="fail_open"} 0`) {
		t.Error("fail_open series should exist at 0 before any request, so rate() and alerts work")
	}
}
