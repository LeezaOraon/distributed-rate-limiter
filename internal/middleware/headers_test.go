package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/LeezaOraon/distributed-rate-limiter/internal/ratelimiter"
)

// stubChecker returns a fixed Result (or error) and records the keys it saw.
type stubChecker struct {
	res  ratelimiter.Result
	err  error
	mu   sync.Mutex
	keys []string
}

func (s *stubChecker) Allow(ctx context.Context, key string) (bool, error) {
	res, err := s.Check(ctx, key)
	return res.Allowed, err
}

func (s *stubChecker) Check(_ context.Context, key string) (ratelimiter.Result, error) {
	s.mu.Lock()
	s.keys = append(s.keys, key)
	s.mu.Unlock()
	return s.res, s.err
}

type recorded struct {
	outcome Outcome
	took    time.Duration
}

type spyRecorder struct{ got []recorded }

func (s *spyRecorder) Observe(o Outcome, d time.Duration) { s.got = append(s.got, recorded{o, d}) }

var okHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})

func serve(h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec
}

func TestRateLimit_AllowedSetsRateLimitHeaders(t *testing.T) {
	stub := &stubChecker{res: ratelimiter.Result{Allowed: true, Limit: 100, Remaining: 42, ResetAfter: 2500 * time.Millisecond}}
	rec := serve(RateLimit(stub, ByIP, FailOpen)(okHandler), httptest.NewRequest("GET", "/", nil))

	if rec.Code != 200 {
		t.Fatalf("code = %d, want 200", rec.Code)
	}
	for h, want := range map[string]string{"X-RateLimit-Limit": "100", "X-RateLimit-Remaining": "42", "X-RateLimit-Reset": "3"} {
		if got := rec.Header().Get(h); got != want {
			t.Errorf("%s = %q, want %q", h, got, want)
		}
	}
	if got := rec.Header().Get("Retry-After"); got != "" {
		t.Errorf("Retry-After = %q on an allowed request, want none", got)
	}
}

func TestRateLimit_DeniedUsesRealRetryAfter(t *testing.T) {
	stub := &stubChecker{res: ratelimiter.Result{Limit: 100, ResetAfter: time.Second, RetryAfter: 4200 * time.Millisecond}}
	rec := serve(RateLimit(stub, ByIP, FailOpen)(okHandler), httptest.NewRequest("GET", "/", nil))

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("code = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "5" {
		t.Errorf("Retry-After = %q, want 5 (4.2s rounded up)", got)
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Errorf("X-RateLimit-Remaining = %q, want 0", got)
	}
}

func TestRateLimit_DeniedWithoutDetailsFallsBackToOneSecond(t *testing.T) {
	// A plain Allower (no Checker) reports no limit, so no X-RateLimit headers.
	rec := serve(RateLimit(denyAll{}, ByIP, FailOpen)(okHandler), httptest.NewRequest("GET", "/", nil))

	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want 1", got)
	}
	if got := rec.Header().Get("X-RateLimit-Limit"); got != "" {
		t.Errorf("X-RateLimit-Limit = %q, want none", got)
	}
}

type denyAll struct{}

func (denyAll) Allow(context.Context, string) (bool, error) { return false, nil }

func TestByHeader_KeysPerHeaderValue(t *testing.T) {
	stub := &stubChecker{res: ratelimiter.Result{Allowed: true}}
	h := RateLimit(stub, ByHeader("X-API-Key"), FailOpen)(okHandler)

	for _, k := range []string{"alice", "bob", "alice"} {
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("X-API-Key", k)
		serve(h, r)
	}
	want := []string{"hdr:X-API-Key:alice", "hdr:X-API-Key:bob", "hdr:X-API-Key:alice"}
	for i := range want {
		if stub.keys[i] != want[i] {
			t.Fatalf("key %d = %q, want %q", i, stub.keys[i], want[i])
		}
	}
}

func TestByHeader_KeyIsIndependentOfIP(t *testing.T) {
	r1 := httptest.NewRequest("GET", "/", nil)
	r1.RemoteAddr = "1.1.1.1:1000"
	r1.Header.Set("X-API-Key", "k")
	r2 := httptest.NewRequest("GET", "/", nil)
	r2.RemoteAddr = "2.2.2.2:2000"
	r2.Header.Set("X-API-Key", "k")

	f := ByHeader("X-API-Key")
	if f(r1) != f(r2) {
		t.Fatal("same API key from different IPs must share one bucket")
	}
}

func TestRateLimit_RecorderSeesEveryOutcome(t *testing.T) {
	cases := []struct {
		name   string
		stub   *stubChecker
		policy FailPolicy
		want   Outcome
		code   int
	}{
		{"allowed", &stubChecker{res: ratelimiter.Result{Allowed: true}}, FailOpen, OutcomeAllowed, 200},
		{"denied", &stubChecker{}, FailOpen, OutcomeDenied, 429},
		{"fail open", &stubChecker{err: context.DeadlineExceeded}, FailOpen, OutcomeFailOpen, 200},
		{"fail closed", &stubChecker{err: context.DeadlineExceeded}, FailClosed, OutcomeFailClosed, 503},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spy := &spyRecorder{}
			rec := serve(RateLimit(tc.stub, ByIP, tc.policy, WithRecorder(spy))(okHandler), httptest.NewRequest("GET", "/", nil))

			if rec.Code != tc.code {
				t.Errorf("code = %d, want %d", rec.Code, tc.code)
			}
			if len(spy.got) != 1 || spy.got[0].outcome != tc.want {
				t.Fatalf("recorded %+v, want exactly one %q", spy.got, tc.want)
			}
		})
	}
}
