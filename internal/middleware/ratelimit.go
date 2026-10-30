package middleware

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/LeezaOraon/distributed-rate-limiter/internal/ratelimiter"
)

// KeyFunc extracts the rate-limit bucket key for an incoming request.
type KeyFunc func(r *http.Request) string

// ByIP keys on the direct TCP peer address. Does not consult
// X-Forwarded-For: that header is attacker-controlled input unless a
// trusted reverse proxy overwrites it, and trusting it blindly here would
// let a client bypass its limit by sending a different fake IP each time.
func ByIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return "ip:" + r.RemoteAddr
	}
	return "ip:" + host
}

// ByHeader keys on the value of the given request header, e.g. an API key.
func ByHeader(header string) KeyFunc {
	return func(r *http.Request) string {
		return "hdr:" + header + ":" + r.Header.Get(header)
	}
}

// FailPolicy decides what happens to a request when the limiter itself
// cannot be reached (e.g. Redis is down or timed out).
type FailPolicy int

const (
	// FailOpen lets the request through when Redis is unreachable,
	// prioritizing API availability over enforcing the limit.
	FailOpen FailPolicy = iota
	// FailClosed rejects the request when Redis is unreachable,
	// prioritizing the limit guarantee over API availability.
	FailClosed
)

// Outcome labels what the middleware did with one request.
type Outcome string

const (
	OutcomeAllowed    Outcome = "allowed"     // limiter admitted the request
	OutcomeDenied     Outcome = "denied"      // over the limit, answered 429
	OutcomeFailOpen   Outcome = "fail_open"   // limiter unreachable, request served uncounted
	OutcomeFailClosed Outcome = "fail_closed" // limiter unreachable, answered 503
)

// Recorder receives one observation per request: what happened and how long
// the limiter check took. It is optional; see WithRecorder.
type Recorder interface {
	Observe(outcome Outcome, checkDuration time.Duration)
}

// Option customizes RateLimit.
type Option func(*config)

type config struct {
	recorder Recorder
}

// WithRecorder reports every decision to r, e.g. for Prometheus metrics.
func WithRecorder(r Recorder) Option {
	return func(c *config) { c.recorder = r }
}

// RateLimit returns middleware that enforces limiter's sliding-window
// limit per key, as produced by keyFunc. policy controls what happens to
// a request when limiter itself cannot be reached.
//
// limiter is an interface so a *ratelimiter.Limiter can be wrapped —
// e.g. in a *ratelimiter.NegativeCache — without this package caring. If it
// also implements ratelimiter.Checker, responses carry X-RateLimit-* headers
// and an accurate Retry-After; otherwise Retry-After falls back to 1 second.
func RateLimit(limiter ratelimiter.Allower, keyFunc KeyFunc, policy FailPolicy, opts ...Option) func(http.Handler) http.Handler {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}
	observe := func(o Outcome, d time.Duration) {
		if cfg.recorder != nil {
			cfg.recorder.Observe(o, d)
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyFunc(r)

			start := time.Now()
			res, err := check(r.Context(), limiter, key)
			took := time.Since(start)

			if err != nil {
				if policy == FailClosed {
					observe(OutcomeFailClosed, took)
					w.Header().Set("Retry-After", "1")
					http.Error(w, "rate limiter unavailable", http.StatusServiceUnavailable)
					return
				}
				observe(OutcomeFailOpen, took)
				next.ServeHTTP(w, r)
				return
			}

			setRateLimitHeaders(w, res)

			if !res.Allowed {
				observe(OutcomeDenied, took)
				w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds(res.RetryAfter)))
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}

			observe(OutcomeAllowed, took)
			next.ServeHTTP(w, r)
		})
	}
}

// check asks the limiter for a decision, with details when it can give them.
func check(ctx context.Context, l ratelimiter.Allower, key string) (ratelimiter.Result, error) {
	if c, ok := l.(ratelimiter.Checker); ok {
		return c.Check(ctx, key)
	}
	allowed, err := l.Allow(ctx, key)
	return ratelimiter.Result{Allowed: allowed}, err
}

// setRateLimitHeaders adds the de-facto X-RateLimit-* headers when the
// limiter reported its limit. X-RateLimit-Reset is seconds until the current
// window bucket ends.
func setRateLimitHeaders(w http.ResponseWriter, res ratelimiter.Result) {
	if res.Limit == 0 {
		return
	}
	h := w.Header()
	h.Set("X-RateLimit-Limit", strconv.FormatInt(res.Limit, 10))
	h.Set("X-RateLimit-Remaining", strconv.FormatInt(res.Remaining, 10))
	h.Set("X-RateLimit-Reset", strconv.Itoa(ceilSeconds(res.ResetAfter)))
}

// retryAfterSeconds rounds up to whole seconds, with a floor of 1.
func retryAfterSeconds(d time.Duration) int {
	if s := ceilSeconds(d); s > 1 {
		return s
	}
	return 1
}

func ceilSeconds(d time.Duration) int {
	return int((d + time.Second - 1) / time.Second)
}
