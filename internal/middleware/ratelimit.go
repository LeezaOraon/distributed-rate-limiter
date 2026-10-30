package middleware

import (
	"net"
	"net/http"

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

// RateLimit returns middleware that enforces limiter's sliding-window
// limit per key, as produced by keyFunc. policy controls what happens to
// a request when limiter itself cannot be reached.
//
// limiter is an interface so a *ratelimiter.Limiter can be wrapped —
// e.g. in a *ratelimiter.NegativeCache — without this package caring.
func RateLimit(limiter ratelimiter.Allower, keyFunc KeyFunc, policy FailPolicy) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyFunc(r)

			allowed, err := limiter.Allow(r.Context(), key)
			if err != nil {
				if policy == FailClosed {
					w.Header().Set("Retry-After", "1")
					http.Error(w, "rate limiter unavailable", http.StatusServiceUnavailable)
					return
				}
				next.ServeHTTP(w, r)
				return
			}

			if !allowed {
				w.Header().Set("Retry-After", "1")
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
