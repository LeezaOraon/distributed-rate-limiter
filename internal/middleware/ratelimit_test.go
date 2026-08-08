package middleware

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/LeezaOraon/distributed-rate-limiter/internal/ratelimiter"
	"github.com/redis/go-redis/v9"
)

func TestRateLimit_Returns429AfterLimit(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	limiter := ratelimiter.New(rdb, 2, time.Minute)

	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := RateLimit(limiter, ByIP, FailOpen)(okHandler)

	// A unique fake RemoteAddr per test run, so repeated runs within the
	// same sliding window never share a bucket with a previous run.
	fakeAddr := fmt.Sprintf("test-client-%d:1234", time.Now().UnixNano())

	newReq := func() *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req = req.WithContext(context.Background())
		req.RemoteAddr = fakeAddr
		return req
	}

	for i := 1; i <= 2; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, newReq())
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d", i, rec.Code)
		}
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, newReq())
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("3rd request: expected 429, got %d", rec.Code)
	}
}

// unreachableLimiter points at a port nothing listens on, with a short
// dial timeout, so Allow() fails fast instead of hanging.
func unreachableLimiter() *ratelimiter.Limiter {
	rdb := redis.NewClient(&redis.Options{
		Addr:        "localhost:1",
		DialTimeout: 50 * time.Millisecond,
		MaxRetries:  -1, // disable go-redis's own retry-with-backoff; DialTimeout alone isn't enough to fail fast
	})
	return ratelimiter.New(rdb, 100, time.Minute)
}

func TestRateLimit_FailOpen_AllowsRequestWhenRedisDown(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := RateLimit(unreachableLimiter(), ByIP, FailOpen)(okHandler)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 (fail-open lets the request through), got %d", rec.Code)
	}
}

func TestRateLimit_FailClosed_RejectsRequestWhenRedisDown(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := RateLimit(unreachableLimiter(), ByIP, FailClosed)(okHandler)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 (fail-closed rejects the request), got %d", rec.Code)
	}
}
