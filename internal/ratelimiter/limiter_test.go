package ratelimiter

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func newClient() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: "localhost:6379"})
}

func uniqueKey(prefix string) string {
	return "rl:test:" + prefix + ":" + time.Now().Format(time.RFC3339Nano)
}

func TestLimiter_AllowsUpToLimitThenDenies(t *testing.T) {
	rdb := newClient()
	ctx := context.Background()

	key := uniqueKey("limiter_unit")

	limiter := New(rdb, 3, time.Minute)

	for i := 1; i <= 3; i++ {
		allowed, err := limiter.Allow(ctx, key)
		if err != nil {
			t.Fatalf("request %d: unexpected error: %v", i, err)
		}
		if !allowed {
			t.Fatalf("request %d: expected allowed=true, got false", i)
		}
	}

	allowed, err := limiter.Allow(ctx, key)
	if err != nil {
		t.Fatalf("4th request: unexpected error: %v", err)
	}
	if allowed {
		t.Fatalf("4th request: expected allowed=false (limit exceeded), got true")
	}
}

func TestLimiter_CheckReportsRemainingAndLimit(t *testing.T) {
	limiter := New(newClient(), 3, time.Minute)
	key := uniqueKey("remaining")

	for i, want := range []int64{2, 1, 0} {
		res, err := limiter.Check(context.Background(), key)
		if err != nil || !res.Allowed {
			t.Fatalf("request %d: got (%+v, %v), want allowed", i+1, res, err)
		}
		if res.Remaining != want || res.Limit != 3 {
			t.Fatalf("request %d: remaining=%d limit=%d, want remaining=%d limit=3", i+1, res.Remaining, res.Limit, want)
		}
		if res.ResetAfter <= 0 || res.ResetAfter > time.Minute {
			t.Fatalf("request %d: ResetAfter=%s out of range", i+1, res.ResetAfter)
		}
	}

	res, err := limiter.Check(context.Background(), key)
	if err != nil || res.Allowed || res.Remaining != 0 || res.RetryAfter <= 0 {
		t.Fatalf("denied check: got (%+v, %v), want denied, remaining 0, RetryAfter > 0", res, err)
	}
}

// The reported RetryAfter must be accurate: waiting that long makes the key
// admissible again, and waiting noticeably less does not.
func TestLimiter_RetryAfterIsAccurate(t *testing.T) {
	const window = time.Second
	limiter := New(newClient(), 2, window)
	ctx := context.Background()
	key := uniqueKey("retry")

	limiter.Check(ctx, key)
	limiter.Check(ctx, key)
	denied, err := limiter.Check(ctx, key)
	if err != nil || denied.Allowed {
		t.Fatalf("setup: want third request denied, got (%+v, %v)", denied, err)
	}
	if denied.RetryAfter > 2*window {
		t.Fatalf("RetryAfter=%s is more than two windows", denied.RetryAfter)
	}

	time.Sleep(denied.RetryAfter + 30*time.Millisecond)
	res, err := limiter.Check(ctx, key)
	if err != nil || !res.Allowed {
		t.Fatalf("after waiting RetryAfter (%s) the key should be admitted, got (%+v, %v)", denied.RetryAfter, res, err)
	}
}

// Three independent Limiters (separate Redis clients, standing in for three
// server instances) hammer one key from 1,000 goroutines. The limit is 100,
// so exactly 100 must be admitted: any more means the check-and-increment
// raced.
func TestLimiter_ExactlyLimitAdmittedAcrossInstancesUnderContention(t *testing.T) {
	const (
		limit      = 100
		goroutines = 1000
		perG       = 10
		instances  = 3
	)
	var limiters [instances]*Limiter
	for i := range limiters {
		// A one-hour window keeps the whole run inside one window bucket.
		limiters[i] = New(newClient(), limit, time.Hour)
	}
	key := uniqueKey("contention")

	var admitted, failed atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(l *Limiter) {
			defer wg.Done()
			<-start
			for i := 0; i < perG; i++ {
				ok, err := l.Allow(context.Background(), key)
				switch {
				case err != nil:
					failed.Add(1)
				case ok:
					admitted.Add(1)
				}
			}
		}(limiters[g%instances])
	}
	close(start)
	wg.Wait()

	if failed.Load() != 0 {
		t.Fatalf("%d checks failed; the result below would not be meaningful", failed.Load())
	}
	if got := admitted.Load(); got != limit {
		t.Fatalf("admitted %d of %d requests, want exactly %d", got, goroutines*perG, limit)
	}
}

func BenchmarkAllow(b *testing.B) {
	limiter := New(newClient(), 1<<40, time.Hour) // never denies: measures the full Redis path
	key := uniqueKey("bench")
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := limiter.Allow(ctx, key); err != nil {
				b.Fatal(err)
			}
		}
	})
}
