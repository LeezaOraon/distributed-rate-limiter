package ratelimiter

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeAllower stands in for the Redis-backed Limiter so these tests need no
// Redis, and can count exactly how many calls reached the inner limiter.
type fakeAllower struct {
	allowed bool
	err     error
	calls   atomic.Int64
}

func (f *fakeAllower) Allow(context.Context, string) (bool, error) {
	f.calls.Add(1)
	return f.allowed, f.err
}

func TestNegativeCache_DeniedKeySkipsInnerLimiter(t *testing.T) {
	inner := &fakeAllower{allowed: false}
	c := NewNegativeCache(inner, time.Minute, 1000)
	ctx := context.Background()

	for i := 1; i <= 100; i++ {
		allowed, err := c.Allow(ctx, "k")
		if err != nil {
			t.Fatalf("request %d: unexpected error: %v", i, err)
		}
		if allowed {
			t.Fatalf("request %d: expected denied", i)
		}
	}

	// The first call consults the inner limiter; the other 99 are served from
	// the local cache. That collapse is the entire point of the optimization.
	if got := inner.calls.Load(); got != 1 {
		t.Fatalf("expected 1 inner call for 100 denied requests, got %d", got)
	}
}

func TestNegativeCache_AllowedKeyIsNeverCached(t *testing.T) {
	inner := &fakeAllower{allowed: true}
	c := NewNegativeCache(inner, time.Minute, 1000)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		allowed, err := c.Allow(ctx, "k")
		if err != nil || !allowed {
			t.Fatalf("request %d: expected allowed, got allowed=%v err=%v", i, allowed, err)
		}
	}

	// Every allowed request must reach Redis. Caching an allowance would let
	// each instance independently admit traffic — the bug this whole package
	// exists to prevent.
	if got := inner.calls.Load(); got != 5 {
		t.Fatalf("expected 5 inner calls (allowances must never be cached), got %d", got)
	}
}

func TestNegativeCache_BlockExpires(t *testing.T) {
	inner := &fakeAllower{allowed: false}
	c := NewNegativeCache(inner, 20*time.Millisecond, 1000)
	ctx := context.Background()

	c.Allow(ctx, "k")
	c.Allow(ctx, "k") // cached, no inner call
	if got := inner.calls.Load(); got != 1 {
		t.Fatalf("expected 1 inner call before expiry, got %d", got)
	}

	time.Sleep(30 * time.Millisecond)

	c.Allow(ctx, "k")
	if got := inner.calls.Load(); got != 2 {
		t.Fatalf("expected the expired block to consult inner again, got %d calls", got)
	}
}

func TestNegativeCache_ErrorsAreNotCached(t *testing.T) {
	inner := &fakeAllower{allowed: false, err: errors.New("redis down")}
	c := NewNegativeCache(inner, time.Minute, 1000)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := c.Allow(ctx, "k"); err == nil {
			t.Fatalf("request %d: expected the inner error to propagate", i)
		}
	}

	// An unreachable limiter is the caller's FailPolicy decision, made fresh
	// per request — caching it would silently pin every request to one policy
	// outcome for blockFor.
	if got := inner.calls.Load(); got != 3 {
		t.Fatalf("expected 3 inner calls (errors must not be cached), got %d", got)
	}
}

func TestNegativeCache_RespectsMaxEntries(t *testing.T) {
	inner := &fakeAllower{allowed: false}
	const max = 10
	c := NewNegativeCache(inner, time.Minute, max)
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		c.Allow(ctx, fmt.Sprintf("key-%d", i))
	}

	if got := c.Len(); got > max {
		t.Fatalf("cache grew past maxEntries: got %d, want <= %d", got, max)
	}
}

// Run with -race. This is the only shared mutable state in the project, so it
// is the only place a data race can exist.
func TestNegativeCache_ConcurrentAccess(t *testing.T) {
	inner := &fakeAllower{allowed: false}
	c := NewNegativeCache(inner, 5*time.Millisecond, 50)
	ctx := context.Background()

	var wg sync.WaitGroup
	for w := 0; w < 50; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				// Overlapping keys force contention on the same map entries,
				// and the short blockFor forces eviction to run concurrently
				// with reads.
				c.Allow(ctx, fmt.Sprintf("key-%d", i%20))
			}
		}(w)
	}
	wg.Wait()
}
