package ratelimiter

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errRedisDown = errors.New("redis down")

// newTestBreaker returns a breaker with a controllable clock.
func newTestBreaker(inner Allower, threshold int, cooldown time.Duration) (*Breaker, *time.Time) {
	now := time.Unix(1000, 0)
	b := NewBreaker(inner, threshold, cooldown)
	b.now = func() time.Time { return now }
	return b, &now
}

func TestBreaker_OpensAfterThresholdAndSkipsInner(t *testing.T) {
	inner := &fakeAllower{err: errRedisDown}
	b, _ := newTestBreaker(inner, 3, time.Second)

	for i := 0; i < 3; i++ {
		if _, err := b.Allow(context.Background(), "k"); !errors.Is(err, errRedisDown) {
			t.Fatalf("call %d: want inner error, got %v", i, err)
		}
	}
	for i := 0; i < 5; i++ {
		if _, err := b.Allow(context.Background(), "k"); !errors.Is(err, ErrCircuitOpen) {
			t.Fatalf("open call %d: want ErrCircuitOpen, got %v", i, err)
		}
	}
	if got := inner.calls.Load(); got != 3 {
		t.Fatalf("inner called %d times, want 3 (open breaker must not call it)", got)
	}
}

func TestBreaker_ProbesAfterCooldownAndCloses(t *testing.T) {
	inner := &fakeAllower{err: errRedisDown}
	b, now := newTestBreaker(inner, 2, time.Second)
	b.Allow(context.Background(), "k")
	b.Allow(context.Background(), "k")

	*now = now.Add(time.Second) // cooldown over
	inner.err = nil
	inner.allowed = true

	if ok, err := b.Allow(context.Background(), "k"); err != nil || !ok {
		t.Fatalf("probe: got (%v, %v), want (true, nil)", ok, err)
	}
	// closed again: further calls reach the inner limiter
	before := inner.calls.Load()
	b.Allow(context.Background(), "k")
	if inner.calls.Load() != before+1 {
		t.Fatal("breaker did not close after a successful probe")
	}
}

func TestBreaker_FailedProbeReopens(t *testing.T) {
	inner := &fakeAllower{err: errRedisDown}
	b, now := newTestBreaker(inner, 2, time.Second)
	b.Allow(context.Background(), "k")
	b.Allow(context.Background(), "k")

	*now = now.Add(time.Second)
	if _, err := b.Allow(context.Background(), "k"); !errors.Is(err, errRedisDown) {
		t.Fatalf("probe should reach inner and fail, got %v", err)
	}
	if _, err := b.Allow(context.Background(), "k"); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("after a failed probe the breaker must be open, got %v", err)
	}
}

func TestBreaker_SuccessResetsFailureCount(t *testing.T) {
	inner := &fakeAllower{err: errRedisDown}
	b, _ := newTestBreaker(inner, 3, time.Second)
	b.Allow(context.Background(), "k")
	b.Allow(context.Background(), "k")
	inner.err = nil
	b.Allow(context.Background(), "k") // success: count back to 0
	inner.err = errRedisDown
	b.Allow(context.Background(), "k")
	b.Allow(context.Background(), "k")

	if _, err := b.Allow(context.Background(), "k"); !errors.Is(err, errRedisDown) {
		t.Fatalf("2 failures after a success must not open a threshold-3 breaker, got %v", err)
	}
}

func TestBreaker_CanceledContextDoesNotCount(t *testing.T) {
	inner := &fakeAllower{err: context.Canceled}
	b, _ := newTestBreaker(inner, 2, time.Second)
	for i := 0; i < 10; i++ {
		b.Allow(context.Background(), "k")
	}
	if _, err := b.Allow(context.Background(), "k"); errors.Is(err, ErrCircuitOpen) {
		t.Fatal("client cancellations must not open the breaker")
	}
}

func TestBreaker_OnlyOneProbeAtATime(t *testing.T) {
	inner := &fakeAllower{err: errRedisDown}
	b, now := newTestBreaker(inner, 1, time.Second)
	b.Allow(context.Background(), "k")
	*now = now.Add(time.Second)

	if !b.admit() {
		t.Fatal("first caller after cooldown should be the probe")
	}
	if b.admit() {
		t.Fatal("a second caller must not pass while the probe is in flight")
	}
}
