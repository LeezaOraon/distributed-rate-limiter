package ratelimiter

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrCircuitOpen is returned without calling Redis while the breaker is open.
// The middleware treats it like any limiter error, so the fail policy decides.
var ErrCircuitOpen = errors.New("ratelimiter: circuit open, skipping limiter")

// Breaker wraps an Allower and stops calling it after repeated failures.
//
// Without it, a dead Redis costs every request a full connect timeout before
// the fail policy applies. After threshold consecutive failures the breaker
// opens: calls fail immediately for cooldown, then a single probe is let
// through. A successful probe closes the breaker; a failed one reopens it.
//
// It never makes the limiter more permissive on its own: it only turns slow
// failures into instant ones, and the caller's FailPolicy still decides what
// an error means.
type Breaker struct {
	inner     Allower
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	mu        sync.Mutex
	failures  int
	openUntil time.Time
	probing   bool
}

// NewBreaker opens after threshold consecutive failures and probes again
// after cooldown.
func NewBreaker(inner Allower, threshold int, cooldown time.Duration) *Breaker {
	return &Breaker{inner: inner, threshold: threshold, cooldown: cooldown, now: time.Now}
}

func (b *Breaker) Allow(ctx context.Context, key string) (bool, error) {
	res, err := b.Check(ctx, key)
	return res.Allowed, err
}

func (b *Breaker) Check(ctx context.Context, key string) (Result, error) {
	if !b.admit() {
		return Result{}, ErrCircuitOpen
	}
	res, err := checkAllower(ctx, b.inner, key)
	b.record(err)
	return res, err
}

// admit reports whether a call may go through to the inner limiter.
func (b *Breaker) admit() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.failures < b.threshold {
		return true // closed
	}
	if b.now().Before(b.openUntil) || b.probing {
		return false // open, or a probe is already in flight
	}
	b.probing = true // half-open: this caller is the probe
	return true
}

func (b *Breaker) record(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.probing = false
	switch {
	case err == nil:
		b.failures = 0
	case errors.Is(err, context.Canceled):
		// The client went away; that says nothing about Redis's health.
	default:
		b.failures++
		if b.failures >= b.threshold {
			b.openUntil = b.now().Add(b.cooldown)
		}
	}
}

// checkAllower uses a's decision details when it has them, and falls back to
// a bare allow/deny when it only implements Allower.
func checkAllower(ctx context.Context, a Allower, key string) (Result, error) {
	if c, ok := a.(Checker); ok {
		return c.Check(ctx, key)
	}
	allowed, err := a.Allow(ctx, key)
	return Result{Allowed: allowed}, err
}
