package ratelimiter

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed sliding_window.lua
var slidingWindowSource string

var slidingWindowScript = redis.NewScript(slidingWindowSource)

// Result is the outcome of one limit check, with what a client needs to
// know to back off: how much budget is left and when to try again.
type Result struct {
	Allowed bool
	// Limit is the maximum number of requests per window.
	Limit int64
	// Remaining is how many more requests would currently be admitted.
	Remaining int64
	// ResetAfter is the time until the current window bucket ends.
	ResetAfter time.Duration
	// RetryAfter is, for a denied request, how long until the key would be
	// admitted again if no more requests arrive. Zero when allowed.
	RetryAfter time.Duration
}

// Checker is an Allower that also reports the details of its decision.
type Checker interface {
	Allower
	Check(ctx context.Context, key string) (Result, error)
}

// Limiter enforces a sliding-window request limit backed by Redis.
type Limiter struct {
	rdb    *redis.Client
	limit  int64
	window time.Duration
}

func New(rdb *redis.Client, limit int64, window time.Duration) *Limiter {
	return &Limiter{rdb: rdb, limit: limit, window: window}
}

// Allow reports whether a request identified by key is permitted right now,
// and atomically counts it against the limit if so.
func (l *Limiter) Allow(ctx context.Context, key string) (bool, error) {
	res, err := l.Check(ctx, key)
	return res.Allowed, err
}

// Check is Allow plus the details needed for rate-limit response headers.
// It is still a single Redis round trip.
func (l *Limiter) Check(ctx context.Context, key string) (Result, error) {
	nowMs := time.Now().UnixMilli()
	windowMs := l.window.Milliseconds()

	res, err := slidingWindowScript.Run(
		ctx, l.rdb,
		[]string{key},
		nowMs, windowMs, l.limit,
	).Result()
	if err != nil {
		return Result{}, err
	}

	values, ok := res.([]interface{})
	if !ok || len(values) != 3 {
		return Result{}, fmt.Errorf("ratelimiter: unexpected script result: %v", res)
	}

	var n [3]int64
	for i, v := range values {
		n[i], ok = v.(int64)
		if !ok {
			return Result{}, fmt.Errorf("ratelimiter: unexpected script value %d type: %v", i, v)
		}
	}
	allowed, count, retryMs := n[0] == 1, n[1], n[2]

	remaining := l.limit - count
	if remaining < 0 {
		remaining = 0
	}
	return Result{
		Allowed:    allowed,
		Limit:      l.limit,
		Remaining:  remaining,
		ResetAfter: time.Duration(windowMs-nowMs%windowMs) * time.Millisecond,
		RetryAfter: time.Duration(retryMs) * time.Millisecond,
	}, nil
}
