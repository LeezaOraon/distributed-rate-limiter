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
	now := time.Now().UnixMilli()

	res, err := slidingWindowScript.Run(
		ctx, l.rdb,
		[]string{key},
		now, l.window.Milliseconds(), l.limit,
	).Result()
	if err != nil {
		return false, err
	}

	values, ok := res.([]interface{})
	if !ok || len(values) != 2 {
		return false, fmt.Errorf("ratelimiter: unexpected script result: %v", res)
	}

	allowed, ok := values[0].(int64)
	if !ok {
		return false, fmt.Errorf("ratelimiter: unexpected 'allowed' type: %v", values[0])
	}

	return allowed == 1, nil
}
