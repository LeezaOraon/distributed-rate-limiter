package ratelimiter

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestLimiter_AllowsUpToLimitThenDenies(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()

	key := "rl:test:limiter_unit:" + time.Now().Format(time.RFC3339Nano)

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
