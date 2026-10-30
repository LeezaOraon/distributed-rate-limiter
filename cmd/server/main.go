package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/LeezaOraon/distributed-rate-limiter/internal/middleware"
	"github.com/LeezaOraon/distributed-rate-limiter/internal/ratelimiter"
	"github.com/redis/go-redis/v9"
)

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt64(key string, fallback int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func main() {
	redisAddr := flag.String("redis-addr", envString("REDIS_ADDR", "localhost:6379"), "Redis address")
	listenAddr := flag.String("listen-addr", envString("LISTEN_ADDR", ":8080"), "HTTP listen address")
	limit := flag.Int64("limit", envInt64("RATE_LIMIT", 5), "max requests allowed per window, per client")
	window := flag.Duration("window", envDuration("RATE_WINDOW", 10*time.Second), "sliding window size")
	failPolicyFlag := flag.String("fail-policy", envString("FAIL_POLICY", "open"), `what to do when Redis is unreachable: "open" or "closed"`)
	denyCache := flag.Duration("deny-cache", envDuration("DENY_CACHE", 500*time.Millisecond), "remember a denied key locally for this long, skipping Redis (0 disables)")
	denyCacheSize := flag.Int64("deny-cache-size", envInt64("DENY_CACHE_SIZE", 100_000), "max keys held in the local denial cache")
	flag.Parse()

	var failPolicy middleware.FailPolicy
	switch *failPolicyFlag {
	case "open":
		failPolicy = middleware.FailOpen
	case "closed":
		failPolicy = middleware.FailClosed
	default:
		log.Fatalf(`invalid -fail-policy %q: must be "open" or "closed"`, *failPolicyFlag)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         *redisAddr,
		DialTimeout:  200 * time.Millisecond,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
	})

	var limiter ratelimiter.Allower = ratelimiter.New(rdb, *limit, *window)
	if *denyCache > 0 {
		// A client already over its limit otherwise costs a Redis round trip
		// per request — the limiter works hardest exactly when it is under
		// attack. Caching only denials can make us stricter, never more
		// permissive, so it cannot let a client exceed its limit.
		limiter = ratelimiter.NewNegativeCache(limiter, *denyCache, int(*denyCacheSize))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})

	handler := middleware.RateLimit(limiter, middleware.ByIP, failPolicy)(mux)

	log.Printf("listening on %s (limit: %d requests / %s per client IP, fail-policy=%s, redis=%s, deny-cache=%s)",
		*listenAddr, *limit, *window, *failPolicyFlag, *redisAddr, *denyCache)
	log.Fatal(http.ListenAndServe(*listenAddr, handler))
}
