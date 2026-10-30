package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/LeezaOraon/distributed-rate-limiter/internal/metrics"
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

// parseKeyBy turns the -key-by value into the middleware's KeyFunc.
func parseKeyBy(v string) (middleware.KeyFunc, error) {
	switch {
	case v == "ip":
		return middleware.ByIP, nil
	case strings.HasPrefix(v, "header:") && len(v) > len("header:"):
		return middleware.ByHeader(strings.TrimPrefix(v, "header:")), nil
	}
	return nil, fmt.Errorf(`invalid -key-by %q: must be "ip" or "header:<Name>"`, v)
}

func main() {
	redisAddr := flag.String("redis-addr", envString("REDIS_ADDR", "localhost:6379"), "Redis address")
	listenAddr := flag.String("listen-addr", envString("LISTEN_ADDR", ":8080"), "HTTP listen address")
	limit := flag.Int64("limit", envInt64("RATE_LIMIT", 5), "max requests allowed per window, per client")
	window := flag.Duration("window", envDuration("RATE_WINDOW", 10*time.Second), "sliding window size")
	failPolicyFlag := flag.String("fail-policy", envString("FAIL_POLICY", "open"), `what to do when Redis is unreachable: "open" or "closed"`)
	denyCache := flag.Duration("deny-cache", envDuration("DENY_CACHE", 500*time.Millisecond), "remember a denied key locally for this long, skipping Redis (0 disables)")
	keyBy := flag.String("key-by", envString("KEY_BY", "ip"), `what identifies a client: "ip" or "header:<Name>" (e.g. header:X-API-Key)`)
	breakerFailures := flag.Int("breaker-failures", int(envInt64("BREAKER_FAILURES", 5)), "consecutive Redis failures before the circuit opens (0 disables)")
	breakerCooldown := flag.Duration("breaker-cooldown", envDuration("BREAKER_COOLDOWN", 2*time.Second), "how long the circuit stays open before probing Redis again")
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

	keyFunc, err := parseKeyBy(*keyBy)
	if err != nil {
		log.Fatal(err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: *redisAddr,
		// go-redis retries a failed command with backoff, and re-dials a dead
		// server several times. With Redis down that made every request wait
		// ~1.7s before the fail policy applied. One attempt, then decide.
		MaxRetries:   -1,
		DialTimeout:  200 * time.Millisecond,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
	})

	var limiter ratelimiter.Allower = ratelimiter.New(rdb, *limit, *window)
	if *breakerFailures > 0 {
		// Innermost, so a denial cache hit never counts as a Redis success.
		limiter = ratelimiter.NewBreaker(limiter, *breakerFailures, *breakerCooldown)
	}
	if *denyCache > 0 {
		// A client already over its limit otherwise costs a Redis round trip
		// per request — the limiter works hardest exactly when it is under
		// attack. Caching only denials can make us stricter, never more
		// permissive, so it cannot let a client exceed its limit.
		limiter = ratelimiter.NewNegativeCache(limiter, *denyCache, int(*denyCacheSize))
	}

	m := metrics.New()

	app := http.NewServeMux()
	app.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok\n"))
	})

	// /metrics sits outside the limiter so scraping is never rate limited.
	handler := http.NewServeMux()
	handler.Handle("/metrics", m.Handler())
	handler.Handle("/", middleware.RateLimit(limiter, keyFunc, failPolicy, middleware.WithRecorder(m))(app))

	log.Printf("listening on %s (limit: %d requests / %s per client, key-by=%s, fail-policy=%s, redis=%s, deny-cache=%s, metrics=/metrics)",
		*listenAddr, *limit, *window, *keyBy, *failPolicyFlag, *redisAddr, *denyCache)
	log.Fatal(http.ListenAndServe(*listenAddr, handler))
}
