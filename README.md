# Distributed Rate Limiter in Go + Redis

A distributed, sliding-window rate limiter enforced atomically across
multiple API instances via Redis and Lua scripting, exposed as standard
`net/http` middleware.

## Why this exists

A rate limiter running as in-process state (e.g. a Go map) only works if
you run a single instance of your API. Real deployments run several
instances behind a load balancer, so the limit has to be enforced against
**shared state** — here, Redis — with the read-check-increment sequence
done **atomically**, so concurrent requests across different instances
can't race past the limit. See `internal/ratelimiter/sliding_window.lua`
for the atomic check, and the root of the conversation history for the
full sliding-window math walkthrough.

## Architecture

```
Client → [API instance 1] ─┐
Client → [API instance 2] ─┼─→  Redis (shared counters, Lua script = atomic check)
Client → [API instance 3] ─┘
```

- **`internal/ratelimiter`** — the sliding-window algorithm, as an atomic
  Redis Lua script (`sliding_window.lua`) invoked via `EVALSHA` from Go
  (`limiter.go`).
- **`internal/middleware`** — standard `net/http` middleware
  (`func(http.Handler) http.Handler`) wrapping the limiter, with pluggable
  key generation (`ByIP`, `ByHeader`) and a configurable `FailPolicy`
  (`FailOpen` / `FailClosed`) for when Redis is unreachable.
- **`internal/metrics`** — Prometheus telemetry served at `/metrics`:
  `ratelimit_requests_total{outcome}` (allowed / denied / fail_open /
  fail_closed), `ratelimit_limiter_errors_total` and a
  `ratelimit_check_duration_seconds` histogram. `fail_open` is the number of
  requests served *without* being counted because Redis was unreachable.
- **`cmd/server`** — a demo HTTP server wiring it all together, configured
  via CLI flags or environment variables.
- **`cmd/loadtest`** — a goroutine/channel-based concurrent load generator
  that proves the limiter holds an exact limit under real concurrent load,
  not just sequential requests.

## Running it

Start Redis:

```
docker compose up -d
```

Run the server (defaults: 5 requests / 10s per client IP, fail-open):

```
go run ./cmd/server
```

Override via flags or environment variables:

```
go run ./cmd/server -limit=100 -window=1m -fail-policy=closed
REDIS_ADDR=localhost:6379 RATE_LIMIT=100 RATE_WINDOW=1m FAIL_POLICY=closed go run ./cmd/server
```

| Flag             | Env var       | Default           | Meaning                                  |
|------------------|---------------|-------------------|-------------------------------------------|
| `-redis-addr`    | `REDIS_ADDR`  | `localhost:6379`  | Redis address                             |
| `-listen-addr`   | `LISTEN_ADDR` | `:8080`           | HTTP listen address                       |
| `-limit`         | `RATE_LIMIT`  | `5`               | Max requests per window, per client       |
| `-window`        | `RATE_WINDOW` | `10s`             | Sliding window size                       |
| `-fail-policy`   | `FAIL_POLICY` | `open`            | `open` or `closed` when Redis is down     |
| `-key-by`        | `KEY_BY`      | `ip`              | `ip` or `header:<Name>` (e.g. `header:X-API-Key`) |
| `-deny-cache`    | `DENY_CACHE`  | `500ms`           | Remember a denied key locally (0 disables) |
| `-breaker-failures` | `BREAKER_FAILURES` | `5`        | Consecutive Redis failures before the circuit opens (0 disables) |
| `-breaker-cooldown` | `BREAKER_COOLDOWN` | `2s`       | How long the circuit stays open before probing Redis |

Responses carry `X-RateLimit-Limit`, `X-RateLimit-Remaining` and
`X-RateLimit-Reset` (seconds until the current window bucket ends); a `429`
also carries a `Retry-After` computed from the sliding window, not a fixed 1.
When Redis is down, a circuit breaker makes the fail policy apply instantly
instead of after connection timeouts.

Try it:

```
curl -i http://localhost:8080/
```

Load-test and benchmark (`cmd/bench` reports throughput, p50/p99, status counts):

```
go run ./cmd/bench -urls=http://127.0.0.1:8081/,http://127.0.0.1:8082/ -n 20000 -c 1000
go test -run='^$' -bench=. ./internal/ratelimiter
```

Prove correctness under concurrent load:

```
go run ./cmd/loadtest -n 30 -c 15
# sent=30 allowed(200)=5 denied(429)=25 other=0
```

## Tests

```
go test ./...
```

`TestLimiter_ExactlyLimitAdmittedAcrossInstancesUnderContention` fires 1,000
goroutines over three independent limiters at one key and asserts exactly the
limit is admitted. CI (`.github/workflows/ci.yml`) runs everything with
`-race -cover` against a Redis service container.

Integration tests in `internal/ratelimiter` and `internal/middleware` run
against a real Redis instance (`docker compose up -d` first) — including
tests that point at an unreachable Redis to verify fail-open/fail-closed
behavior.
