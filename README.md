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

Try it:

```
curl -i http://localhost:8080/
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

Integration tests in `internal/ratelimiter` and `internal/middleware` run
against a real Redis instance (`docker compose up -d` first) — including
tests that point at an unreachable Redis to verify fail-open/fail-closed
behavior.
