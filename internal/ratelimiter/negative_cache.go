package ratelimiter

import (
	"context"
	"sync"
	"time"
)

// Allower reports whether a request identified by key is permitted, counting
// it against the limit if so. *Limiter is the Redis-backed implementation;
// *NegativeCache wraps any Allower.
type Allower interface {
	Allow(ctx context.Context, key string) (bool, error)
}

// NegativeCache wraps an Allower and remembers, in process memory, which keys
// were recently denied, so a client hammering the API while already over its
// limit does not cost a Redis round trip per request. Under an attack that is
// the difference between one Redis call per request and roughly one per key
// per blockFor interval.
//
// Only denials are cached, never allowances. A stale entry can therefore only
// make the limiter stricter than Redis would be, never more permissive, so a
// cache hit can't let a client exceed its limit — the worst case is denying a
// request slightly longer than necessary, to a client already over its limit.
// Caching allowances would reintroduce exactly the per-instance drift this
// package exists to prevent.
//
// blockFor should stay well below the limiter's window (sub-second to a couple
// of seconds); it trades how long a denial is remembered against how many
// round trips are saved.
type NegativeCache struct {
	inner      Allower
	blockFor   time.Duration
	maxEntries int

	// mu guards blocked. Reads vastly outnumber writes — every request takes
	// the read lock, only a denial takes the write lock — so RWMutex rather
	// than Mutex.
	mu      sync.RWMutex
	blocked map[string]time.Time // key -> when the local block expires
}

// NewNegativeCache wraps inner so that a denied key is denied locally for
// blockFor without consulting inner again. At most maxEntries keys are
// remembered; beyond that, denials fall through to inner as before.
func NewNegativeCache(inner Allower, blockFor time.Duration, maxEntries int) *NegativeCache {
	return &NegativeCache{
		inner:      inner,
		blockFor:   blockFor,
		maxEntries: maxEntries,
		blocked:    make(map[string]time.Time),
	}
}

func (c *NegativeCache) Allow(ctx context.Context, key string) (bool, error) {
	now := time.Now()

	c.mu.RLock()
	until, found := c.blocked[key]
	c.mu.RUnlock()

	if found && now.Before(until) {
		return false, nil
	}

	allowed, err := c.inner.Allow(ctx, key)
	if err != nil || allowed {
		// Errors are never cached: whether to allow or deny on an unreachable
		// limiter is the caller's FailPolicy decision, made per request.
		return allowed, err
	}

	c.block(key, now)
	return false, nil
}

func (c *NegativeCache) block(key string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.blocked) >= c.maxEntries {
		c.evictExpiredLocked(now)
		if len(c.blocked) >= c.maxEntries {
			// Still full of live entries. Skip caching rather than evict a
			// valid block: the cost is a Redis round trip, not a wrong answer.
			return
		}
	}

	c.blocked[key] = now.Add(c.blockFor)
}

// evictExpiredLocked drops entries whose block has elapsed. Callers must hold
// the write lock. Deleting during range is safe in Go.
func (c *NegativeCache) evictExpiredLocked(now time.Time) {
	for k, until := range c.blocked {
		if !now.Before(until) {
			delete(c.blocked, k)
		}
	}
}

// Len reports how many keys are currently remembered, expired-but-not-yet-
// evicted entries included.
func (c *NegativeCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.blocked)
}
