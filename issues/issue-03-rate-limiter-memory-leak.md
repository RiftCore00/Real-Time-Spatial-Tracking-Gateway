## Title
Eliminate the unbounded memory leak in sliding-window rate limiters by implementing time-bucketed eviction with amortized O(1) cleanup

## Difficulty
10/10 — Expert. Estimated effort: 3–4 days for a senior engineer.

## Context
Both `src/rate-limiter.js` (per-message, 1-second window) and `src/conn-rate-limiter.js` (per-IP connection, 60-second window) store a `Map<string, number[]>` of timestamps. Stale entries are only evicted during an active `check()` call for that specific key — if a client connects once and never reconnects, or sends a burst and disconnects, their entry persists in the Map forever. In a fleet-tracking deployment with 10,000+ devices each sending 1 update/second, and with device reconnections creating new client IDs, this is an unbounded O(n) memory leak measured in megabytes per hour.

The `remove()` method on `createRateLimiter` exists but is never called from `server.js` (which itself is broken and doesn't wire up disconnect cleanup for the per-message rate limiter). Even if it were called, `createConnRateLimiter` has no `remove()` method at all.

This issue is **not** about fixing `server.js` — it is about fixing the rate limiters themselves so they are safe by construction, regardless of whether the caller remembers to call `remove()`.

## Problem statement
Implement bounded, self-cleaning sliding-window rate limiters for both modules that:

1. **Guarantee memory is bounded**: No rate limiter instance should hold more than `K` entries where `K` is proportional to the number of active clients (not the historical total).
2. **Evict stale entries proactively**: Entries older than the window (1 second for per-message, 60 seconds for per-IP) must be evicted without requiring a `check()` call for that key.
3. **Maintain O(1) amortized `check()`**: The per-call cost must not degrade below O(1) amortized, even with eviction running.
4. **Expose cleanup methods**: Both limiters must expose a `cleanup()` or `evict()` method that removes all stale entries, callable from a periodic timer or disconnect handler.
5. **Expose metrics**: Both limiters must expose `size` (current entry count) for observability.

## Current behavior
`src/rate-limiter.js` (lines 34–42):
```js
let timestamps = windows.get(clientId);
if (!timestamps) {
  timestamps = [];
  windows.set(clientId, timestamps);
}
while (timestamps.length > 0 && timestamps[0] <= cutoff) {
  timestamps.shift();  // O(n) shift on each check
}
```
The `shift()` is O(n) in the worst case. The Map only shrinks when `check()` is called for that specific key. `remove()` exists but is never called in production.

`src/conn-rate-limiter.js` (lines 14–22): Identical pattern, identical problems, no `remove()` method.

## Required behavior
- Both `createRateLimiter` and `createConnRateLimiter` must not accumulate unbounded entries.
- Each must expose `cleanup()` that iterates and removes entries where the oldest timestamp is older than the window.
- Each must expose `size` getter returning the current Map size.
- The `shift()` pattern must be replaced with a more efficient approach (e.g., ring buffer, or batch cleanup that replaces the array rather than shifting).
- Memory usage must be measurable: a test that creates 100K entries, calls `cleanup()`, and verifies the Map shrinks to ≤ the number of entries with recent timestamps.

## Constraints
- Do not change the public API: `check(clientId)` and `remove(clientId)` on `createRateLimiter`, `check(ip)` on `createConnRateLimiter`.
- Do not add new npm dependencies.
- Do not modify `server.js` or any test file (except adding new test files if needed for the new behavior).
- The sliding window semantics must remain identical: `maxPerSecond` messages in a 1-second window for per-message; `maxPerMinute` connections in a 60-second window for per-IP.
- `check()` must still record the timestamp on success and return `false` on limit exceeded — no behavior change for callers.

## Acceptance criteria
- [ ] `createRateLimiter(5).check("c1")` returns `true` for 5 calls and `false` on the 6th (existing behavior preserved)
- [ ] `createConnRateLimiter(3).check("1.1.1.1")` returns `true` for 3 calls and `false` on the 4th (existing behavior preserved)
- [ ] After inserting 100K entries with old timestamps, calling `cleanup()` reduces Map size to ≤ entries with timestamps within the window
- [ ] `size` getter returns current entry count
- [ ] `rate-limiter.test.js` passes (all 7 tests)
- [ ] `conn-rate-limiter.test.js` passes (all 4 createConnRateLimiter tests — skip the server tests)
- [ ] Memory test: a test that calls `check()` for 50K unique keys, waits >1 second, calls `cleanup()`, and asserts `size` dropped by ≥ 90%
- [ ] No `O(n)` `Array.shift()` in hot path (lint or code review)

## Out of scope
- Changes to `server.js`, `auth.js`, `validator.js`, `room-manager.js`, `index.js`, or `logger.js`.
- Fixing the disconnect cleanup wiring in `server.js` (that is issue 1's responsibility).
- Implementing token-bucket or leaky-bucket algorithms — this is specifically about fixing the sliding-window implementation.

## Hints and references
- A ring buffer (fixed-size circular array with head/tail pointers) replaces `Array.shift()` with an O(1) pointer bump, and `cleanup()` becomes a single pointer advance to the first non-expired entry.
- Alternatively, batch-replace the array: instead of `shift()` one at a time, after `check()` completes, filter the array in one pass and replace the Map value. This is O(k) where k is the number of expired entries, amortized over all `check()` calls in that window.
- The `cleanup()` method is useful for a periodic `setInterval` in `server.js` (e.g., every 10 seconds) to prevent unbounded growth even for keys that are never checked again.
- Consider: what happens if `cleanup()` runs concurrently with `check()` on the same key? In Node.js single-threaded runtime this cannot happen within a single tick, but if `cleanup()` is async-aware or yields, it could. Keep it synchronous and atomic per tick.
