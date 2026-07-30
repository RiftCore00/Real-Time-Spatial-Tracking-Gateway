# TODO: Fix unbounded memory leak in sliding-window rate limiters

## Steps

- [x] Read and understand source files (rate-limiter.js, conn-rate-limiter.js)
- [x] Read existing test files (rate-limiter.test.js, conn-rate-limiter.test.js)
- [x] Issue analysis complete
- [x] **Edit `src/rate-limiter.js`**:
  - [x] Replace `shift()` loop with batch `filter()` in `check()`
  - [x] Add `cleanup()` method (iterate all keys, filter stale timestamps)
  - [x] Add `size` getter
- [x] **Edit `src/conn-rate-limiter.js`**:
  - [x] Replace `shift()` loop with batch `filter()` in `check()`
  - [x] Add `cleanup()` method (iterate all keys, filter stale timestamps)
  - [x] Add `size` getter
  - [x] Add `remove(ip)` method for API parity
- [ ] Create PR

