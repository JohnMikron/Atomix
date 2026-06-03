# Changelog

All notable changes to this project will be documented in this file.

## [4.3.0] - 2026-06-03

### Fixed (hardening pass)
- **`total_conflicts` diagnostic silently broken**: The counter was wired to
  `TransactionCoordinator.prepare_commit`, but the live commit path uses
  `Transaction._prepare_inside_lock`, which never incremented it. Result:
  0 conflicts reported even under heavy contention. Now incremented at the
  single point of truth - `TransactionCoordinator.record_conflict()` - and
  called from the prepare path that is actually used.
- **Dead code removed**: `Transaction._prepare()` and
  `TransactionCoordinator.prepare_commit()` were never called from anywhere. Both removed.
- **Reaper shutdown `ValueError: I/O operation on closed file`**: The
  `atexit` race between the Reaper stop and the logging module's own
  teardown was spamming stack traces to the user's terminal. Fixed at
  three layers: a custom `_SafeStreamHandler` that swallows stream errors
  in both `emit` and `handleError`; `_safe_log_info` / `_safe_log_debug`
  / `_safe_log_warning` / `_safe_log_error` helpers used in the Reaper
  loop and `_cleanup`; and the `STMReaper` no longer touches `logger` for
  shutdown bookkeeping.
- **`STMReaper.stop()` could hang the join for the full interval**: The
  loop used `time.sleep(interval)`, so `stop()` with the production
  5-second interval had to time out the 2-second join before reporting
  "stopped" while the reaper was still alive. Replaced with
  `threading.Event.wait()` for interruptible sleep; `stop()` now returns
  in <10 ms even with a 60 s interval and reports the actual thread state
  via its `bool` return value.
- **Honest concurrency model documentation**: The Concurrency model in the
  README and module docstrings now accurately describes the synchronization
  mechanisms. Reads scale, write commits serialize on a single global coordinator lock.

### Added
- New `tests/test_v44_fixes.py` regression suite covering
  conflict counter accuracy, Reaper shutdown safety on closed streams,
  Reaper responsiveness with long intervals, dead-code absence, and
  documentation accuracy.
- Added `tests/test_stm_concurrency.py` stress test validating that the
  serial commit lock prevents lost updates.
- Modularized package split to improve project structure.

## [4.2.0] - 2026-05-28

### Fixed
- **CRITICAL: `Ref._commit_value` race condition**: `old_value` was read before acquiring the lock - now safely read inside the lock.
- **`Ref.read()` missing catch**: Now catches `TimeoutException` and falls back to lock-based read, matching `deref()` behavior.
- **`HistoryManager.compute_max_history` rate calc**: Used fixed 60.0s divisor - now uses actual elapsed time from first access.
- **`PersistentHashMap.__getitem__` None bug**: Previously raised `KeyError` for keys storing `None` - now uses sentinel-based detection.
- **`Atom.compare_and_set` tight coupling**: Added `SeqLock.cas_value()` public API - no more direct access to `_write_lock`/`_value`/`_sequence`.
- **`_cleanup()` silenced errors**: Now logs cleanup errors via `logger.debug()`.
- **`QueueClosedException` not exported**: Added to `core.__all__` and `__init__.py`.
- **`__init__.py` missing 14 exports**: Synced with full `core.__all__` (added `io`, `ensure`, `commute`, `Snapshot`, `get_snapshot_at`, `dump_stm_stats`, `get_history`, `run_concurrent`, `SpinLock`, `SeqLock`, `RWLock`, `ContentionManager`, `HistoryManager`, `STMReaper`).

### Added
- `SeqLock.cas_value()` - value-only CAS with identity/equality comparison.
- New `tests/test_v42_fixes.py` regression test suite.

## [4.1.0] - 2026-05-20

### Fixed
- **SeqLock.read() unbounded busy-wait**: Added `max_spins=10000` upper bound with `TimeoutException` to prevent infinite hang under extreme contention.
- **_notify_watchers exception silencing**: Both `Ref._notify_watchers` and `Atom._notify_watchers` now log warnings via `logger.warning()` instead of silently swallowing errors.
- **Ref.deref() broad except**: Now catches both `HistoryExpiredException` and `TimeoutException` explicitly, matching new SeqLock behavior.
- **Atom.swap tight coupling**: Added `read_seq()` and `read_value()` public API to `SeqLock`. `Atom.swap()` now uses these instead of accessing private `_sequence`/`_value`.
- **STMReaper graceful shutdown**: `STMReaper.stop()` now calls `self.join(timeout=2.0)` to wait for thread exit before logging.
- **Ref.__del__ bare except**: Changed from `except:` to `except Exception:`.
- **monitoring.py broken import**: Fixed `ConflictError` - `ConflictException` (correct public API name).
- **Unnecessary `# type: ignore`**: Removed from `Ref.value` property, `Atom.reset()`, and `Atom.compare_and_set()`.

### Added
- `SeqLock.read_seq()` and `SeqLock.read_value()` public API methods.
- New `tests/test_v41_fixes.py` regression test suite.

## [4.0.0] - 2026-05-10

### Fixed
- **Module docstring version mismatch**: Docstring now correctly points to v4.0.0.
- **Floating `# type: ignore` comments**: Removed misplaced comments.
- **Bare `except:` in `_cleanup()`**: Changed to `except Exception:` so `SystemExit` and `KeyboardInterrupt` propagate correctly.
- **`Atom.swap()` double-read race condition**: Removed redundant write lock acquisition during optimistic reads.
- **Nested `transaction()` depth corner case**: Inner transaction contexts now correctly reuse outer contexts instead of committing prematurely.
- **Unnecessary `# type: ignore` on `defaultdict` assignments**: Changed bare `# type: ignore` to targeted type annotations.

### Changed
- **Version bump to 4.0.0** across all modules and documentation.

### Added
- New `tests/test_v4_fixes.py` covering all 6 bug fixes.

## [3.3.5] - 2026-04-15

### Fixed
- `SeqLock.read()` spinloop GIL safety under high contention using exponential backoff.
- `VersionStamp` ordering issue, focusing on `logical_time` over `physical_time` to prevent false conflict detection.
- `Atom.swap()` encapsulation violation, by introducing `SeqLock.cas()`.
- `STMQueue.put()` and `get()` busy wait loops removed in favor of explicit `wait()` blocks outside of transactions.
- `dosync` snapshot unregistration drift bug fixed during retries.
- `HistoryManager` now caches the `psutil` lazy import to improve performance.
- `PersistentHashMap` hash collision handling using recursive sub-tries and depth tracking.

### Added
- 5 new test classes validating `VersionStamp`, `SeqLock` GIL safety, `HistoryExpiredException`, `dosync` snapshots, and sub-trie collisions.

## [3.3.4] - 2026-04-05

### Fixed
- Removed all stray/floating `# type: ignore` comments.
- Eliminated `old_tx`/`existing_tx` redundant alias in `dosync()`.
- Fixed `TestSTMAvanced` -> `TestSTMAdvanced` class name typo.
- Removed `dosync` double-wrapping in `examples/basic_usage.py`.
- Aligned Python version references to 3.13+ across README.
- Added missing changelog entries.

### Removed
- Removed legacy `setup.py` in favor of `pyproject.toml`.

## [3.3.3] - 2026-03-25

### Fixed
- Removed duplicate `self._start_time` assignment in `_reset_for_retry()`.
- Removed redundant `import random` inside `Atom.swap()`.
- Consolidated `old_tx` / `existing_tx` lookups in `dosync()`.
- Fixed misindented `# type: ignore` comments.
- Fixed late-binding closure bug.

### Changed
- Refactored `benchmarks/benchmark_stm.py` for realistic contention simulation.

### Added
- GitHub Actions CI pipeline.

## [3.3.2] - 2026-03-20

### Changed
- Finalized hardening and synchronized project-wide version to 3.3.2.
- Audited API imports and exports.

## [3.3.1] - 2026-03-15

### Fixed
- **dosync context restoration**: Fixed bug where existing transaction context was lost after a `dosync` block.
- **Atom.swap livelock**: Implemented exponential backoff with jitter in `Atom.swap`.
- **PersistentHashMap.contains**: Corrected logic to properly detect `None` values.
- **_trim_history reentrancy**: Removed redundant lock acquisition.
- **psutil performance**: Optimized `psutil` import to be lazy-loaded.

### Changed
- **Public API**: Renamed internal `_transaction` to public-ready `transaction` context manager.

### Removed
- Removed legacy `Atomix.py` root file.
- Cleaned up development and linting scripts from the repository.

### Added
- Comprehensive `docs/TUTORIAL.md` and `docs/COMPARISON.md`.
- New ecosystem examples: FastAPI, Flask, Monitoring, and Structured Concurrency.
- `py.typed` marker for PEP 561 compliance.
