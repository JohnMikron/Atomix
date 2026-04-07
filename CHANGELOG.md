# Changelog

All notable changes to this project will be documented in this file.

## [4.3.0] - 2026-04-03

### Fixed
- **`STMReaper` atexit.register**: Reaper thread now registered with `atexit` on creation and reset — ensures graceful shutdown on SIGTERM/crash.
- **`SeqLock.read()` max_spins 10000→100000**: Prevents false `TimeoutException` under legitimate high-contention (50+ threads).
- **`Atom.swap()` max retries**: Added explicit test verifying `CommitException` is raised when retries exceeded.
- **`test_v41_fixes.py` docstring**: Fixed misleading docstring that said "v4.2.0" but covers v4.1.0 fixes.
- **`test_v41_fixes.py` naming**: Fixed `test_version_is_4_1_0` → `test_version_is_current` to avoid version confusion.
- **`monitoring.py` import test**: Added dedicated test verifying the monitoring module imports without error.
- **Full `_notify_watchers` audit**: Verified all 6 call sites in core.py use proper `logger.warning()` — zero silent `except: pass`.

## [4.2.0] - 2026-04-03

### Fixed
- **CRITICAL: `Ref._commit_value` race condition**: `old_value` was read before acquiring the lock — now safely read inside the lock.
- **`Ref.read()` missing catch**: Now catches `TimeoutException` and falls back to lock-based read, matching `deref()` behavior.
- **`HistoryManager.compute_max_history` rate calc**: Used fixed 60.0s divisor — now uses actual elapsed time from first access.
- **`PersistentHashMap.__getitem__` None bug**: Previously raised `KeyError` for keys storing `None` — now uses sentinel-based detection.
- **`Atom.compare_and_set` tight coupling**: Added `SeqLock.cas_value()` public API — no more direct access to `_write_lock`/`_value`/`_sequence`.
- **`_cleanup()` silenced errors**: Now logs cleanup errors via `logger.debug()`.
- **`QueueClosedException` not exported**: Added to `core.__all__` and `__init__.py`.
- **`__init__.py` missing 14 exports**: Synced with full `core.__all__` (added `io`, `ensure`, `commute`, `Snapshot`, `get_snapshot_at`, `dump_stm_stats`, `get_history`, `run_concurrent`, `SpinLock`, `SeqLock`, `RWLock`, `ContentionManager`, `HistoryManager`, `STMReaper`).

### Added
- `SeqLock.cas_value()` — value-only CAS with identity/equality comparison.
- New `tests/test_v42_fixes.py` regression test suite.

## [4.1.0] - 2026-04-03

### Fixed
- **SeqLock.read() unbounded busy-wait**: Added `max_spins=10000` upper bound with `TimeoutException` to prevent infinite hang under extreme contention.
- **_notify_watchers exception silencing**: Both `Ref._notify_watchers` and `Atom._notify_watchers` now log warnings via `logger.warning()` instead of silently swallowing errors.
- **Ref.deref() broad except**: Now catches both `HistoryExpiredException` and `TimeoutException` explicitly, matching new SeqLock behavior.
- **Atom.swap tight coupling**: Added `read_seq()` and `read_value()` public API to `SeqLock`. `Atom.swap()` now uses these instead of accessing private `_sequence`/`_value`.
- **STMReaper graceful shutdown**: `STMReaper.stop()` now calls `self.join(timeout=2.0)` to wait for thread exit before logging.
- **Ref.__del__ bare except**: Changed from `except:` to `except Exception:`.
- **monitoring.py broken import**: Fixed `ConflictError` → `ConflictException` (correct public API name).
- **Unnecessary `# type: ignore`**: Removed from `Ref.value` property, `Atom.reset()`, and `Atom.compare_and_set()`.

### Added
- `SeqLock.read_seq()` and `SeqLock.read_value()` public API methods.
- New `tests/test_v41_fixes.py` regression test suite.

## [4.0.0] - 2026-04-03

### Fixed
- **Module docstring version mismatch**: Docstring incorrectly said v3.3.4 while `__version__` was 3.3.5. All now consistently say 4.0.0.
- **Floating `# type: ignore` comments**: Removed 5 misplaced `# type: ignore` comments that were outside function bodies or inside section header comments (between `rest()`/`_slice()`, in `Snapshot and History`, `Diagnostics and Monitoring`, `Public API` headers, and after `_cleanup()`).
- **Bare `except:` in `_cleanup()`**: Changed to `except Exception:` so `SystemExit` and `KeyboardInterrupt` propagate correctly during interpreter shutdown.
- **`Atom.swap()` double-read race condition**: Removed redundant `with self._seqlock._write_lock` block that acquired the write lock, read seq/value, then immediately re-read them outside the lock — creating a race condition. Now uses a clean lock-free optimistic read with odd-sequence spin-wait before CAS.
- **Nested `transaction()` depth corner case**: When an outer transaction already exists, `transaction()` now yields the existing transaction directly instead of entering `with tx:` (which incremented `_depth` and caused a premature commit when the inner context exited).
- **Unnecessary `# type: ignore` on `defaultdict` assignments**: Changed bare `# type: ignore` to targeted `# type: ignore[assignment]` on `ContentionManager._contention_scores` and `HistoryManager._access_patterns` for Pyre compatibility.

### Changed
- **Version bump to 4.0.0** across `core.py`, `__init__.py`, `pyproject.toml`, and all documentation.
- Removed `# type: ignore` from `atexit.register(_cleanup)` call.

### Added
- New `tests/test_v4_fixes.py` covering all 6 bug fixes with targeted regression tests.

## [3.3.5] - 2026-03-17

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

## [3.3.4] - 2026-03-14

### Fixed
- Removed all stray/floating `# type: ignore` comments (5 locations in `core.py`).
- Eliminated `old_tx`/`existing_tx` redundant alias in `dosync()`.
- Fixed `TestSTMAvanced` → `TestSTMAdvanced` class name typo.
- Removed `dosync` double-wrapping in `examples/basic_usage.py`.
- Aligned Python version references to 3.13+ across README and badges.
- Added missing v3.3.0–v3.3.3 entries to README changelog.
- Moved misplaced changelog entries above copyright line in README.

### Removed
- Removed legacy `setup.py` in favor of `pyproject.toml`.

## [3.3.3] - 2026-03-14

### Fixed
- Removed duplicate `self._start_time` assignment in `_reset_for_retry()`.
- Removed redundant `import random` inside `Atom.swap()`.
- Consolidated `old_tx` / `existing_tx` lookups in `dosync()`.
- Fixed misindented `# type: ignore` comments in `STMQueue.get()`, `Ref._validate()`, and `Ref.commute()`.
- Fixed late-binding closure bug in `examples/basic_usage.py`.

### Changed
- Refactored `benchmarks/benchmark_stm.py` for realistic contention simulation.
- Removed duplicate `TransactionCoordinator` import from `__init__.py`.

### Added
- GitHub Actions CI pipeline (`.github/workflows/test.yml`).

## [3.3.2] - 2026-03-13

### Changed
- **Version Bump**: Finalized hardening and synchronized project-wide version to 3.3.2.
- **API Audit**: Performed a deep audit to ensure all internal references match the new modular structure.

## [3.3.1] - 2026-03-13

### Fixed
- **dosync context restoration**: Fixed bug where existing transaction context was lost after a `dosync` block.
- **Atom.swap livelock**: Implemented exponential backoff with jitter in `Atom.swap`.
- **PersistentHashMap.contains**: Corrected logic to properly detect `None` values.
- **_trim_history reentrancy**: Removed redundant lock acquisition.
- **psutil performance**: Optimized `psutil` import to be lazy-loaded.

### Changed
- **Public API**: Renamed internal `_transaction` to public-ready `transaction` context manager.

### Removed
- Removed legacy `Atomix.py` root file in favor of modular `atomix_stm` package.
- Cleaned up development and linting scripts from the repository.

### Added
- Comprehensive `docs/TUTORIAL.md` and `docs/COMPARISON.md`.
- New ecosystem examples: FastAPI, Flask, Monitoring, and Structured Concurrency.
- `py.typed` marker for PEP 561 compliance.
