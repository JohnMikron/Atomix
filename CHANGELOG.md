# Changelog

All notable changes to this project will be documented in this file.

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
