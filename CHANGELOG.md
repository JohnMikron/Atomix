# Changelog

All notable changes to this project will be documented in this file.

## [3.3.3] - 2026-03-14

### Fixed
- Removed duplicate `self._start_time` assignment in `_reset_for_retry()`.
- Removed redundant `import random` inside `Atom.swap()`.
- Consolidated `old_tx` / `existing_tx` lookups in `dosync()`.
- Fixed misindented `# type: ignore` comments in `STMQueue.get()`, `Ref._validate()`, and `Ref.commute()`.
- Fixed late-binding closure bug in `examples/basic_usage.py`.

### Changed
- Refactored `benchmarks/benchmark_stm.py` to run atomic operations individually for realistic contention simulation.
- Removed duplicate `TransactionCoordinator` import from `__init__.py`.

### Added
- GitHub Actions CI pipeline (`.github/workflows/test.yml`) for automated testing on Python 3.13 and 3.14-dev.

## [3.3.2] - 2026-03-13

### Changed
- **Version Bump**: Finalized hardening and synchronized project-wide version to 3.3.2.
- **API Audit**: Performed a deep audit to ensure all internal references match the new modular structure.

## [3.3.1] - 2026-03-13

### Fixed
- **dosync context restoration**: Fixed bug where existing transaction context was lost after a `dosync` block.
- **Atom.swap livelock**: Implemented exponential backoff with jitter in `Atom.swap` to prevent CPU spinning in high-contention environments.
- **PersistentHashMap.contains**: Corrected logic to properly detect `None` values.
- **_trim_history reentrancy**: Removed redundant lock acquisition to improve stability.
- **psutil performance**: Optimized `psutil` import to be lazy-loaded.

### Changed
- **Public API**: Renamed internal `_transaction` to public-ready `transaction` context manager.
- **Versioning**: Standardized versioning across all core files and metadata.

### Removed
- Removed legacy `Atomix.py` root file in favor of modular `atomix_stm` package.
- Cleaned up development and linting scripts from the repository.

### Added
- Comprehensive `docs/TUTORIAL.md` and `docs/COMPARISON.md`.
- New ecosystem examples: FastAPI, Flask, Monitoring, and Structured Concurrency.
- `py.typed` marker for PEP 561 compliance.
