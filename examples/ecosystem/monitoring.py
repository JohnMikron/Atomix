"""
Atomix STM Ecosystem Monitoring Utility
========================================

Demonstrates production telemetry monitoring for Atomix STM.
Tracks contention levels, transaction throughput, conflict exceptions,
active snapshot counts, and memory pressure diagnostics.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, Optional

from atomix_stm import (
    Atom,
    ConflictException,
    Ref,
    RetryException,
    TimeoutException,
    atomically,
    get_stm_stats,
)

logger = logging.getLogger("atomix_stm.monitoring")


class STMMonitor:
    """Real-time diagnostic monitor for Atomix STM transactions and contention."""

    def __init__(self, sample_interval: float = 1.0) -> None:
        self.sample_interval = sample_interval
        self._is_active = Atom(True)
        self._total_observed_conflicts = Ref(0, name="monitor_conflicts")
        self._total_observed_retries = Ref(0, name="monitor_retries")
        self._last_snapshot: Ref[Optional[Dict[str, Any]]] = Ref(
            None, name="monitor_last_snapshot"
        )

    def record_conflict(self, exc: ConflictException) -> None:
        """Record an observed conflict event in telemetry."""
        logger.warning(
            "STM Conflict detected on refs %s: %s", exc.conflicting_refs, exc
        )

        @atomically
        def _inc_conflict() -> None:
            self._total_observed_conflicts.alter(lambda count: count + 1)

        _inc_conflict()

    def record_retry(self, exc: RetryException) -> None:
        """Record an observed retry event."""
        logger.debug("STM Transaction retrying (attempt %s)", exc.retry_count)

        @atomically
        def _inc_retry() -> None:
            self._total_observed_retries.alter(lambda count: count + 1)

        _inc_retry()

    def sample_metrics(self) -> Dict[str, Any]:
        """Collect current STM runtime metrics and update internal telemetry snapshot."""
        stats = get_stm_stats()
        enriched: Dict[str, Any] = {
            "timestamp": time.time(),
            "core_stats": stats,
            "monitored_conflicts": self._total_observed_conflicts.deref(),
            "monitored_retries": self._total_observed_retries.deref(),
        }

        @atomically
        def _save_snapshot() -> None:
            self._last_snapshot.set(enriched)

        _save_snapshot()
        return enriched

    def report_json(self) -> str:
        """Return formatted JSON report of current telemetry."""
        metrics = self.sample_metrics()
        return json.dumps(metrics, indent=2)

    def stop(self) -> None:
        """Stop the monitoring session."""
        self._is_active.reset(False)


def run_sample_monitor() -> None:
    """Example entrypoint demonstrating active monitoring under load."""
    monitor = STMMonitor(sample_interval=0.5)
    account_a = Ref(1000, name="account_a")
    account_b = Ref(1000, name="account_b")

    logger.info("Starting demonstration transactional transfer with monitoring...")
    try:

        @atomically
        def transfer() -> None:
            val_a = account_a.deref()
            val_b = account_b.deref()
            account_a.set(val_a - 100)
            account_b.set(val_b + 100)

        transfer()
    except ConflictException as e:
        monitor.record_conflict(e)
    except RetryException as e:
        monitor.record_retry(e)
    except TimeoutException as e:
        logger.error("Transaction timed out: %s", e)

    metrics_report = monitor.report_json()
    print("STM Diagnostics Report:")
    print(metrics_report)
    monitor.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_sample_monitor()
