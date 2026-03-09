with open('Atomix.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Patch ContentionManager
old_contention_metrics = '''    def get_metrics(self) -> Dict[str, Any]:
        """Get contention metrics."""
        with self._contention_lock:
            return {
                "contention_level": self.get_contention_level(),
                "throughput": self.get_throughput(),
                "total_retries": self._total_retries,
                "total_commits": self._total_commits,
                "global_contention": self._global_contention
            }'''
            
new_contention_metrics = '''    def get_metrics(self) -> Dict[str, Any]:
        """Get contention metrics."""
        with self._contention_lock:
            return {
                "contention_level": self.get_contention_level(),
                "throughput": self.get_throughput(),
                "total_retries": self._total_retries,
                "total_commits": self._total_commits,
                "global_contention": self._global_contention
            }
            
    def reset(self) -> None:
        """Reset contention state."""
        with self._contention_lock:
            self._contention_scores.clear()
            self._global_contention = 0
            self._total_retries = 0
            self._total_commits = 0
            self._history_window.clear()
            self._last_throughput = 0.0'''

# Patch HistoryManager
old_history_stats = '''    def get_stats(self) -> Dict[str, Any]:
        """Get history manager statistics."""
        with self._snapshots_lock:
            active = len(self._active_snapshots)
        
        with self._patterns_lock:
            tracked_refs = len(self._access_patterns)
        
        return {
            "active_snapshots": active,
            "tracked_refs": tracked_refs,
            "cleanups_performed": self._cleanups_performed,
            "snapshots_removed": self._snapshots_removed,
            "memory_pressure_events": self._memory_pressure_events
        }'''

new_history_stats = '''    def get_stats(self) -> Dict[str, Any]:
        """Get history manager statistics."""
        with self._snapshots_lock:
            active = len(self._active_snapshots)
        
        with self._patterns_lock:
            tracked_refs = len(self._access_patterns)
        
        return {
            "active_snapshots": active,
            "tracked_refs": tracked_refs,
            "cleanups_performed": self._cleanups_performed,
            "snapshots_removed": self._snapshots_removed,
            "memory_pressure_events": self._memory_pressure_events
        }
        
    def reset(self) -> None:
        """Reset history manager state."""
        with self._snapshots_lock:
            self._active_snapshots.clear()
        with self._patterns_lock:
            self._access_patterns.clear()
        self._cleanups_performed = 0
        self._snapshots_removed = 0
        self._memory_pressure_events = 0'''

# Patch Coordinator Reset
old_reset = '''    def reset(self) -> None:
        """Reset coordinator state (testing)."""
        with self._init_lock:
            self._tx_counter = itertools.count(1)
            self._ref_counter = itertools.count(1)
            self._logical_clock = 0
            self._current_epoch = 0
            self._refs.clear()
            self._active_txs.clear()
            self._stats = {
                "total_transactions": 0,
                "total_commits": 0,
                "total_aborts": 0,
                "total_conflicts": 0
            }
            if hasattr(self, '_reaper') and getattr(self._reaper, '_running', False):
                self._reaper.stop()
            self._reaper = STMReaper(self, interval=5.0)
            self._reaper.start()'''

new_reset = '''    def reset(self) -> None:
        """Reset coordinator state (testing)."""
        with self._init_lock:
            self._tx_counter = itertools.count(1)
            self._ref_counter = itertools.count(1)
            self._logical_clock = 0
            self._current_epoch = 0
            self._refs.clear()
            self._active_txs.clear()
            self._stats = {
                "total_transactions": 0,
                "total_commits": 0,
                "total_aborts": 0,
                "total_conflicts": 0
            }
            if hasattr(self, 'contention'):
                self.contention.reset()
            if hasattr(self, 'history'):
                self.history.reset()
                
            if hasattr(self, '_reaper') and getattr(self._reaper, '_running', False):
                self._reaper.stop()
            self._reaper = STMReaper(self, interval=5.0)
            self._reaper.start()'''

import re
def apply_patch(text, old, new, name):
    # normalize newlines to match safely
    regex = old.replace('(', '\\(').replace(')', '\\)').replace('{', '\\{').replace('}', '\\}').replace('.', '\\.')
    regex = regex.replace('[', '\\[').replace(']', '\\]').replace('+', '\\+').replace('*', '\\*')
    regex = re.sub(r'\s+', r'\\s+', regex)
    
    if re.search(regex, text):
        print(f"Patched {name}")
        return re.sub(regex, new.replace('\\', '\\\\'), text, count=1)
    else:
        print(f"Warning: Could not patch {name}")
        return text

content = apply_patch(content, old_contention_metrics, new_contention_metrics, "contention")
content = apply_patch(content, old_history_stats, new_history_stats, "history")
content = apply_patch(content, old_reset, new_reset, "reset")

with open('Atomix.py', 'w', encoding='utf-8') as f:
    f.write(content)
