import time
from dataclasses import dataclass, field
from typing import Dict, Any
from enum import Enum


# P2: Collector runtime state
class CollectorStateEnum(str, Enum):
    READY = "READY"
    DEGRADED = "DEGRADED"
    OFFLINE = "OFFLINE"
    ERROR = "ERROR"


@dataclass
class CollectorState:
    start_time: float = field(default_factory=time.time)
    last_event_ts: float = 0
    last_write_ts: float = 0
    total_events: int = 0
    event_counts: Dict[str, int] = field(default_factory=lambda: {"bbo": 0, "trade": 0, "mark_price": 0, "funding": 0, "open_interest": 0})
    writer_stats: Dict[str, Any] = field(default_factory=dict)
    
    # P0 Metrics
    dropped_events: int = 0
    reconnect_counts: Dict[str, int] = field(default_factory=lambda: {"binance": 0, "bybit": 0, "okx": 0})
    gaps_detected: int = 0
    
    # P1-D: Quality Metrics
    snapshot_fetches_total: int = 0
    snapshot_events_seen: int = 0
    gap_alignment_corrections: int = 0
    
    # P2: State tracking
    current_state: CollectorStateEnum = CollectorStateEnum.READY
    state_since: float = field(default_factory=time.time)
    
    # For EPS calculation
    _last_counts: Dict[str, int] = field(default_factory=lambda: {"bbo": 0, "trade": 0, "mark_price": 0, "funding": 0, "open_interest": 0})
    _last_check_time: float = field(default_factory=time.time)
    eps: Dict[str, float] = field(default_factory=lambda: {"bbo": 0.0, "trade": 0.0, "mark_price": 0.0, "funding": 0.0, "open_interest": 0.0})
    
    def gap_coverage_ratio(self) -> float:
        """Measure tracking quality: what % of time we have valid last_ts_event."""
        if self.gaps_detected == 0:
            return 1.0
        # Simplified: higher gaps = worse coverage
        return max(0.0, 1.0 - (self.gaps_detected / max(1, self.total_events / 1000)))
    
    def effective_data_loss_rate(self) -> float:
        """PRIMARY KPI: Estimated % of events lost (gaps + drops)."""
        lost = self.gaps_detected + self.dropped_events
        if self.total_events == 0:
            return 0.0
        return (lost / self.total_events) * 100
    
    def derive_state(self, ws_connected: Dict[str, bool], writer_alive: bool, fatal_error: bool, queue_size: int, queue_maxsize: int) -> CollectorStateEnum:
        """P2: Derive current state from metrics (read-only, no side effects)."""
        if fatal_error or not writer_alive:
            return CollectorStateEnum.ERROR
        
        ws_count = sum(1 for v in ws_connected.values() if v)
        if ws_count == 0:
            return CollectorStateEnum.OFFLINE
        
        queue_util = (queue_size / queue_maxsize * 100) if queue_maxsize > 0 else 0
        
        # DEGRADED if: queue >=80%, drops >0, or high gap rate
        if queue_util >= 80 or self.dropped_events > 0:
            return CollectorStateEnum.DEGRADED
        
        return CollectorStateEnum.READY
    
    def update_state(self, new_state: CollectorStateEnum) -> bool:
        """P2: Update state and reset timer if changed. Returns True if state changed."""
        if new_state != self.current_state:
            old_state = self.current_state
            self.current_state = new_state
            self.state_since = time.time()
            return True
        return False

    def update_event(self, stream_type: str):
        self.last_event_ts = time.time() * 1000
        self.total_events += 1
        if stream_type in self.event_counts:
            self.event_counts[stream_type] += 1
        
        # Periodic EPS update
        now = time.time()
        elapsed = now - self._last_check_time
        if elapsed >= 10:
            for s in self.eps:
                delta = self.event_counts[s] - self._last_counts[s]
                self.eps[s] = round(delta / elapsed, 1)
                self._last_counts[s] = self.event_counts[s]
            self._last_check_time = now
