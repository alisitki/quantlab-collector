import time
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, Any, Deque, Tuple, List
from enum import Enum

from config import SILENCE_WINDOW_SECONDS


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
    
    # Silence Intervals (renamed from gaps_detected)
    # - silence_intervals_total: lifetime counter
    # - _silence_timestamps: deque of (timestamp, key) for rolling window
    silence_intervals_total: int = 0
    _silence_timestamps: Deque[Tuple[float, tuple]] = field(default_factory=deque)
    
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
    
    # Backpressure metrics
    backpressure_events_count: int = 0
    backpressure_wait_seconds_total: float = 0.0
    backpressure_active: bool = False
    backpressure_mode: str = "normal"  # "normal", "high", "critical"
    _last_backpressure_log: float = 0.0  # Rate-limit mode transition logs
    
    # P7: Adaptive Drain metrics
    # drain_mode: "normal" (5s flush gap) or "accelerated" (1s flush gap)
    drain_mode: str = "normal"
    
    # P7: Guardrail metrics (read-only, for observability)
    # All rates are events/second, updated every 10s
    ingestion_rate: float = 0.0      # events/sec into queue
    drain_rate: float = 0.0          # events/sec written to storage
    queue_growth_rate: float = 0.0   # ingestion_rate - drain_rate
    _last_drain_count: int = 0       # For drain_rate calculation
    _growth_warning_start: float = 0.0  # For sustained growth warning
    
    # P8: Per-exchange event rate metrics (blind spot removal)
    # Tracks events per exchange to detect single-exchange silence
    event_count_by_exchange: Dict[str, int] = field(default_factory=lambda: {"binance": 0, "bybit": 0, "okx": 0})
    _last_exchange_counts: Dict[str, int] = field(default_factory=lambda: {"binance": 0, "bybit": 0, "okx": 0})
    eps_by_exchange: Dict[str, float] = field(default_factory=lambda: {"binance": 0.0, "bybit": 0.0, "okx": 0.0})
    
    # P8: Heartbeat truthfulness (data-based ws_connected)
    # Tracks last valid message timestamp per exchange for truthful liveness
    last_message_ts_by_exchange: Dict[str, float] = field(default_factory=lambda: {"binance": 0.0, "bybit": 0.0, "okx": 0.0})

    # P9: Data Quality Ledger - Windowed Metrics (Aligned to 15m UTC)
    # Base snapshots are taken at the start of each 15-minute window
    window_start_time: float = 0.0
    window_is_partial: bool = True  # First window since restart is always partial
    _window_base_dropped: int = 0
    _window_base_reconnects: Dict[str, int] = field(default_factory=lambda: {"binance": 0, "bybit": 0, "okx": 0})
    _window_base_s3_failures: int = 0
    
    # 1s Sampling containers for the current window
    # These are populated by the quality_ledger_task and cleared every 15m
    window_queue_samples: List[float] = field(default_factory=list)
    window_backpressure_samples: List[str] = field(default_factory=list)  # normal, high, critical
    window_offline_seconds: Dict[str, float] = field(default_factory=lambda: {"binance": 0.0, "bybit": 0.0, "okx": 0.0})
    window_accelerated_seconds: float = 0.0
    window_eps_samples: Dict[str, List[float]] = field(default_factory=lambda: {"binance": [], "bybit": [], "okx": []})

    def take_window_snapshot(self):
        """P9: Take a base snapshot of lifetime counters for the current window."""
        self.window_start_time = time.time()
        self._window_base_dropped = self.dropped_events
        self._window_base_reconnects = self.reconnect_counts.copy()
        self._window_base_s3_failures = self.writer_stats.get("upload_failures", 0)
        
        # Clear sampling containers
        self.window_queue_samples.clear()
        self.window_backpressure_samples.clear()
        for ex in self.window_offline_seconds:
            self.window_offline_seconds[ex] = 0.0
        self.window_accelerated_seconds = 0.0
        for ex in self.window_eps_samples:
            self.window_eps_samples[ex].clear()
            
    def get_window_deltas(self) -> Dict[str, Any]:
        """P9: Calculate deltas for the current 15-minute window."""
        current_s3_fails = self.writer_stats.get("upload_failures", 0)
        
        reconnect_deltas = {}
        for ex, count in self.reconnect_counts.items():
            base = self._window_base_reconnects.get(ex, 0)
            reconnect_deltas[ex] = count - base
            
        return {
            "dropped_events": self.dropped_events - self._window_base_dropped,
            "reconnects": sum(reconnect_deltas.values()),
            "reconnects_by_exchange": reconnect_deltas,
            "s3_upload_failures": current_s3_fails - self._window_base_s3_failures
        }

    
    def record_silence_interval(self, key: tuple):
        """Record a silence interval detection (both total and rolling window)."""
        self.silence_intervals_total += 1
        self._silence_timestamps.append((time.time(), key))
        self._evict_old_silence_entries()
    
    def _evict_old_silence_entries(self):
        """Remove entries older than SILENCE_WINDOW_SECONDS from rolling window."""
        cutoff = time.time() - SILENCE_WINDOW_SECONDS
        while self._silence_timestamps and self._silence_timestamps[0][0] < cutoff:
            self._silence_timestamps.popleft()
    
    @property
    def silence_intervals_15m(self) -> int:
        """Return count of silence intervals in the last 15 minutes."""
        self._evict_old_silence_entries()
        return len(self._silence_timestamps)
    
    # Legacy compatibility (deprecated - use silence_intervals_total)
    @property
    def gaps_detected(self) -> int:
        return self.silence_intervals_total
    
    def effective_data_loss_rate(self) -> float:
        """PRIMARY KPI: Estimated % of events lost (drops only - silence is not loss)."""
        # NOTE: silence_intervals are NOT data loss, only dropped_events count
        if self.total_events == 0:
            return 0.0
        return (self.dropped_events / self.total_events) * 100
    
    def derive_state(self, ws_connected: Dict[str, bool], writer_alive: bool, fatal_error: bool, queue_size: int, queue_maxsize: int) -> CollectorStateEnum:
        """P2: Derive current state from metrics (read-only, no side effects)."""
        if fatal_error or not writer_alive:
            return CollectorStateEnum.ERROR
        
        ws_count = sum(1 for v in ws_connected.values() if v)
        if ws_count == 0:
            return CollectorStateEnum.OFFLINE
        
        queue_util = (queue_size / queue_maxsize * 100) if queue_maxsize > 0 else 0
        
        # DEGRADED if: queue >=80%, drops >0
        # NOTE: silence_intervals do NOT trigger DEGRADED (they're not data loss)
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

    def update_event(self, stream_type: str, exchange: str = None):
        """Update event counters. P8: Now also tracks per-exchange metrics."""
        now = time.time()
        self.last_event_ts = now * 1000
        self.total_events += 1
        if stream_type in self.event_counts:
            self.event_counts[stream_type] += 1
        
        # P8: Track per-exchange event count and heartbeat timestamp
        if exchange and exchange in self.event_count_by_exchange:
            self.event_count_by_exchange[exchange] += 1
            self.last_message_ts_by_exchange[exchange] = now
        
        # Periodic EPS update (both total and per-exchange)
        elapsed = now - self._last_check_time
        if elapsed >= 10:
            # Per-stream EPS
            for s in self.eps:
                delta = self.event_counts[s] - self._last_counts[s]
                self.eps[s] = round(delta / elapsed, 1)
                self._last_counts[s] = self.event_counts[s]
            
            # P8: Per-exchange EPS
            for ex in self.eps_by_exchange:
                delta = self.event_count_by_exchange[ex] - self._last_exchange_counts[ex]
                self.eps_by_exchange[ex] = round(delta / elapsed, 1)
                self._last_exchange_counts[ex] = self.event_count_by_exchange[ex]
            
            self._last_check_time = now
    
    def get_ws_connected(self, heartbeat_timeout: float = 30.0) -> Dict[str, bool]:
        """P8: Data-based ws_connected - True if received message within heartbeat_timeout seconds."""
        now = time.time()
        return {
            ex: (now - ts) < heartbeat_timeout if ts > 0 else False
            for ex, ts in self.last_message_ts_by_exchange.items()
        }
