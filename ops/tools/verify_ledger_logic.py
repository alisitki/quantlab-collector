import sys
import unittest
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Dict, List, Any

# Mock state
@dataclass
class MockState:
    dropped_events: int = 0
    reconnect_counts: Dict[str, int] = field(default_factory=lambda: {"binance": 0, "bybit": 0, "okx": 0})
    writer_stats: Dict[str, Any] = field(default_factory=lambda: {"upload_failures": 0})
    window_start_time: float = 0.0
    window_is_partial: bool = False
    window_queue_samples: List[float] = field(default_factory=list)
    window_offline_seconds: Dict[str, float] = field(default_factory=lambda: {"binance": 0.0, "bybit": 0.0, "okx": 0.0})
    window_accelerated_seconds: float = 0.0
    window_eps_samples: Dict[str, List[float]] = field(default_factory=lambda: {"binance": [], "bybit": [], "okx": []})
    backpressure_mode: str = "normal"
    drain_mode: str = "normal"
    
    _window_base_dropped: int = 0
    _window_base_reconnects: Dict[str, int] = field(default_factory=lambda: {"binance": 0, "bybit": 0, "okx": 0})
    _window_base_s3_failures: int = 0

    def get_window_deltas(self):
        reconnect_deltas = {}
        for ex, count in self.reconnect_counts.items():
            base = self._window_base_reconnects.get(ex, 0)
            reconnect_deltas[ex] = count - base
            
        return {
            "dropped_events": self.dropped_events - self._window_base_dropped,
            "reconnects": sum(reconnect_deltas.values()),
            "s3_upload_failures": self.writer_stats["upload_failures"] - self._window_base_s3_failures
        }

def classify_quality(state, deltas, eps_stats, all_samples_present):
    # P10: Nuanced (v2) logic copied from collector.py
    peak_q = max(state.window_queue_samples) if state.window_queue_samples else 0.0
    max_offline = max(state.window_offline_seconds.values()) if state.window_offline_seconds else 0.0
    
    quality = "GOOD"
    
    # BAD: drops OR >120s offline OR >=80% queue
    if deltas["dropped_events"] > 0 or max_offline > 120 or peak_q >= 80:
        quality = "BAD"
    # DEGRADED: missing EPS samples OR (>60s offline OR many zero EPS OR sustained accelerated drain OR multi-reconnects)
    else:
        max_zero_eps_sec = 0
        for ex, samples in state.window_eps_samples.items():
            zero_sec = sum(1 for s in samples if s < 0.1)
            max_zero_eps_sec = max(max_zero_eps_sec, zero_sec)
            
        if (not all_samples_present or (
            max_offline > 60 or
            max_zero_eps_sec > 5 or
            state.window_accelerated_seconds > 60 or
            deltas["reconnects"] >= 3
        )):
            quality = "DEGRADED"
    return quality

class TestLedgerLogic(unittest.TestCase):
    def test_good_window(self):
        state = MockState()
        state.window_queue_samples = [10.0, 20.0, 45.0]
        state.window_offline_seconds = {"binance": 0, "bybit": 0, "okx": 0}
        state.window_eps_samples = {"binance": [100], "bybit": [100], "okx": [100]}
        
        deltas = state.get_window_deltas()
        eps_stats = {"binance": {"min": 100}, "bybit": {"min": 100}, "okx": {"min": 100}}
        
        self.assertEqual(classify_quality(state, deltas, eps_stats, True), "GOOD")

    def test_good_minor_reconnect(self):
        state = MockState()
        state.reconnect_counts = {"binance": 2, "bybit": 0, "okx": 0}
        deltas = state.get_window_deltas()
        self.assertEqual(classify_quality(state, deltas, {}, True), "GOOD")

    def test_degraded_reconnect_many(self):
        state = MockState()
        state.reconnect_counts = {"binance": 3, "bybit": 0, "okx": 0}
        deltas = state.get_window_deltas()
        self.assertEqual(classify_quality(state, deltas, {}, True), "DEGRADED")

    def test_bad_dropped_events(self):
        state = MockState()
        state.dropped_events = 10
        state._window_base_dropped = 0
        deltas = state.get_window_deltas()
        self.assertEqual(classify_quality(state, deltas, {}, True), "BAD")

    def test_bad_long_offline(self):
        state = MockState()
        state.window_offline_seconds = {"binance": 121, "bybit": 0, "okx": 0}
        self.assertEqual(classify_quality(state, {"dropped_events": 0}, {}, True), "BAD")

    def test_bad_peak_queue(self):
        state = MockState()
        state.window_queue_samples = [80.1]
        self.assertEqual(classify_quality(state, {"dropped_events": 0}, {}, True), "BAD")

    def test_degraded_sustained_offline(self):
        # 61s > 60s
        state = MockState()
        state.window_offline_seconds = {"binance": 61, "bybit": 0, "okx": 0}
        self.assertEqual(classify_quality(state, {"dropped_events": 0, "reconnects": 0}, {}, True), "DEGRADED")

    def test_good_short_offline(self):
        # 59s <= 60s
        state = MockState()
        state.window_offline_seconds = {"binance": 59, "bybit": 0, "okx": 0}
        state.window_eps_samples = {"binance": [100], "bybit": [100], "okx": [100]}
        self.assertEqual(classify_quality(state, {"dropped_events": 0, "reconnects": 0}, {}, True), "GOOD")

    def test_degraded_sustained_zero_eps(self):
        # 6 samples of 0 EPS > 5
        state = MockState()
        state.window_eps_samples = {"binance": [0.0]*6, "bybit": [100]*6, "okx": [100]*6}
        self.assertEqual(classify_quality(state, {"dropped_events": 0, "reconnects": 0}, {}, True), "DEGRADED")

    def test_good_short_zero_eps(self):
        # 5 samples of 0 EPS <= 5
        state = MockState()
        state.window_eps_samples = {"binance": [0.0]*5, "bybit": [100]*5, "okx": [100]*5}
        self.assertEqual(classify_quality(state, {"dropped_events": 0, "reconnects": 0}, {}, True), "GOOD")

    def test_degraded_sustained_accelerated_drain(self):
        # 61s > 60s
        state = MockState()
        state.window_accelerated_seconds = 61
        self.assertEqual(classify_quality(state, {"dropped_events": 0, "reconnects": 0}, {}, True), "DEGRADED")

    def test_good_short_accelerated_drain(self):
        # 60s <= 60s
        state = MockState()
        state.window_accelerated_seconds = 60
        state.window_eps_samples = {"binance": [100], "bybit": [100], "okx": [100]}
        self.assertEqual(classify_quality(state, {"dropped_events": 0, "reconnects": 0}, {}, True), "GOOD")

if __name__ == "__main__":
    unittest.main()
