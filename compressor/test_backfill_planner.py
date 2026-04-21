#!/usr/bin/env python3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from backfill_planner import BackfillPlanner


class StubStateManager:
    def __init__(self, state, last_date=None):
        self._state = state
        self._last_date = last_date

    def _read_state(self):
        return self._state

    def get_last_compacted_date(self):
        return self._last_date


class BackfillPlannerTest(unittest.TestCase):
    def test_legacy_partial_skip_does_not_mark_day_complete(self):
        state = {
            "partitions": {
                "binance/bbo/btcusdt/20260418": {
                    "status": "skipped",
                    "day_quality": "PARTIAL",
                    "error": "Partial day data, retry expected",
                }
            }
        }
        planner = BackfillPlanner({"20260418", "20260419"}, StubStateManager(state), "20260420")
        self.assertEqual(planner.get_completed_dates(), set())
        self.assertEqual(planner.plan_reverse(), ["20260419", "20260418"])

    def test_counts_as_complete_drives_completed_dates(self):
        state = {
            "partitions": {
                "binance/bbo/btcusdt/20260418": {
                    "status": "partial",
                    "counts_as_complete": False,
                },
                "binance/bbo/ethusdt/20260419": {
                    "status": "success",
                    "counts_as_complete": True,
                },
            },
            "days": {
                "20260417": {"status": "quarantine", "counts_as_complete": True},
            },
        }
        planner = BackfillPlanner(
            {"20260417", "20260418", "20260419"},
            StubStateManager(state, last_date="20260416"),
            "20260420",
        )
        self.assertEqual(planner.get_completed_dates(), {"20260417", "20260419"})


if __name__ == "__main__":
    unittest.main()
