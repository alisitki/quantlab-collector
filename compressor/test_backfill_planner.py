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
    def test_v2_fetch_ready_drives_completed_dates(self):
        state = {
            "schema_version": 2,
            "dates": {
                "20260417": {
                    "summary": {"fetch_ready": True},
                    "exchanges": {},
                },
                "20260418": {
                    "summary": {"fetch_ready": False},
                    "exchanges": {},
                },
                "20260419": {
                    "summary": {"fetch_ready": True},
                    "exchanges": {},
                }
            },
        }
        planner = BackfillPlanner({"20260417", "20260418", "20260419"}, StubStateManager(state), "20260420")
        self.assertEqual(planner.get_completed_dates(), {"20260417", "20260419"})
        self.assertEqual(planner.plan_reverse(), ["20260418"])

    def test_catch_up_uses_progress_and_fetch_ready(self):
        state = {
            "schema_version": 2,
            "progress": {"last_complete_date": "20260417"},
            "dates": {
                "20260418": {
                    "summary": {"fetch_ready": False},
                    "exchanges": {},
                },
                "20260419": {
                    "summary": {"fetch_ready": True},
                    "exchanges": {},
                },
            }
        }
        planner = BackfillPlanner(
            {"20260417", "20260418", "20260419", "20260420"},
            StubStateManager(state, last_date="20260417"),
            "20260420",
        )
        self.assertEqual(planner.plan_catch_up(), ["20260418"])


if __name__ == "__main__":
    unittest.main()
