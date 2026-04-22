#!/usr/bin/env python3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from state_semantics import (
    PARTIAL_DAY_RETRY_ERROR,
    entry_counts_as_complete,
    partition_results_are_complete,
)


class StateSemanticsTest(unittest.TestCase):
    def test_partial_legacy_skipped_is_incomplete(self):
        entry = {
            "status": "skipped",
            "day_quality": "PARTIAL",
            "error": PARTIAL_DAY_RETRY_ERROR,
        }
        self.assertFalse(entry_counts_as_complete(entry))

    def test_success_and_quarantine_are_complete(self):
        self.assertTrue(entry_counts_as_complete({"status": "success"}))
        self.assertTrue(entry_counts_as_complete({"status": "quarantine"}))

    def test_explicit_counts_as_complete_wins(self):
        self.assertFalse(
            entry_counts_as_complete({"status": "success", "counts_as_complete": False})
        )

    def test_resource_limit_error_is_never_complete(self):
        self.assertFalse(
            entry_counts_as_complete(
                {
                    "status": "quarantine",
                    "counts_as_complete": True,
                    "error_type": "RESOURCE_LIMIT",
                    "error": "Too many open files",
                }
            )
        )

    def test_day_results_must_all_be_complete(self):
        self.assertTrue(
            partition_results_are_complete(
                [{"status": "success"}, {"status": "skipped", "skip_reason": "already_success"}]
            )
        )
        self.assertFalse(
            partition_results_are_complete(
                [{"status": "success"}, {"status": "partial", "error": PARTIAL_DAY_RETRY_ERROR}]
            )
        )


if __name__ == "__main__":
    unittest.main()
