#!/usr/bin/env python3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from compact import classify_compaction_error


class CompactErrorClassificationTest(unittest.TestCase):
    def test_too_many_open_files_is_failed_resource_limit(self):
        self.assertEqual(
            classify_compaction_error("Failed to open file: [Errno 24] Too many open files"),
            ("RESOURCE_LIMIT", "failed"),
        )

    def test_dictionary_conflict_remains_quarantine(self):
        self.assertEqual(
            classify_compaction_error("Column cannot have more than one dictionary."),
            ("DICT_CONFLICT", "quarantine"),
        )


if __name__ == "__main__":
    unittest.main()
