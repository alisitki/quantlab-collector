#!/usr/bin/env python3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from compact import CompactionJob
from manifest_state import (
    AVAILABLE,
    REASON_ALWAYS_MISSING_COMBO,
    REASON_MISSING_RAW,
    REASON_QUALITY_BAD,
    REASON_RESOURCE_LIMIT,
    build_consumer_manifest,
    get_entry,
    preserve_progress_seed,
)


class ManifestHarness(CompactionJob):
    def __init__(self, raw_parts, artifacts, quality_by_date):
        self._raw_parts = set(raw_parts)
        self._artifacts = dict(artifacts)
        self._quality_by_date = dict(quality_by_date)

    def _scan_raw_partitions_before(self, today):
        return {part for part in self._raw_parts if part[0] < today}

    def _scan_compact_artifacts_before(self, today):
        return {key: value for key, value in self._artifacts.items() if key[0] < today}

    def _fetch_quality_data(self, date_str):
        return {"day_quality": self._quality_by_date.get(date_str, "GOOD")}


class ManifestStateV2Test(unittest.TestCase):
    def test_missing_raw_gap_becomes_explicit_entry(self):
        harness = ManifestHarness(
            raw_parts={
                ("20260423", "binance", "trade", "btcusdt"),
                ("20260423", "binance", "bbo", "btcusdt"),
                ("20260425", "binance", "bbo", "btcusdt"),
            },
            artifacts={
                ("20260423", "binance", "trade", "btcusdt"): {
                    "data_key": "exchange=binance/stream=trade/symbol=btcusdt/date=20260423/data.parquet",
                    "meta_key": "exchange=binance/stream=trade/symbol=btcusdt/date=20260423/meta.json",
                    "quality_day_key": "exchange=binance/stream=trade/symbol=btcusdt/date=20260423/quality_day.json",
                },
                ("20260425", "binance", "bbo", "btcusdt"): {
                    "data_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260425/data.parquet",
                    "meta_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260425/meta.json",
                    "quality_day_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260425/quality_day.json",
                },
            },
            quality_by_date={"20260423": "GOOD", "20260425": "GOOD"},
        )
        state = harness._build_manifest_state({}, "20260426")
        entry = get_entry(state, "20260425", "binance", "trade", "btcusdt")
        self.assertEqual(entry["availability"], "unavailable")
        self.assertFalse(entry["retryable"])
        self.assertEqual(entry["reason_code"], REASON_MISSING_RAW)

    def test_always_missing_combo_and_available_artifact_are_explicit(self):
        harness = ManifestHarness(
            raw_parts={
                ("20260425", "binance", "bbo", "btcusdt"),
            },
            artifacts={
                ("20260425", "binance", "bbo", "btcusdt"): {
                    "data_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260425/data.parquet",
                    "meta_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260425/meta.json",
                    "quality_day_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260425/quality_day.json",
                },
            },
            quality_by_date={"20260425": "GOOD"},
        )
        state = harness._build_manifest_state({}, "20260426")

        available = get_entry(state, "20260425", "binance", "bbo", "btcusdt")
        self.assertEqual(available["availability"], AVAILABLE)
        self.assertIn("artifacts", available)
        self.assertIn("data_key", available["artifacts"])

        missing = get_entry(state, "20260425", "bybit", "trade", "maticusdt")
        self.assertEqual(missing["reason_code"], REASON_ALWAYS_MISSING_COMBO)
        self.assertFalse(missing["retryable"])

    def test_legacy_resource_limit_and_quality_bad_retry_semantics(self):
        harness = ManifestHarness(
            raw_parts={("20260425", "binance", "trade", "btcusdt")},
            artifacts={},
            quality_by_date={"20260425": "GOOD", "20260424": "BAD"},
        )
        legacy_state = {
            "partitions": {
                "binance/trade/btcusdt/20260425": {
                    "status": "failed",
                    "error_type": "RESOURCE_LIMIT",
                    "updated_at": "2026-04-26T00:00:00Z",
                }
            }
        }
        state = harness._build_manifest_state(legacy_state, "20260426")
        entry = get_entry(state, "20260425", "binance", "trade", "btcusdt")
        self.assertEqual(entry["reason_code"], REASON_RESOURCE_LIMIT)
        self.assertTrue(entry["retryable"])

        state_bad = harness._build_manifest_state(
            {
                "days": {
                    "20260424": {
                        "status": "quarantine",
                        "updated_at": "2026-04-25T00:00:00Z",
                    }
                }
            },
            "20260425",
        )
        bad_entry = get_entry(state_bad, "20260424", "binance", "trade", "btcusdt")
        self.assertEqual(bad_entry["reason_code"], REASON_QUALITY_BAD)
        self.assertFalse(bad_entry["retryable"])

    def test_legacy_last_compacted_date_is_preserved_as_progress_seed(self):
        harness = ManifestHarness(
            raw_parts={
                ("20260104", "binance", "bbo", "btcusdt"),
                ("20260424", "binance", "bbo", "btcusdt"),
            },
            artifacts={
                ("20260104", "binance", "bbo", "btcusdt"): {
                    "data_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260104/data.parquet",
                    "meta_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260104/meta.json",
                    "quality_day_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260104/quality_day.json",
                },
                ("20260424", "binance", "bbo", "btcusdt"): {
                    "data_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260424/data.parquet",
                    "meta_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260424/meta.json",
                    "quality_day_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260424/quality_day.json",
                },
            },
            quality_by_date={"20260104": "GOOD", "20260424": "GOOD"},
        )
        state = harness._build_manifest_state({"last_compacted_date": "20260424"}, "20260426")
        self.assertEqual(state["progress"]["last_complete_date"], "20260424")

    def test_consumer_manifest_only_exposes_availability_and_artifacts(self):
        harness = ManifestHarness(
            raw_parts={
                ("20260425", "binance", "bbo", "btcusdt"),
            },
            artifacts={
                ("20260425", "binance", "bbo", "btcusdt"): {
                    "data_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260425/data.parquet",
                    "meta_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260425/meta.json",
                    "quality_day_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260425/quality_day.json",
                },
            },
            quality_by_date={"20260425": "GOOD"},
        )
        state = harness._build_manifest_state({}, "20260426")
        manifest = build_consumer_manifest(state)

        available = manifest["dates"]["20260425"]["exchanges"]["binance"]["streams"]["bbo"]["symbols"]["btcusdt"]
        self.assertEqual(
            available,
            {
                "available": True,
                "artifacts": {
                    "data_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260425/data.parquet",
                    "meta_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260425/meta.json",
                    "quality_day_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260425/quality_day.json",
                },
            },
        )

        unavailable = manifest["dates"]["20260425"]["exchanges"]["binance"]["streams"]["trade"]["symbols"]["btcusdt"]
        self.assertEqual(unavailable, {"available": False})
        self.assertNotIn("progress", manifest)

    def test_preserve_progress_seed_keeps_higher_frontier(self):
        state = {
            "schema_version": 2,
            "progress": {"last_complete_date": "20260104"},
            "dates": {},
        }
        preserve_progress_seed(state, "20260424")
        self.assertEqual(state["progress"]["last_complete_date"], "20260424")


if __name__ == "__main__":
    unittest.main()
