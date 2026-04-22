#!/usr/bin/env python3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).parent))

from merge_writer import StreamingMergeWriter


class FakeStats:
    def __init__(self, min_value, max_value):
        self.min = min_value
        self.max = max_value
        self.has_min_max = True


class FakeColumn:
    def __init__(self, stats):
        self.statistics = stats


class FakeRowGroup:
    def __init__(self, stats):
        self._stats = stats

    def column(self, _idx):
        return FakeColumn(self._stats)


class FakeMetadata:
    def __init__(self, stats):
        self._stats = stats

    def row_group(self, _idx):
        return FakeRowGroup(self._stats)


class FakeParquetFile:
    instances = []
    stats_by_name = {}

    def __init__(self, path):
        self.path = Path(path)
        self.closed = False
        self.schema_arrow = pa.schema(
            [pa.field("ts_event", pa.int64()), pa.field("value", pa.int64())]
        )
        self.metadata = FakeMetadata(self.stats_by_name[self.path.name])
        FakeParquetFile.instances.append(self)

    def iter_batches(self, batch_size=1000):
        yield pa.record_batch(
            [pa.array([self.metadata._stats.min]), pa.array([1])],
            names=["ts_event", "value"],
        )

    def close(self):
        self.closed = True


class FakeParquetWriter:
    def __init__(self, *_args, **_kwargs):
        self.closed = False
        self.batches = 0

    def write_batch(self, _batch):
        self.batches += 1

    def close(self):
        self.closed = True


class MergeWriterFileCleanupTest(unittest.TestCase):
    def setUp(self):
        FakeParquetFile.instances = []
        FakeParquetFile.stats_by_name = {
            "a.parquet": FakeStats(100, 199),
            "b.parquet": FakeStats(200, 299),
        }

    def test_check_ordering_closes_all_opened_files(self):
        merger = StreamingMergeWriter(
            [Path("a.parquet"), Path("b.parquet")],
            Path("unused.parquet"),
            add_seq_column=False,
        )
        with patch("merge_writer.pq.ParquetFile", FakeParquetFile):
            is_ordered, reason = merger._check_ordering()

        self.assertTrue(is_ordered)
        self.assertEqual(reason, "strictly_ordered")
        self.assertTrue(FakeParquetFile.instances)
        self.assertTrue(all(instance.closed for instance in FakeParquetFile.instances))

    def test_fast_concat_closes_all_opened_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            merger = StreamingMergeWriter(
                [Path("a.parquet"), Path("b.parquet")],
                Path(tmpdir) / "out.parquet",
                add_seq_column=False,
            )
            with patch("merge_writer.pq.ParquetFile", FakeParquetFile), patch(
                "merge_writer.pq.ParquetWriter", FakeParquetWriter
            ):
                metadata = merger._fast_concat()

        self.assertEqual(metadata["rows"], 2)
        self.assertEqual(len(FakeParquetFile.instances), 3)
        self.assertTrue(all(instance.closed for instance in FakeParquetFile.instances))


if __name__ == "__main__":
    unittest.main()
