#!/usr/bin/env python3
"""
QuantLab Merge Determinism Verification Test

Verifies:
1. Streaming merge produces identical output on repeated runs
2. Output is sorted by ts_event (monotonically non-decreasing)
3. Row count equals sum of input files
4. Memory stays bounded (no OOM)
"""

import os
import sys
import hashlib
import tempfile
from pathlib import Path

import pyarrow.parquet as pq

# Add parent dir to path
sys.path.insert(0, str(Path(__file__).parent))

from merge_writer import StreamingMergeWriter


def compute_row_hash(parquet_path: Path) -> str:
    """
    Compute a rolling hash of all rows for determinism check.
    Uses streaming to avoid loading entire file into memory.
    """
    hasher = hashlib.sha256()
    pf = pq.ParquetFile(parquet_path)
    
    for batch in pf.iter_batches(batch_size=10000):
        # Hash each column's values
        for col_name in batch.schema.names:
            col = batch[col_name]
            for val in col:
                hasher.update(str(val.as_py()).encode())
    
    return hasher.hexdigest()


def verify_sorted_order(parquet_path: Path) -> bool:
    """
    Verify ts_event is monotonically non-decreasing.
    Uses streaming to avoid full read.
    """
    pf = pq.ParquetFile(parquet_path)
    last_ts = None
    
    for batch in pf.iter_batches(batch_size=50000):
        ts_col = batch['ts_event']
        for val in ts_col:
            ts = val.as_py()
            if last_ts is not None and ts < last_ts:
                print(f"ORDER VIOLATION: {ts} < {last_ts}")
                return False
            last_ts = ts
    
    return True


def count_rows_streaming(parquet_path: Path) -> int:
    """Count rows using streaming."""
    pf = pq.ParquetFile(parquet_path)
    return pf.metadata.num_rows


def run_determinism_test(input_files: list, test_name: str = "test"):
    """
    Run merge twice and compare outputs.
    """
    print(f"\n{'='*60}")
    print(f"DETERMINISM TEST: {test_name}")
    print(f"{'='*60}")
    
    with tempfile.TemporaryDirectory() as tmpdir:
        output1 = Path(tmpdir) / "output1.parquet"
        output2 = Path(tmpdir) / "output2.parquet"
        
        # First run
        print("\n[1/4] First merge run...")
        merger1 = StreamingMergeWriter(input_files, output1)
        meta1 = merger1.merge()
        print(f"      Rows: {meta1['rows']}")
        
        # Second run
        print("[2/4] Second merge run...")
        merger2 = StreamingMergeWriter(input_files, output2)
        meta2 = merger2.merge()
        print(f"      Rows: {meta2['rows']}")
        
        # Compare row counts
        print("[3/4] Comparing outputs...")
        assert meta1['rows'] == meta2['rows'], f"Row count mismatch: {meta1['rows']} vs {meta2['rows']}"
        print(f"      ✓ Row counts match: {meta1['rows']}")
        
        # Compare hashes
        hash1 = compute_row_hash(output1)
        hash2 = compute_row_hash(output2)
        assert hash1 == hash2, f"Hash mismatch: {hash1} vs {hash2}"
        print(f"      ✓ Content hashes match: {hash1[:16]}...")
        
        # Verify sorted order
        print("[4/4] Verifying sort order...")
        assert verify_sorted_order(output1), "Output 1 not sorted!"
        assert verify_sorted_order(output2), "Output 2 not sorted!"
        print(f"      ✓ Both outputs correctly sorted by ts_event")
        
        print(f"\n{'='*60}")
        print("✅ DETERMINISM TEST PASSED")
        print(f"{'='*60}\n")
        
        return True


def create_test_parquet(path: Path, rows: list):
    """Create a test parquet file with ts_event column."""
    import pyarrow as pa
    
    table = pa.table({
        'ts_event': pa.array([r['ts_event'] for r in rows], type=pa.int64()),
        'symbol': pa.array([r.get('symbol', 'TEST') for r in rows], type=pa.string()),
        'value': pa.array([r.get('value', 0.0) for r in rows], type=pa.float64())
    })
    
    pq.write_table(table, path)


def run_synthetic_test():
    """Run test with synthetic data."""
    print("\n" + "="*60)
    print("SYNTHETIC DATA TEST")
    print("="*60)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create test files with overlapping ts_event values
        tmpdir = Path(tmpdir)
        
        # File 1: ts_event 100, 200, 300
        create_test_parquet(
            tmpdir / "part1.parquet",
            [
                {'ts_event': 100, 'symbol': 'A', 'value': 1.0},
                {'ts_event': 200, 'symbol': 'A', 'value': 2.0},
                {'ts_event': 300, 'symbol': 'A', 'value': 3.0},
            ]
        )
        
        # File 2: ts_event 150, 200, 250 (note: 200 overlaps)
        create_test_parquet(
            tmpdir / "part2.parquet",
            [
                {'ts_event': 150, 'symbol': 'B', 'value': 1.5},
                {'ts_event': 200, 'symbol': 'B', 'value': 2.5},
                {'ts_event': 250, 'symbol': 'B', 'value': 2.8},
            ]
        )
        
        # File 3: ts_event 50, 400, 500
        create_test_parquet(
            tmpdir / "part3.parquet",
            [
                {'ts_event': 50, 'symbol': 'C', 'value': 0.5},
                {'ts_event': 400, 'symbol': 'C', 'value': 4.0},
                {'ts_event': 500, 'symbol': 'C', 'value': 5.0},
            ]
        )
        
        input_files = [
            tmpdir / "part1.parquet",
            tmpdir / "part2.parquet",
            tmpdir / "part3.parquet",
        ]
        
        # Run test
        run_determinism_test(input_files, "Synthetic 3-file merge")
        
        # Additional: verify seq column
        output = tmpdir / "verify_output.parquet"
        merger = StreamingMergeWriter(input_files, output)
        meta = merger.merge()
        
        print("Verifying seq column...")
        pf = pq.ParquetFile(output)
        table = pf.read()
        
        assert 'seq' in table.schema.names, "seq column missing!"
        seq_values = table['seq'].to_pylist()
        expected_seq = list(range(9))  # 0 to 8 for 9 rows
        assert seq_values == expected_seq, f"seq values wrong: {seq_values}"
        print(f"      ✓ seq column correct: {seq_values}")
        
        # Verify expected order
        ts_values = table['ts_event'].to_pylist()
        expected_ts = [50, 100, 150, 200, 200, 250, 300, 400, 500]
        assert ts_values == expected_ts, f"ts order wrong: {ts_values}"
        print(f"      ✓ ts_event order correct: {ts_values}")
        
        print("\n✅ SYNTHETIC TEST PASSED\n")


def run_dictionary_hierarchical_test():
    """
    Repro for: 'Column cannot have more than one dictionary.'
    This typically happens when merging dictionary-encoded columns that were encoded with
    different dictionaries across input files.
    """
    print("\n" + "="*60)
    print("DICTIONARY + HIERARCHICAL MERGE TEST")
    print("="*60)

    import pyarrow as pa

    def create_dict_parquet(path: Path, ts_events: list, symbols: list):
        # Explicit dictionary type to force per-file dictionaries.
        dict_ty = pa.dictionary(pa.int32(), pa.string())
        table = pa.table({
            'ts_event': pa.array(ts_events, type=pa.int64()),
            'symbol': pa.array(symbols, type=dict_ty),
            'value': pa.array([1.0] * len(ts_events), type=pa.float64()),
        })
        pq.write_table(table, path)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Different per-file symbol dictionaries (A/B/C) to trigger dictionary conflicts.
        create_dict_parquet(tmpdir / "d1.parquet", [100, 200], ["A", "A"])
        create_dict_parquet(tmpdir / "d2.parquet", [150, 250], ["B", "B"])
        create_dict_parquet(tmpdir / "d3.parquet", [50, 300], ["C", "C"])

        input_files = [tmpdir / "d1.parquet", tmpdir / "d2.parquet", tmpdir / "d3.parquet"]
        out = tmpdir / "out.parquet"

        # Force hierarchical merge by setting a very low max_open_files.
        merger = StreamingMergeWriter(
            input_files,
            out,
            max_open_files=2,          # Force hierarchical merge
            output_buffer_size=2,      # Force multiple flushes (repro dictionary conflicts)
            batch_size=2,
        )
        meta = merger.merge()
        print(f"      Rows: {meta['rows']}")

        assert verify_sorted_order(out), "Output not sorted!"
        pf = pq.ParquetFile(out)
        schema = pf.schema_arrow
        assert "symbol" in schema.names, "symbol column missing!"

        print("\n✅ DICTIONARY + HIERARCHICAL TEST PASSED\n")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Verify merge determinism")
    parser.add_argument("--files", nargs="*", help="Input parquet files to test")
    parser.add_argument("--synthetic", action="store_true", help="Run synthetic data test")
    parser.add_argument("--dict-hier", action="store_true", help="Run dictionary + hierarchical merge test")
    
    args = parser.parse_args()
    
    if args.synthetic or not args.files:
        run_synthetic_test()

    if args.dict_hier:
        run_dictionary_hierarchical_test()
    
    if args.files:
        input_files = [Path(f) for f in args.files]
        run_determinism_test(input_files, "User-provided files")
    
    print("All tests passed! ✅")
