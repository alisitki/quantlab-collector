import os
import boto3
import pyarrow.parquet as pq
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import tempfile
import argparse
from datetime import datetime, timedelta

load_dotenv('/home/deploy/quantlab-backend/core/.env')
ENDPOINT = os.getenv('S3_COMPACT_ENDPOINT')
BUCKET = os.getenv('S3_COMPACT_BUCKET')
ACCESS_KEY = os.getenv('S3_COMPACT_ACCESS_KEY')
SECRET_KEY = os.getenv('S3_COMPACT_SECRET_KEY')

def validate_compact_file(s3, key):
    """Download and validate a compact parquet file."""
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=True) as tmp:
        try:
            s3.download_file(BUCKET, key, tmp.name)
            pf = pq.ParquetFile(tmp.name)
            # Full read test
            for _ in pf.iter_batches(batch_size=100_000):
                pass
            return True, pf.metadata.num_rows, None
        except Exception as e:
            return False, 0, str(e)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str, help="Date to verify (YYYYMMDD)")
    args = parser.parse_args()

    date_str = args.date
    if not date_str:
        date_str = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    
    s3 = boto3.client('s3', endpoint_url=ENDPOINT, 
                      aws_access_key_id=ACCESS_KEY, 
                      aws_secret_access_key=SECRET_KEY)
    
    print(f"Scanning compact bucket for date={date_str}...")
    paginator = s3.get_paginator('list_objects_v2')
    keys = []
    for page in paginator.paginate(Bucket=BUCKET):
        for obj in page.get('Contents', []):
            if f"date={date_str}" in obj['Key'] and obj['Key'].endswith('.parquet'):
                keys.append(obj['Key'])
    
    print(f"Found {len(keys)} parquet files to verify.")
    if not keys:
        return

    results = {'ok': 0, 'fail': 0, 'total_rows': 0}
    failures = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(validate_compact_file, s3, key): key for key in keys}
        for i, future in enumerate(as_completed(futures)):
            key = futures[future]
            ok, rows, err = future.result()
            if ok:
                results['ok'] += 1
                results['total_rows'] += rows
            else:
                results['fail'] += 1
                failures.append((key, err))
            if (i + 1) % 10 == 0 or (i + 1) == len(keys):
                print(f"Progress: {i+1}/{len(keys)}", end='\r')
    
    print(f"\n\n{'='*60}")
    print(f"COMPACT INTEGRITY CHECK: {date_str}")
    print(f"{'='*60}")
    print(f"Files Scanned: {len(keys)}")
    print(f"Valid:         {results['ok']}")
    print(f"Invalid:       {results['fail']}")
    print(f"Total Rows:    {results['total_rows']:,}")
    
    if failures:
        print(f"\nFAILED FILES:")
        for k, e in failures:
            print(f"  - {k}: {e[:80]}")
    else:
        print(f"\n✅ ALL FILES VALID")

if __name__ == "__main__":
    main()
