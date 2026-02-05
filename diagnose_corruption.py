#!/usr/bin/env python3
"""
S3 Data Corruption Diagnostic Tool

This script scans S3 and tests a sample of files to determine
the extent of data corruption across the collector's 1-month history.
"""
import boto3
import pyarrow.parquet as pq
import io
import sys
import json
import random
from collections import defaultdict
from datetime import datetime, timedelta

# S3 Configuration
S3_ENDPOINT = 'https://hel1.your-objectstorage.com'
S3_BUCKET = 'quantlab-raw'
S3_ACCESS_KEY = 'MS84VKS2JPC3FNPNC3N7'
S3_SECRET_KEY = 'Xnv0vZtOtuTEqpyVLqo7gttZyP1AGBeOiurnpCDi'

s3 = boto3.client('s3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

def list_all_dates():
    """List all unique dates in the bucket."""
    dates = set()
    
    for exchange in ['binance', 'bybit', 'okx']:
        prefix = f'exchange={exchange}/'
        paginator = s3.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix, Delimiter='/'):
            for common_prefix in page.get('CommonPrefixes', []):
                # Parse date from path
                pass
        
        # Get sample of files to extract dates
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=1000)
        for obj in resp.get('Contents', []):
            parts = obj['Key'].split('/')
            for part in parts:
                if part.startswith('date='):
                    dates.add(part.replace('date=', ''))
    
    return sorted(dates)


def count_files_per_date():
    """Count files per date across all exchanges."""
    date_counts = defaultdict(int)
    
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='exchange='):
        for obj in page.get('Contents', []):
            key = obj['Key']
            parts = key.split('/')
            for part in parts:
                if part.startswith('date='):
                    date = part.replace('date=', '')
                    date_counts[date] += 1
                    break
    
    return dict(sorted(date_counts.items()))


def test_file_integrity(key: str) -> tuple:
    """
    Test a single file for corruption.
    Returns: (status, error_message)
    - status: 'OK', 'CORRUPT', 'ERROR'
    """
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = obj['Body'].read()
        
        # Check PAR1 magic
        if not data.endswith(b'PAR1'):
            return ('CORRUPT', 'Missing PAR1 footer')
        
        # Try to read the table
        table = pq.read_table(io.BytesIO(data))
        
        # Basic validation
        if len(table) == 0:
            return ('OK', 'Empty but valid')
        
        return ('OK', f'{len(table)} rows')
        
    except Exception as e:
        err_msg = str(e)
        if 'snappy' in err_msg.lower() or 'corrupt' in err_msg.lower():
            return ('CORRUPT', err_msg[:100])
        elif 'thrift' in err_msg.lower():
            return ('CORRUPT', err_msg[:100])
        else:
            return ('ERROR', err_msg[:100])


def sample_and_test(sample_size: int = 50):
    """
    Sample files across all dates and test integrity.
    """
    print("Step 1: Counting files per date...")
    date_counts = count_files_per_date()
    
    total_files = sum(date_counts.values())
    print(f"\nTotal files: {total_files}")
    print(f"Date range: {min(date_counts.keys())} to {max(date_counts.keys())}")
    print(f"Days: {len(date_counts)}")
    print("\nFiles per date:")
    for date, count in date_counts.items():
        print(f"  {date}: {count:,} files")
    
    # Collect all files for sampling
    print(f"\n\nStep 2: Sampling {sample_size} random files for integrity test...")
    all_files = []
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='exchange='):
        for obj in page.get('Contents', []):
            all_files.append(obj['Key'])
    
    # Random sample
    sample = random.sample(all_files, min(sample_size, len(all_files)))
    
    # Test each file
    results = {'OK': [], 'CORRUPT': [], 'ERROR': []}
    
    for i, key in enumerate(sample, 1):
        status, msg = test_file_integrity(key)
        results[status].append((key, msg))
        
        icon = '✅' if status == 'OK' else '❌' if status == 'CORRUPT' else '⚠️'
        print(f"[{i}/{len(sample)}] {icon} {key.split('/')[-1][:30]}... : {status}")
    
    return date_counts, total_files, results


def main():
    print("=" * 70)
    print("S3 DATA CORRUPTION DIAGNOSTIC")
    print("=" * 70)
    print()
    
    date_counts, total_files, results = sample_and_test(sample_size=50)
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    ok_count = len(results['OK'])
    corrupt_count = len(results['CORRUPT'])
    error_count = len(results['ERROR'])
    sample_size = ok_count + corrupt_count + error_count
    
    print(f"\nSample size: {sample_size} files (out of {total_files:,} total)")
    print(f"✅ OK:      {ok_count} ({ok_count/sample_size*100:.1f}%)")
    print(f"❌ CORRUPT: {corrupt_count} ({corrupt_count/sample_size*100:.1f}%)")
    print(f"⚠️  ERROR:   {error_count} ({error_count/sample_size*100:.1f}%)")
    
    if corrupt_count > 0:
        print("\n\nCorrupted files:")
        for key, msg in results['CORRUPT']:
            # Extract date from key
            date = [p.replace('date=', '') for p in key.split('/') if p.startswith('date=')][0]
            print(f"  📅 {date} - {key.split('/')[-1]}")
            print(f"     Error: {msg}")
    
    # Calculate estimated corruption rate
    corruption_rate = corrupt_count / sample_size if sample_size > 0 else 0
    estimated_corrupt = int(total_files * corruption_rate)
    
    print("\n" + "=" * 70)
    print("ESTIMATION")
    print("=" * 70)
    print(f"\nBased on sampling:")
    print(f"  Corruption rate: ~{corruption_rate*100:.2f}%")
    print(f"  Estimated corrupt files: ~{estimated_corrupt:,} (out of {total_files:,})")
    print(f"  Estimated healthy files: ~{total_files - estimated_corrupt:,}")
    
    # Save results
    output = {
        'total_files': total_files,
        'sample_size': sample_size,
        'ok': ok_count,
        'corrupt': corrupt_count,
        'error': error_count,
        'corruption_rate': corruption_rate,
        'estimated_corrupt': estimated_corrupt,
        'date_counts': date_counts,
        'corrupt_files': [(k, m) for k, m in results['CORRUPT']],
    }
    
    with open('/opt/quantlab/corruption_diagnostic.json', 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"\nFull results saved to: /opt/quantlab/corruption_diagnostic.json")
    
    return corruption_rate


if __name__ == '__main__':
    rate = main()
    sys.exit(1 if rate > 0.1 else 0)  # Exit 1 if >10% corruption
