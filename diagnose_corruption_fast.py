#!/usr/bin/env python3
"""
S3 Data Corruption Diagnostic - FAST VERSION
Samples a few files per date instead of scanning everything.
"""
import boto3
import pyarrow.parquet as pq
import io
import sys
import json
from collections import defaultdict

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


def test_file_integrity(key: str) -> tuple:
    """Test a single file for corruption."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = obj['Body'].read()
        
        # Try to read the table
        table = pq.read_table(io.BytesIO(data))
        return ('OK', f'{len(table)} rows')
        
    except Exception as e:
        err_msg = str(e)
        if 'snappy' in err_msg.lower() or 'corrupt' in err_msg.lower() or 'thrift' in err_msg.lower():
            return ('CORRUPT', err_msg[:100])
        else:
            return ('ERROR', err_msg[:100])


def get_quick_overview():
    """Get a quick overview by sampling files."""
    
    # Test data range - get dates available
    print("Step 1: Finding available date ranges...")
    dates_found = set()
    
    # Quick scan - just look at binance bbo for dates (fastest)
    resp = s3.list_objects_v2(
        Bucket=S3_BUCKET, 
        Prefix='exchange=binance/stream=bbo/symbol=btcusdt/date=',
        MaxKeys=1000
    )
    
    for obj in resp.get('Contents', []):
        parts = obj['Key'].split('/')
        for part in parts:
            if part.startswith('date='):
                dates_found.add(part.replace('date=', ''))
    
    dates_sorted = sorted(dates_found)
    print(f"   Found {len(dates_sorted)} dates: {dates_sorted[0]} to {dates_sorted[-1]}")
    
    # Estimate total file count more efficiently
    print("\nStep 2: Estimating total file count...")
    total_estimate = 0
    exchanges = ['binance', 'bybit', 'okx']
    streams = ['bbo', 'trade', 'mark_price', 'funding', 'open_interest']
    
    sample_date = dates_sorted[-1]  # Latest date
    for exchange in exchanges:
        for stream in streams:
            prefix = f'exchange={exchange}/stream={stream}/symbol=btcusdt/date={sample_date}/'
            resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=1)
            if resp.get('KeyCount', 0) > 0:
                # Sample count for this stream on one day
                resp2 = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=1000)
                count = len(resp2.get('Contents', []))
                # Estimate: 3 symbols * days * count
                total_estimate += count * 3 * len(dates_sorted)
    
    print(f"   Estimated total files: ~{total_estimate:,}")
    
    # Sample files from multiple dates and test
    print(f"\nStep 3: Sampling and testing files from each date...")
    
    results = {'OK': 0, 'CORRUPT': 0, 'ERROR': 0}
    corrupt_files = []
    files_tested = 0
    date_stats = {}
    
    for date in dates_sorted:
        date_ok = 0
        date_corrupt = 0
        
        # Test 3 random files from each date
        for exchange in ['binance', 'bybit', 'okx']:
            for stream in ['bbo', 'trade']:
                prefix = f'exchange={exchange}/stream={stream}/symbol=btcusdt/date={date}/'
                resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=3)
                
                for obj in resp.get('Contents', []):
                    key = obj['Key']
                    status, msg = test_file_integrity(key)
                    results[status] += 1
                    files_tested += 1
                    
                    if status == 'CORRUPT':
                        date_corrupt += 1
                        corrupt_files.append((date, key, msg))
                    elif status == 'OK':
                        date_ok += 1
        
        icon = '✅' if date_corrupt == 0 else '❌'
        print(f"   {icon} {date}: tested {date_ok + date_corrupt} files, {date_corrupt} corrupt")
        date_stats[date] = {'ok': date_ok, 'corrupt': date_corrupt}
    
    return {
        'dates': dates_sorted,
        'total_estimate': total_estimate,
        'files_tested': files_tested,
        'results': results,
        'corrupt_files': corrupt_files,
        'date_stats': date_stats,
    }


def main():
    print("=" * 70)
    print("S3 DATA CORRUPTION DIAGNOSTIC (FAST)")
    print("=" * 70)
    print()
    
    data = get_quick_overview()
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    results = data['results']
    total = sum(results.values())
    
    print(f"\nFiles tested: {total}")
    print(f"✅ OK:      {results['OK']} ({results['OK']/total*100:.1f}%)")
    print(f"❌ CORRUPT: {results['CORRUPT']} ({results['CORRUPT']/total*100:.1f}%)")
    print(f"⚠️  ERROR:   {results['ERROR']} ({results['ERROR']/total*100:.1f}%)")
    
    if data['corrupt_files']:
        print("\n\nCorrupted files found:")
        for date, key, msg in data['corrupt_files'][:10]:
            print(f"   📅 {date}: {key.split('/')[-1]}")
            print(f"      {msg[:80]}")
    
    corruption_rate = results['CORRUPT'] / total if total > 0 else 0
    estimated_corrupt = int(data['total_estimate'] * corruption_rate)
    
    print("\n" + "=" * 70)
    print("ESTIMATION & RECOMMENDATION")
    print("=" * 70)
    print(f"\nCorruption rate: ~{corruption_rate*100:.2f}%")
    print(f"Estimated corrupt files: ~{estimated_corrupt:,} (out of ~{data['total_estimate']:,})")
    print(f"Estimated healthy files: ~{data['total_estimate'] - estimated_corrupt:,}")
    
    # Key insight
    print("\n" + "=" * 70)
    print("ROOT CAUSE ANALYSIS")
    print("=" * 70)
    
    if results['CORRUPT'] > 0:
        # Check if corruption is date-specific
        corrupt_dates = set(d for d, _, _ in data['corrupt_files'])
        print(f"\nCorrupt dates: {sorted(corrupt_dates)}")
        
        if len(corrupt_dates) < len(data['dates']) / 2:
            print("\n⚠️  Corruption is LOCALIZED to specific dates - likely a transient issue")
            print("   (network glitch, memory pressure, or system issue during those times)")
        else:
            print("\n⚠️  Corruption is WIDESPREAD - likely a systematic issue")
    else:
        print("\n✅ No corruption detected in sample!")
        print("   The issues may be rare or localized to specific files.")
    
    # Save results
    with open('/opt/quantlab/corruption_diagnostic.json', 'w') as f:
        json.dump(data, f, indent=2, default=str)
    
    print(f"\nResults saved to: /opt/quantlab/corruption_diagnostic.json")
    
    return corruption_rate


if __name__ == '__main__':
    rate = main()
    sys.exit(1 if rate > 0.1 else 0)
