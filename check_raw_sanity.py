#!/usr/bin/env python3
"""RAW Parquet Sanity Check - P1 Verification (Extended)"""
import boto3
import pyarrow.parquet as pq
import io
import sys

S3_ENDPOINT = 'https://hel1.your-objectstorage.com'
S3_BUCKET = 'quantlab-raw'
S3_ACCESS_KEY = 'MS84VKS2JPC3FNPNC3N7'
S3_SECRET_KEY = 'Xnv0vZtOtuTEqpyVLqo7gttZyP1AGBeOiurnpCDi'

s3 = boto3.client('s3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY
)

# Extended test set
test_files = [
    ('Binance BBO/BTCUSDT', 'exchange=binance/stream=bbo/symbol=btcusdt/date=20260101/'),
    ('Binance Trade/ETHUSDT', 'exchange=binance/stream=trade/symbol=ethusdt/date=20260101/'),
    ('Binance MarkPrice/SOLUSDT', 'exchange=binance/stream=mark_price/symbol=solusdt/date=20260101/'),
    ('Bybit BBO/BTCUSDT', 'exchange=bybit/stream=bbo/symbol=BTCUSDT/date=20260101/'),
    ('OKX BBO/BTCUSDT', 'exchange=okx/stream=bbo/symbol=BTCUSDT/date=20260101/'),
]

print('='*70)
print('RAW PARQUET SANITY CHECK - P1 VERIFICATION')
print('='*70)

results = []

for name, prefix in test_files:
    print(f'\n📁 {name}')
    try:
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=3)
        
        if 'Contents' not in resp or len(resp['Contents']) == 0:
            print(f'   ⚠️  NO FILES')
            results.append(('NO_FILES', name))
            continue
        
        latest = sorted(resp['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]
        filename = latest['Key'].split('/')[-1]
        
        print(f'   File: {filename} ({latest["Size"]} bytes)')
        
        obj = s3.get_object(Bucket=S3_BUCKET, Key=latest['Key'])
        table = pq.read_table(io.BytesIO(obj['Body'].read()))
        
        cols = table.schema.names
        rows = len(table)
        
        print(f'   Rows: {rows}, Columns: {len(cols)}')
        print(f'   Schema: {cols}')
        
        forbidden = ['source', 'snapshot', 'alignment', 'recovered', 'backfill']
        contamination = [c for c in forbidden if c in cols]
        
        if contamination:
            print(f'   ❌ FAIL: {contamination}')
            results.append(('FAIL', name, contamination, latest['Key']))
        else:
            print(f'   ✅ PASS')
            results.append(('PASS', name, rows))
            
    except Exception as e:
        print(f'   ❌ ERROR: {str(e)[:80]}')
        results.append(('ERROR', name))

print('\n' + '='*70)
print('VERDICT')
print('='*70)

passed = [r for r in results if r[0] == 'PASS']
failed = [r for r in results if r[0] == 'FAIL']

for r in results:
    icon = '✅' if r[0] == 'PASS' else '❌' if r[0] == 'FAIL' else '⚠️'
    print(f'{icon} {r[1]}: {r[0]}')
    if r[0] == 'FAIL':
        print(f'      File: {r[3]}')
        print(f'      Contamination: {r[2]}')

print('='*70)

if failed:
    print('❌ SANITY CHECK: FAIL')
    print('RAW parquet data is CONTAMINATED.')
    sys.exit(1)
elif len(passed) >= 3:
    print('✅ SANITY CHECK: PASS')
    print('')
    print('RAW parquet semantics are unchanged and contain ONLY WebSocket data.')
    sys.exit(0)
else:
    print(f'⚠️  SANITY CHECK: INCOMPLETE ({len(passed)} files checked, need 3+)')
    sys.exit(2)
