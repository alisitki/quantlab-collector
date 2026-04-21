#!/usr/bin/env python3
"""
QuantLab Parquet Compaction Runner
Implements state-based catch-up and REVERSE backfill logic.
"""

import os
import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
from compact import CompactionJob, get_today_date, get_yesterday_date, format_bytes, logger, Colors
from backfill_planner import BackfillPlanner
from state_semantics import entry_counts_as_complete, partition_results_are_complete

import argparse
from datetime import datetime, timedelta, timezone
import signal
import time
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Set, Optional, Any
import multiprocessing

# Global shutdown flags
shutdown_requested = False
shutdown_event = None

def signal_handler(sig, frame):
    global shutdown_requested, shutdown_event
    if not shutdown_requested:
        logger.warning(f"Interrupt received (signal {sig}), requested graceful shutdown...")
        shutdown_requested = True
        if shutdown_event:
            shutdown_event.set()
    else:
        logger.error("Second interrupt received! Forcing immediate exit...")
        # Force kill all children in the same process group
        os.killpg(os.getpgrp(), signal.SIGKILL)
        sys.exit(1)

class StatusReporter:
    def __init__(self, total_partitions: int, date: str, workers: int):
        self.total = total_partitions
        self.completed = 0
        self.success = 0
        self.quarantine = 0
        self.failed = 0
        self.partial = 0
        self.skipped = 0
        self.locked = 0
        self.active_workers = 0
        self.date = date
        self.workers = workers
        self.start_time = time.time()
        
    def update(self, result: Dict):
        self.completed += 1
        status = result.get('status')
        if status == 'success': self.success += 1
        elif status == 'quarantine': self.quarantine += 1
        elif status == 'failed': self.failed += 1
        elif status == 'partial': self.partial += 1
        elif status == 'skipped': self.skipped += 1
        elif status == 'locked': self.locked += 1
        
    def render(self):
        elapsed = time.time() - self.start_time
        pct = (self.completed / self.total * 100) if self.total > 0 else 100
        eta_desc = "..."
        if self.completed > 0:
            avg_time = elapsed / self.completed
            remaining = self.total - self.completed
            # Use active_workers if available, otherwise 1
            effective_workers = self.workers
            eta_sec = (avg_time * remaining) 
            if eta_sec > 3600:
                eta_desc = f"{int(eta_sec//3600)}h {int((eta_sec%3600)//60)}m"
            else:
                eta_desc = f"{int(eta_sec//60)}m {int(eta_sec%60)}s"
            
        sys.stdout.write(f"\r[{self.date}] {self.completed}/{self.total} ({pct:.1f}%) | "
                        f"Active: {min(self.active_workers, self.workers)}/{self.workers} | "
                        f"S:{self.success} Q:{self.quarantine} P:{self.partial} L:{self.locked} F:{self.failed} | "
                        f"ETA: {eta_desc}   ")
        sys.stdout.flush()

def process_partition_wrapper(p_args):
    """Pickleable wrapper for ProcessPoolExecutor"""
    job_cfg, kwargs, s_event = p_args
    from compact import CompactionJob
    
    # Re-initialize job in worker process
    job = CompactionJob(**job_cfg)
    # Set the cross-process shutdown check
    job.check_shutdown = lambda: s_event.is_set()
    
    return job.compact_date_partition(**kwargs)

def main():
    global shutdown_requested
    parser = argparse.ArgumentParser(description='QuantLab Parquet Compaction Runner')
    parser.add_argument('--mode', choices=['daily', 'catch-up', 'backfill', 'cleanup', 'wipe', 'quicktest'], default='catch-up',
                        help='daily: process yesterday. catch-up: process since last state. backfill: reverse catch-up or range.')
    parser.add_argument('--date-from', help='Target start date (YYYYMMDD)')
    parser.add_argument('--date-to', help='Target end date (YYYYMMDD)')
    parser.add_argument('--overwrite', action='store_true', help='Overwrite existing compacted files')
    parser.add_argument('--retry-quarantine', action='store_true', help='Retry partitions previously marked quarantined in state')
    parser.add_argument('--quality-report', action='store_true', help='Only print quality report for last 14 days')
    parser.add_argument('--apply', action='store_true', help='Apply changes (required for cleanup and wipe modes)')
    parser.add_argument('--wipe-after', action='store_true', help='Clear bucket/state after successful run (requires --apply)')
    
    # Granular filters
    parser.add_argument('--exchanges', help='Comma-separated list of exchanges')
    parser.add_argument('--streams', help='Comma-separated list of streams')
    parser.add_argument('--symbols', help='Comma-separated list of symbols')
    parser.add_argument('--symbols-file', help='Path to file with 1 symbol per line')
    
    # Limits
    parser.add_argument('--max-partitions-per-day', type=int, help='Limit partitions processed per day')
    parser.add_argument('--max-symbols', type=int, help='Limit unique symbols processed')
    parser.add_argument('--max-days', type=int, help='Max number of days to process (defaults to 10000 for backfill, 1 for daily)')
    parser.add_argument('--workers', type=int, default=1, help='Number of parallel workers (ProcessPool)')
    
    # Quicktest args
    parser.add_argument('--date', help='Target date for quicktest (YYYYMMDD)')
    parser.add_argument('--quicktest-n', type=int, default=2, help='How many partitions to test')
    parser.add_argument('--quicktest-max-files', type=int, default=400, help='Max files per partition in quicktest')
    parser.add_argument('--wipe-before', dest='wipe_before', action='store_true', help='Perform wipe before quicktest')
    parser.add_argument('--no-wipe-before', dest='wipe_before', action='store_false', help='Skip wipe before quicktest')
    parser.add_argument('--no-wipe-after', dest='wipe_after', action='store_false', help='Skip wipe after quicktest')
    parser.set_defaults(wipe_before=True, wipe_after=None) # None means we decide based on mode
    
    args = parser.parse_args()

    # Post-process defaults for quicktest
    if args.mode == 'quicktest':
        args.apply = True
        if args.wipe_after is None: args.wipe_after = True
    else:
        if args.wipe_after is None: args.wipe_after = False
    global shutdown_event
    
    # Ensure signal handlers are inherited or correctly set
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    manager = multiprocessing.Manager()
    shutdown_event = manager.Event()

    # Load environment
    env_path = Path(__file__).parent.parent / '.env'
    load_dotenv(env_path, override=True)
    
    # Get configuration
    s3_endpoint = os.getenv('S3_ENDPOINT')
    compact_endpoint = os.getenv('S3_COMPACT_ENDPOINT')
    raw_access_key = os.getenv('S3_ACCESS_KEY')
    raw_secret_key = os.getenv('S3_SECRET_KEY')
    raw_bucket = os.getenv('S3_BUCKET', 'quantlab-raw')
    compact_access_key = os.getenv('S3_ACCESS_KEY') # Using RAW keys as default if compact not set
    compact_secret_key = os.getenv('S3_SECRET_KEY')
    
    # Check for specific compact keys
    if os.getenv('S3_COMPACT_ACCESS_KEY'):
        compact_access_key = os.getenv('S3_COMPACT_ACCESS_KEY')
        compact_secret_key = os.getenv('S3_COMPACT_SECRET_KEY')

    compact_bucket = os.getenv('S3_COMPACT_BUCKET', 'quantlab-compact')

    if compact_endpoint and compact_endpoint != s3_endpoint:
        logger.error("S3_COMPACT_ENDPOINT must match S3_ENDPOINT on this deployment")
        sys.exit(1)
    
    if not all([s3_endpoint, raw_access_key, raw_secret_key, compact_access_key, compact_secret_key]):
        logger.error("Missing S3 configuration (raw or compact) in .env")
        sys.exit(1)
        
    # Prep state key based on mode
    from compact import STATE_FILE_KEY
    state_key = STATE_FILE_KEY
    if args.mode == 'quicktest':
        state_key = "compacted/quicktest/_state.json"
        
    # 0. Prep job config for workers (picklable)
    job_cfg = {
        's3_endpoint': s3_endpoint,
        'raw_access_key': raw_access_key,
        'raw_secret_key': raw_secret_key,
        'compact_access_key': compact_access_key,
        'compact_secret_key': compact_secret_key,
        'raw_bucket': raw_bucket,
        'compact_bucket': compact_bucket,
        'state_key': state_key
    }
    
    job = CompactionJob(**job_cfg)
    
    # Attach shutdown check for sequential parts
    job.check_shutdown = lambda: shutdown_requested
    
    # 0. Helper for full wipe
    def perform_wipe(apply_flag: bool):
        logger.info(f"WIPE MODE initiated (Apply: {apply_flag})")
        paginator = job.s3_client_compact.get_paginator('list_objects_v2')
        keys_to_delete = []
        total_size = 0
        
        for page in paginator.paginate(Bucket=compact_bucket):
            for obj in page.get('Contents', []):
                total_size += obj['Size']
                keys_to_delete.append({'Key': obj['Key']})
        
        if not keys_to_delete:
            logger.info("Bucket is already empty.")
            return

        logger.info(f"Candidate for deletion: {len(keys_to_delete)} keys, {format_bytes(total_size)}")
        
        if apply_flag:
            for i in range(0, len(keys_to_delete), 1000):
                chunk = keys_to_delete[i:i+1000]
                job.s3_client_compact.delete_objects(Bucket=compact_bucket, Delete={'Objects': chunk})
            logger.info(f"WIPE COMPLETE: Deleted {len(keys_to_delete)} keys.")
        else:
            logger.info("DRY-RUN: No keys deleted. Use --apply to execute.")

    if args.mode == 'wipe':
        perform_wipe(args.apply)
        sys.exit(0)
    
    job.state_manager.cleanup_stale_locks()
    
    if args.quality_report:
        logger.info("\n" + "=" * 30 + " QUALITY REPORT (Last 14 Days) " + "=" * 30)
        for i in range(14, 0, -1):
            target = (datetime.now(timezone.utc) - timedelta(days=i)).strftime('%Y%m%d')
            try:
                report = job._fetch_quality_data(target)
                s = report['stats']
                logger.info(f"{target}, {report['day_quality']}, {s['bad']}, {s['degraded']}, {s['total_drops']}, {s['binance_offline_total']}")
            except Exception as e:
                logger.error(f"{target}: Error fetching quality data - {e}")
        logger.info("=" * 91 + "\n")
        sys.exit(0)

    # Filtering
    allowed_exchanges = set(args.exchanges.split(',')) if args.exchanges else None
    allowed_streams = set(args.streams.split(',')) if args.streams else None
    allowed_symbols = set()
    if args.symbols: allowed_symbols.update(args.symbols.split(','))
    if args.symbols_file:
        try:
            with open(args.symbols_file, 'r') as f:
                allowed_symbols.update(line.strip() for line in f if line.strip())
        except Exception as e:
            logger.error(f"Error reading symbols-file: {e}")
            sys.exit(1)

    # 1. Determine dates
    last_date = job.state_manager.get_last_compacted_date()
    today = get_today_date()
    
    if args.mode == 'cleanup':
        if not args.date_from:
            logger.error("--date-from is required for cleanup mode")
            sys.exit(1)
        start = args.date_from
        end = args.date_to or start
        curr = datetime.strptime(start, '%Y%m%d')
        stop = datetime.strptime(end, '%Y%m%d')
        dates = []
        while curr <= stop:
            dates.append(curr.strftime('%Y%m%d'))
            curr += timedelta(days=1)
            
        logger.info(f"CLEANUP MODE | Range: {start} to {end} | Apply: {args.apply}")
        for date in dates:
            partitions = job.discover_partitions_for_date(date)
            for p in partitions:
                if allowed_exchanges and p['exchange'] not in allowed_exchanges: continue
                if allowed_streams and p['stream'] not in allowed_streams: continue
                if allowed_symbols and p['symbol'] not in allowed_symbols: continue
                
                prefix = f"exchange={p['exchange']}/stream={p['stream']}/symbol={p['symbol']}/date={date}/"
                logger.info(f"Cleaning partition: {prefix} (Apply: {args.apply})")
                if args.apply:
                    resp = job.s3_client_compact.list_objects_v2(Bucket=compact_bucket, Prefix=prefix)
                    for obj in resp.get('Contents', []):
                        job.s3_client_compact.delete_object(Bucket=compact_bucket, Key=obj['Key'])
                    state = job.state_manager._read_state()
                    key = f"{p['exchange']}/{p['stream']}/{p['symbol']}/{date}"
                    if key in state.get("partitions", {}):
                        del state["partitions"][key]
                        job.s3_client_compact.put_object(Bucket=compact_bucket, Key="compacted/_state.json", Body=json.dumps(state, indent=2).encode('utf-8'))
                else: logger.info("DRY-RUN: Use --apply to execute.")
        sys.exit(0)

    # Date Planning
    missing_dates = []
    if args.mode == 'quicktest':
        logger.info(Colors.colorate(">>> STARTING QUICKTEST MODE <<<", Colors.CYAN))
        # 1. Wipe Before
        if getattr(args, 'wipe_before', True):
            logger.info("Quicktest: Performing --wipe-before")
            perform_wipe(True)
            
        # 2. Target Date
        target_date = args.date
        if not target_date:
            planner = BackfillPlanner(job.discover_dates(), job.state_manager, today)
            dates = planner.plan_reverse()
            if dates:
                target_date = dates[0]
            else:
                target_date = get_yesterday_date()
        
        logger.info(f"QUICKTEST | selected_date={target_date} | today_excluded=YES | workers={args.workers or 2}")
        missing_dates = [target_date]
        
    elif args.mode == 'backfill':
        if args.date_from:
            curr = datetime.strptime(args.date_from, '%Y%m%d')
            stop = datetime.strptime(args.date_to or args.date_from, '%Y%m%d')
            while curr <= stop:
                missing_dates.append(curr.strftime('%Y%m%d'))
                curr += timedelta(days=1)
        else:
            planner = BackfillPlanner(job.discover_dates(), job.state_manager, today)
            missing_dates = planner.plan_reverse()
            # Limit the plan to max_days if specified
            max_d = args.max_days if args.max_days is not None else 10000
            missing_dates = missing_dates[:max_d]
            
            if not missing_dates:
                logger.info(Colors.colorate("DONE (reached raw start)", Colors.GREEN))
                sys.exit(0)
            logger.info(f"REVERSE BACKFILL: Targeting {missing_dates}")
            
    elif args.mode == 'daily':
        yesterday = get_yesterday_date()
        # Idempotent check for yesterday
        planner = BackfillPlanner(job.discover_dates(), job.state_manager, today)
        missing_dates = [d for d in planner.plan_reverse() if d == yesterday]
        if not missing_dates:
            logger.info(Colors.colorate(f"SKIPPING: Yesterday ({yesterday}) already successfully compacted.", Colors.BLUE))
            sys.exit(0)
    else: # catch-up
        raw_dates = sorted(list(job.discover_dates()))
        if last_date is None:
            yesterday = get_yesterday_date()
            if yesterday in raw_dates: missing_dates = [yesterday]
        else:
            missing_dates = [d for d in raw_dates if last_date < d < today]

    if not missing_dates:
        logger.info("No days to process. Task complete.")
        sys.exit(0)
        
    logger.info(f"Scheduled: {missing_dates}")
    job_success = True
    qt_total_p = 0
    t0_qt = time.time()
    failed_results = [] # Global for quicktest diagnostics
    advance_cursor_open = True
    
    for target_date in missing_dates:
        if shutdown_requested: break
        
        # 0. Cleanup stale locks before starting new day
        job.state_manager.cleanup_stale_locks(target_date)
        
        # Day-level Quality Check
        quality_report = job._fetch_quality_data(target_date)
        if quality_report['day_quality'] == 'BAD' and args.mode != 'quicktest':
            logger.warning(Colors.colorate(f"DAY QUARANTINE: {target_date} (Quality is BAD)", Colors.YELLOW))
            job.state_manager.log_day_status(target_date, 'quarantine')
            if not args.overwrite and args.mode not in ['backfill', 'quicktest'] and advance_cursor_open:
                job.state_manager.update_last_compacted_date(target_date)
            continue
            
        partitions = job.discover_partitions_for_date(target_date)
        
        if args.mode == 'quicktest':
            candidate_symbols = args.symbols.split(',') if args.symbols else ['adausdt', 'xrpusdt', 'dogeusdt']
            if args.streams:
                candidate_streams = args.streams.split(',')
            else:
                candidate_streams = ['bbo']

            candidates = [p for p in partitions if 
                         p['symbol'] in candidate_symbols and 
                         p['stream'] in candidate_streams]
            
            if len(candidates) < args.quicktest_n:
                candidates = [p for p in partitions if p['stream'] in candidate_streams]
            
            scored = []
            for p in candidates:
                prefix = f"exchange={p['exchange']}/stream={p['stream']}/symbol={p['symbol']}/date={target_date}/"
                resp = job.s3_client_raw.list_objects_v2(Bucket=job.raw_bucket, Prefix=prefix, MaxKeys=1000)
                count = resp.get('KeyCount', 0)
                if count <= args.quicktest_max_files:
                    scored.append((count, p))
                else:
                    logger.info(f"Quicktest: skipping {p['symbol']} (files={count} > limit={args.quicktest_max_files})")
            
            scored.sort(key=lambda x: x[0])
            filtered = [x[1] for x in scored[:args.quicktest_n]]
            qt_total_p += len(filtered)
            logger.info(f"QUICKTEST | partitions={filtered}")
        else:
            filtered = [p for p in partitions if 
                        (not allowed_exchanges or p['exchange'] in allowed_exchanges) and
                        (not allowed_streams or p['stream'] in allowed_streams) and
                        (not allowed_symbols or p['symbol'] in allowed_symbols)]
            
            if not filtered:
                if args.mode not in ['backfill', 'quicktest']:
                    advance_cursor_open = False
                continue
            if args.max_partitions_per_day: filtered = filtered[:args.max_partitions_per_day]
        
        total_p = len(filtered)
        logger.info(f"\n>>> DATE {target_date} | {total_p} partitions | workers={args.workers}")
        
        reporter = StatusReporter(total_p, target_date, args.workers)
        day_stats = {'success': 0, 'failed': 0, 'quarantine': 0, 'partial': 0, 'skipped': 0, 'aborted': 0, 'locked': 0}
        day_results = []
        total_in, total_out = 0, 0
        t0_day = time.time()
        
        # PARALLEL EXECUTION
        executor = ProcessPoolExecutor(max_workers=args.workers)
        try:
            futures = {}
            for p in filtered:
                if shutdown_requested: break
                
                p_kwargs = {
                    'exchange': p['exchange'],
                    'stream': p['stream'],
                    'symbol': p['symbol'],
                    'date': target_date,
                    'overwrite': args.overwrite,
                    'retry_quarantine': args.retry_quarantine,
                }
                future = executor.submit(process_partition_wrapper, (job_cfg, p_kwargs, shutdown_event))
                futures[future] = p
                reporter.active_workers += 1
                reporter.render()
            
            for future in as_completed(futures):
                if shutdown_requested:
                    break

                p = futures[future]
                reporter.active_workers -= 1
                
                try:
                    res = future.result()
                except Exception as e:
                    logger.error(f"Worker crashed for {p['symbol']}: {e}")
                    res = {'status': 'failed', 'error': str(e)}
                
                reporter.update(res)
                reporter.render()
                day_results.append(res)
                
                # Collect stats
                st = res.get('status', 'unknown')
                day_stats[st] = day_stats.get(st, 0) + 1
                if st == 'success':
                    total_in += res.get('total_size_bytes', 0)
                    total_out += res.get('output_size_bytes', 0)
                
                if (st == 'quarantine' and res.get('error')) or not entry_counts_as_complete(res):
                    job_success = False
                if res.get('error'):
                    failed_results.append(res)
        except KeyboardInterrupt:
            logger.warning("\nForceful shutdown initiated...")
            shutdown_requested = True
            if shutdown_event: shutdown_event.set()
        finally:
            if shutdown_requested:
                # Forcefully cancel pending and kill workers if possible
                for f in futures:
                    if not f.done(): f.cancel()
                executor.shutdown(wait=False, cancel_futures=True)
            else:
                executor.shutdown(wait=True)

        # End of day summary
        duration = time.time() - t0_day
        logger.info(f"\n--- DAY SUMMARY: {target_date} ---")
        logger.info(f"Status: SUCCESS={day_stats.get('success',0)} FAILED={day_stats.get('failed',0)} "
                    f"QUARANTINE={day_stats.get('quarantine',0)} PARTIAL={day_stats.get('partial',0)} "
                    f"LOCKED={day_stats.get('locked',0)}")
        
        if total_in > 0:
            ratio = (total_out / total_in) * 100
            logger.info(f"Stats: {format_bytes(total_in)} -> {format_bytes(total_out)} ({ratio:.1f}%) | {duration:.1f}s")

        day_complete = partition_results_are_complete(day_results)
        if (
            not args.overwrite
            and args.mode not in ['backfill', 'quicktest']
            and advance_cursor_open
            and day_complete
        ):
            job.state_manager.update_last_compacted_date(target_date)
        elif args.mode not in ['backfill', 'quicktest']:
            advance_cursor_open = False

        if args.wipe_after:
            perform_wipe(True)
            
    # Final Result
    duration_total = time.time() - t0_qt
    logger.info("\n" + "="*60)
    if args.mode == 'quicktest':
        success_p = day_stats.get('success', 0)
        failed_p = day_stats.get('failed', 0)
        quarantine_p = day_stats.get('quarantine', 0)
        partial_p = day_stats.get('partial', 0)
        
        if qt_total_p == 0:
            logger.error(Colors.colorate(f"\n!!! QUICKTEST FAILED: no partitions selected (max_files={args.quicktest_max_files}) !!!", Colors.RED))
            sys.exit(1)
            
        if failed_p > 0 or quarantine_p > 0 or partial_p > 0:
            logger.error(Colors.colorate(f"\n!!! QUICKTEST FAILED / INCOMPLETE ({failed_p + quarantine_p + partial_p} items) !!!", Colors.RED))
            for res in failed_results:
                symbol = res.get('symbol', 'unknown')
                stream = res.get('stream', 'unknown')
                date = res.get('date', 'unknown')
                error = res.get('error', 'unknown')
                failing_key = res.get('failing_key')
                stack = res.get('stacktrace', '')
                
                logger.error("-" * 40)
                logger.error(f"FAILURE: {symbol} {stream} {date}")
                logger.error(f"ERROR: {error}")
                if failing_key:
                    logger.error(Colors.colorate(f"FAILING S3 RAW KEY: {failing_key}", Colors.YELLOW))
                    reproducer = (
                        f"python3 -c \"import boto3, pyarrow.parquet as pq, os; "
                        f"from dotenv import load_dotenv; load_dotenv('../.env'); "
                        f"s3 = boto3.client('s3', endpoint_url=os.getenv('S3_ENDPOINT'), "
                        f"aws_access_key_id=os.getenv('S3_ACCESS_KEY'), aws_secret_access_key=os.getenv('S3_SECRET_KEY')); "
                        f"s3.download_file(os.getenv('S3_RAW_BUCKET', 'quantlab-raw'), '{failing_key}', 'repro.parquet'); "
                        f"pf = pq.ParquetFile('repro.parquet'); "
                        f"print('Rows:', pf.metadata.num_rows); "
                        f"print('Schema:', pf.schema_arrow)\""
                    )
                    logger.info(Colors.colorate("MINIMAL REPRODUCER COMMAND:", Colors.CYAN))
                    logger.info(f"\n{reproducer}\n")
                
                if stack:
                    logger.error("STACKTRACE (First 30 lines):")
                    lines = stack.split('\n')
                    logger.error('\n'.join(lines[:30]))
            logger.error("-" * 40)
            
        # Verify .tmp keys are gone & count keys
        final_keys = []
        tmp_keys = []
        try:
            resp = job.s3_client_compact.list_objects_v2(Bucket=job.compact_bucket)
            for obj in resp.get('Contents', []):
                final_keys.append(obj['Key'])
                if obj['Key'].endswith('.tmp'): tmp_keys.append(obj['Key'])
        except: pass
        
        wiped_status = "yes" if args.wipe_after else "no"
        logger.info(Colors.colorate(
            f"QUICKTEST DONE | success={success_p} | failed={failed_p} | quarantined={quarantine_p} | partial={partial_p} | "
            f"elapsed={duration_total:.1f}s | bucket_wiped={wiped_status}",
            Colors.GREEN if failed_p == 0 and quarantine_p == 0 and partial_p == 0 and success_p > 0 and not tmp_keys else Colors.RED
        ))
        
        if tmp_keys:
            logger.error(Colors.colorate(f"FAILED: Found {len(tmp_keys)} orphan .tmp files!", Colors.RED))
            for k in tmp_keys: logger.error(f"  -> {k}")
        else:
            logger.info("CLEAN: No .tmp files found.")
            
        if args.wipe_after:
            logger.info("Quicktest: Performing --wipe-after (state included)")
            perform_wipe(True)
            # Verify bucket is TRULY empty
            try:
                resp = job.s3_client_compact.list_objects_v2(Bucket=job.compact_bucket)
                cnt = resp.get('KeyCount', 0)
                if cnt == 0:
                    logger.info(Colors.colorate("VERIFIED: Bucket key count = 0", Colors.GREEN))
                else:
                    logger.error(Colors.colorate(f"ERROR: Bucket NOT empty after wipe! key count = {cnt}", Colors.RED))
            except: pass
            
    elif shutdown_requested:
        logger.warning(f"JOB COMPLETE (INTERRUPTED)")
    elif job_success:
        logger.info(Colors.colorate("JOB COMPLETE", Colors.GREEN))
    else:
        logger.error(f"JOB COMPLETE (WITH FAILURES)")
    logger.info("="*60 + "\n")

if __name__ == "__main__":
    main()
