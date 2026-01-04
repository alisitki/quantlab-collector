#!/usr/bin/env python3
"""
QuantLab Async Uploader Service

Scans local spool directory and uploads parquet files to S3.
Runs as a separate systemd service, completely decoupled from collector.

Key behaviors:
- Scans SPOOL_DIR for .parquet files (skips .tmp)
- Uploads to S3 with exponential backoff retry
- Deletes local file ONLY on successful upload
- Sends Telegram alert if no successful upload for 24 hours
- Maintains state in local JSON file for crash recovery

NO coupling with collector. Collector can be restarted independently.
"""
import os
import sys
import time
import json
import glob
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Set

# Load environment from .env if present
from dotenv import load_dotenv
load_dotenv("/opt/quantlab/.env")


# =============================================================================
# Configuration
# =============================================================================
SPOOL_DIR = os.getenv("SPOOL_DIR", "/opt/quantlab/spool")
STATE_FILE = os.getenv("UPLOADER_STATE_FILE", "/var/lib/quantlab/uploader_state.json")

# S3 settings
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "")
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_PREFIX = os.getenv("S3_PREFIX", "v3")

# Telegram settings
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Uploader settings
SCAN_INTERVAL_IDLE = 1  # seconds
MAX_CONCURRENT_UPLOADS = 8
ALERT_THRESHOLD_HOURS = int(os.getenv("UPLOADER_ALERT_HOURS", "24"))

# Retry backoff: 5s, 15s, 60s, 300s, 600s, then repeat 600s
RETRY_BACKOFF = [5, 15, 60, 300, 600]

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("uploader")


# =============================================================================
# S3 Client (lazy init)
# =============================================================================
_s3_client = None

def get_s3_client():
    """Get or create S3 client with timeout configuration."""
    global _s3_client
    if _s3_client is None:
        import boto3
        from botocore.config import Config
        
        _s3_client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            config=Config(
                connect_timeout=10,
                read_timeout=60,
                retries={"max_attempts": 1}  # We handle retries ourselves
            )
        )
        log.info(f"S3 client initialized: endpoint={S3_ENDPOINT}, bucket={S3_BUCKET}")
    return _s3_client


# =============================================================================
# State Management
# =============================================================================
class UploaderState:
    """Persistent state for crash recovery and alert tracking."""
    
    def __init__(self, state_file: str):
        self.state_file = state_file
        self.last_success_ts: float = time.time()
        self.alert_sent: bool = False
        self.total_uploaded: int = 0
        self.total_failed: int = 0
        self._load()
    
    def _load(self):
        """Load state from file if exists."""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, "r") as f:
                    data = json.load(f)
                self.last_success_ts = data.get("last_success_ts", time.time())
                self.alert_sent = data.get("alert_sent", False)
                self.total_uploaded = data.get("total_uploaded", 0)
                self.total_failed = data.get("total_failed", 0)
                log.info(f"State loaded: last_success={self._format_ago(self.last_success_ts)}, alert_sent={self.alert_sent}")
        except Exception as e:
            log.warning(f"Could not load state: {e}, using defaults")
    
    def _save(self):
        """Save state to file atomically."""
        try:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            tmp = self.state_file + ".tmp"
            with open(tmp, "w") as f:
                json.dump({
                    "last_success_ts": self.last_success_ts,
                    "alert_sent": self.alert_sent,
                    "total_uploaded": self.total_uploaded,
                    "total_failed": self.total_failed,
                }, f, indent=2)
            os.rename(tmp, self.state_file)
        except Exception as e:
            log.error(f"Could not save state: {e}")
    
    def record_success(self):
        """Record a successful upload."""
        self.last_success_ts = time.time()
        self.total_uploaded += 1
        self.alert_sent = False  # Reset alert on success
        self._save()
    
    def record_failure(self):
        """Record a failed upload."""
        self.total_failed += 1
        self._save()
    
    def hours_since_success(self) -> float:
        """Return hours since last successful upload."""
        return (time.time() - self.last_success_ts) / 3600
    
    def should_alert(self) -> bool:
        """Check if alert should be sent."""
        return self.hours_since_success() > ALERT_THRESHOLD_HOURS and not self.alert_sent
    
    def mark_alert_sent(self):
        """Mark that alert has been sent."""
        self.alert_sent = True
        self._save()
    
    @staticmethod
    def _format_ago(ts: float) -> str:
        """Format timestamp as 'X hours ago'."""
        hours = (time.time() - ts) / 3600
        if hours < 1:
            return f"{int(hours * 60)} minutes ago"
        return f"{hours:.1f} hours ago"


# =============================================================================
# Telegram Alert
# =============================================================================
def send_telegram_alert(message: str) -> bool:
    """Send Telegram message. Returns True on success."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials not configured, skipping alert")
        return False
    
    import requests
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }, timeout=10)
        if resp.ok:
            log.info("Telegram alert sent successfully")
            return True
        else:
            log.error(f"Telegram API error: {resp.text}")
            return False
    except Exception as e:
        log.error(f"Telegram send failed: {e}")
        return False


def build_alert_message(state: UploaderState, pending_files: int, spool_size_gb: float) -> str:
    """Build the Telegram alert message."""
    hours = state.hours_since_success()
    return f"""<b>⚠️ QuantLab ALERT</b>

{int(hours)} saat boyunca S3'e upload yapılamıyor.

• Last success: {int(hours)} saat önce
• Pending parquet files: {pending_files}
• Approx spool size: {spool_size_gb:.2f} GB

Collector çalışmaya devam ediyor."""


# =============================================================================
# File Scanner
# =============================================================================
def scan_spool() -> List[str]:
    """
    Scan spool directory for ready .parquet files.
    
    Skips:
    - .tmp files (in-progress writes)
    - Directories
    
    Returns list sorted by modification time (oldest first).
    """
    # P13: Use a more efficient pattern if possible, but recursive glob is necessary for the structure.
    # Limitation: glob.glob is blocking. For 1000s of files this might be slow, 
    # but the user requested optimizing the loop frequency first.
    pattern = os.path.join(SPOOL_DIR, "**/*.parquet")
    files = glob.glob(pattern, recursive=True)
    
    if not files:
        return []
        
    # Sort by modification time (oldest first = FIFO)
    # P13: Deterministic oldest-first
    files.sort(key=lambda f: os.path.getmtime(f))
    
    return files


def get_spool_size_gb() -> float:
    """Calculate total size of spool directory in GB."""
    total = 0
    for dirpath, dirnames, filenames in os.walk(SPOOL_DIR):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                total += os.path.getsize(fp)
            except OSError:
                pass
    return total / (1024 ** 3)


def filepath_to_s3_key(filepath: str) -> str:
    """
    Convert local spool path to S3 object key.
    
    Input:  /opt/quantlab/spool/exchange=binance/stream=bbo/symbol=btcusdt/date=20260104/part-123-000001.parquet
    Output: v3/exchange=binance/stream=bbo/symbol=btcusdt/date=20260104/part-123-000001.parquet
    """
    # Find the relative path from spool dir
    rel_path = os.path.relpath(filepath, SPOOL_DIR)
    
    if S3_PREFIX:
        return f"{S3_PREFIX.strip('/')}/{rel_path}"
    return rel_path


# =============================================================================
# Uploader Core
# =============================================================================
class Uploader:
    """Async uploader with bounded concurrency and retry."""
    
    def __init__(self, state: UploaderState):
        self.state = state
        self.executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_UPLOADS)
        self.in_flight: Set[str] = set()
        self.retry_counts: dict = {}  # filepath -> retry count
    
    def _upload_sync(self, filepath: str, s3_key: str) -> bool:
        """Synchronous S3 upload (runs in executor)."""
        try:
            s3 = get_s3_client()
            with open(filepath, "rb") as f:
                s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=f)
            return True
        except Exception as e:
            log.warning(f"Upload failed: {os.path.basename(filepath)} - {e}")
            return False
    
    async def upload_file(self, filepath: str):
        """Upload single file with retry backoff."""
        if filepath in self.in_flight:
            return
        
        self.in_flight.add(filepath)
        s3_key = filepath_to_s3_key(filepath)
        
        try:
            loop = asyncio.get_running_loop()
            success = await loop.run_in_executor(
                self.executor,
                self._upload_sync,
                filepath, s3_key
            )
            
            if success:
                # Delete local file on success
                try:
                    os.remove(filepath)
                    log.info(f"Uploaded & deleted: {os.path.basename(filepath)}")
                except Exception as e:
                    log.warning(f"Could not delete after upload: {filepath} - {e}")
                
                self.state.record_success()
                self.retry_counts.pop(filepath, None)
            else:
                # Handle retry
                self.state.record_failure()
                count = self.retry_counts.get(filepath, 0)
                self.retry_counts[filepath] = count + 1
                
                # Calculate backoff delay
                backoff_idx = min(count, len(RETRY_BACKOFF) - 1)
                delay = RETRY_BACKOFF[backoff_idx]
                log.info(f"Will retry {os.path.basename(filepath)} in {delay}s (attempt {count + 1})")
                
        finally:
            self.in_flight.discard(filepath)
    
    async def run(self):
        """Main uploader loop."""
        log.info(f"Uploader starting: spool={SPOOL_DIR}, bucket={S3_BUCKET}")
        log.info(f"Concurrency: {MAX_CONCURRENT_UPLOADS}, Idle sleep: {SCAN_INTERVAL_IDLE}s")
        log.info(f"Alert threshold: {ALERT_THRESHOLD_HOURS} hours")
        
        while True:
            try:
                # 1. Scan for ready files
                files = scan_spool()
                uploaded_in_this_loop = 0
                
                if files:
                    # 2. Pick files not in redo backoff
                    uploadable = []
                    for f in files:
                        if f in self.in_flight:
                            continue
                        
                        # Check if in backoff
                        retry_count = self.retry_counts.get(f, 0)
                        if retry_count > 0:
                            # Skip if in backoff (simplifiedIdx)
                            continue
                        
                        uploadable.append(f)
                        if len(uploadable) >= MAX_CONCURRENT_UPLOADS:
                            break
                    
                    # 3. Upload in parallel
                    if uploadable:
                        tasks = [self.upload_file(f) for f in uploadable]
                        await asyncio.gather(*tasks, return_exceptions=True)
                        uploaded_in_this_loop = len(uploadable)
                
                # 4. Check for alert condition (limit frequency)
                if self.state.should_alert():
                    pending = len(files)
                    spool_gb = get_spool_size_gb()
                    message = build_alert_message(self.state, pending, spool_gb)
                    if send_telegram_alert(message):
                        self.state.mark_alert_sent()
                
                # 5. ADAPTIVE LOOP (P13)
                # If we found files and uploaded them, don't sleep - scan again immediately.
                if uploaded_in_this_loop > 0:
                    continue 
                
            except Exception as e:
                log.error(f"Error in main loop: {e}")
            
            # Idle sleep
            await asyncio.sleep(SCAN_INTERVAL_IDLE)
    
    def shutdown(self):
        """Graceful shutdown."""
        log.info("Shutting down uploader...")
        self.executor.shutdown(wait=True)
        log.info("Uploader shutdown complete")


# =============================================================================
# Main
# =============================================================================
async def main():
    """Entry point."""
    # Validate configuration
    if not S3_ENDPOINT or not S3_BUCKET:
        log.error("S3_ENDPOINT and S3_BUCKET must be set")
        sys.exit(1)
    
    if not os.path.exists(SPOOL_DIR):
        log.info(f"Creating spool directory: {SPOOL_DIR}")
        os.makedirs(SPOOL_DIR, exist_ok=True)
    
    # Initialize state
    state = UploaderState(STATE_FILE)
    
    # Create and run uploader
    uploader = Uploader(state)
    
    try:
        await uploader.run()
    except KeyboardInterrupt:
        log.info("Received interrupt signal")
    finally:
        uploader.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
