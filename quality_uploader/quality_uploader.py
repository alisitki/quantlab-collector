#!/usr/bin/env python3
"""
QuantLab Quality JSON Uploader Service (Fast Mode)

Scans local quality directory and uploads JSON files to S3.
Optimized for fast initial backlog processing.
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
from typing import Optional, List, Set, Dict

from dotenv import load_dotenv
load_dotenv("/opt/quantlab/.env")


# =============================================================================
# Configuration
# =============================================================================
QUALITY_DIR = os.getenv("QUALITY_DIR", "/opt/quantlab/quality")
STATE_FILE = os.getenv("QUALITY_UPLOADER_STATE", "/opt/quantlab/quality_uploader/quality_uploader_state.json")

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "")
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_QUALITY_PREFIX = os.getenv("S3_QUALITY_PREFIX", "quality")

# FAST settings for backlog
SCAN_INTERVAL_SECONDS = 5      # Fast scan during backlog
MAX_CONCURRENT_UPLOADS = 16    # More parallelism
BATCH_SIZE = 50                # Process 50 files per scan

RETRY_BACKOFF = [5, 15, 60, 300]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("quality-uploader")


# =============================================================================
# State Management
# =============================================================================
class UploaderState:
    """Tracks uploaded files by mtime. Batches saves for performance."""
    
    def __init__(self, state_file: str):
        self.state_file = state_file
        self.uploaded: Dict[str, float] = {}
        self._dirty = False
        self._load()
    
    def _load(self):
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, "r") as f:
                    data = json.load(f)
                self.uploaded = data.get("uploaded", {})
                log.info(f"State loaded: {len(self.uploaded)} files tracked")
        except Exception as e:
            log.warning(f"Could not load state: {e}, starting fresh")
            self.uploaded = {}
    
    def save(self):
        """Save state if dirty."""
        if not self._dirty:
            return
        try:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            tmp = self.state_file + ".tmp"
            with open(tmp, "w") as f:
                json.dump({"uploaded": self.uploaded}, f)
            os.replace(tmp, self.state_file)
            self._dirty = False
        except Exception as e:
            log.error(f"Could not save state: {e}")
    
    def needs_upload(self, filepath: str, base_dir: str) -> bool:
        rel_path = os.path.relpath(filepath, base_dir)
        current_mtime = os.path.getmtime(filepath)
        if rel_path not in self.uploaded:
            return True
        return current_mtime > self.uploaded[rel_path]
    
    def mark_uploaded(self, filepath: str, base_dir: str):
        rel_path = os.path.relpath(filepath, base_dir)
        self.uploaded[rel_path] = os.path.getmtime(filepath)
        self._dirty = True


# =============================================================================
# S3 Client
# =============================================================================
_s3_client = None

def get_s3_client():
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
                read_timeout=30, 
                retries={"max_attempts": 1},
                max_pool_connections=MAX_CONCURRENT_UPLOADS
            )
        )
        log.info(f"S3 client initialized: {S3_ENDPOINT}")
    return _s3_client


# =============================================================================
# Uploader
# =============================================================================
def scan_quality_dir() -> List[str]:
    pattern = os.path.join(QUALITY_DIR, "**/*.json")
    files = [f for f in glob.glob(pattern, recursive=True) if not f.endswith(".tmp")]
    files.sort(key=lambda f: os.path.getmtime(f))
    return files


def filepath_to_s3_key(filepath: str) -> str:
    rel_path = os.path.relpath(filepath, QUALITY_DIR)
    if S3_QUALITY_PREFIX:
        return f"{S3_QUALITY_PREFIX.strip('/')}/{rel_path}"
    return rel_path


class QualityUploader:
    def __init__(self, state: UploaderState):
        self.state = state
        self.executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_UPLOADS)
        self.total_uploaded = 0
        self.total_skipped = 0
    
    def _upload_sync(self, filepath: str, s3_key: str) -> bool:
        try:
            s3 = get_s3_client()
            with open(filepath, "rb") as f:
                s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=f, ContentType="application/json")
            return True
        except Exception as e:
            log.warning(f"Upload failed: {os.path.basename(filepath)} - {e}")
            return False
    
    async def upload_batch(self, files: List[str]):
        """Upload a batch of files in parallel."""
        loop = asyncio.get_running_loop()
        
        async def upload_one(filepath):
            s3_key = filepath_to_s3_key(filepath)
            success = await loop.run_in_executor(self.executor, self._upload_sync, filepath, s3_key)
            if success:
                self.state.mark_uploaded(filepath, QUALITY_DIR)
                self.total_uploaded += 1
                return True
            return False
        
        results = await asyncio.gather(*[upload_one(f) for f in files], return_exceptions=True)
        success_count = sum(1 for r in results if r is True)
        log.info(f"Batch complete: {success_count}/{len(files)} uploaded")
    
    async def run(self):
        log.info(f"Quality Uploader starting (FAST MODE)")
        log.info(f"Source: {QUALITY_DIR} -> s3://{S3_BUCKET}/{S3_QUALITY_PREFIX}/")
        log.info(f"Concurrency: {MAX_CONCURRENT_UPLOADS}, Batch: {BATCH_SIZE}")
        
        while True:
            try:
                files = scan_quality_dir()
                pending = [f for f in files if self.state.needs_upload(f, QUALITY_DIR)]
                
                if pending:
                    log.info(f"Found {len(pending)} files to upload")
                    
                    # Process in batches
                    for i in range(0, len(pending), BATCH_SIZE):
                        batch = pending[i:i+BATCH_SIZE]
                        await self.upload_batch(batch)
                        self.state.save()  # Save after each batch
                    
                    log.info(f"Total uploaded: {self.total_uploaded}, State tracked: {len(self.state.uploaded)}")
                else:
                    # No backlog - slow down
                    await asyncio.sleep(55)  # Extra sleep when idle
                    
            except Exception as e:
                log.error(f"Error in main loop: {e}")
            
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)
    
    def shutdown(self):
        log.info("Shutting down...")
        self.state.save()
        self.executor.shutdown(wait=True)


async def main():
    if not S3_ENDPOINT or not S3_BUCKET:
        log.error("S3_ENDPOINT and S3_BUCKET must be set")
        sys.exit(1)
    
    state = UploaderState(STATE_FILE)
    uploader = QualityUploader(state)
    
    try:
        await uploader.run()
    except KeyboardInterrupt:
        pass
    finally:
        uploader.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
