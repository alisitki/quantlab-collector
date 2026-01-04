"""
Configuration constants for the multi-exchange collector.
"""
import os

# Symbols to collect (USDT-margined perpetual futures)
SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "LINKUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "LTCUSDT",
    "MATICUSDT",
]

# WebSocket endpoints
BINANCE_WS_URL = "wss://fstream.binance.com/stream?streams="
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

# Writer settings
# P12: Size-based flush (min 10MB) or time-based fallback (3 min)
# This prevents uploader backlog from many tiny files.
MAX_PARQUET_MB = 10
MAX_FLUSH_SEC = 180
DATA_DIR = "/opt/quantlab/data"
QUALITY_DIR = "/opt/quantlab/quality"

# Queue settings
QUEUE_MAXSIZE = 500000  # P0: Increased from 100K to prevent saturation-triggered reconnects

# Reconnect settings
RECONNECT_DELAY = 2  # seconds
MAX_RECONNECT_DELAY = 60  # seconds
HEARTBEAT_TIMEOUT = 30  # seconds

# Schema versioning (increment when event structure changes)
STREAM_VERSION = 1

# Storage backend: "local" or "spool"
# "spool" mode: writes to SPOOL_DIR for async uploader to process
# "local" mode: writes to DATA_DIR (legacy, no S3)
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "spool")

# Spool directory for local-first architecture
# Uploader will scan this directory and upload to S3
SPOOL_DIR = os.getenv("SPOOL_DIR", "/opt/quantlab/spool")

# S3 settings (ONLY used by uploader service, NOT by collector)
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "")
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_PREFIX = os.getenv("S3_PREFIX", "")

# API settings
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "9100"))

# ============================================================================
# Silence Interval Detection (inter-event time gap monitoring)
# ============================================================================
# NOTE: This detects "silence" (no events) per stream, NOT sequence gaps.
# For streams with variable frequency (trade), silence is expected and not monitored.
# Thresholds in milliseconds. Set to None to disable detection for a stream.

SILENCE_THRESHOLDS_MS = {
    'bbo': None,         # Disabled: BBO updates vary by market activity
    'trade': None,       # Disabled: Trade frequency highly variable (low-liquidity symbols)
    'mark_price': 15000, # Mark price should update every 1-3s; 15s = stale
    'funding': 120000,   # Funding updates every 8h; 2min silence = unusual
    'open_interest': 120000,  # OI updates every few seconds; 2min = unusual
}

# Rolling window size for silence_intervals_15m metric
SILENCE_WINDOW_SECONDS = 900  # 15 minutes

# ============================================================================
# Backpressure Configuration (queue-aware ingestion policy)
# ============================================================================
# Thresholds as percentage of QUEUE_MAXSIZE
BACKPRESSURE_HIGH_WATERMARK = 80     # Start blocking puts (apply backpressure)
BACKPRESSURE_CRITICAL_WATERMARK = 95 # Only priority streams block, others may drop
BACKPRESSURE_TIMEOUT_SECONDS = 30    # Max wait before controlled drop

# Priority streams (always block at CRITICAL, never drop)
# mark_price, funding, open_interest are critical for derivatives trading
PRIORITY_STREAMS = {'mark_price', 'funding', 'open_interest'}

# ============================================================================
# Adaptive Drain Configuration (queue-aware writer flush acceleration)
# ============================================================================
# When queue exceeds DRAIN_ACCELERATION_THRESHOLD, writer enters "accelerated" mode
# and reduces MIN_FLUSH_GAP_SECONDS from 5s to 1s to increase drain rate.
# Hysteresis: enter at 50%, exit at 40% to prevent oscillation.
DRAIN_ACCELERATION_ENTER_PCT = 50    # Queue % to enter accelerated mode
DRAIN_ACCELERATION_EXIT_PCT = 40     # Queue % to exit accelerated mode
DRAIN_NORMAL_FLUSH_GAP = 5           # MIN_FLUSH_GAP_SECONDS in normal mode
DRAIN_ACCELERATED_FLUSH_GAP = 1      # MIN_FLUSH_GAP_SECONDS in accelerated mode

# ============================================================================
# Reconnect Circuit Breaker (prevents reconnect storms)
# ============================================================================
# If exchange hits RECONNECT_MAX_PER_WINDOW reconnects within RECONNECT_WINDOW_SECONDS,
# pause reconnects for RECONNECT_PAUSE_SECONDS.
RECONNECT_MAX_PER_WINDOW = 5         # Max reconnects before circuit breaker trips
RECONNECT_WINDOW_SECONDS = 60        # Window for counting reconnects
RECONNECT_PAUSE_SECONDS = 120        # Pause duration when circuit breaker trips

# ============================================================================
# Guardrail Metrics Configuration
# ============================================================================
# Warning log if queue_growth_rate exceeds this for SUSTAINED_GROWTH_WARN_SECONDS
SUSTAINED_GROWTH_WARN_THRESHOLD = 2000  # events/sec
SUSTAINED_GROWTH_WARN_SECONDS = 30      # seconds

