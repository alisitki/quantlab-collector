"""Structured JSON logging for QuantLab Collector."""
import json
import sys
from datetime import datetime, timezone
from typing import Optional, Dict, Any


def json_log(
    level: str,
    event: str,
    exchange: Optional[str] = None,
    stream: Optional[str] = None,
    symbol: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None
):
    """
    Emit structured JSON log to STDOUT.
    
    P2: All collector logs must be valid JSON, one object per line.
    
    Args:
        level: INFO, WARN, ERROR
        event: Event name (collector_start, state_change, ws_connect, etc)
        exchange: Optional exchange name
        stream: Optional stream name
        symbol: Optional symbol
        details: Optional structured details dict
    """
    log_obj = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "component": "collector",
        "event": event,
    }
    
    if exchange:
        log_obj["exchange"] = exchange
    if stream:
        log_obj["stream"] = stream
    if symbol:
        log_obj["symbol"] = symbol
    if details:
        log_obj["details"] = details
    
    print(json.dumps(log_obj), flush=True)
