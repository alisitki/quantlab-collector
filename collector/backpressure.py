"""
Backpressure module for queue-aware event ingestion.

Implements lossless backpressure to prevent data loss during transient writer stalls
(e.g., S3 GatewayTimeout) while avoiding total system deadlock.

Behavior:
- queue < HIGH_WATERMARK (80%): put_nowait (fast path)
- HIGH_WATERMARK <= queue < CRITICAL_WATERMARK (95%): await put (backpressure)
- queue >= CRITICAL_WATERMARK: priority streams block, trade may drop with logging
"""
import asyncio
import time

from config import (
    BACKPRESSURE_HIGH_WATERMARK,
    BACKPRESSURE_CRITICAL_WATERMARK,
    BACKPRESSURE_TIMEOUT_SECONDS,
    PRIORITY_STREAMS,
)


async def enqueue_event(queue: asyncio.Queue, event, state, stream_type: str) -> bool:
    """
    Queue-aware event ingestion with backpressure support.
    
    Args:
        queue: The asyncio.Queue to put the event into
        event: The event object to enqueue
        state: CollectorState for metrics tracking
        stream_type: Stream type string (e.g., 'bbo', 'trade', 'mark_price')
    
    Returns:
        True if event was enqueued, False if dropped
    """
    queue_size = queue.qsize()
    queue_max = queue.maxsize
    
    if queue_max <= 0:
        # Unbounded queue - always use put_nowait
        queue.put_nowait(event)
        return True
    
    utilization = (queue_size / queue_max) * 100
    
    # Fast path: queue below high watermark
    if utilization < BACKPRESSURE_HIGH_WATERMARK:
        _update_backpressure_mode(state, "normal")
        try:
            queue.put_nowait(event)
            if state:
                state.update_event(stream_type, exchange=getattr(event, 'exchange', None))
            return True
        except asyncio.QueueFull:
            # Race condition: queue filled between check and put
            # Fall through to backpressure path
            pass
    
    # Backpressure path: queue is filling up
    is_priority = stream_type in PRIORITY_STREAMS
    
    if utilization >= BACKPRESSURE_CRITICAL_WATERMARK:
        _update_backpressure_mode(state, "critical")
        
        # Non-priority streams at critical level: drop immediately
        if not is_priority:
            if state:
                state.dropped_events += 1
            _log_drop(stream_type, "critical_non_priority")
            return False
    else:
        _update_backpressure_mode(state, "high")
    
    # Apply backpressure: await with timeout
    start_time = time.time()
    
    if state:
        state.backpressure_active = True
        state.backpressure_events_count += 1
    
    try:
        await asyncio.wait_for(
            queue.put(event),
            timeout=BACKPRESSURE_TIMEOUT_SECONDS
        )
        wait_time = time.time() - start_time
        if state:
            state.update_event(stream_type, exchange=getattr(event, 'exchange', None))
            state.backpressure_wait_seconds_total += wait_time
        return True
        
    except asyncio.TimeoutError:
        # Timeout: controlled drop with alarm
        if state:
            state.dropped_events += 1
            state.backpressure_wait_seconds_total += BACKPRESSURE_TIMEOUT_SECONDS
        _log_drop(stream_type, f"timeout_{BACKPRESSURE_TIMEOUT_SECONDS}s")
        return False
    
    finally:
        # Check if we can exit backpressure mode
        current_util = (queue.qsize() / queue_max) * 100 if queue_max > 0 else 0
        if current_util < BACKPRESSURE_HIGH_WATERMARK:
            if state:
                state.backpressure_active = False


def _update_backpressure_mode(state, new_mode: str):
    """Update backpressure mode with rate-limited logging."""
    if not state:
        return
    
    old_mode = state.backpressure_mode
    if old_mode != new_mode:
        now = time.time()
        # Rate-limit mode transition logs to once per 10 seconds
        if now - state._last_backpressure_log >= 10:
            if new_mode != "normal":
                print(f"[BACKPRESSURE] Mode: {old_mode} -> {new_mode}")
            elif old_mode != "normal":
                print(f"[BACKPRESSURE] Mode: {old_mode} -> {new_mode} (recovered)")
            state._last_backpressure_log = now
        
        state.backpressure_mode = new_mode
        state.backpressure_active = (new_mode != "normal")


def _log_drop(stream_type: str, reason: str):
    """Log event drop with reason."""
    print(f"[BACKPRESSURE] Dropped {stream_type} event: {reason}")
