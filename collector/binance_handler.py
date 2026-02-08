"""
Binance Futures WebSocket handler.
Collects: BBO (bookTicker), Trades (aggTrade), Mark Price + Funding (markPrice@1s)
Note: Open Interest is NOT available via WebSocket on Binance Futures.

P7: Includes reconnect circuit breaker to prevent reconnect storms.
"""
import asyncio
import json
import time
import random
from collections import deque
from typing import Callable
import websockets

from models import (
    BBOEvent, TradeEvent, FundingEvent, MarkPriceEvent, AlignmentEvent,
    StreamType, Exchange
)
from config import (
    BINANCE_WS_URL, SYMBOLS, RECONNECT_DELAY, MAX_RECONNECT_DELAY,
    RECONNECT_MAX_PER_WINDOW, RECONNECT_WINDOW_SECONDS, RECONNECT_PAUSE_SECONDS,
)
from backpressure import enqueue_event


def normalize_symbol(symbol: str) -> str:
    """Normalize symbol to standard format (BTCUSDT)."""
    return symbol.upper()


def build_streams(symbols: list[str]) -> list[str]:
    """Build list of stream names for given symbols."""
    streams = []
    for symbol in symbols:
        s = symbol.lower()
        streams.append(f"{s}@bookTicker")      # BBO
        streams.append(f"{s}@aggTrade")        # Trades
        streams.append(f"{s}@markPrice@1s")    # Mark Price + Funding
    return streams


class BinanceHandler:
    def __init__(self, queue: asyncio.Queue, symbols: list[str] = None, state = None):
        self.queue = queue
        self.symbols = symbols or SYMBOLS
        self.running = False
        self.ws = None
        self.reconnect_delay = RECONNECT_DELAY
        self.state = state  # P0: For tracking drops/reconnects
        
        # P7: Circuit breaker state
        self._reconnect_timestamps: deque = deque()  # Track recent reconnects
        self._circuit_breaker_until: float = 0       # Pause reconnects until this time
        
    async def start(self):
        """Main entry point - starts the WebSocket connection."""
        self.running = True
        
        while self.running:
            try:
                await self._connect()
            except Exception as e:
                print(f"[Binance] Connection error: {e}")
                if self.state:
                    self.state.reconnect_counts["binance"] += 1
                await self._handle_reconnect()
    
    async def _connect(self):
        """Establish WebSocket connection and listen for messages."""
        streams = build_streams(self.symbols)
        url = BINANCE_WS_URL + "/".join(streams)
        
        # Subscribe detail log
        print(f"[Binance] Subscribing:")
        for symbol in self.symbols:
            print(f"  symbol={symbol} streams=[bookTicker, aggTrade, markPrice@1s]")
        
        print(f"[Binance] Connecting to {len(streams)} streams...")
        
        # P15: Increased ping_timeout from 10s to 30s to handle Binance server delays
        # ping_interval=30s, ping_timeout=30s, close_timeout=10s for graceful close
        async with websockets.connect(
            url,
            ping_interval=30,
            ping_timeout=30,
            close_timeout=10,
        ) as ws:
            self.ws = ws
            self.reconnect_delay = RECONNECT_DELAY  # Reset on successful            
            print(f"[Binance] Connected!")
            self.reconnect_delay = RECONNECT_DELAY
            
            # P1-A: Fetch snapshot for gap alignment (RAM-only)
            await self._align_gap_tracking()
            
            async for msg in ws:
                if not self.running:
                    break
                await self._handle_message(msg)
    
    async def _handle_message(self, raw_msg: str):
        """Parse and normalize incoming message."""
        ts_recv = int(time.time() * 1000)
        
        try:
            msg = json.loads(raw_msg)
        except json.JSONDecodeError:
            return
        
        if "stream" not in msg or "data" not in msg:
            return
        
        stream = msg["stream"]
        data = msg["data"]
        symbol = normalize_symbol(stream.split("@")[0])
        
        try:
            if "bookticker" in stream.lower():
                event = self._parse_bbo(data, symbol, ts_recv)
            elif "aggtrade" in stream.lower():
                event = self._parse_trade(data, symbol, ts_recv)
            elif "markprice" in stream.lower():
                # markPrice stream contains both mark price AND funding rate
                await self._handle_mark_price_stream(data, symbol, ts_recv)
                return
            else:
                return
            
            # Backpressure-aware queue put
            await enqueue_event(self.queue, event, self.state, event.stream)
            
        except Exception as e:
            print(f"[Binance] Parse error: {e}")
    
    def _parse_bbo(self, data: dict, symbol: str, ts_recv: int) -> BBOEvent:
        """Parse bookTicker stream."""
        return BBOEvent(
            ts_event=data.get("T", ts_recv),  # Transaction time
            ts_recv=ts_recv,
            exchange=Exchange.BINANCE.value,
            symbol=symbol,
            stream=StreamType.BBO.value,
            bid_price=float(data["b"]),
            bid_qty=float(data["B"]),
            ask_price=float(data["a"]),
            ask_qty=float(data["A"]),
        )
    
    def _parse_trade(self, data: dict, symbol: str, ts_recv: int) -> TradeEvent:
        """Parse aggTrade stream."""
        # m = true means buyer is market maker (sell side for taker)
        side = -1 if data.get("m", False) else 1
        
        return TradeEvent(
            ts_event=data.get("T", ts_recv),  # Trade time
            ts_recv=ts_recv,
            exchange=Exchange.BINANCE.value,
            symbol=symbol,
            stream=StreamType.TRADE.value,
            price=float(data["p"]),
            qty=float(data["q"]),
            side=side,
            trade_id=str(data["a"]),  # Aggregate trade ID
        )
    
    async def _handle_mark_price_stream(self, data: dict, symbol: str, ts_recv: int):
        """Parse markPrice stream - emits both MarkPrice and Funding events."""
        ts_event = data.get("E", ts_recv)
        
        # Mark Price event
        mark_event = MarkPriceEvent(
            ts_event=ts_event,
            ts_recv=ts_recv,
            exchange=Exchange.BINANCE.value,
            symbol=symbol,
            stream=StreamType.MARK_PRICE.value,
            mark_price=float(data["p"]),
            index_price=float(data.get("i", 0)) or None,
        )
        # Backpressure-aware queue put
        await enqueue_event(self.queue, mark_event, self.state, mark_event.stream)
        
        # Funding Rate event (if present)
        if "r" in data and data["r"]:
            funding_event = FundingEvent(
                ts_event=ts_event,
                ts_recv=ts_recv,
                exchange=Exchange.BINANCE.value,
                symbol=symbol,
                stream=StreamType.FUNDING.value,
                funding_rate=float(data["r"]),
                next_funding_ts=int(data.get("T", 0)),
            )
            # Backpressure-aware queue put
            await enqueue_event(self.queue, funding_event, self.state, funding_event.stream)
    
    async def _handle_reconnect(self):
        """
        Handle reconnection with exponential backoff, jitter, and circuit breaker.
        
        P7: Circuit breaker trips after RECONNECT_MAX_PER_WINDOW reconnects
        within RECONNECT_WINDOW_SECONDS, pausing for RECONNECT_PAUSE_SECONDS.
        """
        now = time.time()
        
        # Check if circuit breaker is active
        if now < self._circuit_breaker_until:
            remaining = self._circuit_breaker_until - now
            print(f"[Binance] Circuit breaker active, waiting {remaining:.0f}s...")
            await asyncio.sleep(remaining)
            return
        
        # Track this reconnect attempt
        self._reconnect_timestamps.append(now)
        
        # Evict old timestamps outside window
        cutoff = now - RECONNECT_WINDOW_SECONDS
        while self._reconnect_timestamps and self._reconnect_timestamps[0] < cutoff:
            self._reconnect_timestamps.popleft()
        
        # Check if circuit breaker should trip
        if len(self._reconnect_timestamps) >= RECONNECT_MAX_PER_WINDOW:
            print(f"[Binance] Circuit breaker TRIPPED: {len(self._reconnect_timestamps)} reconnects in {RECONNECT_WINDOW_SECONDS}s, pausing {RECONNECT_PAUSE_SECONDS}s")
            self._circuit_breaker_until = now + RECONNECT_PAUSE_SECONDS
            self._reconnect_timestamps.clear()
            await asyncio.sleep(RECONNECT_PAUSE_SECONDS)
            self.reconnect_delay = RECONNECT_DELAY  # Reset backoff after pause
            return
        
        # Apply jitter: delay * (0.5 + random 0-1)
        jittered_delay = self.reconnect_delay * (0.5 + random.random())
        print(f"[Binance] Reconnecting in {jittered_delay:.1f}s...")
        await asyncio.sleep(jittered_delay)
        
        # Exponential backoff for next attempt
        self.reconnect_delay = min(self.reconnect_delay * 2, MAX_RECONNECT_DELAY)
    
    async def _align_gap_tracking(self):
        """P1-A: Fetch REST snapshot to align gap tracking (RAM-only, not written)."""
        from rest_snapshot import fetch_binance_snapshot
        
        for symbol in self.symbols:
            try:
                snapshot = await fetch_binance_snapshot(symbol)
                if snapshot and self.state:
                    self.state.snapshot_fetches_total += 1
                    
                    # Send alignment event to writer (NOT persisted)
                    alignment_event = AlignmentEvent(
                        exchange="binance",
                        symbol=symbol,
                        bbo_ts=snapshot.get('bbo_ts', 0),
                        trade_ts=snapshot.get('trade_ts', 0),
                        mark_price_ts=snapshot.get('mark_price_ts', 0),
                        funding_ts=snapshot.get('funding_ts', 0),
                        open_interest_ts=0
                    )
                    
                    try:
                        self.queue.put_nowait(alignment_event)
                        print(f"[Binance] Alignment sent for {symbol}")
                    except asyncio.QueueFull:
                        pass
            except Exception as e:
                print(f"[Binance] Alignment failed for {symbol}: {e}")
    
    async def stop(self):
        """Stop the handler."""
        self.running = False
        if self.ws:
            await self.ws.close()


async def binance_ws_task(queue: asyncio.Queue, symbols: list[str] = None, state = None):
    """Task wrapper for the Binance handler."""
    handler = BinanceHandler(queue, symbols, state)
    await handler.start()
