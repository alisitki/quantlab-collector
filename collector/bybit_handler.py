"""
Bybit v5 WebSocket handler.
Collects: BBO, Trades, Open Interest, Funding Rate, Mark Price from unified streams.
Uses: tickers.{symbol} for BBO + Funding + Mark Price + OI, publicTrade.{symbol} for trades

P7: Includes reconnect circuit breaker to prevent reconnect storms.
"""
import asyncio
import json
import time
import random
from collections import deque
from typing import Optional
import websockets

from models import (
    BBOEvent, TradeEvent, OpenInterestEvent, FundingEvent, MarkPriceEvent, AlignmentEvent,
    StreamType, Exchange
)
from config import (
    BYBIT_WS_URL, SYMBOLS, RECONNECT_DELAY, MAX_RECONNECT_DELAY,
    RECONNECT_MAX_PER_WINDOW, RECONNECT_WINDOW_SECONDS, RECONNECT_PAUSE_SECONDS,
)
from backpressure import enqueue_event


def normalize_symbol(symbol: str) -> str:
    """Normalize symbol to standard format (BTCUSDT)."""
    return symbol.upper()


class BybitHandler:
    def __init__(self, queue: asyncio.Queue, symbols: list[str] = None, state = None):
        self.queue = queue
        self.symbols = symbols or SYMBOLS
        self.running = False
        self.ws = None
        self.reconnect_delay = RECONNECT_DELAY
        self.state = state  # P0: For tracking drops/reconnects
        
        # Track last values for delta updates (Bybit uses snapshot/delta)
        self._last_oi: dict[str, float] = {}
        self._last_funding: dict[str, tuple] = {}  # (rate, next_ts)
        
        # P7: Circuit breaker state
        self._reconnect_timestamps: deque = deque()
        self._circuit_breaker_until: float = 0
        
    async def start(self):
        """Main entry point - starts the WebSocket connection."""
        self.running = True
        
        while self.running:
            try:
                await self._connect()
            except Exception as e:
                print(f"[Bybit] Connection error: {e}")
                if self.state:
                    self.state.reconnect_counts["bybit"] += 1
                await self._handle_reconnect()
    
    async def _connect(self):
        """Establish WebSocket connection, subscribe, and listen."""
        print(f"[Bybit] Connecting...")
        
        async with websockets.connect(BYBIT_WS_URL, ping_interval=30, ping_timeout=30, close_timeout=10) as ws:
            self.ws = ws
            self.reconnect_delay = RECONNECT_DELAY
            print(f"[Bybit] Connected!")
            
            # P1-A: Fetch snapshot for gap alignment (RAM-only)
            await self._align_gap_tracking()
            
            # Subscribe to streams
            await self._subscribe(ws)
            
            # Start ping task (Bybit requires application-level pings)
            ping_task = asyncio.create_task(self._ping_loop(ws))
            
            try:
                async for msg in ws:
                    if not self.running:
                        break
                    await self._handle_message(msg)
            finally:
                ping_task.cancel()
    
    async def _subscribe(self, ws):
        """Subscribe to all required streams."""
        # Subscribe detail log
        print(f"[Bybit] Subscribing:")
        for symbol in self.symbols:
            print(f"  symbol={symbol} topics=[tickers, publicTrade]")
        
        args = []
        for symbol in self.symbols:
            args.append(f"tickers.{symbol}")       # BBO + Funding + Mark + OI
            args.append(f"publicTrade.{symbol}")   # Trades
        
        subscribe_msg = {
            "op": "subscribe",
            "args": args
        }
        await ws.send(json.dumps(subscribe_msg))
        print(f"[Bybit] Subscribed to {len(args)} topics")
    
    async def _ping_loop(self, ws):
        """Send periodic pings to keep connection alive."""
        while self.running:
            try:
                await asyncio.sleep(20)
                await ws.send(json.dumps({"op": "ping"}))
            except asyncio.CancelledError:
                break
            except Exception:
                pass
    
    async def _handle_message(self, raw_msg: str):
        """Parse and normalize incoming message."""
        ts_recv = int(time.time() * 1000)
        
        try:
            msg = json.loads(raw_msg)
        except json.JSONDecodeError:
            return
        
        # Skip non-data messages
        if "topic" not in msg or "data" not in msg:
            return
        
        topic = msg["topic"]
        data = msg["data"]
        
        try:
            if topic.startswith("tickers."):
                symbol = normalize_symbol(topic.split(".")[1])
                await self._handle_tickers(data, symbol, ts_recv)
            elif topic.startswith("publicTrade."):
                symbol = normalize_symbol(topic.split(".")[1])
                await self._handle_trades(data, symbol, ts_recv)
        except Exception as e:
            print(f"[Bybit] Parse error: {e}")
    
    async def _handle_tickers(self, data: dict, symbol: str, ts_recv: int):
        """Handle tickers stream - contains BBO, funding, mark, OI."""
        ts_event = int(data.get("ts", ts_recv))
        
        # BBO (always present in snapshot, may be delta)
        bid1_price = data.get("bid1Price")
        ask1_price = data.get("ask1Price")
        if bid1_price and ask1_price:
            bbo_event = BBOEvent(
                ts_event=ts_event,
                ts_recv=ts_recv,
                exchange=Exchange.BYBIT.value,
                symbol=symbol,
                stream=StreamType.BBO.value,
                bid_price=float(bid1_price),
                bid_qty=float(data.get("bid1Size", 0)),
                ask_price=float(ask1_price),
                ask_qty=float(data.get("ask1Size", 0)),
            )
            # Backpressure-aware queue put
            await enqueue_event(self.queue, bbo_event, self.state, bbo_event.stream)
        
        # Mark Price
        mark_price = data.get("markPrice")
        if mark_price:
            mark_event = MarkPriceEvent(
                ts_event=ts_event,
                ts_recv=ts_recv,
                exchange=Exchange.BYBIT.value,
                symbol=symbol,
                stream=StreamType.MARK_PRICE.value,
                mark_price=float(mark_price),
                index_price=float(data.get("indexPrice", 0)) or None,
            )
            # Backpressure-aware queue put
            await enqueue_event(self.queue, mark_event, self.state, mark_event.stream)
        
        # Funding Rate
        funding_rate = data.get("fundingRate")
        next_funding = data.get("nextFundingTime")
        if funding_rate and next_funding:
            current = (funding_rate, next_funding)
            if self._last_funding.get(symbol) != current:
                self._last_funding[symbol] = current
                funding_event = FundingEvent(
                    ts_event=ts_event,
                    ts_recv=ts_recv,
                    exchange=Exchange.BYBIT.value,
                    symbol=symbol,
                    stream=StreamType.FUNDING.value,
                    funding_rate=float(funding_rate),
                    next_funding_ts=int(next_funding),
                )
                # Backpressure-aware queue put
                await enqueue_event(self.queue, funding_event, self.state, funding_event.stream)
        
        # Open Interest
        open_interest = data.get("openInterest")
        if open_interest:
            oi_val = float(open_interest)
            if self._last_oi.get(symbol) != oi_val:
                self._last_oi[symbol] = oi_val
                oi_event = OpenInterestEvent(
                    ts_event=ts_event,
                    ts_recv=ts_recv,
                    exchange=Exchange.BYBIT.value,
                    symbol=symbol,
                    stream=StreamType.OPEN_INTEREST.value,
                    open_interest=oi_val,
                )
                # Backpressure-aware queue put
                await enqueue_event(self.queue, oi_event, self.state, oi_event.stream)
    
    async def _handle_trades(self, data: list, symbol: str, ts_recv: int):
        """Handle publicTrade stream."""
        # data is a list of trades
        for trade in data:
            side = 1 if trade.get("S") == "Buy" else -1
            
            event = TradeEvent(
                ts_event=int(trade.get("T", ts_recv)),
                ts_recv=ts_recv,
                exchange=Exchange.BYBIT.value,
                symbol=symbol,
                stream=StreamType.TRADE.value,
                price=float(trade["p"]),
                qty=float(trade["v"]),
                side=side,
                trade_id=str(trade.get("i", "")),
            )
            # Backpressure-aware queue put
            await enqueue_event(self.queue, event, self.state, event.stream)
    
    async def _handle_reconnect(self):
        """Handle reconnection with exponential backoff, jitter, and circuit breaker."""
        now = time.time()
        
        if now < self._circuit_breaker_until:
            remaining = self._circuit_breaker_until - now
            print(f"[Bybit] Circuit breaker active, waiting {remaining:.0f}s...")
            await asyncio.sleep(remaining)
            return
        
        self._reconnect_timestamps.append(now)
        cutoff = now - RECONNECT_WINDOW_SECONDS
        while self._reconnect_timestamps and self._reconnect_timestamps[0] < cutoff:
            self._reconnect_timestamps.popleft()
        
        if len(self._reconnect_timestamps) >= RECONNECT_MAX_PER_WINDOW:
            print(f"[Bybit] Circuit breaker TRIPPED: {len(self._reconnect_timestamps)} reconnects in {RECONNECT_WINDOW_SECONDS}s, pausing {RECONNECT_PAUSE_SECONDS}s")
            self._circuit_breaker_until = now + RECONNECT_PAUSE_SECONDS
            self._reconnect_timestamps.clear()
            await asyncio.sleep(RECONNECT_PAUSE_SECONDS)
            self.reconnect_delay = RECONNECT_DELAY
            return
        
        jittered_delay = self.reconnect_delay * (0.5 + random.random())
        print(f"[Bybit] Reconnecting in {jittered_delay:.1f}s...")
        await asyncio.sleep(jittered_delay)
        self.reconnect_delay = min(self.reconnect_delay * 2, MAX_RECONNECT_DELAY)
    
    async def _align_gap_tracking(self):
        """P1-A: Fetch REST snapshot to align gap tracking (RAM-only)."""
        from rest_snapshot import fetch_bybit_snapshot
        
        for symbol in self.symbols:
            try:
                snapshot = await fetch_bybit_snapshot(symbol)
                if snapshot and self.state:
                    self.state.snapshot_fetches_total += 1
                    alignment_event = AlignmentEvent(
                        exchange="bybit",
                        symbol=symbol,
                        bbo_ts=snapshot.get('bbo_ts', 0),
                        trade_ts=snapshot.get('trade_ts', 0),
                        mark_price_ts=snapshot.get('mark_price_ts', 0),
                        funding_ts=snapshot.get('funding_ts', 0),
                        open_interest_ts=snapshot.get('open_interest_ts', 0)
                    )
                    try:
                        self.queue.put_nowait(alignment_event)
                    except asyncio.QueueFull:
                        pass
            except Exception as e:
                print(f"[Bybit] Alignment failed for {symbol}: {e}")
    
    async def stop(self):
        """Stop the handler."""
        self.running = False
        if self.ws:
            await self.ws.close()


async def bybit_ws_task(queue: asyncio.Queue, symbols: list[str] = None, state = None):
    """Task wrapper for the Bybit handler."""
    handler = BybitHandler(queue, symbols, state)
    await handler.start()
