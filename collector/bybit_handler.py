"""
Bybit v5 WebSocket handler.
Collects: BBO, Trades, Open Interest, Funding Rate, Mark Price from unified streams.
Uses: tickers.{symbol} for BBO + Funding + Mark Price + OI, publicTrade.{symbol} for trades
"""
import asyncio
import json
import time
from typing import Optional
import websockets

from models import (
    BBOEvent, TradeEvent, OpenInterestEvent, FundingEvent, MarkPriceEvent, AlignmentEvent,
    StreamType, Exchange
)
from config import BYBIT_WS_URL, SYMBOLS, RECONNECT_DELAY, MAX_RECONNECT_DELAY


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
        
        async with websockets.connect(BYBIT_WS_URL, ping_interval=20, ping_timeout=10) as ws:
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
            # P0: Non-blocking queue put
            try:
                self.queue.put_nowait(bbo_event)
            except asyncio.QueueFull:
                if self.state:
                    self.state.dropped_events += 1
        
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
            # P0: Non-blocking queue put
            try:
                self.queue.put_nowait(mark_event)
            except asyncio.QueueFull:
                if self.state:
                    self.state.dropped_events += 1
        
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
                # P0: Non-blocking queue put
                try:
                    self.queue.put_nowait(funding_event)
                except asyncio.QueueFull:
                    if self.state:
                        self.state.dropped_events += 1
        
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
                # P0: Non-blocking queue put
                try:
                    self.queue.put_nowait(oi_event)
                except asyncio.QueueFull:
                    if self.state:
                        self.state.dropped_events += 1
    
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
            # P0: Non-blocking queue put
            try:
                self.queue.put_nowait(event)
            except asyncio.QueueFull:
                if self.state:
                    self.state.dropped_events += 1
    
    async def _handle_reconnect(self):
        """Handle reconnection with exponential backoff."""
        print(f"[Bybit] Reconnecting in {self.reconnect_delay}s...")
        await asyncio.sleep(self.reconnect_delay)
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
