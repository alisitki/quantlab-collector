"""
OKX WebSocket handler.
Collects all 5 streams via dedicated channels:
- tickers: BBO
- trades: Trades  
- open-interest: Open Interest
- funding-rate: Funding Rate
- mark-price: Mark Price
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
from config import OKX_WS_URL, SYMBOLS, RECONNECT_DELAY, MAX_RECONNECT_DELAY


def to_okx_symbol(symbol: str) -> str:
    """Convert BTCUSDT to BTC-USDT-SWAP format."""
    # Remove USDT suffix and add -USDT-SWAP
    base = symbol.replace("USDT", "")
    return f"{base}-USDT-SWAP"


def from_okx_symbol(inst_id: str) -> str:
    """Convert BTC-USDT-SWAP to BTCUSDT format."""
    # BTC-USDT-SWAP -> BTCUSDT
    parts = inst_id.split("-")
    if len(parts) >= 2:
        return f"{parts[0]}{parts[1]}"
    return inst_id


class OKXHandler:
    def __init__(self, queue: asyncio.Queue, symbols: list[str] = None, state = None):
        self.queue = queue
        self.symbols = symbols or SYMBOLS
        self.running = False
        self.ws = None
        self.reconnect_delay = RECONNECT_DELAY
        self.state = state  # P0: For tracking drops/reconnects
        
    async def start(self):
        """Main entry point - starts the WebSocket connection."""
        self.running = True
        
        while self.running:
            try:
                await self._connect()
            except Exception as e:
                print(f"[OKX] Connection error: {e}")
                if self.state:
                    self.state.reconnect_counts["okx"] += 1
                await self._handle_reconnect()
    
    async def _connect(self):
        """Establish WebSocket connection, subscribe, and listen."""
        print(f"[OKX] Connecting...")
        
        async with websockets.connect(OKX_WS_URL, ping_interval=20, ping_timeout=10) as ws:
            self.ws = ws
            self.reconnect_delay = RECONNECT_DELAY
            print(f"[OKX] Connected!")
            
            # P1-A: Fetch snapshot for gap alignment (RAM-only)
            await self._align_gap_tracking()
            
            # Subscribe to all channels
            await self._subscribe(ws)
            
            async for msg in ws:
                if not self.running:
                    break
                await self._handle_message(msg)
    
    async def _subscribe(self, ws):
        """Subscribe to all required channels."""
        # Subscribe detail log
        print(f"[OKX] Subscribing:")
        for symbol in self.symbols:
            okx_symbol = to_okx_symbol(symbol)
            print(f"  symbol={okx_symbol} channels=[tickers, trades, open-interest, funding-rate, mark-price]")
        
        args = []
        
        for symbol in self.symbols:
            okx_symbol = to_okx_symbol(symbol)
            
            # All 5 channels
            args.append({"channel": "tickers", "instId": okx_symbol})
            args.append({"channel": "trades", "instId": okx_symbol})
            args.append({"channel": "open-interest", "instId": okx_symbol})
            args.append({"channel": "funding-rate", "instId": okx_symbol})
            args.append({"channel": "mark-price", "instId": okx_symbol})
        
        subscribe_msg = {
            "op": "subscribe",
            "args": args
        }
        await ws.send(json.dumps(subscribe_msg))
        print(f"[OKX] Subscribed to {len(args)} channels")
    
    async def _handle_message(self, raw_msg: str):
        """Parse and normalize incoming message."""
        ts_recv = int(time.time() * 1000)
        
        try:
            msg = json.loads(raw_msg)
        except json.JSONDecodeError:
            return
        
        # Skip subscribe confirmations and errors
        if "event" in msg:
            if msg["event"] == "error":
                print(f"[OKX] Subscription error: {msg}")
            return
        
        if "arg" not in msg or "data" not in msg:
            return
        
        channel = msg["arg"].get("channel")
        inst_id = msg["arg"].get("instId")
        data_list = msg["data"]
        
        if not channel or not data_list:
            return
        
        symbol = from_okx_symbol(inst_id)
        
        try:
            for data in data_list:
                if channel == "tickers":
                    event = self._parse_ticker(data, symbol, ts_recv)
                elif channel == "trades":
                    event = self._parse_trade(data, symbol, ts_recv)
                elif channel == "open-interest":
                    event = self._parse_oi(data, symbol, ts_recv)
                elif channel == "funding-rate":
                    event = self._parse_funding(data, symbol, ts_recv)
                elif channel == "mark-price":
                    event = self._parse_mark_price(data, symbol, ts_recv)
                else:
                    continue
                
                if event:
                    # P0: Non-blocking queue put
                    try:
                        self.queue.put_nowait(event)
                    except asyncio.QueueFull:
                        if self.state:
                            self.state.dropped_events += 1
                    
        except Exception as e:
            print(f"[OKX] Parse error: {e}")
    
    def _parse_ticker(self, data: dict, symbol: str, ts_recv: int) -> BBOEvent:
        """Parse tickers channel for BBO."""
        return BBOEvent(
            ts_event=int(data.get("ts", ts_recv)),
            ts_recv=ts_recv,
            exchange=Exchange.OKX.value,
            symbol=symbol,
            stream=StreamType.BBO.value,
            bid_price=float(data.get("bidPx", 0)),
            bid_qty=float(data.get("bidSz", 0)),
            ask_price=float(data.get("askPx", 0)),
            ask_qty=float(data.get("askSz", 0)),
        )
    
    def _parse_trade(self, data: dict, symbol: str, ts_recv: int) -> TradeEvent:
        """Parse trades channel."""
        side = 1 if data.get("side") == "buy" else -1
        
        return TradeEvent(
            ts_event=int(data.get("ts", ts_recv)),
            ts_recv=ts_recv,
            exchange=Exchange.OKX.value,
            symbol=symbol,
            stream=StreamType.TRADE.value,
            price=float(data["px"]),
            qty=float(data["sz"]),
            side=side,
            trade_id=str(data.get("tradeId", "")),
        )
    
    def _parse_oi(self, data: dict, symbol: str, ts_recv: int) -> OpenInterestEvent:
        """Parse open-interest channel."""
        return OpenInterestEvent(
            ts_event=int(data.get("ts", ts_recv)),
            ts_recv=ts_recv,
            exchange=Exchange.OKX.value,
            symbol=symbol,
            stream=StreamType.OPEN_INTEREST.value,
            open_interest=float(data.get("oi", 0)),
        )
    
    def _parse_funding(self, data: dict, symbol: str, ts_recv: int) -> FundingEvent:
        """Parse funding-rate channel."""
        return FundingEvent(
            ts_event=int(data.get("ts", ts_recv)),
            ts_recv=ts_recv,
            exchange=Exchange.OKX.value,
            symbol=symbol,
            stream=StreamType.FUNDING.value,
            funding_rate=float(data.get("fundingRate", 0)),
            next_funding_ts=int(data.get("nextFundingTime", 0)),
        )
    
    def _parse_mark_price(self, data: dict, symbol: str, ts_recv: int) -> MarkPriceEvent:
        """Parse mark-price channel."""
        return MarkPriceEvent(
            ts_event=int(data.get("ts", ts_recv)),
            ts_recv=ts_recv,
            exchange=Exchange.OKX.value,
            symbol=symbol,
            stream=StreamType.MARK_PRICE.value,
            mark_price=float(data.get("markPx", 0)),
            index_price=None,  # Not in mark-price channel
        )
    
    async def _handle_reconnect(self):
        """Handle reconnection with exponential backoff."""
        print(f"[OKX] Reconnecting in {self.reconnect_delay}s...")
        await asyncio.sleep(self.reconnect_delay)
        self.reconnect_delay = min(self.reconnect_delay * 2, MAX_RECONNECT_DELAY)
    
    async def _align_gap_tracking(self):
        """P1-A: Fetch REST snapshot to align gap tracking (RAM-only)."""
        from rest_snapshot import fetch_okx_snapshot
        
        for symbol in self.symbols:
            try:
                snapshot = await fetch_okx_snapshot(symbol)
                if snapshot and self.state:
                    self.state.snapshot_fetches_total += 1
                    alignment_event = AlignmentEvent(
                        exchange="okx",
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
                print(f"[OKX] Alignment failed for {symbol}: {e}")
    
    async def stop(self):
        """Stop the handler."""
        self.running = False
        if self.ws:
            await self.ws.close()


async def okx_ws_task(queue: asyncio.Queue, symbols: list[str] = None, state = None):
    """Task wrapper for the OKX handler."""
    handler = OKXHandler(queue, symbols, state)
    await handler.start()
