"""
Normalized event models for multi-exchange data collector.
All exchange data is normalized to these models before being queued for writing.
"""
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum


class Exchange(str, Enum):
    BINANCE = "binance"
    BYBIT = "bybit"
    OKX = "okx"


class StreamType(str, Enum):
    BBO = "bbo"
    TRADE = "trade"
    OPEN_INTEREST = "open_interest"
    FUNDING = "funding"
    MARK_PRICE = "mark_price"


STREAM_VERSION = 1


@dataclass
class BBOEvent:
    """Best Bid/Offer event."""
    ts_event: int
    ts_recv: int
    exchange: str
    symbol: str
    stream: str
    bid_price: float
    bid_qty: float
    ask_price: float
    ask_qty: float
    stream_version: int = STREAM_VERSION
    
    def to_dict(self) -> dict:
        return {
            "ts_event": self.ts_event,
            "ts_recv": self.ts_recv,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "stream": self.stream,
            "stream_version": self.stream_version,
            "bid_price": self.bid_price,
            "bid_qty": self.bid_qty,
            "ask_price": self.ask_price,
            "ask_qty": self.ask_qty,
        }


@dataclass
class TradeEvent:
    """Trade event."""
    ts_event: int
    ts_recv: int
    exchange: str
    symbol: str
    stream: str
    price: float
    qty: float
    side: int
    trade_id: str
    stream_version: int = STREAM_VERSION
    
    def to_dict(self) -> dict:
        return {
            "ts_event": self.ts_event,
            "ts_recv": self.ts_recv,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "stream": self.stream,
            "stream_version": self.stream_version,
            "price": self.price,
            "qty": self.qty,
            "side": self.side,
            "trade_id": self.trade_id,
        }


@dataclass
class OpenInterestEvent:
    """Open Interest event."""
    ts_event: int
    ts_recv: int
    exchange: str
    symbol: str
    stream: str
    open_interest: float
    stream_version: int = STREAM_VERSION
    
    def to_dict(self) -> dict:
        return {
            "ts_event": self.ts_event,
            "ts_recv": self.ts_recv,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "stream": self.stream,
            "stream_version": self.stream_version,
            "open_interest": self.open_interest,
        }


@dataclass
class FundingEvent:
    """Funding rate event."""
    ts_event: int
    ts_recv: int
    exchange: str
    symbol: str
    stream: str
    funding_rate: float
    next_funding_ts: int
    stream_version: int = STREAM_VERSION
    
    def to_dict(self) -> dict:
        return {
            "ts_event": self.ts_event,
            "ts_recv": self.ts_recv,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "stream": self.stream,
            "stream_version": self.stream_version,
            "funding_rate": self.funding_rate,
            "next_funding_ts": self.next_funding_ts,
        }


@dataclass
class MarkPriceEvent:
    """Mark price event."""
    ts_event: int
    ts_recv: int
    exchange: str
    symbol: str
    stream: str
    mark_price: float
    index_price: Optional[float] = None
    stream_version: int = STREAM_VERSION
    
    def to_dict(self) -> dict:
        return {
            "ts_event": self.ts_event,
            "ts_recv": self.ts_recv,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "stream": self.stream,
            "stream_version": self.stream_version,
            "mark_price": self.mark_price,
            "index_price": self.index_price,
        }


# Type alias for any event type
Event = BBOEvent | TradeEvent | OpenInterestEvent | FundingEvent | MarkPriceEvent


# P1-A: RAM-only alignment event (NEVER written to parquet)
@dataclass
class AlignmentEvent:
    """
    RAM-only event used for gap tracking alignment after WS reconnects.
    Updates last_ts_event in writer without persisting to RAW parquet.
    
    CRITICAL: This event is NEVER written to storage.
    It exists only to synchronize gap detection after reconnects.
    """
    exchange: str
    symbol: str
    bbo_ts: int = 0
    trade_ts: int = 0
    mark_price_ts: int = 0
    funding_ts: int = 0
    open_interest_ts: int = 0
