"""
REST snapshot fetchers - RAM-only, never persisted.
Used for gap tracking alignment after WS reconnects.

CRITICAL: Snapshots fetched here are NEVER written to RAW parquet.
They only update last_ts_event in writer's RAM for gap detection accuracy.
"""
import aiohttp
from typing import Optional, Dict


async def fetch_binance_snapshot(symbol: str) -> Optional[Dict[str, int]]:
    """
    Fetch latest timestamps from Binance REST APIs.
    Returns timestamp dict for gap tracking alignment.
    Does NOT return full events - only timestamps.
    """
    async with aiohttp.ClientSession() as session:
        try:
            # BBO snapshot
            url = f"https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol={symbol}"
            async with session.get(url, timeout=2) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # Note: bookTicker doesn't have timestamp, use current time
                    # This is approximate alignment, better than nothing
                    import time
                    current_ts = int(time.time() * 1000)
                    
                    return {
                        'bbo_ts': current_ts,
                        'trade_ts': 0,  # Would need aggTrades call
                        'mark_price_ts': current_ts,
                        'funding_ts': 0,
                        'open_interest_ts': 0
                    }
        except Exception as e:
            print(f"[Snapshot] Binance fetch failed for {symbol}: {e}")
    return None


async def fetch_bybit_snapshot(symbol: str) -> Optional[Dict[str, int]]:
    """
    Fetch latest timestamps from Bybit REST APIs.
    Bybit's tickers endpoint provides all data in one call.
    """
    async with aiohttp.ClientSession() as session:
        try:
            url = f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}"
            async with session.get(url, timeout=2) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('retCode') == 0 and data.get('result', {}).get('list'):
                        ticker = data['result']['list'][0]
                        # Bybit includes timestamp
                        ts = int(ticker.get('time', 0))
                        
                        return {
                            'bbo_ts': ts,
                            'trade_ts': ts,
                            'mark_price_ts': ts,
                            'funding_ts': ts,
                            'open_interest_ts': ts
                        }
        except Exception as e:
            print(f"[Snapshot] Bybit fetch failed for {symbol}: {e}")
    return None


async def fetch_okx_snapshot(symbol: str) -> Optional[Dict[str, int]]:
    """
    Fetch latest timestamps from OKX REST APIs.
    OKX symbol format: BTC-USDT-SWAP
    """
    # Convert BTCUSDT to BTC-USDT-SWAP
    if symbol.endswith('USDT'):
        base = symbol[:-4]
        okx_symbol = f"{base}-USDT-SWAP"
    else:
        return None
    
    async with aiohttp.ClientSession() as session:
        try:
            url = f"https://www.okx.com/api/v5/market/ticker?instId={okx_symbol}"
            async with session.get(url, timeout=2) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('code') == '0' and data.get('data'):
                        ticker = data['data'][0]
                        ts = int(ticker.get('ts', 0))
                        
                        return {
                            'bbo_ts': ts,
                            'trade_ts': ts,
                            'mark_price_ts': ts,
                            'funding_ts': ts,
                            'open_interest_ts': ts
                        }
        except Exception as e:
            print(f"[Snapshot] OKX fetch failed for {symbol}: {e}")
    return None
