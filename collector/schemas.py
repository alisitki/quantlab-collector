"""
PyArrow schemas for Parquet storage.
Each stream type has its own schema for efficient storage.
"""
import pyarrow as pa


# Common fields for all events
COMMON_FIELDS = [
    ("ts_event", pa.int64()),
    ("ts_recv", pa.int64()),
    ("exchange", pa.string()),
    ("symbol", pa.string()),
    ("stream", pa.string()),
    ("stream_version", pa.int8()),
]


BBO_SCHEMA = pa.schema(COMMON_FIELDS + [
    ("bid_price", pa.float64()),
    ("bid_qty", pa.float64()),
    ("ask_price", pa.float64()),
    ("ask_qty", pa.float64()),
])


TRADE_SCHEMA = pa.schema(COMMON_FIELDS + [
    ("price", pa.float64()),
    ("qty", pa.float64()),
    ("side", pa.int8()),
    ("trade_id", pa.string()),
])


OPEN_INTEREST_SCHEMA = pa.schema(COMMON_FIELDS + [
    ("open_interest", pa.float64()),
])


FUNDING_SCHEMA = pa.schema(COMMON_FIELDS + [
    ("funding_rate", pa.float64()),
    ("next_funding_ts", pa.int64()),
])


MARK_PRICE_SCHEMA = pa.schema(COMMON_FIELDS + [
    ("mark_price", pa.float64()),
    ("index_price", pa.float64()),
])


# Schema registry by stream type
SCHEMAS = {
    "bbo": BBO_SCHEMA,
    "trade": TRADE_SCHEMA,
    "open_interest": OPEN_INTEREST_SCHEMA,
    "funding": FUNDING_SCHEMA,
    "mark_price": MARK_PRICE_SCHEMA,
}


def get_schema(stream_type: str) -> pa.Schema:
    """Get the schema for a given stream type."""
    if stream_type not in SCHEMAS:
        raise ValueError(f"Unknown stream type: {stream_type}")
    return SCHEMAS[stream_type]
