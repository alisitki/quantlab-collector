# Compacted Manifest

Consumer dosyası: `compacted/_manifest.json`

Tek kural:

- `available = true` ise fetch edilir
- `available = false` ise fetch edilmez
- path üretilmez, yalnızca `artifacts.*` kullanılır

## Yapı

```json
{
  "schema_version": 1,
  "updated_at": "2026-04-26T12:06:47.278757Z",
  "dates": {
    "20260425": {
      "exchanges": {
        "binance": {
          "streams": {
            "bbo": {
              "symbols": {
                "btcusdt": {
                  "available": true,
                  "artifacts": {
                    "data_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260425/data.parquet",
                    "meta_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260425/meta.json",
                    "quality_day_key": "exchange=binance/stream=bbo/symbol=btcusdt/date=20260425/quality_day.json"
                  }
                }
              }
            },
            "trade": {
              "symbols": {
                "btcusdt": {
                  "available": false
                }
              }
            }
          }
        }
      }
    }
  }
}
```

## Lookup Path

`dates.{YYYYMMDD}.exchanges.{exchange}.streams.{stream}.symbols.{symbol}`

## Entry

Available entry:

```json
{
  "available": true,
  "artifacts": {
    "data_key": "string",
    "meta_key": "string",
    "quality_day_key": "string"
  }
}
```

Unavailable entry:

```json
{
  "available": false
}
```
