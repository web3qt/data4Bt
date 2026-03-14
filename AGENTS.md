# Repository Rules

## Futures UM History Coverage

- The long-term goal of this repository is to progressively complete Binance USDT-M futures `1m` history.
- When extending the historical window backward, the symbol universe must be all USDT-M contracts that existed in the target window, not only the symbols that are trading today.
- Historical backfill workflows must keep their own state directories and monitoring files so they do not reuse state from current-window tasks.
