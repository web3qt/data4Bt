# Futures UM 1s Latest 15d Design

## Goal

Download Binance USDT-M futures `1s` kline data for the latest 15 complete UTC days into a dedicated ClickHouse database and table, isolated from the existing `1m` workflows.

## Constraints

- The existing scheduler/import path is month-oriented and built around monthly archive discovery.
- The current CSV parser hardcodes `1m` interval validation and labeling.
- The current ClickHouse repository hardcodes `klines_1m` as the base table.
- The new workflow must not reuse the existing futures `1m` database or state files.
- The workflow should tolerate missing official archive files for some symbols or days.

## Chosen Approach

1. Keep the current `1m` monthly workflow intact.
2. Add base-interval configurability to the parser and ClickHouse repository so they can support `1s`.
3. Add a dedicated `futures_um 1s latest 15d` command that:
   - discovers current USDT-M symbols from Binance futures exchange info
   - generates daily archive tasks for the latest 15 complete UTC days
   - uses isolated state files
   - imports into `data4BT_futures_um_1s.klines_1s`
4. Disable materialized views and web monitoring for this workflow by default.
5. Treat missing daily ZIP files as expected gaps and continue.

## Data Flow

1. Load `configs/config-futures-um-1s-15d.yml`.
2. Connect to ClickHouse database `data4BT_futures_um_1s`.
3. Ensure base table `klines_1s` and `symbol_infos` exist.
4. Fetch all active USDT-M futures symbols from Binance.
5. Compute the window `[UTC yesterday - 14 days, UTC yesterday]`.
6. Build daily archive URLs under `data/futures/um/daily/klines/<SYMBOL>/1s/`.
7. Import per-symbol daily tasks through the existing downloader/importer pipeline.
8. Persist progress under `state/futures_um_1s_15d/`.

## Risks

- Binance public archives may not expose `1s` files for every symbol/day even if the market exists.
- The workload is much larger than `1m`; the first run may take substantial time and storage.
- Existing code paths that assume `1m` must remain unchanged for current workflows.
