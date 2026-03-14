# Futures UM 1s Top30 90d Design

## Goal

Select the top 30 USDT-M futures symbols by recent 7-day quote volume and backfill their recent 90 complete UTC days of `1s` data into the existing `data4BT_futures_um_1s.klines_1s` table.

## Constraints

- Do not create a new ClickHouse database or table.
- Do not continue processing non-top30 symbols for this task.
- Do not reuse the previous all-symbol `1s` task state directory.
- Keep the symbol selection deterministic within one run.
- Reuse the existing aggTrades-to-`1s` import path.

## Chosen Approach

1. Add a new dedicated command for `futures_um 1s top30 90d`.
2. Rank symbols from the existing `klines_1s` table using the latest 7 complete UTC days currently present in ClickHouse:
   - `sum(quote_asset_volume)` descending
   - top 30 symbols only
3. Build a 90 complete UTC day backfill window ending at UTC yesterday.
4. Query existing per-symbol date ranges from ClickHouse and skip days already covered by the existing range, so the task only fills missing days instead of redownloading the recent overlap.
5. Store progress and logs in a new isolated state/log namespace, while still writing rows into the existing `data4BT_futures_um_1s.klines_1s`.

## Data Flow

1. Load a dedicated config for `top30 90d`.
2. Connect to `data4BT_futures_um_1s.klines_1s`.
3. Query the table for the latest available complete UTC day.
4. Compute the 7-day ranking window from that day backward.
5. Select the top 30 symbols by `sum(quote_asset_volume)`.
6. Compute the 90-day backfill window ending at UTC yesterday.
7. Query existing date ranges for those 30 symbols.
8. Generate daily aggTrades tasks only for days outside the existing stored range.
9. Import tasks through the current downloader, aggTrades parser, and repository.
10. Persist progress under a new state directory.

## Risks

- If the current `klines_1s` table is stale, the top30 ranking will reflect the latest complete day already present in the table rather than wall-clock yesterday.
- Date-range skipping is range-based, not gap-based; if a selected symbol has holes inside an already covered range, this workflow will not detect those holes.
- Ranking and backfill windows use different anchors by design:
  - ranking uses the latest complete day already present in the table
  - backfill uses UTC yesterday

## Scope Decisions

- Keep existing rows for non-top30 symbols untouched.
- Do not delete old rows outside the selected top30 universe.
- Do not build a separate liquidity snapshot table; ranking is computed on demand at task start.
