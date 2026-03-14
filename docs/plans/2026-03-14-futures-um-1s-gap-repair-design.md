# Futures UM 1s Gap Repair Design

## Goal

Add a project-native workflow that detects and repairs Binance USDT-M futures `1s` data gaps for specific symbols and UTC day windows by comparing stored `klines_1s` rows against Binance official `daily aggTrades`.

## Root Cause

Two separate issues were confirmed during production debugging:

1. The importer buffer can contain multiple symbols, but the ClickHouse repository deduplication path assumes the whole batch belongs to `klines[0].Symbol`. That allows duplicate inserts for later symbols in a mixed batch.
2. The current `1s` window workflows skip a day as soon as any row exists for that UTC date. They do not verify whether the day is complete, so partially imported days remain permanently underfilled unless they are manually deleted first.

## Constraints

- Keep writing into the existing `data4BT_futures_um_1s.klines_1s` table.
- Reuse the existing Binance downloader and aggTrades parser instead of external scripts.
- Support targeted repairs for explicit symbols and bounded UTC day windows.
- Preserve a non-destructive preview mode before any deletes or reimports.
- Avoid relying on prior workflow state when repairing already-partial days.

## Chosen Approach

1. Fix the importer/repository batch handling so mixed-symbol buffers are safe.
2. Add a new command, `futures-um-1s-repair-gaps`, that:
   - resolves target symbols from `-symbols` or config `explicit_symbols`
   - resolves the UTC day window from `-start` / `-end` or scheduler dates
   - computes per-day stored second counts from `klines_1s`
   - downloads Binance official `daily aggTrades` for the same days and counts unique trade seconds via the existing parser
   - reports mismatched `symbol-day` pairs in `-dry-run`
   - in apply mode, deletes only mismatched `symbol-day` ranges and reimports only those days directly through the downloader + parser + repository stack
   - verifies repaired days against official counts before returning success

## Data Flow

1. Load config and CLI overrides.
2. Build the inclusive UTC day range.
3. Query ClickHouse for `count()` grouped by `symbol, toDate(open_time)`.
4. For each `symbol-day`, download official Binance `daily aggTrades` and parse to `1s` klines.
5. Compare stored count vs parsed official second count.
6. If `dry-run`, print mismatch summary and stop.
7. If apply mode:
   - delete stored rows for mismatched `symbol-day` ranges
   - wait for ClickHouse mutations to finish
   - redownload and reinsert only mismatched `symbol-day` data
   - rerun the count comparison for the repaired days

## Scope Decisions

- Do not automatically scan all symbols unless the caller explicitly supplies them through config or flags.
- Do not backfill synthetic `1s` rows for empty official days. If Binance has zero official seconds, the repaired day should remain empty.
- Do not introduce a new database or table.
- Do not reuse generic scheduler/state machinery for the repair loop; direct repair logic is simpler and avoids stale state interference.

## Risks

- Official Binance archive downloads can still timeout on large days, so the repair loop must keep retry behavior from the downloader.
- ClickHouse delete mutations are asynchronous; the repair command must wait for mutations before reinserting.
- The command compares day-level second counts, not OHLCV payload equality. This is enough for the known production issue but not a full content audit.
