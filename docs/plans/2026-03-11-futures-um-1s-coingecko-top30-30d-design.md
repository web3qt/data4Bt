# Futures UM 1s CoinGecko Top30 30D Design

## Goal

Switch the current `futures_um` `1s` workflow from liquidity-ranked Top30 symbols to a fixed explicit list of 30 Binance USDT-M perpetual symbols selected from the current CoinGecko market-cap ranking. Keep using the existing ClickHouse table `data4BT_futures_um_1s.klines_1s`, but isolate state, logs, and run/watch scripts for this new workflow.

## Chosen Approach

Add an optional explicit symbol list to config. When the list is present, the existing top30 recent-window runner will use that list directly and skip the liquidity-ranking query. When the list is absent, the runner will continue to use the existing liquidity-based ranking behavior.

This keeps the code change small, avoids a second runner implementation, and preserves the current workflow for the liquidity-ranked case.

## Scope

- Add `binance.explicit_symbols` to config.
- Teach the current recent-window runner to prefer explicit symbols when configured.
- Add a dedicated CoinGecko Top30 30D config and run/watch scripts.
- Reuse the existing `2026-02-09 00:00:00` to `2026-03-09 23:59:59` window.
- Stop the completed liquidity-ranked runner and start the new CoinGecko-ranked workflow state.
- Keep the shared table; do not create a new database or table.

## Non-Goals

- No automatic CoinGecko API call inside the Go application for this change.
- No deletion of unrelated legacy symbols outside the explicitly selected workflow unless required for the current selected-symbol scope.
- No change to the aggTrades -> 1s aggregation logic.

## Symbol Universe

The explicit Binance USDT-M symbol list for this run is:

`BTCUSDT, ETHUSDT, BNBUSDT, XRPUSDT, USDCUSDT, SOLUSDT, TRXUSDT, DOGEUSDT, ADAUSDT, BCHUSDT, HYPEUSDT, XMRUSDT, LINKUSDT, CCUSDT, XLMUSDT, LTCUSDT, AVAXUSDT, HBARUSDT, SUIUSDT, ZECUSDT, TONUSDT, WLFIUSDT, PAXGUSDT, DOTUSDT, MUSDT, UNIUSDT, TAOUSDT, SKYUSDT, ASTERUSDT, AAVEUSDT`

These were selected on `2026-03-11` by taking the current CoinGecko market-cap ranking and filtering to Binance `USDT-M` perpetual symbols.

## Data Handling

The workflow will continue to write into `data4BT_futures_um_1s.klines_1s`.

Because the table is shared, monitoring queries must remain scoped to the explicit symbol list. The workflow state must use a new directory so it does not reuse the prior liquidity-ranked Top30 state.

## Verification

- Unit test the new explicit-symbol selection helper first.
- Run targeted Go tests and build.
- Start the CoinGecko Top30 runner.
- Verify the new state directory, logs, and watch output reflect the explicit 30 symbols and the target window.
