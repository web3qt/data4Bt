# Futures UM 1s Top30 90d Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a dedicated workflow that ranks USDT-M symbols by recent 7-day quote volume from the existing `1s` table and backfills the top 30 symbols for the latest 90 complete UTC days into the same table.

**Architecture:** Reuse the existing aggTrades-based `1s` importer, but add a new ranked-symbol runner that queries ClickHouse for the top30 universe and existing per-symbol date ranges before generating daily download tasks. State, logs, and wrapper scripts are isolated from the prior all-symbol workflow even though storage stays in the same ClickHouse table.

**Tech Stack:** Go, ClickHouse, Binance aggTrades daily archives, existing importer/state stack, shell wrappers.

---

### Task 1: Add failing tests for ranked symbol selection and window logic

**Files:**
- Modify: `pkg/windowimport/futures_um_1s_runner_test.go`

**Step 1: Write the failing tests**

Add tests that expect:
- ranking and backfill window helpers to compute complete UTC day windows correctly
- missing-day task generation to skip days already covered by an existing symbol date range

**Step 2: Run test to verify it fails**

Run:
- `go test ./pkg/windowimport -run 'Test(BuildBackfillWindow|BuildRankingWindow|BuildMissingWindowTasks)'`

Expected: fail because the helpers do not exist yet.

### Task 2: Add failing test for downloader timeout preservation

**Files:**
- Modify: `pkg/binance/downloader_test.go`

**Step 1: Write the failing test**

Assert that an explicit configured timeout such as `120s` is preserved instead of being overwritten by the default.

**Step 2: Run test to verify it fails**

Run:
- `go test ./pkg/binance -run TestNewBinanceDownloader_PreservesExplicitTimeout`

Expected: fail if the downloader still forces large configured timeouts back down to `30s`.

### Task 3: Implement window helpers, range skipping, and timeout fix

**Files:**
- Modify: `pkg/windowimport/futures_um_1s_runner.go`
- Modify: `pkg/binance/downloader.go`

**Step 1: Implement helper functions**

Add helpers for:
- ranking window resolution from the latest available UTC day in the table
- 90-day backfill window resolution ending at UTC yesterday
- task generation that skips dates already covered by stored symbol date ranges

**Step 2: Fix timeout handling**

Only apply the `30s` default when no timeout is explicitly configured.

**Step 3: Run targeted tests**

Run:
- `go test ./pkg/windowimport -run 'Test(BuildBackfillWindow|BuildRankingWindow|BuildMissingWindowTasks|BuildLatestWindowTasks)'`
- `go test ./pkg/binance -run 'Test(NewBinanceDownloader|NewBinanceDownloader_PreservesExplicitTimeout)'`

Expected: pass.

### Task 4: Add a dedicated Top30 90d runner and command wiring

**Files:**
- Modify: `pkg/windowimport/futures_um_1s_runner.go`
- Modify: `cmd/main.go`

**Step 1: Implement ranked runner**

Add a new command entry point that:
- queries ClickHouse for the latest available complete day
- ranks the top 30 symbols by 7-day `sum(quote_asset_volume)`
- loads existing date ranges for those symbols
- generates only missing daily tasks across the 90-day backfill window
- imports through the existing aggTrades pipeline

**Step 2: Add command wiring**

Expose a new CLI command, for example `futures-um-1s-top30-90d`.

**Step 3: Re-run command package tests**

Run:
- `go test ./cmd/...`

Expected: pass.

### Task 5: Add dedicated config and wrapper scripts

**Files:**
- Create: `configs/config-futures-um-1s-top30-90d.yml`
- Create: `run-futures-um-1s-top30-90d`
- Create: `watch-futures-um-1s-top30-90d`
- Create: `test/test_watch_futures_um_1s_top30_90d.sh`

**Step 1: Add config**

Use:
- existing database `data4BT_futures_um_1s`
- existing base table `klines_1s`
- isolated state path such as `state/futures_um_1s_top30_90d/progress.json`
- isolated log path such as `logs/futures_um_1s_top30_90d.log`
- `scheduler.batch_days: 90`

**Step 2: Add shell wrappers**

Provide:
- a run script with foreground/background support
- a watch script that reports progress for the new state namespace

**Step 3: Add watch-script test**

Add a focused shell test similar to the existing `watch-futures-um-1s-15d` monitor test.

### Task 6: Verify with real ranking and import attempt

**Files:**
- Verify: `configs/config-futures-um-1s-top30-90d.yml`
- Verify: `run-futures-um-1s-top30-90d`
- Verify: `watch-futures-um-1s-top30-90d`

**Step 1: Run targeted tests**

Run:
- `go test ./pkg/windowimport -run 'Test(BuildBackfillWindow|BuildRankingWindow|BuildMissingWindowTasks|BuildLatestWindowTasks)'`
- `go test ./pkg/binance -run 'Test(NewBinanceDownloader|NewBinanceDownloader_PreservesExplicitTimeout)'`
- `bash test/test_watch_futures_um_1s_top30_90d.sh`

**Step 2: Run smoke verification**

Run:
- `go test ./cmd/...`
- `go build -o ./data4bt cmd/main.go`

**Step 3: Launch the task**

Run:
- `./run-futures-um-1s-top30-90d --bg`

**Step 4: Confirm initial behavior**

Verify:
- logs show `top30` ranking and `90d` window
- the task count is constrained to the selected symbols only
- the state directory is isolated from the previous all-symbol workflow
- the row count starts increasing only for the selected top30 symbols
