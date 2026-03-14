# Futures UM 1s Latest 15d Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a dedicated workflow that imports Binance USDT-M futures `1s` kline data for the latest 15 complete UTC days into a dedicated ClickHouse database and table.

**Architecture:** Extend the parser and repository so the base interval and base table are configurable, then add a standalone daily-window runner that reuses the existing downloader/importer/state stack without changing the monthly scheduler.

**Tech Stack:** Go, ClickHouse, Binance public data archives, existing importer/state infrastructure.

---

### Task 1: Add failing tests for configurable base interval support

**Files:**
- Modify: `pkg/parser/csv_parser_test.go`
- Modify: `pkg/clickhouse/repository_test.go`

**Step 1: Write the failing tests**

Add tests that expect:
- parser to label and validate `1s` klines correctly
- repository to create and write to `klines_1s` when configured

**Step 2: Run test to verify it fails**

Run:
- `go test ./pkg/parser -run 'TestCSVParser_(Parse1sData|ValidateKLine1s)'`
- `go test ./pkg/clickhouse -run TestRepository_UsesConfiguredBaseTable`

Expected: fail because parser and repository still hardcode `1m`.

### Task 2: Implement configurable parser and repository base table

**Files:**
- Modify: `internal/config/config.go`
- Modify: `pkg/parser/csv_parser.go`
- Modify: `pkg/clickhouse/repository.go`
- Modify: `test/testutils/test_helpers.go`

**Step 1: Add config fields**

Add:
- `parser.interval`
- `database.clickhouse.base_table`

with defaults of `1m` and `klines_1m`.

**Step 2: Implement parser support**

Make parser:
- preserve configured interval on output rows
- validate close-open span for `1s` and `1m`
- compute expected record counts from the configured interval

**Step 3: Implement repository support**

Make the repository use the configured base table for base-table operations.

**Step 4: Re-run targeted tests**

Run the Task 1 commands and expect pass.

### Task 3: Add a dedicated recent-window runner

**Files:**
- Create: `pkg/windowimport/futures_um_1s_runner.go`
- Create: `pkg/windowimport/futures_um_1s_runner_test.go`
- Modify: `cmd/main.go`

**Step 1: Write the failing tests**

Test that the runner:
- computes the latest 15 complete UTC days
- generates daily archive URLs for every symbol/day
- groups tasks per symbol for concurrent import

**Step 2: Run test to verify it fails**

Run:
- `go test ./pkg/windowimport -run 'Test(BuildDailyArchiveURL|BuildLatestWindowTasks)'`

Expected: fail because the package does not exist yet.

**Step 3: Write minimal implementation**

Implement a runner that:
- loads current USDT-M symbols
- seeds isolated per-symbol state totals
- builds per-symbol daily tasks with explicit URLs
- calls `ImportDataConcurrent`

**Step 4: Add command wiring**

Expose a new command through `cmd/main.go`, for example `futures-um-1s-15d`.

### Task 4: Add dedicated config and runner script

**Files:**
- Create: `configs/config-futures-um-1s-15d.yml`
- Create: `run-futures-um-1s-15d`

**Step 1: Add config**

Use:
- `market_type: futures_um`
- `data_path: /data/futures/um/daily/klines`
- `interval: 1s`
- `database: data4BT_futures_um_1s`
- `base_table: klines_1s`
- isolated state/log paths
- disabled materialized views / disabled web monitoring

**Step 2: Add runner script**

Implement a shell wrapper that:
- initializes the database
- runs the new command
- supports foreground/background execution

### Task 5: Verify and attempt the import

**Files:**
- Verify: `configs/config-futures-um-1s-15d.yml`
- Verify: `run-futures-um-1s-15d`

**Step 1: Run targeted tests**

Run:
- `go test ./pkg/parser -run 'TestCSVParser_(Parse1sData|ValidateKLine1s)'`
- `go test ./pkg/windowimport -run 'Test(BuildDailyArchiveURL|BuildLatestWindowTasks)'`

**Step 2: Run smoke verification**

Run:
- `go test ./cmd/...`

**Step 3: Initialize dedicated DB**

Run:
- `go run cmd/main.go -cmd=init-db -config=configs/config-futures-um-1s-15d.yml`

**Step 4: Attempt the real import**

Run:
- `./run-futures-um-1s-15d --bg`

**Step 5: Confirm initial activity**

Verify:
- state files created under `state/futures_um_1s_15d/`
- `data4BT_futures_um_1s.klines_1s` exists
- row count starts increasing or logs show download attempts
