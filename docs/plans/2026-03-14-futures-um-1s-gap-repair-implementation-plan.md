# Futures UM 1s Gap Repair Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a built-in `1s` gap repair command that compares stored daily second counts with Binance official `aggTrades`, repairs mismatched days, and hardens mixed-symbol batch writes.

**Architecture:** Fix mixed-symbol writes in the importer/repository path first, then add a repair workflow that uses the existing downloader and aggTrades parser to compare per-day official seconds vs ClickHouse counts. Repair mode deletes only mismatched `symbol-day` ranges, waits for ClickHouse mutations, reinserts those days directly, and verifies the result before succeeding.

**Tech Stack:** Go, ClickHouse, Binance official daily aggTrades ZIP archives, existing downloader/parser/repository stack.

---

### Task 1: Lock in the mixed-symbol write bug with tests

**Files:**
- Modify: `pkg/importer/importer_test.go`

**Step 1: Write the failing test**

Add a test that flushes a buffer containing more than one symbol and expects the importer to save per-symbol batches instead of forwarding one mixed batch.

**Step 2: Run test to verify it fails**

Run:
- `env CGO_ENABLED=0 go test ./pkg/importer -run TestImporterFlushBufferSavesEachSymbolSeparately -count=1`

Expected: fail on the current implementation.

### Task 2: Add failing tests for `1s` day-count diff logic

**Files:**
- Create: `pkg/windowimport/futures_um_1s_repair_test.go`

**Step 1: Write the failing tests**

Add tests for:
- diffing stored vs official day counts and returning only mismatches
- building repair tasks from mismatch days only

**Step 2: Run tests to verify they fail**

Run:
- `env CGO_ENABLED=0 go test ./pkg/windowimport -run 'Test(DiffDailySecondCounts|BuildRepairTasks)' -count=1`

Expected: fail before implementation.

### Task 3: Fix mixed-symbol flush/save behavior

**Files:**
- Modify: `pkg/importer/importer.go`
- Modify: `pkg/clickhouse/repository.go`

**Step 1: Implement the minimal fix**

- Split importer flush batches by symbol before calling repository save.
- Make repository save robust to mixed-symbol input as a second line of defense.

**Step 2: Re-run targeted tests**

Run:
- `env CGO_ENABLED=0 go test ./pkg/importer -run TestImporterFlushBufferSavesEachSymbolSeparately -count=1`

Expected: pass.

### Task 4: Add reusable `1s` repair helpers

**Files:**
- Create: `pkg/windowimport/futures_um_1s_repair.go`
- Create: `pkg/windowimport/futures_um_1s_repair_test.go`

**Step 1: Implement pure helpers**

Add types/helpers for:
- UTC day iteration
- stored-vs-official day count diffing
- repair task generation from mismatch days

**Step 2: Re-run targeted tests**

Run:
- `env CGO_ENABLED=0 go test ./pkg/windowimport -run 'Test(DiffDailySecondCounts|BuildRepairTasks)' -count=1`

Expected: pass.

### Task 5: Wire a project-native repair command

**Files:**
- Modify: `cmd/main.go`
- Modify: `pkg/clickhouse/repository.go`

**Step 1: Add command wiring**

Expose `futures-um-1s-repair-gaps` in the CLI command switch and help text.

**Step 2: Implement repair flow**

Add logic that:
- resolves target symbols and date window
- queries ClickHouse day counts
- fetches official day counts through downloader + parser
- prints summary in `-dry-run`
- in apply mode, deletes mismatched `symbol-day` ranges, waits for mutations, reimports those days directly, and verifies repaired counts

**Step 3: Re-run focused command/package tests**

Run:
- `env CGO_ENABLED=0 go test ./pkg/importer ./pkg/windowimport ./cmd/... -count=1`

Expected: pass.

### Task 6: Verify end-to-end with the known production symbols

**Files:**
- Verify: `configs/config-futures-um-1s-targeted-gap-repair-20251211-20260309.yml`

**Step 1: Dry-run the new command**

Run the repair command in `-dry-run` mode against:
- `SKYUSDT,MUSDT,USDCUSDT,CCUSDT,WLFIUSDT`
- `2025-12-11` through `2026-03-09`

Expected: the command reports the same mismatch days discovered during the manual investigation.

**Step 2: Apply and verify**

Run the command in apply mode and confirm all mismatched days reach official counts.

**Step 3: Smoke build**

Run:
- `env CGO_ENABLED=0 go build -o ./data4bt cmd/main.go`

Expected: build succeeds.
