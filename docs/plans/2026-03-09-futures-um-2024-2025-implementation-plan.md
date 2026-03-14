# Futures UM 2024-03 To 2025-02 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a dedicated workflow that backfills Binance USDT-M futures `1m` history from `2024-03-01` through `2025-02-28` using all USDT symbols that existed in that historical window.

**Architecture:** Keep the existing `./data4bt -cmd=update-latest` monthly importer as the execution engine. Add a shell-based discovery layer that builds a historical symbol universe from Binance's official S3 listings, seeds an isolated state directory, and drives a dedicated monitor script.

**Tech Stack:** Bash, jq, curl, existing `data4bt` Go binary, ClickHouse.

---

### Task 1: Add a tested shell helper for historical S3 parsing

**Files:**
- Create: `scripts/futures_um_history_lib.sh`
- Create: `test/test_futures_um_history_lib.sh`

**Step 1: Write the failing test**

Write a shell test that expects helper functions to:
- extract symbol directories from S3 XML
- extract monthly ZIP months while ignoring `.CHECKSUM`
- filter months to `[2024-03, 2025-02]`
- URI-encode prefixes containing non-ASCII symbols

**Step 2: Run test to verify it fails**

Run: `bash test/test_futures_um_history_lib.sh`
Expected: fail because the helper file or functions do not exist yet.

**Step 3: Write minimal implementation**

Implement pure shell helpers in `scripts/futures_um_history_lib.sh`.

**Step 4: Run test to verify it passes**

Run: `bash test/test_futures_um_history_lib.sh`
Expected: pass.

### Task 2: Add the dedicated historical futures config and runner

**Files:**
- Create: `configs/config-futures-um-2024-2025.yml`
- Create: `run-futures-um-2024-2025`

**Step 1: Add config**

Clone the existing futures one-year config and change:
- state path to `state/futures_um_2024_2025/progress.json`
- log path to `logs/futures_um_2024_2025.log`
- `scheduler.end_date` to `2025-02-28`

**Step 2: Add runner**

Implement a runner that:
- builds `./data4bt` if needed
- initializes DB tables
- discovers historical window symbols from Binance S3 listings
- writes `symbols.txt`, `timelines.json`, and seeded `progress.json`
- starts `./data4bt -cmd=update-latest -config=configs/config-futures-um-2024-2025.yml`

**Step 3: Verify runner help / one-shot discovery path**

Run the runner in reset mode until discovery artifacts are generated, without claiming backfill completion.

### Task 3: Add the dedicated watch script

**Files:**
- Create: `watch-futures-um-2024-2025`

**Step 1: Reuse the existing watch pattern**

Base it on the existing futures and backfill watch scripts.

**Step 2: Add window-aware progress accounting**

Use `state/futures_um_2024_2025/timelines.json` and `progress.json` to compute:
- expected months
- processed months
- completed symbol count
- top progress rows

**Step 3: Verify the watch script**

Run: `./watch-futures-um-2024-2025 --once`
Expected: it prints state and DB window summaries without crashing.

### Task 4: Persist repository rules

**Files:**
- Create: `AGENTS.md`

**Step 1: Add historical futures rules**

Document that historical backfills must use symbols that existed in the target window and must use isolated state directories.

### Task 5: Verify and start the workflow

**Files:**
- Verify: `configs/config-futures-um-2024-2025.yml`
- Verify: `run-futures-um-2024-2025`
- Verify: `watch-futures-um-2024-2025`

**Step 1: Run targeted verification**

Run:
- `bash test/test_futures_um_history_lib.sh`
- `./watch-futures-um-2024-2025 --once`

**Step 2: Start the backfill**

Run:
- `./run-futures-um-2024-2025 --bg`

**Step 3: Confirm process and initial progress**

Run:
- `./watch-futures-um-2024-2025 --once`

Expected:
- dedicated process running
- isolated state files present
- historical symbol discovery count populated
