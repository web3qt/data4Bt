# Futures UM 1s CoinGecko Top30 30D Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Switch the recent-window `futures_um` `1s` workflow to use a fixed CoinGecko-derived explicit Top30 Binance USDT-M symbol list for the `2026-02-09` to `2026-03-09` window.

**Architecture:** Extend config with an optional explicit symbol list and reuse the existing recent-window runner. If explicit symbols are configured, skip the liquidity-ranking query and import only that list. Keep state/logging/scripts isolated for the CoinGecko workflow while continuing to write into the existing shared `1s` table.

**Tech Stack:** Go, Viper config loading, ClickHouse, Binance public aggTrades archives, shell run/watch scripts.

---

### Task 1: Add explicit-symbol selection support

**Files:**
- Modify: `internal/config/config.go`
- Modify: `pkg/windowimport/futures_um_1s_runner.go`
- Test: `pkg/windowimport/futures_um_1s_runner_test.go`

**Step 1: Write the failing test**

Add a test that verifies an explicit symbol list overrides the default selection path and is normalized.

**Step 2: Run test to verify it fails**

Run: `env CGO_ENABLED=0 GOCACHE=/tmp/go-build go test ./pkg/windowimport -run 'TestResolveConfiguredSymbols'`

Expected: fail because the helper does not exist yet.

**Step 3: Write minimal implementation**

Add `ExplicitSymbols []string` to `BinanceConfig`, implement a helper that trims, de-duplicates, and preserves order, and call it in the recent-window runner before the liquidity-ranking query.

**Step 4: Run test to verify it passes**

Run: `env CGO_ENABLED=0 GOCACHE=/tmp/go-build go test ./pkg/windowimport -run 'TestResolveConfiguredSymbols'`

Expected: PASS.

### Task 2: Add CoinGecko Top30 workflow config and scripts

**Files:**
- Create: `configs/config-futures-um-1s-coingecko-top30-30d.yml`
- Create: `run-futures-um-1s-coingecko-top30-30d`
- Create: `watch-futures-um-1s-coingecko-top30-30d`
- Create: `test/test_watch_futures_um_1s_coingecko_top30_30d.sh`

**Step 1: Write the script test**

Clone the existing watch-script test pattern for the new state/log/script names.

**Step 2: Run test to verify it fails**

Run: `bash test/test_watch_futures_um_1s_coingecko_top30_30d.sh`

Expected: fail because the new watch script does not exist yet.

**Step 3: Write minimal implementation**

Create the config with the explicit symbol list and isolated state/log paths. Create run/watch scripts that mirror the existing 30d workflow but point to the CoinGecko config and state.

**Step 4: Run test to verify it passes**

Run: `bash test/test_watch_futures_um_1s_coingecko_top30_30d.sh`

Expected: PASS.

### Task 3: Verify and execute the operational switch

**Files:**
- Use: `logs/`
- Use: `state/`

**Step 1: Run targeted verification**

Run:

```bash
env CGO_ENABLED=0 GOCACHE=/tmp/go-build go test ./pkg/windowimport -run 'TestResolveConfiguredSymbols'
env CGO_ENABLED=0 GOCACHE=/tmp/go-build go test ./cmd/...
env CGO_ENABLED=0 GOCACHE=/tmp/go-build go build -o ./data4bt cmd/main.go
bash test/test_watch_futures_um_1s_coingecko_top30_30d.sh
```

Expected: all pass.

**Step 2: Start the CoinGecko workflow**

Run: `./run-futures-um-1s-coingecko-top30-30d --bg`

Expected: a detached `screen` session is created.

**Step 3: Verify runtime state**

Run:

```bash
./watch-futures-um-1s-coingecko-top30-30d --once
screen -ls
```

Expected: the workflow is running or completed with the explicit 30-symbol scope reflected in state and logs.
