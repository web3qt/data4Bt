#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
WATCH_SCRIPT="$PROJECT_ROOT/watch-futures-um-1s-coingecko-top30-30d"

assert_contains() {
  local haystack="$1"
  local needle="$2"
  local message="$3"

  if [[ "$haystack" != *"$needle"* ]]; then
    echo "ASSERTION FAILED: $message" >&2
    echo "missing: [$needle]" >&2
    exit 1
  fi
}

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

mkdir -p "$TMP_DIR/state"

cat > "$TMP_DIR/state/progress.json" <<'EOF'
{
  "BTCUSDT": {
    "symbol": "BTCUSDT",
    "status": "completed"
  },
  "ETHUSDT": {
    "symbol": "ETHUSDT",
    "status": "running"
  },
  "BNBUSDT": {
    "symbol": "BNBUSDT",
    "status": "failed"
  }
}
EOF

cat > "$TMP_DIR/state/symbol_progress.json" <<'EOF'
{
  "BTCUSDT": {
    "symbol": "BTCUSDT",
    "total_months": 30,
    "completed_months": 30,
    "failed_months": 0,
    "current_month": "",
    "progress": 100,
    "status": "completed",
    "last_update": "2026-03-11T12:30:00+08:00",
    "worker_id": 0
  },
  "ETHUSDT": {
    "symbol": "ETHUSDT",
    "total_months": 30,
    "completed_months": 12,
    "failed_months": 0,
    "current_month": "2026-02",
    "progress": 40,
    "status": "running",
    "last_update": "2026-03-11T12:31:00+08:00",
    "worker_id": 2
  },
  "BNBUSDT": {
    "symbol": "BNBUSDT",
    "total_months": 30,
    "completed_months": 4,
    "failed_months": 1,
    "current_month": "2026-02",
    "progress": 13.33,
    "status": "failed",
    "last_update": "2026-03-11T12:32:00+08:00",
    "worker_id": 4
  }
}
EOF

cat > "$TMP_DIR/fake_clickhouse.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
query="${1:-}"
case "$query" in
  *"WHERE symbol IN ('BNBUSDT','BTCUSDT','ETHUSDT')"*)
    printf '123456\t2\t2026-02-09 00:00:00.000\t2026-03-09 23:59:59.000\n'
    ;;
  *)
    echo "unexpected query: $query" >&2
    exit 1
    ;;
esac
EOF
chmod +x "$TMP_DIR/fake_clickhouse.sh"

cat > "$TMP_DIR/fake_screen_ls.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
cat <<'OUT'
There is a screen on:
	63214.futures_um_1s_coingecko_top30_30d	(Detached)
1 Socket in /tmp/.screen.
OUT
EOF
chmod +x "$TMP_DIR/fake_screen_ls.sh"

cat > "$TMP_DIR/futures_um_1s_coingecko_top30_30d.log" <<'EOF'
line 1
line 2
EOF

output="$(
  STATE_DIR="$TMP_DIR/state" \
  LOG_FILE="$TMP_DIR/futures_um_1s_coingecko_top30_30d.log" \
  SCREEN_NAME="futures_um_1s_coingecko_top30_30d" \
  TOTAL_SYMBOLS="30" \
  CLICKHOUSE_QUERY_HELPER="$TMP_DIR/fake_clickhouse.sh" \
  SCREEN_LS_HELPER="$TMP_DIR/fake_screen_ls.sh" \
  "$WATCH_SCRIPT" --once
)"

assert_contains "$output" "Runner: RUNNING (screen: futures_um_1s_coingecko_top30_30d)" "runner status"
assert_contains "$output" "DB rows: 123456" "db rows"
assert_contains "$output" "DB symbols with data: 2/30" "db symbol count"
assert_contains "$output" "State tracked: 3/30" "state tracked count"
assert_contains "$output" "State completed: 1/30" "completed count"
assert_contains "$output" "State running: 1" "running count"
assert_contains "$output" "State failed: 1" "failed count"
assert_contains "$output" "worker=2 symbol=ETHUSDT progress=40.00% current_month=2026-02" "running symbol line"
assert_contains "$output" "line 2" "recent log"

echo "test_watch_futures_um_1s_coingecko_top30_30d.sh: PASS"
