#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
WATCH_SCRIPT="$PROJECT_ROOT/watch-futures-um-1s-15d"

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

cat > "$TMP_DIR/state/symbol_progress.json" <<'EOF'
{
  "AAAUSDT": {
    "symbol": "AAAUSDT",
    "total_months": 15,
    "completed_months": 15,
    "failed_months": 0,
    "current_month": "",
    "progress": 100,
    "status": "completed",
    "last_update": "2026-03-10T17:00:00+08:00",
    "worker_id": 0
  },
  "BBBUSDT": {
    "symbol": "BBBUSDT",
    "total_months": 15,
    "completed_months": 6,
    "failed_months": 0,
    "current_month": "2026-03",
    "progress": 40,
    "status": "running",
    "last_update": "2026-03-10T17:01:00+08:00",
    "worker_id": 2
  },
  "CCCUSDT": {
    "symbol": "CCCUSDT",
    "total_months": 15,
    "completed_months": 3,
    "failed_months": 1,
    "current_month": "2026-02",
    "progress": 20,
    "status": "failed",
    "last_update": "2026-03-10T17:02:00+08:00",
    "worker_id": 4
  }
}
EOF

cat > "$TMP_DIR/fake_clickhouse.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
query="${1:-}"
case "$query" in
  *"SELECT count(), uniqExact(symbol), min(open_time), max(open_time)"*)
    printf '12345\t2\t2026-02-23 00:00:00.000\t2026-03-09 23:59:59.000\n'
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
	63211.futures_um_1s_15d	(Detached)
1 Socket in /tmp/.screen.
OUT
EOF
chmod +x "$TMP_DIR/fake_screen_ls.sh"

cat > "$TMP_DIR/futures_um_1s_15d.log" <<'EOF'
line 1
line 2
EOF

output="$(
  STATE_DIR="$TMP_DIR/state" \
  LOG_FILE="$TMP_DIR/futures_um_1s_15d.log" \
  SCREEN_NAME="futures_um_1s_15d" \
  TOTAL_SYMBOLS="557" \
  CLICKHOUSE_QUERY_HELPER="$TMP_DIR/fake_clickhouse.sh" \
  SCREEN_LS_HELPER="$TMP_DIR/fake_screen_ls.sh" \
  "$WATCH_SCRIPT" --once
)"

assert_contains "$output" "Runner: RUNNING (screen: futures_um_1s_15d)" "runner status"
assert_contains "$output" "DB rows: 12345" "db rows"
assert_contains "$output" "DB symbols with data: 2/557" "db symbol count"
assert_contains "$output" "State tracked: 3/557" "state tracked count"
assert_contains "$output" "State completed: 1/557" "completed count"
assert_contains "$output" "State running: 1" "running count"
assert_contains "$output" "State failed: 1" "failed count"
assert_contains "$output" "worker=2 symbol=BBBUSDT progress=40.00%" "running symbol line"
assert_contains "$output" "line 2" "recent log"

echo "test_watch_futures_um_1s_15d.sh: PASS"
