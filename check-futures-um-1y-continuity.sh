#!/usr/bin/env bash
set -euo pipefail

CH_CONTAINER="${CH_CONTAINER:-data4bt-clickhouse}"
DB="${DB:-data4BT_futures_um}"
TABLE="${TABLE:-klines_1m}"
START_TS="${START_TS:-2025-03-01 00:00:00}"
END_TS_EXCLUSIVE="${END_TS_EXCLUSIVE:-2026-03-01 00:00:00}"

q() {
  docker exec "$CH_CONTAINER" clickhouse-client -q "$1"
}

echo "=== Continuity Check (${DB}.${TABLE}) ==="
echo "Window: [${START_TS}, ${END_TS_EXCLUSIVE})"
echo

echo "[1] Row count in window:"
q "SELECT count() FROM ${DB}.${TABLE} WHERE open_time >= toDateTime64('${START_TS}', 3) AND open_time < toDateTime64('${END_TS_EXCLUSIVE}', 3)"
echo

echo "[2] Summary by symbol continuity:"
q "
WITH m AS (
  SELECT
    symbol,
    min(toStartOfMonth(open_time)) AS min_m,
    max(toStartOfMonth(open_time)) AS max_m,
    countDistinct(toStartOfMonth(open_time)) AS month_cnt,
    dateDiff('month', min_m, max_m) + 1 AS expected_cnt
  FROM ${DB}.${TABLE}
  WHERE open_time >= toDateTime64('${START_TS}', 3)
    AND open_time < toDateTime64('${END_TS_EXCLUSIVE}', 3)
  GROUP BY symbol
)
SELECT
  count() AS symbols_with_data,
  sum(month_cnt = 12) AS full_12m_symbols,
  sum(month_cnt < 12) AS lt_12m_symbols,
  sum(month_cnt = expected_cnt) AS contiguous_symbols,
  sum(month_cnt < expected_cnt) AS symbols_with_internal_gaps
FROM m
FORMAT Vertical
"
echo

echo "[3] Symbols that have internal gaps (if any):"
q "
WITH m AS (
  SELECT
    symbol,
    min(toStartOfMonth(open_time)) AS min_m,
    max(toStartOfMonth(open_time)) AS max_m,
    countDistinct(toStartOfMonth(open_time)) AS month_cnt,
    dateDiff('month', min_m, max_m) + 1 AS expected_cnt
  FROM ${DB}.${TABLE}
  WHERE open_time >= toDateTime64('${START_TS}', 3)
    AND open_time < toDateTime64('${END_TS_EXCLUSIVE}', 3)
  GROUP BY symbol
)
SELECT
  symbol,
  month_cnt,
  expected_cnt,
  min_m,
  max_m
FROM m
WHERE month_cnt < expected_cnt
ORDER BY symbol
LIMIT 100
"
