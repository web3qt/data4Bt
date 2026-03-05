#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CH_CONTAINER="${CH_CONTAINER:-data4bt-clickhouse}"
SRC_DB="${SRC_DB:-data4BT}"
DST_DB="${DST_DB:-data4BT_futures_um}"
TABLE="${TABLE:-klines_1m}"

# 这次合约 2 个月任务实际写入窗口（UTC）
CREATED_FROM="${CREATED_FROM:-2026-03-05 05:14:00}"
CREATED_TO="${CREATED_TO:-2026-03-05 05:27:59}"

# 这次任务目标数据月份范围（UTC）
OPEN_FROM="${OPEN_FROM:-2026-01-01 00:00:00}"
OPEN_TO_EXCLUSIVE="${OPEN_TO_EXCLUSIVE:-2026-03-01 00:00:00}"

ch() {
  docker exec "$CH_CONTAINER" clickhouse-client -q "$1"
}

echo "== Step 1: Ensure destination DB/table exists =="
ch "CREATE DATABASE IF NOT EXISTS ${DST_DB}"
ch "CREATE TABLE IF NOT EXISTS ${DST_DB}.${TABLE} AS ${SRC_DB}.${TABLE}"

echo "== Step 2: Count source rows to migrate =="
SRC_COUNT="$(ch "SELECT count() FROM ${SRC_DB}.${TABLE} WHERE created_at >= toDateTime('${CREATED_FROM}') AND created_at <= toDateTime('${CREATED_TO}') AND open_time >= toDateTime64('${OPEN_FROM}', 3) AND open_time < toDateTime64('${OPEN_TO_EXCLUSIVE}', 3)")"
echo "source_count=${SRC_COUNT}"

if [[ "${SRC_COUNT}" -eq 0 ]]; then
  echo "No rows matched migration window. Exit."
  exit 0
fi

echo "== Step 3: Insert into destination =="
ch "INSERT INTO ${DST_DB}.${TABLE} SELECT * FROM ${SRC_DB}.${TABLE} WHERE created_at >= toDateTime('${CREATED_FROM}') AND created_at <= toDateTime('${CREATED_TO}') AND open_time >= toDateTime64('${OPEN_FROM}', 3) AND open_time < toDateTime64('${OPEN_TO_EXCLUSIVE}', 3)"

echo "== Step 4: Verify destination increment =="
DST_COUNT="$(ch "SELECT count() FROM ${DST_DB}.${TABLE} WHERE created_at >= toDateTime('${CREATED_FROM}') AND created_at <= toDateTime('${CREATED_TO}') AND open_time >= toDateTime64('${OPEN_FROM}', 3) AND open_time < toDateTime64('${OPEN_TO_EXCLUSIVE}', 3)")"
echo "destination_count=${DST_COUNT}"

if [[ "${DST_COUNT}" -lt "${SRC_COUNT}" ]]; then
  echo "Destination count (${DST_COUNT}) < source count (${SRC_COUNT}), aborting delete."
  exit 1
fi

echo "== Step 5: Delete migrated rows from source =="
ch "ALTER TABLE ${SRC_DB}.${TABLE} DELETE WHERE created_at >= toDateTime('${CREATED_FROM}') AND created_at <= toDateTime('${CREATED_TO}') AND open_time >= toDateTime64('${OPEN_FROM}', 3) AND open_time < toDateTime64('${OPEN_TO_EXCLUSIVE}', 3)"

echo "== Step 6: Wait mutation done =="
while true; do
  PENDING="$(ch "SELECT count() FROM system.mutations WHERE database='${SRC_DB}' AND table='${TABLE}' AND is_done=0")"
  if [[ "${PENDING}" -eq 0 ]]; then
    break
  fi
  echo "pending_mutations=${PENDING} ..."
  sleep 2
done

echo "== Step 7: Final verification =="
SRC_AFTER="$(ch "SELECT count() FROM ${SRC_DB}.${TABLE} WHERE created_at >= toDateTime('${CREATED_FROM}') AND created_at <= toDateTime('${CREATED_TO}') AND open_time >= toDateTime64('${OPEN_FROM}', 3) AND open_time < toDateTime64('${OPEN_TO_EXCLUSIVE}', 3)")"
DST_AFTER="$(ch "SELECT count() FROM ${DST_DB}.${TABLE} WHERE created_at >= toDateTime('${CREATED_FROM}') AND created_at <= toDateTime('${CREATED_TO}') AND open_time >= toDateTime64('${OPEN_FROM}', 3) AND open_time < toDateTime64('${OPEN_TO_EXCLUSIVE}', 3)")"
echo "source_after=${SRC_AFTER}"
echo "destination_after=${DST_AFTER}"

if [[ "${SRC_AFTER}" -ne 0 ]]; then
  echo "Source still has matched rows after delete."
  exit 1
fi

echo "Migration done."
