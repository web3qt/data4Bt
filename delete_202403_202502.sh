#!/usr/bin/env bash

set -euo pipefail

CH_URL="${CH_URL:-http://localhost:8123}"
CH_USER="${CH_USER:-default}"
CH_PASS="${CH_PASS:-123456}"
CH_DB="${CH_DB:-data4BT}"

START_TS="${1:-2024-03-01 00:00:00}"
END_TS="${2:-2025-02-28 23:59:00}"

query() {
  local sql="$1"
  curl -sS -u "${CH_USER}:${CH_PASS}" -X POST "${CH_URL}/" --data "${sql}"
}

echo "Deleting ${CH_DB}.klines_1m in [${START_TS}, ${END_TS}]"

if ! query "SELECT 1 FORMAT TabSeparated" >/dev/null 2>&1; then
  echo "Cannot connect to ClickHouse at ${CH_URL}. Please start DB first."
  exit 2
fi

BEFORE="$(query "SELECT count() FROM ${CH_DB}.klines_1m WHERE open_time >= toDateTime64('${START_TS}', 3) AND open_time <= toDateTime64('${END_TS}', 3) FORMAT TabSeparated")"
echo "Rows in range before delete: ${BEFORE}"

if [[ "${BEFORE}" == "0" ]]; then
  echo "Nothing to delete."
  exit 0
fi

query "ALTER TABLE ${CH_DB}.klines_1m DELETE WHERE open_time >= toDateTime64('${START_TS}', 3) AND open_time <= toDateTime64('${END_TS}', 3)"
echo "Delete mutation submitted. Waiting for completion..."

while true; do
  PENDING="$(query "SELECT count() FROM system.mutations WHERE database='${CH_DB}' AND table='klines_1m' AND is_done=0 FORMAT TabSeparated")"
  if [[ "${PENDING}" == "0" ]]; then
    break
  fi
  sleep 2
done

AFTER="$(query "SELECT count() FROM ${CH_DB}.klines_1m WHERE open_time >= toDateTime64('${START_TS}', 3) AND open_time <= toDateTime64('${END_TS}', 3) FORMAT TabSeparated")"
echo "Rows in range after delete: ${AFTER}"

if [[ "${AFTER}" == "0" ]]; then
  echo "Delete completed successfully."
else
  echo "Delete finished but some rows remain. Please inspect partitions/mutations."
  exit 1
fi
