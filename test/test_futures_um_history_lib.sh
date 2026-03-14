#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$REPO_DIR/scripts/futures_um_history_lib.sh"

assert_eq() {
  local actual="$1"
  local expected="$2"
  local message="$3"
  if [[ "$actual" != "$expected" ]]; then
    echo "ASSERTION FAILED: $message" >&2
    echo "expected: [$expected]" >&2
    echo "actual:   [$actual]" >&2
    exit 1
  fi
}

root_xml='<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <CommonPrefixes><Prefix>data/futures/um/monthly/klines/BTCUSDT/</Prefix></CommonPrefixes>
  <CommonPrefixes><Prefix>data/futures/um/monthly/klines/ETHUSDC/</Prefix></CommonPrefixes>
  <CommonPrefixes><Prefix>data/futures/um/monthly/klines/我踏马来了USDT/</Prefix></CommonPrefixes>
</ListBucketResult>'

symbol_xml='<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Contents><Key>data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-02.zip</Key></Contents>
  <Contents><Key>data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-02.zip.CHECKSUM</Key></Contents>
  <Contents><Key>data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2024-03.zip</Key></Contents>
  <Contents><Key>data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2025-02.zip</Key></Contents>
  <Contents><Key>data/futures/um/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2025-03.zip</Key></Contents>
</ListBucketResult>'

symbols="$(printf '%s' "$root_xml" | extract_futures_um_symbols_from_s3_xml)"
assert_eq "$symbols" $'BTCUSDT\nETHUSDC\n我踏马来了USDT' "extract_futures_um_symbols_from_s3_xml"

months="$(printf '%s' "$symbol_xml" | extract_kline_months_from_s3_xml)"
assert_eq "$months" $'2024-02\n2024-03\n2025-02\n2025-03' "extract_kline_months_from_s3_xml"

window_months="$(printf '%s\n' "$months" | filter_months_in_window "2024-03" "2025-02")"
assert_eq "$window_months" $'2024-03\n2025-02' "filter_months_in_window"

encoded="$(encode_uri_component 'data/futures/um/monthly/klines/我踏马来了USDT/1m/')"
assert_eq "$encoded" 'data%2Ffutures%2Fum%2Fmonthly%2Fklines%2F%E6%88%91%E8%B8%8F%E9%A9%AC%E6%9D%A5%E4%BA%86USDT%2F1m%2F' "encode_uri_component"

echo "test_futures_um_history_lib.sh: PASS"
