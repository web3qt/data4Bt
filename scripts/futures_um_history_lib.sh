#!/usr/bin/env bash
set -euo pipefail

extract_futures_um_symbols_from_s3_xml() {
  perl -0ne 'while (m{<Prefix>data/futures/um/monthly/klines/([^/]+)/</Prefix>}g) { print "$1\n" }'
}

extract_kline_months_from_s3_xml() {
  perl -0ne 'while (m{<Key>data/futures/um/monthly/klines/[^/]+/1m/[^<]*-(\d{4}-\d{2})\.zip</Key>}g) { print "$1\n" }' \
    | sort -u
}

filter_months_in_window() {
  local start_month="$1"
  local end_month="$2"
  awk -v start="$start_month" -v end="$end_month" '$0 >= start && $0 <= end'
}

encode_uri_component() {
  jq -rn --arg v "$1" '$v|@uri'
}
