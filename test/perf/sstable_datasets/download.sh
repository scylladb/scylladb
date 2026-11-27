#!/bin/bash
#
# Copyright (C) 2026-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Download raw inputs for the benchmark datasets.
# Usage: download.sh <data_root> [dataset...]
# Each dataset goes into its own directory <root>/raw/<name>/.
# Downloaded files are untrusted data and are never executed.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=${1:?data root}
shift
UA="Mozilla/5.0 (compatible; dataset-fetch/1.0)"

# fetch <name> <url> [<url2-fallback>]
fetch() {
    local name=$1 url=$2 alt=${3:-}
    local dir="$ROOT/raw/$name"
    mkdir -p "$dir"
    local out
    out="$dir/$(basename "$url")"
    if [[ -s "$out" && ! -e "$out.partial" ]]; then
        echo "have $out"
        return 0
    fi
    touch "$out.partial"
    for u in "$url" $alt; do
        if curl -fsSL --retry 5 --retry-delay 5 --retry-all-errors -A "$UA" -o "$out" "$u"; then
            rm -f "$out.partial"
            echo "got $out ($(stat -c %s "$out") bytes) from $u"
            return 0
        fi
        echo "failed $u" >&2
    done
    return 1
}

dl_drive_stats() { fetch drive_stats https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2024.zip; }
dl_ghcn_daily() {
    fetch ghcn_daily https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_year/2023.csv.gz \
        https://noaa-ghcn-pds.s3.amazonaws.com/csv.gz/by_year/2023.csv.gz
}
dl_fhvhv_trips() { fetch fhvhv_trips https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2024-01.parquet; }
dl_github_events() { fetch github_events https://data.gharchive.org/2024-01-15-15.json.gz; }
dl_reviews() { fetch reviews https://mcauleylab.ucsd.edu/public_datasets/data/amazon_2023/raw/review_categories/Software.jsonl.gz; }
dl_trades() {
    for d in 15 16 17; do
        fetch trades https://data.binance.vision/data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-$d.zip
    done
}
dl_pageviews() {
    for h in 15 16 17 18; do
        fetch pageviews https://dumps.wikimedia.org/other/pageviews/2024/2024-01/pageviews-20240115-${h}0000.gz
    done
}
dl_price_paid() {
    fetch price_paid https://price-paid-data.publicdata.landregistry.gov.uk/pp-2023.csv
    fetch price_paid https://price-paid-data.publicdata.landregistry.gov.uk/pp-2022.csv
}

dl_isd_lite() {
    # The station list chooses which station files to fetch (conv_isd_lite.py).
    # The files are fetched four at a time. A candidate without a 2023 file
    # (HTTP 404) is skipped; the converter skips it too.
    fetch isd_lite https://noaa-isd-pds.s3.amazonaws.com/isd-history.csv
    export ROOT UA
    python3 -I "$HERE/conv_isd_lite.py" --candidates "$ROOT" |
        xargs -P 4 -I{} bash -c '
            out="$ROOT/raw/isd_lite/$1-2023.gz"
            [[ -s "$out" && ! -e "$out.partial" ]] && exit 0
            touch "$out.partial"
            for try in 1 2 3 4 5; do
                code=$(curl -sSL -A "$UA" -o "$out" -w "%{http_code}" \
                    "https://noaa-isd-pds.s3.amazonaws.com/isd-lite/data/2023/$1-2023.gz") || code=fail
                [[ $code == 200 || $code == 404 ]] && break
                sleep 5
            done
            case $code in
            200) rm -f "$out.partial" ;;
            404) rm -f "$out" "$out.partial"; echo "no 2023 file for $1" ;;
            *) echo "failed $1: $code" >&2; exit 1 ;;
            esac' _ {}
    echo "got $(find "$ROOT/raw/isd_lite" -maxdepth 1 -name '*.gz' | wc -l) station files"
}

if (( $# == 0 )); then
    set -- drive_stats ghcn_daily fhvhv_trips github_events reviews trades pageviews price_paid isd_lite
fi
for n in "$@"; do
    "dl_$n"
done
