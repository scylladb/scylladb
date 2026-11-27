#!/bin/bash
#
# Copyright (C) 2026-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Re-create all benchmark datasets from scratch: download, convert, validate.
# Usage: prepare_all.sh <data_root> [dataset...]
# The datasets go to <data_root>/prepared/<name>/, the downloads to
# <data_root>/raw/<name>/ and the logs to <data_root>/logs/.
# The conversion of fhvhv_trips needs pyarrow.
# Set KEEP_RAW=1 to reuse existing downloads instead of deleting them.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=${1:?usage: $0 <data_root> [dataset...]}
shift
ALL="drive_stats ghcn_daily fhvhv_trips github_events reviews trades pageviews price_paid isd_lite"
SETS=${*:-$ALL}

mkdir -p "$ROOT/logs"
ROOT=$(cd "$ROOT" && pwd)

for n in $SETS; do
    rm -rf "$ROOT/prepared/$n"
    [[ "${KEEP_RAW:-0}" == 1 ]] || rm -rf "$ROOT/raw/$n"
done

# Downloads run in parallel, one process per dataset.
pids=()
for n in $SETS; do
    "$HERE/download.sh" "$ROOT" "$n" > "$ROOT/logs/download_$n.log" 2>&1 &
    pids+=($!)
done
for p in "${pids[@]}"; do wait "$p"; done

# Conversions also run in parallel.  Python runs isolated (-I) from a
# directory outside the downloads; data paths are passed as arguments.
cd "$HERE"
pids=()
for n in $SETS; do
    python3 -I "$HERE/conv_$n.py" "$ROOT" > "$ROOT/logs/convert_$n.log" 2>&1 &
    pids+=($!)
done
fail=0
for p in "${pids[@]}"; do wait "$p" || fail=1; done
[[ $fail == 0 ]] || { echo "conversion failed; see $ROOT/logs" >&2; exit 1; }

for n in $SETS; do
    python3 -I "$HERE/validate.py" "$ROOT" "$n" || fail=1
done
exit $fail
