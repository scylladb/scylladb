#!/bin/bash
#
# Copyright (C) 2026-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Runs scylla perf-sstable-datasets on every prepared dataset, one after
# the other, so that the runs do not compete for the machine.
# Usage: run_all.sh <data_root> <results_dir> [extra perf-sstable-datasets options]
# Reads <data_root>/prepared/<name>/ (see prepare_all.sh) and writes
# <results_dir>/<name>/ (see run_dataset.sh). Each <results_dir>/<name>/
# must not exist yet, or must not hold the sstable directories of a
# previous run. Then run:
#   python3 results_tables.py <results_dir>
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=${1:?usage: $0 <data_root> <results_dir> [extra options]}
OUT=${2:?usage: $0 <data_root> <results_dir> [extra options]}
shift 2

mkdir -p "$OUT"
for d in "$ROOT"/prepared/*/; do
    name=$(basename "$d")
    echo "=== $name" >&2
    "$HERE/run_dataset.sh" "$d" "$OUT/$name" "$@" > "$OUT/$name.stdout"
done
