#!/bin/bash
#
# Copyright (C) 2026-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Usage: run_dataset.sh <dataset_dir> <out_dir> [extra perf-sstable-datasets options, e.g. --runs 5 --keys 2000]
# Runs scylla perf-sstable-datasets on one dataset (schema.cql + data.jsonl).
# Must run inside the dbuild container. Writes <out_dir>/{mt_zstd,mt_zstd_dict,aa_zstd}/,
# <out_dir>/dict.bin, <out_dir>/results.json and <out_dir>/summary.txt.
set -euo pipefail
if [ $# -lt 2 ]; then
    echo "usage: $0 <dataset_dir> <out_dir> [extra options]" >&2
    exit 1
fi
dataset=$1; out=$2; shift 2
# SCYLLA overrides the binary; the default is the dev build of this repository.
SCYLLA=${SCYLLA:-$(cd "$(dirname "$0")/../../.." && pwd)/build/dev/scylla}
[ -f "$dataset/schema.cql" ] && [ -f "$dataset/data.jsonl" ] || { echo "missing $dataset/schema.cql or $dataset/data.jsonl" >&2; exit 1; }
# The memtable takes 2x (wide partitions) to 5x (one small row per partition) the JSON size.
json_bytes=$(stat -c %s "$dataset/data.jsonl")
mem_gb=$(( (json_bytes * 8 / 1024 / 1024 / 1024) + 4 ))
[ $mem_gb -lt 4 ] && mem_gb=4
[ $mem_gb -gt 40 ] && mem_gb=40
mkdir -p "$out"
# -c1: one shard. --idle-poll-time-us 0 --poll-aio 0: the reactor sleeps when idle instead of
# busy-polling, so thread CPU time and instruction counts measure work, not polling.
"$SCYLLA" perf-sstable-datasets -c1 -m${mem_gb}G \
    --idle-poll-time-us 0 --poll-aio 0 \
    --blocked-reactor-notify-ms 5000 \
    --logger-log-level features=warn \
    --dataset "$dataset" --out "$out" "$@" 2>&1 | tee "$out/log.txt" | sed -n '/^schema:/,$p' | tee "$out/summary.txt"
exit ${PIPESTATUS[0]}
