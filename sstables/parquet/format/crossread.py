#!/usr/bin/env python3
# Copyright (C) 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
"""Compare our decoder's per-column digests against pyarrow's own read.

  usage: crossread.py <test_crossread binary> <file.parquet>...
"""
import json, subprocess, sys
import pyarrow as pa
import pyarrow.parquet as pq

def main():
    binary, files = sys.argv[1], sys.argv[2:]
    ours = json.loads(subprocess.run([binary, *files], capture_output=True,
                                     check=True, text=True).stdout)
    bad = 0
    for entry in ours:
        path = entry["file"]
        # Our reader decodes row group 0 only.
        tbl = pq.ParquetFile(path).read_row_group(0)
        by_name = {c["name"]: c for c in entry["columns"]}
        problems = []
        for name in tbl.column_names:
            arr = tbl.column(name)
            # Timestamps come back as Python datetimes, whose .timestamp() would
            # reinterpret a naive value in local time. Our decoder sees the stored
            # integer, so compare the stored integer.
            if pa.types.is_timestamp(arr.type):
                arr = arr.cast(pa.int64())
            col = arr.to_pylist()
            nulls = sum(1 for v in col if v is None)
            sample = next((v for v in col if v is not None), 0)
            if isinstance(sample, (bytes, str)):
                s = float(sum(len(v) for v in col if v is not None))
            else:
                s = float(sum(v for v in col if v is not None))
            got = by_name.get(name)
            if got is None:
                problems.append("%s: missing from our decode" % name); continue
            if got["n"] != len(col):
                problems.append("%s: n %d vs %d" % (name, got["n"], len(col)))
            if got["nulls"] != nulls:
                problems.append("%s: nulls %d vs %d" % (name, got["nulls"], nulls))
            if abs(got["sum"] - s) > max(1e-6, abs(s) * 1e-9):
                problems.append("%s: sum %.6f vs %.6f" % (name, got["sum"], s))
        if problems:
            bad += 1
            print("FAIL %s" % path)
            for p in problems[:5]: print("    ", p)
        else:
            print("PASS %-44s %d rows x %d cols agree with pyarrow"
                  % (path.split("/")[-1], entry["rows"], len(entry["columns"])))
    print("\n%s" % ("CROSS-READ PASS" if bad == 0 else "CROSS-READ FAIL (%d)" % bad))
    return 1 if bad else 0

if __name__ == "__main__":
    sys.exit(main())
