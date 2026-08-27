#!/usr/bin/env python3
# Copyright (C) 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
"""Verify our nested writer output against pyarrow.

Nesting is where self-consistency proves least: the levels, the group structure
and the LIST annotation all have to match what other implementations expect, and
a file can be self-consistently wrong in all three.
"""
import sys, pathlib
import pyarrow.parquet as pq

d = pathlib.Path(sys.argv[1])
p, e = d / "w_nested.parquet", d / "w_nested.tags.txt"
want = e.read_text().splitlines()

f = pq.ParquetFile(p)
col = f.metadata.schema.column(0)
print(f"  pyarrow sees path={col.path} max_def={col.max_definition_level} "
      f"max_rep={col.max_repetition_level}")
if (col.max_definition_level, col.max_repetition_level) != (3, 1):
    print("FAIL pyarrow disagrees about the levels"); sys.exit(1)

got = []
for v in pq.read_table(p).column("tags").to_pylist():
    if v is None:            got.append("NULL")
    elif len(v) == 0:        got.append("EMPTY")
    else:                    got.append("|".join("~" if x is None else x for x in v))

if len(got) != len(want):
    print(f"FAIL row count: pyarrow {len(got)}, expected {len(want)}"); sys.exit(1)
bad = [(i, g, w) for i, (g, w) in enumerate(zip(got, want)) if g != w]
for i, g, w in bad[:6]:
    print(f"FAIL row {i}: pyarrow '{g}', expected '{w}'")
print(f"  {len(got)} rows, {len(bad)} mismatches")
print("NESTED WRITE INTEROP " + ("FAIL" if bad else "PASS"))
sys.exit(1 if bad else 0)
