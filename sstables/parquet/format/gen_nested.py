# Copyright (C) 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
import pyarrow as pa, pyarrow.parquet as pq, json, sys, pathlib
out = pathlib.Path(sys.argv[1]); out.mkdir(parents=True, exist_ok=True)
n = 500
t = pa.table({
    "id":   pa.array(range(n), pa.int64()),
    "tags": pa.array([[f"t{i%3}", f"u{i%5}"] if i % 4 else ([] if i % 8 else None)
                      for i in range(n)], pa.list_(pa.string())),
    "attrs": pa.array([[(f"k{i%3}", i), (f"k{(i+1)%3}", None)] if i % 3 else None
                       for i in range(n)], pa.map_(pa.string(), pa.int32())),
    "nested": pa.array([[[1, 2], [3]] if i % 2 else None for i in range(n)],
                       pa.list_(pa.list_(pa.int32()))),
})
p = out / "nested.parquet"
pq.write_table(t, p, compression="zstd", data_page_version="2.0",
               row_group_size=200, write_page_index=True)
md = pq.ParquetFile(p).metadata
cols = []
for i in range(md.num_columns):
    c = md.schema.column(i)
    cols.append({"path": c.path, "max_def": c.max_definition_level,
                 "max_rep": c.max_repetition_level})
(out / "nested.levels.json").write_text(json.dumps(cols, indent=1))

# tags as a line-per-row text form, so the C++ side needs no JSON parser:
#   NULL        the list itself is null
#   EMPTY       present but zero elements
#   a|b|~       elements, ~ for a null element
lines = []
for v in t.column("tags").to_pylist():
    if v is None:
        lines.append("NULL")
    elif len(v) == 0:
        lines.append("EMPTY")
    else:
        lines.append("|".join("~" if e is None else e for e in v))
(out / "nested.tags.txt").write_text("\n".join(lines) + "\n")
print(f"wrote {p} rows={md.num_rows} cols={md.num_columns} rgs={md.num_row_groups}")
for c in cols: print("  ", c)
