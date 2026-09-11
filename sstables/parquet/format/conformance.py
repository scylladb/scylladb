#!/usr/bin/env python3
# Copyright (C) 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
"""
Conformance test for the hand-written Parquet footer parser.

Asserts that our C++ parser and pyarrow agree, field by field, on real Parquet
files written by real writers (parquet-cpp-arrow, parquet-mr, ...). This is the
assertion that matters: the codec is only useful if it reads what the ecosystem
writes.

  usage: conformance.py <path-to-test_parquet_metadata binary> <file.parquet>...
"""
import json, subprocess, sys
import pyarrow.parquet as pq

CODEC_ALIASES = {"LZ4_RAW": {"LZ4_RAW", "LZ4"}}


def check(binary, path):
    ours = json.loads(subprocess.run([binary, "dump", path], capture_output=True,
                                     check=True, text=True).stdout)
    ref = pq.ParquetFile(path)
    md = ref.metadata
    fails = []

    def eq(what, a, b):
        if a != b:
            fails.append("%s: ours=%r pyarrow=%r" % (what, a, b))

    eq("num_rows", ours["num_rows"], md.num_rows)
    eq("num_row_groups", ours["num_row_groups"], md.num_row_groups)
    eq("num_leaf_columns", ours["num_leaf_columns"], md.num_columns)
    eq("created_by", ours["created_by"], md.created_by or "")

    # leaf names and physical types, in schema order
    ref_leaves = [(md.schema.column(i).name, md.schema.column(i).physical_type)
                  for i in range(md.num_columns)]
    our_leaves = [(l["name"], l["type"]) for l in ours["leaves"]]
    eq("leaf list", our_leaves, ref_leaves)

    # every column chunk of every row group
    for g in range(md.num_row_groups):
        rg = md.row_group(g)
        og = ours["row_groups"][g]
        eq("rg%d.num_rows" % g, og["num_rows"], rg.num_rows)
        eq("rg%d.total_byte_size" % g, og["total_byte_size"], rg.total_byte_size)
        eq("rg%d.num_columns" % g, og["num_columns"], rg.num_columns)
        for c in range(rg.num_columns):
            cc, oc = rg.column(c), og["columns"][c]
            tag = "rg%d.col%d(%s)" % (g, c, cc.path_in_schema)
            eq(tag + ".path", oc["path"], cc.path_in_schema)
            eq(tag + ".type", oc["type"], cc.physical_type)
            allowed = CODEC_ALIASES.get(oc["codec"], {oc["codec"]})
            if cc.compression not in allowed:
                fails.append("%s.codec: ours=%r pyarrow=%r" % (tag, oc["codec"], cc.compression))
            eq(tag + ".num_values", oc["num_values"], cc.num_values)
            eq(tag + ".compressed", oc["total_compressed_size"], cc.total_compressed_size)
            eq(tag + ".uncompressed", oc["total_uncompressed_size"], cc.total_uncompressed_size)
            eq(tag + ".data_page_offset", oc["data_page_offset"], cc.data_page_offset)
            eq(tag + ".has_dict", oc["has_dict_page"], cc.dictionary_page_offset is not None)
            our_enc = set(oc["encodings"])
            ref_enc = set(cc.encodings)
            # PLAIN_DICTIONARY/RLE_DICTIONARY are spelled differently across versions
            norm = lambda s: {("RLE_DICTIONARY" if e == "PLAIN_DICTIONARY" else e) for e in s}
            if norm(our_enc) != norm(ref_enc):
                fails.append("%s.encodings: ours=%r pyarrow=%r" % (tag, sorted(our_enc), sorted(ref_enc)))
    return fails, md.num_columns * md.num_row_groups


def main():
    binary, files = sys.argv[1], sys.argv[2:]
    total_fail = 0
    for p in files:
        try:
            fails, nchunks = check(binary, p)
        except Exception as e:
            print("ERROR %s: %s" % (p, e))
            total_fail += 1
            continue
        if fails:
            total_fail += len(fails)
            print("FAIL %s (%d mismatches)" % (p, len(fails)))
            for f in fails[:10]:
                print("   ", f)
        else:
            print("PASS %s  (%d column chunks verified)" % (p, nchunks))
    print("\n%s" % ("CONFORMANCE PASS" if total_fail == 0 else "CONFORMANCE FAIL (%d)" % total_fail))
    return 1 if total_fail else 0


if __name__ == "__main__":
    sys.exit(main())
