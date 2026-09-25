#!/usr/bin/env python3
#
# Copyright (C) 2024-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
# Memory-bounded streaming lcov merger for the SonarQube coverage POC.
#
# Why not `lcov -a` / the repo's lcov_utils?
#   * genuine `lcov` (Perl) loads each tracefile into memory and is too
#     memory-hungry to merge the ~147 boost tracefiles under a safe cap.
#   * the repo's lcov_utils parser asserts on LLVM-22 duplicate `FN` records.
#
# This merger keeps a single accumulator (the union) in memory and reads every
# input file exactly once, line by line, summing execution counts. Memory is
# bounded by the size of the *union* (unique files/lines), not by the number or
# size of the inputs.
#
# Usage: lcov_merge.py -o OUT.info IN1.info IN2.info ...

import argparse
import sys
from collections import defaultdict


class FileCov:
    __slots__ = ("da", "fn", "fnda", "brda")

    def __init__(self):
        self.da = defaultdict(int)               # line -> summed hits
        self.fn = {}                             # func name -> line
        self.fnda = defaultdict(int)             # func name -> summed hits
        self.brda = {}                           # (line, block, branch) -> count or None


def merge_file(acc, path):
    cur = None
    with open(path, "r", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            if line == "end_of_record":
                cur = None
                continue
            tag, _, rest = line.partition(":")
            if tag == "SF":
                cur = acc.get(rest)
                if cur is None:
                    cur = FileCov()
                    acc[rest] = cur
            elif cur is None:
                # TN or stray line outside a record; ignore
                continue
            elif tag == "DA":
                parts = rest.split(",")
                ln = int(parts[0])
                hits = int(parts[1])
                cur.da[ln] += hits
            elif tag == "FN":
                # FN:<line>,<name>   (name may contain commas)
                lnstr, _, name = rest.partition(",")
                cur.fn[name] = int(lnstr)
            elif tag == "FNDA":
                cntstr, _, name = rest.partition(",")
                cur.fnda[name] += int(cntstr)
            elif tag == "BRDA":
                # BRDA:<line>,<block>,<branch>,<count|->
                ln, block, branch, count = rest.split(",")
                key = (int(ln), int(block), int(branch))
                if count == "-":
                    cur.brda.setdefault(key, None)
                else:
                    prev = cur.brda.get(key)
                    cur.brda[key] = int(count) + (prev or 0)
            # FNF/FNH/LF/LH/BRF/BRH are recomputed on write; ignore here.


def write_merged(acc, out):
    with open(out, "w") as f:
        for sf in sorted(acc):
            fc = acc[sf]
            f.write("TN:\n")
            f.write(f"SF:{sf}\n")
            # functions
            for name, ln in sorted(fc.fn.items(), key=lambda kv: (kv[1], kv[0])):
                f.write(f"FN:{ln},{name}\n")
            fnh = 0
            for name in sorted(fc.fnda):
                cnt = fc.fnda[name]
                fnh += 1 if cnt > 0 else 0
                f.write(f"FNDA:{cnt},{name}\n")
            if fc.fn or fc.fnda:
                f.write(f"FNF:{len(fc.fn)}\n")
                f.write(f"FNH:{fnh}\n")
            # branches
            brf = brh = 0
            for key in sorted(fc.brda):
                ln, block, branch = key
                cnt = fc.brda[key]
                brf += 1
                if cnt:
                    brh += 1
                f.write(f"BRDA:{ln},{block},{branch},{'-' if cnt is None else cnt}\n")
            if fc.brda:
                f.write(f"BRF:{brf}\n")
                f.write(f"BRH:{brh}\n")
            # lines
            lh = 0
            for ln in sorted(fc.da):
                hits = fc.da[ln]
                lh += 1 if hits > 0 else 0
                f.write(f"DA:{ln},{hits}\n")
            f.write(f"LF:{len(fc.da)}\n")
            f.write(f"LH:{lh}\n")
            f.write("end_of_record\n")


def main():
    ap = argparse.ArgumentParser(description="Memory-bounded streaming lcov merger")
    ap.add_argument("-o", "--output", required=True)
    ap.add_argument("inputs", nargs="+")
    args = ap.parse_args()

    acc = {}
    for i, path in enumerate(args.inputs, 1):
        merge_file(acc, path)
        print(f"  [lcov_merge] folded {i}/{len(args.inputs)} ({len(acc)} files)",
              file=sys.stderr, flush=True)
    write_merged(acc, args.output)
    total_lines = sum(len(fc.da) for fc in acc.values())
    covered = sum(1 for fc in acc.values() for h in fc.da.values() if h > 0)
    pct = (100.0 * covered / total_lines) if total_lines else 0.0
    print(f"[lcov_merge] {len(acc)} files, {covered}/{total_lines} lines "
          f"({pct:.1f}%) -> {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
