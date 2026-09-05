#!/usr/bin/env python3
# Copyright (C) 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
"""
Interop assertion for our Parquet writer.

Regenerates the exact values test_writer.cc wrote (same PRNG, same seed),
then reads the file back with pyarrow and compares every value. If this
passes, files produced by ScyllaDB's writer are readable by the Arrow
ecosystem -- which is most of the strategic case for choosing Parquet.

  usage: writer_interop.py <dir-with-manifest.json>
"""
import json, sys, pathlib
import pyarrow.parquet as pq

STATUS = ["active", "pending", "closed", "archived", "error"]


class MT64:
    """std::mt19937_64, so Python can reproduce the C++ value stream exactly."""
    N, M = 312, 156
    MATRIX_A = 0xB5026F5AA96619E9
    UM = 0xFFFFFFFF80000000
    LM = 0x7FFFFFFF

    def __init__(self, seed):
        self.mt = [0] * self.N
        self.mt[0] = seed & 0xFFFFFFFFFFFFFFFF
        for i in range(1, self.N):
            self.mt[i] = (6364136223846793005 * (self.mt[i-1] ^ (self.mt[i-1] >> 62)) + i) & 0xFFFFFFFFFFFFFFFF
        self.idx = self.N

    def _gen(self):
        for i in range(self.N):
            x = (self.mt[i] & self.UM) | (self.mt[(i+1) % self.N] & self.LM)
            xa = x >> 1
            if x & 1:
                xa ^= self.MATRIX_A
            self.mt[i] = self.mt[(i + self.M) % self.N] ^ xa
        self.idx = 0

    def __call__(self):
        if self.idx >= self.N:
            self._gen()
        x = self.mt[self.idx]
        self.idx += 1
        x ^= (x >> 29) & 0x5555555555555555
        x ^= (x << 17) & 0x71D67FFFEDA60000
        x ^= (x << 37) & 0xFFF7EEE000000000
        x ^= x >> 43
        return x & 0xFFFFFFFFFFFFFFFF


def expected(rows, with_nulls, seed=1234, status_card=5):
    rng = MT64(seed)
    ids, grade, amount, status, ts = [], [], [], [], []
    t = 1700000000000000
    for i in range(rows):
        ids.append(i * 7 + 3)
        # Mirrors test_writer.cc exactly: every draw is unconditional.
        p = (not with_nulls) or (rng() % 4) != 0
        gv = int(rng() % 100)
        grade.append(gv if p else None)
        p2 = (not with_nulls) or (rng() % 4) != 0
        av = (rng() % 1000000) / 100.0
        amount.append(av if p2 else None)
        p3 = (not with_nulls) or (rng() % 4) != 0
        sv = STATUS[rng() % status_card]
        status.append(sv if p3 else None)
        t += rng() % 1000
        ts.append(t)
    return {"id": ids, "grade": grade, "amount": amount, "status": status, "__ts": ts}


def check(entry):
    path = entry["path"]
    tbl = pq.read_table(path)
    got = {c: tbl.column(c).to_pylist() for c in tbl.column_names}
    want = expected(entry["rows"], entry["nulls"], entry["seed"],
                    entry.get("status_card", 5))
    problems = []
    if set(got) != set(want):
        return ["column set: got %s want %s" % (sorted(got), sorted(want))]
    for c in want:
        if len(got[c]) != len(want[c]):
            problems.append("%s: length %d vs %d" % (c, len(got[c]), len(want[c])))
            continue
        for i, (a, b) in enumerate(zip(got[c], want[c])):
            if a is None or b is None:
                if a is not b:
                    problems.append("%s[%d]: null mismatch got=%r want=%r" % (c, i, a, b))
                    break
            elif isinstance(b, float):
                if abs(a - b) > 1e-9:
                    problems.append("%s[%d]: %r vs %r" % (c, i, a, b)); break
            elif a != b:
                problems.append("%s[%d]: %r vs %r" % (c, i, a, b)); break
    return problems


def main():
    d = pathlib.Path(sys.argv[1])
    with open(d / "manifest.json") as f:
        man = json.load(f)
    bad = 0
    for e in man:
        try:
            problems = check(e)
        except Exception as ex:
            print("ERROR %-16s pyarrow could not read it: %s" % (e["name"], ex))
            bad += 1
            continue
        if problems:
            bad += 1
            print("FAIL  %-16s (%d problems)" % (e["name"], len(problems)))
            for p in problems[:5]:
                print("        ", p)
        else:
            print("PASS  %-16s %7d rows x 5 cols verified against pyarrow  (%s)"
                  % (e["name"], e["rows"], e["compression"]))
    print("\n%s" % ("WRITER INTEROP PASS" if bad == 0 else "WRITER INTEROP FAIL (%d)" % bad))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
