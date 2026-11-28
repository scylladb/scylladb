#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""lcov_utils must accept a function reported at more than one start line.

Test binaries built from the same header can disagree on where a function
starts - a constructor at its declaration in one binary and at its body in
another. Merging the per-binary traces of a suite with many binaries (boost,
raft) then gave one record two FN entries with the same name, and loading that
merged trace back failed:

    RuntimeError: ('assertion in loading .../boost.info, bytes_ostream.hh', AssertionError())

These tests need no build: they feed lcov_utils small hand-written traces.
"""

from __future__ import annotations

from pathlib import Path

from test.pylib import lcov_utils

CTOR = "_ZN13bytes_ostreamC2Em"


def _record(fn_line: int, hits: int) -> str:
    # One function, reported at fn_line; lines 174 and 178 both exist as DA.
    return (
        "SF:bytes_ostream.hh\n"
        f"FN:{fn_line},{CTOR}\n"
        f"FNDA:{hits},{CTOR}\n"
        "FNF:1\n"
        f"FNH:{int(hits > 0)}\n"
        f"DA:174,{hits}\n"
        f"DA:178,{hits}\n"
        "LF:2\n"
        f"LH:{2 if hits else 0}\n"
        "end_of_record\n"
    )


def _function_hits(lcov: lcov_utils.LcovFile) -> dict:
    (record,) = lcov.records.values()
    return dict(record.function_hits)


def test_loads_a_record_listing_one_function_at_two_lines(tmp_path: Path) -> None:
    # The shape test.py itself wrote for boost.info before this was fixed.
    trace = tmp_path / "dup.info"
    trace.write_text(
        "SF:bytes_ostream.hh\n"
        f"FN:174,{CTOR}\n"
        f"FN:178,{CTOR}\n"
        f"FNDA:3,{CTOR}\n"
        f"FNDA:4,{CTOR}\n"
        "FNF:2\nFNH:2\n"
        "DA:174,7\nDA:178,7\n"
        "LF:2\nLH:2\n"
        "end_of_record\n"
    )
    lcov = lcov_utils.LcovFile(trace)
    # One function, at its earliest line, with both counts.
    assert _function_hits(lcov) == {(174, CTOR): 7}


def test_union_of_records_disagreeing_on_a_function_line(tmp_path: Path) -> None:
    a = tmp_path / "a.info"
    b = tmp_path / "b.info"
    a.write_text(_record(178, 2))
    b.write_text(_record(174, 5))
    merged = lcov_utils.LcovFile(a)
    merged.union(lcov_utils.LcovFile(b))
    assert _function_hits(merged) == {(174, CTOR): 7}

    # What a merge writes must load back: this round trip is what failed.
    out = tmp_path / "merged.info"
    merged.write(out)
    assert out.read_text().count(f",{CTOR}\n") == 2  # one FN line and one FNDA line
    assert _function_hits(lcov_utils.LcovFile(out)) == {(174, CTOR): 7}


def test_ordinary_record_is_unchanged(tmp_path: Path) -> None:
    trace = tmp_path / "one.info"
    trace.write_text(_record(174, 3))
    assert _function_hits(lcov_utils.LcovFile(trace)) == {(174, CTOR): 3}
