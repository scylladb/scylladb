#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Tests for merging lcov traces produced by different binaries."""

import pathlib

from test.pylib.lcov_utils import LcovFile


def trace(fn_line: int) -> str:
    return (
        "SF:utils/managed_bytes.hh\n"
        f"FN:{fn_line},ctor\n"
        "FNDA:1,ctor\n"
        "DA:67,1\n"
        "DA:71,1\n"
        "end_of_record\n"
    )


def test_union_of_one_function_reported_at_different_lines(tmp_path: pathlib.Path) -> None:
    """A header compiled with and without the PCH can put a function's start on
    different lines; the merged trace must still list it once, or it cannot be
    loaded back."""
    paths = []
    for fn_line in (67, 71):
        paths.append(tmp_path / f"{fn_line}.info")
        paths[-1].write_text(trace(fn_line))

    merged = LcovFile(paths[0]).union(LcovFile(paths[1]))
    merged.write(tmp_path / "merged.info")

    record = next(iter(LcovFile(tmp_path / "merged.info").records.values()))
    assert record.function_hits == {(67, "ctor"): 2}
