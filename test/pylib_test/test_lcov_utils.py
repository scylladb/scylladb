#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Tests for merging lcov traces produced by different binaries."""

import pathlib
import xml.etree.ElementTree as ET

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


def test_cobertura_report(tmp_path: pathlib.Path) -> None:
    """SonarQube takes the hits of every line and the covered/total branches of
    the lines that branch from the Cobertura report."""
    (tmp_path / "trace.info").write_text(
        "SF:utils/uuid.cc\n"
        "DA:3,0\n"
        "DA:5,2\n"
        "BRDA:5,0,0,2\n"
        "BRDA:5,0,1,-\n"
        "end_of_record\n"
    )
    LcovFile(tmp_path / "trace.info").write_cobertura(tmp_path / "coverage.xml")

    root = ET.parse(tmp_path / "coverage.xml").getroot()
    assert root.find("sources/source").text is None
    [cls] = root.iter("class")
    assert cls.get("filename") == "utils/uuid.cc"
    assert [line.attrib for line in cls.iter("line")] == [
        {"number": "3", "hits": "0", "branch": "false"},
        {"number": "5", "hits": "2", "branch": "true", "condition-coverage": "50% (1/2)"},
    ]
