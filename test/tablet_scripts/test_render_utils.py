#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import argparse
import builtins

from tablets.render_utils import Column
from tablets.render_utils import PresentationOptions
from tablets.render_utils import SEPARATING_LINE
from tablets.render_utils import add_presentation_options
from tablets.render_utils import print_table
from tablets.render_utils import render_table


def test_render_table_csv_skips_separators_and_normalizes_headers() -> None:
    rendered = render_table(
        rows=[
            ["foo", "\033[31m1\033[0m"],
            SEPARATING_LINE,
            ["bar", None],
        ],
        headers=["col\n1", "col2"],
        csv_output=True,
    )

    assert rendered.splitlines() == [
        "col 1,col2",
        "foo,1",
        "bar,",
    ]


def test_print_table_keeps_non_csv_columns_in_table_output(monkeypatch) -> None:
    captured = {}
    monkeypatch.setattr(builtins, "print", lambda text: captured.setdefault("text", text))

    columns = [
        Column("name"),
        Column("bar", "left", csv=False),
        Column("size", "right"),
    ]
    # Without CSV output, non-csv columns are still rendered.
    assert print_table([["foo", "████", "1"]], columns) is True
    assert "bar" in captured["text"]
    assert "████" in captured["text"]


def test_print_table_does_not_reparse_numbers(monkeypatch) -> None:
    """
    Cells reach print_table already formatted. Letting tabulate re-parse them as
    numbers drops the OVC sign and trailing zeros ("+1.60" -> "1.6"), so number
    parsing must stay disabled for every caller.
    """
    captured = {}
    monkeypatch.setattr(builtins, "print", lambda text: captured.setdefault("text", text))

    columns = [Column("name"), Column("ovc\n[%]", "right"), Column("size\n[%]", "right")]
    assert print_table([["ks.tbl", "+1.60", "100.00"]], columns) is True
    assert "+1.60" in captured["text"]
    assert "100.00" in captured["text"]


def test_print_table_renders_a_table_a_filter_emptied(monkeypatch) -> None:
    """
    A filter matching nothing leaves no rows. Alignment is derived from the rows, so
    the aligned columns must not outlive them.
    """
    captured = {}
    monkeypatch.setattr(builtins, "print", lambda text: captured.setdefault("text", text))

    columns = [Column("name"), Column("bar", "left", csv=False), Column("size", "right")]
    assert print_table([], columns) is True
    assert "name" in captured["text"]
    assert print_table([], columns, PresentationOptions(csv=True)) is True


def test_add_presentation_options_adds_requested_flags() -> None:
    parser = argparse.ArgumentParser()
    add_presentation_options(parser, has_hosts=True, has_tables=True)

    args = parser.parse_args(["--csv", "--host-id", "--table-id", "--no-hr"])

    assert args.csv is True
    assert args.host_id is True
    assert args.table_id is True
    assert args.hr is False


def test_print_table_ignores_broken_pipe(monkeypatch) -> None:
    monkeypatch.setattr(builtins, "print", lambda *args, **kwargs: (_ for _ in ()).throw(BrokenPipeError()))

    assert print_table([["x"]], [Column("h")]) is False


def test_print_table_derives_headers_alignment_and_csv_filter(monkeypatch) -> None:
    captured = {}
    monkeypatch.setattr(builtins, "print", lambda text: captured.setdefault("text", text))

    columns = [
        Column("name"),
        Column("bar", "left", csv=False),
        Column("size", "right"),
    ]
    assert print_table([["foo", "████", "1"]], columns, PresentationOptions(csv=True)) is True
    assert captured["text"].splitlines() == [
        "name,size",
        "foo,1",
    ]
