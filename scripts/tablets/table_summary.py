#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Shows per-table summary of size and internal tablet imbalance.

This is a high-level table-oriented view of size and tablet-size distribution.
It is useful for comparing tables to each other and spotting tables with uneven
tablet sizes.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from statistics import mean

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tablets.filters import add_cluster_filter_options
from tablets.filters import add_table_filter_options
from tablets.filters import get_table_filter
from tablets.filters import TabletFilter
from tablets.filters import get_tablet_filter
from tablets.render_utils import Column
from tablets.render_utils import PresentationOptions
from tablets.render_utils import SEPARATING_LINE
from tablets.render_utils import add_presentation_options
from tablets.render_utils import format_ovc_pct
from tablets.render_utils import format_pct
from tablets.render_utils import format_size
from tablets.render_utils import get_presentation_options_from_args
from tablets.render_utils import positive_int
from tablets.render_utils import print_table
from tablets.render_utils import render_hbar
from tablets.render_utils import render_sparkline
from tablets.stats import StatsAggregator
from tablets.stats import share
from tablets.topology import TableId
from tablets.topology import TableName
from tablets.topology import add_topology_source_args, get_topology_source_from_args


class SizeMode(Enum):
    """
    What the sizes are of. See --un-replicated.

    An un-replicated report takes the average tablet replica size for a given tablet; a
    replicated one sums all tablet replicas. Either way the report counts what it sizes:
    tablets, or replicas.
    """
    REPLICATED = "replicated"
    UNREPLICATED = "un-replicated"

    @property
    def unit(self) -> str:
        """
        What the headers call it. A row is one table's, so a replica of one of its tablets
        needs no further naming.
        """
        return "replica" if self is SizeMode.REPLICATED else "tablet"


def measure_tablet(replica_sizes: list[int], size_mode: SizeMode) -> list[float]:
    """
    The sizes a tablet contributes to its table: all of its replicas, or their average.

    Only shown replicas are measured, so under a filter a tablet counts for what the
    selection holds of it rather than for what the cluster holds.
    """
    if size_mode is SizeMode.REPLICATED:
        return list(replica_sizes)
    return [mean(replica_sizes)]


def get_columns(size_mode: SizeMode) -> list[Column]:
    """
    Builds the report's columns.

    The size mode names the sizes, and names the unit the report counts and sizes.
    """
    return [
        Column("name"),
        Column(f"{size_mode.value}\nsize [B]", "right"),
        Column(f"{size_mode.value}\nsize [%]", "right"),
        Column("", "left", csv=False),  # size hbar
        Column(f"{size_mode.unit}\ncount", "right"),
        Column(f"{size_mode.unit}\nsize ovc [%]", "right"),
        Column(f"{size_mode.unit}\navg size [B]", "right"),
        Column(f"{size_mode.unit}\nmax size [B]", "right"),
        Column("size\nin token space", "left", csv=False),
    ]


@dataclass
class TableRow:
    """
    A table the report has a row for, once a filter has been applied: what it is called, the
    size of every unit the report measures, and the size of its whole token space in token
    order, which is always un-replicated so that its shape is that of the data rather than
    of its placement.

    A tablet with no replica left contributes zero to the token space and nothing at all to
    the sizes: it takes up token space without being something the report counts.
    """
    name: TableName
    table_id: TableId
    sizes: StatsAggregator
    token_space: list[float]


# Tables read best listed by name, and worst-first by every other key.
SORT_KEYS = {
    "name": lambda row: row.name,
    "size": lambda row: row.sizes.total,
    # Overcommit is never below 1, so a table with none to report sorts last.
    "tablet.ovc": lambda row: row.sizes.ovc() or 0,
    "tablet.avg": lambda row: row.sizes.avg(),
}


def sort_table_rows(rows: list[TableRow], sort_key: str) -> list[TableRow]:
    return sorted(rows, key=SORT_KEYS[sort_key], reverse=sort_key != "name")


def measure_table(topo, table_id: TableId, name: TableName, accepts_tablet: TabletFilter,
                  size_mode: SizeMode) -> TableRow | None:
    """
    A table's row, or None when the filter left nothing of it to show.
    """
    row = TableRow(name=name, table_id=table_id, sizes=StatsAggregator(), token_space=[])
    for tablet in topo.get_tablet_map(table_id).tablets:
        replica_sizes = [topo.get_tablet_size(table_id, tablet, replica)
                         for replica in tablet.replicas
                         if accepts_tablet(replica)]
        if not replica_sizes:
            row.token_space.append(0)
            continue
        row.token_space.append(mean(replica_sizes))
        for size in measure_tablet(replica_sizes, size_mode):
            row.sizes.add(size)
    return row if row.sizes.count else None


def build_all_row(rows: list[TableRow], options: PresentationOptions) -> list | None:
    if not rows:
        return None

    # The ALL row measures every shown table's units taken together.
    sizes = StatsAggregator()
    for row in rows:
        sizes.merge(row.sizes)
    # Overcommit is a table's own measure and does not add up, so ALL reports the worst one.
    max_ovc = max((ovc for row in rows if (ovc := row.sizes.ovc()) is not None), default=None)

    return [
        "ALL",
        format_size(sizes.total, options),
        "100.00",
        None,
        sizes.count,
        format_ovc_pct(max_ovc, options),
        format_size(sizes.avg(), options),
        format_size(sizes.max, options),
        None,
    ]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Print one summary row per table",
        epilog=(
            "Size mode:\n"
            "  --un-replicated (the default) takes the average tablet replica size for a given tablet;\n"
            "  --replicated sums all tablet replicas. The report counts what it sizes: tablets, or replicas.\n"
            "  Every column below follows the mode, and the headers name the unit.\n"
            "  The \"size in token space\" histogram is always un-replicated, so that its shape is that of\n"
            "  the data rather than of its placement.\n"
            "\n"
            "Columns:\n"
            "  size [B]             Total size shown for the table.\n"
            "  size [%]             Fraction this table's shown size takes of the shown total.\n"
            "  count                Number of units shown for the table.\n"
            "  ovc [%]              Size overcommit of the shown units, as percentage deviation from their average.\n"
            "  avg size [B]         Average size of a shown unit.\n"
            "  max size [B]         Size of the largest shown unit.\n"
            "  size in token space  Histogram of the whole table's token space in token order, with filtered-out tablets contributing zero.\n"
            "\n"
            "ALL row semantics:\n"
            "  size [%] is always 100.00, the size bar is blank, and ovc [%] is the maximal shown table OVC.\n"
            "\n"
            "Filter semantics:\n"
            "  When --host/--shard/--rack/--dc is used, size is computed only from matching replicas,\n"
            "  and size [%] is relative to the total post-filtering.\n"
            "  The \"size in token space\" histogram only shows contribution from matching replicas.\n"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    source_group = parser.add_argument_group("Source options")
    add_topology_source_args(source_group)

    filtering_group = parser.add_argument_group("Filtering options")
    add_cluster_filter_options(filtering_group)
    add_table_filter_options(filtering_group)

    report_group = parser.add_argument_group("Report options")
    report_group.add_argument("--sort", choices=list(SORT_KEYS), default="size",
                              help="Sort rows by the selected column (default: size)")
    report_group.add_argument("--no-summary", action="store_true",
                              help="Skip the ALL row")
    report_group.add_argument("-n", type=positive_int, metavar="COUNT",
                              help="Show only the first COUNT tables, followed by a '...' row when more were left out")
    size_mode_group = report_group.add_mutually_exclusive_group()
    size_mode_group.add_argument("--un-replicated", dest="size_mode", action="store_const",
                                 const=SizeMode.UNREPLICATED, default=SizeMode.UNREPLICATED,
                                 help="Count tablets, take the average tablet replica size for a given tablet (default)")
    size_mode_group.add_argument("--replicated", dest="size_mode", action="store_const",
                                 const=SizeMode.REPLICATED,
                                 help="Count tablet replicas")

    presentation_group = parser.add_argument_group("Presentation options")
    add_presentation_options(presentation_group, has_tables=True)

    args = parser.parse_args()
    options = get_presentation_options_from_args(args)

    with get_topology_source_from_args(args) as src:
        topo = src.get_topology()

        accepts_table = get_table_filter(args, topo)
        accepts_tablet = get_tablet_filter(args, topo)

        rows = []
        for table_id, table_name in topo.iter_tables():
            if not accepts_table(table_id):
                continue
            row = measure_table(topo, table_id, table_name, accepts_tablet, args.size_mode)
            if row is not None:
                rows.append(row)

        rows = sort_table_rows(rows, args.sort)
        table_sizes = StatsAggregator.of(row.sizes.total for row in rows)

        formatted_rows = []
        all_row = None if args.no_summary else build_all_row(rows, options)
        if all_row is not None:
            formatted_rows.extend([all_row, SEPARATING_LINE])
        # Sizes and shares stay relative to every selected table, so -n only narrows what
        # is printed, never what the numbers mean.
        shown_rows = rows[:args.n] if args.n is not None else rows
        for row in shown_rows:
            formatted_rows.append([
                row.table_id if options.table_id else row.name,
                format_size(row.sizes.total, options),
                format_pct(share(row.sizes.total, table_sizes.total), options=options),
                render_hbar(row.sizes.total, table_sizes.max),
                row.sizes.count,
                format_ovc_pct(row.sizes.ovc(), options),
                format_size(row.sizes.avg(), options),
                format_size(row.sizes.max, options),
                render_sparkline(row.token_space, width=16, options=options),
            ])

        columns = get_columns(args.size_mode)
        # CSV is consumed by other tools, so it carries data rows only; the rendered table
        # says that -n left something out.
        if len(shown_rows) < len(rows) and not options.csv:
            formatted_rows.append(["..."] + [None] * (len(columns) - 1))

        print_table(formatted_rows, columns, options)
        return 0


if __name__ == "__main__":
    sys.exit(main())
