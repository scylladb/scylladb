#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Shows per-table summary of size and internal tablet imbalance.

This is a high-level table-oriented view of replicated size and tablet-size
distribution. It is useful for comparing tables to each other and spotting
tables with uneven tablet sizes.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tablets.filters import add_cluster_filter_options
from tablets.filters import add_table_filter_options
from tablets.filters import filter_table_id
from tablets.filters import filter_tablet
from tablets.render_utils import Column
from tablets.render_utils import PresentationOptions
from tablets.render_utils import SEPARATING_LINE
from tablets.render_utils import add_presentation_options
from tablets.render_utils import format_ovc_pct
from tablets.render_utils import format_size
from tablets.render_utils import get_presentation_options_from_args
from tablets.render_utils import print_table
from tablets.render_utils import render_hbar
from tablets.render_utils import render_sparkline
from tablets.topology import add_topology_source_args, get_topology_source_from_args


def sort_table_rows(rows: list[dict], sort_key: str) -> list[dict]:
    if sort_key == "name":
        return sorted(rows, key=lambda row: row["name"])
    if sort_key == "size":
        return sorted(rows, key=lambda row: row["total_size"], reverse=True)
    if sort_key == "tablet.ovc":
        return sorted(rows, key=lambda row: (row["tablet_ovc"] is not None, row["tablet_ovc"] or 0), reverse=True)
    if sort_key == "tablet.avg":
        return sorted(rows, key=lambda row: row["avg_tablet_size"], reverse=True)
    raise ValueError(f"Unsupported sort key: {sort_key}")


def calculate_total_size_and_tablet_ovc(tablet_sizes: list[int]) -> tuple[int, float | None]:
    total_size = sum(tablet_sizes)
    if not tablet_sizes:
        return total_size, None
    avg_tablet_size = total_size / len(tablet_sizes)
    if not avg_tablet_size:
        return total_size, None
    return total_size, max(tablet_sizes) / avg_tablet_size


def collect_filtered_tablet_sizes(topo, table_id, tmap, args) -> tuple[list[int], list[int], list[int]]:
    tablet_sizes = []
    token_space_sizes = []
    replica_sizes = []
    for tablet in tmap.tablets:
        filtered_replica_sizes = [topo.get_tablet_size(table_id, tablet, replica)
                                  for replica in tablet.replicas
                                  if filter_tablet(args, topo, replica)]
        if filtered_replica_sizes:
            size = sum(filtered_replica_sizes)
            tablet_sizes.append(size)
            token_space_sizes.append(size)
            replica_sizes.extend(filtered_replica_sizes)
        else:
            token_space_sizes.append(0)
    return tablet_sizes, token_space_sizes, replica_sizes


def build_all_row(rows: list[dict], options: PresentationOptions) -> list | None:
    if not rows:
        return None

    total_size = sum(row["total_size"] for row in rows)
    total_tablet_count = sum(row["tablet_count"] for row in rows)
    total_replica_count = sum(row["replica_count"] for row in rows)
    max_tablet_ovc = max((row["tablet_ovc"] for row in rows if row["tablet_ovc"] is not None), default=None)
    max_tablet_size = max((row["max_tablet_size"] for row in rows), default=0)

    return [
        "ALL",
        format_size(total_size, options),
        "100.00",
        "",
        total_tablet_count,
        format_ovc_pct(max_tablet_ovc),
        format_size(total_size / total_replica_count, options) if total_replica_count else "",
        format_size(max_tablet_size, options),
        "",
    ]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Print one summary row per table",
        epilog=(
            "Columns:\n"
            "  replicated size [B]  Total replicated size shown for the table.\n"
            "  replicated size [%]  Fraction this table's shown replicated size takes of the shown total.\n"
            "  tablet count         Number of tablets with at least one shown replica.\n"
            "  ovc [%]              Tablet-size overcommit of shown tablet sizes in token space, shown as percentage deviation from average.\n"
            "  tablet avg size [B]  Average size of shown tablet replicas.\n"
            "  tablet max size [B]  Maximum size of a shown tablet replica.\n"
            "  size in token space  Histogram of the whole table's token space in token order, with filtered-out tablets contributing zero.\n"
            "\n"
            "ALL row semantics:\n"
            "  replicated size [%] is always 100.00, the size bar is blank, and ovc [%] is the maximal shown table OVC.\n"
            "\n"
            "Filter semantics:\n"
            "  When --host/--shard/--rack/--dc is used, replicated size is computed only from matching replicas,\n"
            "  and replicated size [%] is relative to the total post-filtering.\n"
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
    report_group.add_argument("--sort", choices=["name", "size", "tablet.ovc", "tablet.avg"], default="size",
                              help="Sort rows by the selected column (default: size)")

    presentation_group = parser.add_argument_group("Presentation options")
    add_presentation_options(presentation_group, has_tables=True)

    args = parser.parse_args()
    options = get_presentation_options_from_args(args)

    with get_topology_source_from_args(args) as src:
        topo = src.get_topology()

        rows = []
        max_table_size = 0
        for table_id, table_name in topo.iter_tables():
            if not filter_table_id(args, topo, table_id):
                continue
            tmap = topo.get_tablet_map(table_id)
            tablet_sizes, token_space_sizes, replica_sizes = collect_filtered_tablet_sizes(topo, table_id, tmap, args)
            tablet_count = len(tablet_sizes)
            total_size, tablet_ovc = calculate_total_size_and_tablet_ovc(tablet_sizes)
            if not tablet_sizes:
                continue
            avg_tablet_size = sum(replica_sizes) / len(replica_sizes) if replica_sizes else 0
            max_tablet_size = max(replica_sizes, default=0)
            max_table_size = max(max_table_size, total_size)

            rows.append({
                "name": table_name,
                "table_id": table_id,
                "total_size": total_size,
                "tablet_count": tablet_count,
                "replica_count": len(replica_sizes),
                "tablet_ovc": tablet_ovc,
                "avg_tablet_size": avg_tablet_size,
                "max_tablet_size": max_tablet_size,
                "histogram": render_sparkline(token_space_sizes, width=16),
            })

        rows = sort_table_rows(rows, args.sort)

        formatted_rows = []
        all_row = build_all_row(rows, options)
        if all_row is not None:
            formatted_rows.extend([all_row, SEPARATING_LINE])
        shown_total_size = sum(row["total_size"] for row in rows)
        for row in rows:
            formatted_rows.append([
                row["table_id"] if options.table_id else row["name"],
                format_size(row["total_size"], options),
                f"{row['total_size'] * 100 / shown_total_size:.2f}" if shown_total_size else "0.00",
                render_hbar(row["total_size"], max_table_size),
                row["tablet_count"],
                format_ovc_pct(row["tablet_ovc"]),
                format_size(row["avg_tablet_size"], options),
                format_size(row["max_tablet_size"], options),
                row["histogram"],
            ])

        print_table(
            formatted_rows,
            [
                Column("name"),
                Column("replicated\nsize [B]", "right"),
                Column("replicated\nsize [%]", "right"),
                Column("", "left", csv=False), # size hbar
                Column("tablet\ncount", "right"),
                Column("tablet size\novc [%]", "right"),
                Column("tablet\navg size [B]", "right"),
                Column("tablet\nmax size [B]", "right"),
                Column("size\nin token space", "left", csv=False),
            ],
            options,
        )
        return 0


if __name__ == "__main__":
    sys.exit(main())
