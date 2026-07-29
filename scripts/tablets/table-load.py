#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Presents table's internal load distribution across tablets / token space.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tablets.filters import add_cluster_filter_options
from tablets.filters import filter_tablet
from tablets.render_utils import Column
from tablets.render_utils import PresentationOptions
from tablets.render_utils import SEPARATING_LINE
from tablets.render_utils import add_presentation_options
from tablets.render_utils import color_replica_histogram
from tablets.render_utils import format_ovc_pct
from tablets.render_utils import format_rack_id
from tablets.render_utils import format_shard_location
from tablets.render_utils import format_size
from tablets.render_utils import get_presentation_options_from_args
from tablets.render_utils import format_host
from tablets.render_utils import print_table
from tablets.render_utils import render_hbar
from tablets.render_utils import render_sparkline
from tablets.topology import add_topology_source_args
from tablets.topology import get_topology_source_from_args
from tablets.topology import resolve_table_id


TOKEN_RING_SIZE = 1 << 64


@dataclass
class TabletLoadRange:
    last_token: int
    tablet_count: int
    size: int
    token_frac: float
    replica_sizes: list[int]
    replicas: list[tuple]

    @property
    def avg_replica_size(self) -> float:
        return self.size / len(self.replica_sizes) if self.replica_sizes else 0

    @property
    def replica_ovc(self) -> float | None:
        avg_replica_size = self.avg_replica_size
        if not avg_replica_size or not self.replica_sizes:
            return None
        return max(self.replica_sizes) / avg_replica_size

    def size_frac(self, total_size: int) -> float:
        return self.size / total_size if total_size else 0


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def sum_replica_sizes_by_index(ranges: list[TabletLoadRange]) -> list[int]:
    replica_count = max((len(r.replica_sizes) for r in ranges), default=0)
    totals = [0] * replica_count
    for load_range in ranges:
        for idx, replica_size in enumerate(load_range.replica_sizes):
            totals[idx] += replica_size
    return totals


def format_avg_size(values: list[float], options: PresentationOptions) -> str:
    if not values:
        return ""
    return format_size(sum(values) / len(values), options)


def format_ovc(size: int, avg_size: float) -> str:
    if not avg_size:
        return ""
    return format_ovc_pct(size / avg_size)


def merge_adjacent_ranges(ranges: list[TabletLoadRange], max_ranges: int | None) -> list[TabletLoadRange]:
    if not max_ranges or len(ranges) <= max_ranges:
        return ranges

    group_count = min(max_ranges, len(ranges))
    base_group_size = len(ranges) // group_count
    extra = len(ranges) % group_count

    merged = []
    idx = 0
    for group_idx in range(group_count):
        current_group_size = base_group_size + (1 if group_idx < extra else 0)
        group = ranges[idx:idx + current_group_size]
        idx += current_group_size
        merged.append(TabletLoadRange(
            last_token=group[-1].last_token,
            tablet_count=sum(r.tablet_count for r in group),
            size=sum(r.size for r in group),
            token_frac=sum(r.token_frac for r in group),
            replica_sizes=sum_replica_sizes_by_index(group),
            replicas=[],
        ))

    return merged


def sort_ranges(ranges: list[TabletLoadRange], sort_key: str) -> list[TabletLoadRange]:
    if sort_key == "last-token":
        return ranges
    if sort_key == "tokens":
        return sorted(ranges, key=lambda load_range: load_range.token_frac, reverse=True)
    if sort_key == "size":
        return sorted(ranges, key=lambda load_range: load_range.size, reverse=True)
    if sort_key == "ovc.rep":
        return sorted(ranges, key=lambda load_range: load_range.replica_ovc or 0, reverse=True)
    raise ValueError(f"Unsupported sort key: {sort_key}")


def build_ranges(table_id, tmap, topo, args=None) -> list[TabletLoadRange]:
    ranges = []
    prev_last_token = tmap.tablets[-1].last_token if tmap.tablets else 0
    for tablet in tmap.tablets:
        replicas = [replica for replica in tablet.replicas if args is None or filter_tablet(args, topo, replica)]
        replica_sizes = [topo.get_tablet_size(table_id, tablet, replica) for replica in replicas]
        token_frac = ((tablet.last_token - prev_last_token) % TOKEN_RING_SIZE) / TOKEN_RING_SIZE
        if replica_sizes:
            ranges.append(TabletLoadRange(
                last_token=tablet.last_token,
                tablet_count=1,
                size=sum(replica_sizes),
                token_frac=token_frac,
                replica_sizes=replica_sizes,
                replicas=replicas,
            ))
        prev_last_token = tablet.last_token
    return ranges


@dataclass
class ReportShape:
    show_tablet_count: bool
    show_replica_summary: bool
    show_avg_replica_size: bool
    show_token_hist: bool
    show_replica_location: bool


def build_shape(args, tablet_count: int) -> ReportShape:
    show_tablet_count = args.max_ranges is not None and tablet_count > args.max_ranges
    return ReportShape(
        show_tablet_count=show_tablet_count,
        show_replica_summary=not args.per_replica,
        show_avg_replica_size=not args.per_replica,
        show_token_hist=args.tokens_hist,
        show_replica_location=args.per_replica,
    )


def format_replica_rack(topo, host_id) -> str:
    host = topo.get_host(host_id)
    if host is None:
        return ""
    if host.dc and host.rack:
        return format_rack_id((host.dc, host.rack))
    return ""


def format_replica_shard(topo, host_id, shard_id: int, options: PresentationOptions) -> str:
    host = topo.get_host(host_id)
    host_part = str(host_id) if host is None else format_host(host, options)
    return format_shard_location(host_part, shard_id)


def build_summary_rows(ranges: list[TabletLoadRange], shape: ReportShape, per_replica: bool,
                       options: PresentationOptions, printed_sizes: list[int], avg_printed_size: float,
                       total_size: int, max_frac: float):
    avg_row = ["Average"]
    row_count = len(printed_sizes if per_replica else ranges)
    avg_frac = (1 / row_count) if row_count else 0
    if shape.show_tablet_count:
        avg_row.append(f"{sum(r.tablet_count for r in ranges) / len(ranges):.3f}" if ranges else "")
    avg_row.append(f"{sum(r.token_frac for r in ranges) * 100 / len(ranges):.4f}" if ranges else "")
    if shape.show_token_hist:
        avg_row.append(None)
    avg_row.append(format_avg_size(printed_sizes if per_replica else [r.size for r in ranges], options))
    if shape.show_avg_replica_size:
        avg_row.append(format_avg_size([r.avg_replica_size for r in ranges], options))
    avg_row.extend([
        None,
        f"{avg_frac * 100:.4f}" if avg_frac else "",
        render_hbar(avg_frac, max_frac) if avg_frac else "",
    ])
    if shape.show_replica_summary:
        avg_row.append(None)
    if shape.show_replica_location:
        avg_row.extend([None, None])

    total_replica_sizes = sum_replica_sizes_by_index(ranges)
    max_ovc = max((size / avg_printed_size for size in printed_sizes), default=0) if avg_printed_size else None
    total_row = ["ALL"]
    if shape.show_tablet_count:
        total_row.append(sum(r.tablet_count for r in ranges))
    total_row.append(None)
    if shape.show_token_hist:
        total_row.append(None)
    total_row.append(format_size(total_size, options))
    if shape.show_avg_replica_size:
        total_row.append(format_size(sum(total_replica_sizes) / len(total_replica_sizes), options) if total_replica_sizes else "")
    total_row.extend([
        format_ovc_pct(max_ovc),
        None,
        None,
    ])
    if shape.show_replica_summary:
        total_row.append(render_sparkline(total_replica_sizes))
    if shape.show_replica_location:
        total_row.extend([None, None])

    return [total_row, avg_row, SEPARATING_LINE]


def build_columns(shape: ReportShape, options: PresentationOptions) -> list[Column]:
    columns = [Column("last token")]
    if shape.show_tablet_count:
        columns.append(Column("tablets", "right"))
    columns.append(Column("tokens\n[%]", "right"))
    if shape.show_token_hist:
        columns.append(Column("tokens", "left", csv=False))
    columns.append(Column("replicated\nsize [B]", "right"))
    if shape.show_avg_replica_size:
        columns.append(Column("size\n/ replica [B]", "right"))
    columns.extend([Column("ovc\n[%]", "right"), Column("size\n[%]", "right"), Column("size", "left", csv=False)])
    if shape.show_replica_summary:
        columns.append(Column("rep", "left", csv=False))
    if shape.show_replica_location:
        columns.extend([Column("rack"), Column("shard")])
    return columns


def build_detail_rows(ranges: list[TabletLoadRange], shape: ReportShape, per_replica: bool,
                      options: PresentationOptions, topo, total_size: int, max_token_frac: float,
                       avg_printed_size: float, max_frac: float):
    rows = []
    for load_range in ranges:
        if per_replica:
            for idx, replica_size in enumerate(load_range.replica_sizes):
                replica_frac = replica_size / total_size if total_size else 0
                # Repeated in csv so that a row identifies its tablet. The tablet's measures
                # below stay blank, or summing a column would count them once per replica.
                row = [load_range.last_token if idx == 0 or options.csv else None]
                if shape.show_tablet_count:
                    row.append(load_range.tablet_count if idx == 0 else None)
                row.append(f"{load_range.token_frac * 100:.4f}" if idx == 0 else None)
                if shape.show_token_hist:
                    row.append(render_hbar(load_range.token_frac, max_token_frac) if idx == 0 else None)
                row.extend([
                    format_size(replica_size, options),
                    format_ovc(replica_size, avg_printed_size),
                    f"{replica_frac * 100:.4f}",
                    color_replica_histogram(render_hbar(replica_frac, max_frac), idx),
                ])
                replica = load_range.replicas[idx]
                if shape.show_replica_location:
                    row.append(format_replica_rack(topo, replica[0]))
                    row.append(format_replica_shard(topo, replica[0], replica[1], options))
                rows.append(row)
            continue

        frac = load_range.size_frac(total_size)
        row = [load_range.last_token]
        if shape.show_tablet_count:
            row.append(load_range.tablet_count)
        row.append(f"{load_range.token_frac * 100:.4f}")
        if shape.show_token_hist:
            row.append(render_hbar(load_range.token_frac, max_token_frac))
        row.extend([
            format_size(load_range.size, options),
            format_size(load_range.avg_replica_size, options),
            format_ovc(load_range.size, avg_printed_size),
            f"{frac * 100:.4f}",
            render_hbar(frac, max_frac),
        ])
        if shape.show_replica_summary:
            row.append(render_sparkline(load_range.replica_sizes))
        rows.append(row)

    return rows


def compute_printed_scales(ranges: list[TabletLoadRange], per_replica: bool, total_size: int) -> tuple[list[int], float, float]:
    """
    Returns ``(printed_sizes, avg_printed_size, max_frac)`` for the rows being printed.

    In per-replica mode the unit is a single replica size; otherwise it is a whole
    tablet/range size. ``max_frac`` scales the size bars and is 0 when nothing is shown.
    """
    if per_replica:
        printed_sizes = [replica_size for r in ranges for replica_size in r.replica_sizes]
    else:
        printed_sizes = [r.size for r in ranges]
    avg_printed_size = sum(printed_sizes) / len(printed_sizes) if printed_sizes else 0
    max_frac = max((size / total_size if total_size else 0) for size in printed_sizes) if printed_sizes else 0
    return printed_sizes, avg_printed_size, max_frac


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Presents per-tablet load information for a single table\n"
                    "\n"
                    "By default shows one row per tablet, aggregating all replicas. Use --per-replica to show one row per tablet replica.\n"
                    "\n"
                    "You can aggregate tablet ranges using --max-ranges. This will merge adjacent tablets until the printed range count\n"
                    "is at most the specified value. It's a way to simulate table-level tablet merge events.\n",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Columns:\n"
            "  last token       Ending token of the tablet, or token range if aggregated.\n"
            "  tablets          Number of shown tablets in the row's token range (shown only when coalescing with --max-ranges).\n"
            "  tokens [%]       Fraction of the table's token space covered by the row.\n"
            "  tokens           Optional bar for token-space coverage (shown with --tokens-hist).\n"
            "  replicated size [B]  Total replicated size of shown replicas in the token range.\n"
            "  size / replica [B]   Replicated size of the row divided by its replica count (shown only without --per-replica).\n"
            "                   For a single-tablet row this is the average replica size. For rows that aggregate\n"
            "                   several tablets (--max-ranges) and for the ALL row, the replica count is the number of\n"
            "                   replicas per tablet, so the value is the size carried by one replica of the whole range.\n"
            "  rep              Per-replica size histogram, one vertical bar per replica, scaled to the largest replica in the current row.\n"
            "  ovc [%]          Size overcommit, shown as percentage deviation from the average row size.\n"
            "                   In the ALL row, it's the maximal overcommit among the rows, and is a measure of imbalance between tablets.\n"
            "  size [%]         Fraction this row's size takes of the shown replicated size of the table.\n"
            "  rack             Replica rack location (shown only with --per-replica).\n"
            "  shard            Replica host:shard location (shown only with --per-replica).\n"
            "\n"
            "Filter semantics:\n"
            "  When --host/--shard/--rack/--dc is used, size and size [%] are computed only from matching replicas.\n"
            "  tokens [%] still refers to the table's token space, not to a filtered token-space total.\n"
            "  Tablets with no matching replicas are omitted from the output.\n"
        ),
    )
    source_group = parser.add_argument_group("Source options")
    add_topology_source_args(source_group)

    filtering_group = parser.add_argument_group("Filtering options")
    add_cluster_filter_options(filtering_group)

    parser.add_argument("table", help="Table selector: exact ks.table, unique bare table name, or table UUID")

    report_group = parser.add_argument_group("Report options")
    report_group.add_argument("--max-ranges", type=positive_int,
                              help="Merge adjacent tablets until the printed range count is at most this value")
    report_group.add_argument("--per-replica", action="store_true",
                              help="Print one row per tablet replica instead of one row per tablet")
    report_group.add_argument("--tokens-hist", action="store_true",
                              help="Show a token-space histogram column")
    report_group.add_argument("--sort", choices=["last-token", "tokens", "size", "ovc.rep"], default="last-token",
                              help="Sort rows by the selected key (default: last-token). ovc.rep is overcommit between row's replicas (the 'rep' column)")

    presentation_group = parser.add_argument_group("Presentation options")
    add_presentation_options(presentation_group, has_hosts=True)

    args = parser.parse_args()
    if args.per_replica and args.max_ranges is not None:
        parser.error("--per-replica is incompatible with --max-ranges")
    options = get_presentation_options_from_args(args)

    with get_topology_source_from_args(args) as src:
        topo = src.get_topology()
        table_id = resolve_table_id(topo, args.table)
        if "." not in args.table and not options.csv:
            print(f"Table: {topo.get_table_name(table_id)}")

        tablet_map = topo.get_tablet_map(table_id)
        ranges = merge_adjacent_ranges(build_ranges(table_id, tablet_map, topo, args), args.max_ranges)
        ranges = sort_ranges(ranges, args.sort)
        total_size = sum(r.size for r in ranges)
        shape = build_shape(args, len(tablet_map.tablets))

        max_token_frac = max((r.token_frac for r in ranges), default=0)
        printed_sizes, avg_printed_size, max_frac = compute_printed_scales(ranges, args.per_replica, total_size)

        rows = build_summary_rows(ranges, shape, args.per_replica, options, printed_sizes, avg_printed_size, total_size, max_frac)
        rows += build_detail_rows(ranges, shape, args.per_replica, options, topo, total_size, max_token_frac, avg_printed_size, max_frac)
        if not print_table(rows, build_columns(shape, options), options):
            return 0
        return 0


if __name__ == "__main__":
    sys.exit(main())
