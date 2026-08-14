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
from itertools import zip_longest
from pathlib import Path
from statistics import mean

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tablets.filters import add_cluster_filter_options
from tablets.filters import TabletFilter
from tablets.filters import get_host_filter
from tablets.filters import get_tablet_filter
from tablets.filters import select_all
from tablets.render_utils import Column
from tablets.render_utils import PresentationOptions
from tablets.render_utils import SEPARATING_LINE
from tablets.render_utils import add_presentation_options
from tablets.render_utils import color_replica_histogram
from tablets.render_utils import format_count
from tablets.render_utils import format_ovc_ratio
from tablets.render_utils import format_pct
from tablets.render_utils import format_rack_id
from tablets.render_utils import format_shard_location
from tablets.render_utils import format_size
from tablets.render_utils import get_presentation_options_from_args
from tablets.render_utils import format_host
from tablets.render_utils import positive_int
from tablets.render_utils import print_table
from tablets.render_utils import render_hbar
from tablets.render_utils import render_sparkline
from tablets.stats import StatsAggregator
from tablets.stats import share
from tablets.topology import HostId
from tablets.topology import TableId
from tablets.topology import TabletMap
from tablets.topology import TabletReplica
from tablets.topology import Token
from tablets.topology import Topology
from tablets.topology import add_topology_source_args
from tablets.topology import get_topology_source_from_args
from tablets.topology import iter_token_fractions
from tablets.topology import resolve_table_id



@dataclass
class TabletLoadRange:
    last_token: Token
    tablet_count: int
    # Replicated size of the range, or of one replica once split. See build_ranges().
    size: int
    token_frac: float
    # Sizes of every replica of the range, in replica order, which a split range keeps so
    # that the ALL row can add them up replica by replica.
    replica_sizes: list[int]
    replicas: list[TabletReplica]

    # Sizes of every replica of the range, taken once when it is built. A split range keeps
    # them, so the measures below describe the whole range whichever row carries them.
    replica_stats: StatsAggregator
    # Set when --per-replica is used, and None otherwise.
    # Indexes into self.replicas.
    replica_idx: int | None = None

    @property
    def avg_replica_size(self) -> float:
        return self.replica_stats.avg()

    @property
    def replica_ovc(self) -> float | None:
        return self.replica_stats.ovc()

    def size_frac(self, total_size: int) -> float:
        return share(self.size, total_size)


def make_load_range(last_token: Token, tablet_count: int, token_frac: float,
                    replica_sizes: list[int], replicas: list[TabletReplica]) -> TabletLoadRange:
    """
    A range over the given replicas, whose size is theirs summed.
    """
    replica_stats = StatsAggregator()
    for replica_size in replica_sizes:
        replica_stats.add(replica_size)
    return TabletLoadRange(
        last_token=last_token,
        tablet_count=tablet_count,
        size=replica_stats.total,
        token_frac=token_frac,
        replica_sizes=replica_sizes,
        replicas=replicas,
        replica_stats=replica_stats,
    )


def sum_replica_sizes_by_index(ranges: list[TabletLoadRange]) -> list[int]:
    """
    The ranges' sizes added up replica by replica, so the n-th total is what every n-th
    replica holds together. Ranges with fewer replicas contribute nothing to the rest.
    """
    return [sum(sizes) for sizes in zip_longest(*(r.replica_sizes for r in ranges), fillvalue=0)]


def format_avg_size(values: list[float], options: PresentationOptions) -> str:
    return format_size(mean(values), options) if values else ""


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
        merged.append(make_load_range(
            last_token=group[-1].last_token,
            tablet_count=sum(r.tablet_count for r in group),
            token_frac=sum(r.token_frac for r in group),
            replica_sizes=sum_replica_sizes_by_index(group),
            replicas=[],
        ))

    return merged


# Ranges arrive in token order, which "token" therefore leaves alone; every other key
# sorts worst-first.
SORT_KEYS = {
    "token": None,
    "tokens": lambda load_range: load_range.token_frac,
    "size": lambda load_range: load_range.size,
    "ovc.rep": lambda load_range: load_range.replica_ovc or 0,
}


def sort_ranges(ranges: list[TabletLoadRange], sort_key: str) -> list[TabletLoadRange]:
    key = SORT_KEYS[sort_key]
    return ranges if key is None else sorted(ranges, key=key, reverse=True)


def build_ranges(table_id: TableId, tmap: TabletMap, topo: Topology,
                 accepts_tablet: TabletFilter = select_all,
                 per_replica: bool = False) -> list[TabletLoadRange]:
    ranges = []
    for tablet, token_frac in iter_token_fractions(tmap.tablets):
        replicas = [replica for replica in tablet.replicas if accepts_tablet(replica)]
        replica_sizes = [topo.get_tablet_size(table_id, tablet, replica) for replica in replicas]
        if not replica_sizes:
            continue
        load_range = make_load_range(
            last_token=tablet.last_token,
            tablet_count=1,
            token_frac=token_frac,
            replica_sizes=replica_sizes,
            replicas=replicas,
        )
        if not per_replica:
            ranges.append(load_range)
            continue

        # One range per replica, sized as that replica. The measures of the whole range
        # are the ones taken above, so every one of its rows reports the same.
        for idx, size in enumerate(replica_sizes):
            ranges.append(TabletLoadRange(
                last_token=load_range.last_token,
                tablet_count=load_range.tablet_count,
                size=size,
                token_frac=load_range.token_frac,
                replica_sizes=replica_sizes,
                replicas=replicas,
                replica_stats=load_range.replica_stats,
                replica_idx=idx,
            ))
    return ranges


@dataclass
class ReportShape:
    """
    Which columns the report has.

    Whether a row stands for one replica or for a whole range decides most of them: a
    replica row says where it sits, and cannot carry measures of the range it is part of.
    """
    per_replica: bool
    show_tablet_count: bool
    show_token_hist: bool


def build_shape(args: argparse.Namespace, tablet_count: int) -> ReportShape:
    show_tablet_count = args.max_ranges is not None and tablet_count > args.max_ranges
    return ReportShape(
        per_replica=args.per_replica,
        show_tablet_count=show_tablet_count,
        show_token_hist=args.tokens_hist,
    )


def format_replica_rack(topo: Topology, host_id: HostId) -> str:
    host = topo.get_host(host_id)
    if host is None:
        return ""
    if host.dc and host.rack:
        return format_rack_id((host.dc, host.rack))
    return ""


def format_replica_shard(topo: Topology, host_id: HostId, shard_id: int,
                         options: PresentationOptions) -> str:
    host = topo.get_host(host_id)
    host_part = str(host_id) if host is None else format_host(host, options)
    return format_shard_location(host_part, shard_id)


def build_summary_row(label: str, shape: ReportShape, *, tablets, tokens, size, avg_replica_size,
                      ovc, size_frac, size_bar, replicas) -> list:
    """
    A summary row, laid out like every other row so that the two stay in step.

    The token histogram is left blank: a summary spans the whole token space, so a bar of
    it would say nothing. So are the location columns, since a summary sits nowhere.
    """
    row = [label]
    if shape.show_tablet_count:
        row.append(tablets)
    row.append(tokens)
    if shape.show_token_hist:
        row.append(None)
    row.append(size)
    if not shape.per_replica:
        row.append(avg_replica_size)
    row.extend([ovc, size_frac, size_bar])
    if not shape.per_replica:
        row.append(replicas)
    if shape.per_replica:
        row.extend([None, None])
    return row


def build_summary_rows(rows: list[TabletLoadRange], shape: ReportShape,
                       options: PresentationOptions, summary: RowsSummary) -> list[list]:
    sizes = summary.sizes
    # Per-tablet measures average over tablets, not over the rows they take: ranges split
    # per replica would otherwise weigh a tablet by its replica count.
    tablets = list({r.last_token: r for r in rows}.values())
    total_replica_sizes = sum_replica_sizes_by_index(tablets)
    # Every row shown takes an equal share of the total, which is what an average row holds.
    avg_frac = share(1, sizes.count)

    total_row = build_summary_row(
        "ALL", shape,
        tablets=sum(r.tablet_count for r in tablets),
        tokens=None,
        size=format_size(sizes.total, options),
        avg_replica_size=format_avg_size(total_replica_sizes, options),
        # The widest row, as its overcommit against the average row.
        ovc=format_ovc_ratio(sizes.max or 0, sizes.avg(), options),
        size_frac=None,
        size_bar=None,
        replicas=render_sparkline(total_replica_sizes, options=options),
    )
    avg_row = build_summary_row(
        "Average", shape,
        tablets=format_count(mean(r.tablet_count for r in tablets), 3, options) if tablets else None,
        tokens=format_pct(mean(r.token_frac for r in tablets), 4, options) if tablets else None,
        size=format_size(sizes.avg(), options) if sizes.count else None,
        avg_replica_size=format_avg_size([r.avg_replica_size for r in tablets], options),
        ovc=None,
        size_frac=format_pct(avg_frac, 4, options) if avg_frac else None,
        size_bar=render_hbar(avg_frac, sizes.max_frac()) if avg_frac else None,
        replicas=None,
    )
    return [total_row, avg_row, SEPARATING_LINE]


def build_columns(shape: ReportShape, options: PresentationOptions) -> list[Column]:
    columns = [Column("last token")]
    if shape.show_tablet_count:
        columns.append(Column("tablets", "right"))
    columns.append(Column("tokens\n[%]", "right"))
    if shape.show_token_hist:
        columns.append(Column("tokens", "left", csv=False))
    columns.append(Column("replicated\nsize [B]", "right"))
    if not shape.per_replica:
        columns.append(Column("size\n/ replica [B]", "right"))
    columns.extend([Column("ovc\n[%]", "right"), Column("size\n[%]", "right"), Column("size", "left", csv=False)])
    if not shape.per_replica:
        columns.append(Column("rep", "left", csv=False))
    if shape.per_replica:
        columns.extend([Column("rack"), Column("shard")])
    return columns


def build_rows(rows: list[TabletLoadRange], shape: ReportShape, options: PresentationOptions,
                topo: Topology, summary: RowsSummary) -> list[list]:
    """
    One table row per row of the report: whether it shows a replica or a whole range is the
    row's own business, so nothing here has to be told which report this is.
    """
    total_size, avg_size = summary.sizes.total, summary.sizes.avg()
    max_frac, max_token_frac = summary.sizes.max_frac(), summary.max_token_frac()
    formatted = []
    prev_last_token = None
    for load_range in rows:
        idx = load_range.replica_idx
        frac = load_range.size_frac(total_size)
        # A tablet's measures show once, on the first of the rows it takes. Sorting can put
        # a tablet's replicas apart, so what counts is the row above, not the replica index.
        # A row of the rendered table reads them off the row above it; a csv row has to stand
        # on its own, so they repeat: summing the token share then counts every replica copy,
        # as cluster_load.py does, and RF=3 totals 300%. An unsplit range is its own tablet,
        # so it always shows them.
        show_tablet = load_range.last_token != prev_last_token or options.csv
        prev_last_token = load_range.last_token

        columns = [load_range.last_token if show_tablet else None]
        if shape.show_tablet_count:
            columns.append(load_range.tablet_count if show_tablet else None)
        columns.append(format_pct(load_range.token_frac, 4, options) if show_tablet else None)
        if shape.show_token_hist:
            columns.append(render_hbar(load_range.token_frac, max_token_frac) if show_tablet else None)
        columns.append(format_size(load_range.size, options))
        if not shape.per_replica:
            columns.append(format_size(load_range.avg_replica_size, options))
        columns.extend([
            format_ovc_ratio(load_range.size, avg_size, options),
            format_pct(frac, 4, options),
            color_replica_histogram(render_hbar(frac, max_frac), idx, options),
        ])
        if not shape.per_replica:
            columns.append(render_sparkline(load_range.replica_sizes, options=options) if load_range.replica_sizes else None)
        if shape.per_replica:
            host_id, shard_id = load_range.replicas[idx]
            columns.append(format_replica_rack(topo, host_id))
            columns.append(format_replica_shard(topo, host_id, shard_id, options))
        formatted.append(columns)

    return formatted


@dataclass(frozen=True)
class RowsSummary:
    """
    What the rows about to be printed are measured and scaled against.
    """
    # Replicated size of every row shown, whose total the shares are taken of.
    sizes: StatsAggregator
    # Token share of every row shown.
    token_fracs: StatsAggregator

    def max_token_frac(self) -> float:
        """
        The largest token share of any range shown, which scales the token bars.
        """
        return self.token_fracs.max or 0


def summarize_rows(rows: list[TabletLoadRange]) -> RowsSummary:
    sizes = StatsAggregator()
    token_fracs = StatsAggregator()
    for row in rows:
        sizes.add(row.size)
        token_fracs.add(row.token_frac)
    return RowsSummary(sizes=sizes, token_fracs=token_fracs)


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
            "  last token           Ending token of the tablet, or token range if aggregated.\n"
            "  tablets              Number of shown tablets in the row's token range (shown only when coalescing with --max-ranges).\n"
            "  tokens [%]           Fraction of the table's token space covered by the row.\n"
            "  tokens               Optional bar for token-space coverage (shown with --tokens-hist).\n"
            "  replicated size [B]  Total replicated size of shown replicas in the token range.\n"
            "  size / replica [B]   Replicated size of the row divided by its replica count (shown only without --per-replica).\n"
            "                       For a single-tablet row this is the average replica size. For rows that aggregate\n"
            "                       several tablets (--max-ranges) and for the ALL row, the replica count is the number of\n"
            "                       replicas per tablet, so the value is the size carried by one replica of the whole range.\n"
            "  rep                  Per-replica size histogram, one vertical bar per replica, scaled to the largest replica in the current row.\n"
            "  ovc [%]              Size overcommit, shown as percentage deviation from the average row size.\n"
            "                       In the ALL row, it's the maximal overcommit among the rows, and is a measure of imbalance between tablets.\n"
            "  size [%]             Fraction this row's size takes of the shown replicated size of the table.\n"
            "  rack                 Replica rack location (shown only with --per-replica).\n"
            "  shard                Replica host:shard location (shown only with --per-replica).\n"
            "\n"
            "Filter semantics:\n"
            "  When --host/--shard/--rack/--dc is used, size and size [%] are computed only from matching replicas.\n"
            "  tokens [%] still refers to the table's token space, not to a filtered token-space total.\n"
            "\n"
            "--per-replica rows:\n"
            "  A tablet's own columns show once, on the first of its rows, since a rendered table reads them off the row above.\n"
            "  In csv they repeat on every row, so a row stands on its own: summing tokens [%] then counts every replica copy,\n"
            "  as cluster-load does, and an RF=3 table totals 300%.\n"
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
    report_group.add_argument("--no-summary", action="store_true",
                              help="Skip the ALL and Average rows")
    report_group.add_argument("--sort", choices=list(SORT_KEYS), default="token",
                              help="Sort rows by the selected key (default: token). ovc.rep is overcommit between row's replicas (the 'rep' column)")

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

        topo.report_missing_tablet_sizes(lambda table: table == table_id,
                                         get_host_filter(args, topo))

        tablet_map = topo.get_tablet_map(table_id)
        tablet_ranges = build_ranges(table_id, tablet_map, topo, get_tablet_filter(args, topo), args.per_replica)
        ranges = merge_adjacent_ranges(tablet_ranges, args.max_ranges)
        ranges = sort_ranges(ranges, args.sort)
        shape = build_shape(args, len(tablet_ranges))
        summary = summarize_rows(ranges)

        table_rows = [] if args.no_summary else build_summary_rows(ranges, shape, options, summary)
        table_rows += build_rows(ranges, shape, options, topo, summary)
        if not print_table(table_rows, build_columns(shape, options), options):
            return 0
        return 0


if __name__ == "__main__":
    sys.exit(main())
