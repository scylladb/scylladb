#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Prints per- rack/node/shard tablet load information.
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tablets.filters import HostFilter
from tablets.filters import TableFilter
from tablets.filters import TabletFilter
from tablets.filters import add_cluster_filter_options
from tablets.filters import add_table_filter_options
from tablets.filters import get_host_filter
from tablets.filters import get_table_filter
from tablets.filters import get_tablet_filter
from tablets.filters import resolve_table_filter_id
from tablets.filters import select_all
from tablets.topology import Host
from tablets.topology import RackId
from tablets.topology import Tablet
from tablets.topology import TableId
from tablets.render_utils import Column
from tablets.render_utils import DEFAULT_PRESENTATION
from tablets.render_utils import PresentationOptions
from tablets.render_utils import SEPARATING_LINE
from tablets.render_utils import add_presentation_options
from tablets.render_utils import format_ovc_pct
from tablets.render_utils import format_host_status
from tablets.render_utils import format_pct
from tablets.render_utils import format_rack_id
from tablets.render_utils import format_shard_location
from tablets.render_utils import format_size
from tablets.render_utils import format_tablets_per_shard
from tablets.render_utils import format_util_pct
from tablets.render_utils import format_host
from tablets.render_utils import get_presentation_options_from_args
from tablets.render_utils import print_table
from tablets.render_utils import red
from tablets.render_utils import render_hbar
from tablets.stats import StatsAggregator
from tablets.stats import overcommit
from tablets.stats import share
from tablets.topology import add_topology_source_args
from tablets.topology import iter_token_fractions
from tablets.topology import get_topology_source_from_args

class CapacityMode(Enum):
    """
    Which capacity utilization is measured against. See --capacity.
    """
    EFFECTIVE = "effective"
    ABSOLUTE = "absolute"


# The --capacity spellings, each mode's short one first.
CAPACITY_MODES = {
    "eff": CapacityMode.EFFECTIVE,
    "effective": CapacityMode.EFFECTIVE,
    "abs": CapacityMode.ABSOLUTE,
    "absolute": CapacityMode.ABSOLUTE,
}


def get_columns(capacity_mode: CapacityMode, options: PresentationOptions = DEFAULT_PRESENTATION) -> list[Column]:
    """
    Builds the report's columns.

    The capacity mode only rewords the capacity and utilization columns. CSV adds the
    leading columns which keep a flat table's rows self-describing, and names the label
    column, which the rendered table leaves blank because its section says what it is.

    A node's status is a column of its own in CSV, which is read by column, and part of the
    location label otherwise.
    """
    effective = capacity_mode == CapacityMode.EFFECTIVE
    columns = [
        Column("location" if options.csv else ""),
        *([Column("status")] if options.csv else []),
        Column("tablets\n/ shard", "right"),
        Column("shard\ncount", "right"),
        Column("tokens\n[%]", "right"),
        Column("token ovc\n[%]", "right"),
        Column("tokens", "left", csv=False),
        Column("size\n[B]", "right"),
        Column("size\n[%]", "right"),
        Column("size ovc\n[%]", "right"),
        Column("size", "left", csv=False),
        Column("eff capacity\n[B]" if effective else "capacity\n[B]", "right"),
        Column("eff util\n[%]" if effective else "util\n[%]", "right"),
        Column("util ovc\n[%]", "right"),
        Column("eff util" if effective else "util", "left", csv=False),
    ]
    if options.csv:
        # CSV is one flat table, so every row has to say which section it came from
        # and which rack it sits in. See build_location_columns().
        columns = [Column("level"), Column("rack"), *columns]
    return columns


# Rack header rows span the rendered table, which has no CSV-only columns.
COLUMN_COUNT = len(get_columns(CapacityMode.EFFECTIVE))


def build_location_columns(level: str, rack_id: RackId | None) -> list:
    """
    Leading CSV-only columns naming the row's section and rack.

    Replace the rack header rows the rendered table groups by, so every row stays
    self-describing once the sections are concatenated into one.
    """
    return [level, format_rack_id(rack_id) if rack_id is not None else None]


def get_host_capacity(host: Host, capacity_mode: CapacityMode) -> int:
    """
    Returns the host capacity for the given mode, or 0 when the snapshot does not
    carry it.

    The modes are never substituted for each other: an effective capacity missing
    from the snapshot is reported as unknown rather than as the absolute capacity,
    which would silently label absolute numbers as effective.
    """
    if capacity_mode == CapacityMode.EFFECTIVE:
        return host.effective_capacity or 0
    return host.storage_capacity or 0

def max_ovc(ovcs: Iterable[float | None]) -> float | None:
    """
    The worst overcommit among peers, or None when none of them has one to report.
    """
    return max((ovc for ovc in ovcs if ovc is not None), default=None)


@dataclass
class ChildStats:
    """
    What the items merged into one measured, counted one apiece.

    An item's overcommit is against these: they are the peers it is shown beside. Note that
    this is not the item's own aggregate said twice: a node's sizes hold one entry per
    replica, these hold one per shard. The totals agree, but only the average is read from
    here, and the average of a shard is not the average of a replica.
    """
    sizes: StatsAggregator = field(default_factory=StatsAggregator)
    tokens: StatsAggregator = field(default_factory=StatsAggregator)
    utils: StatsAggregator = field(default_factory=StatsAggregator)

    def add(self, child: LevelLoad) -> None:
        self.sizes.add(child.size)
        self.tokens.add(child.token_fraction)
        self.utils.add(child.util)


@dataclass
class LevelLoad:
    """
    One row's worth of load: a shard, a node, a rack, or a whole DC.

    Every level is measured the same way, over the replicas it was given: their sizes and
    the token space they own, counted once each. What differs between levels is only which
    replicas an item is given, and what the item is called.

    So sizes and tokens are per replica, however far up the level is, and the counts they
    carry are of replicas. Per-item measures come from `children`.
    """
    dc: str
    rack_id: RackId | None = None
    host: Host | None = None
    shard_id: int | None = None

    # Aggregated tablet replica sizes for this item. One sample per tablet replica.
    sizes: StatsAggregator = field(default_factory=StatsAggregator)
    # Aggregated tablet replica token fractions for this item. One sample per tablet replica.
    tokens: StatsAggregator = field(default_factory=StatsAggregator)

    # Structural, so a filter narrowing the replicas above leaves them alone. See collect_levels().
    shard_count: int = 0
    capacity: int = 0

    # Aggregated stats for the level below, with one sample per element.
    # For example, if this level describes a node, children contains samples for that node's shards.
    children: ChildStats = field(default_factory=ChildStats)

    # Overcommit of size on this item relative to peers at the same level.
    # For a shard, peers are shards on the same node.
    # For a node, peers are nodes on the same rack.
    # For a rack, peers are racks in the same DC.
    ovc: float | None = None
    # Overcommit of token fraction on this item relative to peers at the same level.
    token_ovc: float | None = None
    # Overcommit of utilization on this item relative to peers at the same level.
    util_ovc: float | None = None

    def add_replica(self, size: int, token_fraction: float) -> None:
        self.sizes.add(size)
        self.tokens.add(token_fraction)

    def merge(self, below: LevelLoad) -> None:
        """
        Takes in an item of the level below: everything it holds, the shards it counts, and
        its measures, which its peers are then compared against.

        Capacity is not merged, because the levels do not agree on which of it to count.
        See collect_levels().
        """
        self.sizes.merge(below.sizes)
        self.tokens.merge(below.tokens)
        self.shard_count += below.shard_count
        self.children.add(below)

    def set_ovc(self, parent: LevelLoad) -> None:
        """
        Sets how far this item is from the peers it is shown beside, which are the other
        items merged into its parent. Only meaningful once the parent has merged them all.
        """
        self.ovc = overcommit(self.size, parent.children.sizes.avg())
        self.token_ovc = overcommit(self.token_fraction, parent.children.tokens.avg())
        self.util_ovc = overcommit(self.util, parent.children.utils.avg())

    def take_worst_ovc(self, below: LevelLoad) -> None:
        """
        Overcommit does not add up, so a level with no peers of its own on the report
        carries the worst of the items it merged instead.
        """
        self.ovc = max_ovc((self.ovc, below.ovc))
        self.token_ovc = max_ovc((self.token_ovc, below.token_ovc))
        self.util_ovc = max_ovc((self.util_ovc, below.util_ovc))

    @property
    def level(self) -> str:
        if self.shard_id is not None:
            return "shard"
        if self.host is not None:
            return "node"
        if self.rack_id is not None:
            return "rack"
        return "dc"

    @property
    def size(self) -> int:
        return self.sizes.total

    @property
    def token_fraction(self) -> float:
        return self.tokens.total

    @property
    def tablet_count(self) -> int:
        return self.sizes.count

    @property
    def tablets_per_shard(self) -> float:
        return self.tablet_count / self.shard_count if self.shard_count else 0

    @property
    def util(self) -> float:
        return self.size / self.capacity if self.capacity else 0


@dataclass
class Levels:
    """
    The report's items at each level, each in the order its section shows them.
    """
    shards: list[LevelLoad]
    nodes: list[LevelLoad]
    racks: list[LevelLoad]
    dcs: list[LevelLoad]
    # One full ring per selected table, which is what a row's token share is taken of.
    # A row counts every replica it holds, so with RF=3 a DC's share reaches 300%.
    total_token_space: float = 0

    @property
    def total_size(self) -> int:
        """
        The replicated size every row's size share is taken of.
        """
        return sum(node.size for node in self.nodes)


@dataclass(frozen=True)
class SectionScales:
    """
    What a section's bars are drawn against: the largest value it shows.
    """
    max_size: int | float
    max_token_fraction: float
    max_util: float


def get_section_scales(loads: list[LevelLoad]) -> SectionScales:
    return SectionScales(
        max_size=max((load.size for load in loads), default=0),
        max_token_fraction=max((load.token_fraction for load in loads), default=0),
        max_util=max((load.util for load in loads), default=0),
    )


def iter_tablets_with_token_fraction(topo, table_ids: list[TableId]) -> Iterator[tuple[TableId, Tablet, float]]:
    for current_table_id in table_ids:
        tablet_map = topo.get_tablet_map(current_table_id)
        for tablet, token_fraction in iter_token_fractions(tablet_map.tablets):
            yield current_table_id, tablet, token_fraction


def get_selected_table_ids(topo, table_id: TableId | None,
                           accepts_table: TableFilter = select_all) -> list[TableId]:
    if table_id is not None:
        return [table_id]
    return [table_id for table_id in topo.iter_table_ids() if accepts_table(table_id)]


def collect_levels(topo, table_ids: list[TableId], capacity_mode: CapacityMode,
                   accepts_tablet: TabletFilter = select_all,
                   accepts_host: HostFilter = select_all) -> Levels:
    """
    Measures every level in one walk of the replicas.

    A replica is counted once, by the shard holding it, and every level above is the merge
    of the one below: shards into their node, nodes into their rack, racks into their DC.
    The levels therefore agree by construction rather than by being summed up separately.

    Shard counts and capacity come from the topology instead, since they are properties of
    the hardware: a node shows all of its shards even under --shard, and a rack shows the
    capacity of every node in it, including nodes a filter kept off the report. A DC counts
    only the racks it shows, which is what its summary row is a summary of.
    """
    nodes: dict[Any, LevelLoad] = {}
    shards: dict[Any, LevelLoad] = {}
    racks: dict[RackId, LevelLoad] = {}
    dcs: dict[str, LevelLoad] = {}

    for host in topo.all_normal_token_owner_hosts():
        if not host.dc or not host.rack:
            continue
        rack_id = (host.dc, host.rack)
        shard_count = host.shard_count or 0
        capacity = get_host_capacity(host, capacity_mode)
        nodes[host.id] = LevelLoad(dc=host.dc, rack_id=rack_id, host=host,
                                   shard_count=shard_count, capacity=capacity)
        for shard_id in range(shard_count):
            if accepts_tablet((host.id, shard_id)):
                shards[(host.id, shard_id)] = LevelLoad(dc=host.dc, rack_id=rack_id, host=host,
                                                        shard_id=shard_id,
                                                        capacity=capacity // shard_count)
        racks.setdefault(rack_id, LevelLoad(dc=host.dc, rack_id=rack_id)).capacity += capacity
        dcs.setdefault(host.dc, LevelLoad(dc=host.dc))

    for table_id, tablet, token_fraction in iter_tablets_with_token_fraction(topo, table_ids):
        for replica in tablet.replicas:
            host_id, _ = replica
            node = nodes.get(host_id)
            if node is None or not accepts_tablet(replica):
                continue
            size = topo.get_tablet_size(table_id, tablet, replica)
            # A replica on a shard its host does not report having is still the node's.
            (shards.get(replica) or node).add_replica(size, token_fraction)

    for (host_id, _), shard in shards.items():
        nodes[host_id].merge(shard)
    for (host_id, _), shard in shards.items():
        shard.set_ovc(nodes[host_id])

    # An empty node stays on the report: it is itself the imbalance, and dropping it would
    # lift the peer averages its rack's OVCs compare against.
    shown_nodes = sorted((node for node in nodes.values()
                          if node.tablet_count or accepts_host(node.host)),
                         key=lambda node: (node.rack_id, node.host.id))
    for node in shown_nodes:
        racks[node.rack_id].merge(node)
    for node in shown_nodes:
        node.set_ovc(racks[node.rack_id])

    shown_racks = [racks[rack_id] for rack_id in sorted({node.rack_id for node in shown_nodes})]
    for rack in shown_racks:
        dcs[rack.dc].merge(rack)
        dcs[rack.dc].capacity += rack.capacity
    for rack in shown_racks:
        rack.set_ovc(dcs[rack.dc])
        dcs[rack.dc].take_worst_ovc(rack)

    shown_dcs = [dcs[dc] for dc in sorted({rack.dc for rack in shown_racks})]
    shown_shards = [shards[(node.host.id, shard_id)]
                    for node in shown_nodes
                    for shard_id in range(node.shard_count)
                    if (node.host.id, shard_id) in shards]

    return Levels(shards=shown_shards, nodes=shown_nodes, racks=shown_racks, dcs=shown_dcs,
                  total_token_space=float(len(table_ids)))


def group_by(loads: list[LevelLoad], key: Callable[[LevelLoad], Any]) -> dict[Any, list[LevelLoad]]:
    groups: dict[Any, list[LevelLoad]] = defaultdict(list)
    for load in loads:
        groups[key(load)].append(load)
    return groups


def format_location(load: LevelLoad, options: PresentationOptions) -> str:
    if load.level == "shard":
        return format_shard_location(format_host(load.host, options), load.shard_id)
    if load.level == "node":
        return format_host(load.host, options)
    if load.level == "rack":
        return format_rack_id(load.rack_id)
    # The rendered table reads the DC off the rack rows above it; a flat CSV cannot.
    return load.dc if options.csv else "DC total"


def build_location_cells(load: LevelLoad, options: PresentationOptions) -> list:
    """
    What the row is of, and what is worth knowing about the host it is on. See
    format_host_status().
    """
    location = format_location(load, options)
    status = format_host_status(load.host)
    if options.csv:
        return [location, status or None]
    return [red(f"{location} ({status})", options) if status else location]


def format_tablets_cell(load: LevelLoad, options: PresentationOptions):
    if load.level == "shard":
        # A shard's own count is its tablets per shard, so it takes the same coloring.
        return format_tablets_per_shard(load.tablet_count, precision=0, options=options)
    if load.level == "node" or load.shard_count:
        return format_tablets_per_shard(load.tablets_per_shard, options=options)
    return None


def build_row(load: LevelLoad, levels: Levels, scales: SectionScales,
              options: PresentationOptions) -> list:
    """
    One row, whichever level it is of.

    A DC summary gets no bars: it is the total the other rows are shares of, so a bar
    scaled against it would fill every time.
    """
    show_bars = load.level != "dc"
    token_frac = share(load.token_fraction, levels.total_token_space)
    size_frac = share(load.size, levels.total_size)
    return [
        *build_location_cells(load, options),
        format_tablets_cell(load, options),
        None if load.level == "shard" else load.shard_count,
        format_pct(token_frac, options=options),
        format_ovc_pct(load.token_ovc, options),
        render_hbar(load.token_fraction, scales.max_token_fraction, width=8) if show_bars else None,
        format_size(load.size, options),
        format_pct(size_frac, options=options),
        format_ovc_pct(load.ovc, options),
        render_hbar(load.size, scales.max_size, width=8) if show_bars else None,
        format_size(load.capacity, options),
        format_util_pct(load.util, options),
        format_ovc_pct(load.util_ovc, options),
        render_hbar(load.util, scales.max_util, width=8) if show_bars else None,
    ]


def build_section_rows(levels: Levels, loads: list[LevelLoad], options: PresentationOptions,
                       subgroup_key: Callable[[LevelLoad], Any] | None = None) -> list:
    """
    Emits a rack header row and a separator before each rack, one row per load, and a
    separator between subgroups within a rack when ``subgroup_key`` is given (used to
    separate hosts in the shard-level section).

    CSV drops that grouping: the rack moves into a column on every row, so the rows
    stay one flat table.
    """
    scales = get_section_scales(loads)
    if options.csv:
        return [build_location_columns(load.level, load.rack_id)
                + build_row(load, levels, scales, options)
                for load in loads]

    rows: list = []
    current_rack = None
    current_subgroup = None
    for load in loads:
        if load.rack_id != current_rack:
            if current_rack is not None:
                rows.append(SEPARATING_LINE)
            current_rack = load.rack_id
            rows.append(build_rack_header_row(load.rack_id, COLUMN_COUNT))
            rows.append(SEPARATING_LINE)
            current_subgroup = None
        if subgroup_key is not None:
            subgroup = subgroup_key(load)
            if current_subgroup is not None and subgroup != current_subgroup:
                rows.append(SEPARATING_LINE)
            current_subgroup = subgroup
        rows.append(build_row(load, levels, scales, options))
    return rows


def build_rack_header_row(rack_id: RackId, column_count: int) -> list:
    return [format_rack_id(rack_id)] + [None] * (column_count - 1)


def build_rack_rows(levels: Levels, options: PresentationOptions, show_summary: bool = True) -> list:
    """
    The rack section: a row per rack, grouped by DC, each group closed by its summary.
    """
    scales = get_section_scales(levels.racks)
    racks_by_dc = group_by(levels.racks, lambda rack: rack.dc)

    def row_for(load: LevelLoad) -> list:
        row = build_row(load, levels, scales, options)
        return build_location_columns(load.level, load.rack_id) + row if options.csv else row

    rows = []
    for idx, dc in enumerate(levels.dcs):
        if idx and not options.csv:
            rows.append(SEPARATING_LINE)
        for rack in racks_by_dc.get(dc.dc, []):
            rows.append(row_for(rack))
        if show_summary:
            if not options.csv:
                rows.append(SEPARATING_LINE)
            rows.append(row_for(dc))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Print cluster load by rack, node, and shard.\n"
                    "\n"
                    "By default shows only per-rack and per-node sections. Use --level to show per-shard information.\n",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Columns:\n"
            "                 Location label. Depending on section, this is a rack id, node, or shard (node:shard).\n"
            "                 A node which is down is marked '(D)', excluded as '(DX).\n"
            "  status         Shown only in CSV output. Contains 'D' for down nodes, 'DX' for excluded nodes.\n"
            "  tablets/shard  Number of tablet replicas per shard for the row.\n"
            "  shard count    Number of shards aggregated by the row. Blank where not applicable.\n"
            "  tokens [%]     Token-space share owned by the row, counting every replica copy. Because replication is counted, totals across peers can exceed 100.\n"
            "  token ovc [%]  Percentage deviation from the average peer token share at the current aggregation level:\n"
            "                 rack rows compare racks within a DC, node rows compare nodes within a rack,\n"
            "                 and shard rows compare shards within a node.\n"
            "  tokens         Fixed-width bar for token share, scaled within the current section.\n"
            "  size [B]       Total replicated tablet size owned by the row.\n"
            "  size [%]       Percentage of the shown replicated data owned by the row.\n"
            "  size ovc [%]   Percentage deviation from the average peer size at the current aggregation level:\n"
            "                 rack rows compare racks within a DC, node rows compare nodes within a rack,\n"
            "                 and shard rows compare shards within a node.\n"
            "  size           Fixed-width bar for size, scaled within the current section.\n"
            "  capacity [B]   Capacity of the kind selected by --capacity. With --capacity effective (the default) the header\n"
            "                 reads 'eff capacity', and the utilization columns below read 'eff util'.\n"
            "  util [%]       Utilization relative to the selected capacity.\n"
            "  util ovc [%]   Percentage deviation from the average peer utilization at the current aggregation level:\n"
            "                 rack rows compare racks within a DC, node rows compare nodes within a rack,\n"
            "                 and shard rows compare shards within a node.\n"
            "  util           Fixed-width bar for utilization, scaled within the current section.\n"
            "\n"
            "Filter semantics:\n"
            "  When --host/--shard/--rack/--dc is used, counts, sizes, token shares, and OVC comparisons are computed only from matching replicas and shown peers.\n"
            "  size [%] stays relative to the shown replicated total, while tokens [%] stays a token-space share for the shown rows, counting every replica copy.\n"
            "  Capacity is a property of the hardware, not of the selection, so no filter narrows it. A node row shows the node's whole\n"
            "  capacity and shard count even under --shard. A rack row shows the capacity of every node in the rack, including nodes\n"
            "  that --host left out of the report. util [%] therefore reads as the share of that full capacity which the selected\n"
            "  replicas occupy.\n"
            "\n"
            "Sections:\n"
            "  1. Rack-level rows, with DC summaries.\n"
            "  2. Node-level rows.\n"
            "  3. Shard-level rows.\n"
            "\n"
            "CSV output:\n"
            "  --csv emits all sections as one table with a single header, since separate sections and the\n"
            "  rack header rows that group nodes within them do not survive a CSV reader. Two columns lead:\n"
            "    level     Section the row came from: dc, rack, node, or shard.\n"
            "    rack      Rack the row belongs to, repeated on every row. Empty on dc rows.\n"
            "  A 'status' column follows the location, holding D, X or DX for a node which is down, excluded, or\n"
            "  both, and empty otherwise.\n"
            "  The label column is named 'location', and a DC summary is labelled with the DC name rather\n"
            "  than 'DC total'.\n"
        ),
    )
    source_group = parser.add_argument_group("Source options")
    add_topology_source_args(source_group)

    filtering_group = parser.add_argument_group("Filtering options")
    add_table_filter_options(filtering_group)
    add_cluster_filter_options(filtering_group)

    report_group = parser.add_argument_group("Report options")
    report_group.add_argument("--level", nargs="+", choices=["rack", "node", "shard"], default=["rack", "node"],
                              help="Aggregation levels to display (default: rack node)")
    report_group.add_argument("--no-summary", action="store_true",
                              help="Skip the per-DC total rows")
    report_group.add_argument("--capacity", choices=list(CAPACITY_MODES), default="effective",
                              help="Display effective or absolute storage capacity/utilization (default: effective)")

    presentation_group = parser.add_argument_group("Presentation options")
    add_presentation_options(presentation_group, has_hosts=True)

    args = parser.parse_args()
    capacity_mode = CAPACITY_MODES[args.capacity]
    options = get_presentation_options_from_args(args)

    with get_topology_source_from_args(args) as src:
        topo = src.get_topology()
        table_id = resolve_table_filter_id(args, topo)
        accepts_tablet = get_tablet_filter(args, topo)
        accepts_host = get_host_filter(args, topo)

        table_ids = get_selected_table_ids(topo, table_id, get_table_filter(args, topo))
        levels = collect_levels(topo, table_ids, capacity_mode, accepts_tablet, accepts_host)
        topo.report_missing_tablet_sizes(lambda table: table in table_ids, accepts_host)

        requested_levels = set(args.level)
        sections = []

        if "rack" in requested_levels:
            sections.append(build_rack_rows(levels, options, show_summary=not args.no_summary))

        if "node" in requested_levels:
            sections.append(build_section_rows(levels, levels.nodes, options))

        if "shard" in requested_levels:
            sections.append(build_section_rows(levels, levels.shards, options,
                                               subgroup_key=lambda shard: shard.host.id))

        columns = get_columns(capacity_mode, options)
        if options.csv:
            # One header, one table: the level and rack columns keep the sections apart.
            print_table([row for rows in sections for row in rows], columns, options)
            return 0

        for idx, rows in enumerate(sections):
            if idx:
                print()
            if not print_table(rows, columns, options):
                return 0
        return 0


if __name__ == "__main__":
    sys.exit(main())
