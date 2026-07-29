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
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tablets.filters import add_cluster_filter_options
from tablets.filters import add_table_filter_options
from tablets.filters import filter_table_id
from tablets.filters import filter_tablet
from tablets.filters import resolve_table_filter_id
from tablets.topology import Host
from tablets.topology import RackId
from tablets.topology import TableId
from tablets.render_utils import Column
from tablets.render_utils import DEFAULT_PRESENTATION
from tablets.render_utils import PresentationOptions
from tablets.render_utils import SEPARATING_LINE
from tablets.render_utils import add_presentation_options
from tablets.render_utils import format_ovc_pct
from tablets.render_utils import format_rack_id
from tablets.render_utils import format_shard_location
from tablets.render_utils import format_size
from tablets.render_utils import format_tablets_per_shard
from tablets.render_utils import format_util_pct
from tablets.render_utils import format_host
from tablets.render_utils import get_presentation_options_from_args
from tablets.render_utils import print_table
from tablets.render_utils import render_hbar
from tablets.topology import add_topology_source_args
from tablets.topology import get_topology_source_from_args

COLUMNS = [
    Column(""),
    Column("tablets\n/ shard", "right"),
    Column("shard\ncount", "right"),
    Column("size\n[B]", "right"),
    Column("size\n[%]", "right"),
    Column("size ovc\n[%]", "right"),
    Column("size", "left", csv=False),
    Column("tokens\n[%]", "right"),
    Column("token ovc\n[%]", "right"),
    Column("tokens", "left", csv=False),
    Column("capacity\n[B]", "right"),
    Column("util\n[%]", "right"),
    Column("util ovc\n[%]", "right"),
    Column("util", "left", csv=False),
]
COLUMN_COUNT = len(COLUMNS)

class CapacityMode(Enum):
    """
    Which capacity utilization is measured against. See --capacity.
    """
    EFFECTIVE = "effective"
    ABSOLUTE = "absolute"


def normalize_capacity_mode(value: str) -> CapacityMode:
    """
    Resolves a --capacity value, including its short spelling, to a CapacityMode.
    """
    if value in {"eff", "effective"}:
        return CapacityMode.EFFECTIVE
    if value in {"abs", "absolute"}:
        return CapacityMode.ABSOLUTE
    raise ValueError(f"Unsupported capacity mode: {value}")


def get_columns(capacity_mode: CapacityMode, options: PresentationOptions = DEFAULT_PRESENTATION) -> list[Column]:
    columns = list(COLUMNS)
    if capacity_mode == CapacityMode.EFFECTIVE:
        columns[10] = Column("eff capacity\n[B]", "right")
        columns[11] = Column("eff util\n[%]", "right")
        columns[13] = Column("eff util", "left", csv=False)
    if options.csv:
        # CSV is one flat table, so every row has to say which section it came from
        # and which rack it sits in. See build_location_columns().
        # The label column is unnamed in the rendered table, where the section makes it
        # obvious; a CSV reader needs a key for it.
        columns[0] = Column("location")
        columns = [Column("level"), Column("rack")] + columns
    return columns


def build_location_columns(level: str, rack_id: RackId | None) -> list:
    """
    Leading CSV-only columns naming the row's section and rack.

    They replace the rack header rows the rendered table groups by, so a row stays
    self-describing once the sections are concatenated.
    """
    return [level, format_rack_id(rack_id) if rack_id is not None else ""]


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

@dataclass
class HostLoad:
    host: Host
    rack_id: RackId
    size: int
    token_fraction: float
    tablet_count: int
    shard_sizes: list[int]
    shard_token_fractions: list[float]
    shard_tablet_counts: list[int]
    ovc: float | None = None
    token_ovc: float | None = None
    util_ovc: float | None = None

    @property
    def shard_count(self) -> int:
        return len(self.shard_sizes)

    @property
    def tablets_per_shard(self) -> float:
        return self.tablet_count / self.shard_count if self.shard_count else 0

    def get_capacity(self, capacity_mode: CapacityMode) -> int:
        return get_host_capacity(self.host, capacity_mode)

    def get_util(self, capacity_mode: CapacityMode) -> float:
        capacity = self.get_capacity(capacity_mode)
        return self.size / capacity if capacity else 0


@dataclass
class ShardLoad:
    host: Host
    rack_id: RackId
    shard_id: int
    size: int
    token_fraction: float
    tablet_count: int
    storage_capacity: int
    effective_capacity: int
    ovc: float | None = None
    token_ovc: float | None = None
    util_ovc: float | None = None

    def get_capacity(self, capacity_mode: CapacityMode) -> int:
        if capacity_mode == CapacityMode.EFFECTIVE:
            return self.effective_capacity
        return self.storage_capacity

    def get_util(self, capacity_mode: CapacityMode) -> float:
        capacity = self.get_capacity(capacity_mode)
        return self.size / capacity if capacity else 0


@dataclass
class RackLoad:
    rack_id: RackId
    hosts: list[HostLoad]
    ovc: float | None = None
    token_ovc: float | None = None
    util_ovc: float | None = None

    @property
    def node_count(self) -> int:
        return len(self.hosts)

    @property
    def total_shard_count(self) -> int:
        return sum(host.shard_count for host in self.hosts)

    @property
    def total_size(self) -> int:
        return sum(host.size for host in self.hosts)

    @property
    def total_token_fraction(self) -> float:
        return sum(host.token_fraction for host in self.hosts)

    @property
    def total_tablet_count(self) -> int:
        return sum(host.tablet_count for host in self.hosts)

    def get_total_capacity(self, capacity_mode: CapacityMode) -> int:
        return sum(host.get_capacity(capacity_mode) for host in self.hosts)

    @property
    def avg_tablets_per_shard(self) -> float:
        values = [host.tablets_per_shard for host in self.hosts if host.shard_count]
        return sum(values) / len(values) if values else 0

    @property
    def total_tablets_per_shard(self) -> float:
        return self.total_tablet_count / self.total_shard_count if self.total_shard_count else 0

    def get_util(self, capacity_mode: CapacityMode) -> float:
        total_capacity = self.get_total_capacity(capacity_mode)
        return self.total_size / total_capacity if total_capacity else 0

    @property
    def max_ovc(self) -> float | None:
        ovcs = [host.ovc for host in self.hosts if host.ovc is not None]
        return max(ovcs) if ovcs else None

    @property
    def max_token_ovc(self) -> float | None:
        ovcs = [host.token_ovc for host in self.hosts if host.token_ovc is not None]
        return max(ovcs) if ovcs else None

    @property
    def max_util_ovc(self) -> float | None:
        ovcs = [host.util_ovc for host in self.hosts if host.util_ovc is not None]
        return max(ovcs) if ovcs else None


@dataclass
class DcLoad:
    dc: str
    racks: list[RackLoad]

    @property
    def total_size(self) -> int:
        return sum(rack.total_size for rack in self.racks)

    @property
    def total_token_fraction(self) -> float:
        return sum(rack.total_token_fraction for rack in self.racks)

    @property
    def total_tablet_count(self) -> int:
        return sum(rack.total_tablet_count for rack in self.racks)

    @property
    def total_shard_count(self) -> int:
        return sum(rack.total_shard_count for rack in self.racks)

    def get_total_capacity(self, capacity_mode: CapacityMode) -> int:
        return sum(rack.get_total_capacity(capacity_mode) for rack in self.racks)

    @property
    def total_tablets_per_shard(self) -> float:
        return self.total_tablet_count / self.total_shard_count if self.total_shard_count else 0

    @property
    def max_size_ovc(self) -> float | None:
        ovcs = [rack.ovc for rack in self.racks if rack.ovc is not None]
        return max(ovcs) if ovcs else None

    @property
    def max_token_ovc(self) -> float | None:
        ovcs = [rack.token_ovc for rack in self.racks if rack.token_ovc is not None]
        return max(ovcs) if ovcs else None

    @property
    def max_util_ovc(self) -> float | None:
        ovcs = [rack.util_ovc for rack in self.racks if rack.util_ovc is not None]
        return max(ovcs) if ovcs else None


@dataclass(frozen=True)
class SectionScales:
    max_size: int | float
    max_token_fraction: float
    max_util: float


def iter_tablets_with_token_fraction(topo, table_ids: list[TableId]):
    for current_table_id in table_ids:
        tablet_map = topo.get_tablet_map(current_table_id)
        prev_last_token = tablet_map.tablets[-1].last_token if tablet_map.tablets else 0
        for tablet in tablet_map.tablets:
            token_fraction = ((tablet.last_token - prev_last_token) % (1 << 64)) / float(1 << 64)
            yield current_table_id, tablet, token_fraction
            prev_last_token = tablet.last_token


def get_selected_table_ids(topo, table_id: TableId | None, args=None) -> list[TableId]:
    if table_id is not None:
        return [table_id]
    return [table_id for table_id in topo.iter_table_ids(include_colocated=False)
            if args is None or filter_table_id(args, topo, table_id)]


def collect_host_loads(topo, table_id: TableId | None, args=None) -> tuple[list[HostLoad], float]:
    selected_table_ids = get_selected_table_ids(topo, table_id, args)
    allowed_racks = topo.get_table_racks(table_id) if table_id is not None else None

    loads: dict = {}
    for host in topo.all_normal_token_owner_hosts():
        if not host.dc or not host.rack:
            continue
        rack_id = (host.dc, host.rack)
        if allowed_racks is not None and rack_id not in allowed_racks:
            continue
        loads[host.id] = HostLoad(
            host=host,
            rack_id=rack_id,
            size=0,
            token_fraction=0,
            tablet_count=0,
            shard_sizes=[0] * (host.shard_count or 0),
            shard_token_fractions=[0.0] * (host.shard_count or 0),
            shard_tablet_counts=[0] * (host.shard_count or 0),
        )

    total_token_space = float(len(selected_table_ids))
    for tablet_table_id, tablet, token_fraction in iter_tablets_with_token_fraction(topo, selected_table_ids):
        for host_id, shard_id in tablet.replicas:
            if args is not None and not filter_tablet(args, topo, (host_id, shard_id)):
                continue
            host_load = loads.get(host_id)
            if host_load is None:
                continue
            size = topo.get_tablet_size(tablet_table_id, tablet, (host_id, shard_id))
            host_load.size += size
            host_load.token_fraction += token_fraction
            host_load.tablet_count += 1
            if shard_id < len(host_load.shard_sizes):
                host_load.shard_sizes[shard_id] += size
                host_load.shard_token_fractions[shard_id] += token_fraction
                host_load.shard_tablet_counts[shard_id] += 1

    filtered_loads = [load for load in loads.values() if load.tablet_count]
    return sorted(filtered_loads, key=lambda load: (load.rack_id, load.host.id)), total_token_space


def collect_rack_loads(host_loads: list[HostLoad]) -> list[RackLoad]:
    by_rack: dict[RackId, list[HostLoad]] = defaultdict(list)
    for load in host_loads:
        by_rack[load.rack_id].append(load)

    return [RackLoad(rack_id, hosts) for rack_id, hosts in sorted(by_rack.items())]


def annotate_ovc(loads: list[Any], size_getter: Callable[[Any], float], token_getter: Callable[[Any], float],
                 util_getter: Callable[[Any], float]) -> None:
    avg_size = sum(size_getter(load) for load in loads) / len(loads) if loads else 0
    avg_token_fraction = sum(token_getter(load) for load in loads) / len(loads) if loads else 0
    avg_util = sum(util_getter(load) for load in loads) / len(loads) if loads else 0
    for load in loads:
        load.ovc = size_getter(load) / avg_size if avg_size else None
        load.token_ovc = token_getter(load) / avg_token_fraction if avg_token_fraction else None
        load.util_ovc = util_getter(load) / avg_util if avg_util else None


def get_section_scales(loads: list[Any], size_getter: Callable[[Any], int | float], token_getter: Callable[[Any], float],
                       util_getter: Callable[[Any], float]) -> SectionScales:
    return SectionScales(
        max_size=max((size_getter(load) for load in loads), default=0),
        max_token_fraction=max((token_getter(load) for load in loads), default=0),
        max_util=max((util_getter(load) for load in loads), default=0),
    )


def annotate_host_ovc(rack_loads: list[RackLoad], capacity_mode: CapacityMode) -> None:
    for rack_load in rack_loads:
        annotate_ovc(rack_load.hosts, lambda host: host.size, lambda host: host.token_fraction,
                     lambda host: host.get_util(capacity_mode))


def collect_shard_loads(host_loads: list[HostLoad], topo, args=None) -> list[ShardLoad]:
    shard_loads: list[ShardLoad] = []
    for host_load in host_loads:
        shard_count = host_load.shard_count or 0
        shard_storage_capacity = get_host_capacity(host_load.host, CapacityMode.ABSOLUTE) // shard_count if shard_count else 0
        shard_effective_capacity = get_host_capacity(host_load.host, CapacityMode.EFFECTIVE) // shard_count if shard_count else 0
        host_shards: list[ShardLoad] = []
        for shard_id in range(shard_count):
            if args is not None and not filter_tablet(args, topo, (host_load.host.id, shard_id)):
                continue
            host_shards.append(ShardLoad(
                host=host_load.host,
                rack_id=host_load.rack_id,
                shard_id=shard_id,
                size=host_load.shard_sizes[shard_id],
                token_fraction=host_load.shard_token_fractions[shard_id],
                tablet_count=host_load.shard_tablet_counts[shard_id],
                storage_capacity=shard_storage_capacity,
                effective_capacity=shard_effective_capacity,
            ))

        shard_loads.extend(host_shards)
    return shard_loads


def annotate_shard_ovc(shard_loads: list[ShardLoad], capacity_mode: CapacityMode) -> None:
    shards_by_host: dict = defaultdict(list)
    for shard in shard_loads:
        shards_by_host[shard.host.id].append(shard)

    for host_shards in shards_by_host.values():
        annotate_ovc(host_shards, lambda shard: shard.size, lambda shard: shard.token_fraction,
                     lambda shard: shard.get_util(capacity_mode))


def collect_dc_loads(rack_loads: list[RackLoad]) -> list[DcLoad]:
    by_dc: dict[str, list[RackLoad]] = defaultdict(list)
    for rack_load in rack_loads:
        by_dc[rack_load.rack_id[0]].append(rack_load)

    return [DcLoad(dc, racks) for dc, racks in sorted(by_dc.items())]


def annotate_rack_ovc(dc_loads: list[DcLoad], capacity_mode: CapacityMode) -> None:
    for dc_load in dc_loads:
        annotate_ovc(dc_load.racks, lambda rack: rack.total_size, lambda rack: rack.total_token_fraction,
                     lambda rack: rack.get_util(capacity_mode))


def build_metric_columns(size: int | float, size_frac: float, size_ovc: float | None, size_hbar_max: int | float,
                         token_fraction: float, total_token_space: float, token_ovc: float | None, token_hbar_max: int | float,
                         capacity: int | float, util: float, util_ovc: float | None, util_hbar_max: int | float,
                         options: PresentationOptions, show_hbars: bool = True) -> list:
    token_pct = token_fraction * 100 / total_token_space if total_token_space else 0
    util_pct = util * 100
    return [
        format_size(size, options),
        f"{size_frac * 100:.2f}",
        format_ovc_pct(size_ovc),
        render_hbar(size, size_hbar_max, width=8) if show_hbars else None,
        f"{token_pct:.2f}",
        format_ovc_pct(token_ovc),
        render_hbar(token_fraction, token_hbar_max, width=8) if show_hbars else None,
        format_size(capacity, options),
        format_util_pct(util_pct),
        format_ovc_pct(util_ovc),
        render_hbar(util, util_hbar_max, width=8) if show_hbars else None,
    ]


def build_host_row(load: HostLoad, total_size: int, total_token_space: float, scales: SectionScales,
                   capacity_mode: CapacityMode, options: PresentationOptions) -> list:
    size_frac = load.size / total_size if total_size else 0
    capacity = load.get_capacity(capacity_mode)
    return [
        format_host(load.host, options),
        format_tablets_per_shard(load.tablets_per_shard),
        load.shard_count,
        *build_metric_columns(load.size, size_frac, load.ovc, scales.max_size,
                              load.token_fraction, total_token_space, load.token_ovc, scales.max_token_fraction,
                              capacity, load.get_util(capacity_mode), load.util_ovc, scales.max_util, options),
    ]


def build_rack_header_row(rack_id: RackId, column_count: int) -> list:
    return [format_rack_id(rack_id)] + [None] * (column_count - 1)


def build_rack_row(rack_load: RackLoad, total_size: int, total_token_space: float, scales: SectionScales,
                   capacity_mode: CapacityMode, options: PresentationOptions) -> list:
    size_frac = rack_load.total_size / total_size if total_size else 0
    return [
        format_rack_id(rack_load.rack_id),
        format_tablets_per_shard(rack_load.total_tablets_per_shard) if rack_load.total_shard_count else "",
        rack_load.total_shard_count,
        *build_metric_columns(rack_load.total_size, size_frac, rack_load.ovc, scales.max_size,
                              rack_load.total_token_fraction, total_token_space, rack_load.token_ovc, scales.max_token_fraction,
                              rack_load.get_total_capacity(capacity_mode), rack_load.get_util(capacity_mode), rack_load.util_ovc, scales.max_util, options),
    ]


def build_dc_summary_row(dc_load: DcLoad, total_size: int, total_token_space: float,
                         capacity_mode: CapacityMode, options: PresentationOptions) -> list:
    size_frac = dc_load.total_size / total_size if total_size else 0
    total_capacity = dc_load.get_total_capacity(capacity_mode)
    return [
        # The rendered table reads the DC off the rack rows above it; a flat CSV cannot.
        dc_load.dc if options.csv else "DC total",
        format_tablets_per_shard(dc_load.total_tablets_per_shard) if dc_load.total_shard_count else "",
        dc_load.total_shard_count,
        *build_metric_columns(dc_load.total_size, size_frac, dc_load.max_size_ovc, 1,
                              dc_load.total_token_fraction, total_token_space, dc_load.max_token_ovc, 1,
                              total_capacity, dc_load.total_size / total_capacity if total_capacity else 0,
                              dc_load.max_util_ovc, 1, options, show_hbars=False),
    ]


def build_shard_row(shard: ShardLoad, total_size: int, total_token_space: float, scales: SectionScales,
                    capacity_mode: CapacityMode, options: PresentationOptions) -> list:
    size_frac = shard.size / total_size if total_size else 0
    return [
        format_shard_location(format_host(shard.host, options), shard.shard_id),
        shard.tablet_count,
        None,
        *build_metric_columns(shard.size, size_frac, shard.ovc, scales.max_size,
                              shard.token_fraction, total_token_space, shard.token_ovc, scales.max_token_fraction,
                              shard.get_capacity(capacity_mode), shard.get_util(capacity_mode), shard.util_ovc, scales.max_util, options),
    ]


def group_rows_by_rack(loads: list[Any], build_row: Callable[[Any], list], level: str,
                       options: PresentationOptions,
                       subgroup_key: Callable[[Any], Any] | None = None) -> list:
    """
    Emits a rack header row and a separator before each rack, one row per load via
    ``build_row``, and a separator between subgroups within a rack when ``subgroup_key``
    is given (used to separate hosts in the shard-level section).

    CSV drops that grouping: the rack moves into a column on every row, so the rows
    stay one flat table.
    """
    if options.csv:
        return [build_location_columns(level, load.rack_id) + build_row(load) for load in loads]

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
        rows.append(build_row(load))
    return rows


def build_node_rows(host_loads: list[HostLoad], total_size: int, total_token_space: float,
                    capacity_mode: CapacityMode, options: PresentationOptions) -> list:
    scales = get_section_scales(host_loads, lambda load: load.size, lambda load: load.token_fraction,
                                lambda load: load.get_util(capacity_mode))
    return group_rows_by_rack(
        host_loads,
        lambda load: build_host_row(load, total_size, total_token_space, scales, capacity_mode, options),
        "node", options,
    )


def build_shard_rows(shard_loads: list[ShardLoad], total_size: int, total_token_space: float,
                     capacity_mode: CapacityMode, options: PresentationOptions) -> list:
    scales = get_section_scales(shard_loads, lambda shard: shard.size, lambda shard: shard.token_fraction,
                                lambda shard: shard.get_util(capacity_mode))
    return group_rows_by_rack(
        shard_loads,
        lambda shard: build_shard_row(shard, total_size, total_token_space, scales, capacity_mode, options),
        "shard", options,
        subgroup_key=lambda shard: shard.host.id,
    )


def build_rack_rows(dc_loads: list[DcLoad], total_size: int, total_token_space: float,
                    capacity_mode: CapacityMode, options: PresentationOptions) -> list:
    rows = []
    need_separator = False
    rack_loads = [rack_load for dc_load in dc_loads for rack_load in dc_load.racks]
    scales = get_section_scales(rack_loads, lambda rack_load: rack_load.total_size,
                                lambda rack_load: rack_load.total_token_fraction, lambda rack_load: rack_load.get_util(capacity_mode))
    for dc_load in dc_loads:
        if need_separator and not options.csv:
            rows.append(SEPARATING_LINE)
        for rack_load in dc_load.racks:
            row = build_rack_row(rack_load, total_size, total_token_space, scales, capacity_mode, options)
            rows.append(build_location_columns("rack", rack_load.rack_id) + row if options.csv else row)
        if not options.csv:
            rows.append(SEPARATING_LINE)
        dc_row = build_dc_summary_row(dc_load, total_size, total_token_space, capacity_mode, options)
        rows.append(build_location_columns("dc", None) + dc_row if options.csv else dc_row)
        need_separator = True
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
            "  tablets/shard  Number of tablet replicas per shard for the row.\n"
            "  shard count    Number of shards aggregated by the row. Blank where not applicable.\n"
            "  size [B]       Total replicated tablet size owned by the row.\n"
            "  size [%]       Percentage of the shown replicated data owned by the row.\n"
            "  size ovc [%]   Percentage deviation from the average peer size at the current aggregation level:\n"
            "                 rack rows compare racks within a DC, node rows compare nodes within a rack,\n"
            "                 and shard rows compare shards within a node.\n"
            "  size           Fixed-width bar for size, scaled within the current section.\n"
            "  tokens [%]     Token-space share owned by the row, counting every replica copy. Because replication is counted, totals across peers can exceed 100.\n"
            "  token ovc [%]  Percentage deviation from the average peer token share at the current aggregation level:\n"
            "                 rack rows compare racks within a DC, node rows compare nodes within a rack,\n"
            "                 and shard rows compare shards within a node.\n"
            "  tokens         Fixed-width bar for token share, scaled within the current section.\n"
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
            "  Capacity is a property of the node, not of the selected replicas, so --shard does not narrow it: rack and node rows keep\n"
            "  the whole node's capacity and shard count. Their util [%] and tablets/shard therefore relate the whole node's capacity.\n"
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
    report_group.add_argument("--capacity", choices=["eff", "effective", "abs", "absolute"], default="effective",
                              help="Display effective or absolute storage capacity/utilization (default: effective)")

    presentation_group = parser.add_argument_group("Presentation options")
    add_presentation_options(presentation_group, has_hosts=True)

    args = parser.parse_args()
    capacity_mode = normalize_capacity_mode(args.capacity)
    options = get_presentation_options_from_args(args)

    with get_topology_source_from_args(args) as src:
        topo = src.get_topology()
        table_id = resolve_table_filter_id(args, topo)

        host_loads, total_token_space = collect_host_loads(topo, table_id, args)
        requested_levels = set(args.level)

        rack_loads = None
        if "rack" in requested_levels or "node" in requested_levels:
            rack_loads = collect_rack_loads(host_loads)
            annotate_host_ovc(rack_loads, capacity_mode)

        total_size = sum(load.size for load in host_loads)
        sections = []

        if "rack" in requested_levels:
            assert rack_loads is not None
            dc_loads = collect_dc_loads(rack_loads)
            annotate_rack_ovc(dc_loads, capacity_mode)
            rack_rows = build_rack_rows(dc_loads, total_size, total_token_space, capacity_mode, options)
            sections.append(rack_rows)

        if "node" in requested_levels:
            assert rack_loads is not None
            node_rows = build_node_rows(host_loads, total_size, total_token_space, capacity_mode, options)
            sections.append(node_rows)

        if "shard" in requested_levels:
            shard_loads = collect_shard_loads(host_loads, topo, args)
            annotate_shard_ovc(shard_loads, capacity_mode)
            shard_rows = build_shard_rows(shard_loads, total_size, total_token_space, capacity_mode, options)
            sections.append(shard_rows)

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
