#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from dataclasses import replace

import pytest

from tablets import cluster_load
from tablets.cluster_load import CapacityMode
from tablets.cluster_load import Levels
from tablets.filters import filter_args
from tablets.filters import get_host_filter
from tablets.filters import get_table_filter
from tablets.filters import get_tablet_filter
from tablets.render_utils import PresentationOptions
from tablets.render_utils import strip_ansi
from tablets.topology import TabletMap
from tablets.topology import Topology

from test.tablet_scripts.topology_building import *


# The hosts the tests below place, whose capacities every utilization here is measured
# against: the first two hold the same effective capacity but differ in absolute capacity,
# and the third sits in a rack of its own, for the tests that want a rack holding nothing.
HOST1 = HostSpec("10.0.0.1", "dc1", "rack1", storage_capacity=600, effective_capacity=400)
HOST2 = HostSpec("10.0.0.2", "dc1", "rack1", storage_capacity=800, effective_capacity=400)
HOST3 = HostSpec("10.0.0.3", "dc1", "rack2", storage_capacity=800, effective_capacity=400)

HOSTS = {HOST1_ID: HOST1, HOST2_ID: HOST2}
TABLES = {TABLE1_ID: ("ks", "tbl")}

# One tablet on a shard of each host, so a host, a shard and the table all hold something
# different, and 120 against 280 is an imbalance to measure.
TABLETS = {TABLE1_ID: [
    (0, {(HOST1_ID, 0): 120}),
    (1 << 63, {(HOST2_ID, 1): 280}),
]}


def collect_levels(topo: Topology, table_id=None, capacity_mode: CapacityMode = CapacityMode.EFFECTIVE,
                   **filters) -> Levels:
    """
    Measures every level under the filters the caller narrows by, the way main() does.
    """
    args = filter_args(**filters)
    table_ids = cluster_load.get_selected_table_ids(topo, table_id, get_table_filter(args, topo))
    return cluster_load.collect_levels(topo, table_ids, capacity_mode,
                                       get_tablet_filter(args, topo),
                                       get_host_filter(args, topo))


def test_a_colocated_table_counts_towards_the_tablet_count() -> None:
    """
    A view shares its base table's tablets, but its tablet is load the shard really
    serves, so it counts. Keeps tablets/shard comparable with table_summary.py, which
    reports one row per table.
    """
    topology = build_topology(hosts=HOSTS, tables=TABLES, tablets=TABLETS)

    view_id = TABLE2_ID
    topology._tables[view_id] = ("ks", "tbl_view")
    topology._tablet_maps[view_id] = TabletMap(view_id, base_table=TABLE1_ID)
    topology._tablet_sizes.update({(view_id, token): {host: 1}
                               for (_, token), sizes in list(topology._tablet_sizes.items())
                               for host in sizes})
    topology._build({})  # links the colocated map to the base table's tablets

    levels = collect_levels(topology)

    # One tablet each from the base table and the view, on each of the two hosts.
    assert [node.tablet_count for node in levels.nodes] == [2, 2]


def test_a_host_holding_no_replica_is_still_reported() -> None:
    """
    An empty node stays on the report: it is itself the imbalance being looked for.
    """
    topology = build_topology(hosts={**HOSTS, HOST3_ID: HOST3}, tables=TABLES, tablets=TABLETS)

    levels = collect_levels(topology)

    shown = {node.host.id: node for node in levels.nodes}
    assert HOST3_ID in shown
    assert shown[HOST3_ID].tablet_count == 0
    assert shown[HOST3_ID].size == 0


def test_a_table_filter_does_not_hide_a_host_without_that_tables_replicas() -> None:
    topology = build_topology(hosts={**HOSTS, HOST3_ID: HOST3}, tables=TABLES, tablets=TABLETS)

    levels = collect_levels(topology, TABLE1_ID, table="ks.tbl")

    assert HOST3_ID in {node.host.id for node in levels.nodes}


def test_a_location_filter_does_hide_a_host_it_excludes() -> None:
    topology = build_topology(hosts={**HOSTS, HOST3_ID: HOST3}, tables=TABLES, tablets=TABLETS)

    for field, value in (("rack", ("dc1", "rack1")), ("host", "10.0.0.1")):
        levels = collect_levels(topology, **{field: value})
        assert HOST3_ID not in {node.host.id for node in levels.nodes}, field


def test_rack_capacity_does_not_follow_the_filters() -> None:
    """
    Capacity belongs to the rack, so a filter narrowing what is shown leaves it alone.
    Utilization then reads as the share of the rack the selection occupies.
    """
    topology = build_topology(hosts=HOSTS, tables=TABLES, tablets=TABLETS)

    def rack1_capacities(**filters) -> list[int]:
        capacities = []
        for mode in (CapacityMode.EFFECTIVE, CapacityMode.ABSOLUTE):
            levels = collect_levels(topology, capacity_mode=mode, **filters)
            rack1 = next(rack for rack in levels.racks if rack.rack_id == ("dc1", "rack1"))
            capacities.append(rack1.capacity)
        return capacities

    unfiltered = rack1_capacities()
    assert unfiltered == [800, 1400]

    for field, value in (("host", "10.0.0.1"), ("shard", ("10.0.0.1", 0)), ("table", "ks.tbl")):
        assert rack1_capacities(**{field: value}) == unfiltered, field


def test_a_node_shows_all_of_its_shards_under_a_shard_filter() -> None:
    """
    Shard count and capacity are the node's own, so --shard narrows which replicas count
    towards its load without shrinking the node it is measured against.
    """
    topology = build_topology(hosts=HOSTS, tables=TABLES, tablets=TABLETS)

    levels = collect_levels(topology, TABLE1_ID, shard=("10.0.0.1", 0))

    node = levels.nodes[0]
    assert (node.host.ip, node.shard_count, node.capacity) == ("10.0.0.1", 2, 400)
    # Only the selected shard is a row of its own.
    assert [(shard.host.ip, shard.shard_id) for shard in levels.shards] == [("10.0.0.1", 0)]


@pytest.mark.parametrize("shard_count", [1, 0], ids=["shard_out_of_range", "no_shard_count"])
def test_a_replica_on_an_unreported_shard_still_counts_for_its_node(shard_count: int) -> None:
    """
    Levels above the shard are the merge of the shards below, so a replica sitting on a
    shard its host does not report having would go missing if it were left to that merge.
    A snapshot that carries no shard count at all is the same case for every replica.
    """
    topology = build_topology(hosts={HOST1_ID: HOST1, HOST2_ID: replace(HOST2, shard_count=shard_count)},
                              tables=TABLES, tablets=TABLETS)

    levels = collect_levels(topology, TABLE1_ID)

    node = next(node for node in levels.nodes if node.host.id == HOST2_ID)
    assert (node.size, node.tablet_count) == (280, 1)
    assert next(rack for rack in levels.racks if rack.rack_id == ("dc1", "rack1")).size == 400
    assert levels.dcs[0].size == 400
    # The replica has no shard of its own to be a row of.
    assert [(shard.host.ip, shard.shard_id) for shard in levels.shards
            if shard.host.id == HOST2_ID] == [("10.0.0.2", 0)] * shard_count


def test_every_level_measures_the_same_replicas() -> None:
    """
    One walk feeds every level, so a shard, its node, its rack and its DC agree on what
    they hold without being summed from one another.
    """
    topology = build_topology(hosts=HOSTS, tables=TABLES, tablets=TABLETS)

    levels = collect_levels(topology, TABLE1_ID)

    assert levels.total_token_space == 1.0
    assert [node.host.ip for node in levels.nodes] == ["10.0.0.1", "10.0.0.2"]
    assert [node.size for node in levels.nodes] == [120, 280]
    assert [node.tablet_count for node in levels.nodes] == [1, 1]
    assert [node.token_fraction for node in levels.nodes] == [0.5, 0.5]
    assert [(shard.host.ip, shard.shard_id, shard.size, shard.tablet_count) for shard in levels.shards] == [
        ("10.0.0.1", 0, 120, 1),
        ("10.0.0.1", 1, 0, 0),
        ("10.0.0.2", 0, 0, 0),
        ("10.0.0.2", 1, 280, 1),
    ]

    assert len(levels.racks) == 1
    rack = levels.racks[0]
    assert (rack.rack_id, rack.size, rack.tablet_count, rack.shard_count) == (("dc1", "rack1"), 400, 2, 4)
    assert rack.token_fraction == 1.0
    assert rack.tablets_per_shard == pytest.approx(0.5)

    assert len(levels.dcs) == 1
    dc = levels.dcs[0]
    assert (dc.dc, dc.size, dc.tablet_count, dc.shard_count) == ("dc1", 400, 2, 4)
    assert dc.token_fraction == 1.0


@pytest.mark.parametrize("capacity_mode, node_capacities, node_utils, node_util_ovcs", [
    pytest.param(CapacityMode.EFFECTIVE, [400, 400], [0.3, 0.7], [0.6, 1.4], id="effective"),
    pytest.param(CapacityMode.ABSOLUTE, [600, 800], [0.2, 0.35], [0.7272727273, 1.2727272727], id="absolute"),
])
def test_capacity_and_utilization_follow_the_capacity_mode(
        capacity_mode: CapacityMode, node_capacities: list[int],
        node_utils: list[float], node_util_ovcs: list[float]) -> None:
    topology = build_topology(hosts=HOSTS, tables=TABLES, tablets=TABLETS)

    levels = collect_levels(topology, TABLE1_ID, capacity_mode=capacity_mode)

    assert [node.capacity for node in levels.nodes] == node_capacities
    assert [node.util for node in levels.nodes] == pytest.approx(node_utils)
    assert [node.util_ovc for node in levels.nodes] == pytest.approx(node_util_ovcs)
    # A shard has the node's capacity divided among its shards.
    assert [shard.capacity for shard in levels.shards] == [capacity // 2
                                                           for capacity in node_capacities
                                                           for _ in range(2)]


def test_overcommit_compares_an_item_against_the_peers_it_is_shown_beside() -> None:
    """
    Nodes are compared within their rack and shards within their node, so a shard holding
    everything its node has overcommits its idle sibling however small the node is.
    """
    topology = build_topology(hosts=HOSTS, tables=TABLES, tablets=TABLETS)

    levels = collect_levels(topology, TABLE1_ID)

    assert [node.ovc for node in levels.nodes] == pytest.approx([0.6, 1.4])
    assert [node.token_ovc for node in levels.nodes] == pytest.approx([1.0, 1.0])
    assert [shard.ovc for shard in levels.shards] == pytest.approx([2.0, 0.0, 0.0, 2.0])
    assert [shard.token_ovc for shard in levels.shards] == pytest.approx([2.0, 0.0, 0.0, 2.0])


def build_two_dc_topology() -> Topology:
    """
    Two DCs of one rack each, holding one tablet replica per host. dc2's host is twice the
    size of dc1's and has twice the capacity.
    """
    return build_topology(
        hosts={
            HOST1_ID: HostSpec("10.0.0.1", "dc1", "rack1", shard_count=1,
                               storage_capacity=1000, effective_capacity=1000),
            HOST2_ID: HostSpec("10.0.0.2", "dc2", "rack2", shard_count=1,
                               storage_capacity=2000, effective_capacity=2000),
        },
        tables={TABLE1_ID: ("ks", "tbl")},
        tablets={TABLE1_ID: [(0, {(HOST1_ID, 0): 100, (HOST2_ID, 0): 200})]},
    )


def test_a_dc_summarizes_only_the_racks_it_shows() -> None:
    """
    A DC row is the total of its rack rows, so a rack a filter kept off the report takes
    its capacity with it. Otherwise the summary would be a share of hardware not shown.
    """
    topo = build_two_dc_topology()

    levels = collect_levels(topo, TABLE1_ID)
    assert [(dc.dc, dc.size, dc.capacity) for dc in levels.dcs] == [("dc1", 100, 1000), ("dc2", 200, 2000)]

    levels = collect_levels(topo, TABLE1_ID, dc="dc2")
    assert [(dc.dc, dc.size, dc.capacity) for dc in levels.dcs] == [("dc2", 200, 2000)]


def test_a_dc_row_reports_the_worst_of_its_racks() -> None:
    """
    Overcommit does not add up, and a DC has no peer to be compared against on the report,
    so its summary carries the worst of the racks it shows.
    """
    topo = build_two_dc_topology()

    levels = collect_levels(topo, TABLE1_ID)

    # One rack per DC, so each rack is its DC's average and overcommits it by nothing.
    assert [rack.ovc for rack in levels.racks] == pytest.approx([1.0, 1.0])
    assert [dc.ovc for dc in levels.dcs] == pytest.approx([1.0, 1.0])


def test_coloring_only_adds_escapes() -> None:
    """
    Stripping the escapes from a colored report gives the uncolored one, so coloring can
    never move a cell. Also catches a helper that was never handed the options.
    """
    topology = build_topology(hosts=HOSTS, tables=TABLES, tablets=TABLETS)

    levels = collect_levels(topology, TABLE1_ID)

    def build(colors: bool) -> list[list[str]]:
        rows = cluster_load.build_section_rows(levels, levels.nodes,
                                               PresentationOptions(colors=colors))
        return [[str(cell) for cell in row] for row in rows]

    colored, plain = build(True), build(False)
    assert colored != plain
    assert [[strip_ansi(cell) for cell in row] for row in colored] == plain


def test_cluster_load_headers_follow_capacity_mode() -> None:
    abs_headers = [col.header for col in cluster_load.get_columns(CapacityMode.ABSOLUTE)]
    eff_headers = [col.header for col in cluster_load.get_columns(CapacityMode.EFFECTIVE)]
    assert abs_headers[10:12] == ["capacity\n[B]", "util\n[%]"]
    assert eff_headers[10:12] == ["eff capacity\n[B]", "eff util\n[%]"]


@pytest.mark.parametrize("host,expected", [
    (replace(HOST1, up=False, excluded=True), "10.0.0.1 (DX)"),
    (replace(HOST1, up=False), "10.0.0.1 (D)"),
    (replace(HOST1, excluded=True), "10.0.0.1 (X)"),
    (replace(HOST1, up=True), "10.0.0.1"),
    (HOST1, "10.0.0.1"),
])
def test_a_node_says_when_it_is_down_or_out_of_balancing(host, expected) -> None:
    """
    A row of a node the cluster cannot reach, or one it is not balancing onto, reads as one:
    its zeros are what the node is not reporting rather than load it does not hold. A
    snapshot taken before the columns existed says nothing either way.
    """
    topology = build_topology(hosts={**HOSTS, HOST1_ID: host}, tables=TABLES, tablets=TABLETS)

    levels = collect_levels(topology)
    row = cluster_load.build_row(levels.nodes[0], levels, cluster_load.get_section_scales(levels.nodes),
                                 PresentationOptions())

    assert row[0] == expected


def test_a_marked_node_is_colored_so_it_is_found_without_reading_the_mark() -> None:
    topology = build_topology(hosts={**HOSTS, HOST1_ID: replace(HOST1, up=False)},
                             tables=TABLES, tablets=TABLETS)

    levels = collect_levels(topology)
    scales = cluster_load.get_section_scales(levels.nodes)
    colored = cluster_load.build_row(levels.nodes[0], levels, scales, PresentationOptions(colors=True))
    working = cluster_load.build_row(levels.nodes[1], levels, scales, PresentationOptions(colors=True))

    assert strip_ansi(colored[0]) == "10.0.0.1 (D)" and colored[0] != strip_ansi(colored[0])
    assert working[0] == "10.0.0.2"


def test_csv_names_the_status_in_a_column_of_its_own() -> None:
    """
    A flat table is read by column, so a note appended to the location would have to be
    parsed back out of it.
    """
    topology = build_topology(hosts={**HOSTS, HOST1_ID: replace(HOST1, up=False, excluded=True)},
                             tables=TABLES, tablets=TABLETS)
    options = PresentationOptions(csv=True)

    levels = collect_levels(topology)
    columns = [column.header for column in cluster_load.get_columns(CapacityMode.EFFECTIVE, options)]
    rows = cluster_load.build_section_rows(levels, levels.nodes, options)

    assert columns[:4] == ["level", "rack", "location", "status"]
    assert [row[2:4] for row in rows] == [["10.0.0.1", "DX"], ["10.0.0.2", None]]


def test_csv_rows_name_their_level_and_rack() -> None:
    """
    Every CSV row names its section and rack, so the sections concatenate into one table.
    """
    topology = build_topology(hosts=HOSTS, tables=TABLES, tablets=TABLETS)

    options = PresentationOptions(csv=True)
    headers = [col.header for col in cluster_load.get_columns(CapacityMode.EFFECTIVE, options)]
    assert headers[:3] == ["level", "rack", "location"]
    levels = collect_levels(topology, TABLE1_ID)

    rows = cluster_load.build_section_rows(levels, levels.nodes, options)

    # No header or separator rows survive: every row is data, and carries its rack.
    assert all(isinstance(row, list) for row in rows)
    assert len(rows) == len(levels.nodes)
    assert {row[0] for row in rows} == {"node"}
    assert {row[1] for row in rows} == {"dc1/rack1"}
    assert [row[2] for row in rows] == ["10.0.0.1", "10.0.0.2"]
    assert all(len(row) == len(headers) for row in rows)


def test_csv_dc_row_is_named_after_its_dc() -> None:
    """
    A flat CSV row names its DC; only the rendered table can rely on the rows above it.
    """
    topology = build_topology(hosts=HOSTS, tables=TABLES, tablets=TABLETS)

    levels = collect_levels(topology, TABLE1_ID)

    def summary_row(options):
        rows = cluster_load.build_rack_rows(levels, options)
        return [row for row in rows if isinstance(row, list)][-1]

    csv_row = summary_row(PresentationOptions(csv=True))
    assert csv_row[0] == "dc"
    assert csv_row[1] is None     # a DC summary belongs to no single rack
    assert csv_row[2] == "dc1"
    assert summary_row(PresentationOptions(csv=False))[0] == "DC total"


def test_missing_effective_capacity_is_not_replaced_by_absolute() -> None:
    """
    Snapshots taken before system.load_per_node had effective_capacity carry only the
    absolute capacity. Reporting that as effective capacity would label absolute numbers
    with an "eff" header, so it must be reported as unknown instead.
    """
    topology = build_topology(
        hosts={host_id: replace(host, effective_capacity=None) for host_id, host in HOSTS.items()},
        tables=TABLES, tablets=TABLETS)

    absolute = collect_levels(topology, TABLE1_ID, capacity_mode=CapacityMode.ABSOLUTE)
    effective = collect_levels(topology, TABLE1_ID, capacity_mode=CapacityMode.EFFECTIVE)

    assert [node.capacity for node in absolute.nodes] == [600, 800]
    assert [node.capacity for node in effective.nodes] == [0, 0]
    assert [node.util for node in effective.nodes] == [0, 0]
    assert [shard.capacity for shard in absolute.shards] == [300, 300, 400, 400]
    assert [shard.capacity for shard in effective.shards] == [0, 0, 0, 0]

    with pytest.raises(Exception, match=r"Effective capacity of host .* is missing"):
        topology.get_host_effective_capacity(effective.nodes[0].host)


def test_collect_levels_respects_host_and_shard_filters() -> None:
    topology = build_topology(hosts=HOSTS, tables=TABLES, tablets=TABLETS)

    levels = collect_levels(topology, TABLE1_ID, host="10.0.0.1")
    assert levels.total_token_space == 1.0
    assert [node.host.ip for node in levels.nodes] == ["10.0.0.1"]
    assert [node.size for node in levels.nodes] == [120]
    assert [node.token_fraction for node in levels.nodes] == [0.5]

    levels = collect_levels(topology, TABLE1_ID, shard=("10.0.0.2", 1))
    assert [node.host.ip for node in levels.nodes] == ["10.0.0.2"]
    assert [node.size for node in levels.nodes] == [280]
    assert [(shard.host.ip, shard.shard_id, shard.size) for shard in levels.shards] == [("10.0.0.2", 1, 280)]


def build_two_keyspace_topology() -> Topology:
    """
    One rack of two hosts, and a table per keyspace: ks1.t1 lives entirely on host1, ks2.t2
    entirely on host2, and each spans the whole ring.
    """
    return build_topology(
        hosts=HOSTS,
        tables={TABLE1_ID: ("ks1", "t1"), TABLE2_ID: ("ks2", "t2")},
        tablets={
            TABLE1_ID: [
                (0, {(HOST1_ID, 0): 100}),
                (1 << 63, {(HOST1_ID, 1): 100}),
            ],
            TABLE2_ID: [
                (0, {(HOST2_ID, 0): 200}),
                (1 << 63, {(HOST2_ID, 1): 200}),
            ],
        },
    )


def test_collect_levels_applies_keyspace_filter() -> None:
    topo = build_two_keyspace_topology()

    levels = collect_levels(topo, keyspace="ks1")

    # Numerator and denominator must both be restricted to ks1: only host1 carries
    # ks1 load, its token fraction sums to the full ring, and the token space counts
    # a single table (not both keyspaces). host2 stays on the report at zero, since a
    # keyspace filter selects tables, not hosts.
    assert levels.total_token_space == 1.0
    assert [node.host.ip for node in levels.nodes] == ["10.0.0.1", "10.0.0.2"]
    assert [node.size for node in levels.nodes] == [200, 0]
    assert [node.token_fraction for node in levels.nodes] == [1.0, 0.0]
