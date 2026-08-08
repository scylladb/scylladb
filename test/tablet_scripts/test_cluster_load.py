#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from argparse import Namespace
from uuid import UUID

import pytest

from tablets.render_utils import PresentationOptions
from tablets.topology import Host
from tablets.topology import Tablet
from tablets.topology import TabletMap
from tablets.topology import Topology

from conftest import load_tablet_script


cluster_load = load_tablet_script("cluster-load")


def build_topology() -> tuple[Topology, UUID]:
    table_id = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    host1_id = UUID("11111111-1111-1111-1111-111111111111")
    host2_id = UUID("22222222-2222-2222-2222-222222222222")

    topo = Topology()
    topo._tables[table_id] = ("ks", "tbl")
    topo._hosts[host1_id] = Host(
        id=host1_id,
        shard_count=2,
        storage_capacity=600,
        effective_capacity=400,
        dc="dc1",
        rack="rack1",
        ip="10.0.0.1",
        node_state="normal",
        num_tokens=1,
    )
    topo._hosts[host2_id] = Host(
        id=host2_id,
        shard_count=2,
        storage_capacity=800,
        effective_capacity=400,
        dc="dc1",
        rack="rack1",
        ip="10.0.0.2",
        node_state="normal",
        num_tokens=1,
    )
    topo._tablet_maps[table_id] = TabletMap(
        table=table_id,
        tablets=[
            Tablet(last_token=0, replicas=[(host1_id, 0)]),
            Tablet(last_token=1 << 63, replicas=[(host2_id, 1)]),
        ],
    )
    topo._tablet_sizes[(table_id, 0)] = {host1_id: 120}
    topo._tablet_sizes[(table_id, 1 << 63)] = {host2_id: 280}
    # Links colocated tablet maps, as building from rows does.
    topo._build({})
    return topo, table_id


def test_collect_cluster_loads_from_manufactured_topology() -> None:
    topo, table_id = build_topology()

    host_loads, total_token_space = cluster_load.collect_host_loads(topo, table_id)

    assert total_token_space == 1.0
    assert [load.host.ip for load in host_loads] == ["10.0.0.1", "10.0.0.2"]
    assert [load.size for load in host_loads] == [120, 280]
    assert [load.tablet_count for load in host_loads] == [1, 1]
    assert [load.token_fraction for load in host_loads] == [0.5, 0.5]
    assert [load.shard_sizes for load in host_loads] == [[120, 0], [0, 280]]

    rack_loads = cluster_load.collect_rack_loads(host_loads)
    cluster_load.annotate_host_ovc(rack_loads, cluster_load.CapacityMode.EFFECTIVE)

    assert len(rack_loads) == 1
    rack_load = rack_loads[0]
    assert rack_load.rack_id == ("dc1", "rack1")
    assert rack_load.total_size == 400
    assert rack_load.total_tablet_count == 2
    assert rack_load.total_shard_count == 4
    assert rack_load.total_token_fraction == 1.0
    assert rack_load.get_total_capacity(cluster_load.CapacityMode.EFFECTIVE) == 800
    assert rack_load.get_total_capacity(cluster_load.CapacityMode.ABSOLUTE) == 1400
    assert rack_load.total_tablets_per_shard == pytest.approx(0.5)
    assert [load.get_capacity(cluster_load.CapacityMode.EFFECTIVE) for load in rack_load.hosts] == [400, 400]
    assert [load.get_capacity(cluster_load.CapacityMode.ABSOLUTE) for load in rack_load.hosts] == [600, 800]
    assert [load.get_util(cluster_load.CapacityMode.EFFECTIVE) for load in rack_load.hosts] == pytest.approx([0.3, 0.7])
    assert [load.get_util(cluster_load.CapacityMode.ABSOLUTE) for load in rack_load.hosts] == pytest.approx([0.2, 0.35])

    assert [load.ovc for load in rack_load.hosts] == pytest.approx([0.6, 1.4])
    assert [load.token_ovc for load in rack_load.hosts] == pytest.approx([1.0, 1.0])
    assert [load.util_ovc for load in rack_load.hosts] == pytest.approx([0.6, 1.4])

    cluster_load.annotate_host_ovc(rack_loads, cluster_load.CapacityMode.ABSOLUTE)
    assert [load.util_ovc for load in rack_load.hosts] == pytest.approx([0.7272727273, 1.2727272727])

    shard_loads = cluster_load.collect_shard_loads(host_loads, topo)
    cluster_load.annotate_shard_ovc(shard_loads, cluster_load.CapacityMode.EFFECTIVE)

    assert [(load.host.ip, load.shard_id, load.size, load.tablet_count) for load in shard_loads] == [
        ("10.0.0.1", 0, 120, 1),
        ("10.0.0.1", 1, 0, 0),
        ("10.0.0.2", 0, 0, 0),
        ("10.0.0.2", 1, 280, 1),
    ]
    assert [load.effective_capacity for load in shard_loads] == [200, 200, 200, 200]
    assert [load.storage_capacity for load in shard_loads] == [300, 300, 400, 400]
    assert [load.ovc for load in shard_loads] == pytest.approx([2.0, 0.0, 0.0, 2.0])
    assert [load.token_ovc for load in shard_loads] == pytest.approx([2.0, 0.0, 0.0, 2.0])

    dc_loads = cluster_load.collect_dc_loads(rack_loads)
    cluster_load.annotate_rack_ovc(dc_loads, cluster_load.CapacityMode.EFFECTIVE)

    assert len(dc_loads) == 1
    assert dc_loads[0].dc == "dc1"
    assert dc_loads[0].total_size == 400
    assert dc_loads[0].total_tablet_count == 2
    assert dc_loads[0].total_shard_count == 4
    assert dc_loads[0].total_token_fraction == 1.0


def test_cluster_load_headers_follow_capacity_mode() -> None:
    abs_headers = [col.header for col in cluster_load.get_columns(cluster_load.CapacityMode.ABSOLUTE)]
    eff_headers = [col.header for col in cluster_load.get_columns(cluster_load.CapacityMode.EFFECTIVE)]
    assert abs_headers[10:12] == ["capacity\n[B]", "util\n[%]"]
    assert eff_headers[10:12] == ["eff capacity\n[B]", "eff util\n[%]"]


def test_csv_rows_name_their_level_and_rack() -> None:
    """
    The rendered table groups nodes under rack header rows. CSV has no grouping, so the
    rack moves into a column and every row says which section it belongs to, letting the
    sections be concatenated into one table with a single header.
    """
    options = PresentationOptions(csv=True)
    headers = [col.header for col in cluster_load.get_columns(cluster_load.CapacityMode.EFFECTIVE, options)]
    assert headers[:3] == ["level", "rack", "location"]

    topo, table_id = build_topology()
    host_loads, total_token_space = cluster_load.collect_host_loads(topo, table_id)
    rack_loads = cluster_load.collect_rack_loads(host_loads)
    cluster_load.annotate_host_ovc(rack_loads, cluster_load.CapacityMode.EFFECTIVE)
    total_size = sum(load.size for load in host_loads)

    rows = cluster_load.build_node_rows(host_loads, total_size, total_token_space,
                                        cluster_load.CapacityMode.EFFECTIVE, options)

    # No header or separator rows survive: every row is data, and carries its rack.
    assert all(isinstance(row, list) for row in rows)
    assert len(rows) == len(host_loads)
    assert {row[0] for row in rows} == {"node"}
    assert {row[1] for row in rows} == {"dc1/rack1"}
    assert [row[2] for row in rows] == ["10.0.0.1", "10.0.0.2"]
    assert all(len(row) == len(headers) for row in rows)


def test_csv_dc_row_is_named_after_its_dc() -> None:
    """
    The rendered table reads the DC off the rack rows above the summary, so it can label
    it "DC total". A flat CSV row has to name the DC itself.
    """
    topo, table_id = build_topology()
    host_loads, total_token_space = cluster_load.collect_host_loads(topo, table_id)
    rack_loads = cluster_load.collect_rack_loads(host_loads)
    cluster_load.annotate_host_ovc(rack_loads, cluster_load.CapacityMode.EFFECTIVE)
    dc_loads = cluster_load.collect_dc_loads(rack_loads)
    cluster_load.annotate_rack_ovc(dc_loads, cluster_load.CapacityMode.EFFECTIVE)
    total_size = sum(load.size for load in host_loads)

    def summary_row(options):
        rows = cluster_load.build_rack_rows(dc_loads, total_size, total_token_space,
                                            cluster_load.CapacityMode.EFFECTIVE, options)
        return [row for row in rows if isinstance(row, list)][-1]

    csv_row = summary_row(PresentationOptions(csv=True))
    assert csv_row[0] == "dc"
    assert csv_row[1] == ""       # a DC summary belongs to no single rack
    assert csv_row[2] == "dc1"
    assert summary_row(PresentationOptions(csv=False))[0] == "DC total"


def test_missing_effective_capacity_is_not_replaced_by_absolute() -> None:
    """
    Snapshots taken before system.load_per_node had effective_capacity carry only the
    absolute capacity. Reporting that as effective capacity would label absolute numbers
    with an "eff" header, so it must be reported as unknown instead.
    """
    topo, table_id = build_topology()
    for host in topo.all_hosts():
        host.effective_capacity = None

    host_loads, _ = cluster_load.collect_host_loads(topo, table_id)

    assert [load.get_capacity(cluster_load.CapacityMode.ABSOLUTE) for load in host_loads] == [600, 800]
    assert [load.get_capacity(cluster_load.CapacityMode.EFFECTIVE) for load in host_loads] == [0, 0]
    assert [load.get_util(cluster_load.CapacityMode.EFFECTIVE) for load in host_loads] == [0, 0]

    shard_loads = cluster_load.collect_shard_loads(host_loads, topo)
    assert [load.storage_capacity for load in shard_loads] == [300, 300, 400, 400]
    assert [load.effective_capacity for load in shard_loads] == [0, 0, 0, 0]

    with pytest.raises(Exception, match="Effective capacity of host .* is missing"):
        topo.get_host_effective_capacity(host_loads[0].host)


def test_collect_cluster_loads_respects_host_and_shard_filters() -> None:
    topo, table_id = build_topology()

    host_loads, total_token_space = cluster_load.collect_host_loads(
        topo,
        table_id,
        Namespace(host="10.0.0.1", shard=None, rack=None, dc=None),
    )
    assert total_token_space == 1.0
    assert [load.host.ip for load in host_loads] == ["10.0.0.1"]
    assert [load.size for load in host_loads] == [120]
    assert [load.token_fraction for load in host_loads] == [0.5]

    host_loads, _ = cluster_load.collect_host_loads(
        topo,
        table_id,
        Namespace(host=None, shard=("10.0.0.2", 1), rack=None, dc=None),
    )
    assert [load.host.ip for load in host_loads] == ["10.0.0.2"]
    assert [load.size for load in host_loads] == [280]

    shard_loads = cluster_load.collect_shard_loads(
        host_loads,
        topo,
        Namespace(host=None, shard=("10.0.0.2", 1), rack=None, dc=None),
    )
    assert [(load.host.ip, load.shard_id, load.size) for load in shard_loads] == [("10.0.0.2", 1, 280)]


def build_two_keyspace_topology() -> Topology:
    host1_id = UUID("11111111-1111-1111-1111-111111111111")
    host2_id = UUID("22222222-2222-2222-2222-222222222222")
    table1_id = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    table2_id = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

    topo = Topology()
    topo._tables[table1_id] = ("ks1", "t1")
    topo._tables[table2_id] = ("ks2", "t2")
    for host_id, ip in ((host1_id, "10.0.0.1"), (host2_id, "10.0.0.2")):
        topo._hosts[host_id] = Host(id=host_id, shard_count=2, storage_capacity=600, effective_capacity=400,
                                    dc="dc1", rack="rack1", ip=ip, node_state="normal", num_tokens=1)

    # table1 lives entirely on host1, table2 entirely on host2; each spans the whole ring.
    topo._tablet_maps[table1_id] = TabletMap(table=table1_id, tablets=[
        Tablet(last_token=0, replicas=[(host1_id, 0)]),
        Tablet(last_token=1 << 63, replicas=[(host1_id, 1)]),
    ])
    topo._tablet_maps[table2_id] = TabletMap(table=table2_id, tablets=[
        Tablet(last_token=0, replicas=[(host2_id, 0)]),
        Tablet(last_token=1 << 63, replicas=[(host2_id, 1)]),
    ])
    topo._tablet_sizes[(table1_id, 0)] = {host1_id: 100}
    topo._tablet_sizes[(table1_id, 1 << 63)] = {host1_id: 100}
    topo._tablet_sizes[(table2_id, 0)] = {host2_id: 200}
    topo._tablet_sizes[(table2_id, 1 << 63)] = {host2_id: 200}
    # Links colocated tablet maps, as building from rows does.
    topo._build({})
    return topo


def test_collect_host_loads_applies_keyspace_filter() -> None:
    topo = build_two_keyspace_topology()

    args = Namespace(table=None, keyspace="ks1", host=None, shard=None, rack=None, dc=None)
    host_loads, total_token_space = cluster_load.collect_host_loads(topo, None, args)

    # Numerator and denominator must both be restricted to ks1: only host1 carries
    # ks1 load, its token fraction sums to the full ring, and the token space counts
    # a single table (not both keyspaces).
    assert total_token_space == 1.0
    assert [load.host.ip for load in host_loads] == ["10.0.0.1"]
    assert [load.size for load in host_loads] == [200]
    assert [load.token_fraction for load in host_loads] == [1.0]
