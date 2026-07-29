#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from argparse import Namespace
from uuid import UUID

from conftest import load_tablet_script
from tablets.render_utils import PresentationOptions
from tablets.topology import Host
from tablets.topology import Tablet
from tablets.topology import TabletMap
from tablets.topology import Topology


table_summary = load_tablet_script("table-summary")


def test_calculate_total_size_and_tablet_ovc_uses_tablet_sizes_in_token_space() -> None:
    total_size, tablet_ovc = table_summary.calculate_total_size_and_tablet_ovc([10, 20, 30])

    assert total_size == 60
    assert tablet_ovc == 1.5


def test_calculate_total_size_and_tablet_ovc_handles_empty_tables() -> None:
    assert table_summary.calculate_total_size_and_tablet_ovc([]) == (0, None)


def test_collect_filtered_tablet_sizes_uses_cluster_filters() -> None:
    table_id = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    host1_id = UUID("11111111-1111-1111-1111-111111111111")
    host2_id = UUID("22222222-2222-2222-2222-222222222222")
    topo = Topology()
    topo._tables[table_id] = ("ks", "tbl")
    topo._hosts[host1_id] = Host(id=host1_id, shard_count=2, ip="10.0.0.1", dc="dc1", rack="rack1")
    topo._hosts[host2_id] = Host(id=host2_id, shard_count=2, ip="10.0.0.2", dc="dc1", rack="rack1")
    topo._tablet_maps[table_id] = TabletMap(
        table=table_id,
        tablets=[
            Tablet(last_token=0, replicas=[(host1_id, 0), (host2_id, 1)]),
            Tablet(last_token=1 << 63, replicas=[(host2_id, 1)]),
        ],
    )
    topo._tablet_sizes[(table_id, 0)] = {host1_id: 100, host2_id: 50}
    topo._tablet_sizes[(table_id, 1 << 63)] = {host2_id: 80}

    args = Namespace(host="10.0.0.1", shard=None, rack=None, dc=None)

    assert table_summary.collect_filtered_tablet_sizes(topo, table_id, topo.get_tablet_map(table_id), args) == ([100], [100, 0], [100])


def test_build_all_row_aggregates_table_summary() -> None:
    all_row = table_summary.build_all_row([
        {
            "total_size": 100,
            "tablet_count": 2,
            "replica_count": 4,
            "tablet_ovc": 1.5,
            "max_tablet_size": 70,
        },
        {
            "total_size": 60,
            "tablet_count": 1,
            "replica_count": 2,
            "tablet_ovc": 1.2,
            "max_tablet_size": 60,
        },
    ], PresentationOptions(human_readable=False))

    assert all_row == [
        "ALL",
        "160",
        "100.00",
        "",
        3,
        "+50.00",
        "26.667",
        "70",
        "",
    ]


def test_replica_based_avg_and_max_sizes() -> None:
    table_id = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    host1_id = UUID("11111111-1111-1111-1111-111111111111")
    host2_id = UUID("22222222-2222-2222-2222-222222222222")
    topo = Topology()
    topo._tables[table_id] = ("ks", "tbl")
    topo._hosts[host1_id] = Host(id=host1_id, shard_count=2, ip="10.0.0.1", dc="dc1", rack="rack1")
    topo._hosts[host2_id] = Host(id=host2_id, shard_count=2, ip="10.0.0.2", dc="dc1", rack="rack1")
    topo._tablet_maps[table_id] = TabletMap(
        table=table_id,
        tablets=[
            Tablet(last_token=0, replicas=[(host1_id, 0), (host2_id, 1)]),
            Tablet(last_token=1 << 63, replicas=[(host2_id, 1)]),
        ],
    )
    topo._tablet_sizes[(table_id, 0)] = {host1_id: 100, host2_id: 50}
    topo._tablet_sizes[(table_id, 1 << 63)] = {host2_id: 80}

    tablet_sizes, _token_space_sizes, replica_sizes = table_summary.collect_filtered_tablet_sizes(
        topo, table_id, topo.get_tablet_map(table_id), Namespace(host=None, shard=None, rack=None, dc=None)
    )

    assert tablet_sizes == [150, 80]
    assert replica_sizes == [100, 50, 80]
    assert sum(replica_sizes) / len(replica_sizes) == 230 / 3
    assert max(replica_sizes) == 100
