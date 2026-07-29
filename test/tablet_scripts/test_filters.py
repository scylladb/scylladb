#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from argparse import Namespace
from uuid import UUID

from tablets.filters import filter_table_id
from tablets.filters import filter_tablet
from tablets.filters import parse_rack_arg
from tablets.filters import parse_shard_location_arg
from tablets.filters import resolve_table_filter_id
from tablets.topology import Host
from tablets.topology import TabletMap
from tablets.topology import Topology


def build_topology() -> tuple[Topology, UUID, UUID]:
    host1_id = UUID("11111111-1111-1111-1111-111111111111")
    host2_id = UUID("22222222-2222-2222-2222-222222222222")
    topo = Topology()
    topo._hosts[host1_id] = Host(id=host1_id, shard_count=2, ip="10.0.0.1", dc="dc1", rack="rack1")
    topo._hosts[host2_id] = Host(id=host2_id, shard_count=2, ip="10.0.0.2", dc="dc2", rack="rack2")
    return topo, host1_id, host2_id


def build_table_topology() -> tuple[Topology, UUID, UUID, UUID]:
    topo = Topology()
    table1 = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    table2 = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    table3 = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
    topo._tables[table1] = ("ks1", "users")
    topo._tables[table2] = ("ks2", "users")
    topo._tables[table3] = ("ks1", "orders")
    topo._tablet_maps[table1] = TabletMap(table1)
    topo._tablet_maps[table2] = TabletMap(table2)
    topo._tablet_maps[table3] = TabletMap(table3)
    return topo, table1, table2, table3


def test_parse_rack_arg() -> None:
    assert parse_rack_arg("dc1/rack1") == ("dc1", "rack1")


def test_parse_shard_location_arg() -> None:
    assert parse_shard_location_arg("10.0.0.1:3") == ("10.0.0.1", 3)
    assert parse_shard_location_arg("3") == 3


def test_filter_tablet_matches_host_shard_rack_and_dc() -> None:
    topo, host1_id, host2_id = build_topology()

    assert filter_tablet(Namespace(host="10.0.0.1", shard=None, rack=None, dc=None), topo, (host1_id, 1)) is True
    assert filter_tablet(Namespace(host="10.0.0.1", shard=None, rack=None, dc=None), topo, (host2_id, 1)) is False

    assert filter_tablet(Namespace(host=None, shard=("10.0.0.1", 1), rack=None, dc=None), topo, (host1_id, 1)) is True
    assert filter_tablet(Namespace(host=None, shard=("10.0.0.1", 1), rack=None, dc=None), topo, (host1_id, 0)) is False

    assert filter_tablet(Namespace(host=None, shard=1, rack=None, dc=None), topo, (host1_id, 1)) is True
    assert filter_tablet(Namespace(host=None, shard=1, rack=None, dc=None), topo, (host2_id, 1)) is True
    assert filter_tablet(Namespace(host=None, shard=1, rack=None, dc=None), topo, (host1_id, 0)) is False

    assert filter_tablet(Namespace(host=None, shard=None, rack=("dc1", "rack1"), dc=None), topo, (host1_id, 0)) is True
    assert filter_tablet(Namespace(host=None, shard=None, rack=("dc1", "rack1"), dc=None), topo, (host2_id, 0)) is False

    assert filter_tablet(Namespace(host=None, shard=None, rack=None, dc="dc2"), topo, (host2_id, 0)) is True
    assert filter_tablet(Namespace(host=None, shard=None, rack=None, dc="dc2"), topo, (host1_id, 0)) is False


def test_resolve_table_filter_id_uses_keyspace_to_disambiguate_bare_table_name() -> None:
    topo, table1, _table2, _table3 = build_table_topology()

    resolved = resolve_table_filter_id(Namespace(table="users", keyspace="ks1"), topo)

    assert resolved == table1


def test_filter_table_id_respects_keyspace() -> None:
    topo, table1, table2, table3 = build_table_topology()

    args = Namespace(table=None, keyspace="ks1")

    assert filter_table_id(args, topo, table1) is True
    assert filter_table_id(args, topo, table2) is False
    assert filter_table_id(args, topo, table3) is True
