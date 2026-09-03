#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from tablets.filters import filter_args
from tablets.filters import get_table_filter
from tablets.filters import get_tablet_filter
from tablets.filters import parse_rack_arg
from tablets.filters import parse_shard_location_arg
from tablets.filters import resolve_table_filter_id
from tablets.topology import Topology

from test.tablet_scripts.topology_building import *


def test_parse_rack_arg() -> None:
    assert parse_rack_arg("dc1/rack1") == ("dc1", "rack1")


def test_parse_shard_location_arg() -> None:
    assert parse_shard_location_arg("10.0.0.1:3") == ("10.0.0.1", 3)
    assert parse_shard_location_arg("3") == 3


def accepts_replica(topology: Topology, replica: tuple, **filters) -> bool:
    return get_tablet_filter(filter_args(**filters), topology)(replica)


def test_filter_tablet_matches_host() -> None:
    topology = build_topology({
        HOST1_ID: HostSpec("10.0.0.1", "dc1", "rack1"),
        HOST2_ID: HostSpec("10.0.0.2", "dc1", "rack1"),
    })

    assert accepts_replica(topology, (HOST1_ID, 1), host="10.0.0.1") is True
    assert accepts_replica(topology, (HOST2_ID, 1), host="10.0.0.1") is False


def test_filter_tablet_matches_shard() -> None:
    topology = build_topology({
        HOST1_ID: HostSpec("10.0.0.1", "dc1", "rack1"),
        HOST2_ID: HostSpec("10.0.0.2", "dc1", "rack1"),
    })

    assert accepts_replica(topology, (HOST1_ID, 1), shard=("10.0.0.1", 1)) is True
    assert accepts_replica(topology, (HOST1_ID, 0), shard=("10.0.0.1", 1)) is False
    assert accepts_replica(topology, (HOST2_ID, 1), shard=("10.0.0.1", 1)) is False

    # A bare shard number selects that shard on every host.
    assert accepts_replica(topology, (HOST1_ID, 1), shard=1) is True
    assert accepts_replica(topology, (HOST2_ID, 1), shard=1) is True
    assert accepts_replica(topology, (HOST1_ID, 0), shard=1) is False


def test_filter_tablet_matches_rack() -> None:
    topology = build_topology({
        HOST3_ID: HostSpec("10.0.0.3", "dc3", "rack1"),
        HOST4_ID: HostSpec("10.0.0.4", "dc3", "rack1"),
        HOST5_ID: HostSpec("10.0.0.5", "dc3", "rack2"),
        HOST1_ID: HostSpec("10.0.0.1", "dc1", "rack1"),   # the same rack name, in another dc
    })

    assert accepts_replica(topology, (HOST3_ID, 0), rack=("dc3", "rack1")) is True
    assert accepts_replica(topology, (HOST4_ID, 0), rack=("dc3", "rack1")) is True
    assert accepts_replica(topology, (HOST5_ID, 0), rack=("dc3", "rack1")) is False
    assert accepts_replica(topology, (HOST1_ID, 0), rack=("dc3", "rack1")) is False


def test_filter_tablet_matches_dc() -> None:
    topology = build_topology({
        HOST3_ID: HostSpec("10.0.0.3", "dc3", "rack1"),
        HOST5_ID: HostSpec("10.0.0.5", "dc3", "rack2"),
        HOST1_ID: HostSpec("10.0.0.1", "dc1", "rack1"),
    })

    assert accepts_replica(topology, (HOST3_ID, 0), dc="dc3") is True
    assert accepts_replica(topology, (HOST5_ID, 0), dc="dc3") is True
    assert accepts_replica(topology, (HOST1_ID, 0), dc="dc3") is False


def test_filter_tablet_narrows_by_every_filter_given() -> None:
    """
    Filters intersect: a replica has to satisfy all of them, so a combination selects less
    than either filter alone.
    """
    topology = build_topology({
        HOST3_ID: HostSpec("10.0.0.3", "dc3", "rack1"),
        HOST4_ID: HostSpec("10.0.0.4", "dc3", "rack1"),
        HOST5_ID: HostSpec("10.0.0.5", "dc3", "rack2"),
        HOST1_ID: HostSpec("10.0.0.1", "dc1", "rack1"),
    })

    # A rack and a bare shard number: that shard, on the hosts of that rack only.
    assert accepts_replica(topology, (HOST3_ID, 1), rack=("dc3", "rack1"), shard=1) is True
    assert accepts_replica(topology, (HOST4_ID, 1), rack=("dc3", "rack1"), shard=1) is True
    assert accepts_replica(topology, (HOST3_ID, 0), rack=("dc3", "rack1"), shard=1) is False
    assert accepts_replica(topology, (HOST5_ID, 1), rack=("dc3", "rack1"), shard=1) is False

    # A dc and a host: the host, and only while it is in that dc.
    assert accepts_replica(topology, (HOST3_ID, 0), dc="dc3", host="10.0.0.3") is True
    assert accepts_replica(topology, (HOST4_ID, 0), dc="dc3", host="10.0.0.3") is False
    assert accepts_replica(topology, (HOST1_ID, 0), dc="dc3", host="10.0.0.1") is False


def test_table_and_replica_filters_come_from_one_args() -> None:
    """
    The two builders read the same arguments and neither sees the other's: a table filter
    selects tables whatever narrows the replicas, and vice versa.
    """
    topology = build_topology({
        HOST1_ID: HostSpec("10.0.0.1", "dc1", "rack1"),
        HOST2_ID: HostSpec("10.0.0.2", "dc1", "rack1"),
    }, tables={
        TABLE1_ID: ("ks1", "users"),
        TABLE3_ID: ("ks1", "orders"),
    })

    args = filter_args(table="ks1.users", host="10.0.0.1")

    accepts_table = get_table_filter(args, topology)
    assert accepts_table(TABLE1_ID) is True
    assert accepts_table(TABLE3_ID) is False

    accepts_tablet = get_tablet_filter(args, topology)
    assert accepts_tablet((HOST1_ID, 0)) is True
    assert accepts_tablet((HOST2_ID, 0)) is False

    args = filter_args(table="ks1.users", shard=1)

    assert get_table_filter(args, topology)(TABLE1_ID) is True
    accepts_tablet = get_tablet_filter(args, topology)
    assert accepts_tablet((HOST1_ID, 1)) is True
    assert accepts_tablet((HOST1_ID, 0)) is False


def test_resolve_table_filter_id_uses_keyspace_to_disambiguate_bare_table_name() -> None:
    # Both keyspaces hold a "users", so the bare name needs one of them to resolve.
    topology = build_topology({HOST1_ID: HostSpec("10.0.0.1", "dc1", "rack1")}, tables={
        TABLE1_ID: ("ks1", "users"),
        TABLE2_ID: ("ks2", "users"),
    })

    resolved = resolve_table_filter_id(filter_args(table="users", keyspace="ks1"), topology)

    assert resolved == TABLE1_ID


def test_filter_table_id_respects_keyspace() -> None:
    topology = build_topology({HOST1_ID: HostSpec("10.0.0.1", "dc1", "rack1")}, tables={
        TABLE1_ID: ("ks1", "users"),
        TABLE2_ID: ("ks2", "users"),
        TABLE3_ID: ("ks1", "orders"),
    })

    accepts_table = get_table_filter(filter_args(keyspace="ks1"), topology)

    assert accepts_table(TABLE1_ID) is True
    assert accepts_table(TABLE2_ID) is False
    assert accepts_table(TABLE3_ID) is True
