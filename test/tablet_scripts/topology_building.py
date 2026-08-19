#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Topologies for the unit tests to assert over, and the ids they are built from.

A test names every part of the topology its assertions depend on, so the layout stays next
to them and nothing they rest on is filled in out of sight.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from tablets.topology import DatacenterName
from tablets.topology import Host
from tablets.topology import HostId
from tablets.topology import KeyspaceName
from tablets.topology import RackName
from tablets.topology import TableId
from tablets.topology import TableName
from tablets.topology import Tablet
from tablets.topology import TabletMap
from tablets.topology import TabletReplica
from tablets.topology import Token
from tablets.topology import Topology


HOST1_ID = UUID("11111111-1111-1111-1111-111111111111")
HOST2_ID = UUID("22222222-2222-2222-2222-222222222222")
HOST3_ID = UUID("33333333-3333-3333-3333-333333333333")
HOST4_ID = UUID("44444444-4444-4444-4444-444444444444")
HOST5_ID = UUID("55555555-5555-5555-5555-555555555555")

TABLE1_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
TABLE2_ID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
TABLE3_ID = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")


@dataclass(frozen=True)
class HostSpec:
    """
    A host to place: where it sits, how many shards it has, and how much it can hold.

    A test states the capacity it measures against, and leaves it out when it measures
    nothing against it, which is a snapshot that carries no capacity for the node.
    """
    ip: str
    dc: DatacenterName
    rack: RackName
    shard_count: int = 2
    storage_capacity: int | None = None
    effective_capacity: int | None = None
    # What system.load_per_node says about the node beyond its load: whether the cluster can
    # reach it, and whether it takes part in balancing. Left out when a test measures neither.
    up: bool | None = None
    excluded: bool | None = None


def build_topology(hosts: dict[HostId, HostSpec],
                   tables: dict[TableId, tuple[KeyspaceName, TableName]] | None = None,
                   tablets: dict[TableId, list[tuple[Token, dict[TabletReplica, int]]]] | None = None) -> Topology:
    """
    A topology of the hosts the caller places, of the tables it names, and of their tablets,
    each given as its last token and the size of every replica.

    Everything a test can assert on is named by the test; nothing is filled in here.
    """
    topo = Topology()
    for host_id, host in hosts.items():
        topo._hosts[host_id] = Host(id=host_id, shard_count=host.shard_count, ip=host.ip,
                                    dc=host.dc, rack=host.rack,
                                    storage_capacity=host.storage_capacity,
                                    effective_capacity=host.effective_capacity,
                                    up=host.up, excluded=host.excluded)
    for table_id, name in (tables or {}).items():
        topo._tables[table_id] = name
        topo._tablet_maps[table_id] = TabletMap(table_id)
    for table_id, table_tablets in (tablets or {}).items():
        topo._tablet_maps[table_id] = TabletMap(
            table=table_id,
            tablets=[Tablet(last_token=token, replicas=list(sizes)) for token, sizes in table_tablets],
        )
        for token, sizes in table_tablets:
            topo._tablet_sizes[(table_id, token)] = {host_id: size for (host_id, _), size in sizes.items()}
    # Links colocated tablet maps to the tablets of their base table, as building from rows does.
    topo._build({})
    return topo

