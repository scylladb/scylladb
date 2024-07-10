#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.pylib.manager_client import ManagerClient
from test.pylib.internal_types import ServerInfo, HostID
from test.pylib.rest_client import read_barrier
from typing import NamedTuple

class TabletReplicas(NamedTuple):
    last_token: int
    replicas: list[tuple[HostID, int]]

async def get_all_tablet_replicas(manager: ManagerClient, server: ServerInfo, keyspace_name: str, table_name: str) -> list[TabletReplicas]:
    """
    Retrieves the tablet distribution for a given table.
    This call is guaranteed to see all prior changes applied to group0 tables.

    :param server: server to query. Can be any live node.
    """

    host = manager.get_cql().cluster.metadata.get_host(server.ip_addr)

    # read_barrier is needed to ensure that local tablet metadata on the queried node
    # reflects the finalized tablet movement.
    await read_barrier(manager.api, server.ip_addr)

    table_id = await manager.get_table_id(keyspace_name, table_name)
    rows = await manager.get_cql().run_async(f"SELECT last_token, replicas FROM system.tablets where "
                                       f"table_id = {table_id}", host=host)
    return [TabletReplicas(
        last_token=x.last_token,
        replicas=[(HostID(str(host)), shard) for (host, shard) in x.replicas]
    ) for x in rows]

async def get_tablet_replicas(manager: ManagerClient, server: ServerInfo, keyspace_name: str, table_name: str, token: int) -> list[tuple[HostID, int]]:
    """
    Gets tablet replicas of the tablet which owns a given token of a given table.
    This call is guaranteed to see all prior changes applied to group0 tables.

    :param server: server to query. Can be any live node.
    """
    rows = await get_all_tablet_replicas(manager, server, keyspace_name, table_name)
    for row in rows:
        if row.last_token >= token:
            return row.replicas
    return []


async def get_tablet_replica(manager: ManagerClient, server: ServerInfo, keyspace_name: str, table_name: str, token: int) -> tuple[HostID, int]:
    """
    Get the first replica of the tablet which owns a given token of a given table.
    This call is guaranteed to see all prior changes applied to group0 tables.

    :param server: server to query. Can be any live node.
    """
    replicas = await get_tablet_replicas(manager, server, keyspace_name, table_name, token)
    return replicas[0]

