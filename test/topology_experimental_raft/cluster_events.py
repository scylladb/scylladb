# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Awaitable

    from test.pylib.random_tables import RandomTables
    from test.pylib.manager_client import ManagerClient


LOGGER = logging.getLogger(__name__)


async def sleep_for_30_seconds(manager: ManagerClient, random_tables: RandomTables) -> None:
    LOGGER.info("Sleep for 30 seconds")
    await asyncio.sleep(30)


async def add_new_keyspace(manager: ManagerClient, random_tables: RandomTables) -> None:
    LOGGER.info("Add a new keyspace to the schema")
    await manager.cql.run_async(
        "CREATE KEYSPACE test_random_failures_new_ks"
        " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3 }"
    )


async def drop_keyspace(manager: ManagerClient, random_tables: RandomTables) -> None:
    LOGGER.info("Drop a keyspace from the schema")
    await manager.cql.run_async("DROP KEYSPACE test_random_failures_ks_to_drop")


async def add_new_udt(manager: ManagerClient, random_tables: RandomTables) -> None:
    LOGGER.info("Add a new UDT to the schema")
    await random_tables.add_udt("test_random_failures_new_udt", "(a text, b int)")


async def drop_udt(manager: ManagerClient, random_tables: RandomTables) -> None:
    LOGGER.info("Drop an UDT from the schema")
    await random_tables.drop_udt("test_random_failures_udt_to_drop")


async def add_new_table(manager: ManagerClient, random_tables: RandomTables) -> None:
    LOGGER.info("Add a new table to the schema")
    await random_tables.add_table(ncolumns=5)


async def drop_table(manager: ManagerClient, random_tables: RandomTables) -> None:
    LOGGER.info("Drop a table from the schema")
    await random_tables.drop_table(random_tables[-1])


async def add_new_node(manager: ManagerClient, random_tables: RandomTables) -> None:
    LOGGER.info("Add a new node to the cluster")
    await manager.server_add()


async def insert_records(manager: ManagerClient, random_tables: RandomTables) -> None:
    LOGGER.info("Add records to a table ")
    await random_tables[0].insert_seq()


async def add_index(manager: ManagerClient, random_tables: RandomTables) -> None:
    LOGGER.info("Add an index to a table ")
    await random_tables[0].add_index(random_tables[0].columns[-1])


async def drop_index(manager: ManagerClient, random_tables: RandomTables) -> None:
    LOGGER.info("Drop an index from a table ")
    await random_tables[0].drop_index("test_random_failures_index_to_drop")


async def restart_non_coordinator_node(manager: ManagerClient, random_tables: RandomTables) -> None:
    LOGGER.info("Restart a non-coordinator node")
    await manager.server_restart(server_id=(await manager.running_servers())[1].server_id)


async def restart_coordinator_node(manager: ManagerClient, random_tables: RandomTables) -> None:
    LOGGER.info("Restart the coordinator node")
    await manager.server_restart(server_id=(await manager.running_servers())[0].server_id)


async def stop_non_coordinator_node_gracefully(manager: ManagerClient, random_tables: RandomTables) -> None:
    LOGGER.info("Stop a non-coordinator node gracefully")
    await manager.server_stop_gracefully(server_id=(await manager.running_servers())[1].server_id)


async def stop_coordinator_node_gracefully(manager: ManagerClient, random_tables: RandomTables) -> None:
    LOGGER.info("Stop the coordinator node gracefully")
    await manager.server_stop_gracefully(server_id=(await manager.running_servers())[0].server_id)


async def kill_non_coordinator_node(manager: ManagerClient, random_tables: RandomTables) -> None:
    LOGGER.info("Kill a non-coordinator node")
    await manager.server_stop(server_id=(await manager.running_servers())[1].server_id)

    LOGGER.info("Sleep for 2 seconds")
    await asyncio.sleep(2)


async def kill_coordinator_node(manager: ManagerClient, random_tables: RandomTables) -> None:
    LOGGER.info("Kill the coordinator node")
    await manager.server_stop(server_id=(await manager.running_servers())[0].server_id)

    LOGGER.info("Sleep for 2 seconds")
    await asyncio.sleep(2)


# - New items should be added to the end of the tuple
# - Existing items should not be rearranged or deleted

CLUSTER_EVENTS: tuple[Callable[[ManagerClient, RandomTables], Awaitable[None]], ...] = (
    sleep_for_30_seconds,
    add_new_keyspace,
    drop_keyspace,
    add_new_table,
    drop_table,
    add_new_udt,
    drop_udt,
    insert_records,
    add_index,
    drop_index,
    add_new_node,
    restart_non_coordinator_node,
    restart_coordinator_node,
    stop_non_coordinator_node_gracefully,
    stop_coordinator_node_gracefully,
    kill_non_coordinator_node,
    kill_coordinator_node,
)
