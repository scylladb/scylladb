#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import os
import asyncio
import logging
import time
from shutil import rmtree
from typing import TYPE_CHECKING

from test.pylib.tablets import get_all_tablet_replicas
from test.cluster.util import get_coordinator_host, get_non_coordinator_host, wait_new_coordinator_elected
from test.cluster.random_failures.error_injections import ERROR_INJECTIONS

if TYPE_CHECKING:
    from typing import ParamSpec, TypeAlias, TypeVar
    from collections.abc import Callable, AsyncIterator

    from test.pylib.random_tables import RandomTables
    from test.pylib.manager_client import ManagerClient


TOPOLOGY_TIMEOUT = 300  # default topology timeout is too big for these tests

LOGGER = logging.getLogger(__name__)


if TYPE_CHECKING:
    ClusterEventType: TypeAlias = Callable[
        [ManagerClient, RandomTables, str],
        AsyncIterator[None],
    ]
    P = ParamSpec("P")
    T = TypeVar("T")


def deselect_for(reason: str, error_injections: list[str] | None = None) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Add a metadata with disabled error injections to a cluster event function.

    If `error_injection` is None then the cluster event disabled for all error injections available.
    """
    def add_deselected_metadata(fn: Callable[P, T]) -> Callable[P, T]:
        if not hasattr(fn, "deselected_random_failures"):
            fn.deselected_random_failures = {}
        for inj in ERROR_INJECTIONS if error_injections is None else error_injections:
            fn.deselected_random_failures[inj] = reason
        return fn
    return add_deselected_metadata


# Each cluster event is an async generator which has 2 yields and should be used in the following way:
#
#   0. Start the generator:
#       >>> cluster_event_steps = cluster_event(manager, random_tables, error_injection)
#
#   1. Run the prepare part (before the first yield)
#       >>> await anext(cluster_event_steps)
#
#   2. Run the cluster event itself (between the yields)
#       >>> await anext(cluster_event_steps)
#
#   3. Run the check part (after the second yield)
#       >>> await anext(cluster_event, None)


async def sleep_for_30_seconds(manager: ManagerClient,
                               random_tables: RandomTables,
                               error_injection: str) -> AsyncIterator[None]:
    yield

    LOGGER.info("Sleep for 30 seconds")
    await asyncio.sleep(30)

    yield


async def add_new_table(manager: ManagerClient,
                        random_tables: RandomTables,
                        error_injection: str) -> AsyncIterator[None]:
    yield

    LOGGER.info("Add a new table to the schema")
    table = await random_tables.add_table(ncolumns=5)

    yield

    LOGGER.info("Check if the table was created by dropping it")
    await random_tables.drop_table(table=table)


async def drop_table(manager: ManagerClient,
                     random_tables: RandomTables,
                     error_injection: str) -> AsyncIterator[None]:
    table_name = "test_random_failures_table_to_drop"

    LOGGER.info("Add a new table to drop")
    table = await random_tables.add_table(ncolumns=5, name=table_name)

    yield

    LOGGER.info("Drop the table `%s' from the schema", table.name)
    await random_tables.drop_table(table=table)

    yield

    LOGGER.info("Check if the table was dropped by re-creating it")
    await random_tables.add_table(ncolumns=5, name=table_name)


async def add_index(manager: ManagerClient,
                    random_tables: RandomTables,
                    error_injection: str) -> AsyncIterator[None]:
    yield

    LOGGER.info("Add an index to a table")
    index_name = await random_tables[0].add_index(column=random_tables[0].columns[-1])

    yield

    LOGGER.info("Check if the index was created by dropping it")
    await random_tables[0].drop_index(name=index_name)


async def drop_index(manager: ManagerClient,
                     random_tables: RandomTables,
                     error_injection: str) -> AsyncIterator[None]:
    index_name = "test_random_failures_index_to_drop"

    LOGGER.info("Add an index to a table")
    await random_tables[0].add_index(column=random_tables[0].columns[-2], name=index_name)

    yield

    LOGGER.info("Drop the index from the table")
    await random_tables[0].drop_index(name=index_name)

    yield

    LOGGER.info("Check if the index was dropped by re-creating it")
    await random_tables[0].add_index(column=random_tables[0].columns[-2], name=index_name)


async def add_new_keyspace(manager: ManagerClient,
                           random_tables: RandomTables,
                           error_injection: str) -> AsyncIterator[None]:
    ks_name = "test_random_failures_new_ks"

    yield

    LOGGER.info("Add a new keyspace to the schema")
    await manager.cql.run_async(
        f"CREATE KEYSPACE {ks_name}"
        f" WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3 }}"
    )

    yield

    LOGGER.info("Check if the keyspace exists by dropping it")
    await manager.cql.run_async(f"DROP KEYSPACE {ks_name}")


async def drop_keyspace(manager: ManagerClient,
                        random_tables: RandomTables,
                        error_injection: str) -> AsyncIterator[None]:
    ks_name = "test_random_failures_ks_to_drop"

    LOGGER.info("Add a keyspace to drop")
    await manager.cql.run_async(
        f"CREATE KEYSPACE {ks_name}"
        f" WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3 }}"
    )

    yield

    LOGGER.info("Drop the keyspace from the schema")
    await manager.cql.run_async(f"DROP KEYSPACE {ks_name}")

    yield

    LOGGER.info("Check if the keyspace was dropped by re-creating it")
    await manager.cql.run_async(
        f"CREATE KEYSPACE {ks_name}"
        f" WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 3 }}"
    )


@deselect_for(
    # TODO: remove this skip when #16317 will be resolved.
    reason="Cannot create CDC log for tables, because keyspace uses tablets. See issue #16317",
)
async def add_cdc(manager: ManagerClient,
                  random_tables: RandomTables,
                  error_injection: str) -> AsyncIterator[None]:
    table_name = "test_random_failures_table_with_cdc"

    yield

    LOGGER.info("Add a table with CDC enabled and add some data")
    table = await random_tables.add_table(ncolumns=5, name=table_name)
    await table.enable_cdc()
    await table.insert_seq()

    yield


@deselect_for(
    # TODO: remove this skip when #16317 will be resolved.
    reason="Cannot create CDC log for tables, because keyspace uses tablets. See issue #16317",
)
async def drop_cdc(manager: ManagerClient,
                   random_tables: RandomTables,
                   error_injection: str) -> AsyncIterator[None]:
    table_name = "test_random_failures_table_with_cdc"

    LOGGER.info("Add a table with CDC enabled and add some data")
    table = await random_tables.add_table(ncolumns=5, name=table_name)
    await table.enable_cdc()
    await table.insert_seq()

    yield

    LOGGER.info("Disable CDC for the table and add some more data")
    await table.disable_cdc()
    await table.insert_seq()

    yield


async def add_new_udt(manager: ManagerClient,
                      random_tables: RandomTables,
                      error_injection: str) -> AsyncIterator[None]:
    udt_name = "test_random_failures_new_udt"

    yield

    LOGGER.info("Add a new UDT to the schema")
    await random_tables.add_udt(name=udt_name, cmd="(a text, b int)")

    yield

    LOGGER.info("Check if the UDT was created by dropping it")
    await random_tables.drop_udt(name=udt_name)


async def drop_udt(manager: ManagerClient,
                   random_tables: RandomTables,
                   error_injection: str) -> AsyncIterator[None]:
    udt_name = "test_random_failures_udt_to_drop"

    LOGGER.info("Add an UDT to drop")
    await random_tables.add_udt(name=udt_name, cmd="(a text, b int)")

    yield

    LOGGER.info("Drop an UDT from the schema")
    await random_tables.drop_udt(name=udt_name)

    yield

    LOGGER.info("Check if the UDT was dropped by re-creating it")
    await random_tables.add_udt(name=udt_name, cmd="(a text, b int)")


async def insert_records(manager: ManagerClient,
                         random_tables: RandomTables,
                         error_injection: str) -> AsyncIterator[None]:
    yield

    LOGGER.info("Add records to the table")
    await random_tables[0].insert_seq()

    yield


async def update_record(manager: ManagerClient,
                        random_tables: RandomTables,
                        error_injection: str) -> AsyncIterator[None]:
    LOGGER.info("Add a record to the table")
    table = random_tables[0]
    await table.insert_seq()

    yield

    pk_columns = table.columns[:table.pks]
    set_column = table.columns[-1]
    next_seq = table.next_seq()
    set_value = set_column.val(seed=next_seq)

    LOGGER.info("Update the record in the table")
    await manager.cql.run_async(
        f"UPDATE {table.full_name} SET {set_column} = %s WHERE {' AND '.join(f'{c.name} = %s' for c in pk_columns)}",
        [set_value] + [c.val(next_seq-1) for c in pk_columns],
    )

    yield


@deselect_for(
    # TODO: remove this skip when #18068 will be resolved.
    reason="LWT is not yet supported with tablets. See issue #18068"
)
async def execute_lwt_transaction(manager: ManagerClient,
                                  random_tables: RandomTables,
                                  error_injection: str) -> AsyncIterator[None]:
    LOGGER.info("Add a record to the table")
    table = random_tables[0]
    await table.insert_seq()

    yield

    pk_columns = table.columns[:table.pks]
    set_column = table.columns[-1]
    next_seq = table.next_seq()
    set_value = set_column.val(seed=next_seq)

    LOGGER.info("Execute a lightweight transaction")
    await manager.cql.run_async(
        f"UPDATE {table.full_name}"
        f" SET {set_column} = %s"
        f" WHERE {' AND '.join(f'{c.name} = %s' for c in pk_columns)}"
        f" IF EXISTS",
        [set_value] + [c.val(next_seq-1) for c in pk_columns],
    )

    yield


@deselect_for(
    error_injections=[
        "stop_after_starting_auth_service",
        "stop_after_setting_mode_to_normal_raft_topology",
        "stop_before_becoming_raft_voter",
        "stop_after_updating_cdc_generation",
        "stop_before_streaming",
        "stop_after_streaming",
    ],
    reason="See issue #19151 (decommission process stuck while boostrapping node is paused)",
)
async def init_tablet_transfer(manager: ManagerClient,
                               random_tables: RandomTables,
                               error_injection: str) -> AsyncIterator[None]:
    await manager.cql.run_async(
        "CREATE KEYSPACE test"
        " WITH replication = { 'class': 'NetworkTopologyStrategy', 'replication_factor': 3 } AND"
        "      tablets = { 'initial': 1 }"
    )
    await manager.cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int)")
    await asyncio.gather(
        *[manager.cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k})") for k in range(256)]
    )

    servers = await manager.running_servers()
    host_ids = set()
    for s in servers:
        host_ids.add(await manager.get_host_id(s.server_id))
        await manager.api.disable_tablet_balancing(node_ip=s.ip_addr)

    replicas = await get_all_tablet_replicas(
        manager=manager,
        server=servers[0],
        keyspace_name="test",
        table_name="test",
    )

    LOGGER.info("Tablet is on [%s]", replicas)

    assert len(replicas) == 1 and len(replicas[0].replicas) == 3

    old_host, old_shard = replicas[0].replicas[0]
    new_host = (host_ids - {r[0] for r in replicas[0].replicas}).pop()

    yield

    LOGGER.info("Move tablet %s -> %s", old_host, new_host)
    await manager.api.move_tablet(
        node_ip=servers[0].ip_addr,
        ks="test",
        table="test",
        src_host=old_host,
        src_shard=old_shard,
        dst_host=new_host,
        dst_shard=0,
        token=0,
    )

    yield

    replicas = await get_all_tablet_replicas(
        manager=manager,
        server=servers[0],
        keyspace_name="test",
        table_name="test",
    )

    assert len(replicas) == 1

    hosts_with_replica = [r[0] for r in replicas[0].replicas]

    assert len(hosts_with_replica) == 3
    assert new_host in hosts_with_replica
    assert old_host not in hosts_with_replica


# @deselect_for(
#     error_injections=[
#         "stop_after_starting_auth_service",
#         "stop_after_setting_mode_to_normal_raft_topology",
#         "stop_before_becoming_raft_voter",
#         "stop_after_updating_cdc_generation",
#         "stop_before_streaming",
#         "stop_after_streaming",
#     ],
#     reason="Can't add a node to a cluster with a banned node",
# )
@deselect_for(
    # TODO: remove this skip when #20751 will be resolved.
    reason="test_random_failures: remove_data_dir_of_dead_node cluster event is incorrect (causes test flakiness). See issue #20751",
)
async def remove_data_dir_of_dead_node(manager: ManagerClient,
                                       random_tables: RandomTables,
                                       error_injection: str) -> AsyncIterator[None]:
    running_servers = await manager.running_servers()
    data_dir = os.path.join(await manager.server_get_workdir(running_servers[1].server_id), "data")

    LOGGER.info("Kill a node")
    await manager.server_stop(server_id=running_servers[1].server_id)
    await manager.server_not_sees_other_server(
        server_ip=running_servers[0].ip_addr,
        other_ip=running_servers[1].ip_addr,
    )

    yield

    LOGGER.info("Remove data dir of the dead node and start it")
    rmtree(path=data_dir, ignore_errors=True)
    await manager.server_start(server_id=running_servers[1].server_id)

    yield


@deselect_for(
    error_injections=[
        "stop_after_starting_auth_service",
        "stop_after_setting_mode_to_normal_raft_topology",
        "stop_before_becoming_raft_voter",
        "stop_after_updating_cdc_generation",
        "stop_before_streaming",
        "stop_after_streaming",
    ],
    reason="See issue #18640 (failed to add a node to a cluster if another bootstrapping node is stuck)",
)
async def add_new_node(manager: ManagerClient,
                       random_tables: RandomTables,
                       error_injection: str) -> AsyncIterator[None]:
    yield

    LOGGER.info("Add a new node to the cluster")
    await manager.server_add(timeout=TOPOLOGY_TIMEOUT)

    yield


@deselect_for(
    error_injections=[
        "stop_after_starting_auth_service",
        "stop_after_setting_mode_to_normal_raft_topology",
        "stop_before_becoming_raft_voter",
        "stop_after_updating_cdc_generation",
        "stop_before_streaming",
        "stop_after_streaming",
    ],
    reason="See issue #19151 (decommission process stuck while bootstrapping node is paused)",
)
async def decommission_node(manager: ManagerClient,
                            random_tables: RandomTables,
                            error_injection: str) -> AsyncIterator[None]:
    yield

    LOGGER.info("Decommission a node")
    await manager.decommission_node(
        server_id=(await manager.running_servers())[1].server_id,
        timeout=TOPOLOGY_TIMEOUT,
    )

    yield


@deselect_for(
    error_injections=[
        "stop_after_starting_auth_service",
        "stop_after_setting_mode_to_normal_raft_topology",
        "stop_before_becoming_raft_voter",
        "stop_after_updating_cdc_generation",
        "stop_before_streaming",
        "stop_after_streaming",
    ],
    reason="Can't add a node to a cluster with a banned node",
)
async def remove_node(manager: ManagerClient,
                      random_tables: RandomTables,
                      error_injection: str) -> AsyncIterator[None]:
    running_servers = await manager.running_servers()

    LOGGER.info("Kill a node")
    await manager.server_stop(server_id=running_servers[1].server_id)
    await manager.server_not_sees_other_server(
        server_ip=running_servers[0].ip_addr,
        other_ip=running_servers[1].ip_addr,
    )

    yield

    LOGGER.info("Remove the dead node")
    await manager.remove_node(
        initiator_id=running_servers[0].server_id,
        server_id=running_servers[1].server_id,
        wait_removed_dead=False,
        timeout=TOPOLOGY_TIMEOUT,
    )

    yield


async def restart_non_coordinator_node(manager: ManagerClient,
                                       random_tables: RandomTables,
                                       error_injection: str) -> AsyncIterator[None]:
    yield

    LOGGER.info("Restart a non-coordinator node")
    await manager.server_restart(server_id=(await get_non_coordinator_host(manager=manager)).server_id, wait_others=3)

    yield


async def restart_coordinator_node(manager: ManagerClient,
                                   random_tables: RandomTables,
                                   error_injection: str) -> AsyncIterator[None]:
    yield

    LOGGER.info("Restart the coordinator node")
    await manager.server_restart(server_id=(await get_coordinator_host(manager=manager)).server_id, wait_others=3)

    yield

async def stop_non_coordinator_node_gracefully(manager: ManagerClient,
                                               random_tables: RandomTables,
                                               error_injection: str) -> AsyncIterator[None]:
    yield

    LOGGER.info("Stop a non-coordinator node gracefully")
    await manager.server_stop_gracefully(server_id=(await get_non_coordinator_host(manager=manager)).server_id)

    yield


async def stop_coordinator_node_gracefully(manager: ManagerClient,
                                           random_tables: RandomTables,
                                           error_injection: str) -> AsyncIterator[None]:
    yield

    LOGGER.info("Stop the coordinator node gracefully")
    await manager.server_stop_gracefully(server_id=(await get_coordinator_host(manager=manager)).server_id)
    await wait_new_coordinator_elected(manager=manager, expected_num_of_elections=2, deadline=time.time() + 60)

    yield


async def kill_non_coordinator_node(manager: ManagerClient,
                                    random_tables: RandomTables,
                                    error_injection: str) -> AsyncIterator[None]:
    yield

    LOGGER.info("Kill a non-coordinator node")
    await manager.server_stop(server_id=(await get_non_coordinator_host(manager=manager)).server_id)

    LOGGER.info("Sleep for 2 seconds")
    await asyncio.sleep(2)

    yield


async def kill_coordinator_node(manager: ManagerClient,
                                random_tables: RandomTables,
                                error_injection: str) -> AsyncIterator[None]:
    yield

    LOGGER.info("Kill the coordinator node")
    await manager.server_stop(server_id=(await get_coordinator_host(manager=manager)).server_id)
    await wait_new_coordinator_elected(manager=manager, expected_num_of_elections=2, deadline=time.time() + 60)

    yield


# - New items should be added to the end of the tuple
# - Existing items should not be rearranged or deleted

CLUSTER_EVENTS: tuple[ClusterEventType, ...] = (
    sleep_for_30_seconds,
    add_new_table,
    drop_table,
    add_index,
    drop_index,
    add_new_keyspace,
    drop_keyspace,
    add_cdc,
    drop_cdc,
    add_new_udt,
    drop_udt,
    insert_records,
    update_record,
    execute_lwt_transaction,
    init_tablet_transfer,
    remove_data_dir_of_dead_node,
    add_new_node,
    decommission_node,
    remove_node,
    restart_non_coordinator_node,
    restart_coordinator_node,
    stop_non_coordinator_node_gracefully,
    stop_coordinator_node_gracefully,
    kill_non_coordinator_node,
    kill_coordinator_node,
)
