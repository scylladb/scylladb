#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import time
import pytest
import logging
import re
from uuid import UUID

from cassandra.cluster import Session, ConsistencyLevel # type: ignore
from cassandra.query import SimpleStatement # type: ignore
from cassandra.pool import Host # type: ignore

from test.pylib.manager_client import ManagerClient, ServerInfo
from test.pylib.util import wait_for_cql_and_get_hosts
from test.pylib.log_browsing import ScyllaLogFile
from test.topology.util import reconnect_driver, wait_until_upgrade_finishes, \
        enter_recovery_state, delete_raft_data_and_upgrade_state


logger = logging.getLogger(__name__)


async def get_local_schema_version(cql: Session, h: Host) -> UUID:
    rs = await cql.run_async("select schema_version from system.local where key = 'local'", host=h)
    assert(rs)
    return rs[0].schema_version


async def get_group0_schema_version(cql: Session, h: Host) -> UUID | None:
    rs = await cql.run_async("select value from system.scylla_local where key = 'group0_schema_version'", host=h)
    if rs:
        return UUID(rs[0].value)
    return None


async def get_scylla_tables_versions(cql: Session, h: Host) -> list[tuple[str, str, UUID | None]]:
    rs = await cql.run_async("select keyspace_name, table_name, version from system_schema.scylla_tables", host=h)
    return [(r.keyspace_name, r.table_name, r.version) for r in rs]


async def get_scylla_tables_version(cql: Session, h: Host, keyspace_name: str, table_name: str) -> UUID | None:
    rs = await cql.run_async(
            f"select version from system_schema.scylla_tables"
            f" where keyspace_name = '{keyspace_name}' and table_name = '{table_name}'",
            host=h)
    if not rs:
        pytest.fail(f"No scylla_tables row found for {keyspace_name}.{table_name}")
    return rs[0].version


async def verify_local_schema_versions_synced(cql: Session, hs: list[Host]) -> None:
    versions = {h: await get_local_schema_version(cql, h) for h in hs}
    logger.info(f"system.local schema_versions: {versions}")
    h1, v1 = next(iter(versions.items()))
    for h, v in versions.items():
        if v != v1:
            pytest.fail(f"{h1}'s system.local schema_version {v1} is different than {h}'s version {v}")


async def verify_group0_schema_versions_synced(cql: Session, hs: list[Host]) -> None:
    versions = {h: await get_group0_schema_version(cql, h) for h in hs}
    logger.info(f"system.scylla_local group0_schema_versions: {versions}")
    h1, v1 = next(iter(versions.items()))
    for h, v in versions.items():
        if v != v1:
            pytest.fail(f"{h1}'s system.scylla_local group0_schema_version {v1} is different than {h}'s version {v}")


async def verify_scylla_tables_versions_synced(cql: Session, hs: list[Host], ignore_system_tables: bool) -> None:
    versions = {h: set(await get_scylla_tables_versions(cql, h)) for h in hs}
    logger.info(f"system_schema.scylla_tables: {versions}")
    h1, v1 = next(iter(versions.items()))
    for h, v in versions.items():
        diff = v.symmetric_difference(v1)
        if ignore_system_tables:
            diff = {(k, t, v) for k, t, v in diff if k != "system"}
        if diff:
            pytest.fail(f"{h1}'s system_schema.scylla_tables contents is different than {h}'s, symmetric diff: {diff}")


async def verify_table_versions_synced(cql: Session, hs: list[Host], ignore_system_tables: bool = False) -> None:
    logger.info("Verifying that versions stored in tables are in sync")
    await verify_group0_schema_versions_synced(cql, hs)
    await verify_local_schema_versions_synced(cql, hs)
    await verify_scylla_tables_versions_synced(cql, hs, ignore_system_tables)


async def verify_in_memory_table_versions(srvs: list[ServerInfo], logs: list[ScyllaLogFile], marks: list[int]):
    """
    Assumes that `logs` are log files of servers `srvs`, correspondingly in order.
    Assumes that `marks` are log markers (obtained by `ScyllaLogFile.mark()`) corresponding to `logs` in order.
    Assumes that an 'alter table ks.t ...' statement was performed after obtaining `marks`.
    Checks that every server printed the same version in `Altering ks.t...' log message.
    """
    logger.info("Verifying that in-memory table schema versions are in sync")
    matches = [await log.grep("Altering ks.t.*version=(.*)", from_mark=mark) for log, mark in zip(logs, marks)]

    def get_version(srv: ServerInfo, matches: list[tuple[str, re.Match[str]]]):
        if not matches:
            pytest.fail(f"Server {srv} didn't log 'Altering' message")
        _, match = matches[0]
        return UUID(match.group(1))

    versions = {srv: get_version(srv, m) for srv, m in zip(srvs, matches)}
    logger.info(f"In-memory table versions: {versions}")

    s1, v1 = next(iter(versions.items()))
    for s, v in versions.items():
        if v != v1:
            pytest.fail(f"{s1}'s in-memory table version {v1} is different than {s}'s version {v}")


@pytest.mark.asyncio
async def test_schema_versioning_with_recovery(manager: ManagerClient):
    """
    Perform schema changes while mixing nodes in RECOVERY mode with nodes in group 0 mode.
    Schema changes originating from RECOVERY node use digest-based schema versioning.
    Schema changes originating from group 0 nodes use persisted versions committed through group 0.

    Verify that schema versions are in sync after each schema change.
    """
    cfg = {'enable_user_defined_functions': False,
           'force_gossip_topology_changes': True}
    logger.info("Booting cluster")
    servers = [await manager.server_add(config=cfg) for _ in range(3)]
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logger.info("Creating keyspace and table")
    await cql.run_async("create keyspace ks with replication = "
                        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
    await verify_table_versions_synced(cql, hosts)
    await cql.run_async("create table ks.t (pk int primary key)")

    logger.info("Waiting for driver")
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    await verify_table_versions_synced(cql, hosts)
    ks_t_version = await get_scylla_tables_version(cql, hosts[0], 'ks', 't')
    assert ks_t_version

    logs = [await manager.server_open_log(srv.server_id) for srv in servers]
    marks = [await log.mark() for log in logs]

    logger.info("Altering table")
    await cql.run_async("alter table ks.t with comment = ''")

    await verify_table_versions_synced(cql, hosts)
    await verify_in_memory_table_versions(servers, logs, marks)

    new_ks_t_version = await get_scylla_tables_version(cql, hosts[0], 'ks', 't')
    assert new_ks_t_version
    assert new_ks_t_version != ks_t_version
    ks_t_version = new_ks_t_version

    # We still have a group 0 majority, don't do this at home.
    srv1 = servers[0]
    logger.info(f"Rebooting {srv1} in RECOVERY mode")
    h1 = next(h for h in hosts if h.address == srv1.ip_addr)
    await cql.run_async("update system.scylla_local set value = 'recovery' where key = 'group0_upgrade_state'", host=h1)
    await manager.server_restart(srv1.server_id)

    cql = await reconnect_driver(manager)
    logger.info(f"Waiting for driver")
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    await verify_table_versions_synced(cql, hosts)

    # We're doing a schema change on RECOVERY node while we have two nodes running in group 0 mode.
    # Don't do this at home.
    #
    # Now, the two nodes are not doing any schema changes right now, so this doesn't actually break anything:
    # the RECOVERY node is operating using the old schema change procedure, which means
    # that it pushes the schema mutations to other nodes directly with RPC, modifying
    # the group 0 state machine on other two nodes.
    #
    # There is one problem with this however. If the RECOVERY node considers some other node
    # as DOWN, it will silently *not* push the schema change, completing the operation
    # "successfully" nevertheless (it will return to the driver without error).
    # Usually in this case we rely on eventual convergence of schema through gossip,
    # which will not happen here, because the group 0 nodes are not doing schema pulls!
    # So we need to make sure that the RECOVERY node sees the other nodes as UP before
    # we perform the schema change, so it pushes the mutations to them.
    logger.info(f"Waiting until RECOVERY node ({srv1}) sees other servers as UP")
    await manager.server_sees_others(srv1.server_id, 2)

    marks = [await log.mark() for log in logs]
    logger.info(f"Altering table on RECOVERY node ({srv1})")
    await cql.run_async("alter table ks.t with comment = ''", host=h1)

    await verify_table_versions_synced(cql, hosts)
    await verify_in_memory_table_versions(servers, logs, marks)

    new_ks_t_version = await get_scylla_tables_version(cql, hosts[0], 'ks', 't')
    assert not new_ks_t_version
    ks_t_version = new_ks_t_version

    logger.info(f"Stopping {srv1} gracefully")
    await manager.server_stop_gracefully(srv1.server_id)

    srv2 = servers[1]
    logger.info(f"Waiting until {srv2} sees {srv1} as dead")
    await manager.server_not_sees_other_server(srv2.ip_addr, srv1.ip_addr)

    # Now we modify schema through group 0 while the RECOVERY node is dead.
    # Don't do this at home.
    marks = [await log.mark() for log in logs]
    h2 = next(h for h in hosts if h.address == srv2.ip_addr)
    logger.info(f"Altering table on group 0 node {srv2}")
    await cql.run_async("alter table ks.t with comment = ''", host=h2)

    await manager.server_start(srv1.server_id)
    cql = await reconnect_driver(manager)
    logger.info(f"Waiting for driver")
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logger.info(f"Waiting until {srv2} sees {srv1} as UP")
    await manager.server_sees_other_server(srv2.ip_addr, srv1.ip_addr)

    # The RECOVERY node will pull schema when it gets a write.
    # The other group 0 node will do a barrier so it will also sync schema before the write returns.
    logger.info("Forcing schema sync through CL=ALL INSERT")
    await cql.run_async(SimpleStatement("insert into ks.t (pk) values (0)", consistency_level=ConsistencyLevel.ALL),
                        host=h2)

    await verify_table_versions_synced(cql, hosts)
    await verify_in_memory_table_versions(servers, logs, marks)

    new_ks_t_version = await get_scylla_tables_version(cql, hosts[0], 'ks', 't')
    assert new_ks_t_version
    ks_t_version = new_ks_t_version

    srv3 = servers[2]
    h3 = next(h for h in hosts if h.address == srv3.ip_addr)
    logger.info("Finishing recovery")
    for h in [h2, h3]:
        await cql.run_async(
                "update system.scylla_local set value = 'recovery' where key = 'group0_upgrade_state'", host=h)
    await asyncio.gather(*(manager.server_restart(srv.server_id) for srv in [srv2, srv3]))

    cql = await reconnect_driver(manager)
    logger.info("Waiting for driver")
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    for h in [h1, h2, h3]:
        await delete_raft_data_and_upgrade_state(cql, h)

    logger.info("Restarting servers")
    await asyncio.gather(*(manager.server_restart(srv.server_id) for srv in servers))

    cql = await reconnect_driver(manager)
    logger.info("Waiting for driver")
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info(f"Waiting until upgrade finishes")
    for h in [h1, h2, h3]:
        await wait_until_upgrade_finishes(cql, h, time.time() + 60)

    await verify_table_versions_synced(cql, hosts)

    for change in [
            "alter table ks.t with comment = ''",
            "alter table ks.t add v int",
            "alter table ks.t alter v type blob"]:

        marks = [await log.mark() for log in logs]
        logger.info(f"Altering table with \"{change}\"")
        await cql.run_async(change)

        new_ks_t_version = await get_scylla_tables_version(cql, hosts[0], 'ks', 't')
        assert new_ks_t_version
        assert new_ks_t_version != ks_t_version
        ks_t_version = new_ks_t_version

        await verify_table_versions_synced(cql, hosts)
        await verify_in_memory_table_versions(servers, logs, marks)

    await cql.run_async("drop keyspace ks")

@pytest.mark.asyncio
async def test_upgrade(manager: ManagerClient):
    """
    While Raft is disabled, we use digest-based schema versioning.
    Once Raft upgrade is complete, we use persisted versions committed through group 0.
    """
    # Raft upgrade tests had to be replaced with recovery tests (scylladb/scylladb#16192)
    # as prerequisite for getting rid of `consistent_cluster_management` flag.
    # So we do the same here: start a cluster in Raft mode, then enter recovery
    # to simulate a non-Raft cluster.
    cfg = {'enable_user_defined_functions': False,
           'force_gossip_topology_changes': True}
    logger.info("Booting cluster")
    servers = [await manager.server_add(config=cfg) for _ in range(2)]
    cql = manager.get_cql()

    logging.info("Waiting until driver connects to every server")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info(f"Setting recovery state on {hosts} and restarting")
    await asyncio.gather(*(enter_recovery_state(cql, h) for h in hosts))
    await asyncio.gather(*(manager.server_restart(srv.server_id) for srv in servers))
    cql = await reconnect_driver(manager)

    logging.info("Waiting until driver connects to every server")
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logger.info("Creating keyspace and table")
    await cql.run_async("create keyspace ks with replication = "
                        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}")
    await verify_table_versions_synced(cql, hosts)
    await cql.run_async("create table ks.t (pk int primary key)")

    logging.info(f"Deleting Raft data and upgrade state on {hosts}")
    await asyncio.gather(*(delete_raft_data_and_upgrade_state(cql, h) for h in hosts))

    logging.info(f"Restarting {servers}")
    await asyncio.gather(*(manager.server_restart(srv.server_id) for srv in servers))
    cql = await reconnect_driver(manager)

    logger.info("Waiting for driver")
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info(f"Waiting until Raft upgrade procedure finishes")
    await asyncio.gather(*(wait_until_upgrade_finishes(cql, h, time.time() + 60) for h in hosts))

    logs = [await manager.server_open_log(srv.server_id) for srv in servers]

    marks = [await log.mark() for log in logs]
    logger.info("Altering table")
    await cql.run_async("alter table ks.t with comment = ''")

    await verify_table_versions_synced(cql, hosts)
    await verify_in_memory_table_versions(servers, logs, marks)

    # `group0_schema_version` should be present
    # and the version column for `ks.t` should be non-null.
    for h in hosts:
        logger.info(f"Checking that `group0_schema_version` is set on {h}")
        assert (await get_group0_schema_version(cql, h)) is not None

    for h in hosts:
        logger.info(f"Checking that `version` column for `ks.t` is set on {h}")
        versions = await get_scylla_tables_versions(cql, h)
        for ks, _, v in versions:
            if ks == "ks":
                assert v is not None
