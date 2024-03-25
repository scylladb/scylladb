#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test consistency of schema changes with topology changes.
"""
import asyncio
import logging
import functools
import operator
import pytest
import time
from cassandra.cluster import ConnectionException, ConsistencyLevel, NoHostAvailable, Session  # type: ignore # pylint: disable=no-name-in-module
from cassandra.pool import Host                          # type: ignore # pylint: disable=no-name-in-module
from cassandra.util import datetime_from_uuid1           # type: ignore # pylint: disable=no-name-in-module
from test.pylib.internal_types import ServerInfo, HostID
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts, read_barrier, get_available_host, unique_name


logger = logging.getLogger(__name__)


async def reconnect_driver(manager: ManagerClient) -> Session:
    """Can be used as a workaround for scylladb/python-driver#295.

       When restarting a node, a pre-existing session connected to the cluster
       may reconnect to the restarted node multiple times. Even if we verify
       that the session can perform a query on that node (e.g. like `wait_for_cql`,
       which tries to select from system.local), the driver may again reconnect
       after that, and following queries may fail.

       The new session created by this function *should* not have this problem,
       although (if I remember correctly) there is no 100% guarantee; still,
       the chance of this problem appearing should be significantly decreased
       with the new session.
    """
    logging.info(f"Reconnecting driver")
    manager.driver_close()
    await manager.driver_connect()
    logging.info(f"Driver reconnected")
    cql = manager.cql
    assert(cql)
    return cql


async def get_token_ring_host_ids(manager: ManagerClient, srv: ServerInfo) -> set[str]:
    """Get the host IDs of normal token owners known by `srv`."""
    token_endpoint_map = await manager.api.client.get_json("/storage_service/tokens_endpoint", srv.ip_addr)
    normal_endpoints = {e["value"] for e in token_endpoint_map}
    logger.info(f"Normal endpoints' IPs by {srv}: {normal_endpoints}")
    host_id_map = await manager.api.client.get_json('/storage_service/host_id', srv.ip_addr)
    all_host_ids = {e["value"] for e in host_id_map}
    logger.info(f"All host IDs by {srv}: {all_host_ids}")
    normal_host_ids = {e["value"] for e in host_id_map if e["key"] in normal_endpoints}
    logger.info(f"Normal endpoints' host IDs by {srv}: {normal_host_ids}")
    return normal_host_ids


async def get_current_group0_config(manager: ManagerClient, srv: ServerInfo) -> set[tuple[str, bool]]:
    """Get the current Raft group 0 configuration known by `srv`.
       The first element of each tuple is the Raft ID of the node (which is equal to the Host ID),
       the second element indicates whether the node is a voter.
     """
    assert manager.cql
    host = (await wait_for_cql_and_get_hosts(manager.cql, [srv], time.time() + 60))[0]
    await read_barrier(manager.cql, host)
    group0_id = (await manager.cql.run_async(
        "select value from system.scylla_local where key = 'raft_group0_id'",
        host=host))[0].value
    config = await manager.cql.run_async(
        f"select server_id, can_vote from system.raft_state where group_id = {group0_id} and disposition = 'CURRENT'",
        host=host)
    result = {(str(m.server_id), bool(m.can_vote)) for m in config}
    logger.info(f"Group 0 members by {srv}: {result}")
    return result


async def get_topology_coordinator(manager: ManagerClient) -> HostID:
    """Get the host ID of the topology coordinator."""
    host = await get_available_host(manager.cql, time.time() + 60)
    await read_barrier(manager.cql, host)
    return await manager.api.get_raft_leader(host.address)


async def check_token_ring_and_group0_consistency(manager: ManagerClient) -> None:
    """Ensure that the normal token owners and group 0 members match
       according to each currently running server.
    """
    servers = await manager.running_servers()
    for srv in servers:
        group0_members = await get_current_group0_config(manager, srv)
        group0_ids = {m[0] for m in group0_members}
        token_ring_ids = await get_token_ring_host_ids(manager, srv)
        assert token_ring_ids == group0_ids


async def wait_for_token_ring_and_group0_consistency(manager: ManagerClient, deadline: float) -> None:
    """Weaker version of the above check; the token ring is not immediately updated after
    bootstrap/replace/decommission - the normal tokens of the new node propagate through gossip.
    Take this into account and wait for the equality condition to hold, with a timeout.
    """
    servers = await manager.running_servers()
    for srv in servers:
        group0_members = await get_current_group0_config(manager, srv)
        group0_ids = {m[0] for m in group0_members}
        async def token_ring_matches():
            token_ring_ids = await get_token_ring_host_ids(manager, srv)
            diff = token_ring_ids ^ group0_ids
            if diff:
                logger.warning(f"Group 0 members and token ring members don't yet match" \
                               f" according to {srv}, symmetric difference: {diff}")
                return None
            return True
        await wait_for(token_ring_matches, deadline, period=.5)


async def restart(manager: ManagerClient, server: ServerInfo) -> None:
    logging.info(f"Stopping {server} gracefully")
    await manager.server_stop_gracefully(server.server_id)
    logging.info(f"Restarting {server}")
    await manager.server_start(server.server_id)
    logging.info(f"{server} restarted")


async def wait_for_upgrade_state(state: str, cql: Session, host: Host, deadline: float) -> None:
    """Wait until group 0 upgrade state reaches `state` on `host`, using `cql` to query it.  Warning: if the
       upgrade procedure may progress beyond `state` this function may not notice when it entered `state` and
       then time out.  Use it only if either `state` is the last state or the conditions of the test don't allow
       the upgrade procedure to progress beyond `state` (e.g. a dead node causing the procedure to be stuck).
    """
    async def reached_state():
        rs = await cql.run_async("select value from system.scylla_local where key = 'group0_upgrade_state'", host=host)
        if rs:
            value = rs[0].value
            if value == state:
                return True
            else:
                logging.info(f"Upgrade not yet in state {state} on server {host}, state: {value}")
        else:
            logging.info(f"Upgrade not yet in state {state} on server {host}, no state was written")
        return None
    await wait_for(reached_state, deadline)


async def wait_until_upgrade_finishes(cql: Session, host: Host, deadline: float) -> None:
    await wait_for_upgrade_state('use_post_raft_procedures', cql, host, deadline)


async def enter_recovery_state(cql: Session, host: Host) -> None:
    await cql.run_async(
            "update system.scylla_local set value = 'recovery' where key = 'group0_upgrade_state'",
            host=host)


async def delete_raft_data(cql: Session, host: Host) -> None:
    await cql.run_async("truncate table system.discovery", host=host)
    await cql.run_async("truncate table system.group0_history", host=host)
    await cql.run_async("delete value from system.scylla_local where key = 'raft_group0_id'", host=host)


async def delete_upgrade_state(cql: Session, host: Host) -> None:
    await cql.run_async("delete from system.scylla_local where key = 'group0_upgrade_state'", host=host)


async def delete_raft_data_and_upgrade_state(cql: Session, host: Host) -> None:
    await delete_raft_data(cql, host)
    await delete_upgrade_state(cql, host)


async def wait_until_topology_upgrade_finishes(manager: ManagerClient, ip_addr: str, deadline: float):
    async def check():
        status = await manager.api.raft_topology_upgrade_status(ip_addr)
        return status == "done" or None
    await wait_for(check, deadline=deadline, period=1.0)


async def delete_raft_topology_state(cql: Session, host: Host):
    await cql.run_async("truncate table system.topology", host=host)


async def wait_for_cdc_generations_publishing(cql: Session, hosts: list[Host], deadline: float):
    for host in hosts:
        async def all_generations_published():
            topo_res = await cql.run_async("SELECT unpublished_cdc_generations FROM system.topology", host=host)
            assert len(topo_res) != 0
            unpublished_generations = topo_res[0].unpublished_cdc_generations
            return unpublished_generations is None or len(unpublished_generations) == 0 or None

        await wait_for(all_generations_published, deadline=deadline, period=1.0)


async def check_system_topology_and_cdc_generations_v3_consistency(manager: ManagerClient, hosts: list[Host]):
    assert len(hosts) != 0

    topo_results = await asyncio.gather(*(manager.cql.run_async("SELECT * FROM system.topology", host=host) for host in hosts))

    for host, topo_res in zip(hosts, topo_results):
        logging.info(f"Dumping the state of system.topology as seen by {host}:")
        for row in topo_res:
            logging.info(f"  {row}")

    for host, topo_res in zip(hosts, topo_results):
        assert len(topo_res) != 0

        for row in topo_res:
            assert row.host_id is not None
            assert row.datacenter is not None
            assert row.ignore_msb is not None
            assert row.node_state == "normal"
            assert row.num_tokens is not None
            assert row.rack is not None
            assert row.release_version is not None
            assert row.supported_features is not None
            assert row.shard_count is not None
            assert row.tokens is not None

            assert len(row.tokens) == row.num_tokens

        assert topo_res[0].committed_cdc_generations is not None
        committed_generations = frozenset(gen[1] for gen in topo_res[0].committed_cdc_generations)

        assert topo_res[0].fence_version is not None
        assert topo_res[0].upgrade_state == "done"

        assert host.host_id in (row.host_id for row in topo_res)

        computed_enabled_features = functools.reduce(operator.and_, (frozenset(row.supported_features) for row in topo_res))
        assert topo_res[0].enabled_features is not None
        enabled_features = frozenset(topo_res[0].enabled_features)
        assert enabled_features == computed_enabled_features
        assert "SUPPORTS_CONSISTENT_TOPOLOGY_CHANGES" in enabled_features

        cdc_res = await manager.cql.run_async("SELECT * FROM system.cdc_generations_v3", host=host)
        assert len(cdc_res) != 0

        all_generations = frozenset(row.id for row in cdc_res)
        assert committed_generations.issubset(all_generations)

        # Check that the contents fetched from the current host are the same as for other nodes
        assert topo_results[0] == topo_res

async def start_writes_to_cdc_table(cql: Session, concurrency: int = 3):
    logger.info(f"Starting to asynchronously write, concurrency = {concurrency}")

    stop_event = asyncio.Event()

    ks_name = unique_name()
    await cql.run_async(f"CREATE KEYSPACE {ks_name} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} AND tablets = {{ 'enabled': false }}")
    await cql.run_async(f"CREATE TABLE {ks_name}.tbl (pk int PRIMARY KEY, v int) WITH cdc = {{'enabled':true}}")

    stmt = cql.prepare(f"INSERT INTO {ks_name}.tbl (pk, v) VALUES (?, 0)")
    stmt.consistency_level = ConsistencyLevel.ONE

    async def do_writes():
        iteration = 0
        while not stop_event.is_set():
            start_time = time.time()
            try:
                await cql.run_async(stmt, [iteration])
            except NoHostAvailable as e:
                for _, err in e.errors.items():
                    # ConnectionException can be raised when the node is shutting down.
                    if not isinstance(err, ConnectionException):
                        logger.error(f"Write started {time.time() - start_time}s ago failed: {e}")
                        raise
            except Exception as e:
                logger.error(f"Write started {time.time() - start_time}s ago failed: {e}")
                raise
            iteration += 1
            await asyncio.sleep(0.01)

    tasks = [asyncio.create_task(do_writes()) for _ in range(concurrency)]

    async def verify():
        generations = await cql.run_async("SELECT * FROM system_distributed.cdc_streams_descriptions_v2")

        stream_to_timestamp = { stream: gen.time for gen in generations for stream in gen.streams}

        cdc_log = await cql.run_async(f"SELECT * FROM {ks_name}.tbl_scylla_cdc_log")
        for log_entry in cdc_log:
            assert log_entry.cdc_stream_id in stream_to_timestamp
            timestamp = stream_to_timestamp[log_entry.cdc_stream_id]
            assert timestamp <= datetime_from_uuid1(log_entry.cdc_time)

    async def finish_and_verify():
        logger.info("Stopping write workers")
        stop_event.set()
        await asyncio.gather(*tasks)
        await verify()

    return finish_and_verify

def log_run_time(f):
    @functools.wraps(f)
    async def wrapped(*args, **kwargs):
        start = time.time()
        res = await f(*args, **kwargs)
        logging.info(f"{f.__name__} took {int(time.time() - start)} seconds.")
        return res
    return wrapped

async def trigger_snapshot(manager, server: ServerInfo) -> None:
    cql = manager.get_cql()
    group0_id = (await cql.run_async(
        "select value from system.scylla_local where key = 'raft_group0_id'"))[0].value

    host = cql.cluster.metadata.get_host(server.ip_addr)
    await manager.api.client.post(f"/raft/trigger_snapshot/{group0_id}", host=server.ip_addr)

