#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test consistency of schema changes with topology changes.
"""
import asyncio
import datetime
import logging
import functools
import operator
import time
import re

from cassandra.cluster import ConnectionException, ConsistencyLevel, NoHostAvailable, Session, SimpleStatement  # type: ignore # pylint: disable=no-name-in-module
from cassandra.pool import Host                          # type: ignore # pylint: disable=no-name-in-module
from cassandra.util import datetime_from_uuid1           # type: ignore # pylint: disable=no-name-in-module
from test.pylib.internal_types import ServerInfo, HostID
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import get_host_api_address, read_barrier
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts, get_available_host, unique_name
from contextlib import asynccontextmanager
from typing import Optional


logger = logging.getLogger(__name__)

UUID_REGEX = re.compile(r"([0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12})")


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
    await read_barrier(manager.api, srv.ip_addr)
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
    host_address = get_host_api_address(host)
    await read_barrier(manager.api, host_address)
    return await manager.api.get_raft_leader(host_address)


async def check_token_ring_and_group0_consistency(manager: ManagerClient) -> None:
    """Ensure that the normal token owners and group 0 members match
       according to each currently running server.

       Note that the normal token owners and group 0 members never match
       in the presence of zero-token nodes.
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

async def wait_until_last_generation_is_in_use(cql: Session):
    topo_res = await cql.run_async("SELECT committed_cdc_generations FROM system.topology")
    assert len(topo_res) != 0
    generations = topo_res[0].committed_cdc_generations
    last_generation_ts = max(gen[0] for gen in generations)

    # datetime objects returned by the driver are timezone-naive and we assume they are in UTC.
    # To subtract timestamp, we need to make sure they are both timezone-aware (or both are timezone-naive).
    last_generation_ts = last_generation_ts.replace(tzinfo=datetime.timezone.utc)
    now = datetime.datetime.now(datetime.timezone.utc)
    seconds = (last_generation_ts - now).total_seconds()
    if seconds > 0:
        logger.info(f"Waiting {seconds} seconds for the last generation to be in use.")
        await asyncio.sleep(seconds)
    else:
        logger.info(f"The last generation is already in use.")

async def check_system_topology_and_cdc_generations_v3_consistency(manager: ManagerClient, hosts: list[Host], cqls: Optional[list[Session]] = None):
    # The cqls parameter is a temporary workaround for testing the recovery mode in the presence of live zero-token
    # nodes. A zero-token node requires a different cql session not to be ignored by the driver because of empty tokens
    # in the system.peers table.
    assert len(hosts) != 0

    if cqls is None:
        cqls = [manager.cql] * len(hosts)

    topo_results = await asyncio.gather(*(cql.run_async("SELECT * FROM system.topology", host=host) for cql, host in zip(cqls, hosts)))

    for host, topo_res in zip(hosts, topo_results):
        logging.info(f"Dumping the state of system.topology as seen by {host}:")
        for row in topo_res:
            logging.info(f"  {row}")

    for cql, host, topo_res in zip(cqls, hosts, topo_results):
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

            assert (0 if row.tokens is None else len(row.tokens)) == row.num_tokens

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

        cdc_res = await cql.run_async("SELECT * FROM system.cdc_generations_v3", host=host)
        assert len(cdc_res) != 0

        all_generations = frozenset(row.id for row in cdc_res)
        assert committed_generations.issubset(all_generations)

        # Check that the contents fetched from the current host are the same as for other nodes
        assert topo_results[0] == topo_res

async def check_node_log_for_failed_mutations(manager: ManagerClient, server: ServerInfo):
    logging.info(f"Checking that node {server} had no failed mutations")
    log = await manager.server_open_log(server.server_id)
    occurrences = await log.grep(expr="Failed to apply mutation from")
    assert len(occurrences) == 0


async def start_writes(cql: Session, rf: int, cl: ConsistencyLevel, concurrency: int = 3):
    logging.info(f"Starting to asynchronously write, concurrency = {concurrency}")

    stop_event = asyncio.Event()

    ks_name = unique_name()
    await cql.run_async(f"CREATE KEYSPACE {ks_name} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}")
    await cql.run_async(f"USE {ks_name}")
    await cql.run_async(f"CREATE TABLE tbl (pk int PRIMARY KEY, v int)")

    # In the test we only care about whether operations report success or not
    # and whether they trigger errors in the nodes' logs. Inserting the same
    # value repeatedly is enough for our purposes.
    stmt = SimpleStatement("INSERT INTO tbl (pk, v) VALUES (0, 0)", consistency_level=cl)

    async def do_writes(worker_id: int):
        write_count = 0
        while not stop_event.is_set():
            start_time = time.time()
            try:
                await cql.run_async(stmt)
                write_count += 1
            except Exception as e:
                logging.error(f"Write started {time.time() - start_time}s ago failed: {e}")
                raise
        logging.info(f"Worker #{worker_id} did {write_count} successful writes")

    tasks = [asyncio.create_task(do_writes(worker_id)) for worker_id in range(concurrency)]

    async def finish():
        logging.info("Stopping write workers")
        stop_event.set()
        await asyncio.gather(*tasks)

    return finish

async def start_writes_to_cdc_table(cql: Session, concurrency: int = 3):
    logger.info(f"Starting to asynchronously write, concurrency = {concurrency}")

    stop_event = asyncio.Event()

    ks_name = unique_name()
    await cql.run_async(f"CREATE KEYSPACE {ks_name} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}} AND tablets = {{ 'enabled': false }}")
    await cql.run_async(f"CREATE TABLE {ks_name}.tbl (pk int PRIMARY KEY, v int) WITH cdc = {{'enabled':true}}")

    stmt = cql.prepare(f"INSERT INTO {ks_name}.tbl (pk, v) VALUES (?, 0)")
    # FIXME: this function is used by tests that use clusters with at least 3 nodes and restart nodes sequentially.
    # Therefore, RF=3 and CL=2 should work, but they don't. Some writes fail because CL=2 is not satisfied.
    # We should investigate why it happens and increase CL to 2 if possible.
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

    def restart(new_cql: Session):
        nonlocal cql
        nonlocal tasks
        logger.info("Restarting write workers")
        assert stop_event.is_set()
        stop_event.clear()
        cql = new_cql
        tasks = [asyncio.create_task(do_writes()) for _ in range(concurrency)]

    async def verify():
        generations = await cql.run_async("SELECT * FROM system_distributed.cdc_streams_descriptions_v2")

        stream_to_timestamp = { stream: gen.time for gen in generations for stream in gen.streams}

        # FIXME: Doesn't work with all_pages=True (https://github.com/scylladb/scylladb/issues/19101)
        cdc_log = await cql.run_async(f"SELECT * FROM {ks_name}.tbl_scylla_cdc_log", all_pages=False)
        for log_entry in cdc_log:
            assert log_entry.cdc_stream_id in stream_to_timestamp
            timestamp = stream_to_timestamp[log_entry.cdc_stream_id]
            assert timestamp <= datetime_from_uuid1(log_entry.cdc_time)

    async def stop_and_verify():
        logger.info("Stopping write workers")
        stop_event.set()
        await asyncio.gather(*tasks)
        await verify()

    return restart, stop_and_verify

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



async def get_coordinator_host_ids(manager: ManagerClient) -> list[str]:
    """ Get coordinator host id from history

    Select all records with elected coordinator
    from description column in system.group0_history table and 
    return list of coordinator host ids, where
    first element in list is active coordinator
    """
    stm = SimpleStatement("select description from system.group0_history "
                          "where key = 'history' and description LIKE 'Starting new topology coordinator%' ALLOW FILTERING;")

    cql = manager.get_cql()
    result = await cql.run_async(stm)
    coordinators_ids = []
    for row in result:
        coordinator_host_id = get_uuid_from_str(row.description)
        if coordinator_host_id:
            coordinators_ids.append(coordinator_host_id)
    assert len(coordinators_ids) > 0, f"No coordinator ids {coordinators_ids} were found"
    return coordinators_ids


async def get_coordinator_host(manager: ManagerClient) -> ServerInfo:
    """Get coordinator ServerInfo"""

    coordinator_host_id = (await get_coordinator_host_ids(manager))[0]
    server_id_maps = {await manager.get_host_id(srv.server_id):srv for srv in await manager.running_servers()}
    coordinator_host = server_id_maps.get(coordinator_host_id, None)
    assert coordinator_host, \
        f"Node with host id {coordinator_host_id} was not found in cluster host ids {list(server_id_maps.keys())}"
    return coordinator_host


def get_uuid_from_str(string: str) -> str:
    """Search uuid in string"""
    uuid = ""
    if match := UUID_REGEX.search(string):
        uuid = match.group(1)
    return uuid


async def wait_new_coordinator_elected(manager: ManagerClient, expected_num_of_elections: int, deadline: float) -> None:
    """Wait new coordinator to be elected

    Wait while the table 'system.group0_history' will have a number of lines 
    with the 'new topology coordinator' equal to the expected_num_of_elections number,
    and the latest host_id coordinator differs from the previous one.
    """
    async def new_coordinator_elected():
        coordinators_ids = await get_coordinator_host_ids(manager)
        if len(coordinators_ids) == expected_num_of_elections \
            and coordinators_ids[0] != coordinators_ids[1]:
            return True
        logger.warning("New coordinator was not elected %s", coordinators_ids)

    await wait_for(new_coordinator_elected, deadline=deadline)

@asynccontextmanager
async def new_test_keyspace(cql, opts):
    """
    A utility function for creating a new temporary keyspace with given
    options. It can be used in a "async with", as:
        async with new_test_keyspace(cql, '...') as keyspace:
    """
    keyspace = unique_name()
    await cql.run_async("CREATE KEYSPACE " + keyspace + " " + opts)
    try:
        yield keyspace
    finally:
        await cql.run_async("DROP KEYSPACE " + keyspace)

previously_used_table_names = []
@asynccontextmanager
async def new_test_table(cql, keyspace, schema, extra=""):
    """
    A utility function for creating a new temporary table with a given schema.
    Because Scylla becomes slower when a huge number of uniquely-named tables
    are created and deleted (see https://github.com/scylladb/scylla/issues/7620)
    we keep here a list of previously used but now deleted table names, and
    reuse one of these names when possible.
    This function can be used in a "async with", as:
       async with create_table(cql, test_keyspace, '...') as table:
    """
    global previously_used_table_names
    if not previously_used_table_names:
        previously_used_table_names.append(unique_name())
    table_name = previously_used_table_names.pop()
    table = keyspace + "." + table_name
    await cql.run_async("CREATE TABLE " + table + "(" + schema + ")" + extra)
    try:
        yield table
    finally:
        await cql.run_async("DROP TABLE " + table)
        previously_used_table_names.append(table_name)

@asynccontextmanager
async def new_materialized_view(cql, table, select, pk, where, extra=""):
    """
    A utility function for creating a new temporary materialized view in
    an existing table.
    """
    keyspace = table.split('.')[0]
    mv = keyspace + "." + unique_name()
    await cql.run_async(f"CREATE MATERIALIZED VIEW {mv} AS SELECT {select} FROM {table} WHERE {where} PRIMARY KEY ({pk}) {extra}")
    try:
        yield mv
    finally:
        await cql.run_async(f"DROP MATERIALIZED VIEW {mv}")


async def get_raft_log_size(cql, host) -> int:
    query = "select count(\"index\") from system.raft"
    return (await cql.run_async(query, host=host))[0][0]


async def get_raft_snap_id(cql, host) -> str:
    query = "select snapshot_id from system.raft limit 1"
    return (await cql.run_async(query, host=host))[0].snapshot_id
