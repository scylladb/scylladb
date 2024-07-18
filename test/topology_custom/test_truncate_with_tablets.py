#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from cassandra.query import SimpleStatement, ConsistencyLevel
from cassandra.protocol import InvalidRequest
from test.pylib.manager_client import ManagerClient, ScyllaLogFile
from test.pylib.tablets import get_all_tablet_replicas
from test.pylib.rest_client import inject_error, InjectionHandler
from test.pylib.internal_types import ServerInfo
from test.topology.util import get_topology_coordinator, wait_for_cql_and_get_hosts
from test.topology.conftest import skip_mode
import time
import pytest
import logging
import asyncio
from re import escape

logger = logging.getLogger(__name__)

async def create_cluster_and_data(manager: ManagerClient, num_records: int):
    logger.info('Bootstrapping cluster')
    cfg = {'enable_user_defined_functions': False, 'enable_tablets': True}
    servers = []
    host_ids = {}

    for i in range(3):
        s = await manager.server_add(config=cfg, cmdline=['--logger-log-level', 'storage_proxy=debug'])
        servers.append(s)
        hid = await manager.get_host_id(s.server_id)
        host_ids[hid] = i

    cql = manager.get_cql()
    cql_hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 30)
    # Make sure we are getting the same number and order of nodes from cql as we already have in servers and host_ids
    assert len(cql_hosts) == len(servers)
    for (ch, s) in zip(cql_hosts, servers):
        assert ch.address.startswith(s.ip_addr)  # CQL address has a port number, s.ip_addr does not

    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 1}")
    await cql.run_async('CREATE TABLE test.test (pk int PRIMARY KEY, c int);')
    keys = range(num_records)
    await asyncio.gather(*[cql.run_async(f'INSERT INTO test.test (pk, c) VALUES ({k}, {k});') for k in keys])

    row = await cql.run_async('SELECT COUNT(*) FROM test.test')
    assert row[0].count == num_records

    return (servers, host_ids, cql_hosts)


# This function searches a log file for the occurence of a given log entry inbetween two other entries.
# entry, begin, end: search strings to match the log lines with: RegEx on search strings is NOT allowed!
# Returns True if ALL matches of entry are found ONLY between matches of begin and end
# This function will fail the calling test if:
#   - There are nested blocks of begin and end entries
#   - There are matches of end without corresponding begin
#   - A block opened with a begin match was not closed with and end match
#   - The entry was not found either in or out of the begin end block
async def is_log_entry_between(log: ScyllaLogFile, entry: str, begin: str, end: str) -> bool:

    parser_in_between = False
    has_matches_outside = False
    has_matches_between = False
    opening_line = ''
    pattern = '(' + escape(entry) + '|' + escape(begin) + '|' + escape(end) + ')'
    logger.debug(f'Searching for pattern:\n{pattern}')
    for l, m in await log.grep(pattern):
        match_str = m.group(0)
        line = l.strip()
        if match_str == begin:
            assert not parser_in_between, f'Nested search blocks not allowed on `{line}`'
            logger.debug(f'match begin: `{begin}` with: `{line}`')
            parser_in_between = True
            opening_line = line
        elif match_str == end:
            assert parser_in_between, f'End search block without begin on `{line}`'
            logger.debug(f'match end: `{end}` with: `{line}`')
            parser_in_between = False
        elif match_str == entry:
            if parser_in_between:
                logger.debug(f'match entry: `{entry}` with: `{line}`')
                has_matches_between = True
            else:
                logger.debug(f'match outside block: `{entry}` with: `{line}`')
                has_matches_outside = True

    assert not parser_in_between, f'Block remained open at log file end. Opening line: `{opening_line}`'
    assert has_matches_between or has_matches_outside, f'Searched log entry not found: `{entry}`'

    return has_matches_between and not has_matches_outside


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.parametrize('truncate_and_raft_leader_are_same_node', [True, False])
@pytest.mark.parametrize('concurrency_path', ['truncate_starts_first', 'tablet_migration_starts_first'])
async def test_truncate_with_tablets_while_migration(manager: ManagerClient, truncate_and_raft_leader_are_same_node: bool, concurrency_path: str):

    '''
    Truncate and tablet migrations must not happen concurrently. Whichever started first must complete before the other can start.
    The following cases are tested:
        - Tablet migration is already running, then a truncate is attempted
        - Truncate is already running, then a tablet migration is attempted

    These are also tested with the raft leader and the truncate coordinator being on the same, and on separate nodes.

    Truncate is waiting by asking storage_service for an effective replication map pointer.
    While this pointer is alive on any node, new migrations will not start.
    '''

    (servers, host_ids, cql_hosts) = await create_cluster_and_data(manager, num_records=1000)

    cql = manager.get_cql()

    # Find raft leader node, and select a truncate coordinator node
    raft_leader_host_id = await get_topology_coordinator(manager)
    raft_leader_index = host_ids[raft_leader_host_id]
    if truncate_and_raft_leader_are_same_node:
        trunc_coord_index = raft_leader_index
    else:
        trunc_coord_index = 0 if raft_leader_index == 1 else 1
    raft_leader = servers[raft_leader_index]
    raft_leader_log = await manager.server_open_log(raft_leader.server_id)
    trunc_coord = servers[trunc_coord_index]
    trunc_coord_cql_host = cql_hosts[trunc_coord_index]
    trunc_coord_log = await manager.server_open_log(trunc_coord.server_id)
    logger.info(f'Raft leader is {raft_leader_index} host_id {raft_leader}')
    logger.info(f'Truncate coordinator is {trunc_coord_index} host_id {trunc_coord}')

    # Select source and destination for the moving tablet
    table_id = await manager.get_table_id('test', 'test')
    tablet_replicas = await get_all_tablet_replicas(manager, raft_leader, 'test', 'test')
    logger.info(f'Tablet is on [{tablet_replicas}]')
    assert len(tablet_replicas) == 1 and len(tablet_replicas[0].replicas) == 2
    src_replica = tablet_replicas[0].replicas[0]
    hosts_with_replicas = [ r[0] for r in tablet_replicas[0].replicas ]
    dest_set = set(host_ids.keys()).difference(set(hosts_with_replicas))
    assert len(dest_set) == 1, 'Number of nodes without replica must be 1'
    dest_replica = (tuple(dest_set)[0], 0)
    logger.info(f'Moving tablet from {src_replica[0]}:{src_replica[1]} to {dest_replica[0]}:{dest_replica[1]}')

    if concurrency_path == 'tablet_migration_starts_first':
        async with inject_error(manager.api, raft_leader.ip_addr, 'tablet_transition_updates') as handler:
            await manager.api.enable_injection(trunc_coord.ip_addr, 'truncate_with_tablets_wait_before', one_shot=True)
            move_task = asyncio.create_task(manager.api.move_tablet(raft_leader.ip_addr, 'test', 'test', src_replica[0], src_replica[1], dest_replica[0], dest_replica[1], 0))
            await raft_leader_log.wait_for('tablet_transition_updates: start')
            trunc_future = cql.run_async('TRUNCATE TABLE test.test', host=trunc_coord_cql_host)
            await trunc_coord_log.wait_for('truncate_with_tablets_wait_before: start')
            await handler.message()
            await manager.api.message_injection(trunc_coord.ip_addr, 'truncate_with_tablets_wait_before')
            await asyncio.gather(move_task, trunc_future)

        entry = 'Truncating test.test'
        begin = f'Initiating tablet streaming (migration) of {table_id}:0 to {dest_replica[0]}:{dest_replica[1]}'
        end = f'streaming for tablet {table_id}:0 resolved'
        assert not await is_log_entry_between(raft_leader_log, entry, begin, end)

        entry = 'Truncated test.test'
        assert not await is_log_entry_between(raft_leader_log, entry, begin, end)

    elif concurrency_path == 'truncate_starts_first':
        async with inject_error(manager.api, raft_leader.ip_addr, 'tablet_transition_updates') as handler:
            await manager.api.enable_injection(trunc_coord.ip_addr, 'truncate_with_tablets_wait_holding_erm', one_shot=True)
            trunc_future = cql.run_async('TRUNCATE TABLE test.test', host=trunc_coord_cql_host)
            await trunc_coord_log.wait_for('truncate_with_tablets_wait_holding_erm: start')
            move_task = asyncio.create_task(manager.api.move_tablet(raft_leader.ip_addr, 'test', 'test', src_replica[0], src_replica[1], dest_replica[0], dest_replica[1], 0))
            await handler.message()
            await manager.api.message_injection(trunc_coord.ip_addr, 'truncate_with_tablets_wait_holding_erm')
            await asyncio.gather(move_task, trunc_future)

        entry = f'Initiating tablet streaming (migration) of {table_id}:0 to {dest_replica[0]}:{dest_replica[1]}'
        begin = 'Truncating test.test'
        end = 'Truncated test.test'
        assert not await is_log_entry_between(raft_leader_log, entry, begin, end)

        entry = f'streaming for tablet {table_id}:0 resolved'
        assert not await is_log_entry_between(raft_leader_log, entry, begin, end)

    row = await cql.run_async(SimpleStatement('SELECT COUNT(*) FROM test.test', consistency_level=ConsistencyLevel.ALL))
    assert row[0].count == 0


@pytest.mark.asyncio
async def test_truncate_mv_fails(manager: ManagerClient):

    num_records = 100
    (servers, host_ids, cql_hosts) = await create_cluster_and_data(manager, num_records)

    cql = manager.get_cql()

    await cql.run_async('CREATE MATERIALIZED VIEW test.test_mv AS SELECT * FROM test.test PRIMARY KEY (pk)')
    with pytest.raises(InvalidRequest, match='Cannot TRUNCATE materialized view directly; must truncate base table instead'):
        await cql.run_async('TRUNCATE TABLE test.test_mv')

    row = await cql.run_async(SimpleStatement('SELECT COUNT(*) FROM test.test', consistency_level=ConsistencyLevel.ALL))
    assert row[0].count == num_records


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.parametrize('trunctate_coordinator_node', ['decommissioned', 'raft_leader', 'tablet_destination'])
async def test_truncate_with_tablets_while_decommission(manager: ManagerClient, trunctate_coordinator_node: str):

    (servers, host_ids, cql_hosts) = await create_cluster_and_data(manager, num_records=1000)

    # Select node to decommission and the node where the tablet will be migrated to
    raft_leader_host_id = await get_topology_coordinator(manager)
    raft_leader = servers[host_ids[raft_leader_host_id]]
    tablet_replicas = await get_all_tablet_replicas(manager, raft_leader, 'test', 'test')
    logger.info(f'Tablet is on [{tablet_replicas}]')
    assert len(tablet_replicas) == 1 and len(tablet_replicas[0].replicas) == 2
    hosts_with_replicas = [ r[0] for r in tablet_replicas[0].replicas ]
    decom_host_id = hosts_with_replicas[0] if raft_leader_host_id == hosts_with_replicas[1] else hosts_with_replicas[1]
    dest_set = set(host_ids.keys()).difference(set(hosts_with_replicas))
    assert len(dest_set) == 1, 'Number of nodes without replica must be 1'
    dest_host_id = tuple(dest_set)[0]

    # Select a truncate coordinator node
    if trunctate_coordinator_node == 'decommissioned':
        trunc_coord_index = host_ids[decom_host_id]
    elif trunctate_coordinator_node == 'raft_leader':
        trunc_coord_index = host_ids[raft_leader_host_id]
    elif trunctate_coordinator_node == 'tablet_destination':
        trunc_coord_index = host_ids[dest_host_id]

    trunc_coord_cql_host_id = cql_hosts[trunc_coord_index]
    trunc_coord = servers[trunc_coord_index]
    trunc_coord_log = await manager.server_open_log(trunc_coord.server_id)

    logger.debug(f'host_ids: {host_ids}')
    logger.debug(f'raft_leader_host_id: {raft_leader_host_id}')
    logger.debug(f'decom_host_id: {decom_host_id}')
    logger.debug(f'dest_host_id: {dest_host_id}')
    logger.debug(f'trunc_coord_cql_host_id: {trunc_coord_cql_host_id}')

    cql = manager.get_cql()

    await manager.api.enable_injection(trunc_coord.ip_addr, 'truncate_with_tablets_wait_holding_erm', one_shot=True)
    trunc_future = cql.run_async('TRUNCATE TABLE test.test', host=trunc_coord_cql_host_id)
    await trunc_coord_log.wait_for('truncate_with_tablets_wait_holding_erm: start')
    decom_task = asyncio.create_task(manager.decommission_node(servers[host_ids[decom_host_id]].server_id))
    await manager.api.message_injection(trunc_coord.ip_addr, 'truncate_with_tablets_wait_holding_erm')
    await asyncio.gather(decom_task, trunc_future)

    raft_leader_log = await manager.server_open_log(raft_leader.server_id)
    entry = f'Will drain node {decom_host_id}'
    begin = 'Truncating test.test'
    end = 'Truncated test.test'
    assert not await is_log_entry_between(raft_leader_log, entry, begin, end)

    row = await cql.run_async(SimpleStatement('SELECT COUNT(*) FROM test.test', consistency_level=ConsistencyLevel.ALL))
    assert row[0].count == 0
