#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import gather_safely
from test.pylib.internal_types import ServerInfo
from test.cluster.util import new_test_keyspace
from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, wait_for_leader
from test.cluster.test_sc_leader_colocation import record_split, server_by_host_id, setup_table, \
    wait_for_children_led_by
from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement

import asyncio
import logging
import pytest


logger = logging.getLogger(__name__)


# Hand-off of the requests of a strongly consistent tablet's group to its child groups while a split
# of the tablet is recorded, and the holding back of the children's appliers until the parent's log
# is final.
#
# Nothing drives a split on its own yet. The tests record one with the sc_record_raft_split
# injection, and append the parent's resize markers with the sc_inject_resize_markers injection,
# enabled before the split is recorded: on its first message the parent's leader appends
# start_resize, which starts the hand-off, and on the second end_resize, which releases the
# children. Nothing finalizes the split, so the requests stay handed off until the table is dropped.

CMDLINE = DEFAULT_CMDLINE + ['--logger-log-level', 'raft_resize_tracker=debug']


def keyspace_opts(rf: int) -> str:
    return (f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}"
            " AND tablets = {'initial': 1} AND consistency = 'global'")


async def inject_marker(manager: ScyllaClusterManager, servers: list[ServerInfo], marker: str, parent_gid: str):
    """Have the parent's leader append the next marker, `marker`, and wait until every server has
    applied it."""
    logs = [await manager.server_open_log(s.server_id) for s in servers]
    marks = [await log.mark() for log in logs]
    # Sent everywhere, since only the node leading the parent appends the marker.
    await gather_safely(*[manager.api.message_injection(s.ip_addr, 'sc_inject_resize_markers') for s in servers])
    for log, mark in zip(logs, marks):
        await log.wait_for(f"group {parent_gid}: {marker} applied", from_mark=mark, timeout=60)
    logger.info(f"{marker} applied in {parent_gid} everywhere")


async def stop_injecting_markers(manager: ScyllaClusterManager, servers: list[ServerInfo]):
    await gather_safely(*[manager.api.disable_injection(s.ip_addr, 'sc_inject_resize_markers') for s in servers])


async def assert_not_sealed(log, mark, parent_gid: str):
    """Fails if end_resize was applied, i.e. if a check meant for the hand-off window ran outside it."""
    assert not await log.grep(f"group {parent_gid}: end_resize applied", from_mark=mark), \
        f"group {parent_gid} applied end_resize before the check"


async def sc_insert(cql, table: str, keys, value=lambda k: k):
    await asyncio.gather(*[
        cql.run_async(SimpleStatement(f"INSERT INTO {table} (pk, c) VALUES ({k}, {value(k)});",
                                      consistency_level=ConsistencyLevel.QUORUM))
        for k in keys])


async def sc_check(cql, table: str, keys, value=lambda k: k):
    async def check_one(k):
        rows = await cql.run_async(SimpleStatement(f"SELECT c FROM {table} WHERE pk = {k};",
                                                   consistency_level=ConsistencyLevel.QUORUM))
        assert len(rows) == 1 and rows[0].c == value(k), f"key {k}: got {rows}, expected c={value(k)}"
    await asyncio.gather(*[check_one(k) for k in keys])


async def setup(manager: ScyllaClusterManager, servers: list[ServerInfo], cql, ks: str):
    """A single-tablet table holding keys 0..255, with a split of its tablet recorded and the
    children led by the parent's leader."""
    parent_gid, parent_leader = await setup_table(manager, cql, ks, servers)
    keys = list(range(256))
    await sc_insert(cql, f"{ks}.test", keys)
    # Enabled before the split is recorded, so that the colocator finds it when it first sees the
    # leaders co-located.
    await gather_safely(*[manager.api.enable_injection(s.ip_addr, 'sc_inject_resize_markers', one_shot=False)
                          for s in servers])
    new_gids = await record_split(manager, servers, ks, 'test')
    await wait_for_children_led_by(manager, servers, new_gids, parent_leader)
    return parent_gid, parent_leader, new_gids, keys


# A write and a linearizable read issued while the parent hands its requests off but has not
# appended end_resize yet. The write is committed by a child and answered from there. The read
# runs the child's read barrier, which waits for whatever the child committed while its applier is
# held back, so it may be answered only once the parent appends end_resize. Either way both are
# answered correctly once it does.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_split_read_write_in_handoff_window(manager: ScyllaClusterManager):
    # The read may wait for as long as the test holds the window.
    config = DEFAULT_CONFIG | {'write_request_timeout_in_ms': 120000, 'read_request_timeout_in_ms': 120000}
    servers = [await manager.server_add(config=config, cmdline=CMDLINE)]
    cql, _ = await manager.get_ready_cql(servers)
    async with new_test_keyspace(manager, keyspace_opts(1)) as ks:
        table = f"{ks}.test"
        parent_gid, _, _, keys = await setup(manager, servers, cql, ks)
        log = await manager.server_open_log(servers[0].server_id)
        mark = await log.mark()

        written_key, read_key = keys[0], keys[1]
        new_value = written_key + 100000
        write_task = read_task = None
        try:
            await inject_marker(manager, servers, 'start_resize', parent_gid)
            write_task = asyncio.ensure_future(cql.run_async(SimpleStatement(
                f"UPDATE {table} SET c = {new_value} WHERE pk = {written_key};",
                consistency_level=ConsistencyLevel.QUORUM)))
            read_task = asyncio.ensure_future(cql.run_async(SimpleStatement(
                f"SELECT c FROM {table} WHERE pk = {read_key};",
                consistency_level=ConsistencyLevel.QUORUM)))

            await log.wait_for("mutate\\(\\): handing off the write to table", from_mark=mark)
            await log.wait_for("query\\(\\): handing off the read of table", from_mark=mark)
            # A child commits the write without applying it.
            await asyncio.wait_for(asyncio.shield(write_task), timeout=60)
            await assert_not_sealed(log, mark, parent_gid)

            await inject_marker(manager, servers, 'end_resize', parent_gid)
        finally:
            await stop_injecting_markers(manager, servers)

        await write_task
        rows = await read_task
        assert len(rows) == 1 and rows[0].c == read_key, f"read in the window returned {rows}, expected c={read_key}"
        await sc_check(cql, table, [written_key], value=lambda k: new_value)
        await sc_check(cql, table, keys[1:])


# Requests issued while the children are led elsewhere than the parent are bounced back to the
# parent and retried until the colocator brings the leaders together again, which it does here
# only once end_resize is appended. The test checks that every request issued while the leaders
# are apart is answered, and answered correctly.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_split_request_while_leaders_diverged(manager: ScyllaClusterManager):
    config = DEFAULT_CONFIG | {'write_request_timeout_in_ms': 60000, 'read_request_timeout_in_ms': 60000}
    servers = await manager.servers_add(3, config=config, cmdline=CMDLINE, auto_rack_dc='dc1')
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    cql, _ = await manager.get_ready_cql(servers)
    async with new_test_keyspace(manager, keyspace_opts(3)) as ks:
        table = f"{ks}.test"
        parent_gid, parent_leader, new_gids, keys = await setup(manager, servers, cql, ks)
        leader_server = server_by_host_id(servers, host_ids, parent_leader)
        leader_log = await manager.server_open_log(leader_server.server_id)
        mark = await leader_log.mark()

        written_key, read_key = keys[0], keys[1]
        new_value = written_key + 100000
        write_task = read_task = None
        try:
            await inject_marker(manager, servers, 'start_resize', parent_gid)

            logger.info(f"Moving the leadership of {new_gids} away from the parent's leader")
            for gid in new_gids:
                await manager.api.client.post("/raft/trigger_stepdown", host=leader_server.ip_addr,
                                              params={"group_id": gid})

            write_task = asyncio.ensure_future(cql.run_async(SimpleStatement(
                f"UPDATE {table} SET c = {new_value} WHERE pk = {written_key};",
                consistency_level=ConsistencyLevel.QUORUM)))
            read_task = asyncio.ensure_future(cql.run_async(SimpleStatement(
                f"SELECT c FROM {table} WHERE pk = {read_key};",
                consistency_level=ConsistencyLevel.QUORUM)))

            await leader_log.wait_for("mutate\\(\\): handing off the write to table", from_mark=mark)
            await leader_log.wait_for("query\\(\\): handing off the read of table", from_mark=mark)
            await assert_not_sealed(leader_log, mark, parent_gid)
            bounces = await leader_log.grep("got not_a_leader .*handoff_group", from_mark=mark)
            logger.info(f"{len(bounces)} request(s) bounced back to the parent")

            # The colocator waits in the injection until end_resize, and brings the leaders back
            # together only after it.
            await inject_marker(manager, servers, 'end_resize', parent_gid)
        finally:
            await stop_injecting_markers(manager, servers)

        write_result, read_result = await asyncio.gather(write_task, read_task, return_exceptions=True)
        assert not isinstance(write_result, BaseException), f"write while the leaders were apart failed: {write_result}"
        assert not isinstance(read_result, BaseException), f"read while the leaders were apart failed: {read_result}"
        assert len(read_result) == 1 and read_result[0].c == read_key, \
            f"read while the leaders were apart returned {read_result}, expected c={read_key}"
        await sc_check(cql, table, [written_key], value=lambda k: new_value)
        await sc_check(cql, table, keys[1:])


# A linearizable read checks whether the parent hands its requests off after the parent's read
# barrier, because the barrier may be what applies start_resize. Hold a read before the barrier,
# start the hand-off under it, and check that the read moves to a child and is answered.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_split_read_handed_off_after_barrier(manager: ScyllaClusterManager):
    servers = [await manager.server_add(config=DEFAULT_CONFIG, cmdline=CMDLINE)]
    server = servers[0]
    cql, _ = await manager.get_ready_cql(servers)
    hold_read = 'sc_coordinator_wait_before_query_read_barrier'
    async with new_test_keyspace(manager, keyspace_opts(1)) as ks:
        table = f"{ks}.test"
        parent_gid, _, _, keys = await setup(manager, servers, cql, ks)
        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()

        read_key = keys[1]
        read_task = None
        read_released = False
        await manager.api.enable_injection(server.ip_addr, hold_read, one_shot=True)
        try:
            read_task = asyncio.ensure_future(cql.run_async(SimpleStatement(
                f"SELECT c FROM {table} WHERE pk = {read_key};", consistency_level=ConsistencyLevel.QUORUM)))
            await manager.api.wait_for_injection_enter(server.ip_addr, hold_read)
            await inject_marker(manager, servers, 'start_resize', parent_gid)
            await manager.api.message_injection(server.ip_addr, hold_read)
            read_released = True
            await log.wait_for("handing off the read of table .* after the parent's read barrier", from_mark=mark)
            await inject_marker(manager, servers, 'end_resize', parent_gid)
        finally:
            if not read_released:
                await manager.api.message_injection(server.ip_addr, hold_read)
            await manager.api.disable_injection(server.ip_addr, hold_read)
            await stop_injecting_markers(manager, servers)

        rows = await read_task
        assert len(rows) == 1 and rows[0].c == read_key, \
            f"read held across the start of the hand-off returned {rows}, expected c={read_key}"


# A handed-off write has to sort after every write of the parent, also when the parent's leader
# clock is ahead of the child's. Write to the parent with its leader's clock shifted an hour ahead
# - after the children seeded their clocks, so that only the parent knows those timestamps - then
# overwrite the same keys once they are handed off, and check that the overwrites win.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_split_handed_off_write_wins(manager: ScyllaClusterManager):
    servers = [await manager.server_add(config=DEFAULT_CONFIG, cmdline=CMDLINE)]
    server = servers[0]
    cql, _ = await manager.get_ready_cql(servers)
    async with new_test_keyspace(manager, keyspace_opts(1)) as ks:
        table = f"{ks}.test"
        parent_gid, parent_leader, new_gids, keys = await setup(manager, servers, cql, ks)
        # The children seed their clocks when they elect a leader, which setup() waited for.
        for gid in new_gids:
            await wait_for_leader(manager, server, gid)

        # Spread over enough keys to land in both children.
        hot_keys = list(range(1000, 1016))
        await manager.api.enable_injection(server.ip_addr, 'sc_leader_timestamp_offset_us', one_shot=False,
                                           parameters={'value': str(3600 * 1000 * 1000)})
        try:
            await sc_insert(cql, table, hot_keys)
        finally:
            await manager.api.disable_injection(server.ip_addr, 'sc_leader_timestamp_offset_us')

        try:
            await inject_marker(manager, servers, 'start_resize', parent_gid)
            await sc_insert(cql, table, hot_keys, value=lambda k: k + 1)
            await inject_marker(manager, servers, 'end_resize', parent_gid)
        finally:
            await stop_injecting_markers(manager, servers)

        await sc_check(cql, table, hot_keys, value=lambda k: k + 1)
        await sc_check(cql, table, keys)
