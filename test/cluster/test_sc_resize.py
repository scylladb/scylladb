#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#


from test.pylib.manager_client import ManagerClient
from test.pylib.util import gather_safely, wait_for
from test.cluster.util import new_test_keyspace, reconnect_driver, \
    get_topology_coordinator, trigger_stepdown
from test.pylib.internal_types import ServerInfo
from cassandra.cluster import ConsistencyLevel
from cassandra.query import SimpleStatement
from test.pylib.tablets import get_tablet_count
from test.cluster.test_strong_consistency import DEFAULT_CMDLINE, DEFAULT_CONFIG, wait_for_leader

import asyncio
import pytest
import logging
import time
import uuid


logger = logging.getLogger(__name__)


# Strongly consistent tablet split (SCYLLADB-15).
#
# The split is driven by the load balancer, so these tests need it to plan resize decisions
# without moving tablets around underneath them. The allow_tablet_resize_without_balancing
# injection in SC_SPLIT_CONFIG plans them with balancing left disabled, which is all a split
# needs and emits no migration, so a tablet stays on the shard it was allocated on for the
# duration of a test.
SC_SPLIT_CMDLINE = DEFAULT_CMDLINE + ['--logger-log-level', 'load_balancer=debug',
                                      '--logger-log-level', 'raft_topology=debug',
                                      '--logger-log-level', 'raft_resize_tracker=debug',
                                      '--logger-log-level', 'raft_commitlog_replay=debug']
SC_SPLIT_CONFIG = DEFAULT_CONFIG | {
    'tablet_load_stats_refresh_interval_in_seconds': 1,
    'error_injections_at_startup': ['allow_tablet_resize_without_balancing'],
}


async def sc_insert(cql, table: str, keys):
    """Write `c = pk` for every key with a linearizable (QUORUM) write."""
    await asyncio.gather(*[
        cql.run_async(SimpleStatement(
            f"INSERT INTO {table} (pk, c) VALUES ({k}, {k});",
            consistency_level=ConsistencyLevel.QUORUM))
        for k in keys])


async def sc_check(cql, table: str, keys):
    """Read every key back with a linearizable (QUORUM) read and verify `c == pk`."""
    # Strongly consistent tables only allow single-partition reads, so read
    # each key individually.
    async def check_one(k):
        rows = await cql.run_async(SimpleStatement(
            f"SELECT c FROM {table} WHERE pk = {k};",
            consistency_level=ConsistencyLevel.QUORUM))
        assert len(rows) == 1, f"key {k} missing (got {len(rows)} rows)"
        assert rows[0].c == k, f"key {k} has wrong value {rows[0].c}"
    await asyncio.gather(*[check_one(k) for k in keys])


async def hold_resize_before_end_resize(manager: ManagerClient, server: ServerInfo):
    """
    Enables the injection which holds a resize in the window where the parent group no longer
    accepts writes but has not been sealed yet.

    Not one-shot: the topology coordinator retries the sealing verb after its 10 second RPC
    timeout, and a retry which finds the injection already consumed commits end_resize and closes
    the window. Every retry has to be held instead, which makes the window last for as long as the
    test wants rather than for as long as the test takes.
    """
    await manager.api.enable_injection(server.ip_addr, 'sc_pause_before_end_resize', one_shot=False)


async def release_resize_before_end_resize(manager: ManagerClient, server: ServerInfo):
    """Lets the held resize continue. Disabling releases whichever retry is currently parked."""
    await manager.api.disable_injection(server.ip_addr, 'sc_pause_before_end_resize')


async def assert_resize_still_held(log, mark, parent_gid: str):
    """
    Fails if the parent group has already been sealed, i.e. if the window
    hold_resize_before_end_resize() opened has closed. Everything a test asserts about that window
    would otherwise be asserted outside it, and pass for the wrong reason.
    """
    assert not await log.grep(f"group {parent_gid}: end_resize applied", from_mark=mark), \
        f"group {parent_gid} was sealed before the check, so the window was not held for it"


def server_by_host_id(servers: list[ServerInfo], host_ids, host_id) -> ServerInfo:
    """The server with the given host id."""
    return [s for s, hid in zip(servers, host_ids) if str(hid) == str(host_id)][0]


async def get_tablet_raft_rows(manager: ManagerClient, ks: str, table: str):
    """The raft group id and the in-progress resize info of every tablet of the table."""
    table_id = await manager.get_table_id(ks, table)
    rows = await manager.get_cql().run_async(
        "SELECT raft_group_id, transition_raft_group_ids"
        f" FROM system.tablets WHERE table_id = {table_id}")
    return [r for r in rows if r.raft_group_id is not None]


async def get_parent_gid(manager: ManagerClient, ks: str, table: str) -> str:
    """The raft group id of the table's only tablet, i.e. the one a split replaces."""
    rows = await get_tablet_raft_rows(manager, ks, table)
    assert len(rows) == 1
    return str(rows[0].raft_group_id)


async def wait_for_split(manager: ManagerClient, server: ServerInfo, ks: str, table: str,
                         timeout: float = 180):
    """Wait until the table's tablet has been replaced by the two it splits into."""
    async def split_done():
        return True if await get_tablet_count(manager, server, ks, table) > 1 else None
    await wait_for(split_done, time.time() + timeout)


async def split_sc_tablet(manager: ManagerClient, server: ServerInfo, cql, ks: str, table: str):
    """Force the single tablet of the table to split and wait until it does."""
    assert await get_tablet_count(manager, server, ks, table) == 1

    logger.info("Altering the table to require at least 2 tablets")
    await cql.run_async(f"ALTER TABLE {ks}.{table} WITH tablets = {{'min_tablet_count': 2}}")

    # The load balancer emits the split decision on its own once the load stats say the table
    # is below the tablet count it now requires.
    await wait_for_split(manager, server, ks, table)
    logger.info("Tablet split completed")


async def sc_write_and_check_both_sides(cql, table: str, keys: list[int]):
    """Check that the table is usable once the split is over.

    Writes a second range of keys, spread widely enough to land on both sides of the split point,
    and reads back both it and everything acknowledged before - so a child which cannot serve its
    half of the token range, or which lost what the parent had applied, is caught here.
    """
    logger.info("Writing more data after the split")
    more_keys = list(range(256, 512))
    await sc_insert(cql, table, more_keys)
    await sc_check(cql, table, keys + more_keys)


# End-to-end strongly consistent tablet split.
#
# With RF=1 the whole resize runs on a single replica; with RF=3 the topology
# coordinator has to drive it on all three, and the leaders of the child groups
# have to be brought to the node leading the parent group.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("rf", [1, 3])
async def test_sc_tablet_split(manager: ManagerClient, rf: int):
    logger.info("Bootstrapping cluster")
    servers = await manager.servers_add(rf, config=SC_SPLIT_CONFIG, cmdline=SC_SPLIT_CMDLINE,
                                        auto_rack_dc='my_dc')
    cql, _ = await manager.get_ready_cql(servers)
    logger.info("Creating a strongly consistent keyspace and table")
    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': "
            f"{rf}}} AND tablets = {{'initial': 1}} AND consistency = 'global'") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = list(range(256))

        logger.info("Writing initial data")
        await sc_insert(cql, table, keys)

        parent_gid = await get_parent_gid(manager, ks, 'test')

        logs = [await manager.server_open_log(s.server_id) for s in servers]
        marks = [await log.mark() for log in logs]

        # The children take over the group ids generated when the finalization starts, and those
        # are in the tablet metadata only while the split runs. Hold the finalization just past
        # its barrier - the write which records them has landed by then - to read them, so that
        # the children can be checked against them once the split is done.
        host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
        coordinator_host_id = await get_topology_coordinator(manager)
        coordinator = server_by_host_id(servers, host_ids, coordinator_host_id)
        post_barrier = 'tablet_resize_finalization_post_barrier'
        await manager.api.enable_injection(coordinator.ip_addr, post_barrier, one_shot=True)
        split_task = asyncio.create_task(split_sc_tablet(manager, servers[0], cql, ks, 'test'))
        try:
            await manager.api.wait_for_injection_enter(coordinator.ip_addr, post_barrier)
            held_rows = await get_tablet_raft_rows(manager, ks, 'test')
            assert len(held_rows) == 1
            assert held_rows[0].transition_raft_group_ids is not None, \
                "the finalization did not record the replacement group ids"
            recorded_gids = {str(gid) for gid in held_rows[0].transition_raft_group_ids}
            assert len(recorded_gids) == 2, f"expected two replacement group ids, got {recorded_gids}"
        finally:
            await manager.api.message_injection(coordinator.ip_addr, post_barrier)
            await manager.api.disable_injection(coordinator.ip_addr, post_barrier)
            await split_task

        # The parent group must have been sealed on every replica, i.e. every
        # replica applied the end_resize marker, which is what releases the
        # appliers of the groups replacing it.
        for server, log, mark in zip(servers, logs, marks):
            assert await log.grep(f"group {parent_gid}: end_resize applied", from_mark=mark), \
                f"the parent group was not sealed on {server.server_id}"

        # Each child tablet has a raft group of its own, the parent's is gone, and the ids
        # generated when the finalization started are no longer part of the tablet metadata.
        child_rows = await get_tablet_raft_rows(manager, ks, 'test')
        assert len(child_rows) == 2
        child_gids = {str(r.raft_group_id) for r in child_rows}
        assert parent_gid not in child_gids, "a child tablet kept the parent's raft group"
        # The point of generating the ids when the finalization starts: the replicas created the
        # groups these name, so the new tablets have to take them rather than get fresh ones.
        assert child_gids == recorded_gids, \
            f"the children did not take the recorded group ids: {child_gids} != {recorded_gids}"
        for r in child_rows:
            assert r.transition_raft_group_ids is None

        # All previously acknowledged writes must survive the split.
        await sc_check(cql, table, keys)

        # Writes and reads on both sides of the split point must keep working
        # after the parent group is replaced by the child groups.
        await sc_write_and_check_both_sides(cql, table, keys)


# Kill a replica in the middle of the sealing, exclude it, and check that the split completes.
# A dead replica is the one failure the sealing cannot retry its way out of - it never answers the
# round which waits for every replica - and excluding it is what makes the sealing skip it.
#
# Three ways round it, since which node is killed and which is asked to exclude decide what the
# attempt has to give up its group0 guard for:
#
#  - a plain replica, excluded through a node which is not the coordinator: the write lands at
#    once, so what the attempt has to give up for is the coordinator being able to see it,
#  - a plain replica, excluded through the coordinator itself: the write blocks on the guard's
#    operation mutex until the attempt gives up,
#  - the coordinator, which hands the finalization to a survivor before it can be excluded.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.parametrize("kill_coordinator,exclude_via_coordinator",
                         [(False, False), (False, True), (True, False)])
async def test_sc_tablet_split_excluded_replica_unblocks_seal(manager: ManagerClient,
                                                              kill_coordinator: bool,
                                                              exclude_via_coordinator: bool):
    logger.info("Bootstrapping cluster")
    servers = await manager.servers_add(3, config=SC_SPLIT_CONFIG, cmdline=SC_SPLIT_CMDLINE,
                                        auto_rack_dc='my_dc')
    cql, _ = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    injection = 'sc_pause_before_end_resize'
    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
            " AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = list(range(256))
        await sc_insert(cql, table, keys)

        parent_gid = await get_parent_gid(manager, ks, 'test')

        leader_host_id = await wait_for_leader(manager, servers[0], parent_gid)
        leader_server = server_by_host_id(servers, host_ids, leader_host_id)
        logger.info(f"Parent group {parent_gid} is led by {leader_host_id}")

        coordinator_host_id = await get_topology_coordinator(manager)
        logger.info(f"The topology coordinator runs on {coordinator_host_id}")
        if kill_coordinator:
            victim = server_by_host_id(servers, host_ids, coordinator_host_id)
        else:
            # A replica which neither drives the sealing nor runs the finalization, so that the
            # only thing the kill breaks is the round which waits for every replica.
            victim = [s for s, hid in zip(servers, host_ids)
                      if s.server_id != leader_server.server_id
                      and str(hid) != str(coordinator_host_id)][0]
        victim_host_id = [hid for s, hid in zip(servers, host_ids) if s.server_id == victim.server_id][0]
        survivors = [s for s in servers if s.server_id != victim.server_id]
        logs = [await manager.server_open_log(s.server_id) for s in survivors]
        marks = [await log.mark() for log in logs]

        # Held so that the kill below provably lands with the finalization already under way,
        # rather than before the table is even reported ready to be split.
        await hold_resize_before_end_resize(manager, leader_server)
        # Nothing to release once the leader is the node which was killed - the injection went
        # with it. Anywhere else it has to be released whatever happens in between, or the
        # sealing stays parked and the test hangs in its cleanup rather than reporting.
        leader_release_needed = victim.server_id != leader_server.server_id
        try:
            logger.info("Altering the table to require at least 2 tablets")
            await cql.run_async(f"ALTER TABLE {table} WITH tablets = {{'min_tablet_count': 2}}")
            await manager.api.wait_for_injection_enter(leader_server.ip_addr, injection)

            logger.info(f"Killing {victim.server_id} in the middle of the sealing")
            await manager.server_stop(victim.server_id, convict=True)
            for s in survivors:
                await manager.server_not_sees_other_server(s.ip_addr, victim.ip_addr)
        finally:
            if leader_release_needed:
                await release_resize_before_end_resize(manager, leader_server)

        # The sealing cannot finish now: the last round waits for every replica which is not
        # excluded. Nothing but the exclusion gets it moving again.
        assert await get_tablet_count(manager, survivors[0], ks, 'test') == 1

        if exclude_via_coordinator:
            excluder = server_by_host_id(servers, host_ids, coordinator_host_id)
        else:
            # Killing the coordinator moves it, so pick a survivor which is not the one running it
            # now rather than the one which was running it when the split started.
            async def surviving_coordinator():
                try:
                    hid = await get_topology_coordinator(manager)
                except Exception as e:
                    logger.info(f"Still no coordinator to ask: {e}")
                    return None
                return hid if str(hid) != str(victim_host_id) else None
            live_coordinator = await wait_for(surviving_coordinator, time.time() + 120)
            excluder = [s for s, hid in zip(servers, host_ids)
                        if s.server_id != victim.server_id and str(hid) != str(live_coordinator)][0]

        # Excluding the dead replica is what lets the next attempt through. Issued against the
        # coordinator it blocks on the guard's operation mutex until the attempt gives up; issued
        # anywhere else it lands at once but stays invisible until then.
        logger.info(f"Excluding {victim.server_id} via {excluder.server_id}")
        await manager.api.exclude_node(excluder.ip_addr, [victim_host_id], timeout=120)

        await wait_for_split(manager, survivors[0], ks, 'test', timeout=300)
        logger.info("The split completed with the dead replica excluded")

        if not kill_coordinator:
            # The sealing gave up rather than hanging, and said which tablet it could not seal.
            # Checked from a mark taken before the window was held, which is where the first such
            # report lands: give_up() rate-limits that warning to one in thirty seconds, so the
            # attempts which run after the kill are covered by the earlier one rather than logging
            # their own, and a mark taken here would find nothing. A coordinator which was killed
            # instead never gets that far: its successor fails the barrier the finalization starts
            # with.
            assert any([await log.grep(f"Raft group {parent_gid} .* has not been sealed on all of",
                                       from_mark=mark) for log, mark in zip(logs, marks)]), \
                "the attempt never reported the tablet it could not seal"

        # The two surviving replicas are a quorum, so nothing was lost and the child groups work.
        await sc_check(cql, table, keys)
        await sc_write_and_check_both_sides(cql, table, keys)


# Hold the split open at each of the four phases it goes through and write a fixed batch of keys
# inside every one, rather than writing in a loop and hoping to catch them:
#
#  - after the barrier which opens the finalization, where the child groups are being created but
#    no marker has been committed, so the parent still serves the range itself,
#  - between the two markers, where the parent no longer serves it and the writes are handed off to
#    the children, which commit them without applying them,
#  - after both markers, where the parent is sealed but the tablet map still names it,
#  - after the split, where each child serves its own half of the range.
#
# Every key is read back at the end, so a write acknowledged in any of the four phases and then
# lost is caught. One replica is enough: the phases are properties of the finalization, and the
# tests which need the seal driven on several replicas run with RF=3.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_tablet_split_writes_in_every_phase(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    server = await manager.server_add(config=SC_SPLIT_CONFIG, cmdline=SC_SPLIT_CMDLINE)
    cql, _ = await manager.get_ready_cql([server])
    post_barrier = 'tablet_resize_finalization_post_barrier'
    handoff = 'sc_pause_before_end_resize'
    sealed = 'sc_pause_after_sealing_parents'
    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
            " AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = list(range(256))
        await sc_insert(cql, table, keys)

        parent_gid = await get_parent_gid(manager, ks, 'test')

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()

        # One batch per phase, each spread across the token space so that it lands on both sides of
        # the split point. Sixteen keys is enough to do that and small enough to write in one go.
        def batch(phase: int) -> list[int]:
            return list(range(phase * 1000, phase * 1000 + 16))

        written: list[int] = []
        await manager.api.enable_injection(server.ip_addr, post_barrier, one_shot=True)
        split_task = asyncio.create_task(split_sc_tablet(manager, server, cql, ks, 'test'))
        try:
            await manager.api.wait_for_injection_enter(server.ip_addr, post_barrier)
            # No marker is committed yet, so this batch is served by the parent itself.
            await sc_insert(cql, table, batch(1))
            written += batch(1)
            await assert_resize_still_held(log, mark, parent_gid)

            # Armed before the finalization is let go, so that it cannot run past the next phase.
            await hold_resize_before_end_resize(manager, server)
            await manager.api.message_injection(server.ip_addr, post_barrier)
            await manager.api.disable_injection(server.ip_addr, post_barrier)

            await manager.api.wait_for_injection_enter(server.ip_addr, handoff)
            await manager.api.enable_injection(server.ip_addr, sealed, one_shot=True)
            # start_resize is committed, so the parent refuses these and they go to the children.
            await sc_insert(cql, table, batch(2))
            written += batch(2)
            assert await log.grep("mutate\\(\\): handing off the write to table", from_mark=mark), \
                "the writes issued between the markers were not handed off to the child groups"
            await assert_resize_still_held(log, mark, parent_gid)
            await release_resize_before_end_resize(manager, server)

            await manager.api.wait_for_injection_enter(server.ip_addr, sealed)
            assert await log.grep(f"group {parent_gid}: end_resize applied", from_mark=mark), \
                "the finalization reached the last phase without sealing the parent"
            assert await get_tablet_count(manager, server, ks, 'test') == 1, \
                "the tablet map was replaced before the last phase"
            # The parent is sealed but still named by the tablet map; the children serve the range.
            await sc_insert(cql, table, batch(3))
            written += batch(3)
            await manager.api.message_injection(server.ip_addr, sealed)
            await manager.api.disable_injection(server.ip_addr, sealed)
        finally:
            # Disabling releases whichever of them is parked and does nothing to the rest, so this
            # lets the finalization run to its end however far the checks above got.
            for injection in (post_barrier, handoff, sealed):
                await manager.api.disable_injection(server.ip_addr, injection)
            await split_task

        # The split is over and each child serves its own half of the range.
        await sc_insert(cql, table, batch(4))
        written += batch(4)

        await sc_check(cql, table, keys + written)


# Hold the window between the two markers open and write a fixed number of batches into it, one
# after another so that each is a queue entry of its own: a child's applier is held back for that
# whole window, so the writes handed off to it are committed but not applied and pile up in its
# raft server's applier queue. The queue holds messages, not writes, and a message is a whole
# committed batch, so awaiting each batch is what makes the count of batches a lower bound on the
# count of messages. Thirty-two is over three times the default limit of ten, and a child whose
# limit was not lifted for the resize stops acknowledging around the eleventh - the write which
# follows never returns, so the batch loop below never finishes.
#
# One replica is enough: the applier queue is per replica.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_tablet_split_writes_past_applier_queue_limit(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    server = await manager.server_add(config=SC_SPLIT_CONFIG, cmdline=SC_SPLIT_CMDLINE)
    cql, _ = await manager.get_ready_cql([server])
    injection = 'sc_pause_before_end_resize'
    batches = 32
    batch_size = 8
    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
            " AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = list(range(256))
        await sc_insert(cql, table, keys)

        parent_gid = await get_parent_gid(manager, ks, 'test')

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()

        handed_off: list[int] = []
        await hold_resize_before_end_resize(manager, server)
        split_task = asyncio.create_task(split_sc_tablet(manager, server, cql, ks, 'test'))
        try:
            await manager.api.wait_for_injection_enter(server.ip_addr, injection)

            for i in range(batches):
                written = list(range(1000 + i * batch_size, 1000 + (i + 1) * batch_size))
                await sc_insert(cql, table, written)
                handed_off += written
            assert await log.grep("mutate\\(\\): handing off the write to table", from_mark=mark), \
                "the writes were served by the parent rather than handed off to the child groups"
            # Those acknowledgements only prove anything if they were collected inside the window:
            # once the parent is sealed the children apply freely and no queue fills up.
            await assert_resize_still_held(log, mark, parent_gid)
            logger.info(f"{len(handed_off)} writes in {batches} batches were acknowledged while "
                        "the resize was held between the markers")
        finally:
            await release_resize_before_end_resize(manager, server)
            await split_task

        await sc_check(cql, table, keys + handed_off)


# Holds the resize in the window between the two markers and follows a single write and a single
# linearizable read which land in it. Neither may be lost or failed, but they are not affected in
# the same way:
#
#  - the write is acknowledged as soon as the group replacing the parent commits it, without
#    waiting for the parent: it carries a timestamp above every one the parent handed out and the
#    new group's applier is held back, so it cannot be applied or observed out of order,
#  - the linearizable read cannot be answered until the group it was sent to has applied its
#    entries, which is what the parent being done releases. Answering it earlier could expose a
#    state older than a write already committed in the group being replaced.
#
# Once the resize completes, both are visible: the read sees the pre-resize state and the value
# written in the window is readable.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_tablet_split_read_write_in_handoff_window(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    # The read below is parked for as long as this test holds the window, and has to still be
    # pending when it is checked. Its own deadline must therefore outlast the allowance the write
    # gets, which the harness default of 30s does not.
    config = SC_SPLIT_CONFIG | {'write_request_timeout_in_ms': 120000,
                                'read_request_timeout_in_ms': 120000}
    server = await manager.server_add(config=config, cmdline=SC_SPLIT_CMDLINE)
    cql, _ = await manager.get_ready_cql([server])
    injection = 'sc_pause_before_end_resize'
    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
            " AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = list(range(256))
        await sc_insert(cql, table, keys)

        parent_gid = await get_parent_gid(manager, ks, 'test')

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()

        await hold_resize_before_end_resize(manager, server)
        split_task = asyncio.create_task(split_sc_tablet(manager, server, cql, ks, 'test'))
        written_key, read_key = keys[0], keys[1]
        new_value = written_key + 100000

        async def write_in_window():
            await cql.run_async(SimpleStatement(
                f"UPDATE {table} SET c = {new_value} WHERE pk = {written_key};",
                consistency_level=ConsistencyLevel.QUORUM))

        async def read_in_window():
            return await cql.run_async(SimpleStatement(
                f"SELECT c FROM {table} WHERE pk = {read_key};",
                consistency_level=ConsistencyLevel.QUORUM))

        write_task = read_task = None
        try:
            await manager.api.wait_for_injection_enter(server.ip_addr, injection)
            write_task = asyncio.create_task(write_in_window())
            read_task = asyncio.create_task(read_in_window())

            # Both requests have to reach the group being resized and be handed off to the
            # group replacing it.
            await log.wait_for("mutate\\(\\): handing off the write to table", from_mark=mark)
            await log.wait_for("query\\(\\): handing off the read of table", from_mark=mark)

            # The write is committed by the group replacing the parent and answered
            # from there, while the resize is still held in this window.
            await asyncio.wait_for(asyncio.shield(write_task), timeout=60)

            # The read cannot be answered while we hold the resize here, so there is
            # no window to hit and nothing to wait for: whenever we look, it is still
            # pending. It is released by the same thing which releases the applier of
            # the group it was sent to, checked after the injection is released below.
            await assert_resize_still_held(log, mark, parent_gid)
            assert not read_task.done(), (
                "a linearizable read was answered by a new group before the group it replaces "
                f"was done: {read_task.exception() or read_task.result()}")
        finally:
            await release_resize_before_end_resize(manager, server)
            await split_task

        # Neither request was lost: the write is acknowledged and the read is
        # answered once the groups replacing the parent are released.
        await write_task
        rows = await read_task
        assert len(rows) == 1 and rows[0].c == read_key, \
            f"read issued during the resize returned {rows}, expected c={read_key}"

        # The write which was handed off to a child group is not lost either.
        rows = await cql.run_async(SimpleStatement(
            f"SELECT c FROM {table} WHERE pk = {written_key};",
            consistency_level=ConsistencyLevel.QUORUM))
        assert len(rows) == 1 and rows[0].c == new_value, \
            f"read after the split returned {rows}, expected c={new_value}"

        # Everything but written_key, whose value the write above replaced.
        await sc_check(cql, table, keys[1:])


# Sealing lifts the children's clocks above the parent's before committing end_resize, so that a
# write arriving after the split cannot be handed a timestamp below one the parent handed out
# before it and lose to the older write. Check that with writes which land in the parent after the
# children seeded their clocks and are not handed off, so nothing carries the parent's clock over.
#
# A leader's clock never runs far enough ahead of wall clock for a test to catch the window, so
# the sc_inflate_write_timestamp injection pushes the parent's an hour ahead instead. The split is
# held before the sealing starts, so the inflated writes provably land in the parent's log after
# the children started and before the handoff begins.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_tablet_split_write_after_split_wins(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    server = await manager.server_add(config=SC_SPLIT_CONFIG, cmdline=SC_SPLIT_CMDLINE)
    cql, _ = await manager.get_ready_cql([server])
    # Holds the finalization right after its barrier, so that the child groups exist - their ids
    # are recorded by the write which enters the finalization - while the sealing has not started
    # yet, and the writes below land in the parent's log rather than being handed off.
    injection = 'tablet_resize_finalization_post_barrier'
    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
            " AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = list(range(256))
        await sc_insert(cql, table, keys)

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()

        await manager.api.enable_injection(server.ip_addr, injection, one_shot=False)
        split_task = asyncio.create_task(split_sc_tablet(manager, server, cql, ks, 'test'))
        try:
            # Wait until the children are running: their leaders seed their clocks when they are
            # elected, and the writes below have to come after that to stay unknown to them.
            async def children_started():
                rows = await get_tablet_raft_rows(manager, ks, 'test')
                if len(rows) != 1 or rows[0].transition_raft_group_ids is None:
                    return None
                return [str(gid) for gid in rows[0].transition_raft_group_ids]
            child_gids = await wait_for(children_started, time.time() + 120)
            # Waited for the leaders rather than for the raft servers: a server which has started
            # has not necessarily held its election yet, and it is the election which seeds the
            # clock the writes below have to be unknown to.
            for child_gid in child_gids:
                await wait_for_leader(manager, server, child_gid)

            # Writes whose timestamps only the parent's clock knows about, spread over enough
            # keys to land on both sides of the split point.
            hot_keys = list(range(1000, 1016))
            logger.info("Writing with an inflated parent clock")
            await manager.api.enable_injection(server.ip_addr, 'sc_inflate_write_timestamp',
                                               one_shot=False)
            try:
                await sc_insert(cql, table, hot_keys)
            finally:
                await manager.api.disable_injection(server.ip_addr, 'sc_inflate_write_timestamp')
        finally:
            await manager.api.disable_injection(server.ip_addr, injection)
            await split_task

        # Overwrites arriving after the split. Each must win over the value it overwrites,
        # however inflated the timestamp the parent handed that one.
        logger.info("Overwriting after the split")
        await asyncio.gather(*[
            cql.run_async(SimpleStatement(
                f"UPDATE {table} SET c = {k + 1} WHERE pk = {k};",
                consistency_level=ConsistencyLevel.QUORUM))
            for k in hot_keys])

        for k in hot_keys:
            rows = await cql.run_async(SimpleStatement(
                f"SELECT c FROM {table} WHERE pk = {k};",
                consistency_level=ConsistencyLevel.QUORUM))
            assert len(rows) == 1 and rows[0].c == k + 1, \
                f"a write issued after the split lost to one from before it: pk={k}, got {rows}"

        await sc_check(cql, table, keys)


# The parent group of a tablet being split can change its leader in the middle
# of the resize, which invalidates the co-location of the leaders of the groups
# replacing it and makes the sealing RPC fail on the old leader. Force such a
# change while the resize is paused between the two markers and verify that the
# split still completes and that nothing is lost.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_tablet_split_with_leader_change(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    servers = await manager.servers_add(3, config=SC_SPLIT_CONFIG, cmdline=SC_SPLIT_CMDLINE,
                                        auto_rack_dc='my_dc')
    cql, _ = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    injection = 'sc_pause_before_end_resize'
    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
            " AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = list(range(256))
        await sc_insert(cql, table, keys)

        parent_gid = await get_parent_gid(manager, ks, 'test')

        # The resize is driven on the leader of the parent group: no other replica can commit
        # the markers. The injection is only enabled there anyway.
        leader_host_id = await wait_for_leader(manager, servers[0], parent_gid)
        leader_server = server_by_host_id(servers, host_ids, leader_host_id)
        logger.info(f"Parent group {parent_gid} is led by {leader_host_id}")

        logs = [await manager.server_open_log(s.server_id) for s in servers]
        marks = [await log.mark() for log in logs]

        await hold_resize_before_end_resize(manager, leader_server)
        split_task = asyncio.create_task(split_sc_tablet(manager, servers[0], cql, ks, 'test'))
        try:
            await manager.api.wait_for_injection_enter(leader_server.ip_addr, injection)

            logger.info("Making the parent group leader step down mid-resize")
            await manager.api.client.post("/raft/trigger_stepdown", host=leader_server.ip_addr,
                                          params={"group_id": parent_gid})

            # The leaders of the child groups are now on a node which no longer
            # leads the parent group, so they have to be moved before the resize
            # can be finished.
            other_server = [s for s in servers if s.server_id != leader_server.server_id][0]
            async def leader_changed():
                new_leader = await manager.api.get_raft_leader(other_server.ip_addr, parent_gid)
                return new_leader if str(new_leader) not in (str(leader_host_id), str(uuid.UUID(int=0))) else None
            new_leader_host_id = await wait_for(leader_changed, time.time() + 60)
            logger.info(f"Parent group {parent_gid} is now led by {new_leader_host_id}")
        finally:
            await release_resize_before_end_resize(manager, leader_server)
            await split_task

        # A child group leader had to be handed over to the node which now leads the parent
        # group before the resize could be finished. That the transfers did complete is implied
        # by the split having finished at all.
        assert any([await log.grep("colocate_leaders: group", from_mark=mark)
                    for log, mark in zip(logs, marks)]), \
            "no child group leadership was transferred to the new parent leader"

        # Nothing was lost and both child groups are usable.
        await sc_check(cql, table, keys)
        await sc_write_and_check_both_sides(cql, table, keys)


# A replica which crashes after a split has the entries of the child groups in
# its commitlog only, so they are applied by commitlog replay rather than by the
# state machine. Verify that nothing is lost and that the child groups keep
# working after the restart.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_tablet_split_survives_crash(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    server = await manager.server_add(config=SC_SPLIT_CONFIG, cmdline=SC_SPLIT_CMDLINE)
    cql, _ = await manager.get_ready_cql([server])
    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
            " AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = list(range(256))
        await sc_insert(cql, table, keys)

        await split_sc_tablet(manager, server, cql, ks, 'test')

        # Written to the child groups only, after the parent group was sealed.
        more_keys = list(range(256, 512))
        await sc_insert(cql, table, more_keys)

        logger.info("Crashing the node")
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        await reconnect_driver(manager)
        cql = manager.get_cql()

        # The split is not undone by the restart and no write is lost.
        assert await get_tablet_count(manager, server, ks, 'test') == 2
        await sc_check(cql, table, keys + more_keys)

        # The child groups are usable after being recovered from the commitlog.
        even_more_keys = list(range(512, 768))
        await sc_insert(cql, table, even_more_keys)
        await sc_check(cql, table, keys + more_keys + even_more_keys)


# A replica which crashes while a split is in progress, in the window where the
# parent group has stopped accepting writes but has not been sealed yet, has the
# entries of the child groups in its commitlog while the parent's log is not final.
# Those entries must not be applied during the replay: more entries may still
# arrive in the parent group and have to be applied first, so the child groups are
# only rewritten to the new commitlog and left in the raft log, to be applied once
# the parent is sealed. Verify that a write acknowledged in that window survives the
# crash and that the split completes afterwards.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_tablet_split_crash_in_handoff_window(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    server = await manager.server_add(config=SC_SPLIT_CONFIG, cmdline=SC_SPLIT_CMDLINE)
    cql, _ = await manager.get_ready_cql([server])
    injection = 'sc_pause_before_end_resize'
    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
            " AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = list(range(256))
        await sc_insert(cql, table, keys)

        parent_gid = await get_parent_gid(manager, ks, 'test')

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()

        await hold_resize_before_end_resize(manager, server)
        split_task = asyncio.create_task(split_sc_tablet(manager, server, cql, ks, 'test'))
        try:
            await manager.api.wait_for_injection_enter(server.ip_addr, injection)

            # Acknowledged by a child group while the parent is not sealed yet, so it
            # is committed in the child's log and not applied anywhere.
            written_key = keys[0]
            new_value = written_key + 100000
            await cql.run_async(SimpleStatement(
                f"UPDATE {table} SET c = {new_value} WHERE pk = {written_key};",
                consistency_level=ConsistencyLevel.QUORUM))
            await log.wait_for("mutate\\(\\): handing off the write to table", from_mark=mark)
            # The crash below only exercises the replay of an unsealed parent's children if it
            # lands inside the window.
            await assert_resize_still_held(log, mark, parent_gid)
        except BaseException:
            # Only on the way out: the node is killed below with the resize still held, so the
            # success path deliberately leaves the injection on. A failure does not get there,
            # and would otherwise leave the node up with the sealing parked behind it, turning a
            # reported assertion into a teardown which blocks on the group0 guard.
            await release_resize_before_end_resize(manager, server)
            raise
        finally:
            # The node is about to be killed with the resize still held, so there is
            # nothing left to wait for here.
            split_task.cancel()
            try:
                await split_task
            except asyncio.CancelledError:
                pass

        logger.info("Crashing the node inside the resize window")
        await manager.server_stop(server.server_id, convict=False)
        mark = await log.mark()
        await manager.server_start(server.server_id)
        await reconnect_driver(manager)
        cql = manager.get_cql()

        # The child groups were replayed as groups whose parent is not done, i.e. their
        # entries were rewritten to the new commitlog rather than applied.
        assert await log.grep("created by a resize in progress", from_mark=mark), \
            "the child groups were not replayed as groups replacing an unsealed one"

        # The parent's start_resize was applied but not flushed - the seal, which is what flushes
        # the markers, never got past the window - so the row recording it went down with the
        # memtable and the replay is what puts it back. This is the only test which reaches that
        # branch: everywhere else the marker rows are already on disk by the time a node restarts.
        assert await log.grep(f"group {parent_gid}: replaying the start_resize marker",
                              from_mark=mark), \
            "the parent's start_resize marker was not applied by the commitlog replay"

        # The resize is resumed after the restart: the injection is gone, so the retried
        # sealing commits end_resize, which releases the child appliers.
        await wait_for_split(manager, server, ks, 'test', timeout=180)

        # The write which was acknowledged in the window is applied, not lost, and both
        # child groups work.
        rows = await cql.run_async(SimpleStatement(
            f"SELECT c FROM {table} WHERE pk = {written_key};",
            consistency_level=ConsistencyLevel.QUORUM))
        assert len(rows) == 1 and rows[0].c == new_value, \
            f"write acknowledged before the crash returned {rows}, expected c={new_value}"
        # Everything but written_key, whose value the write above replaced.
        await sc_check(cql, table, keys[1:])

        more_keys = list(range(256, 512))
        await sc_insert(cql, table, more_keys)
        await sc_check(cql, table, more_keys)


# A write which lands in the parent group while the split is in progress - after the storage
# split emptied the tablet's memtables, before the parent stops accepting writes - is applied to
# a memtable which is pinned by nothing but a handle into the parent's own commitlog entry.
# Finalization then takes the parent out of the tablet map, and the commitlog replay discards the
# entries of a group it cannot find there, so unless the data reached the sstables before the map
# was replaced, a crash right after the split loses an acknowledged write.
#
# Postpone the finalization so that such writes provably land in the parent, let the split
# complete, then kill the node without giving it a chance to flush anything on the way out.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_tablet_split_survives_crash_after_writes_to_parent(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    server = await manager.server_add(config=SC_SPLIT_CONFIG, cmdline=SC_SPLIT_CMDLINE)
    cql, _ = await manager.get_ready_cql([server])
    # Holds the finalization right after its barrier, so that the child groups exist while the
    # sealing has not started yet, and the writes below land in the parent's log rather than being
    # handed off to the children.
    injection = 'tablet_resize_finalization_post_barrier'
    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
            " AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = list(range(256))
        await sc_insert(cql, table, keys)

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()

        await manager.api.enable_injection(server.ip_addr, injection, one_shot=False)
        split_task = asyncio.create_task(split_sc_tablet(manager, server, cql, ks, 'test'))
        # Written while the tablet is being split, to the parent group: the children exist by
        # then, but nothing is handed off to them until the sealing starts.
        window_keys = list(range(1000, 1256))
        try:
            async def children_started():
                rows = await get_tablet_raft_rows(manager, ks, 'test')
                if len(rows) != 1 or rows[0].transition_raft_group_ids is None:
                    return None
                return [str(gid) for gid in rows[0].transition_raft_group_ids]
            child_gids = await wait_for(children_started, time.time() + 120)
            for child_gid in child_gids:
                await log.wait_for(f"group id {child_gid} is started", from_mark=mark)

            logger.info("Writing to the parent group while the split is in progress")
            await sc_insert(cql, table, window_keys)
            assert not await log.grep("handing off the write to table", from_mark=mark), \
                "the writes were handed off to the children instead of landing in the parent"
        finally:
            await manager.api.disable_injection(server.ip_addr, injection)
            await split_task

        assert await get_tablet_count(manager, server, ks, 'test') == 2

        logger.info("Crashing the node right after the split")
        await manager.server_stop(server.server_id, convict=False)
        await manager.server_start(server.server_id)
        await reconnect_driver(manager)
        cql = manager.get_cql()

        # The writes which were applied in the parent group are still there, even though the
        # replay of its commitlog entries had nowhere to put them.
        await sc_check(cql, table, keys + window_keys)

        more_keys = list(range(2000, 2256))
        await sc_insert(cql, table, more_keys)
        await sc_check(cql, table, keys + window_keys + more_keys)


# A child group's applier is parked inside apply(), waiting for the parent to be sealed, for the
# whole window between the no_op entry and end_resize. A raft server joins its applier fiber
# before it aborts anything the fiber could be waiting on, so a node shut down in that window
# can only complete its shutdown if the teardown interrupts the wait itself.
#
# Hold the window on the leader of the parent group and shut another replica down gracefully;
# without the interrupt its shutdown never completes. Then bring it back and let the split finish.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_tablet_split_graceful_shutdown_in_handoff_window(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    # The line which says that an applier is parked is a trace one, and this is the only test
    # which has to see it: the shutdown it checks is only meaningful once one is.
    cmdline = SC_SPLIT_CMDLINE + ['--logger-log-level', 'sc_state_machine=trace']
    servers = await manager.servers_add(3, config=SC_SPLIT_CONFIG, cmdline=cmdline,
                                        auto_rack_dc='my_dc')
    cql, _ = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    injection = 'sc_pause_before_end_resize'
    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
            " AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = list(range(256))
        await sc_insert(cql, table, keys)

        parent_gid = await get_parent_gid(manager, ks, 'test')

        leader_host_id = await wait_for_leader(manager, servers[0], parent_gid)
        leader_server = server_by_host_id(servers, host_ids, leader_host_id)
        logger.info(f"Parent group {parent_gid} is led by {leader_host_id}")
        # Shut down a replica which does not drive the resize, so that the shutdown does not
        # interfere with the sealing itself. Its child groups are parked all the same: the no_op
        # entries are committed in every replica of each child.
        other_server = [s for s in servers if s.server_id != leader_server.server_id][0]
        other_log = await manager.server_open_log(other_server.server_id)
        other_mark = await other_log.mark()

        leader_log = await manager.server_open_log(leader_server.server_id)
        mark = await leader_log.mark()

        await hold_resize_before_end_resize(manager, leader_server)
        # The split is only started here, not waited for: the wait comes after the node is back,
        # since the finalization cannot get past the barrier it starts with while one is down.
        assert await get_tablet_count(manager, servers[0], ks, 'test') == 1
        logger.info("Altering the table to require at least 2 tablets")
        await cql.run_async(f"ALTER TABLE {table} WITH tablets = {{'min_tablet_count': 2}}")
        stopped = False
        try:
            await manager.api.wait_for_injection_enter(leader_server.ip_addr, injection)
            # The appliers of the children are parked on the node about to be shut down.
            await other_log.wait_for("waiting for parent group to finish resizing",
                                     from_mark=other_mark)
            await assert_resize_still_held(leader_log, mark, parent_gid)

            # The check: without an interrupt for the parked appliers this never returns.
            logger.info(f"Shutting {other_server.server_id} down inside the window")
            await manager.server_stop_gracefully(other_server.server_id)
            stopped = True
        finally:
            # Released first: the sealing has nothing left to hold, and a held resize keeps the
            # topology coordinator busy while the node below is coming back.
            await release_resize_before_end_resize(manager, leader_server)
            if stopped:
                logger.info("Bringing the node back")
                await manager.server_start(other_server.server_id)

        await reconnect_driver(manager)
        cql = manager.get_cql()

        await wait_for_split(manager, servers[0], ks, 'test', timeout=300)
        await sc_check(cql, table, keys)
        await sc_write_and_check_both_sides(cql, table, keys)


# Restart a replica in the window between the last round of the sealing and the group0 write which
# replaces the tablet map, where the parent is sealed but still named by the metadata, and check
# that the finalization is picked up and finished afterwards. The replica has to rebuild the resize
# state there, and the markers are the only record of it left.
#
# The rows recording them are on disk by now - the round which applied end_resize flushed
# system.raft_groups before reporting success - so restore_applied_markers() reads them back when
# the parent's group starts, not the commitlog replay; the replay's own marker branch is covered by
# the test which crashes between the markers, before any of this is flushed.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_tablet_split_restart_after_sealing(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    server = await manager.server_add(config=SC_SPLIT_CONFIG, cmdline=SC_SPLIT_CMDLINE)
    cql, _ = await manager.get_ready_cql([server])
    injection = 'sc_pause_after_sealing_parents'
    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
            " AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = list(range(256))
        await sc_insert(cql, table, keys)

        parent_gid = await get_parent_gid(manager, ks, 'test')

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()

        await manager.api.enable_injection(server.ip_addr, injection, one_shot=False)
        split_task = asyncio.create_task(split_sc_tablet(manager, server, cql, ks, 'test'))
        try:
            await manager.api.wait_for_injection_enter(server.ip_addr, injection)
            # The parent is sealed, but the tablet map still names it.
            assert await log.grep(f"group {parent_gid}: end_resize applied", from_mark=mark), \
                "the parent was not sealed before the finalization was held"
            assert await get_tablet_count(manager, server, ks, 'test') == 1
        except BaseException:
            # See the same handler in test_sc_tablet_split_crash_in_handoff_window: only a failure
            # leaves the node alive with the finalization parked, and only a failure has to undo it.
            await manager.api.disable_injection(server.ip_addr, injection)
            raise
        finally:
            # The node is about to be killed with the finalization still held, so there is
            # nothing left to wait for here.
            split_task.cancel()
            try:
                await split_task
            except asyncio.CancelledError:
                pass

        logger.info("Crashing the node in the sealed-but-not-finalized window")
        await manager.server_stop(server.server_id, convict=False)
        mark = await log.mark()
        await manager.server_start(server.server_id)
        await reconnect_driver(manager)
        cql = manager.get_cql()

        # The parent's markers were in its commitlog only, so the replay had to apply them as
        # marker entries and rebuild the resize state from what it wrote.
        assert await log.grep(f"group {parent_gid}: start_resize applied", from_mark=mark), \
            "the parent's markers were not applied by the commitlog replay"

        # The finalization is resumed after the restart: the injection is gone, so the retried
        # sealing finds the parent sealed and replaces the tablet map.
        await wait_for_split(manager, server, ks, 'test', timeout=180)

        await sc_check(cql, table, keys)
        await sc_write_and_check_both_sides(cql, table, keys)


# The sealing is driven by the topology coordinator, and the coordinator can move to another node
# in the middle of it. The successor picks the finalization up from the topology state, re-plans
# it, and drives the sealing to the end - which is safe because every step of the sealing is
# idempotent.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_tablet_split_coordinator_failover_mid_seal(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    servers = await manager.servers_add(3, config=SC_SPLIT_CONFIG, cmdline=SC_SPLIT_CMDLINE,
                                        auto_rack_dc='my_dc')
    cql, _ = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    injection = 'sc_pause_before_end_resize'
    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
            " AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = list(range(256))
        await sc_insert(cql, table, keys)

        parent_gid = await get_parent_gid(manager, ks, 'test')

        leader_host_id = await wait_for_leader(manager, servers[0], parent_gid)
        leader_server = server_by_host_id(servers, host_ids, leader_host_id)
        leader_log = await manager.server_open_log(leader_server.server_id)
        mark = await leader_log.mark()

        coordinator_host_id = await get_topology_coordinator(manager)
        coordinator_server = server_by_host_id(servers, host_ids, coordinator_host_id)
        logger.info(f"Parent group {parent_gid} is led by {leader_host_id}, "
                    f"the topology coordinator runs on {coordinator_host_id}")

        await hold_resize_before_end_resize(manager, leader_server)
        split_task = asyncio.create_task(split_sc_tablet(manager, servers[0], cql, ks, 'test'))
        try:
            await manager.api.wait_for_injection_enter(leader_server.ip_addr, injection)
            await assert_resize_still_held(leader_log, mark, parent_gid)

            logger.info("Making the topology coordinator step down mid-seal")
            await trigger_stepdown(manager, coordinator_server)

            async def coordinator_changed():
                new_coordinator = await get_topology_coordinator(manager)
                return new_coordinator if str(new_coordinator) != str(coordinator_host_id) else None
            new_coordinator_host_id = await wait_for(coordinator_changed, time.time() + 120)
            logger.info(f"The topology coordinator now runs on {new_coordinator_host_id}")
        finally:
            await release_resize_before_end_resize(manager, leader_server)
            await split_task

        # The successor finished what its predecessor started.
        assert await get_tablet_count(manager, servers[0], ks, 'test') == 2
        await sc_check(cql, table, keys)
        await sc_write_and_check_both_sides(cql, table, keys)


# A request handed off to a child group can only be served where the child's leader is co-located
# with the parent's, so a child led by another node bounces it back to the parent, where it is
# retried until the co-location is restored. Break the co-location deliberately, issue a write and
# a linearizable read into it, and check that both are still answered correctly. Whether either
# actually bounces is left unasserted: it depends on how quickly the colocator brings the
# leadership back, and the bounce is only counted in the log.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_tablet_split_request_while_leaders_diverged(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    # A bouncing request is retried until the leaders are back together, which the colocator does
    # in its own time; with the request timeout the harness sets it could give up first, and this
    # test is about what it does while it waits, not about that deadline.
    config = SC_SPLIT_CONFIG | {'write_request_timeout_in_ms': 60000,
                                'read_request_timeout_in_ms': 60000}
    servers = await manager.servers_add(3, config=config, cmdline=SC_SPLIT_CMDLINE,
                                        auto_rack_dc='my_dc')
    cql, _ = await manager.get_ready_cql(servers)
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])
    injection = 'sc_pause_before_end_resize'
    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
            " AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = list(range(256))
        await sc_insert(cql, table, keys)

        parent_gid = await get_parent_gid(manager, ks, 'test')

        leader_host_id = await wait_for_leader(manager, servers[0], parent_gid)
        leader_server = server_by_host_id(servers, host_ids, leader_host_id)
        leader_log = await manager.server_open_log(leader_server.server_id)
        mark = await leader_log.mark()
        logger.info(f"Parent group {parent_gid} is led by {leader_host_id}")

        await hold_resize_before_end_resize(manager, leader_server)
        split_task = asyncio.create_task(split_sc_tablet(manager, servers[0], cql, ks, 'test'))
        write_task = read_task = None
        try:
            await manager.api.wait_for_injection_enter(leader_server.ip_addr, injection)
            # The writes are handed off from here on, so a request reaching the parent's leader
            # now goes to a child.
            await leader_log.wait_for("group .*: start_resize applied", from_mark=mark)

            # Take the child groups away from the node leading the parent. The colocator brings
            # them back, so the requests below may find them either way - what matters is that
            # every request is answered whatever it finds.
            child_gids = [str(gid) for gid in
                          (await get_tablet_raft_rows(manager, ks, 'test'))[0].transition_raft_group_ids]
            logger.info(f"Moving the leadership of {child_gids} away from the parent's leader")
            for child_gid in child_gids:
                await manager.api.client.post("/raft/trigger_stepdown",
                                              host=leader_server.ip_addr,
                                              params={"group_id": child_gid})

            written_key, read_key = keys[0], keys[1]
            new_value = written_key + 100000
            write_task = asyncio.ensure_future(cql.run_async(SimpleStatement(
                f"UPDATE {table} SET c = {new_value} WHERE pk = {written_key};",
                consistency_level=ConsistencyLevel.QUORUM)))
            read_task = asyncio.ensure_future(cql.run_async(SimpleStatement(
                f"SELECT c FROM {table} WHERE pk = {read_key};",
                consistency_level=ConsistencyLevel.QUORUM)))

            # Both requests are handed off to a child; whenever the child is led elsewhere they
            # bounce back to the parent and are retried there. Whether a given request sees that
            # depends on how quickly the colocator brings the leadership back, so the bounces are
            # reported below rather than waited for - what this pins is that a request issued
            # while the leaders are apart is answered at all, and answered correctly.
            await leader_log.wait_for("mutate\\(\\): handing off the write to table", from_mark=mark)
            await leader_log.wait_for("query\\(\\): handing off the read of table", from_mark=mark)
            await assert_resize_still_held(leader_log, mark, parent_gid)

            # Neither request can complete before the parent is sealed: a child which was elected
            # after its no_op went in seeds its clock from a read barrier, and that barrier is held
            # back for as long as the parent is not done. Releasing the resize is what lets both
            # through - the point of the test is that they are still there to be let through.
            bounces = await leader_log.grep("got not_a_leader .*handoff_group", from_mark=mark)
            logger.info(f"{len(bounces)} request(s) bounced back to the parent")
        finally:
            await release_resize_before_end_resize(manager, leader_server)
            await split_task

        # Awaited outside the cleanup above, and both of them whatever either does, so that a
        # failing write cannot leave the read unawaited and skip the assertions this test exists
        # for - or report itself as the read's failure.
        write_result, read_result = await asyncio.gather(write_task, read_task,
                                                        return_exceptions=True)
        assert not isinstance(write_result, BaseException), \
            f"write issued while the leaders were diverged failed: {write_result}"
        assert not isinstance(read_result, BaseException), \
            f"read issued while the leaders were diverged failed: {read_result}"
        rows = read_result

        assert len(rows) == 1 and rows[0].c == read_key, \
            f"read issued while the leaders were diverged returned {rows}, expected c={read_key}"
        rows = await cql.run_async(SimpleStatement(
            f"SELECT c FROM {table} WHERE pk = {written_key};",
            consistency_level=ConsistencyLevel.QUORUM))
        assert len(rows) == 1 and rows[0].c == new_value, \
            f"write issued while the leaders were diverged returned {rows}, expected c={new_value}"

        # Everything but written_key, whose value the write above replaced.
        await sc_check(cql, table, keys[1:])


# A linearizable read checks whether the parent handed its writes off twice: once before its read
# barrier and once after, because the barrier itself may be what applies the start_resize marker.
# Hold a read between the two checks, let the sealing start under it, and verify that the second
# check is the one which moves it to a child.
#
# The read has to be started after the finalization's own barrier: it keeps a reference to the
# effective replication map for as long as it runs, and that barrier waits for exactly those to
# go away, so a read parked across it would hold the finalization up instead.
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_sc_tablet_split_read_handed_off_after_barrier(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    server = await manager.server_add(config=SC_SPLIT_CONFIG, cmdline=SC_SPLIT_CMDLINE)
    cql, _ = await manager.get_ready_cql([server])
    hold_read = 'sc_coordinator_wait_after_query_handoff_check'
    post_barrier = 'tablet_resize_finalization_post_barrier'
    async with new_test_keyspace(manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
            " AND tablets = {'initial': 1} AND consistency = 'global'") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = list(range(256))
        await sc_insert(cql, table, keys)

        parent_gid = await get_parent_gid(manager, ks, 'test')

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()

        read_key = keys[1]
        read_task = None
        split_task = None
        await manager.api.enable_injection(server.ip_addr, post_barrier, one_shot=True)
        try:
            split_task = asyncio.create_task(split_sc_tablet(manager, server, cql, ks, 'test'))
            await manager.api.wait_for_injection_enter(server.ip_addr, post_barrier)

            # Park a read after its first handoff check, while nothing is handed off yet.
            await manager.api.enable_injection(server.ip_addr, hold_read, one_shot=True)
            read_task = asyncio.ensure_future(cql.run_async(SimpleStatement(
                f"SELECT c FROM {table} WHERE pk = {read_key};",
                consistency_level=ConsistencyLevel.QUORUM)))
            await manager.api.wait_for_injection_enter(server.ip_addr, hold_read)

            # Let the sealing start under the parked read, so that the handoff begins between its
            # two checks.
            await hold_resize_before_end_resize(manager, server)
            await manager.api.message_injection(server.ip_addr, post_barrier)
            await manager.api.wait_for_injection_enter(server.ip_addr, 'sc_pause_before_end_resize')
            await assert_resize_still_held(log, mark, parent_gid)
        finally:
            await manager.api.disable_injection(server.ip_addr, post_barrier)
            await manager.api.message_injection(server.ip_addr, hold_read)
            await release_resize_before_end_resize(manager, server)
            if split_task:
                await split_task

        # The read was moved to a child by the check which runs after the read barrier, and it is
        # answered once the parent is sealed.
        rows = await read_task
        assert len(rows) == 1 and rows[0].c == read_key, \
            f"read held across the start of the handoff returned {rows}, expected c={read_key}"
        assert await log.grep("handing off the read of table .* after the read barrier",
                              from_mark=mark), \
            "the read was not handed off by the check which follows the read barrier"

        await sc_check(cql, table, keys)
