#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.rest_client import inject_error_one_shot
from test.cluster.util import check_token_ring_and_group0_consistency, new_test_keyspace

import pytest
import asyncio
import logging

logger = logging.getLogger(__name__)
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_cleanup_stop(manager: ScyllaClusterManager):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'compaction_manager=debug',
        '--smp', '1',
    ]
    servers = [await manager.server_add(cmdline=cmdline)]

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': 'false'};") as ks:
        table = f"{ks}.test"
        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, c int);")

        keys = range(100)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {table} (pk, c) VALUES ({k}, {k});") for k in keys])
        async def check(expected_keys):
            logger.info("Checking table")
            cql = manager.get_cql()
            rows = await cql.run_async(f"SELECT * FROM {table};")
            assert len(rows) == len(expected_keys)
            for r in rows:
                assert r.c == r.pk

        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        await check(keys)

        await inject_error_one_shot(manager.api, servers[0].ip_addr, "sstable_cleanup_wait")
        cleanup_task = asyncio.create_task(manager.api.cleanup_keyspace(servers[0].ip_addr, ks))

        await manager.api.wait_for_injection_enter(servers[0].ip_addr, "sstable_cleanup_wait")

        stop_cleanup = asyncio.create_task(manager.api.stop_compaction(servers[0].ip_addr, "CLEANUP"))
        await asyncio.sleep(1)

        await manager.api.message_injection(servers[0].ip_addr, "sstable_cleanup_wait")
        await stop_cleanup
        caught_exception = False
        try:
            await cleanup_task
        except Exception as e:
            caught_exception = True
            logger.info(f"Exception: {e}")

        await check(keys)
        assert caught_exception == True


CMDLINE = [
    '--logger-log-level', 'compaction_manager=debug',
    '--smp', '1',
]

VNODES_KS_OPTS = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'enabled': 'false'};"
TABLETS_KS_OPTS = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};"


async def create_and_fill(cql, manager: ScyllaClusterManager, server, ks: str) -> None:
    """Create a single table in ks, fill it and flush it, so maintenance operations have something to do."""
    await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in range(100)])
    await manager.api.flush_keyspace(server.ip_addr, ks)


async def park_major(manager: ScyllaClusterManager, server, ks: str):
    """Park a major compaction of ks in the major_compaction_wait injection, which sits after
       the major executor took the shard's single maintenance operation semaphore unit.
       The injection is not one-shot, so every compaction group of ks parks.
    """
    await manager.api.enable_injection(server.ip_addr, "major_compaction_wait", one_shot=False)
    major = asyncio.create_task(manager.api.keyspace_compaction(server.ip_addr, ks))
    await manager.api.wait_for_injection_enter(server.ip_addr, "major_compaction_wait")
    return major


async def start_parked_topology_cleanup(manager: ScyllaClusterManager, server, log, mark):
    """Trigger the vnodes cleanup fiber with one of its per-keyspace cleanup jobs parked after
       it took its permit, so that a second per-keyspace cleanup queues on the topology cleanup
       semaphore and says so.
    """
    await inject_error_one_shot(manager.api, server.ip_addr, "sstable_cleanup_wait")

    cleanup = asyncio.create_task(manager.api.cleanup_all(server.ip_addr))
    # Assert the fiber path is the one being exercised, rather than a user cleanup.
    await log.wait_for("raft_topology - start vnodes_cleanup", from_mark=mark, timeout=60)
    await manager.api.wait_for_injection_enter(server.ip_addr, "sstable_cleanup_wait")
    await log.wait_for("waiting for topology cleanup", from_mark=mark, timeout=60)
    return cleanup


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_topology_cleanup_not_blocked_by_user_major(manager: ScyllaClusterManager):
    """A cleanup started by the vnodes cleanup fiber must complete even while a user major
       compaction holds the shard's maintenance operation semaphore for the whole duration.
    """
    server_a = await manager.server_add(cmdline=CMDLINE)
    await manager.disable_tablet_balancing()

    cql = manager.get_cql()

    async with new_test_keyspace(manager, TABLETS_KS_OPTS) as tablets_ks:
        await create_and_fill(cql, manager, server_a, tablets_ks)

        # The fiber skips tablets keyspaces, so the cleanup under test never stops this
        # major and the maintenance operation semaphore stays held throughout.
        major = await park_major(manager, server_a, tablets_ks)

        async with new_test_keyspace(manager, VNODES_KS_OPTS) as vnodes_ks:
            await create_and_fill(cql, manager, server_a, vnodes_ks)

            log_a = await manager.server_open_log(server_a.server_id)
            mark = await log_a.mark()

            # Take ranges away from A, so its sstables really hold out-of-range data and the
            # cleanup reaches the executor instead of returning before it.
            await manager.server_add(cmdline=CMDLINE)
            await check_token_ring_and_group0_consistency(manager)

            cleanup = asyncio.create_task(manager.api.cleanup_all(server_a.ip_addr))
            await log_a.wait_for("raft_topology - start vnodes_cleanup", from_mark=mark, timeout=60)
            await log_a.wait_for(f"vnodes_cleanup {vnodes_ks} finished", from_mark=mark, timeout=60)
            await log_a.wait_for("vnodes_cleanup ended", from_mark=mark, timeout=60)
            await cleanup

            # The cleanup really got as far as the executor.
            assert len(await log_a.grep("The following SSTables require cleanup", from_mark=mark)) > 0
            # The major was neither released nor stopped while the cleanup ran.
            assert len(await log_a.grep("major_compaction_wait: released", from_mark=mark)) == 0
            assert len(await log_a.grep(f"Compaction for {tablets_ks}/test was stopped", from_mark=mark)) == 0

            await manager.api.message_injection(server_a.ip_addr, "major_compaction_wait")
            await asyncio.wait_for(major, timeout=120)


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_maintenance_semaphore_wait_is_logged(manager: ScyllaClusterManager):
    """A contended acquisition of the maintenance operation semaphore is logged, an uncontended
       one is not, and a user cleanup still queues on that semaphore.
    """
    server_a = await manager.server_add(cmdline=CMDLINE)
    await manager.disable_tablet_balancing()

    cql = manager.get_cql()

    async with new_test_keyspace(manager, TABLETS_KS_OPTS) as tablets_ks:
        await create_and_fill(cql, manager, server_a, tablets_ks)

        log_a = await manager.server_open_log(server_a.server_id)
        mark = await log_a.mark()

        major = await park_major(manager, server_a, tablets_ks)

        # Nothing else held the semaphore, so the major took it without waiting and said nothing.
        assert len(await log_a.grep("waiting for maintenance operation", from_mark=mark)) == 0
        assert len(await log_a.grep("acquired maintenance operation after waiting for", from_mark=mark)) == 0

        async with new_test_keyspace(manager, VNODES_KS_OPTS) as vnodes_ks:
            await create_and_fill(cql, manager, server_a, vnodes_ks)

            # Take ranges away from A, so the user cleanup below reaches the executor.
            await manager.server_add(cmdline=CMDLINE)
            await check_token_ring_and_group0_consistency(manager)

            scrub_mark = await log_a.mark()
            # SKIP rewrites the sstables, so the scrub queues on the maintenance operation
            # semaphore. VALIDATE would take no semaphore at all.
            scrub = asyncio.create_task(manager.api.keyspace_scrub_sstables(server_a.ip_addr, vnodes_ks, "SKIP"))
            await log_a.wait_for("waiting for maintenance operation", from_mark=scrub_mark, timeout=60)

            # A user cleanup over REST must keep queuing on the same semaphore.
            cleanup_mark = await log_a.mark()
            user_cleanup = asyncio.create_task(manager.api.cleanup_keyspace(server_a.ip_addr, vnodes_ks))
            await log_a.wait_for(r"Cleanup task .*: waiting for maintenance operation", from_mark=cleanup_mark, timeout=60)
            assert not user_cleanup.done()

            await manager.api.message_injection(server_a.ip_addr, "major_compaction_wait")
            await asyncio.wait_for(major, timeout=120)
            await asyncio.wait_for(scrub, timeout=120)
            await asyncio.wait_for(user_cleanup, timeout=120)

            assert len(await log_a.grep("acquired maintenance operation after waiting for", from_mark=scrub_mark)) > 0


@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_topology_cleanups_serialize_among_themselves(manager: ScyllaClusterManager):
    """Topology cleanups of different vnode keyspaces still run one at a time per shard."""
    server_a = await manager.server_add(cmdline=CMDLINE)

    cql = manager.get_cql()

    async with new_test_keyspace(manager, VNODES_KS_OPTS) as ks1, new_test_keyspace(manager, VNODES_KS_OPTS) as ks2:
        await create_and_fill(cql, manager, server_a, ks1)
        await create_and_fill(cql, manager, server_a, ks2)

        log_a = await manager.server_open_log(server_a.server_id)
        mark = await log_a.mark()

        # Take ranges away from A, so both keyspaces really require cleanup.
        await manager.server_add(cmdline=CMDLINE)
        await check_token_ring_and_group0_consistency(manager)

        cleanup = await start_parked_topology_cleanup(manager, server_a, log_a, mark)

        await manager.api.message_injection(server_a.ip_addr, "sstable_cleanup_wait")
        await log_a.wait_for("vnodes_cleanup ended", from_mark=mark, timeout=120)
        await cleanup
