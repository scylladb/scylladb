#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from cassandra.query import SimpleStatement, ConsistencyLevel
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import HTTPError, read_barrier
from test.pylib.tablets import get_tablet_replica, get_all_tablet_replicas, get_tablet_info
from test.topology.conftest import skip_mode
from test.topology.util import wait_for_cql_and_get_hosts, new_test_keyspace, reconnect_driver, wait_for
import time
import pytest
import random
import logging
import asyncio
import os
import glob

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("action", ['move', 'add_replica', 'del_replica'])
@pytest.mark.asyncio
async def test_tablet_transition_sanity(manager: ManagerClient, action):
    logger.info("Bootstrapping cluster")
    cfg = {'enable_user_defined_functions': False, 'tablets_mode_for_new_keyspaces': 'enabled'}
    host_ids = []
    servers = []

    async def make_server():
        s = await manager.server_add(config=cfg)
        servers.append(s)
        host_ids.append(await manager.get_host_id(s.server_id))
        await manager.api.disable_tablet_balancing(s.ip_addr)

    await make_server()
    await make_server()
    await make_server()

    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        keys = range(256)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])

        replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        logger.info(f"Tablet is on [{replicas}]")
        assert len(replicas) == 1 and len(replicas[0].replicas) == 2
        old_replica = replicas[0].replicas[0]
        replicas = [ r[0] for r in replicas[0].replicas ]
        for h in host_ids:
            if h not in replicas:
                new_replica = (h, 0)
                break
        else:
            assert False, "Cannot find node without replica"

        if action == 'move':
            logger.info(f"Move tablet {old_replica[0]} -> {new_replica[0]}")
            await manager.api.move_tablet(servers[0].ip_addr, ks, "test", old_replica[0], old_replica[1], new_replica[0], new_replica[1], 0)
        if action == 'add_replica':
            logger.info(f"Adding replica to tablet, host {new_replica[0]}")
            await manager.api.add_tablet_replica(servers[0].ip_addr, ks, "test", new_replica[0], new_replica[1], 0)
        if action == 'del_replica':
            logger.info(f"Deleting replica from tablet, host {old_replica[0]}")
            await manager.api.del_tablet_replica(servers[0].ip_addr, ks, "test", old_replica[0], old_replica[1], 0)

        replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        logger.info(f"Tablet is now on [{replicas}]")
        assert len(replicas) == 1
        replicas = [ r[0] for r in replicas[0].replicas ]
        if action == 'move':
            assert len(replicas) == 2
            assert new_replica[0] in replicas
            assert old_replica[0] not in replicas
        if action == 'add_replica':
            assert len(replicas) == 3
            assert old_replica[0] in replicas
            assert new_replica[0] in replicas
        if action == 'del_replica':
            assert len(replicas) == 1
            assert old_replica[0] not in replicas

        for h, s in zip(host_ids, servers):
            host = await wait_for_cql_and_get_hosts(cql, [s], time.time() + 30)
            if h != host_ids[0]:
                await read_barrier(manager.api, host[0].address)  # host-0 did the barrier in get_all_tablet_replicas above
            res = await cql.run_async(f"SELECT COUNT(*) FROM MUTATION_FRAGMENTS({ks}.test)", host=host[0])
            logger.info(f"Host {h} reports {res} as mutation fragments count")
            if h in replicas:
                assert res[0].count != 0
            else:
                assert res[0].count == 0


@pytest.mark.parametrize("fail_replica", ["source", "destination"])
@pytest.mark.parametrize("fail_stage", ["streaming", "allow_write_both_read_old", "write_both_read_old", "write_both_read_new", "use_new", "cleanup", "cleanup_target", "end_migration", "revert_migration"])
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_node_failure_during_tablet_migration(manager: ManagerClient, fail_replica, fail_stage):
    if fail_stage == 'cleanup' and fail_replica == 'destination':
        pytest.skip('Failing destination during cleanup is pointless')
    if fail_stage == 'cleanup_target' and fail_replica == 'source':
        pytest.skip('Failing source during target cleanup is pointless')

    logger.info("Bootstrapping cluster")
    cfg = {'enable_user_defined_functions': False, 'tablets_mode_for_new_keyspaces': 'enabled', 'failure_detector_timeout_in_ms': 2000}
    host_ids = []
    servers = []

    async def make_server():
        s = await manager.server_add(config=cfg)
        servers.append(s)
        host_ids.append(await manager.get_host_id(s.server_id))
        await manager.api.disable_tablet_balancing(s.ip_addr)

    await make_server()
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 1}") as ks:
        await make_server()
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        keys = range(256)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])
        await make_server()

        if fail_stage in ["cleanup_target", "revert_migration"]:
            # we'll stop 2 servers, group0 quorum should be there
            #
            # it seems that we need five nodes to have three remaining, but
            # when removing the 1st node it will be marked as non-voter so to
            # remove the 2nd node just two remaining will be enough
            #
            # also this extra node will be used to call removenode on
            # removing the 1st node will wait for the operation to go through
            # raft log, and it will not finish before tablet migration. An
            # attempt to remove the 2nd node, to make cleanup_target stage
            # go ahead, will step on the legacy API lock on storage_service,
            # so we need to ask some other node to do it
            await make_server()

        logger.info(f"Cluster is [{host_ids}]")

        replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        logger.info(f"Tablet is on [{replicas}]")
        assert len(replicas) == 1 and len(replicas[0].replicas) == 2

        last_token = replicas[0].last_token
        old_replica = None
        for r in replicas[0].replicas:
            assert r[0] != host_ids[2], "Tablet got migrated to node2"
            if r[0] == host_ids[1]:
                old_replica = r
        assert old_replica is not None
        new_replica = (host_ids[2], 0)
        logger.info(f"Moving tablet {old_replica} -> {new_replica}")

        class node_failer:
            def __init__(self, stage, replica, ks):
                self.stage = stage
                self.replica = replica
                self.fail_idx = 1 if self.replica == "source" else 2
                self.ks = ks

            async def setup(self):
                logger.info(f"Will fail {self.stage}")
                if self.stage == "streaming":
                    await manager.api.enable_injection(servers[2].ip_addr, "stream_mutation_fragments", one_shot=True)
                    self.log = await manager.server_open_log(servers[2].server_id)
                    self.mark = await self.log.mark()
                elif self.stage in [ "allow_write_both_read_old", "write_both_read_old", "write_both_read_new", "use_new", "end_migration", "do_revert_migration" ]:
                    await manager.api.enable_injection(servers[self.fail_idx].ip_addr, "raft_topology_barrier_and_drain_fail", one_shot=False,
                            parameters={'keyspace': self.ks, 'table': 'test', 'last_token': last_token, 'stage': self.stage.removeprefix('do_')})
                    self.log = await manager.server_open_log(servers[self.fail_idx].server_id)
                    self.mark = await self.log.mark()
                elif self.stage == "cleanup":
                    await manager.api.enable_injection(servers[self.fail_idx].ip_addr, "cleanup_tablet_crash", one_shot=True)
                    self.log = await manager.server_open_log(servers[self.fail_idx].server_id)
                    self.mark = await self.log.mark()
                elif self.stage == "cleanup_target":
                    assert self.fail_idx == 2
                    self.stream_fail = node_failer('streaming', 'source', ks)
                    await self.stream_fail.setup()
                    self.cleanup_fail = node_failer('cleanup', 'destination', ks)
                    await self.cleanup_fail.setup()
                elif self.stage == "revert_migration":
                    self.wbro_fail = node_failer('write_both_read_old', 'source' if self.replica == 'destination' else 'destination', ks)
                    await self.wbro_fail.setup()
                    self.revert_fail = node_failer('do_revert_migration', self.replica, ks)
                    await self.revert_fail.setup()
                else:
                    assert False, f"Unknown stage {self.stage}"

            async def wait(self):
                logger.info(f"Wait for {self.stage} to happen")
                if self.stage == "streaming":
                    await self.log.wait_for('stream_mutation_fragments: waiting', from_mark=self.mark)
                elif self.stage in [ "allow_write_both_read_old", "write_both_read_old", "write_both_read_new", "use_new", "end_migration", "do_revert_migration" ]:
                    await self.log.wait_for('raft_topology_cmd: barrier handler waits', from_mark=self.mark);
                elif self.stage == "cleanup":
                    await self.log.wait_for('Crashing tablet cleanup', from_mark=self.mark)
                elif self.stage == "cleanup_target":
                    await self.stream_fail.wait()
                    self.stream_stop_task = asyncio.create_task(self.stream_fail.stop())
                    await self.cleanup_fail.wait()
                elif self.stage == "revert_migration":
                    await self.wbro_fail.wait()
                    self.wbro_fail_task = asyncio.create_task(self.wbro_fail.stop())
                    await self.revert_fail.wait()
                else:
                    assert False

            async def stop(self, via=0):
                if self.stage == "cleanup_target":
                    await self.cleanup_fail.stop(via=3) # removenode of source is happening via node0 already
                    await self.stream_stop_task
                    return
                if self.stage == "revert_migration":
                    await self.revert_fail.stop(via=3)
                    await self.wbro_fail_task
                    return

                logger.info(f"Stop {self.replica} {host_ids[self.fail_idx]}")
                await manager.server_stop(servers[self.fail_idx].server_id)
                logger.info(f"Remove {self.replica} {host_ids[self.fail_idx]} via {host_ids[via]}")
                await manager.remove_node(servers[via].server_id, servers[self.fail_idx].server_id)
                logger.info(f"Done with {self.replica} {host_ids[self.fail_idx]}")


        failer = node_failer(fail_stage, fail_replica, ks)
        await failer.setup()
        migration_task = asyncio.create_task(
            manager.api.move_tablet(servers[0].ip_addr, ks, "test", old_replica[0], old_replica[1], new_replica[0], new_replica[1], 0))
        await failer.wait()
        await failer.stop()

        logger.info("Done, waiting for migration to finish")
        await migration_task

        replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        logger.info(f"Tablet is now on [{replicas}]")
        assert len(replicas) == 1
        for r in replicas[0].replicas:
            assert r[0] != host_ids[failer.fail_idx]

        # For dropping the keyspace after the node failure
        await reconnect_driver(manager)

@pytest.mark.asyncio
async def test_tablet_back_and_forth_migration(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cfg = {'enable_user_defined_functions': False, 'tablets_mode_for_new_keyspaces': 'enabled'}
    host_ids = []
    servers = []

    async def make_server():
        s = await manager.server_add(config=cfg)
        servers.append(s)
        host_ids.append(await manager.get_host_id(s.server_id))
        await manager.api.disable_tablet_balancing(s.ip_addr)

    async def assert_rows(num):
        res = await cql.run_async(f"SELECT * FROM {ks}.test")
        assert len(res) == num

    await make_server()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await make_server()

        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({1}, {1});")
        await assert_rows(1)

        replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
        logger.info(f"Tablet is on [{replicas}]")
        assert len(replicas) == 1 and len(replicas[0].replicas) == 1

        old_replica = replicas[0].replicas[0]
        assert old_replica[0] != host_ids[1]
        new_replica = (host_ids[1], 0)

        logger.info(f"Moving tablet {old_replica} -> {new_replica}")
        manager.api.move_tablet(servers[0].ip_addr, ks, "test", old_replica[0], old_replica[1], new_replica[0], new_replica[1], 0)

        await assert_rows(1)
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({2}, {2});")
        await assert_rows(2)

        logger.info(f"Moving tablet {new_replica} -> {old_replica}")
        manager.api.move_tablet(servers[0].ip_addr, ks, "test", new_replica[0], new_replica[1], old_replica[0], old_replica[1], 0)

        await assert_rows(2)
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({3}, {3});")
        await assert_rows(3)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_staging_backlog_is_preserved_with_file_based_streaming(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    # the error injection will halt view updates from staging, allowing migration to transfer the view update backlog.
    cfg = {'enable_user_defined_functions': False, 'tablets_mode_for_new_keyspaces': 'enabled',
           'error_injections_at_startup': ['view_update_generator_consume_staging_sstable']}
    servers = [await manager.server_add(config=cfg)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")
        await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv1 AS \
            SELECT * FROM {ks}.test WHERE pk IS NOT NULL AND c IS NOT NULL \
            PRIMARY KEY (c, pk);")

        logger.info("Populating single tablet")
        keys = range(256)
        await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])

        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        # check
        async def check(expected):
            rows = await cql.run_async(f"SELECT pk from {ks}.test")
            assert len(list(rows)) == len(expected)
        await check(keys)

        logger.info("Adding new server")
        servers.append(await manager.server_add(config=cfg))

        async def get_table_dir(manager, server_id):
            node_workdir = await manager.server_get_workdir(server_id)
            return glob.glob(os.path.join(node_workdir, "data", ks, "test-*"))[0]

        s0_table_dir = await get_table_dir(manager, servers[0].server_id)
        logger.info(f"Table dir in server 0: {s0_table_dir}")

        s1_table_dir = await get_table_dir(manager, servers[1].server_id)
        logger.info(f"Table dir in server 1: {s1_table_dir}")

        # Explicitly close the driver to avoid reconnections if scylla fails to update gossiper state on shutdown.
        # It's a problem until https://github.com/scylladb/scylladb/issues/15356 is fixed.
        manager.driver_close()
        cql = None
        await manager.server_stop_gracefully(servers[0].server_id)

        def move_sstables_to_staging(table_dir: str):
            table_staging_dir = os.path.join(table_dir, "staging")
            logger.info(f"Moving sstables to staging dir: {table_staging_dir}")
            for sst in glob.glob(os.path.join(table_dir, "*-Data.db")):
                for src_path in glob.glob(os.path.join(table_dir, sst.removesuffix("-Data.db") + "*")):
                    dst_path = os.path.join(table_staging_dir, os.path.basename(src_path))
                    logger.info(f"Moving sstable file {src_path} to {dst_path}")
                    os.rename(src_path, dst_path)

        def sstable_count_in_staging(table_dir: str):
            table_staging_dir = os.path.join(table_dir, "staging")
            return len(glob.glob(os.path.join(table_staging_dir, "*-Data.db")))

        move_sstables_to_staging(s0_table_dir)
        s0_sstables_in_staging = sstable_count_in_staging(s0_table_dir)

        await manager.server_start(servers[0].server_id)
        cql = manager.get_cql()
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        tablet_token = 0 # Doesn't matter since there is one tablet
        replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)
        s1_host_id = await manager.get_host_id(servers[1].server_id)
        dst_shard = 0

        migration_task = asyncio.create_task(
            manager.api.move_tablet(servers[0].ip_addr, ks, "test", replica[0], replica[1], s1_host_id, dst_shard, tablet_token))

        logger.info("Waiting for migration to finish")
        await migration_task
        logger.info("Migration done")

        # FIXME: After https://github.com/scylladb/scylladb/issues/19149 is fixed, we can check that view updates complete
        #   after migration and then check for base-view consistency. By the time being, we only check that backlog is
        #   transferred by looking at staging directory.

        s1_sstables_in_staging = sstable_count_in_staging(s1_table_dir)
        logger.info(f"SSTable count in staging dir of server 1: {s1_sstables_in_staging}")

        logger.info("Allowing view update generator to progress again")
        for server in servers:
            manager.api.disable_injection(server.ip_addr, 'view_update_generator_consume_staging_sstable')

        assert s0_sstables_in_staging > 0
        assert s0_sstables_in_staging == s1_sstables_in_staging

        await check(keys)

@pytest.mark.asyncio
@pytest.mark.parametrize("migration_stage_and_injection", [("cleanup", "cleanup_tablet_wait"), ("end_migration", "handle_tablet_migration_end_migration")], ids=["cleanup", "end_migration"])
@skip_mode('release', 'error injections are not supported in release mode')
async def test_restart_leaving_replica_during_cleanup(manager: ManagerClient, migration_stage_and_injection):
    """
    Migrate a tablet from one node to another, and while in some migration
    cleanup stage, either before or after the tablet is cleaned, restart the
    leaving replica. Then trigger a tablet merge.

    This reproduces issue #23481: when the leaving replica is restarted in
    end_migration, after the SSTables are cleaned, it starts and allocates the
    state for the tablet in the storage group manager, even though it was
    cleaned already. The resulting state causes the following merge process to
    fail with an assert.
    """
    stage, injection = migration_stage_and_injection

    cmdline = ['--target-tablet-size-in-bytes', '30000',]
    cfg = {'error_injections_at_startup': ['short_tablet_stats_refresh_interval']}
    servers = await manager.servers_add(2, config=cfg, cmdline=cmdline)

    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c blob) WITH gc_grace_seconds=0 AND bloom_filter_fp_chance=1")

        s1_log = await manager.server_open_log(servers[0].server_id)
        s1_mark = await s1_log.mark()

        total_keys = 200
        keys = range(total_keys)
        insert = cql.prepare(f"INSERT INTO {ks}.test(pk, c) VALUES(?, ?)")
        for pk in keys:
            value = random.randbytes(2000)
            cql.execute(insert, [pk, value])

        await asyncio.gather(*[manager.api.flush_keyspace(s.ip_addr, ks) for s in servers])

        table_id = await manager.get_table_id(ks, 'test')
        async def get_tablet_count():
            rows = await manager.cql.run_async(f"SELECT tablet_count FROM system.tablets where table_id = {table_id}")
            return rows[0].tablet_count

        await s1_log.wait_for('Detected tablet split for table', from_mark=s1_mark)

        await manager.api.disable_tablet_balancing(servers[0].ip_addr)

        tablet_token = 0

        s0_host_id = await manager.get_host_id(servers[0].server_id)
        s1_host_id = await manager.get_host_id(servers[1].server_id)

        # Find which server holds the tablet
        replica = await get_tablet_replica(manager, servers[0], ks, 'test', tablet_token)
        if replica[0] == s0_host_id:
            src_server, dst_host_id = servers[0], s1_host_id
        else:
            src_server, dst_host_id = servers[1], s0_host_id

        # Injection for waiting in cleanup stage
        await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, injection, one_shot=False) for s in servers])

        # Start migration - move tablet to other node
        move_task = asyncio.create_task(manager.api.move_tablet(servers[0].ip_addr, ks, 'test', replica[0], replica[1], dst_host_id, 0, tablet_token))

        # Wait for the tablet to reach cleanup stage
        async def tablet_stage_is_cleanup():
            tinfo = await get_tablet_info(manager, servers[0], ks, 'test', tablet_token)
            if tinfo.stage == stage:
                return True
        await wait_for(tablet_stage_is_cleanup, time.time() + 60)

        # Restart the leaving replica (src_server)
        await manager.server_restart(src_server.server_id)

        await asyncio.gather(*[manager.api.disable_injection(s.ip_addr, injection) for s in servers])

        await manager.api.enable_tablet_balancing(servers[0].ip_addr)

        delete_keys = range(total_keys - 1)
        await asyncio.gather(*[cql.run_async(f"DELETE FROM {ks}.test WHERE pk={k};") for k in delete_keys])
        keys = range(total_keys - 1, total_keys)

        await manager.api.disable_tablet_balancing(servers[0].ip_addr)

        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, ks)
            await manager.api.keyspace_compaction(server.ip_addr, ks)
        await manager.api.enable_tablet_balancing(servers[0].ip_addr)

        await s1_log.wait_for('Detected tablet merge for table', from_mark=s1_mark)
        await manager.api.quiesce_topology(servers[0].ip_addr)
