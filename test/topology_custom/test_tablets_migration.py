#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from cassandra.query import SimpleStatement, ConsistencyLevel
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import HTTPError, read_barrier
from test.pylib.tablets import get_all_tablet_replicas
from test.topology.conftest import skip_mode
from test.topology.util import wait_for_cql_and_get_hosts
import time
import pytest
import logging
import asyncio

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("action", ['move', 'add_replica', 'del_replica'])
@pytest.mark.asyncio
async def test_tablet_transition_sanity(manager: ManagerClient, action):
    logger.info("Bootstrapping cluster")
    cfg = {'enable_user_defined_functions': False, 'enable_tablets': True}
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

    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 1}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")
    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    replicas = await get_all_tablet_replicas(manager, servers[0], 'test', 'test')
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
        await manager.api.move_tablet(servers[0].ip_addr, "test", "test", old_replica[0], old_replica[1], new_replica[0], new_replica[1], 0)
    if action == 'add_replica':
        logger.info(f"Adding replica to tablet, host {new_replica[0]}")
        await manager.api.add_tablet_replica(servers[0].ip_addr, "test", "test", new_replica[0], new_replica[1], 0)
    if action == 'del_replica':
        logger.info(f"Deleting replica from tablet, host {old_replica[0]}")
        await manager.api.del_tablet_replica(servers[0].ip_addr, "test", "test", old_replica[0], old_replica[1], 0)

    replicas = await get_all_tablet_replicas(manager, servers[0], 'test', 'test')
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
        res = await cql.run_async("SELECT COUNT(*) FROM MUTATION_FRAGMENTS(test.test)", host=host[0])
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
    cfg = {'enable_user_defined_functions': False, 'enable_tablets': True, 'failure_detector_timeout_in_ms': 2000}
    host_ids = []
    servers = []

    async def make_server():
        s = await manager.server_add(config=cfg)
        servers.append(s)
        host_ids.append(await manager.get_host_id(s.server_id))
        await manager.api.disable_tablet_balancing(s.ip_addr)

    await make_server()
    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2} AND tablets = {'initial': 1}")
    await make_server()
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])
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

    replicas = await get_all_tablet_replicas(manager, servers[0], 'test', 'test')
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
        def __init__(self, stage, replica):
            self.stage = stage
            self.replica = replica
            self.fail_idx = 1 if self.replica == "source" else 2

        async def setup(self):
            logger.info(f"Will fail {self.stage}")
            if self.stage == "streaming":
                await manager.api.enable_injection(servers[2].ip_addr, "stream_mutation_fragments", one_shot=True)
                self.log = await manager.server_open_log(servers[2].server_id)
                self.mark = await self.log.mark()
            elif self.stage in [ "allow_write_both_read_old", "write_both_read_old", "write_both_read_new", "use_new", "end_migration", "do_revert_migration" ]:
                await manager.api.enable_injection(servers[self.fail_idx].ip_addr, "raft_topology_barrier_and_drain_fail", one_shot=False,
                        parameters={'keyspace': 'test', 'table': 'test', 'last_token': last_token, 'stage': self.stage.removeprefix('do_')})
                self.log = await manager.server_open_log(servers[self.fail_idx].server_id)
                self.mark = await self.log.mark()
            elif self.stage == "cleanup":
                await manager.api.enable_injection(servers[self.fail_idx].ip_addr, "cleanup_tablet_crash", one_shot=True)
                self.log = await manager.server_open_log(servers[self.fail_idx].server_id)
                self.mark = await self.log.mark()
            elif self.stage == "cleanup_target":
                assert self.fail_idx == 2
                self.stream_fail = node_failer('streaming', 'source')
                await self.stream_fail.setup()
                self.cleanup_fail = node_failer('cleanup', 'destination')
                await self.cleanup_fail.setup()
            elif self.stage == "revert_migration":
                self.wbro_fail = node_failer('write_both_read_old', 'source' if self.replica == 'destination' else 'destination')
                await self.wbro_fail.setup()
                self.revert_fail = node_failer('do_revert_migration', self.replica)
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


    failer = node_failer(fail_stage, fail_replica)
    await failer.setup()
    migration_task = asyncio.create_task(
        manager.api.move_tablet(servers[0].ip_addr, "test", "test", old_replica[0], old_replica[1], new_replica[0], new_replica[1], 0))
    await failer.wait()
    await failer.stop()

    logger.info("Done, waiting for migration to finish")
    await migration_task

    replicas = await get_all_tablet_replicas(manager, servers[0], 'test', 'test')
    logger.info(f"Tablet is now on [{replicas}]")
    assert len(replicas) == 1
    for r in replicas[0].replicas:
        assert r[0] != host_ids[failer.fail_idx]

@pytest.mark.asyncio
async def test_tablet_back_and_forth_migration(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cfg = {'enable_user_defined_functions': False, 'enable_tablets': True}
    host_ids = []
    servers = []

    async def make_server():
        s = await manager.server_add(config=cfg)
        servers.append(s)
        host_ids.append(await manager.get_host_id(s.server_id))
        await manager.api.disable_tablet_balancing(s.ip_addr)

    async def assert_rows(num):
        res = await cql.run_async(f"SELECT * FROM test.test")
        assert len(res) == num

    await make_server()
    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")
    await make_server()

    await cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({1}, {1});")
    await assert_rows(1)

    replicas = await get_all_tablet_replicas(manager, servers[0], 'test', 'test')
    logger.info(f"Tablet is on [{replicas}]")
    assert len(replicas) == 1 and len(replicas[0].replicas) == 1

    old_replica = replicas[0].replicas[0]
    assert old_replica[0] != host_ids[1]
    new_replica = (host_ids[1], 0)

    logger.info(f"Moving tablet {old_replica} -> {new_replica}")
    manager.api.move_tablet(servers[0].ip_addr, "test", "test", old_replica[0], old_replica[1], new_replica[0], new_replica[1], 0)

    await assert_rows(1)
    await cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({2}, {2});")
    await assert_rows(2)

    logger.info(f"Moving tablet {new_replica} -> {old_replica}")
    manager.api.move_tablet(servers[0].ip_addr, "test", "test", new_replica[0], new_replica[1], old_replica[0], old_replica[1], 0)

    await assert_rows(2)
    await cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({3}, {3});")
    await assert_rows(3)
