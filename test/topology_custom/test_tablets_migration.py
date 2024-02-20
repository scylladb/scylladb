#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from cassandra.query import SimpleStatement, ConsistencyLevel
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import HTTPError
from test.pylib.tablets import get_all_tablet_replicas
from test.topology.conftest import skip_mode
import pytest
import logging
import asyncio

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("fail_replica", ["source", "destination"])
@pytest.mark.parametrize("fail_stage", ["streaming", "allow_write_both_read_old", "write_both_read_old", "write_both_read_new", "use_new"])
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_node_failure_during_tablet_migration(manager: ManagerClient, fail_replica, fail_stage):
    logger.info("Bootstrapping cluster")
    cfg = {'enable_user_defined_functions': False, 'experimental_features': ['tablets', 'consistent-topology-changes']}
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

    fail_idx = 1 if fail_replica == "source" else 2

    logger.info(f"Will fail {fail_stage}")
    if fail_stage == "streaming":
        await manager.api.enable_injection(servers[2].ip_addr, "stream_mutation_fragments", one_shot=True)
        s2_log = await manager.server_open_log(servers[2].server_id)
        s2_mark = await s2_log.mark()
    elif fail_stage in [ "allow_write_both_read_old", "write_both_read_old", "write_both_read_new", "use_new" ]:
        await manager.api.enable_injection(servers[fail_idx].ip_addr, "raft_topology_barrier_and_drain_fail", one_shot=False, parameters={'keyspace': 'test', 'table': 'test', 'last_token': last_token, 'stage': fail_stage})
        sx_log = await manager.server_open_log(servers[fail_idx].server_id)
        sx_mark = await sx_log.mark()
    else:
        assert False, f"Unknown stage {fail_stage}"

    migration_task = asyncio.create_task(
        manager.api.move_tablet(servers[0].ip_addr, "test", "test", old_replica[0], old_replica[1], new_replica[0], new_replica[1], 0))

    logger.info(f"Wait for {fail_stage} to happen")
    if fail_stage == "streaming":
        await s2_log.wait_for('stream_mutation_fragments: waiting', from_mark=s2_mark)
    elif fail_stage in [ "allow_write_both_read_old", "write_both_read_old", "write_both_read_new", "use_new" ]:
        await sx_log.wait_for('raft_topology_cmd: barrier handler waits', from_mark=sx_mark);
    else:
        assert False

    logger.info(f"Stop {fail_replica} {host_ids[fail_idx]}")
    await manager.server_stop(servers[fail_idx].server_id)
    logger.info(f"Remove {fail_replica} {host_ids[fail_idx]}")
    await manager.remove_node(servers[0].server_id, servers[fail_idx].server_id)

    logger.info("Done, waiting for migration to finish")
    await migration_task

    replicas = await get_all_tablet_replicas(manager, servers[0], 'test', 'test')
    logger.info(f"Tablet is now on [{replicas}]")
    assert len(replicas) == 1
    for r in replicas[0].replicas:
        assert r[0] != host_ids[fail_idx]
