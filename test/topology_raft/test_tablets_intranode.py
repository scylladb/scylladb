#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from cassandra.query import SimpleStatement, ConsistencyLevel
from cassandra.cluster import Session, ConsistencyLevel

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot, HTTPError
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for_cql_and_get_hosts, start_writes
from test.pylib.tablets import get_tablet_replica, get_all_tablet_replicas
from test.topology.conftest import skip_mode
from test.topology.util import reconnect_driver

import pytest
import asyncio
import logging
import time
import random
import os
import glob
from typing import NamedTuple
import threading


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_intranode_migration(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=trace',
        '--logger-log-level', 'stream_session=trace',
        '--logger-log-level', 'tablets=trace',
        '--logger-log-level', 'database=trace',
    ]
    servers = [await manager.server_add(cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    finish_writes = await start_writes(cql, "test", "test")

    tablet_token = 0 # Doesn't matter since there is one tablet
    replica = await get_tablet_replica(manager, servers[0], 'test', 'test', tablet_token)

    s0_host_id = await manager.get_host_id(servers[0].server_id)
    src_shard = replica[1]
    dst_shard = src_shard ^ 1

    await manager.api.move_tablet(servers[0].ip_addr, "test", "test", replica[0], src_shard, replica[0], dst_shard, tablet_token)

    key_count = await finish_writes()

    rows = await cql.run_async("SELECT * FROM test.test;")
    assert len(rows) == key_count
    for r in rows:
        assert r.c == r.pk


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_crash_during_intranode_migration(manager: ManagerClient):
    cmdline = [
        '--logger-log-level', 'tablets=trace',
        '--logger-log-level', 'database=trace',
        '--commitlog-sync', 'batch', # So that ACKed writes are not lost on crash
    ]
    servers = [await manager.server_add(cmdline=cmdline)]

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()

    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
                        " AND tablets = {'initial': 4};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    finish_writes = await start_writes(cql, "test", "test", ignore_errors=True)

    tablet_token = 0 # Choose one tablet, any of them
    replica = await get_tablet_replica(manager, servers[0], 'test', 'test', tablet_token)

    src_shard = replica[1]
    dst_shard = src_shard ^ 1

    await manager.api.enable_injection(servers[0].ip_addr, 'crash-in-tablet-write-both-read-new', one_shot=True)

    migration_task = asyncio.create_task(manager.api.move_tablet(servers[0].ip_addr, "test", "test",
                                                replica[0], src_shard, replica[0], dst_shard, tablet_token))

    s0_logs = await manager.server_open_log(servers[0].server_id)
    await s0_logs.wait_for('crash-in-tablet-write-both-read-new hit')
    await manager.server_stop(servers[0].server_id)
    await manager.server_start(servers[0].server_id)
    await wait_for_cql_and_get_hosts(manager.cql, servers, time.time() + 60)

    # Wait for the tablet migration to finish
    await manager.api.quiesce_topology(servers[0].ip_addr)

    try:
        await migration_task
    except:
        pass

    key_count = await finish_writes()

    rows = await cql.run_async("SELECT * FROM test.test;")
    assert len(rows) == key_count
    for r in rows:
        assert r.c == r.pk


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_cross_shard_migration(manager: ManagerClient):
    """
    Test scenario where writes are concurrently made with migration, where
    some of them are coordinated by the owning host and some by the non-owning host.

    This reproduces the following problem with sharder logic:
      1) node A: tablet is in stage write_both_read_new (replicas: B:1 -> A:1)
      2) node B: tablet is in stage write_both_read_new
      3) node A: tablet is in stage use_new
      4) node B: coordinate write to node A (since stage is still "write both" here)
      5) node A: receive write request, sharder thinks no shard owns the tablet, fails the write

    In this scenario, sharder on node A should still return shard 1.
    """

    logger.info("Bootstrapping cluster")
    cmdline = [
        '--logger-log-level', 'storage_service=trace',
        '--logger-log-level', 'tablets=trace',
        '--logger-log-level', 'database=trace',
    ]

    servers = await manager.servers_add(2, cmdline=cmdline)

    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
                        " AND tablets = {'initial': 2};")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    finish_writes = await start_writes(cql, "test", "test")

    tablet0_token = -1
    tablet1_token = 1
    replica0 = await get_tablet_replica(manager, servers[0], 'test', 'test', tablet0_token)
    replica1 = await get_tablet_replica(manager, servers[0], 'test', 'test', tablet1_token)

    s0_host_id = await manager.get_host_id(servers[0].server_id)
    s1_host_id = await manager.get_host_id(servers[1].server_id)

    # Place tablets on non-zero shards so that defaulted shard (0) is never the right shard.
    # This is to catch the problem when sharder (incorrectly) thinks that tablet does not have
    # any replica on the current host and assigns shard 0 to it in shard_for_read().
    await manager.api.move_tablet(servers[0].ip_addr, "test", "test", replica0[0], replica0[1], s0_host_id, 1, tablet0_token)
    await manager.api.move_tablet(servers[0].ip_addr, "test", "test", replica1[0], replica1[1], s1_host_id, 1, tablet1_token)

    # Put whole token ring into migration so that all requests hit the migration path. Half of them
    # will be coordinated by the owning host, half will be coordinated by the non-owning host.
    migration0 = asyncio.create_task(manager.api.move_tablet(servers[0].ip_addr, "test", "test",
                                                             s0_host_id, 1, s1_host_id, 1, tablet0_token))
    migration1 = asyncio.create_task(manager.api.move_tablet(servers[0].ip_addr, "test", "test",
                                                             s1_host_id, 1, s0_host_id, 1, tablet1_token))

    await migration0
    await migration1

    key_count = await finish_writes()

    rows = await cql.run_async("SELECT * FROM test.test;")
    assert len(rows) == key_count
    for r in rows:
        assert r.c == r.pk
