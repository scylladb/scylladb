#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import logging
import os
import pathlib
import psutil
import pytest
import time
import uuid
from cassandra.cluster import ConsistencyLevel, EXEC_PROFILE_DEFAULT
from typing import Callable

from test.cluster.conftest import skip_mode
from test.cluster.util import get_topology_coordinator, find_server_by_host_id, new_test_keyspace, new_test_table
from test.pylib.manager_client import ManagerClient
from test.pylib.tablets import get_tablet_count
from test.storage.conftest import space_limited_servers

logger = logging.getLogger(__name__)


def write_generator(table, size_in_kb: int):
    for idx in range(size_in_kb):
        yield f"INSERT INTO {table} (pk, t) VALUES ({idx}, '{'x' * 1020}')"


class random_content_file:
    def __init__(self, path: str, size_in_bytes: int):
        path = pathlib.Path(path)
        self.filename = path if path.is_file() else path / str(uuid.uuid4())
        self.size = size_in_bytes if size_in_bytes > 0 else 0

    def __enter__(self):
        with open(self.filename, 'wb') as fh:
            fh.write(os.urandom(self.size))

    def __exit__(self, exc_type, exc_value, exc_traceback):
        os.unlink(self.filename)


# Since we create 100M volumes, we need to reduce the commitlog segment size
# otherwise we hit out of space.
global_cmdline = ["--disk-space-monitor-normal-polling-interval-in-seconds", "1",
                  "--critical-disk-utilization-level", "0.8",
                  "--commitlog-segment-size-in-mb", "2",
                  "--schema-commitlog-segment-size-in-mb", "4",
                  ]


@pytest.mark.asyncio
async def test_user_writes_rejection(manager: ManagerClient, volumes_factory: Callable) -> None:
    async with space_limited_servers(manager, volumes_factory, ["100M"]*3, cmdline=global_cmdline) as servers:
        cql, hosts = await manager.get_ready_cql(servers)

        workdir = await manager.server_get_workdir(servers[0].server_id)
        log = await manager.server_open_log(servers[0].server_id)
        mark = await log.mark()

        async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}") as ks:
            for server in servers:
                await manager.api.disable_autocompaction(server.ip_addr, ks)

            async with new_test_table(manager, ks, "pk int PRIMARY KEY, t text") as cf:

                logger.info("Create a big file on the target node to reach critical disk utilization level")
                disk_info = psutil.disk_usage(workdir)
                with random_content_file(workdir, int(disk_info.total*0.85) - disk_info.used):
                    for _ in range(2):
                        mark, _ = await log.wait_for("database - Setting critical disk utilization mode: true", from_mark=mark)

                    logger.info("Write data and verify it did not reach the target node")
                    query = next(write_generator(cf, 1))
                    await cql.run_async(query)

                    cl_one_profile = cql.execution_profile_clone_update(EXEC_PROFILE_DEFAULT, consistency_level = ConsistencyLevel.ONE)
                    res = cql.execute(f"SELECT * from {cf} where pk = 0;", host=hosts[0], execution_profile=cl_one_profile)
                    assert res.one() is None

                    for host in hosts[1:]:
                        res = cql.execute(f"SELECT * from {cf} where pk = 0;", host=host, execution_profile=cl_one_profile)
                        assert res.one()

                    logger.info("Restart the node")
                    mark = await log.mark()
                    await manager.server_restart(servers[0].server_id)
                    for _ in range(2):
                        mark, _ = await log.wait_for("database - Setting critical disk utilization mode: true", from_mark=mark)

                    time.sleep(1) # Let the cluster run for a sec to grep for potential errors
                    assert await log.grep("database - Setting critical disk utilization mode: false", from_mark=mark) == []

                    try:
                        cql.execute(f"INSERT INTO {cf} (pk, t) VALUES (-1, 'x')", host=host[0], execution_profile=cl_one_profile).result()
                    except Exception:
                        pass
                    else:
                        pytest.fail("Expected to fail due to critical disk utilization level")

                logger.info("With blob file removed, wait for DB to drop below the critical disk utilization level")
                for _ in range(2):
                    mark, _ = await log.wait_for("database - Setting critical disk utilization mode: false", from_mark=mark)

                logger.info("Write more data and expect it to succeed")
                cl_all_profile = cql.execution_profile_clone_update(EXEC_PROFILE_DEFAULT, consistency_level = ConsistencyLevel.ALL)
                cql.execute(next(write_generator(cf, 1)), execution_profile=cl_all_profile)


@pytest.mark.asyncio
async def test_autotoogle_compaction(manager: ManagerClient, volumes_factory: Callable) -> None:
    cmdline = [*global_cmdline,
               "--logger-log-level", "compaction=debug"]
    async with space_limited_servers(manager, volumes_factory, ["100M"]*3, cmdline=cmdline) as servers:
        cql, _ = await manager.get_ready_cql(servers)

        workdir = await manager.server_get_workdir(servers[0].server_id)
        log = await manager.server_open_log(servers[0].server_id)
        mark = await log.mark()

        async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}") as ks:
            for server in servers:
                await manager.api.disable_autocompaction(server.ip_addr, ks)

            async with new_test_table(manager, ks, "pk int PRIMARY KEY, t text") as cf:
                table = cf.split('.')[-1]

                for _ in range(3):
                    await asyncio.gather(*[cql.run_async(query) for query in write_generator(cf, 10)])
                    await manager.api.flush_keyspace(servers[0].ip_addr, ks)

                logger.info("Create a big file on the target node to reach critical disk utilization level")
                disk_info = psutil.disk_usage(workdir)
                with random_content_file(workdir, int(disk_info.total*0.85) - disk_info.used):
                    for _ in range(2):
                        mark, _ = await log.wait_for("compaction_manager - Drained", from_mark=mark)

                    logger.info("Restart the node")
                    await manager.server_restart(servers[0].server_id)
                    for _ in range(2):
                        mark, _ = await log.wait_for("compaction_manager - Drained", from_mark=mark)

                logger.info("With blob file removed, wait for DB to drop below the critical disk utilization level")
                for _ in range(2):
                    mark, _ = await log.wait_for("compaction_manager - Enabled", from_mark=mark)

                await manager.api.keyspace_compaction(servers[0].ip_addr, ks)
                await log.wait_for(rf"Compact {ks}\.{table} .* Compacted .* sstables to .*", from_mark=mark)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_reject_split_compaction(manager: ManagerClient, volumes_factory: Callable) -> None:
    async with space_limited_servers(manager, volumes_factory, ["100M"]*3, cmdline=global_cmdline) as servers:
        cql, _ = await manager.get_ready_cql(servers)

        workdir = await manager.server_get_workdir(servers[0].server_id)
        log = await manager.server_open_log(servers[0].server_id)
        mark = await log.mark()

        async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}") as ks:
            async with new_test_table(manager, ks, "pk int PRIMARY KEY, t text") as cf:
                for _ in range(30):
                    await asyncio.gather(*[cql.run_async(query) for query in write_generator(cf, 100)])
                await manager.api.flush_keyspace(servers[0].ip_addr, ks)

                logger.info("Trigger split compaction")
                await manager.api.enable_injection(servers[0].ip_addr, "split_sstable_rewrite", one_shot=False)
                cql.execute_async(f"ALTER KEYSPACE {ks} WITH tablets = {{'initial': 32}}")

                mark, _ = await log.wait_for("split_sstable_rewrite: waiting", from_mark=mark)

                logger.info("Create a big file on the target node to reach critical disk utilization level")
                disk_info = psutil.disk_usage(workdir)
                with random_content_file(workdir, int(disk_info.total*0.85) - disk_info.used):
                    await log.wait_for(f"Split task .* for table {cf} .* stopped, reason: Compaction for {cf} was stopped due to: drain")


@pytest.mark.asyncio
async def test_split_compaction_not_triggered(manager: ManagerClient, volumes_factory: Callable) -> None:
    async with space_limited_servers(manager, volumes_factory, ["100M"]*3, cmdline=global_cmdline) as servers:
        cql, _ = await manager.get_ready_cql(servers)

        workdir = await manager.server_get_workdir(servers[0].server_id)
        s1_log = await manager.server_open_log(servers[0].server_id)
        s1_mark = await s1_log.mark()

        s2_log = await manager.server_open_log(servers[1].server_id)

        async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}") as ks:
            async with new_test_table(manager, ks, "pk int PRIMARY KEY, t text") as cf:
                for _ in range(30):
                    await asyncio.gather(*[cql.run_async(query) for query in write_generator(cf, 100)])
                await manager.api.flush_keyspace(servers[0].ip_addr, ks)

                logger.info("Create a big file on the target node to reach critical disk utilization level")
                disk_info = psutil.disk_usage(workdir)
                with random_content_file(workdir, int(disk_info.total*0.85) - disk_info.used):
                    for _ in range(2):
                        s1_mark, _ = await s1_log.wait_for("compaction_manager - Drained", from_mark=s1_mark)

                    logger.info("Trigger split compaction")
                    s2_mark = await s2_log.mark()
                    cql.execute_async(f"ALTER KEYSPACE {ks} WITH tablets = {{'initial': 32}}")

                    s2_log.wait_for(f"compaction .* Split {cf}", from_mark=s2_mark)
                    assert await s1_log.grep(f"compaction .* Split {cf}", from_mark=s1_mark) == []


@pytest.mark.asyncio
async def test_tablet_repair(manager: ManagerClient, volumes_factory: Callable) -> None:
    cfg = {
        'tablet_load_stats_refresh_interval_in_seconds': 1,
        }
    async with space_limited_servers(manager, volumes_factory, ["100M"]*3, cmdline=global_cmdline, config=cfg) as servers:
        cql, _ = await manager.get_ready_cql(servers)

        workdir = await manager.server_get_workdir(servers[0].server_id)
        log = await manager.server_open_log(servers[0].server_id)
        host = await manager.get_host_id(servers[0].server_id)
        mark = await log.mark()

        async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 4}") as ks:
            async with new_test_table(manager, ks, "pk int PRIMARY KEY, t text") as cf:
                table = cf.split('.')[-1]

                for _ in range(2):
                    await asyncio.gather(*[cql.run_async(query) for query in write_generator(cf, 100)])
                    await manager.api.flush_keyspace(servers[0].ip_addr, ks)
                await manager.server_stop_gracefully(servers[0].server_id)
                await manager.server_wipe_sstables(servers[0].server_id, ks, table)
                await manager.server_start(servers[0].server_id)

                logger.info("Create a big file on the target node to reach critical disk utilization level")
                disk_info = psutil.disk_usage(workdir)
                with random_content_file(workdir, int(disk_info.total*0.85) - disk_info.used):
                    for _ in range(2):
                        mark, _ = await log.wait_for("repair - Drained", from_mark=mark)

                    coord = await get_topology_coordinator(manager)
                    coord_serv = await find_server_by_host_id(manager, servers, coord)
                    coord_log = await manager.server_open_log(coord_serv.server_id)
                    coord_mark = await coord_log.mark()

                    logger.info("Schedule tablet repair")
                    response = await manager.api.tablet_repair(servers[0].ip_addr, ks, table, "all", await_completion=False)
                    task_id = response['tablet_task_id']

                    for _ in range(await get_tablet_count(manager, servers[1], ks, table)):
                        coord_mark, matches = await coord_log.wait_for("Initiating tablet repair host=(?P<host>.*) tablet=(?P<tablet>.*)", from_mark=coord_mark)
                        dst_host, tablet = matches[0][1].group("host"), matches[0][1].group("tablet")
                        if host == dst_host:
                            # Tablet repair is triggered on the node with disk utilization above the critical level.
                            # A local tablet repair task is refused to be created and the tablet repair fails.
                            error = "Repair service is disabled. No repairs will be started until it's re-enabled"
                        else:
                            # Tablet repair is triggered on the node with disk utilization below the critical level.
                            # A local tablet repair task is created and the row-level repair is executed. It will try
                            # to send missing rows to the node with critical disk utilization that are rejected.
                            error = f"put_row_diff: Repair follower={host} failed in put_row_diff handler"

                        await coord_log.wait_for(f"repair for tablet {tablet} failed: seastar::rpc::remote_verb_error.*{error}", from_mark=coord_mark)

                    logger.info("Restart the node")
                    mark = await log.mark()
                    await manager.server_restart(servers[0].server_id, wait_others=2)
                    await manager.driver_connect()
                    for _ in range(2):
                        mark, _ = await log.wait_for("repair - Drained", from_mark=mark)

                logger.info("With blob file removed, wait for the tablet repair to succeed")
                await manager.api.wait_task(servers[0].ip_addr, task_id)


@pytest.mark.asyncio
async def test_autotoogle_reject_incoming_migrations(manager: ManagerClient, volumes_factory: Callable) -> None:
    cfg = {
        'tablet_load_stats_refresh_interval_in_seconds': 1,
        }
    async with space_limited_servers(manager, volumes_factory, ["100M"]*3, cmdline=global_cmdline, config=cfg) as servers:
        await asyncio.gather(*[manager.api.disable_tablet_balancing(server.ip_addr) for server in servers])

        cql, _ = await manager.get_ready_cql(servers)

        async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
            async with new_test_table(manager, ks, "pk int PRIMARY KEY, t text") as cf:
                table = cf.split('.')[-1]
                await asyncio.gather(*[cql.run_async(query) for query in write_generator(cf, 10)])

                logger.info("Get tablet to migrate")
                table_id = await cql.run_async(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{table}'")
                table_id = table_id[0].id

                tablet_infos = await cql.run_async(f"SELECT last_token, replicas FROM system.tablets WHERE table_id = {table_id}")
                tablet_infos = list(tablet_infos)

                assert len(tablet_infos) == 1
                tablet_info = tablet_infos[0]
                assert len(tablet_info.replicas) == 1

                hosts = {await manager.get_host_id(server.server_id) : server for server in servers}

                source_host, source_shard = tablet_info.replicas[0]
                del hosts[str(source_host)]
                target_host, target_server = list(hosts.items())[0]
                target_shard = source_shard
                logger.info(f"Tablet to migrate: {tablet_info.last_token} from {source_host} to {target_host}")

                logger.info("Create a big file on the target node to reach critical disk utilization level")
                workdir = await manager.server_get_workdir(target_server.server_id)
                log = await manager.server_open_log(target_server.server_id)
                mark = await log.mark()

                disk_info = psutil.disk_usage(workdir)
                with random_content_file(workdir, int(disk_info.total*0.85) - disk_info.used):
                    for _ in range(2):
                        mark, _ = await log.wait_for("database - Setting critical disk utilization mode: true", from_mark=mark)

                    logger.info("Migrate a tablet to the target node and expect a failure")
                    await manager.api.move_tablet(node_ip=servers[0].ip_addr, ks=ks, table=table, src_host=source_host,
                                                src_shard=source_shard, dst_host=target_host, dst_shard=target_shard,
                                                token=tablet_info.last_token)
                    mark, _ = await log.wait_for("Streaming for tablet migration .* failed", from_mark=mark)

                logger.info("With blob file removed, wait for DB to drop below the critical disk utilization level")
                for _ in range(2):
                    mark, _ = await log.wait_for("database - Setting critical disk utilization mode: false", from_mark=mark)

                logger.info("Migrate a tablet to the target node and expect a success")
                await manager.api.move_tablet(node_ip=servers[0].ip_addr, ks=ks, table=table, src_host=source_host,
                                            src_shard=source_shard, dst_host=target_host, dst_shard=target_shard,
                                            token=tablet_info.last_token)
                mark, _ = await log.wait_for("Streaming for tablet migration .* successful", from_mark=mark)


@pytest.mark.asyncio
async def test_node_restart_while_tablet_split(manager: ManagerClient, volumes_factory: Callable) -> None:
    cfg = {
        'tablet_load_stats_refresh_interval_in_seconds': 1,
        }
    async with space_limited_servers(manager, volumes_factory, ["100M"]*3, cmdline=global_cmdline, config=cfg) as servers:
        cql, _ = await manager.get_ready_cql(servers)
        workdir = await manager.server_get_workdir(servers[0].server_id)
        log = await manager.server_open_log(servers[0].server_id)
        mark = await log.mark()

        logger.info("Create and populate test table")
        async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
            async with new_test_table(manager, ks, "pk int PRIMARY KEY, t text") as cf:
                table = cf.split('.')[-1]
                table_id = (await cql.run_async(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}' AND table_name = '{table}'"))[0].id

                await asyncio.gather(*[cql.run_async(query) for query in write_generator(cf, 64)])
                await manager.api.flush_keyspace(servers[0].ip_addr, ks)

                # Ensure that the topology state update reaches the node. We should never reach 5s timeout here.
                # But one call to retrieve resize_task_info may not be enough as the entry to system.tablets might
                # not be there yet.
                async def assert_resize_task_info(table_id, cb):
                    async with asyncio.timeout(5):
                        while (response := await cql.run_async(f"SELECT resize_task_info from system.tablets where table_id = {table_id};")):
                            try:
                                assert cb(response)
                            except AssertionError:
                                await asyncio.sleep(0.1)
                            else:
                                break

                logger.info("Create a big file on the target node to reach critical disk utilization level")
                disk_info = psutil.disk_usage(workdir)
                with random_content_file(workdir, int(disk_info.total*0.85) - disk_info.used):
                    for _ in range(2):
                        mark, _ = await log.wait_for("compaction_manager - Drained", from_mark=mark)

                    logger.info("Trigger split in table and restart the node")
                    coord = await get_topology_coordinator(manager)
                    logger.info(f"Topology coordinator is {coord}")
                    coord_serv = await find_server_by_host_id(manager, servers, coord)
                    coord_log = await manager.server_open_log(coord_serv.server_id)

                    await cql.run_async(f"ALTER TABLE {cf} WITH tablets = {{'min_tablet_count': 2}};")
                    coord_log.wait_for(f"Generating resize decision for table {table_id} of type split")

                    await manager.server_restart(servers[0].server_id, wait_others=2)

                    logger.info("Check if tablet split happened")
                    await assert_resize_task_info(table_id, lambda response: len(response) == 1 and response[0].resize_task_info is not None)

                    time.sleep(1) # Let the cluster run for a sec to grep for potential errors
                    assert await log.grep(f"compaction .* Split {cf}", from_mark=mark) == []

                logger.info("With blob file removed, wait for DB to drop below the critical disk utilization level")
                for _ in range(2):
                    mark, _ = await log.wait_for("compaction_manager - Enabled", from_mark=mark)
                mark, _ = await log.wait_for(f"Detected tablet split for table {cf}, increasing from 1 to 2 tablets", from_mark=mark)
                await assert_resize_task_info(table_id, lambda response: len(response) == 2 and all(r.resize_task_info is None for r in response))
