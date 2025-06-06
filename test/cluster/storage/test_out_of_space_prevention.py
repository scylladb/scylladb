#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient

import asyncio
import logging
import os
import pathlib
import psutil
import pytest
import uuid
from cassandra.cluster import ConsistencyLevel, EXEC_PROFILE_DEFAULT
from test.cluster.util import new_test_keyspace, new_test_table

logger = logging.getLogger(__name__)


def write_generator(table, size_in_kb: int):
    for idx in range(size_in_kb):
        yield f"INSERT INTO {table} (pk, t) VALUES ({idx}, '{'x' * 1020}')"


def create_random_content_file(path: str, size_in_bytes: int):
    path = pathlib.Path(path)
    filename = path if path.is_file() else path / str(uuid.uuid4())
    size_in_bytes = size_in_bytes if size_in_bytes > 0 else 0

    with open(filename, 'wb') as fh:
        fh.write(os.urandom(size_in_bytes))

    return filename


@pytest.mark.asyncio
async def test_user_writes_rejection(manager: ManagerClient) -> None:
    servers = await manager.all_servers()
    cql, hosts = await manager.get_ready_cql(servers)

    workdir = await manager.server_get_workdir(servers[0].server_id)
    log = await manager.server_open_log(servers[0].server_id)
    mark = await log.mark()

    logger.info("Create a big file on the target node to reach critical disk utilization level")
    disk_info = psutil.disk_usage(workdir)
    filename = create_random_content_file(workdir, int(disk_info.total*0.95) - disk_info.used)
    disk_info = psutil.disk_usage(workdir)

    logger.info("Start adding more data")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}") as ks:
        for server in servers:
            await manager.api.disable_autocompaction(server.ip_addr, ks)

        async with new_test_table(manager, ks, "pk int PRIMARY KEY, t text") as cf:
            logger.info("Populate data to the table so DB hits critical disk utilization level")
            count = disk_info.free // 1024
            for query in write_generator(cf, count):
                cql.execute(query)

            logger.info("Verify the last write did not reach the target node")
            profile = cql.execution_profile_clone_update(EXEC_PROFILE_DEFAULT, consistency_level = ConsistencyLevel.ONE)
            res = cql.execute(f"SELECT * from {cf} where pk = {count-1};", host=hosts[0], execution_profile=profile)
            assert res.one() is None

            for host in hosts[1:]:
                res = cql.execute(f"SELECT * from {cf} where pk = {count-1};", host=host, execution_profile=profile)
                assert res.one()

            logger.info("Remove the file and wait for DB to drop below the critical disk utilization level")
            os.unlink(filename)
            mark, _ = await log.wait_for("out_of_space_controller - disabling database critical disk utilization mode", from_mark=mark)

            logger.info("Write more data and expect it to succeed")
            count = disk_info.free // 1024
            profile = cql.execution_profile_clone_update(EXEC_PROFILE_DEFAULT, consistency_level = ConsistencyLevel.ALL)
            for query in write_generator(cf, count):
                cql.execute(query, execution_profile=profile)


@pytest.mark.asyncio
async def test_autotoogle_compaction(manager: ManagerClient) -> None:
    servers = await manager.all_servers()
    cql, _ = await manager.get_ready_cql(servers)

    workdir = await manager.server_get_workdir(servers[0].server_id)
    log = await manager.server_open_log(servers[0].server_id)
    mark = await log.mark()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}") as ks:
        for server in servers:
            await manager.api.disable_autocompaction(server.ip_addr, ks)

        async with new_test_table(manager, ks, "pk int PRIMARY KEY, t text") as cf:
            for _ in range(3):
                for query in write_generator(cf, 10):
                    cql.execute(query)
                await manager.api.flush_keyspace(servers[0].ip_addr, ks)

            logger.info("Create a big file on the target node to reach critical disk utilization level")
            disk_info = psutil.disk_usage(workdir)
            filename = create_random_content_file(workdir, int(disk_info.total*0.985) - disk_info.used)

            for _ in range(2):
                mark, _ = await log.wait_for("compaction_manager - Drained", from_mark=mark)

            logger.info("Remove the file and wait for DB to drop below the critical disk utilization level")
            os.unlink(filename)
            mark, _ = await log.wait_for("out_of_space_controller - enabling compaction manager", from_mark=mark)

            manager.api.keyspace_compaction(servers[0].ip_addr, ks)
            await log.wait_for(r"]\s+compaction\s+-", from_mark=mark)


@pytest.mark.asyncio
async def test_autotoogle_repair(manager: ManagerClient) -> None:
    servers = await manager.all_servers()
    cql, _ = await manager.get_ready_cql(servers)

    workdir = await manager.server_get_workdir(servers[0].server_id)
    log = await manager.server_open_log(servers[0].server_id)
    mark = await log.mark()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, t text") as cf:
            for query in write_generator(cf, 10):
                cql.execute(query)

            logger.info("Create a big file on the target node to reach critical disk utilization level")
            disk_info = psutil.disk_usage(workdir)
            filename = create_random_content_file(workdir, int(disk_info.total*0.985) - disk_info.used)
            mark, _ = await log.wait_for("out_of_space_controller - disabling repair service", from_mark=mark)

            logger.info("Try to run repair and expect a failure")
            with pytest.raises(Exception):
                await manager.api.repair(servers[0].ip_addr, *cf.split('.'))

            logger.info("Remove the file and wait for DB to drop below the critical disk utilization level")
            os.unlink(filename)
            mark, _ = await log.wait_for("out_of_space_controller - enabling repair service", from_mark=mark)

            logger.info("Try to run repair and expect a success")
            await manager.api.repair(servers[0].ip_addr, *cf.split('.'))


@pytest.mark.asyncio
async def test_autotoogle_reject_incoming_migrations(manager: ManagerClient) -> None:
    servers = await manager.all_servers()
    await asyncio.gather(*[manager.api.disable_tablet_balancing(server.ip_addr) for server in servers])

    cql, _ = await manager.get_ready_cql(servers)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, t text") as cf:
            table = cf.split('.')[-1]

            for query in write_generator(cf, 10):
                cql.execute(query)

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
            filename = create_random_content_file(workdir, int(disk_info.total*0.985) - disk_info.used)
            mark, _ = await log.wait_for("out_of_space_controller - enabling database critical disk utilization mode", from_mark=mark)

            logger.info("Migrate a tablet to the target node and expect a failure")
            await manager.api.move_tablet(node_ip=servers[0].ip_addr, ks=ks, table=table, src_host=source_host,
                                        src_shard=source_shard, dst_host=target_host, dst_shard=target_shard,
                                        token=tablet_info.last_token)
            mark, _ = await log.wait_for("Streaming for tablet migration .* failed", from_mark=mark)
            logger.info("Remove the file and wait for DB to drop below the critical disk utilization level")
            os.unlink(filename)
            mark, _ = await log.wait_for("out_of_space_controller - disabling database critical disk utilization mode", from_mark=mark)

            logger.info("Migrate a tablet to the target node and expect a success")
            await manager.api.move_tablet(node_ip=servers[0].ip_addr, ks=ks, table=table, src_host=source_host,
                                        src_shard=source_shard, dst_host=target_host, dst_shard=target_shard,
                                        token=tablet_info.last_token)
            mark, _ = await log.wait_for("Streaming for tablet migration .* successful", from_mark=mark)
