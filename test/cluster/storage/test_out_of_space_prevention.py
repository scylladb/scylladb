#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from test.pylib.manager_client import ManagerClient

import logging
import os
import pathlib
import pytest
import psutil
import time
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

    disk_info = psutil.disk_usage(workdir)
    filename = create_random_content_file(workdir, int(disk_info.total*0.95) - disk_info.used)
    disk_info = psutil.disk_usage(workdir)

    logger.info("Start adding more data")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}") as ks:
        for server in servers:
            await manager.api.disable_autocompaction(server.ip_addr, ks)

        async with new_test_table(manager, ks, "pk int PRIMARY KEY, t text") as cf:
            count = disk_info.free // 1024
            for query in write_generator(cf, count):
                cql.execute(query)

            profile = cql.execution_profile_clone_update(EXEC_PROFILE_DEFAULT, consistency_level = ConsistencyLevel.ONE)
            res = cql.execute(f"SELECT * from {cf} where pk = {count-1};", host=hosts[0], execution_profile=profile)
            assert res.one() is None

            for host in hosts[1:]:
                res = cql.execute(f"SELECT * from {cf} where pk = {count-1};", host=host, execution_profile=profile)
                assert res.one()

            os.unlink(filename)
            mark = await log.wait_for("out_of_space_controller - enabling user table writes", mark)

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

            disk_info = psutil.disk_usage(workdir)
            filename = create_random_content_file(workdir, int(disk_info.total*0.985) - disk_info.used)

            for _ in range(2):
                mark = await log.wait_for("compaction_manager - Drained", mark)

            # Remove the file and wait to ensure compaction manager is re-enabled.
            # The polling interval for a disk monitor is 1s.
            os.unlink(filename)
            time.sleep(1)

            manager.api.keyspace_compaction(servers[0].ip_addr, ks)
            await log.wait_for(r"]\s+compaction\s+-", mark)


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

            disk_info = psutil.disk_usage(workdir)
            filename = create_random_content_file(workdir, int(disk_info.total*0.985) - disk_info.used)
            mark = await log.wait_for("out_of_space_controller - disabling repair service", mark)

            with pytest.raises(Exception):
                await manager.api.repair(servers[0].ip_addr, *cf.split('.'))

            # Remove the file and wait to ensure compaction manager is re-enabled.
            # The polling interval for a disk monitor is 1s.
            os.unlink(filename)
            mark = await log.wait_for("out_of_space_controller - enabling repair service", mark)

            logger.info(f"reapir {cf}")
            await manager.api.repair(servers[0].ip_addr, *cf.split('.'))
