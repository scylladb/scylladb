# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
from test.pylib.manager_client import ManagerClient

import pytest
import logging

from test.pylib.rest_client import inject_error_one_shot
from test.cluster.util import new_test_keyspace

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_broken_bootstrap(manager: ManagerClient):
    server_a = await manager.server_add()
    server_b = await manager.server_add(start=False)

    async with new_test_keyspace(manager, "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}") as ks:
        table = f"{ks}.test"
        await manager.cql.run_async(f"CREATE TABLE {table} (a int PRIMARY KEY, b int)")
        for i in range(100):
            await manager.cql.run_async(f"INSERT INTO {table} (a, b) VALUES ({i}, {i})")
        await inject_error_one_shot(manager.api, server_a.ip_addr, "crash-before-bootstrapping-node-added")
        try:
            # Timeout fast since we do not expect the operation to complete
            # because the coordinator is dead by now due to the error injection
            # above
            await manager.server_start(server_b.server_id, timeout=5)
            pytest.fail("Expected server_add to fail")
        except Exception:
            pass

        await manager.server_stop(server_b.server_id)
        await manager.server_stop(server_a.server_id)

        stop_event = asyncio.Event()
        async def worker():
            logger.info("Worker started")
            while not stop_event.is_set():
                for i in range(100):
                    await manager.cql.run_async(f"INSERT INTO {table} (a, b) VALUES ({i}, {i})")
                    response = await manager.cql.run_async(f"SELECT * FROM {table} WHERE a = {i}")
                    assert response[0].b == i
                await asyncio.sleep(0.1)
            logger.info("Worker stopped")

        await manager.server_start(server_a.server_id)
        await manager.driver_connect()

        worker_task = asyncio.create_task(worker())

        await asyncio.sleep(20)
        stop_event.set()
        await worker_task
