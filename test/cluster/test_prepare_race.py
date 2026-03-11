#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import pytest

from test.cluster.util import new_test_keyspace, new_test_table
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_prepare_fails_if_cached_statement_is_invalidated_mid_prepare(manager: ManagerClient):
    server = await manager.server_add()
    cql = manager.get_cql()
    log = await manager.server_open_log(server.server_id)
    
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY") as table:
            query = f"SELECT * FROM {table} WHERE pk = ?"
            loop = asyncio.get_running_loop()
            await cql.run_async(f"INSERT INTO {table} (pk) VALUES (7)")
            await cql.run_async(f"INSERT INTO {table} (pk) VALUES (8)")

            handler = await inject_error_one_shot(manager.api, server.ip_addr, "query_processor_prepare_wait_after_cache_get")
            mark = await log.mark()
            prepare_future = loop.run_in_executor(None, lambda: cql.prepare(query))
            await log.wait_for("query_processor_prepare_wait_after_cache_get: waiting for message", from_mark=mark, timeout=60)

            # Trigger table schema update (metadata-only) to invalidate prepared statements while PREPARE is paused.
            await cql.run_async(f"ALTER TABLE {table} WITH comment = 'invalidate-prepared-race'")

            await handler.message()
            done, _ = await asyncio.wait({prepare_future}, timeout=15)
            if not done:
                pytest.fail("Timed out waiting for PREPARE to complete after signaling injection")

            result = done.pop().result()
            print(f"PREPARE succeeded as expected: {result!r}")

            rows = cql.execute(result, [7])
            row = rows.one()
            assert row is not None and row.pk == 7

            # Invalidate prepared statements again, then execute the same prepared object.
            # The driver should transparently re-prepare and re-request execution.
            await cql.run_async(f"ALTER TABLE {table} WITH comment = 'invalidate-prepared-race-again'")

            reprepare_handler = await inject_error_one_shot(manager.api, server.ip_addr, "query_processor_prepare_wait_after_cache_get")
            reprepare_mark = await log.mark()
            execute_future = loop.run_in_executor(None, lambda: cql.execute(result, [8]))
            await log.wait_for("query_processor_prepare_wait_after_cache_get: waiting for message", from_mark=reprepare_mark, timeout=60)

            await reprepare_handler.message()
            execute_done, _ = await asyncio.wait({execute_future}, timeout=15)
            if not execute_done:
                pytest.fail("Timed out waiting for driver execute to finish after re-prepare signaling")

            retried_rows = execute_done.pop().result()
            retried_row = retried_rows.one()
            assert retried_row is not None and retried_row.pk == 8
