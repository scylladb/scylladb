#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import asyncio
import logging
import pytest
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts
from test.pylib.tablets import get_tablet_replica
from test.cluster.util import new_test_keyspace

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@pytest.mark.skip(reason="fix not yet applied")
@pytest.mark.skip_mode(
    mode="release", reason="error injections are not supported in release mode"
)
async def test_alter_column_type_does_not_lose_data(manager: ManagerClient) -> None:
    """
    Reproduces a race condition where ALTER TABLE ... ALTER c TYPE varint
    causes a transient data visibility loss.

    The scenario: a read is in flight with an old schema (int) while ALTER TABLE
    commits a new schema (varint) to the memtable. When the read resumes, the
    replica tries to convert varint data back to int via schema_upgrader_v2's
    converting_mutation_partition_applier::accept_cell. Since
    int->is_value_compatible_with(varint) returns false, without the fix the
    cell is silently dropped, making the query return NULL instead of the actual
    value.

    The fix: mutation_reader::do_upgrade_schema detects the schema version
    downgrade (target schema is older than stream schema, both are raft-based
    timeuuid) and throws incompatible_schema_downgrade_exception. The CQL
    transport layer catches this and returns UNPREPARED to the driver, which
    re-prepares with the latest schema and retries the query successfully.

    The test exercises both read paths:
    - Local: coordinator IS the replica (exception propagates in-process)
    - Remote: coordinator is NOT the replica (exception propagates via RPC
      using replica::exception_variant)
    """
    cmdline = [
        "--logger-log-level",
        "debug_error_injection=debug",
    ]

    servers = await manager.servers_add(2, cmdline=cmdline)
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    await manager.disable_tablet_balancing()

    async with new_test_keyspace(
        manager,
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
        " AND tablets = {'initial': 1}",
    ) as ks:

        async def find_replica_idx(table_name):
            """Return the index into servers/hosts of the node holding the tablet replica."""
            replica_host_id, _ = await get_tablet_replica(manager, servers[0], ks, table_name, 0)
            for i, s in enumerate(servers):
                host_id = await manager.get_host_id(s.server_id)
                if host_id == replica_host_id:
                    return i
            raise RuntimeError(f"Could not find replica server for {table_name}")

        async def run_race_scenario(table_name, local_coordinator, label):
            """
            Run the ALTER TYPE race scenario for a single table.

            :param table_name: table to test (must already be created with int column)
            :param local_coordinator: if True, coordinator = replica (local path);
                                      if False, coordinator != replica (remote/RPC path)
            :param label: label for log messages
            """
            replica_idx = await find_replica_idx(table_name)
            coordinator_idx = replica_idx if local_coordinator else 1 - replica_idx
            replica_server = servers[replica_idx]
            coordinator_host = hosts[coordinator_idx]
            replica_log = await manager.server_open_log(replica_server.server_id)

            logger.info(f"[{label}] Table {table_name}: replica on server {replica_idx},"
                        f" coordinator on server {coordinator_idx}")

            await cql.run_async(f"INSERT INTO {ks}.{table_name} (pk, v) VALUES (1, 10)")

            # Prepare SELECT with the original int schema
            select_stmt = cql.prepare(f"SELECT v FROM {ks}.{table_name} WHERE pk = ?")

            # Verify data is readable before ALTER
            rows = await cql.run_async(select_stmt, [1], host=coordinator_host)
            assert len(rows) == 1 and rows[0].v == 10, (
                f"[{label}] Pre-ALTER check failed: {rows}"
            )

            # Pause reads on the replica inside table::query()
            await manager.api.enable_injection(
                replica_server.ip_addr,
                "replica_query_wait",
                one_shot=False,
                parameters={"table": table_name},
            )

            log_mark = await replica_log.mark()

            # Start the prepared SELECT — it will pause at the injection
            logger.info(f"[{label}] Starting prepared SELECT (will pause at replica_query_wait)")
            select_task = asyncio.ensure_future(
                cql.run_async(select_stmt, [1], host=coordinator_host)
            )

            # Wait for the read to hit the injection point on the replica
            await replica_log.wait_for(
                "replica_query_wait: waiting",
                from_mark=log_mark,
                timeout=60,
            )

            # ALTER TABLE while the read is paused. This completes fully
            # (commit + post_commit), so the memtable schema becomes varint
            # and prepared statement caches are invalidated.
            logger.info(f"[{label}] Running ALTER TABLE while read is paused")
            await cql.run_async(f"ALTER TABLE {ks}.{table_name} ALTER v TYPE varint")
            logger.info(f"[{label}] ALTER TABLE completed")

            # Release the read. do_upgrade_schema detects the schema downgrade
            # and accept_cell throws incompatible_schema_downgrade_exception.
            # The exception propagates to the CQL layer as UNPREPARED; the
            # driver re-prepares and retries.
            logger.info(f"[{label}] Releasing replica_query_wait")
            await manager.api.message_injection(
                replica_server.ip_addr, "replica_query_wait"
            )
            await manager.api.disable_injection(
                replica_server.ip_addr, "replica_query_wait"
            )

            logger.info(f"[{label}] Waiting for SELECT to complete (may re-prepare and retry)")
            rows = await select_task
            assert len(rows) == 1, f"[{label}] Expected 1 row, got {len(rows)}"
            assert rows[0].v == 10, (
                f"[{label}] Data loss detected during ALTER TYPE race:"
                f" expected v=10, got v={rows[0].v}"
            )

            # Final verification with a fresh query
            rows = await cql.run_async(f"SELECT v FROM {ks}.{table_name} WHERE pk = 1")
            assert len(rows) == 1 and rows[0].v == 10, (
                f"[{label}] Post-test verification failed: {rows}"
            )
            logger.info(f"[{label}] PASSED")

        # Two tables because ALTER int->varint is one-way
        await cql.run_async(f"CREATE TABLE {ks}.t_local (pk int PRIMARY KEY, v int)")
        await cql.run_async(f"CREATE TABLE {ks}.t_remote (pk int PRIMARY KEY, v int)")

        # Sub-test 1: coordinator IS the replica (local read path)
        await run_race_scenario("t_local", local_coordinator=True, label="local")

        # Sub-test 2: coordinator is NOT the replica (remote read path via RPC)
        await run_race_scenario("t_remote", local_coordinator=False, label="remote")
