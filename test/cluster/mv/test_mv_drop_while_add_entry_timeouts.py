#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import time
from contextlib import AsyncExitStack

import pytest
from cassandra.policies import FallthroughRetryPolicy
from cassandra.protocol import ConfigurationException, ServerError
from cassandra.query import SimpleStatement

from test.cluster.util import new_test_keyspace
from test.pylib.manager_client import ManagerClient, wait_for_cql_and_get_hosts
from test.pylib.rest_client import inject_error
from test.pylib.util import unique_name, wait_for


# Reproduces scylladb/scylladb#24177
@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.skip_mode(mode='dev', reason='error injections are not supported in dev mode')
async def test_drop_mv_timeout_then_non_existing(manager: ManagerClient) -> None:
    node_count = 3
    servers = await manager.servers_add(node_count, auto_rack_dc="datacenter1")
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    host0 = hosts[0]

    keyspace_opts = "WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 3}"
    async with new_test_keyspace(manager, keyspace_opts, host=host0) as ks:
        await cql.run_async(
            f"CREATE TABLE {ks}.base (pk int, ck int, v text, PRIMARY KEY (pk, ck))",
            host=host0,
        )
        view_name = unique_name("test_view_")
        churn_table = None

        try:
            await cql.run_async(
                f"CREATE MATERIALIZED VIEW {ks}.{view_name} AS "
                f"SELECT pk, ck, v FROM {ks}.base "
                "WHERE pk IS NOT NULL AND ck IS NOT NULL "
                "PRIMARY KEY (ck, pk)",
                host=host0,
            )
            # ~100MB of payload: 100_000 rows * 1024 bytes each.
            row_count = 100_000
            value_size = 1024
            batch_size = 100
            value_payload = "v" * value_size
            for start in range(0, row_count, batch_size):
                inserts = [
                    f"INSERT INTO {ks}.base (pk, ck, v) VALUES ({i}, {i % 10}, '{value_payload}')"
                    for i in range(start, min(start + batch_size, row_count))
                ]
                batch = "BEGIN UNLOGGED BATCH\n" + "\n".join(inserts) + "\nAPPLY BATCH\n"
                await cql.run_async(batch, host=host0)

            for host in hosts:
                await cql.run_async(
                    "UPDATE system.config SET value = '75' WHERE name = 'group0_raft_op_timeout_in_ms'",
                    host=host,
                )

            churn_table = unique_name("churn_")
            create_churn_stmt = SimpleStatement(
                f"CREATE TABLE {ks}.{churn_table} (pk int PRIMARY KEY, v text)",
                retry_policy=FallthroughRetryPolicy(),
            )
            churn_created = False
            try:
                await cql.run_async(create_churn_stmt, host=host0)
                churn_created = True
            except ServerError as exc:
                if "raft operation [add_entry] timed out" not in str(exc):
                    raise

            if churn_created:
                try:
                    await cql.run_async(f"ALTER TABLE {ks}.{churn_table} ADD v2 text", host=host0)
                except Exception as exc:
                    msg = str(exc)
                    if "already exists" not in msg and "conflicts with an existing column" not in msg:
                        raise

            saw_timeout = False
            leader_host_id = await manager.api.get_raft_leader(servers[0].ip_addr)
            servers_by_host_id = await manager.all_servers_by_host_id()
            leader_server = servers_by_host_id.get(leader_host_id)
            injection_targets = [s.ip_addr for s in servers] if leader_server is None else [leader_server.ip_addr]

            async with AsyncExitStack() as stack:
                for node_ip in injection_targets:
                    await stack.enter_async_context(
                        inject_error(manager.api, node_ip, "group0_state_machine::delay_apply")
                    )
                stmt = SimpleStatement(
                    f"DROP MATERIALIZED VIEW {ks}.{view_name}",
                    retry_policy=FallthroughRetryPolicy(),
                )
                try:
                    await cql.run_async(stmt, host=host0)
                except ConfigurationException as exc:
                    if f"Cannot drop non existing materialized view '{view_name}' in keyspace '{ks}'" in str(exc):
                        pytest.fail(f"Drop MV reported non-existing on first drop: {exc}")
                    raise    
                except ServerError as exc:
                    # Timeout is expected in this repro; don't fail on it.
                    if "raft operation [add_entry] timed out" not in str(exc):
                        pytest.fail(f"Drop MV failed with unexpected ServerError: {exc}")
                    saw_timeout = True
                except Exception as exc:
                    pytest.fail(f"Failed with unexpected error: {exc}")

            if not saw_timeout:
                pytest.fail("Expected a raft add_entry timeout during DROP MV, but no timeout was observed.")
            # After the timeout, retry drop until it completes.
            async def drop_completed():
                try:
                    await cql.run_async(
                        f"DROP MATERIALIZED VIEW IF EXISTS {ks}.{view_name}",
                        host=host0,
                    )
                    return True
                except ServerError as exc:
                    if "raft operation [add_entry] timed out" in str(exc):
                        return None
                    pytest.fail(f"Drop MV retry failed with unexpected ServerError: {exc}")
                except Exception as exc:
                    pytest.fail(f"Drop MV retry failed with unexpected error: {exc}")
            await wait_for(drop_completed, time.time() + 60)
            # If we hit a "non existing materialized view" on the first drop, that's the bug.
        finally:
            await cql.run_async(f"DROP MATERIALIZED VIEW IF EXISTS {ks}.{view_name}", host=host0)
            if churn_table:
                await cql.run_async(f"DROP TABLE IF EXISTS {ks}.{churn_table}", host=host0)
            await cql.run_async(f"DROP TABLE IF EXISTS {ks}.base", host=host0)
