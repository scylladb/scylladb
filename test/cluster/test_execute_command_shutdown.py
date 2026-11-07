#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# A running EXECUTE COMMAND must not hold up shutdown for its whole sampling window.

import time

from cassandra.auth import PlainTextAuthProvider

from test.pylib.scylla_cluster_manager import ScyllaClusterManager

WINDOW_MS = 60000


async def start_long_command(manager: ScyllaClusterManager, config: dict):
    # The command is superuser-only.
    config = config | {'authenticator': 'PasswordAuthenticator', 'authorizer': 'CassandraAuthorizer'}
    [server] = await manager.servers_add(1, config=config, cmdline=['--logger-log-level', 'database=debug'],
        driver_connect_opts={'auth_provider': PlainTextAuthProvider(username='cassandra', password='cassandra')})
    log = await manager.server_open_log(server.server_id)
    cql = manager.get_cql()
    task = cql.run_async(f"EXECUTE COMMAND toppartitions WITH duration = {WINDOW_MS}", timeout=WINDOW_MS / 1000 + 60)
    await log.wait_for("toppartitions_data_listener: installing", timeout=60)
    return server, log, task


async def finish(task):
    try:
        await task
    except Exception:
        pass


# Node shutdown ends the window before the CQL server's drain, whose timeout here outlasts the window.
async def test_node_shutdown_aborts_command(manager: ScyllaClusterManager):
    server, _, task = await start_long_command(manager, {"request_timeout_on_shutdown_in_seconds": 120})
    start = time.monotonic()
    await manager.server_stop_gracefully(server.server_id)
    assert time.monotonic() - start < WINDOW_MS / 2000
    await finish(task)


# Stopping only the CQL server ends the window once its drain timeout expires.
async def test_cql_server_stop_aborts_command(manager: ScyllaClusterManager):
    server, log, task = await start_long_command(manager, {"request_timeout_on_shutdown_in_seconds": 1})
    await manager.api.client.delete("/storage_service/native_transport", host=server.ip_addr)
    await log.wait_for("toppartitions_data_listener: uninstalling", timeout=WINDOW_MS / 2000)
    await finish(task)
