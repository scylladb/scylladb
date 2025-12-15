#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import pytest
from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient
from test.cluster.util import new_test_keyspace, reconnect_driver


@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_create_table_notification_deadlock_with_shutdown(manager: ManagerClient):
    """
    Execute a CREATE TABLE query during node shutdown and reproduce a deadlock between
    the create table notification and unregistering listeners.
    Reproduces scylladb/scylladb#27364
    """
    server = await manager.server_add()
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "") as ks:
        pause_in_notif_injection = "pause_in_allocate_tablets_for_new_table"
        await manager.api.enable_injection(server.ip_addr, pause_in_notif_injection, one_shot=True)

        # Start creating the table asynchronously. it will wait at the injection point during the notification.
        cql.run_async(f"CREATE TABLE {ks}.t (pk int primary key, v int)")

        log = await manager.server_open_log(server.server_id)
        mark = await log.mark()

        # Start shutting down the node. it will wait while unregistering a listener because there is
        # a notification running that holds the lock of the migration listeners vector.
        stop_task = asyncio.create_task(manager.server_stop_gracefully(server.server_id))
        await log.wait_for('Shutting down native transport server', timeout=60, from_mark=mark)
        await asyncio.sleep(1)

        # Now continue to run the nested notification. Since there is a waiter, it may deadlock when
        # reading the migration listeners vector.
        await manager.api.message_injection(server.ip_addr, pause_in_notif_injection)
        await stop_task

        # reconnect for dropping the keyspace
        await manager.server_start(server.server_id)
        await reconnect_driver(manager)
