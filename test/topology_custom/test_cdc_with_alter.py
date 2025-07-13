#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.manager_client import ManagerClient
from test.topology.util import new_test_keyspace
from cassandra.protocol import InvalidRequest

import asyncio
import logging
import threading
import pytest


logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_add_and_drop_column_with_cdc(manager: ManagerClient):
    """ Test writing to a table with CDC enabled while adding and dropping a column.
        In particular we are interested at the behavior when the schemas of the base table
        and the CDC log may not be in sync, and we write a value to a column that exists
        in the base table but not in the CDC table.
        Reproduces #24952
    """

    servers = await manager.servers_add(3)
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'enabled': false}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, v int) WITH cdc={{'enabled': true}}")

        # sleep before CDC augmentation, because we want to have a write that starts with some base schema, and then
        # the table is altered while the write is in progress, and the CDC augmentation will use the new schema that
        # is not compatible with the base schema.
        await asyncio.gather(*[manager.api.enable_injection(s.ip_addr, "sleep_before_cdc_augmentation", one_shot=False) for s in servers])

        # The writer thread writes to the column 'a' while it's being added and dropped.
        # We want to write a value to that column while it's in different stages - may exist
        # in one table but not in the other.
        stop_writer = threading.Event()
        writer_error = threading.Event()
        def do_writes():
            i = 0
            try:
                while not stop_writer.is_set():
                    try:
                        cql.execute(f"INSERT INTO {ks}.test(pk, v, a) VALUES({i}, {i+1}, {i+2})")
                    except InvalidRequest as e:
                        if "Unknown identifier" in str(e) or "does not have base column" in str(e):
                            pass
                        else:
                            raise
                    i += 1
            except Exception as e:
                logger.error(f"Unexpected error while writing to {ks}.test: {e}")
                writer_error.set()

        writer_thread = threading.Thread(target=do_writes)
        writer_thread.start()

        await cql.run_async(f"ALTER TABLE {ks}.test ADD a int")
        await asyncio.sleep(1)
        await cql.run_async(f"ALTER TABLE {ks}.test DROP a")

        stop_writer.set()
        writer_thread.join()

        if writer_error.is_set():
            pytest.fail("Unexpected error occurred during writes to the table")

        base_rows = await cql.run_async(f"SELECT COUNT(*) FROM {ks}.test")
        cdc_rows = await cql.run_async(f"SELECT COUNT(*) FROM {ks}.test_scylla_cdc_log")
        assert base_rows[0].count == cdc_rows[0].count, f"Base table rows: {base_rows[0].count}, CDC log rows: {cdc_rows[0].count}"
