#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot
import pytest
import asyncio
import logging


logger = logging.getLogger(__name__)


async def inject_error_one_shot_on(manager, error_name, servers):
    errs = [inject_error_one_shot(manager.api, s.ip_addr, error_name) for s in servers]
    await asyncio.gather(*errs)


@pytest.mark.asyncio
async def test_tablet_metadata_propagates_with_schema_changes_in_snapshot_mode(manager: ManagerClient):
    """Test that you can create a table and insert and query data"""

    servers = await manager.running_servers()

    s0 = servers[0].server_id
    not_s0 = servers[1:]

    # s0 should miss schema and tablet changes
    await manager.server_stop_gracefully(s0)

    manager.cql.execute("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', "
                        "'replication_factor': 3, 'initial_tablets': 100};")

    # force s0 to catch up later from the snapshot and not the raft log
    await inject_error_one_shot_on(manager, 'raft_server_force_snapshot', not_s0)
    manager.cql.execute("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    keys = range(10)
    await asyncio.gather(*[manager.cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, 1);") for k in keys])

    rows = manager.cql.execute("SELECT * FROM test.test;")
    assert len(list(rows)) == len(keys)
    for r in rows:
        assert r.c == 1

    await manager.server_start(s0, wait_others=2)

    manager.driver_close()
    await manager.driver_connect(server=servers[0])

    # Trigger a schema change to invoke schema agreement waiting to make sure that s0 has the latest schema
    manager.cql.execute("CREATE KEYSPACE test_dummy WITH replication = {'class': 'NetworkTopologyStrategy', "
                        "'replication_factor': 1, 'initial_tablets': 1};")

    await asyncio.gather(*[manager.cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, 2);", execution_profile='whitelist')
                           for k in keys])

    rows = manager.cql.execute("SELECT * FROM test.test;").all()
    assert len(rows) == len(keys)
    for r in rows:
        assert r.c == 2

    # Check that after rolling restart the tablet metadata is still there
    for s in servers:
        await manager.server_restart(s, wait_others=2)

    manager.driver_close()
    await manager.driver_connect(server=servers[0])

    await asyncio.gather(*[manager.cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, 3);", execution_profile='whitelist')
                           for k in keys])

    rows = manager.cql.execute("SELECT * FROM test.test;").all()
    assert len(rows) == len(keys)
    for r in rows:
        assert r.c == 3

    manager.cql.execute("DROP KEYSPACE test;")
    manager.cql.execute("DROP KEYSPACE test_dummy;")


@pytest.mark.asyncio
async def test_scans(manager: ManagerClient):
    manager.cql.execute("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', "
                        "'replication_factor': 1, 'initial_tablets': 8};")
    manager.cql.execute("CREATE TABLE test.test (pk int PRIMARY KEY, c int);")

    keys = range(100)
    await asyncio.gather(*[manager.cql.run_async(f"INSERT INTO test.test (pk, c) VALUES ({k}, {k});") for k in keys])

    rows = manager.cql.execute("SELECT count(*) FROM test.test;")
    assert rows.one().count == len(keys)

    rows = manager.cql.execute("SELECT * FROM test.test;").all()
    assert len(rows) == len(keys)
    for r in rows:
        assert r.c == r.pk

    manager.cql.execute("DROP KEYSPACE test;")
