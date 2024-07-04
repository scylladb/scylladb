#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import pytest
import time
from test.topology.conftest import skip_mode
from test.pylib.manager_client import ManagerClient

from cassandra.cluster import ConsistencyLevel  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore


# Reproduces #18929
# Test view build operations running during node shutdown and view drain.
# Verify the drain order is correct and the view build doesn't fail with
# database write failures.
@pytest.mark.asyncio
@skip_mode('release', "error injections aren't enabled in release mode")
async def test_mv_build_during_shutdown(manager: ManagerClient):
    server = await manager.server_add()

    cql = manager.get_cql()
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE ks.t (pk int primary key, v int)")

    for i in range(100):
        await cql.run_async(f"insert into ks.t (pk, v) values ({i}, {i+1})")

    # Start building two views. The first is delayed by the injection, and the second
    # view build is queued, waiting on the view builder semaphore.
    await manager.api.enable_injection(server.ip_addr, "delay_before_get_view_natural_endpoint", one_shot=True)
    cql.run_async("CREATE materialized view ks.t_view1 AS select pk, v from ks.t where v is not null primary key (v, pk)")
    cql.run_async("CREATE materialized view ks.t_view2 AS select pk, v from ks.t where v is not null primary key (v, pk)")

    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()

    # Start node shutdown. this will drain and abort the running view build.
    # As we continue and drain the view building of view1 and view2 we will
    # have writes to the database, running during the draining phase.
    # If the drain order is correct it should succeed without errors.
    await manager.server_stop_gracefully(server.server_id)

    # Verify no db write errors during the shutdown
    occurrences = await log.grep(expr="exception during mutation write", from_mark=mark)
    assert len(occurrences) == 0
