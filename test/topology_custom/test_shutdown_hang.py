#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import asyncio
import logging
import time
import pytest

from cassandra.query import SimpleStatement # type: ignore
from cassandra.cluster import ConsistencyLevel # type: ignore
from cassandra.protocol import WriteTimeout # type: ignore

from test.pylib.manager_client import ManagerClient
from test.topology.util import wait_for_token_ring_and_group0_consistency
from test.topology.conftest import skip_mode


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_hints_manager_shutdown_hang(manager: ManagerClient) -> None:
    """Reproducer for #8079"""
    s1 = await manager.server_add(config={
        'error_injections_at_startup': ['decrease_hints_flush_period']
    })
    s2 = await manager.server_add()
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 30)

    cql = manager.get_cql()

    logger.info("Create keyspace and table")
    await cql.run_async("create keyspace ks with replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
    await cql.run_async("create table ks.t (pk int primary key)")

    logger.info(f"Stop {s2}")
    await manager.server_stop(s2.server_id)

    logger.info("Write data with small timeout")
    # We're using a small timeout for the insert so it's not unexpected that it would fail on slow
    # CI machines. To avoid flakiness we disable the test in debug mode (as well as release since
    # it requires an error injection - so it will run only in dev mode) and we retry the write 10 times.
    passed = False
    for _ in range(10):
        try:
            await cql.run_async(SimpleStatement("insert into ks.t (pk) values (0) using timeout 500ms",
                                                consistency_level=ConsistencyLevel.ONE))
        except WriteTimeout:
            logger.info("write timeout, retrying")
        else:
            passed = True
            break

    if not passed:
        pytest.fail("Write timed out on each attempt")

    # The write succeeded but a background task was left to finish the write to the other node
    # (which is dead but the first node didn't mark it as dead yet).
    # The background task will timeout shortly because of 'using timeout' in the statement.
    # This will cause a hint to get created.
    # The hints manager starts sending the hint soon after (hint flushing happens every
    # ~1 second with the error injection).
    logger.info("Sleep")
    await asyncio.sleep(2)

    logger.info(f"Stop {s1} gracefully")
    await manager.server_stop_gracefully(s1.server_id)
