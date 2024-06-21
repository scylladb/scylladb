#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient, ServerInfo
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.topology.conftest import skip_mode

from cassandra.cluster import ConsistencyLevel, NoHostAvailable, Session
from cassandra.protocol import InvalidRequest
from cassandra.query import SimpleStatement

import asyncio
import pytest
import logging
import time
from datetime import datetime
from typing import Optional


logger = logging.getLogger(__name__)


async def wait_for_publishing_generations(cql: Session, servers: list[ServerInfo]) -> Optional[set[datetime]]:
    query_gen_timestamps = SimpleStatement(
        "SELECT tounixtimestamp(time) AS t FROM system_distributed.cdc_generation_timestamps WHERE key = 'timestamps'",
        consistency_level = ConsistencyLevel.ONE)

    async def generations_published() -> Optional[set[datetime]]:
        # Multiply by 1000 because USING TIMESTAMP expects microseconds.
        gen_timestamps = {r.t * 1000 for r in await cql.run_async(query_gen_timestamps)}
        assert len(gen_timestamps) <= len(servers)
        if len(gen_timestamps) == len(servers):
            return gen_timestamps
        return None

    logger.info("Waiting for publishing CDC generations")
    gen_timestamps = await wait_for(generations_published, time.time() + 60)
    logger.info(f"CDC generations' timestamps: {gen_timestamps}")
    return gen_timestamps


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_writes_to_recent_previous_cdc_generations(request, manager: ManagerClient):
    """
    Test that writes to previous CDC generations succeed if the timestamp of the generation being written to
    is greater than (now - generation_leeway)

    The test starts 3 nodes, and thus, it creates 3 generations. Let's denote their timestamps by ts1, ts2, ts3,
    where ts1 is the timestamp of the first (and oldest) generation, and so on. We create a scenario where
    now - generation_leeway < ts1 < ts2 < ts3 < now.
    We check that writes with a timestamp from the interval [ts1, now] succeed.

    The value of generation_leeway is 5 s. So, the condition
    now - generation_leeway < ts1
    might be false if the test runs too slowly. To avoid this, we increase generation_leeway to 5 min
    through the error injection.
    """
    logger.info("Bootstrapping nodes")
    servers = await manager.servers_add(3, config={
        'error_injections_at_startup': ['increase_cdc_generation_leeway']
    })

    cql = manager.get_cql()

    logger.info("Waiting for driver")
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    gen_timestamps = await wait_for_publishing_generations(cql, servers)

    logger.info("Creating a test table")
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int) WITH cdc = {'enabled': true}")

    async def do_write(timestamp: int):
        await cql.run_async(f"INSERT INTO test.test (pk, c) VALUES (0, 0) USING TIMESTAMP {timestamp}")

    logger.info("Executing writes to all generations")
    for ts in gen_timestamps:
        # Writes to all three generations should succeed.
        await do_write(ts + 1)
        await do_write(ts)
        # A write with a timestamp smaller than the oldest generation's one should fail.
        if ts == min(gen_timestamps):
            with pytest.raises(NoHostAvailable):
                await do_write(ts - 1)
        else:
            await do_write(ts - 1)


@pytest.mark.asyncio
@skip_mode('debug', 'test requires nodes to be started quickly')
async def test_writes_to_old_previous_cdc_generation(request, manager: ManagerClient):
    """
    Test that writes to a previous CDC generation succeed if the write's timestamp is greater than
    (now - generation_leeway) even if the generation's timestamp is not greater than (now - generation_leeway).

    The test starts 2 nodes, and thus, it creates 2 generations. Let's denote their timestamps by ts1 and ts2, where
    ts1 is the timestamp of the first (and oldest) generation. We create a scenario where
    ts1 < now - generation_leeway < ts2 < now.
    We check that writes with a timestamp from the interval (now - generation_leeway, ts2) succeed.

    ts2 might already be much smaller than now after we finish adding the second node. Then, a write with a timestamp
    smaller than ts2 could unexpectedly fail because we reject writes to previous generations with timestamps not
    greater than (now - generation_leeway). To make this situation less likely, we increase ring-delay to 5 s, which
    increases ts2 by 15 s (see cdc::new_generation_timestamp). If this situation still happens, the test detects it and
    doesn't fail. We don't want to increase ring-delay more as it would slow down the test even more. To sum up, the
    test shouldn't be flaky, but there is no guarantee that it will always test what it's supposed to test. We also
    ignore it in debug mode due to the reasons above.
    """
    logger.info("Bootstrapping nodes")
    servers = await manager.servers_add(2, cmdline=['--ring-delay', '5000'])

    cql = manager.get_cql()

    logger.info("Waiting for driver")
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    gen_timestamps = await wait_for_publishing_generations(cql, servers)

    logger.info("Creating a test table")
    await cql.run_async("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c int) WITH cdc = {'enabled': true}")

    ts2 = max(gen_timestamps)
    time_diff = (ts2 / (1000 * 1000)) - time.time()
    if time_diff > 0:
        logger.info("Waiting for the second generation to become active")
        await asyncio.sleep(time_diff)

    try:
        # Writes with timestamps from the interval (now - generation_leeway, ts2) should succeed.
        await cql.run_async(f"INSERT INTO test.test (pk, c) VALUES (0, 0) USING TIMESTAMP {ts2 - 1}")
    except InvalidRequest:
        # If the write has failed, we check that its timestamp is not greater than (now - generation_leeway). Then,
        # the failure is expected. This situation can happen if the test is running too slowly.
        # We use time.time() because we don't know "the real now" used by the node for verification. time.time() is for
        # sure not lower, so if the assertion below fails, it would also fail for "the real now". However, if the
        # assertion doesn't fail, we don't know if the write has been correct. We prefer a non-flaky test that catches
        # some bugs over a flaky one that catches more bugs.
        assert (ts2 - 1) / (1000 * 1000) <= time.time() - 5

    # Writes with timestamps not greater than (now - generation_leeway) should fail.
    with pytest.raises(InvalidRequest):
        await cql.run_async(f"INSERT INTO test.test (pk, c) VALUES (0, 0) USING TIMESTAMP {ts2 - 5 * 1000 * 1000}")
