#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import time
import asyncio
import logging
import statistics

import pytest

from test.pylib.util import wait_for_cql_and_get_hosts


BATCH_SIZE = 50
NUMBER_OF_BATCHES = 40

logger = logging.getLogger(__name__)


@pytest.fixture
async def one_node_cluster(manager):
    node = await manager.server_add()
    await manager.server_start(node.server_id)
    await wait_for_cql_and_get_hosts(manager.get_cql(), [node], time.time() + 60)


@pytest.mark.slow
@pytest.mark.asyncio
@pytest.mark.replication_factor(1)
@pytest.mark.usefixtures("one_node_cluster")
async def test_schema_change_performance(random_tables):
    """Reproduce problem described in #7620 using CQL instead of Alternator.

    The difference with Alternator reproducer that all tables created in a single keyspace.
    """
    async def add_drop_table():
        start_time = time.perf_counter()
        await random_tables.drop_table(await random_tables.add_table(ncolumns=5))
        return time.perf_counter() - start_time

    average_durations = []

    for n in range(NUMBER_OF_BATCHES):
        batch = []
        for _ in range(BATCH_SIZE):
            batch.append(add_drop_table())
        average_durations.append(statistics.mean(await asyncio.gather(*batch)))
        logger.debug("average duration of add/drop table in batch #%03d is %.3fs", n, average_durations[-1])

    logger.info(
        "average durations of add/drop table iterations in %d batches (batch size is %d): %s",
        NUMBER_OF_BATCHES, BATCH_SIZE, average_durations,
    )

    await asyncio.sleep(1)

    logger.debug("verifying schema status")
    await random_tables.verify_schema()
