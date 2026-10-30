#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient, ServerInfo
from test.pylib.rest_client import inject_error
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.topology.conftest import skip_mode

from cassandra.cluster import ConsistencyLevel # type: ignore # pylint: disable=no-name-in-module
from cassandra.query import SimpleStatement # type: ignore # pylint: disable=no-name-in-module

import asyncio
import pytest
import logging
import time
from datetime import datetime
from typing import Optional


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_cdc_generations_are_published(request, manager: ManagerClient):
    """Test that the CDC generation publisher eventually publishes committed CDC generations in the correct order."""
    query_gen_timestamps = SimpleStatement(
        "select time from system_distributed.cdc_generation_timestamps where key = 'timestamps'",
        consistency_level = ConsistencyLevel.ONE)

    servers = list[ServerInfo]()
    gen_timestamps = set[datetime]()

    async def new_gen_appeared() -> Optional[set[datetime]]:
        cql = manager.get_cql()
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        new_gen_timestamps = {r.time for r in await cql.run_async(query_gen_timestamps)}
        assert len(gen_timestamps) + 1 >= len(new_gen_timestamps)
        if gen_timestamps < new_gen_timestamps:
            gen_timestamps_diff = new_gen_timestamps.difference(gen_timestamps)
            # Check that we didn't lose any CDC generation.
            assert len(gen_timestamps_diff) == 1
            # Check that the new timestamp is not lower than the previous ones.
            new_timestamp = next(iter(gen_timestamps_diff))
            assert new_timestamp == max(new_gen_timestamps)
            return new_gen_timestamps
        return None

    logger.info("Bootstrapping first node")
    servers = [await manager.server_add()]
    gen_timestamps = await wait_for(new_gen_appeared, time.time() + 60)
    logger.info(f"Timestamps after bootstrapping first node: {gen_timestamps}")

    logger.info("Bootstrapping second node")
    servers += [await manager.server_add()]
    gen_timestamps = await wait_for(new_gen_appeared, time.time() + 60)
    logger.info(f"Timestamps after bootstrapping second node: {gen_timestamps}")

    logger.info("Bootstrapping third node")
    servers += [await manager.server_add()]
    gen_timestamps = await wait_for(new_gen_appeared, time.time() + 60)
    logger.info(f"Timestamps after bootstrapping third node: {gen_timestamps}")

    logger.info(f"Stopping node {servers[0]}")
    await manager.server_stop_gracefully(servers[0].server_id)
    logger.info(f"Removing node {servers[0]} using {servers[1]}")
    await manager.remove_node(servers[1].server_id, servers[0].server_id)
    servers = servers[1:]
    # Performing multiple cdc_streams_check_and_repair requests concurrently after removing a node should result
    # in creating exactly one CDC generation, because cdc_streams_check_and_repair doesn't create a new generation
    # if the current one is optimal.
    await asyncio.gather(*[manager.api.client.post("/storage_service/cdc_streams_check_and_repair", servers[i % 2].ip_addr)
                          for i in range(10)])
    gen_timestamps = await wait_for(new_gen_appeared, time.time() + 60)
    logger.info(f"Timestamps after check_and_repair: {gen_timestamps}")


@pytest.mark.asyncio
async def test_multiple_unpublished_cdc_generations(request, manager: ManagerClient):
    """Test that the CDC generation publisher works correctly when there is more than one unpublished CDC generation."""
    query_gen_timestamps = SimpleStatement(
        "select time from system_distributed.cdc_generation_timestamps where key = 'timestamps'",
        consistency_level = ConsistencyLevel.ONE)

    logger.info("Bootstrapping first node")
    servers = [await manager.server_add()]

    async with inject_error(manager.api, servers[0].ip_addr, "cdc_generation_publisher_fiber") as handler:
        # This injection delays publishing CDC generations committed after bootstrapping the following nodes.
        # After all 3 nodes bootstrap, there should be 3 or 4 unpublished CDC generations (4 if publishing the first
        # CDC generation hasn't started before injecting an error).
        logger.info("Bootstrapping other nodes")
        servers += await manager.servers_add(3)

        cql = manager.get_cql()
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

        gen_timestamps = set[datetime]()

        async def new_gen_appeared() -> Optional[set[datetime]]:
            new_gen_timestamps = {r.time for r in await cql.run_async(query_gen_timestamps)}
            assert gen_timestamps <= new_gen_timestamps
            if gen_timestamps < new_gen_timestamps:
                # Check that we didn't lose any CDC generations.
                assert not gen_timestamps.difference(new_gen_timestamps)
                # Check that all new timestamps are not lower than the previous ones.
                gen_timestamps_diff = new_gen_timestamps.difference(gen_timestamps)
                for new_timestamp in gen_timestamps_diff:
                    assert not gen_timestamps or new_timestamp >= max(gen_timestamps)
                return new_gen_timestamps
            return None

        # Check that all 4 CDC generations are eventually published in the correct order.
        for _ in range(4):
            await handler.message()
        while len(gen_timestamps) < 4:
            # We prefer to detect CDC generation publications one-by-one, because it increases our chances of catching
            # potential bugs like incorrect order of publications. Therefore, we use very short period - 0.01 s.
            gen_timestamps = await wait_for(new_gen_appeared, time.time() + 60, 0.01)

        assert len(gen_timestamps) == 4
