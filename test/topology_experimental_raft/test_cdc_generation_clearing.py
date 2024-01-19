#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.rest_client import inject_error_one_shot
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts

from cassandra.cluster import ConsistencyLevel # type: ignore # pylint: disable=no-name-in-module
from cassandra.query import SimpleStatement # type: ignore # pylint: disable=no-name-in-module

import pytest
import logging
import time
from typing import Optional


logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_current_cdc_generation_is_not_removed(manager: ManagerClient):
    """Test that the current CDC generation is not removed from CDC_GENERATIONS_V3 by the CDC generation
       publisher regardless of its timestamp."""
    # We enable the injection to ensure that a too-late timestamp does not prevent removing the CDC generation.
    logger.info("Bootstrapping first node")
    server = await manager.server_add(
        cmdline=['--logger-log-level', 'storage_service=trace:raft_topology=trace'],
        config={'error_injections_at_startup': ['clean_obsolete_cdc_generations_ignore_ts']}
    )

    log_file = await manager.server_open_log(server.server_id)

    # Wait until the CDC geneneration publisher publishes the first generation and tries to remove it.
    await log_file.wait_for("CDC generation publisher fiber has nothing to do. Sleeping.")

    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)
    query_gen_ids = SimpleStatement(
        "SELECT id FROM system.cdc_generations_v3 WHERE key = 'cdc_generations'",
        consistency_level = ConsistencyLevel.ONE)

    # The first and only generation should not be removed.
    gen_ids = {r.id for r in await cql.run_async(query_gen_ids)}
    logger.info(f"Generations after clearing attempt: {gen_ids}")
    assert len(gen_ids) == 1


@pytest.mark.asyncio
async def test_dependency_on_timestamps(manager: ManagerClient):
    """Test that CDC generations are not removed from CDC_GENERATIONS_V3 when the difference between their timestamp
       and the topology coordinator's clock is too small. Then, test that the CDC generation publisher removes
       the clean-up candidate (together with older generations) if its timestamp is old enough."""
    logger.info("Bootstrapping first node")
    servers = [await manager.server_add(cmdline=['--logger-log-level', 'storage_service=trace:raft_topology=trace'])]

    log_file1 = await manager.server_open_log(servers[0].server_id)
    mark: Optional[int] = None

    query_gen_ids = SimpleStatement(
        "SELECT id FROM system.cdc_generations_v3 WHERE key = 'cdc_generations'",
        consistency_level = ConsistencyLevel.ONE)

    async def tried_to_remove_new_gen() -> Optional[tuple[int, set[str]]]:
        await log_file1.wait_for("CDC generation publisher fiber has nothing to do. Sleeping.", mark)
        new_mark = await log_file1.mark()

        cql = manager.get_cql()
        await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
        new_gen_ids = {r.id for r in await cql.run_async(query_gen_ids)}

        return (new_mark, new_gen_ids)

    # The first generation should not be removed. It has become the clean-up candidate and its timestamp is too close
    # to the topology coordinator's clock.
    mark, gen_ids = await wait_for(tried_to_remove_new_gen, time.time() + 60)
    logger.info(f"Generations after first clearing attempt: {gen_ids}")
    assert len(gen_ids) == 1
    first_gen_id = next(iter(gen_ids))

    logger.info("Bootstrapping second node")
    servers += [await manager.server_add()]

    # Both generations should not be removed. The first generation is still the clean-up candidate with a
    # too-late timestamp.
    mark, gen_ids = await wait_for(tried_to_remove_new_gen, time.time() + 60)
    logger.info(f"Generations after second clearing attempt: {gen_ids}")
    assert len(gen_ids) == 2 and first_gen_id in gen_ids

    # We enable this injection to stop timestamps from preventing the clearing of CDC generations.
    await inject_error_one_shot(manager.api, servers[0].ip_addr, "clean_obsolete_cdc_generations_ignore_ts")

    logger.info("Bootstrapping third node")
    servers += [await manager.server_add()]

    # The first generation should be removed thanks to the above injection.
    mark, gen_ids = await wait_for(tried_to_remove_new_gen, time.time() + 60)
    logger.info(f"Generations after third clearing attempt: {gen_ids}")
    assert len(gen_ids) == 2 and first_gen_id not in gen_ids
