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
async def test_cdc_generation_clearing(manager: ManagerClient):
    """Test that obsolete CDC generations are removed from CDC_GENERATIONS_V3 if their timestamp is old enough
       according to the topology coordinator's clock."""
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

    # The first generation should not be removed. We cannot remove the only generation.
    mark, gen_ids = await wait_for(tried_to_remove_new_gen, time.time() + 60)
    logger.info(f"Generations after first clearing attempt: {gen_ids}")
    assert len(gen_ids) == 1
    first_gen_id = next(iter(gen_ids))

    logger.info("Bootstrapping second node")
    servers += [await manager.server_add()]

    # The first and second generations should not be removed. The first generation's timestamp is too close to the
    # topology coordinator's clock.
    mark, gen_ids = await wait_for(tried_to_remove_new_gen, time.time() + 60)
    logger.info(f"Generations after second clearing attempt: {gen_ids}")
    assert len(gen_ids) == 2 and first_gen_id in gen_ids
    second_gen_id = max(gen_ids)

    await inject_error_one_shot(manager.api, servers[0].ip_addr, "clean_obsolete_cdc_generations_change_ts_ub")

    logger.info("Bootstrapping third node")
    servers += [await manager.server_add()]

    # The first and second generations should be removed thanks to the above injection.
    mark, gen_ids = await wait_for(tried_to_remove_new_gen, time.time() + 60)
    logger.info(f"Generations after third clearing attempt: {gen_ids}")
    assert len(gen_ids) == 1 and first_gen_id not in gen_ids and second_gen_id not in gen_ids
