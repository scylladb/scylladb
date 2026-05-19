#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
from test.pylib.rest_client import inject_error, read_barrier
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts
from test.cluster.util import wait_for_cdc_generations_publishing, \
        check_system_topology_and_cdc_generations_v3_consistency

from cassandra.cluster import ConsistencyLevel # type: ignore # pylint: disable=no-name-in-module
from cassandra.pool import Host # type: ignore # pylint: disable=no-name-in-module
from cassandra.query import SimpleStatement # type: ignore # pylint: disable=no-name-in-module

import asyncio
import pytest
import logging
import time
from typing import Callable


logger = logging.getLogger(__name__)


async def fetch_committed_gen_ids(cql, host: Host, live_hosts: list[Host]) -> set[str]:
    query_committed_gen_ids = SimpleStatement(
        "SELECT host_id, committed_cdc_generations FROM system.topology",
        consistency_level = ConsistencyLevel.ONE)

    live_host_ids = {live_host.host_id for live_host in live_hosts}
    committed_rows = await cql.run_async(query_committed_gen_ids, host=host)
    committed_topology_rows = [row for row in committed_rows if row.host_id in live_host_ids]
    assert len(committed_topology_rows) != 0
    committed_cdc_generations = committed_topology_rows[0].committed_cdc_generations
    assert committed_cdc_generations is not None
    return {gen[1] for gen in committed_cdc_generations}


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_cdc_generation_clearing(manager: ManagerClient):
    """Test that obsolete CDC generations are removed from CDC_GENERATIONS_V3 and TOPOLOGY.committed_cdc_generations
       if their timestamp is old enough according to the topology coordinator's clock."""

    increase_leeway_injection = 'increase_cdc_generation_leeway'
    cleanup_injection = 'clean_obsolete_cdc_generations_change_ts_ub'

    cmdline = ['--logger-log-level', 'storage_service=trace:raft_topology=trace']
    config = {'error_injections_at_startup': [increase_leeway_injection]}

    logger.info("Bootstrapping first node")
    servers = [await manager.server_add(cmdline=cmdline, config=config)]

    cql = manager.get_cql()

    async def wait_for_published_generations() -> tuple[set[str], list[Host]]:
        deadline = time.time() + 60

        hosts = await wait_for_cql_and_get_hosts(cql, servers, deadline)
        await wait_for_cdc_generations_publishing(cql, hosts, deadline)

        committed_gen_ids = await fetch_committed_gen_ids(cql, hosts[0], hosts)
        return committed_gen_ids, hosts

    # The first generation should not be removed. We cannot remove the only generation.
    gen_ids, hosts = await wait_for_published_generations()
    logger.info(f"Generations after first clearing attempt: {gen_ids}")
    assert len(gen_ids) == 1
    first_gen_id = next(iter(gen_ids))
    await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts)

    logger.info("Bootstrapping second node")
    servers += [await manager.server_add(cmdline=cmdline, config=config)]

    # The first and second generations should not be removed. The first generation's timestamp is too close to the
    # topology coordinator's clock, which is ensured by the `increase_cdc_generation_leeway` error injection.
    gen_ids, hosts = await wait_for_published_generations()
    logger.info(f"Generations after second clearing attempt: {gen_ids}")
    assert len(gen_ids) == 2 and first_gen_id in gen_ids
    await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])
    await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts)
    second_gen_id = max(gen_ids)

    await asyncio.gather(*[manager.api.enable_injection(server.ip_addr, cleanup_injection, one_shot=False) for server in servers])
    config['error_injections_at_startup'].append(cleanup_injection)
    try:
        logger.info("Bootstrapping third node")
        servers += [await manager.server_add(cmdline=cmdline, config=config)]

        # The first and second generations should be removed thanks to the above injection.
        gen_ids, hosts = await wait_for_published_generations()
        logger.info(f"Generations after third clearing attempt: {gen_ids}")
        assert len(gen_ids) == 1 and first_gen_id not in gen_ids and second_gen_id not in gen_ids
        await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])
        await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts)
        third_gen_id = max(gen_ids)

        logger.info("Bootstrapping fourth node")
        servers += [await manager.server_add(cmdline=cmdline, config=config)]

        # The third generation should be removed thanks to the above injection.
        gen_ids, hosts = await wait_for_published_generations()
        logger.info(f"Generations after fourth clearing attempt: {gen_ids}")
        assert len(gen_ids) == 1 and third_gen_id not in gen_ids
        await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])
        await check_system_topology_and_cdc_generations_v3_consistency(manager, hosts)
    finally:
        config['error_injections_at_startup'].remove(cleanup_injection)
        await asyncio.gather(*[manager.api.disable_injection(server.ip_addr, cleanup_injection) for server in servers])


@pytest.mark.asyncio
async def test_unpublished_cdc_generations_arent_cleared(manager: ManagerClient):
    """Test that unpublished CDC generations aren't removed from CDC_GENERATIONS_V3 and
       TOPOLOGY.committed_cdc_generations regardless of their timestamps."""
    logger.info("Bootstrapping first node")
    servers = await manager.servers_add(1, config={
        'error_injections_at_startup': ['clean_obsolete_cdc_generations_change_ts_ub']
    })

    cql = manager.get_cql()
    logger.info("Waiting for driver")
    [host1] = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    logging.info("Waiting for the first CDC generation publishing")
    await wait_for_cdc_generations_publishing(cql, [host1], time.time() + 60)

    query_gen_ids = SimpleStatement(
        "SELECT id FROM system.cdc_generations_v3 WHERE key = 'cdc_generations'",
        consistency_level = ConsistencyLevel.ONE)

    async def get_gen_ids() -> set[str]:
        return {r.id for r in await cql.run_async(query_gen_ids, host=host1)}

    gen_ids = await get_gen_ids()
    assert len(gen_ids) == 1
    first_gen_id = next(iter(gen_ids))

    async with inject_error(manager.api, servers[0].ip_addr, "cdc_generation_publisher_fiber") as handler:
        logger.info("Bootstrapping second and third nodes")
        servers += await manager.servers_add(2)

        [host2, host3] = await wait_for_cql_and_get_hosts(cql, servers[-2:], time.time() + 60)

        log_file1 = await manager.server_open_log(servers[0].server_id)
        await log_file1.wait_for(f"CDC generation publisher fiber sleeps after injection")
        mark = await log_file1.mark()

        # The second and third generations are committed but unpublished due to the cdc_generation_publisher_fiber
        # injection. After unblocking the CDC generation publisher below, it should publish the second generation and
        # delete only the first generation. Note that all three generations are old enough to be deleted due to the
        # clean_obsolete_cdc_generations_change_ts_ub injection. So, only the third generation wouldn't be deleted (as
        # it is the last committed generation) if the CDC generation publisher didn't care whether a generation was
        # published.
        #
        # The message below will allow the CDC generation publisher to execute only one step of its loop so we can check
        # what it has done in this step. Eventually, the CDC generation publisher will publish all generations and
        # delete the first and second ones.
        await handler.message()
        await log_file1.wait_for(f"CDC generation publisher fiber sleeps after injection", from_mark=mark)
        mark = await log_file1.mark()
        gen_ids = await get_gen_ids()
        assert len(gen_ids) == 2 and first_gen_id not in gen_ids
        await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])
        await check_system_topology_and_cdc_generations_v3_consistency(manager, [host1, host2, host3])

        # Allow the CDC generation publisher to finish its job. One generation should remain.
        await handler.message()
        await log_file1.wait_for(f"CDC generation publisher fiber has nothing to do. Sleeping.", from_mark=mark)
        gen_ids = await get_gen_ids()
        assert len(gen_ids) == 1
        await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])
        await check_system_topology_and_cdc_generations_v3_consistency(manager, [host1, host2, host3])
