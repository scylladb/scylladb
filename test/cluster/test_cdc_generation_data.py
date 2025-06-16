from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.cluster.util import check_token_ring_and_group0_consistency
from test.cluster.conftest import skip_mode
import logging
import pytest
import asyncio


logger = logging.getLogger(__name__)

"""
The injection forces the topology coordinator to send CDC generation data in multiple parts,
if it didn't the command size would go over commitlog segment size limit making it impossible to commit and apply the command.
"""
@pytest.mark.asyncio
async def test_send_data_in_parts(manager: ManagerClient):
    config = {
        'schema_commitlog_segment_size_in_mb': 2
    }

    first_server = await manager.server_add(config=config)

    async with inject_error(manager.api, first_server.ip_addr, 'cdc_generation_mutations_replication'):
        async with inject_error(manager.api, first_server.ip_addr,
                                'cdc_generation_mutations_topology_snapshot_replication'):
            await manager.server_add(config=config)

    await check_token_ring_and_group0_consistency(manager)

    cql = manager.get_cql()
    rows = await cql.run_async("SELECT description FROM system.group0_history")

    for row in rows:
        if row.description.startswith('insert CDC generation data (UUID: ') and row.description.endswith('), part'):
            break
    else:
        pytest.fail("No CDC generation data sent in parts was found")


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_group0_apply_while_node_is_being_shutdown(manager: ManagerClient):
    # This a regression test for #24401.

    logger.info("Starting s0")
    s0 = await manager.server_add(cmdline=['--logger-log-level', 'raft_group0=debug'])

    logger.info("Injecting topology_state_load_before_update_cdc into s0")
    await manager.api.enable_injection(s0.ip_addr, "topology_state_load_before_update_cdc", False)

    logger.info("Starting s1")
    s1_start_task = asyncio.create_task(manager.server_add())

    logger.info("Waiting for topology_state_load_before_update_cdc on s0")
    log = await manager.server_open_log(s0.server_id)
    await log.wait_for('topology_state_load_before_update_cdc hit, wait for message')

    logger.info("Triggering s0 shutdown")
    stop_s0_task = asyncio.create_task(manager.server_stop_gracefully(s0.server_id))

    logger.info("Waiting for group0 to start aborting on s0")
    await log.wait_for('Raft group0 service is aborting...')

    logger.info("Releasing topology_state_load_before_update_cdc on s0")
    await manager.api.message_injection(s0.ip_addr, 'topology_state_load_before_update_cdc')

    await stop_s0_task
    try:
        await s1_start_task
    except Exception:
        pass  # ingore errors, since we don't care

    errors = await log.grep_for_errors()
    assert errors == []
