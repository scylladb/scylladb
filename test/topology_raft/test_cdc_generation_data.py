from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.topology.util import check_token_ring_and_group0_consistency

import pytest

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
