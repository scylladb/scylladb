#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import pytest
import logging

from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_no_dc_rack_change(manager: ManagerClient) -> None:
    """
    Check that it is not possible to change node's DC or rack during restart.
    """
    # Case 1: changed DC name - should fail
    s1 = await manager.server_add(config = {"endpoint_snitch": "SimpleSnitch"}, property_file = {'dc': 'DC1', 'rack' : 'rack1'})
    await manager.server_update_config(s1.server_id, 'endpoint_snitch', 'GossipingPropertyFileSnitch')
    await manager.server_stop_gracefully(s1.server_id)
    await manager.server_start(s1.server_id, expected_error="Saved DC name \"datacenter1\" is not equal to the DC name \"DC1\" specified by the snitch")
    await manager.server_update_config(s1.server_id, 'endpoint_snitch', 'SimpleSnitch')
    await manager.server_start(s1.server_id)
    
    # Case 2: changed rack name - should fail
    s2 = await manager.server_add(config = {"endpoint_snitch": "SimpleSnitch"}, property_file = {'dc': 'datacenter1', 'rack' : 'R1'})
    await manager.server_update_config(s2.server_id, 'endpoint_snitch', 'GossipingPropertyFileSnitch')
    await manager.server_stop_gracefully(s2.server_id)
    await manager.server_start(s2.server_id, expected_error="Saved rack name \"rack1\" is not equal to the rack name \"R1\" specified by the snitch")
    await manager.server_update_config(s2.server_id, 'endpoint_snitch', 'SimpleSnitch')
    await manager.server_start(s2.server_id)
