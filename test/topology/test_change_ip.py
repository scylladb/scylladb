#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test clusters can restart fine after an IP address change.
"""

import logging
import pytest
logger = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_change_two(manager, random_tables):
    """Stop two nodes, change their IPs and start, check the cluster is
    functional"""
    servers = await manager.running_servers()
    table = await random_tables.add_table(ncolumns=5)
    s_1 = servers[1].server_id
    s_2 = servers[2].server_id
    logger.info("Gracefully stopping servers %s and %s to change ips", s_1, s_2)
    await manager.server_stop_gracefully(s_1)
    await manager.server_stop_gracefully(s_2)
    await manager.server_change_ip(s_1)
    await manager.server_change_ip(s_2)
    await manager.server_start(s_1)
    await manager.server_start(s_2)
    await table.add_column()
    await random_tables.verify_schema()
