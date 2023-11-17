# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient

import pytest
import logging

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_concurrent_bootstrap(manager: ManagerClient):
    """"Add 3 nodes concurrently to an empty cluster"""
    await manager.servers_add(3)
