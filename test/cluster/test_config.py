# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import logging
import pytest
import requests
import sys
import threading
import time

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import read_barrier
from test.pylib.util import wait_for

async def wait_for_config(manager, server, config_name, value):
    async def config_value_equal():
        await read_barrier(manager.api, server.ip_addr)
        resp = await manager.api.get_config(server.ip_addr, config_name)
        logging.info(f"Obtained config via REST api - config_name={config_name} value={value}")
        if resp == value:
            return True
        return None
    await wait_for(config_value_equal, deadline=time.time() + 60)

@pytest.mark.asyncio
async def test_non_liveupdatable_config(manager):

    server = await manager.server_add()
    not_liveupdatable_param = "auto_bootstrap"
    liveupdatable_param = "compaction_enforce_min_threshold"

    logging.info("Verify initial (default) config values")
    await wait_for_config(manager, server, not_liveupdatable_param, True)
    await wait_for_config(manager, server, liveupdatable_param, False)

    logging.info("Change config values - verify only liveupdatable config is changed")
    await manager.server_update_config(server.server_id, not_liveupdatable_param, False)
    await manager.server_update_config(server.server_id, liveupdatable_param, True)
    await wait_for_config(manager, server, liveupdatable_param, True)
    await wait_for_config(manager, server, not_liveupdatable_param, True)
