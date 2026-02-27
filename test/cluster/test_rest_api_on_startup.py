# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
from test.pylib.manager_client import ManagerClient

import asyncio
from datetime import datetime, timedelta
import pytest
import logging
import time

from test.pylib.rest_client import HTTPError

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_rest_api_on_startup(request, manager: ManagerClient):

    host = None
    stop_loop = False

    # Asynchronously keep sending a REST API request in a loop.
    # This lets us verify that the server doesn't crash when it is
    # started or restarted while requests are ongoing.
    async def test_rest_api():
        timeout = 60 # seconds
        start_time = time.time()

        while True:
            if time.time() - start_time > timeout:
                raise TimeoutError

            try:
                logger.info(f"Sending raft_topology/reload request")
                result = await manager.api.client.post("/storage_service/raft_topology/reload", host=host)
                logger.info(f"Received result {result}")
                if stop_loop:
                    return
            except Exception as ex:
                # Some errors are expected, for example:
                # - Initially, `host=None`, so `manager.api.client.post` fails
                # - Scylla returns 404 until the `/storage_service/raft_topology/reload` endpoint exists
                # - aiohttp raises ClientConnectorError when it cannot connect
                # This is okay. The important point is that ScyllaDB does not crash,
                # so the final request should succeed.
                pass

            # Avoid spamming requests to prevent log flooding.
            # One request per millisecond should be sufficient to expose issues.
            await asyncio.sleep(0.001)

    fut = asyncio.create_task(test_rest_api())
    logger.info("Starting server")
    server = await manager.server_add()
    host = server.ip_addr

    logger.info("Restarting server")
    await manager.server_restart(server.server_id)

    logger.info("Stopping the loop")
    stop_loop = True
    await fut

