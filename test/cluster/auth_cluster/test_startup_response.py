#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import pytest
import logging
import sys
import asyncio
import concurrent.futures
import time
from unittest import mock
from cassandra.cluster import Cluster, DefaultConnection, NoHostAvailable
from cassandra import connection
from cassandra.auth import PlainTextAuthProvider
from test.pylib.manager_client import ManagerClient
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config

@pytest.mark.asyncio
async def test_startup_no_auth_response(manager: ManagerClient, build_mode):
    """
    Test behavior when client hangs on startup auth response.
    This is stressing uninitialized_connections_semaphore_cpu_concurrency
    switching between CPU and Network states (1 or 0 semaphore units taken
    per connection).
    Test is probabilistic in the sense that it triggers bug reliably
    only with sufficient `num_connections` but empirically this number
    is tested to be very low.
    """
    server = await manager.server_add(config=auth_config)

    # Define a custom connection class that hangs on startup response
    class NoOpConnection(DefaultConnection):
        def _handle_startup_response(self, startup_response):
            pass

    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')

    connections_observed = False
    num_connections = 100
    timeout = 360

    def attempt_bad_connection():
        c = Cluster([server.ip_addr], port=9042, auth_provider=auth_provider, connect_timeout=timeout, connection_class=NoOpConnection)
        try:
            c.connect()
            pytest.fail("Should not connect")
        except Exception:
            # We expect failure or timeout
            pass
        finally:
            c.shutdown()

    def attempt_good_connection():
        nonlocal connections_observed
        c = Cluster([server.ip_addr], port=9042, auth_provider=auth_provider, connect_timeout=timeout/3)
        try:
            session = c.connect()
            res = session.execute("SELECT COUNT(*) FROM system.clients WHERE connection_stage = 'AUTHENTICATING' ALLOW FILTERING;")
            count = res[0][0]
            logging.info(f"Observed {count} AUTHENTICATING connections...")
            if count >= num_connections/2:
                connections_observed = True
        finally:
            c.shutdown()

    loop = asyncio.get_running_loop()

    logging.info("Attempting concurrent connections with custom hanging connection class...")
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=num_connections + 1)

    async def verify_loop():
        logging.info("Verifying server availability concurrently...")
        start_time = time.time()
        while time.time() - start_time < timeout:
            logging.info(f"Good connection attempt at delta {time.time() - start_time:.2f}s")
            try:
                await loop.run_in_executor(executor, attempt_good_connection)
            except Exception as e:
                logging.info(f"Good connection attempt failed: {e}")
            if connections_observed:
                break
            await asyncio.sleep(0.1)
        logging.info("Verification loop completed")

    good_future = asyncio.create_task(verify_loop())
    bad_futures = [loop.run_in_executor(executor, attempt_bad_connection) for _ in range(num_connections)]

    await good_future
    executor.shutdown(wait=False, cancel_futures=True)
    assert connections_observed
