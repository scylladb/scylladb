#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import pytest
import logging
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
    only with sufficient `num_connections`.
    """
    server = await manager.server_add(config=auth_config)

    # Define a custom connection class that hangs on startup response
    class NoOpConnection(DefaultConnection):
        def _handle_startup_response(self, startup_response):
            pass

    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')

    bad_timeout = 10
    # Good timeout has to be lower than bad to ensure that there
    # are enough bad connections to potentially stuck the server.
    good_timeout = 5

    if build_mode == 'debug':
        bad_timeout *= 2
        good_timeout *= 2

    def attempt_bad_connection():
        c = Cluster([server.ip_addr], port=9042, auth_provider=auth_provider, connect_timeout=bad_timeout, connection_class=NoOpConnection)
        try:
            c.connect()
            pytest.fail("Should not connect")
        except Exception:
            # We expect failure or timeout
            pass
        finally:
            c.shutdown()

    def attempt_good_connection():
         c = Cluster([server.ip_addr], port=9042, auth_provider=auth_provider, connect_timeout=good_timeout)
         try:
            session = c.connect()
            session.execute("SELECT now() FROM system.local")
         finally:
            c.shutdown()

    loop = asyncio.get_running_loop()
    num_connections = 256
    logging.info("Attempting concurrent connections with custom hanging connection class...")
    
    # We use a ThreadPoolExecutor to run blocking driver calls.
    # We allocate enough workers for all bad connections + one for the verification loop.
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_connections + 1) as executor:
        
        async def verify_loop():
            logging.info("Verifying server availability concurrently...")
            start_time = time.time()
            while time.time() - start_time < bad_timeout:
                logging.info(f"Good connection attempt at delta {time.time() - start_time:.2f}s")
                await loop.run_in_executor(executor, attempt_good_connection)
                await asyncio.sleep(0.5)
            logging.info("Verification loop completed successfully")

        # Create the verification task first to ensure it's scheduled
        good_future = asyncio.create_task(verify_loop())

        # Launch bad connections
        bad_futures = [loop.run_in_executor(executor, attempt_bad_connection) for _ in range(num_connections)]
        
        # Wait for all connections to finish
        await good_future
        await asyncio.gather(*bad_futures)
