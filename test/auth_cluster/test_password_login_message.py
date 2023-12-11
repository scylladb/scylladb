#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.pylib.manager_client import ManagerClient
import pytest
import logging
from cassandra.cluster import NoHostAvailable

logger = logging.getLogger(__name__)

"""
Tests the error message after half of the cluster goes down.

This test reproduces ScyllaDB OSS issue #2339. We create a
new cluster with 2 nodes and then stop one of them. We then
try to connect to the cluster and expect to get the error
message:
"Cannot achieve consistency level for cl QUORUM. Requires 1, alive 0"
"""
@pytest.mark.asyncio
async def test_login_message_after_half_of_the_cluster_is_down(manager: ManagerClient) -> None:
    """Tests the error message after half of the cluster goes down"""
    config = {
        'failure_detector_timeout_in_ms': 2000,
        'authenticator': 'PasswordAuthenticator',
        'authorizer': 'CassandraAuthorizer',
        'permissions_validity_in_ms': 0,
        'permissions_update_interval_in_ms': 0,
    }

    servers = await manager.servers_add(2, config=config)
    cql = manager.get_cql()
    # system_auth is ALTERed to make the test stable, because by default
    # system_auth has SimpleStrategy with RF=1, but if RF=1, we cannot make sure
    # which node has the replica, and we have to kill one node
    cql.execute(f"ALTER KEYSPACE system_auth WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}}")
    await manager.server_stop_gracefully(servers[1].server_id)
    try:
        """This is expected to fail"""
        await manager.driver_connect(server=servers[0])
    except NoHostAvailable as e:
        message = str([v for k, v in e.args[1].items()][0])
        expected_msg = "Cannot achieve consistency level for cl QUORUM. Requires 2, alive 1"
        assert expected_msg in message, f"Expected message: '{expected_msg}', got: '{message}'"
    else:
        pytest.fail("Expected NoHostAvailable exception")
