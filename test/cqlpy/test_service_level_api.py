# -*- coding: utf-8 -*-
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

########################################
# Tests for the service levels HTTP API.
########################################

import pytest
from .rest_api import get_request, post_request
from .util import new_session, unique_name
import time

def get_shard_count(cql):
    return cql.execute("SELECT shard_count FROM system.topology").one().shard_count

def read_barrier(cql):
    cql.execute("DROP TABLE IF EXISTS nosuchkeyspace.nosuchtable")

def count_opened_connections(cql, retry_unauthenticated=True):
    response = get_request(cql, "service_levels/count_connections")
    return response

def switch_tenants(cql):
    return post_request(cql, "service_levels/switch_tenants")

def count_opened_connections_from_table(cql):
    connections = cql.execute("SELECT username, scheduling_group FROM system.clients WHERE client_type='cql' ALLOW FILTERING")
    result = {}
    for row in connections:
        user = row[0]
        shg = row[1]

        if shg in result:
            if user in result[shg]:
                result[shg][user] += 1
            else:
                result[shg][user] = 1
        else:
            result[shg] = {user: 1}
    
    return result

def wait_until_all_connections_authenticated(cql, wait_s = 1, timeout_s = 30):
    start_time = time.time()
    while time.time() - start_time < timeout_s:
        result = cql.execute("SELECT COUNT(*) FROM system.clients WHERE username='anonymous' ALLOW FILTERING")
        if result.one()[0] == 0:
            return
        else:
            time.sleep(wait_s)
    
    raise RuntimeError(f"Awaiting for connections authentication timed out.")

# The driver creates 1 connection per shard plus 1 control connection.
# This function validates that all connections execept the control one use correct scheduling group.
def verify_scheduling_group_assignment(cql, user, target_scheduling_group, shard_count):
    shards_with_correct_sg = set()
    connections = cql.execute(f"SELECT username, scheduling_group, shard_id FROM system.clients WHERE client_type='cql' AND username='{user}' ALLOW FILTERING")

    for conn in connections:
        if target_scheduling_group in conn.scheduling_group:
            shards_with_correct_sg.add(conn.shard_id)
    
    assert len(shards_with_correct_sg) == shard_count, (f"Not all user '{user}' connections are working under target scheduling group '{target_scheduling_group}'."
                                                        f"Shards with correct scehduling group: {shards_with_correct_sg}, shard")

# Test if `/service_levels/count_connections` prints counted CQL connections
# per scheduling group per user.
def test_count_opened_cql_connections(cql):
    user = f"test_user_{unique_name()}"
    sl = f"sl_{unique_name()}"

    cql.execute(f"CREATE ROLE {user} WITH login = true AND password='{user}'")
    cql.execute(f"CREATE SERVICE LEVEL {sl} WITH shares = 100")
    cql.execute(f"ATTACH SERVICE LEVEL {sl} TO {user}")
    read_barrier(cql)

    try:
        with new_session(cql, user):
            wait_until_all_connections_authenticated(cql)
            verify_scheduling_group_assignment(cql, user, sl, get_shard_count(cql))

            api_response = count_opened_connections(cql)
            assert f"sl:{sl}" in api_response
            assert user in api_response[f"sl:{sl}"]

            table_response = count_opened_connections_from_table(cql)
            assert api_response == table_response
    finally:
        cql.execute(f"DETACH SERVICE LEVEL FROM {user}")
        cql.execute(f"DROP ROLE {user}")
        cql.execute(f"DROP SERVICE LEVEL {sl}")

# Test if `/service_levels/switch_tenants` updates scheduling group 
# of CQL connections without restarting them.
# 
# This test creates a `test_user` and 2 service levels `sl1` and `sl2`.
# Firstly the user is assigned to `sl1` and his connections is created.
# Then the test changes user's service level to `sl2` and 
# `/service_levels/switch_tenants` endpoint is called.
def test_switch_tenants(cql):
    user = f"test_user_{unique_name()}"
    sl1 = f"sl1_{unique_name()}"
    sl2 = f"sl2_{unique_name()}"
    shard_count = get_shard_count(cql)

    cql.execute(f"CREATE ROLE {user} WITH login = true AND password='{user}' AND superuser = true")
    cql.execute(f"CREATE SERVICE LEVEL {sl1} WITH shares = 100")
    cql.execute(f"CREATE SERVICE LEVEL {sl2} WITH shares = 200")
    cql.execute(f"ATTACH SERVICE LEVEL {sl1} TO {user}")
    read_barrier(cql)

    try:
        with new_session(cql, user) as user_session:
            wait_until_all_connections_authenticated(cql)
            verify_scheduling_group_assignment(cql, user, sl1, shard_count)

            cql.execute(f"DETACH SERVICE LEVEL FROM {user}")
            cql.execute(f"ATTACH SERVICE LEVEL {sl2} TO {user}")
            read_barrier(cql)

            switch_tenants(cql)
            # Switching tenants may be blocked if a connection is waiting for a request (see 'generic_server::connection::process_until_tenant_switch()').
            # Execute enough cheap statements, so that connection on each shard will process at one statement and update its tenant.
            for _ in range(100):
                read_barrier(user_session)
            verify_scheduling_group_assignment(cql, user, sl2, shard_count)
    finally:
        cql.execute(f"DETACH SERVICE LEVEL FROM {user}")
        cql.execute(f"DROP ROLE {user}")
        cql.execute(f"DROP SERVICE LEVEL {sl1}")
        cql.execute(f"DROP SERVICE LEVEL {sl2}")
