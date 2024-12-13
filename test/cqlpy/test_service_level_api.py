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

def wait_for_scheduling_group_assignment(cql, user, scheduling_group, wait_s = 2, timeout_s = 60):
    start_time = time.time()
    while time.time() - start_time < timeout_s:
        connections = cql.execute(f"SELECT username, scheduling_group FROM system.clients WHERE client_type='cql' AND username='{user}' ALLOW FILTERING")
        
        require_wait = False
        for row in connections:
            if row[1] != f"sl:{scheduling_group}":
                require_wait = True
                break
        if require_wait:
            time.sleep(wait_s)
            continue
        return

    raise RuntimeError(f"Awaiting for user '{user}' to switch tenant to scheduling group '{scheduling_group}' timed out.")

# Test if `/service_levels/count_connections` prints counted CQL connections
# per scheduling group per user.
def test_count_opened_cql_connections(cql):
    user = f"test_user_{unique_name()}"
    sl = f"sl_{unique_name()}"

    cql.execute(f"CREATE ROLE {user} WITH login = true AND password='{user}'")
    cql.execute(f"CREATE SERVICE LEVEL {sl} WITH shares = 100")
    cql.execute(f"ATTACH SERVICE LEVEL {sl} TO {user}")

    # Service level controller updates in 10 seconds interval, so wait
    # for sl1 to be assgined to test_user
    time.sleep(10)
    try:
        with new_session(cql, user): # new sessions is created only to create user's connection to Scylla
            wait_until_all_connections_authenticated(cql)
            wait_for_scheduling_group_assignment(cql, user, sl)

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


    cql.execute(f"CREATE ROLE {user} WITH login = true AND password='{user}'")
    cql.execute(f"CREATE SERVICE LEVEL {sl1} WITH shares = 100")
    cql.execute(f"CREATE SERVICE LEVEL {sl2} WITH shares = 200")
    cql.execute(f"ATTACH SERVICE LEVEL {sl1} TO {user}")

    # Service level controller updates in 10 seconds interval, so wait
    # for sl1 to be assgined to test_user
    time.sleep(10)
    try:
        with new_session(cql, user): # new sessions is created only to create user's connection to Scylla
            wait_until_all_connections_authenticated(cql)
            wait_for_scheduling_group_assignment(cql, user, sl1)

            user_connections_sl1 = cql.execute(f"SELECT scheduling_group FROM system.clients WHERE username='{user}' ALLOW FILTERING")
            for conn in user_connections_sl1:
                assert conn[0] == f"sl:{sl1}"

            cql.execute(f"DETACH SERVICE LEVEL FROM {user}")
            cql.execute(f"ATTACH SERVICE LEVEL {sl2} TO {user}")
            # Again wait for service level controller to notice the change
            time.sleep(10)

            switch_tenants(cql)
            wait_for_scheduling_group_assignment(cql, user, sl2)

            user_connections_sl2 = cql.execute(f"SELECT scheduling_group FROM system.clients WHERE username='{user}' ALLOW FILTERING")
            print(count_opened_connections(cql))
            for conn in user_connections_sl2:
                assert conn[0] == f"sl:{sl2}"
    finally:
        cql.execute(f"DETACH SERVICE LEVEL FROM {user}")
        cql.execute(f"DROP ROLE {user}")
        cql.execute(f"DROP SERVICE LEVEL {sl1}")
        cql.execute(f"DROP SERVICE LEVEL {sl2}")


            


            
