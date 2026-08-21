# -*- coding: utf-8 -*-
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

########################################
# Tests for the service levels HTTP API.
########################################

import pytest
from cassandra import ConsistencyLevel
from cassandra.protocol import QueryMessage
from .rest_api import get_request, post_request
from .util import new_session, new_test_table, unique_name
import time

# All tests in this file check the Scylla-only service levels feature,
# so let's mark them all scylla_only with an autouse fixture:
@pytest.fixture(scope="function", autouse=True)
def all_tests_are_scylla_only(scylla_only):
    pass

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

def wait_for_clients(cql, username, clients_num, wait_s = 1, timeout_s = 30):
    start_time = time.time()
    while time.time() - start_time < timeout_s:
        result = cql.execute(f"SELECT COUNT(*) FROM system.clients WHERE username='{username}' ALLOW FILTERING")
        if result.one()[0] == clients_num:
            return
        else:
            time.sleep(wait_s)

    raise RuntimeError(f"Awaiting for {clients_num} clients timed out.")

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
# This function returns the set of shards whose per-shard connection of `user`
# runs under `target_scheduling_group`.
def shards_in_scheduling_group(cql, user, target_scheduling_group):
    shards_with_correct_sg = set()
    connections = cql.execute(f"SELECT username, scheduling_group, shard_id FROM system.clients WHERE client_type='cql' AND username='{user}' ALLOW FILTERING")

    for conn in connections:
        if target_scheduling_group in conn.scheduling_group:
            shards_with_correct_sg.add(conn.shard_id)

    return shards_with_correct_sg

# A connection starts in sl:driver and is reclassified to its user's service level
# only once it runs user (non-system) load. Send a user-table query on every
# per-shard connection of `session` so each of them gets reclassified.
def reclassify_user_connections(session, user_table):
    query = QueryMessage(f"SELECT * FROM {user_table}", ConsistencyLevel.ONE)
    for pool in list(session._pools.values()):
        for conn in list(pool._connections.values()):
            conn.wait_for_response(query)

# The scheduling-group switch takes effect only after the query that triggered the
# reclassification finishes, so system.clients may briefly still report sl:driver.
# Re-trigger reclassification and poll until all per-shard connections of `user`
# run under `target_scheduling_group`.
def wait_for_scheduling_group_assignment(cql, session, user, user_table, target_scheduling_group, shard_count, wait_s = 1, timeout_s = 30):
    start_time = time.time()
    shards_with_correct_sg = set()
    while time.time() - start_time < timeout_s:
        reclassify_user_connections(session, user_table)
        shards_with_correct_sg = shards_in_scheduling_group(cql, user, target_scheduling_group)
        if len(shards_with_correct_sg) == shard_count:
            return
        time.sleep(wait_s)

    raise RuntimeError(f"Not all user '{user}' connections are working under target scheduling group "
                       f"'{target_scheduling_group}'. Shards with correct scheduling group: {shards_with_correct_sg}, "
                       f"expected {shard_count} shards.")

# Test if `/service_levels/count_connections` prints counted CQL connections
# per scheduling group per user.
def test_count_opened_cql_connections(cql, test_keyspace):
    user = f"test_user_{unique_name()}"
    sl = f"sl_{unique_name()}"

    cql.execute(f"CREATE ROLE {user} WITH login = true AND password='{user}'")
    cql.execute(f"CREATE SERVICE LEVEL {sl} WITH shares = 100")
    cql.execute(f"ATTACH SERVICE LEVEL {sl} TO {user}")

    try:
        with new_test_table(cql, test_keyspace, "id int PRIMARY KEY") as user_table:
            cql.execute(f"GRANT SELECT ON {user_table} TO {user}")
            read_barrier(cql)

            with new_session(cql, user) as session:
                wait_for_clients(cql, user, 3) # 3 from smp=2 + control connection
                wait_until_all_connections_authenticated(cql)
                # The connections start in sl:driver and only move to sl:{sl} once
                # they run user load, so issue a user-table query on each of them.
                wait_for_scheduling_group_assignment(cql, session, user, user_table, sl, get_shard_count(cql))

                api_response = count_opened_connections(cql)
                assert f"sl:{sl}" in api_response
                assert user in api_response[f"sl:{sl}"]

                table_response = count_opened_connections_from_table(cql)
                assert api_response == table_response
    finally:
        cql.execute(f"DETACH SERVICE LEVEL FROM {user}")
        cql.execute(f"DROP ROLE {user}")
        cql.execute(f"DROP SERVICE LEVEL {sl}")
