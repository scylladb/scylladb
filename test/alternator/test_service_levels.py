# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import pytest
from test.alternator.util import random_string, is_aws
from test.alternator.conftest import new_dynamodb_session
from test.alternator.test_metrics import metrics, get_metrics, check_increases_metric
from contextlib import contextmanager
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, ConsistencyLevel
from cassandra.policies import RoundRobinPolicy
import time
import re

# Quote an identifier if it needs to be double-quoted in CQL. Quoting is
# *not* needed if the identifier matches [a-z][a-z0-9_]*, otherwise it does.
# double-quotes ('"') in the string are doubled.
def maybe_quote(identifier):
    if re.match('^[a-z][a-z0-9_]*$', identifier):
        return identifier
    return '"' + identifier.replace('"', '""') + '"'

# Convenience context manager for temporarily GRANTing some permission and
# then revoking it.
@contextmanager
def temporary_grant(cql, permission, resource, role):
    role = maybe_quote(role)
    cql.execute(f"GRANT {permission} ON {resource} TO {role}")
    try:
        yield
    finally:
        cql.execute(f"REVOKE {permission} ON {resource} FROM {role}")

# Convenience function for getting the full CQL table name (ksname.cfname)
# for the given Alternator table. This uses our insider knowledge that
# table named "x" is stored in keyspace called "alternator_x", and if we
# ever change this we'll need to change this function too.
def cql_table_name(tab):
    return maybe_quote('alternator_' + tab.name) + '.' + maybe_quote(tab.name)

# This file is all about testing RBAC as configured via CQL, so we need to
# connect to CQL to set these tests up. The "cql" fixture below enables that.
# If we're not testing Scylla, or the CQL port is not available on the same
# IP address as the Alternator IP address, a test using this fixture will
# be skipped with a message about the CQL API not being available.
@pytest.fixture(scope="module")
def cql(dynamodb):
    if is_aws(dynamodb):
        pytest.skip('Scylla-only CQL API not supported by AWS')
    url = dynamodb.meta.client._endpoint.host
    host, = re.search(r'.*://([^:]*):', url).groups()
    profile = ExecutionProfile(
        load_balancing_policy=RoundRobinPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL)
    cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        contact_points=[host],
        port=9042,
        protocol_version=4,
        auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'),
    )
    try:
        ret = cluster.connect()
        # "BEGIN BATCH APPLY BATCH" is the closest to do-nothing I could find
        ret.execute("BEGIN BATCH APPLY BATCH")
    except NoHostAvailable:
        pytest.skip('Could not connect to Scylla-only CQL API')
    yield ret
    cluster.shutdown()

def test_service_level_metrics(test_table, request, dynamodb, cql, metrics):
    print("Please make sure authorization is enforced in your Scylla installation: alternator_enforce_authorization: true")
    p = random_string()
    c = random_string()
    _ = get_metrics(metrics)
    # Use additional user created by test/alternator/run to execute write under sl_alternator service level.
    ses = new_dynamodb_session(request, dynamodb, user='alternator_custom_sl')
    # service_level_controler acts asynchronously in a loop so we can fail metric check
    # if it hasn't processed service level update yet. It can take as long as 10 seconds.
    started = time.time()
    timeout = 30
    while True:
        try:
            with temporary_grant(cql, 'MODIFY', cql_table_name(test_table), 'alternator_custom_sl'):
              with check_increases_metric(metrics,
                                        ['scylla_storage_proxy_coordinator_write_latency_count'],
                                        {'scheduling_group_name': 'sl:sl_alternator'}):
                ses.meta.client.put_item(TableName=test_table.name, Item={'p': p, 'c': c})
            break # no exception, test passed
        except:
            if time.time() - started > timeout:
                raise
            else:
                time.sleep(0.5) # retry
