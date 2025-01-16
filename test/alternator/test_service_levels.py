# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import pytest
from test.alternator.test_metrics import metrics, get_metrics, check_increases_metric
from contextlib import contextmanager
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, ConsistencyLevel
from cassandra.policies import RoundRobinPolicy
import time
import re

from .util import random_string, is_aws, unique_table_name
from .test_cql_rbac import new_role, new_dynamodb

# new_service_level() is a context manager for temporarily creating a new
# service level with a unique name and attaching it to the given role.
# The fixture returns the new service level's name.
@contextmanager
def new_service_level(cql, role):
    # The service level name is not a table's name but it doesn't matter.
    # Because our unique_table_name() uses (deliberately) a non-lower-case
    # character, the role name has to be quoted in double quotes when used
    # in CQL below.
    sl = unique_table_name()
    cql.execute(f'CREATE SERVICE LEVEL "{sl}"')
    cql.execute(f'ATTACH SERVICE LEVEL "{sl}" TO "{role}"')
    try:
        yield sl
    finally:
        cql.execute(f'DROP SERVICE LEVEL "{sl}"')

def test_service_level_metrics(test_table, request, dynamodb, cql, metrics):
    print("Please make sure authorization is enforced in your Scylla installation: alternator_enforce_authorization: true")
    p = random_string()
    c = random_string()
    _ = get_metrics(metrics)
    with new_role(cql, superuser=True) as (role, key):
        with new_service_level(cql, role) as sl:
            with new_dynamodb(dynamodb, role, key) as ses:
            # service_level_controler acts asynchronously in a loop so we can fail metric check
            # if it hasn't processed service level update yet. It can take as long as 10 seconds.
                started = time.time()
                timeout = 30
                while True:
                    try:
                        with check_increases_metric(metrics,
                                            ['scylla_storage_proxy_coordinator_write_latency_count'],
                                            {'scheduling_group_name': f'sl:{sl}'}):
                            ses.meta.client.put_item(TableName=test_table.name, Item={'p': p, 'c': c})
                        break # no exception, test passed
                    except:
                        if time.time() - started > timeout:
                            raise
                        else:
                            time.sleep(0.5) # retry
