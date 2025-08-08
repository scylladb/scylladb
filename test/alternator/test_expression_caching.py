# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

from contextlib import contextmanager
from functools import cache
from itertools import count

import pytest
from botocore.exceptions import ClientError
import types

from test.alternator.util import random_string, full_scan, full_query
from test.alternator.test_metrics import metrics, get_metrics, get_metric

# This file contains tests for the Alternator expression caching.
# It tests caching for UpdateExpression, ConditionExpression and ProjectionExpression, based on the metrics: 
# - scylla_alternator_expression_cache_misses,
# - scylla_alternator_expression_cache_hits,
# - scylla_alternator_expression_cache_evictions.
# It assumes that cache works, when metrics report expected number of events.
#
# As the cache is per shard, the caching events for each request rely on the shard that processes them.
# There are basic tests, checking minimal values, that work even if the shard handling the requests changes during the test.
# The rest (checking exact values) strictly requires that all requests are handled by the same shard.
# While in practice this is what happens (connection is reused), theoretically it is not guaranteed.
# Optimally, the tests should be run with "--smp 1" or use shard-aware port (when it is implemented).
# For now we monitor that new connections are not created during the test, so the shard is not changed.
# If it happens, the test will be marked as xfail.
# We also need to know which shard was used (in case of LWT requests can be bounced and parsed again).
# Again in practice it is shard 0, theoretically it can be different and shard-aware port would solve it.
# But we can detect it from the metrics.


# Helper function that gets the setting for alternator_max_expression_cache_entries.
# This is cached as we use it only for getting the initial value.
@cache
def alternator_max_expression_cache_entries(cql):
    return int(list(cql.execute("SELECT value FROM system.config WHERE name = 'alternator_max_expression_cache_entries_per_shard'"))[0][0])

# Context manager to temporarily change cache settings
# Note: It flushes the cache, by first setting it to 0, then to the desired size.
@contextmanager
def cache_settings(cql, cache_size):
    initial_cache_size = alternator_max_expression_cache_entries(cql)
    cql.execute(f"UPDATE system.config SET value='0' WHERE name='alternator_max_expression_cache_entries_per_shard';")
    cql.execute(f"UPDATE system.config SET value='{cache_size}' WHERE name='alternator_max_expression_cache_entries_per_shard';")
    # TODO: Ensure that the cache size is reset to the desired value - maybe get some metric showing current status
    # for now sleep
    import time
    time.sleep(0.2)
    try:
        yield
    finally:
        cql.execute(f"UPDATE system.config SET value='{initial_cache_size}' WHERE name='alternator_max_expression_cache_entries_per_shard';")

# Checks if actual cache metric increase (misses, hits, evictions) during the scope of this context match given expected values.
# If misses, hits or evictions are None, they are not checked.
# Labels are used to filter metrics, e.g. {'shard': 0, 'expression': 'UpdateExpression'}.
# Note: evictions are filtered by shard label only (if given), as they are not per-expression.
# If labels are None, no filtering is done.
# If minimal is True, it checks that actual values are at least as high as expected.
@contextmanager
def cache_metrics_check(metrics, misses=None, hits=None, evictions=None, labels=None, minimal=False):
    metric_defs = { 'misses': {'name': 'scylla_alternator_expression_cache_misses', 'labels': labels},
                    'hits': {'name': 'scylla_alternator_expression_cache_hits', 'labels': labels},
                    'evictions': {'name': 'scylla_alternator_expression_cache_evictions', 'labels': {'shard': labels['shard']} if labels and 'shard' in labels else None} }
    expected_metric_values = {'misses': misses, 'hits': hits, 'evictions': evictions}
    expected_metric_values = {name: expected_metric_values[name] for name in metric_defs if expected_metric_values[name] is not None}
    the_metrics = get_metrics(metrics)
    actual_metric_values = {metric: get_metric(metrics, metric_defs[metric]['name'], metric_defs[metric]['labels'], the_metrics) for metric in expected_metric_values}
    yield
    the_metrics = get_metrics(metrics)
    actual_metric_values = {metric: get_metric(metrics, metric_defs[metric]['name'], metric_defs[metric]['labels'], the_metrics) - actual_metric_values[metric] for metric in actual_metric_values}
    cmp = lambda a, e: a >= e if minimal else a == e
    assert all(cmp(actual_metric_values[metric], expected_metric_values[metric]) for metric in expected_metric_values), \
        f'cache metrics: {actual_metric_values} changed not as expected: {expected_metric_values}'

# Helper function to change a value in the table using UpdateExpression and/or ConditionExpression.
# Operation can be "put_item", "update_item" or "delete_item"
# If operation is "put_item", it sets the value directly.
# If operation is "update_item", it uses UpdateExpression to set the value.
# If operation is "delete_item", it deletes the item.
# If condition_fails is True, it uses ConditionExpression to ensure the value is not equal to the given value, which will fail if the value is already set.
# If condition_fails is False, it uses ConditionExpression to ensure the value is equal to the given value, which will succeed if the value is already set.
# If condition_fails is None, no ConditionExpression is used.
# The function asserts that the value is set correctly in the table after the operation.
def set_a_value(table, key, value, pattern='', operation="update_item", condition_fails=None):
    params = { }
    if operation == "put_item":
        params['Item'] = {'p': key, 'a': value}
    else:
        params['Key'] = {'p': key}
    if operation == "update_item":
        params['UpdateExpression'] = f'SET a = :val{pattern}'
    if condition_fails is not None:
        params['ConditionExpression'] = f'a <> :val{pattern}'
    if "UpdateExpression" in params or "ConditionExpression" in params:
        params['ExpressionAttributeValues'] = {f':val{pattern}': value}

    if condition_fails:
        with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
            getattr(table, operation)(**params)
    else:
        getattr(table, operation)(**params)
    
    if operation == "delete_item" and not condition_fails:
        assert not 'Item' in table.get_item(Key={'p': key}, ConsistentRead=True)
    else:
        assert table.get_item(Key={'p': key}, ConsistentRead=True)['Item']['a'] == value

# Helper function to get the number of shards from metrics.
# It looks for the 'shard' label in the metrics and returns the maximum shard number found.
# If no shards are found, it returns 2 by default (as tests are run with "--smp 2" by default).
@cache
def scylla_shard_count(metrics):
    import re
    matches = re.findall(r',shard="(\d+)"', get_metrics(metrics))
    if not matches:
        return 2 # by default tests are run with "--smp 2"
    return 1+max(int(n) for n in matches)

# Basic test to check that the expressions are cached at all. It uses only UpdateExpression.
# It does not depend on the shard that handles each requests - requests are repeated and minimal values are checked,
# so it will work even if the shards handling them would be changed during the test.
def test_caching_basic_workflow(cql, metrics, test_table_s):
    with cache_settings(cql, 2):
        p = random_string()
        shards_count = scylla_shard_count(metrics)
        counter = count(1)

        # All cache clean - must be a miss, no evictions
        with cache_metrics_check(metrics, misses=1, evictions=0, minimal=True):
            set_a_value(test_table_s, p, next(counter), pattern='1')

        # Repeat 'shards_count' times - at least one hit, no evictions
        with cache_metrics_check(metrics, hits=1, evictions=0, minimal=True):
            for i in range(0, shards_count):
                set_a_value(test_table_s, p, next(counter), pattern='1')

        # New pattern - must be a miss, no evictions
        with cache_metrics_check(metrics, misses=1, evictions=0, minimal=True):
            set_a_value(test_table_s, p, next(counter), pattern='2')

        # Repeat 'shards_count' times - at least one hit, no evictions
        with cache_metrics_check(metrics, hits=1, evictions=0, minimal=True):
            for i in range(0, shards_count):
                set_a_value(test_table_s, p, next(counter), pattern='2')

# Basic test that cache has max size and entries are ever evicted.
# It does not depend on the shard that handles each requests - requests are repeated and minimal values are checked,
# so it will work even if the shards handling them would be changed during the test.
def test_caching_is_ever_evicted(cql, metrics, test_table_s):
    cache_size = 2
    with cache_settings(cql, cache_size):
        p = random_string()
        shards_count = scylla_shard_count(metrics)
        counter = count(1)
        patterns = cache_size*shards_count + 1
        with cache_metrics_check(metrics, misses=patterns, evictions=1, minimal=True):
            for i in range(0, patterns):
                set_a_value(test_table_s, p, next(counter), pattern=i)

# Context manager to ensure that all requests are handled by the same shard.
# It yields the shard number of the existing connection determined by metrics of GetItem operation.
# It patches the connection pool to check if the same connection (and consequently shard) is used
# - if not, the test will be marked as xfail.
# Note: This is a workaround to ensure that the tests are run with the same shard.
# Ideally, the tests should be run with "--smp 1" or use a shard-aware port (when it is implemented).
@contextmanager
def sticky_shard_connection(metrics, table, monkeypatch):
    endpoint = table.meta.client._endpoint
    proxy_url = endpoint.http_session._proxy_config.proxy_url_for(endpoint.host)
    manager = endpoint.http_session._get_connection_manager(endpoint.host, proxy_url)
    conn_pool = manager.connection_from_url(endpoint.host)
    original_get_conn = type(conn_pool)._get_conn
    sticky_conn = None
    def patch_get_conn(self, *args, **kwargs):
        conn = original_get_conn(self, *args, **kwargs)
        nonlocal sticky_conn
        if sticky_conn is None:
            sticky_conn = conn
        elif sticky_conn is not conn:
            pytest.xfail('If connections are not reused this test may become flaky, as it requires that all requests are handled by the same shard.')
        return sticky_conn
    monkeypatch.setattr(conn_pool, '_get_conn', types.MethodType(patch_get_conn, conn_pool))

    def get_op_count(metrics, the_metrics, shard):
        return get_metric(metrics, 'scylla_alternator_operation', {'op': "GetItem", 'shard': shard}, the_metrics)
    the_metrics = get_metrics(metrics)
    saved_metrics = { x: get_op_count(metrics, the_metrics, x) for x in range(scylla_shard_count(metrics)) }
    table.get_item(Key={'p': "key"}, ConsistentRead=True)
    # To test that it xfails if new connection is created, we need to close the existing one.
    #while (conn := conn_pool.pool.get(block=False)):
    #    conn.close()
    the_metrics = get_metrics(metrics)
    con_shard = [ shard for shard,value in saved_metrics.items() if get_op_count(metrics, the_metrics, shard) - value == 1 ]
    if len(con_shard) != 1:
        pytest.xfail(f'Expected exactly one shard to handle the requests, but found {len(con_shard)}: {con_shard}')
    yield con_shard[0]

# Tests that UpdateExpression patterns are cached correctly - 
# the first time we see an expression, it is a miss, then we have hits for the same pattern.
# With full cache we have evictions along with misses.
# It strictly requires that all requests are handled by the same shard, as it checks exact values.
def test_caching_update_expression(cql, metrics, test_table_s, monkeypatch):
    with cache_settings(cql, 2), sticky_shard_connection(metrics, test_table_s, monkeypatch) as shard:
        p = random_string()
        labels = {'shard': shard, 'expression': "UpdateExpression"}
        counter = count(1)

        # Pattern 'A'
        with cache_metrics_check(metrics, hits=0, misses=1, evictions=0, labels=labels):
            set_a_value(test_table_s, p, next(counter), pattern='A')
        with cache_metrics_check(metrics, hits=3, misses=0, evictions=0, labels=labels):
            for i in range(3):
                set_a_value(test_table_s, p, next(counter), pattern='A')

        # Pattern 'B'
        with cache_metrics_check(metrics, hits=0, misses=1, evictions=0, labels=labels):
            set_a_value(test_table_s, p, next(counter), pattern='B')
        with cache_metrics_check(metrics, hits=3, misses=0, evictions=0, labels=labels):
            for i in range(3):
                set_a_value(test_table_s, p, next(counter), pattern='B')

        # Patterns 'A' and 'B' mixed
        with cache_metrics_check(metrics, hits=10, misses=0, evictions=0, labels=labels):
            for i in range(5):
                set_a_value(test_table_s, p, next(counter), pattern='A')
                set_a_value(test_table_s, p, next(counter), pattern='B')

        # Patterns 0 - 9
        with cache_metrics_check(metrics, hits=0, misses=10, evictions=10, labels=labels):
            for i in range(10):
                set_a_value(test_table_s, p, next(counter), pattern=i)

# Tests that ConditionExpression patterns are cached correctly -
# the first time we see an expression, it is a miss, then we have hits for the same pattern.
# With full cache we have evictions along with misses.
# It strictly requires that all requests are handled by the same shard, as it checks exact values.
def test_caching_condition_expression(cql, metrics, test_table_s, monkeypatch):
    with cache_settings(cql, 2), sticky_shard_connection(metrics, test_table_s, monkeypatch) as shard:
        p = random_string()
        labels = {'shard': shard, 'expression': "ConditionExpression"}
        counter = count(1)

        # UpdateItem - it uses also UpdateExpression, so there are more evictions.
        # Pattern 'A'
        with cache_metrics_check(metrics, hits=0, misses=1, evictions=0, labels=labels):
            set_a_value(test_table_s, p, next(counter), pattern='A', condition_fails=False)
        with cache_metrics_check(metrics, hits=6, misses=0, evictions=0, labels=labels):
            for i in range(3):
                v = next(counter)
                set_a_value(test_table_s, p, v, pattern='A', condition_fails=False)
                set_a_value(test_table_s, p, v, pattern='A', condition_fails=True)

        # Pattern 'B'
        with cache_metrics_check(metrics, hits=0, misses=1, evictions=2, labels=labels):
            set_a_value(test_table_s, p, next(counter), pattern='B', condition_fails=False)
        with cache_metrics_check(metrics, hits=6, misses=0, evictions=0, labels=labels):
            for i in range(3):
                v = next(counter)
                set_a_value(test_table_s, p, v, pattern='B', condition_fails=False)
                set_a_value(test_table_s, p, v, pattern='B', condition_fails=True)

        # Patterns 'A' and 'B' mixed
        with cache_metrics_check(metrics, hits=0, misses=6, evictions=12, labels=labels):
            for i in range(3):
                set_a_value(test_table_s, p, next(counter), pattern='A', condition_fails=False)
                set_a_value(test_table_s, p, next(counter), pattern='B', condition_fails=False)

        # PutItem/DeleteItem
        with cache_metrics_check(metrics, hits=10, misses=2, evictions=2, labels=labels):
            for i in range(3):
                v = next(counter)
                set_a_value(test_table_s, p, v, pattern='C', operation="put_item", condition_fails=False)
                set_a_value(test_table_s, p, v, pattern='D', operation="put_item", condition_fails=True)
                set_a_value(test_table_s, p, v, pattern='C', operation="delete_item", condition_fails=True)
                set_a_value(test_table_s, p, next(counter), pattern='D', operation="delete_item", condition_fails=False)

# Tests that ProjectionExpression patterns are cached correctly -
# the first time we see an expression, it is a miss, then we have hits for the same pattern.
# With full cache we have evictions along with misses.
# It strictly requires that all requests are handled by the same shard, as it checks exact values.
def test_caching_projection_expression(cql, metrics, test_table_s, monkeypatch):
    with cache_settings(cql, 2), sticky_shard_connection(metrics, test_table_s, monkeypatch) as shard:
        p = random_string()
        labels = {'shard': shard, 'expression': "ProjectionExpression"}
        ret = {'a': 'hello'}

        # fill cache
        set_a_value(test_table_s, p, "hi", pattern='A')
        set_a_value(test_table_s, p, "hello", pattern='B')

        # GetItem
        with cache_metrics_check(metrics, hits=5, misses=1, evictions=1, labels=labels):
            for i in range(6):
                assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a')['Item'] == ret

        with cache_metrics_check(metrics, hits=5, misses=1, evictions=1, labels=labels):
            for i in range(6):
                assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='#name', ExpressionAttributeNames={'#name': 'a'})['Item'] == ret

        with cache_metrics_check(metrics, hits=6, misses=0, evictions=0, labels=labels):
            for i in range(3):
                assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='#name', ExpressionAttributeNames={'#name': 'a'})['Item'] == ret
                assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a')['Item'] == ret

        # Scan/Query/BatchGetItem
        with cache_metrics_check(metrics, hits=8, misses=1, evictions=1, labels=labels):
            for i in range(3):
                assert [ {'a': x['a']} for x in full_scan(test_table_s, ProjectionExpression="p, a") if x['p'] == p] == [ret]
                assert full_query(test_table_s, ProjectionExpression="a", KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}}) == [ret]
                assert test_table_s.meta.client.batch_get_item(RequestItems = 
                            {test_table_s.name: { 'Keys': [{'p': p}], 'ProjectionExpression': 'a', 'ConsistentRead': True}})['Responses'][test_table_s.name] == [ret]

# Test Query separately with all three possible expressions: KeyConditionExpression, FilterExpression, and ProjectionExpression.
# It strictly requires that all requests are handled by the same shard, as it checks exact values.
def test_caching_query_expression(cql, metrics, test_table_s, monkeypatch):
    with cache_settings(cql, 3), sticky_shard_connection(metrics, test_table_s, monkeypatch) as shard:
        p = random_string()
        labels = {'shard': shard}
        ret = {'a': 'hello'}

        # fill cache
        set_a_value(test_table_s, p, "hi", pattern='A')
        set_a_value(test_table_s, p, "hello", pattern='B')

        # Query
        with cache_metrics_check(metrics, hits=6, misses=3, evictions=2, labels=labels):
            for i in range(3):
                assert full_query(test_table_s, ProjectionExpression="a", KeyConditionExpression='p=:p', FilterExpression='a=:a', ExpressionAttributeValues={':p': p, ':a': 'hello'}) == [ret]

# Test that validation errors will be reported correctly and will not affect cache.
# It strictly requires that all requests are handled by the same shard, as it checks exact values.
def test_caching_invalid_expression(cql, metrics, test_table_s, monkeypatch):
    with cache_settings(cql, 2), sticky_shard_connection(metrics, test_table_s, monkeypatch) as shard:
        p = random_string()
        labels = {'shard': shard}
        counter = count(1)

        # fill cache
        set_a_value(test_table_s, p, next(counter), pattern='A')
        set_a_value(test_table_s, p, next(counter), pattern='B')
        
        # Try invalid requests
        # We don't check 'misses' here as it is debatable if they should be counted.
        with cache_metrics_check(metrics, hits=0, evictions=0, labels=labels):
            with pytest.raises(ClientError, match='ValidationException'): #UpdateExpression
                set_a_value(test_table_s, p, next(counter), pattern='#@*')
            with pytest.raises(ClientError, match='ValidationException'): #ConditionExpression
                set_a_value(test_table_s, p, next(counter), pattern='#@*', operation="put_item", condition_fails=False)
            with pytest.raises(ClientError, match='ValidationException'): #ProjectionExpression
                test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='^@*')

        # Check that cache is still working
        with cache_metrics_check(metrics, hits=10, misses=0, evictions=0, labels=labels):
            for i in range(5):
                set_a_value(test_table_s, p, next(counter), pattern='A')
                set_a_value(test_table_s, p, next(counter), pattern='B')
