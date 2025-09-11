# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

from contextlib import contextmanager, nullcontext
from functools import cache, wraps
from itertools import count

import pytest
from botocore.exceptions import ClientError

from test.alternator.util import random_string
from test.cqlpy.util import config_value_context
from test.alternator.test_metrics import metrics, get_metrics, get_metric

# This file contains tests for the Alternator expression caching.
# It tests caching for UpdateExpression, ConditionExpression and ProjectionExpression, based on the metrics: 
# - scylla_alternator_expression_cache_misses,
# - scylla_alternator_expression_cache_hits,
# - scylla_alternator_expression_cache_evictions.
# It assumes that cache works, when metrics report expected number of events.
#
# As the cache is per shard, the caching events for each request rely on the shard that processes them.
# E.g after the first request's 'miss', the repeated request would 'hit' only when it is processed by the same shard.
# However cache functionality is tested in unit test. Here mainly we want to confirm that requests actually use cache.
#
# There are basic tests, checking minimum certain numbers of events, that work even if the shard handling 
# the requests changes during the test.
#
# More detailed tests (checking exact numbers) strictly require that all requests are handled by the same shard.
# While in practice this is what happens (connection is reused), theoretically it is not guaranteed.
# To confirm that result is relyable, we check the number of operations reported by selected shard.
# If it did not get all requests, we repeat the tests.

# Context manager to temporarily change cache settings for the duration of the yield
# Note: It flushes the cache, by first setting it to 0, then to the desired size.
@contextmanager
def config_max_cache_entries_per_shard(cql, cache_size):
    with config_value_context(cql, 'alternator_max_expression_cache_entries_per_shard', '0'):
        with config_value_context(cql, 'alternator_max_expression_cache_entries_per_shard', str(cache_size)):
            yield

# Checks if actual cache metric increase (misses, hits, evictions) during the scope of this context match given expected values.
# If misses, hits or evictions are None, they are not checked.
# Optionally it can also check increase in the number of operations - on error it raises `UnexpectedOperationCount`
# Labels are used to filter metrics, e.g. {'shard': 0, 'expression': 'UpdateExpression'}.
# Note: evictions and operations are filtered by shard label only (if given), as they are not per-expression.
# If labels are None, no filtering is done.
# If allow_higher_values is True, it checks that actual values are at least as high as expected.
@contextmanager
def cache_metrics_check(metrics, misses=None, hits=None, evictions=None, operations=None, labels=None, allow_higher_values=False):
    metric_defs = { 'misses': {'name': 'scylla_alternator_expression_cache_misses', 'labels': labels},
                    'hits': {'name': 'scylla_alternator_expression_cache_hits', 'labels': labels},
                    'evictions': {'name': 'scylla_alternator_expression_cache_evictions', 'labels': {'shard': labels['shard']} if labels and 'shard' in labels else None},
                    'operations': {'name': 'scylla_alternator_operation', 'labels': {'shard': labels['shard']} if labels and 'shard' in labels else None},
                    'shard_bounce_for_lwt': {'name': 'scylla_alternator_shard_bounce_for_lwt', 'labels': {'shard': labels['shard']} if labels and 'shard' in labels else None} }
    expected_metric_values = {'misses': misses, 'hits': hits, 'evictions': evictions, 'operations': operations, 'shard_bounce_for_lwt': 0}
    expected_metric_values = {name: expected_metric_values[name] for name in metric_defs if expected_metric_values[name] is not None}
    the_metrics = get_metrics(metrics + "?__help__=false&__name__=alternator_*") or "#"
    actual_metric_values = {metric: get_metric(metrics, metric_defs[metric]['name'], metric_defs[metric]['labels'], the_metrics) for metric in expected_metric_values}
    yield
    the_metrics = get_metrics(metrics + "?__help__=false&__name__=alternator_*") or "#"
    actual_metric_values = {metric: get_metric(metrics, metric_defs[metric]['name'], metric_defs[metric]['labels'], the_metrics) - actual_metric_values[metric] for metric in actual_metric_values}
    cmp = lambda a, e: a >= e if allow_higher_values else a == e
    if ("operations" in actual_metric_values):
        actual_metric_values["operations"] += actual_metric_values["shard_bounce_for_lwt"]
    actual_metric_values["shard_bounce_for_lwt"] = 0
    if ("operations" in actual_metric_values and not cmp(actual_metric_values["operations"], expected_metric_values["operations"])):
        raise UnexpectedOperationCount(
            f"{metric_defs['operations']['labels']} expected {expected_metric_values['operations']} operations, got {actual_metric_values['operations']}"
        )
    assert all(cmp(actual_metric_values[metric], expected_metric_values[metric]) for metric in expected_metric_values), \
        f'cache metrics: {actual_metric_values} changed not as expected: {expected_metric_values}'

class UnexpectedOperationCount(Exception):
        pass

# Helper function that sets a new `value` in the table, by calling `UpdateItem` with UpdateExpression.
# Success is confirmed by getting an item from table.
# To succeed, `expression_variable` must be a valid expression 'variable' name to be used in ExpressionAttributeValues.
# By changing `expression_variable` we get a different expression string, which is a cache key.
def update_with_value(table, key, expression_variable, value):
    table.update_item(Key={ 'p': key }, UpdateExpression=f'SET a = {expression_variable}', ExpressionAttributeValues={f'{expression_variable}': value})
    assert table.get_item(Key={ 'p': key }, ConsistentRead=True)['Item']['a'] == value

# Helper function similar to `update_with_value`, but adding ConditionalExpression that check if new value is not equal to old one.
# `update_should_succeed` indicates whether condition should be met (new value is set) or not (new value was equal to old one) and a exception is expected.
def update_with_value_if_different(table, key, expression_variable, value, update_should_succeed):
    with nullcontext() if update_should_succeed else pytest.raises(ClientError, match='ConditionalCheckFailedException'):
        table.update_item(Key={ 'p': key }, UpdateExpression=f'SET a = {expression_variable}', ExpressionAttributeValues={f'{expression_variable}': value},
                           ConditionExpression=f'a <> {expression_variable}')
    assert table.get_item(Key={ 'p': key }, ConsistentRead=True)['Item']['a'] == value

# Helper function to get the number of shards from metrics.
# It looks for the 'shard' label in the metrics and returns the maximum shard number found.
# If no shards are found, it returns 2 by default (as tests are run with "--smp 2" by default).
@cache
def scylla_shard_count(metrics):
    import re
    matches = re.findall(r',shard="(\d+)"', get_metrics(metrics))
    assert matches, "No per-shard metrics found - could not determine shards count"
    return 1+max(int(n) for n in matches)

# Basic test to check that the expressions are cached at all. It uses only UpdateExpression.
# It does not depend on the shard that handles each request - we measure sums of events from all shards
# and repeat requests to reach minimal number of events, even if each request is handled by a different shard.
def test_caching_basic_workflow(cql, metrics, test_table_s):
    with config_max_cache_entries_per_shard(cql, 2):
        p = random_string()
        shards_count = scylla_shard_count(metrics)
        counter = count(1)

        with cache_metrics_check(metrics, evictions=0):
            # All cache clean - must be a miss, no evictions
            with cache_metrics_check(metrics, misses=1, allow_higher_values=True):
                update_with_value(table=test_table_s, key=p, expression_variable=':val1', value=next(counter))

            # Repeat 'shards_count' times - at least one hit, no evictions
            with cache_metrics_check(metrics, hits=1, allow_higher_values=True):
                for _ in range(0, shards_count):
                    update_with_value(table=test_table_s, key=p, expression_variable=':val1', value=next(counter))

            # New pattern - must be a miss, no evictions
            with cache_metrics_check(metrics, misses=1, allow_higher_values=True):
                update_with_value(table=test_table_s, key=p, expression_variable=':val2', value=next(counter))

            # Repeat 'shards_count' times - at least one hit, no evictions
            with cache_metrics_check(metrics, hits=1, allow_higher_values=True):
                for _ in range(0, shards_count):
                    update_with_value(table=test_table_s, key=p, expression_variable=':val2', value=next(counter))

# Basic test that cache has max size and entries are ever evicted.
# It does not depend on the shard that handles each request - we measure sums of events from all shards
# and repeat requests to reach minimal number of events, even if each request is handled by a different shard.
def test_caching_is_ever_evicted(cql, metrics, test_table_s):
    cache_size = 2
    with config_max_cache_entries_per_shard(cql, cache_size):
        p = random_string()
        shards_count = scylla_shard_count(metrics)
        counter = count(1)
        patterns = cache_size*shards_count + 1
        with cache_metrics_check(metrics, misses=patterns, evictions=1, allow_higher_values=True):
            for i in range(0, patterns):
                update_with_value(table=test_table_s, key=p, expression_variable=f':val{i}', value=next(counter))

# Basic test that all expressions in all requests are cached.
# It does not depend on the shard that handles each request - we measure sums of events from all shards.
# For each case we have a new expression and repeat requests 'shards_count' times to ensure at least one miss and one hit.
def test_caching_all_expressions(cql, metrics, test_table_s):
    with config_max_cache_entries_per_shard(cql, 200):
        p = random_string()
        shards_count = scylla_shard_count(metrics)
        counter = count(1)

        # UpdateExpression
        with cache_metrics_check(metrics, hits=1, misses=1, labels={'expression': "UpdateExpression"}, allow_higher_values=True):
            for _ in range(0, shards_count+1):
                update_with_value(table=test_table_s, key=p, expression_variable=':val_UE', value=next(counter))

        # ConditionExpression in UpdateItem
        with cache_metrics_check(metrics, hits=1, misses=1, labels={'expression': "ConditionExpression"}, allow_higher_values=True):
            for _ in range(0, shards_count+1):
                update_with_value_if_different(table=test_table_s, key=p, expression_variable=':val_CE_UI', value=next(counter), update_should_succeed=True)

        # ConditionExpression in PutItem
        with cache_metrics_check(metrics, hits=1, misses=1, labels={'expression': "ConditionExpression"}, allow_higher_values=True):
            for _ in range(0, shards_count+1):
                v = next(counter)
                test_table_s.put_item(Item={'p': p, 'a': v}, ConditionExpression=f'a <> :val_CE_PI', ExpressionAttributeValues={':val_CE_PI': v})
                assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == v

        # ConditionExpression in DeleteItem
        with cache_metrics_check(metrics, hits=1, misses=1, labels={'expression': "ConditionExpression"}, allow_higher_values=True):
            for _ in range(0, shards_count+1):
                test_table_s.delete_item(Key={'p': p}, ConditionExpression='a <> :val_CE_DI', ExpressionAttributeValues={':val_CE_DI': next(counter)})
                assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)

        v = next(counter)
        update_with_value(table=test_table_s, key=p, expression_variable=':val_UE', value=v)

        # ProjectionExpression in GetItem
        with cache_metrics_check(metrics, hits=1, misses=1, labels={'expression': "ProjectionExpression"}, allow_higher_values=True):
            for _ in range(0, shards_count+1):
                assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a')['Item']['a'] == v

        # ProjectionExpression in Scan
        with cache_metrics_check(metrics, hits=1, misses=1, labels={'expression': "ProjectionExpression"}, allow_higher_values=True):
            for _ in range(0, shards_count+1):
                assert [{'a': v}] == [{'a': x['a']} for x in test_table_s.scan(ConsistentRead=True, ProjectionExpression="p, a")['Items'] if x['p'] == p]

        # ProjectionExpression in BatchGetItem
        with cache_metrics_check(metrics, hits=1, misses=1, labels={'expression': "ProjectionExpression"}, allow_higher_values=True):
            args = { 'Keys': [{'p': p}], 'ProjectionExpression': '#a_BGI', 'ExpressionAttributeNames': {'#a_BGI': 'a'}, 'ConsistentRead': True }
            for _ in range(0, shards_count+1):
                assert [{'a': v}] == test_table_s.meta.client.batch_get_item(RequestItems = {test_table_s.name: args})['Responses'][test_table_s.name]

        # ProjectionExpression in Query
        with cache_metrics_check(metrics, hits=1, misses=1, labels={'expression': "ProjectionExpression"}, allow_higher_values=True):
            for _ in range(0, shards_count+1):
                assert [{'a': v}] == test_table_s.query(ConsistentRead=True, ProjectionExpression='#a_Q1', ExpressionAttributeNames={'#a_Q1': 'a'},
                                            KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})['Items']

        # KeyConditionExpression and FilterExpression in Query
        with cache_metrics_check(metrics, hits=1, misses=1, labels={'expression': "ConditionExpression"}, allow_higher_values=True):
            for _ in range(0, shards_count+1):
                assert [{'a': v}] == test_table_s.query(ConsistentRead=True, ExpressionAttributeValues={':p': p, ':a': v}, ExpressionAttributeNames={'#a': 'a'},
                                            ProjectionExpression="#a", KeyConditionExpression='p=:p', FilterExpression='a=:a')['Items']

def get_connection_shard(metrics, table):
    metric = metrics + "?__help__=false&__name__=alternator_operation"
    def get_op_count(metrics, the_metrics, shard):
        return get_metric(metrics, 'scylla_alternator_operation', {'shard': shard}, the_metrics)
    the_metrics = get_metrics(metric)
    saved_metrics = { x: get_op_count(metrics, the_metrics, x) for x in range(scylla_shard_count(metrics)) }

    table.get_item(Key={'p': "key"}, ConsistentRead=True)

    the_metrics = get_metrics(metric)
    con_shard = [ shard for shard,value in saved_metrics.items() 
                    if get_op_count(metrics, the_metrics, shard) - value == 1 ]
    if len(con_shard) != 1:
        pytest.fail(f'Expected exactly one shard to handle the requests, but found {len(con_shard)}: {con_shard}')
    return con_shard[0]

def retrying_on_unexpected_operation_count(num_attempts=5):
    def decorator(test_func):
        @wraps(test_func)
        def wrapper(*args, **kwargs):
            last_exc = []
            for _ in range(1, num_attempts + 1):
                try:
                    return test_func(*args, **kwargs)
                except UnexpectedOperationCount as exc:
                    last_exc.append(exc)
            pytest.fail(f"Unexpected number of operations reached observed shard. All attempts failed: {last_exc}")
        return wrapper
    return decorator

# Tests that UpdateExpression patterns are cached correctly - 
# the first time we see an expression, it is a miss, then we have hits for the same pattern.
# With full cache we have evictions along with misses.
# It strictly requires that all requests are handled by the same shard, as it checks exact values.
@retrying_on_unexpected_operation_count()
def test_caching_update_expression(cql, metrics, test_table_s):
    shard = get_connection_shard(metrics, test_table_s)
    with config_max_cache_entries_per_shard(cql, 2):
        p = random_string()
        labels = {'shard': shard, 'expression': "UpdateExpression"}
        counter = count(1)

        # Pattern 'A'
        with cache_metrics_check(metrics, hits=0, misses=1, evictions=0, operations=2, labels=labels):
            update_with_value(table=test_table_s, key=p, expression_variable=':valA', value=next(counter))
        with cache_metrics_check(metrics, hits=3, misses=0, evictions=0, operations=6, labels=labels):
            for i in range(3):
                update_with_value(table=test_table_s, key=p, expression_variable=':valA', value=next(counter))

        # Pattern 'B'
        with cache_metrics_check(metrics, hits=0, misses=1, evictions=0, operations=2, labels=labels):
            update_with_value(table=test_table_s, key=p, expression_variable=':valB', value=next(counter))
        with cache_metrics_check(metrics, hits=3, misses=0, evictions=0, operations=6, labels=labels):
            for i in range(3):
                update_with_value(table=test_table_s, key=p, expression_variable=':valB', value=next(counter))

        # Patterns 'A' and 'B' mixed
        with cache_metrics_check(metrics, hits=10, misses=0, evictions=0, operations=20, labels=labels):
            for i in range(5):
                update_with_value(table=test_table_s, key=p, expression_variable=':valA', value=next(counter))
                update_with_value(table=test_table_s, key=p, expression_variable=':valB', value=next(counter))

        # Patterns 0 - 9
        with cache_metrics_check(metrics, hits=0, misses=10, evictions=10, operations=20, labels=labels):
            for i in range(10):
                update_with_value(table=test_table_s, key=p, expression_variable=f':val{i}', value=next(counter))

# Tests that ConditionExpression patterns are cached correctly from UpdateItme, PutItem, DeleteItem.
# The first time we see an expression, it is a miss, then we have hits for the same pattern.
# With full cache we have evictions along with misses.
# It strictly requires that all requests are handled by the same shard, as it checks exact values.
@retrying_on_unexpected_operation_count()
def test_caching_condition_expression(cql, metrics, test_table_s):
    shard = get_connection_shard(metrics, test_table_s)
    with config_max_cache_entries_per_shard(cql, 2):
        p = random_string()
        labels = {'shard': shard, 'expression': "ConditionExpression"}
        counter = count(1)

        # UpdateItem - it uses also UpdateExpression, so there are more evictions.
        # Pattern 'A'
        with cache_metrics_check(metrics, hits=0, misses=1, evictions=0, operations=2, labels=labels):
            update_with_value_if_different(table=test_table_s, key=p, expression_variable=':valA', value=next(counter), update_should_succeed=True)
        with cache_metrics_check(metrics, hits=6, misses=0, evictions=0, operations=12, labels=labels):
            for _ in range(3):
                v = next(counter)
                update_with_value_if_different(table=test_table_s, key=p, expression_variable=':valA', value=v, update_should_succeed=True)
                update_with_value_if_different(table=test_table_s, key=p, expression_variable=':valA', value=v, update_should_succeed=False)

        # Pattern 'B'
        with cache_metrics_check(metrics, hits=0, misses=1, evictions=2, operations=2, labels=labels):
            update_with_value_if_different(table=test_table_s, key=p, expression_variable=':valB', value=next(counter), update_should_succeed=True)
        with cache_metrics_check(metrics, hits=6, misses=0, evictions=0, operations=12, labels=labels):
            for i in range(3):
                v = next(counter)
                update_with_value_if_different(table=test_table_s, key=p, expression_variable=':valB', value=v, update_should_succeed=True)
                update_with_value_if_different(table=test_table_s, key=p, expression_variable=':valB', value=v, update_should_succeed=False)

        # Patterns 'A' and 'B' mixed
        with cache_metrics_check(metrics, hits=0, misses=6, evictions=12, operations=12, labels=labels):
            for i in range(3):
                update_with_value_if_different(table=test_table_s, key=p, expression_variable=':valA', value=next(counter), update_should_succeed=True)
                update_with_value_if_different(table=test_table_s, key=p, expression_variable=':valB', value=next(counter), update_should_succeed=True)

        # PutItem/DeleteItem
        with cache_metrics_check(metrics, hits=10, misses=2, evictions=2, operations=21, labels=labels):
            for _ in range(3):
                v = next(counter)
                # put_item expression_pattern='C'
                test_table_s.put_item(Item={'p': p, 'a': v}, ConditionExpression=f'a <> :valC', ExpressionAttributeValues={':valC': v})
                assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == v
                # put_item expression_pattern='D'
                with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
                    test_table_s.put_item(Item={'p': p, 'a': v}, ConditionExpression=f'a <> :valD', ExpressionAttributeValues={':valD': v})
                assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True)['Item']['a'] == v

                # delete_item expression_pattern='C'
                with pytest.raises(ClientError, match='ConditionalCheckFailedException'):
                    test_table_s.delete_item(Key={'p': p}, ConditionExpression='a <> :valC', ExpressionAttributeValues={':valC': v})
                # delete_item expression_pattern='D'
                test_table_s.delete_item(Key={'p': p}, ConditionExpression='a <> :valD', ExpressionAttributeValues={':valD': next(counter)})
                assert not 'Item' in test_table_s.get_item(Key={'p': p}, ConsistentRead=True)

# Tests that ProjectionExpression patterns are cached correctly from GetItem, Scan, Query, BatchGetItem.
# The first time we see an expression, it is a miss, then we have hits for the same pattern.
# With full cache we have evictions along with misses.
# It strictly requires that all requests are handled by the same shard, as it checks exact values.
@retrying_on_unexpected_operation_count()
def test_caching_projection_expression(cql, metrics, test_table_s):
    shard = get_connection_shard(metrics, test_table_s)
    with config_max_cache_entries_per_shard(cql, 2):
        p = random_string()
        labels = {'shard': shard, 'expression': "ProjectionExpression"}
        ret = {'a': 'hello'}

        # fill cache
        update_with_value(table=test_table_s, key=p, expression_variable=':valA', value="hi")
        update_with_value(table=test_table_s, key=p, expression_variable=':valB', value="hello")

        # GetItem
        with cache_metrics_check(metrics, hits=5, misses=1, evictions=1, operations=6, labels=labels):
            for _ in range(6):
                assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a')['Item'] == ret

        with cache_metrics_check(metrics, hits=5, misses=1, evictions=1, operations=6, labels=labels):
            for _ in range(6):
                assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='#name', ExpressionAttributeNames={'#name': 'a'})['Item'] == ret

        with cache_metrics_check(metrics, hits=6, misses=0, evictions=0, operations=6, labels=labels):
            for _ in range(3):
                assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='#name', ExpressionAttributeNames={'#name': 'a'})['Item'] == ret
                assert test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='a')['Item'] == ret

        # Scan/Query/BatchGetItem
        with cache_metrics_check(metrics, hits=8, misses=1, evictions=1, operations=9, labels=labels):
            for _ in range(3):
                assert [ret] == [ {'a': x['a']} for x in test_table_s.scan(ConsistentRead=True, ProjectionExpression="p, a")['Items'] if x['p'] == p]
                assert [ret] == test_table_s.query(ConsistentRead=True, ProjectionExpression="a", 
                                                   KeyConditions={'p': {'AttributeValueList': [p], 'ComparisonOperator': 'EQ'}})['Items']
                assert [ret] == test_table_s.meta.client.batch_get_item(RequestItems = {test_table_s.name: {
                                             'Keys': [{'p': p}], 'ProjectionExpression': 'a', 'ConsistentRead': True}})['Responses'][test_table_s.name]

# Test Query separately with all three possible expressions: KeyConditionExpression, FilterExpression and ProjectionExpression.
# It strictly requires that all requests are handled by the same shard, as it checks exact values.
@retrying_on_unexpected_operation_count()
def test_caching_query_expression(cql, metrics, test_table_s):
    shard = get_connection_shard(metrics, test_table_s)
    with config_max_cache_entries_per_shard(cql, 3):
        p = random_string()
        labels = {'shard': shard}
        ret = {'a': 'hello'}

        # fill cache
        update_with_value(table=test_table_s, key=p, expression_variable=':valA', value="hi")
        update_with_value(table=test_table_s, key=p, expression_variable=':valB', value="hello")

        # Query
        with cache_metrics_check(metrics, hits=6, misses=3, evictions=2, operations=3, labels=labels):
            for _ in range(3):
                assert [ret] == test_table_s.query(ConsistentRead=True, ExpressionAttributeValues={':p': p, ':a': 'hello'}, 
                                                   ProjectionExpression="a", KeyConditionExpression='p=:p', FilterExpression='a=:a')['Items']

# Test that validation errors will be reported correctly and will not affect cache.
# It strictly requires that all requests are handled by the same shard, as it checks exact values.
@retrying_on_unexpected_operation_count()
def test_caching_invalid_expression(cql, metrics, test_table_s):
    shard = get_connection_shard(metrics, test_table_s)
    with config_max_cache_entries_per_shard(cql, 2):
        p = random_string()
        labels = {'shard': shard}
        counter = count(1)

        # fill cache
        update_with_value(table=test_table_s, key=p, expression_variable=':valA', value=next(counter))
        update_with_value(table=test_table_s, key=p, expression_variable=':valB', value=next(counter))

        # Try invalid requests
        # We don't check 'misses' here as it is debatable if they should be counted.
        with cache_metrics_check(metrics, hits=0, evictions=0, operations=3, labels=labels):
            with pytest.raises(ClientError, match='ValidationException'): #UpdateExpression
                update_with_value(table=test_table_s, key=p, expression_variable=':val#@*', value=next(counter))
            with pytest.raises(ClientError, match='ValidationException'): #ConditionExpression
                v = next(counter)
                test_table_s.put_item(Item={'p': p, 'a': v}, ConditionExpression=f'a <> :val#@*', ExpressionAttributeValues={':val#@*': v})
            with pytest.raises(ClientError, match='ValidationException'): #ProjectionExpression
                test_table_s.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression='^@*')

        # Check that cache is still working
        with cache_metrics_check(metrics, hits=10, misses=0, evictions=0, operations=20, labels=labels):
            for _ in range(5):
                update_with_value(table=test_table_s, key=p, expression_variable=':valA', value=next(counter))
                update_with_value(table=test_table_s, key=p, expression_variable=':valB', value=next(counter))

# Test that each shard has its own cache, that is used for separate connections to the same shard,
# but not shared between connections to different shards.
# It strictly requires control over which shards processes each request.
@retrying_on_unexpected_operation_count()
def test_caching_on_shards(cql, metrics, test_table_s, new_dynamodb_session):
    p = random_string()
    update_with_value(table=test_table_s, key=p, expression_variable=':val', value="test")
    with config_max_cache_entries_per_shard(cql, 2):
        connection_tables1 = [None] * scylla_shard_count(metrics)
        connection_tables2 = [None] * scylla_shard_count(metrics)
        # Depending on the shard assignment policy in the system,
        # in case we get a shard we don't want to, if we drop it immediately,
        # we may get it again in the next try. Keeping it should increase chances of getting all shards.
        to_drop = []
        connection_tables1[get_connection_shard(metrics, test_table_s)] = test_table_s
        setup_retries = 100*len(connection_tables1)
        while (None in connection_tables1 or None in connection_tables2):
            setup_retries -= 1
            if setup_retries == 0:
                print(connection_tables1)
                print(connection_tables2)
                pytest.fail("Failed to setup connections to all shards.")
            t = new_dynamodb_session().Table(test_table_s.name)
            s = get_connection_shard(metrics, t)
            print(s)
            if connection_tables1[s] is None:
                connection_tables1[s] = t
            elif connection_tables2[s] is None:
                connection_tables2[s] = t
            else:
                to_drop.append(t)

        def try_projection_expression(table, expression_variable):
            assert {'a': "test"} == table.get_item(Key={'p': p}, ConsistentRead=True, ProjectionExpression=expression_variable,
                                                   ExpressionAttributeNames={expression_variable: 'a'})['Item']

        for shard, table in enumerate(connection_tables1):
            labels = {'shard': shard}
            with cache_metrics_check(metrics, hits=18, misses=2, evictions=0, operations=20, labels=labels):
                for _ in range(5):
                    try_projection_expression(table, '#valA')
                    try_projection_expression(table, f'#val{shard}')
                    try_projection_expression(connection_tables2[shard], '#valA')
                    try_projection_expression(connection_tables2[shard], f'#val{shard}')
