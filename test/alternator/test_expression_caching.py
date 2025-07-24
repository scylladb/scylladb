from contextlib import contextmanager
from functools import cache
from itertools import count

import pytest
from botocore.exceptions import ClientError

from test.alternator.util import random_string, full_scan, full_query
from test.alternator.test_metrics import metrics, get_metrics, get_metric

@cache
def alternator_max_expression_cache_entries(cql):
    return int(list(cql.execute("SELECT value FROM system.config WHERE name = 'alternator_max_expression_cache_entries'"))[0][0])

@contextmanager
def cache_settings(cql, cache_size):
    initial_cache_size = alternator_max_expression_cache_entries(cql)
    cql.execute(f"UPDATE system.config SET value='0' WHERE name='alternator_max_expression_cache_entries';")
    cql.execute(f"UPDATE system.config SET value='{cache_size}' WHERE name='alternator_max_expression_cache_entries';")
    # TODO: Ensure that the cache size is reset to the desired value - get some metric showing current status
    # for now sleep
    import time
    time.sleep(0.2)
    try:
        yield
    finally:
        cql.execute(f"UPDATE system.config SET value='{initial_cache_size}' WHERE name='alternator_max_expression_cache_entries';")

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

@cache
def scylla_shard_count(metrics):
    import re
    matches = re.findall(r',shard="(\d+)"', get_metrics(metrics))
    if not matches:
        return 2 # by default tests are run with "--smp 2"
    return 1+max(int(n) for n in matches)

# Test that repeated UpdateExpression is cached.
@pytest.mark.xfail(reason="Not implemented yet")
def test_caching_update_expression(cql, metrics, test_table_s):
    p = random_string()
    labels = {'shard': 0, 'expression': "UpdateExpression"}
    counter = count(1)
    with cache_settings(cql, 2):
        # Pattern 'A'
        with cache_metrics_check(metrics, hits=9, misses=1, evictions=0, labels=labels):
            for i in range(10):
                set_a_value(test_table_s, p, next(counter), pattern='A')

        # Pattern 'B'
        with cache_metrics_check(metrics, hits=9, misses=1, evictions=0, labels=labels):
            for i in range(10):
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

# Test that repeated ConditionExpression is cached.
@pytest.mark.xfail(reason="Not implemented yet")
def test_caching_condition_expression(cql, metrics, test_table_s):
    p = random_string()
    labels = {'shard': 0, 'expression': "ConditionExpression"}
    counter = count(1)
    with cache_settings(cql, 2):
        # UpdateItem - it uses also UpdateExpression, so there are more evictions.
        # Pattern 'A'
        with cache_metrics_check(metrics, hits=5, misses=1, evictions=0, labels=labels):
            for i in range(3):
                v = next(counter)
                set_a_value(test_table_s, p, v, pattern='A', condition_fails=False)
                set_a_value(test_table_s, p, v, pattern='A', condition_fails=True)

        # Pattern 'B'
        with cache_metrics_check(metrics, hits=5, misses=1, evictions=2, labels=labels):
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

# Test that repeated ProjectionExpression is cached.
@pytest.mark.xfail(reason="Not implemented yet")
def test_caching_projection_expression(cql, metrics, test_table_s):
    p = random_string()
    labels = {'shard': 0, 'expression': "ProjectionExpression"}
    ret = {'a': 'hello'}
    with cache_settings(cql, 2):
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
@pytest.mark.xfail(reason="Not implemented yet")
def test_caching_query_expression(cql, metrics, test_table_s):
    p = random_string()
    labels = {'shard': 0}
    ret = {'a': 'hello'}
    with cache_settings(cql, 3):
        # fill cache
        set_a_value(test_table_s, p, "hi", pattern='A')
        set_a_value(test_table_s, p, "hello", pattern='B')

        # Query
        with cache_metrics_check(metrics, hits=6, misses=3, evictions=2, labels=labels):
            for i in range(3):
                assert full_query(test_table_s, ProjectionExpression="a", KeyConditionExpression='p=:p', FilterExpression='a=:a', ExpressionAttributeValues={':p': p, ':a': 'hello'}) == [ret]

# Test that validation errors will be reported correctly and will not affect cache.
@pytest.mark.xfail(reason="Not implemented yet")
def test_caching_invalid_expression(cql, metrics, test_table_s):
    p = random_string()
    labels = {'shard': 0}
    counter = count(1)
    with cache_settings(cql, 2):
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
