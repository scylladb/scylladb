#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest
import redis
import logging
from util import random_string, connect
from packaging.version import Version

logger = logging.getLogger('redis-test')

def test_hset_hget_delete(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    field = random_string(10)
    val = random_string(10)
    other_val = random_string(10)

    with pytest.raises(redis.exceptions.ResponseError) as excinfo:
        r.execute_command("HSET testkey")
    assert "wrong number of arguments for 'hset' command" in str(excinfo.value)

    with pytest.raises(redis.exceptions.ResponseError) as excinfo:
        r.execute_command("HSET testkey testfield")
    assert "wrong number of arguments for 'hset' command" in str(excinfo.value)

    assert r.hset(key, field, val) == 1
    assert r.hget(key, field) == val

    # Check overwrite
    r.hset(key, field, other_val)
    assert r.hget(key, field) == other_val

    # Check delete
    assert r.delete(key) == 1
    assert r.hget(key, field) == None 

@pytest.mark.xfail(reason="HSET command does not support multiple key/field")
def test_hset_multiple_key_field(redis_host, redis_port):
    # This test requires the library to support multiple mappings in one
    # command, or we cannot test this feature. This was added to redis-py
    # in version 3.5.0, in April 29, 2020.
    if Version(redis.__version__) < Version('3.5.0'):
        pytest.skip('redis-py library too old to run this test')
    r = connect(redis_host, redis_port)
    key = random_string(10)
    field = random_string(10)
    val = random_string(10)
    field2 = random_string(10)
    val2 = random_string(10)

    assert r.hset(key, None, None, {field: val, field2: val2}) == 2

@pytest.mark.xfail(reason="HSET command does not support return of changes, it always return 1")
def test_hset_return_changes(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    field = random_string(10)
    val = random_string(10)

    assert r.hset(key, field, val) == 1
    assert r.hset(key, field, val) == 0

def test_hget_nonexistent_key(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    field = random_string(10)

    assert r.hget(key, field) == None

def test_hset_hdel_hgetall(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    field = random_string(10)
    val = random_string(10)
    other_field = random_string(10)
    other_val = random_string(10)

    # Check result is empty
    assert r.hgetall(key) == {}

    # Set 2 key/field and test hgetall
    assert r.hset(key, field, val) == 1
    assert r.hset(key, other_field, other_val) == 1
    assert r.hgetall(key) == {field: val, other_field: other_val}

    # Delete 1 key/field and check removal with hgetall
    assert r.hdel(key, field) == 1
    assert r.hgetall(key) == {other_field: other_val}

    # Delete another key/field and check removal with hgetall
    assert r.hdel(key, other_field) == 1
    assert r.hgetall(key) == {}

    with pytest.raises(redis.exceptions.ResponseError) as excinfo:
        r.execute_command("HGETALL testkey testfield")
    assert "wrong number of arguments for 'hgetall' command" in str(excinfo.value)

@pytest.mark.xfail(reason="HDEL command does not support return of changes, it always return 1")
def test_hdel_return_changes(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    field = random_string(10)
    val = random_string(10)

    assert r.hset(key, field, val) == 1
    assert r.hdel(key, field) == 1
    assert r.hdel(key, field) == 0

def test_hdel_several_keys(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    field1 = random_string(10)
    val1 = random_string(10)
    field2 = random_string(10)
    val2 = random_string(10)
    field3 = random_string(10)
    val3 = random_string(10)

    # Set 2 key/field
    assert r.hset(key, field1, val1) == 1
    assert r.hset(key, field2, val2) == 1
    assert r.hset(key, field3, val3) == 1

    # Delete 2 of them
    assert r.hdel(key, field1, field3) == 2

    # Check the remaining item
    assert r.hgetall(key) == {field2: val2}

def test_delete_hash(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    field = random_string(10)
    val = random_string(10)
    field2 = random_string(10)
    val2 = random_string(10)

    assert r.hset(key, field, val) == 1
    assert r.hset(key, field2, val2) == 1
    assert r.hgetall(key) == {field: val, field2: val2}
    assert r.delete(key) == 1
    assert r.hgetall(key) == {}

def test_hexists(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    field = random_string(10)

    assert r.hexists(key, field) == 0
    assert r.hset(key, field, random_string(10)) == 1
    assert r.hexists(key, field) == 1
