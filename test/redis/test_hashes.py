#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
#

import pytest
import redis
import logging
from util import random_string, connect

logger = logging.getLogger('redis-test')

def test_hset_hget_delete(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    field = random_string(10)
    val = random_string(10)
    other_val = random_string(10)

    try:
        r.execute_command("HSET testkey")
        raise Exception('Expect that `HSET testkey` does not work')
    except redis.exceptions.ResponseError as ex:
        assert str(ex) == "wrong number of arguments for 'hset' command"

    try:
        r.execute_command("HSET testkey testfield")
        raise Exception('Expect that `HSET testkey testfield` does not work')
    except redis.exceptions.ResponseError as ex:
        assert str(ex) == "wrong number of arguments for 'hset' command"

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
    from distutils.version import LooseVersion
    if LooseVersion(redis.__version__) < LooseVersion('3.5.0'):
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
