#
# Copyright (C) 2019 pengjian.uestc @ gmail.com
#
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
import re
import time
from util import random_string, connect

logger = logging.getLogger('redis-test')

def test_set_get_delete(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    val = random_string(10)

    assert r.set(key, val) == True
    assert r.get(key) == val
    assert r.delete(key) == 1
    assert r.get(key) == None 


def test_get(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    r.delete(key)

    assert r.get(key) == None 

def test_del_existent_key(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    val = random_string(10)

    r.set(key, val)
    assert r.get(key) == val
    assert r.delete(key) == 1

@pytest.mark.xfail(reason="DEL command does not support to return number of deleted keys")
def test_del_non_existent_key(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    r.delete(key)
    assert r.delete(key) == 0

def test_set_empty_string(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    val = ""
    r.set(key, val)
    assert r.get(key) == val
    r.delete(key)

def test_set_large_string(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    val = random_string(4096)
    r.set(key, val)
    assert r.get(key) == val
    r.delete(key)

def test_ping(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    assert r.ping() == True

def test_echo(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    assert r.echo('hello world') == 'hello world'

def test_select(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    val = random_string(4096)
    r.set(key, val)
    assert r.get(key) == val

    logger.debug('Switch to database 1')
    assert r.execute_command('SELECT 1') == 'OK'
    assert r.get(key) == None

    logger.debug('Switch back to default database 0')
    assert r.execute_command('SELECT 0') == 'OK'
    assert r.get(key) == val
    r.delete(key)
    assert r.get(key) == None

def test_select_invalid_db(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    logger.debug('Assume that user will not set redis_database_count to be bigger as 100')
    invalid_db_idx = 100

    logger.debug('Try to switch to invalid database {}'.format(invalid_db_idx))
    try:
        query = 'SELECT {}'.format(invalid_db_idx)
        r.execute_command(query)
        raise Exception('Expect that `{}` does not work'.format(query))
    except redis.exceptions.ResponseError as ex:
        assert str(ex) == 'DB index is out of range'

def test_exists_existent_key(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    val = random_string(10)

    r.set(key, val)
    assert r.get(key) == val
    assert r.exists(key) == 1

def test_exists_non_existent_key(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    r.delete(key)
    assert r.exists(key) == 0

def test_exists_multiple_existent_key(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key1 = random_string(10)
    val1 = random_string(10)
    key2 = random_string(10)
    val2 = random_string(10)
    key3 = random_string(10)
    val3 = random_string(10)
    key4 = random_string(10)

    r.set(key1, val1)
    r.set(key2, val2)
    r.set(key3, val3)
    r.delete(key4)
    assert r.get(key1) == val1
    assert r.get(key2) == val2
    assert r.get(key3) == val3
    assert r.get(key4) == None
    assert r.exists(key1, key2, key3, key4) == 3

def test_setex_ttl(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    val = random_string(10)

    assert r.setex(key, 100, val) == True
    time.sleep(1)
    assert r.ttl(key) == 99

def test_set_ex(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key = random_string(10)
    val = random_string(10)

    assert r.execute_command('SET', key, val, 'EX', 100)
    time.sleep(1)
    assert r.ttl(key) == 99

def test_lolwut(redis_host, redis_port):
    pattern1 = r'''
^⠀⡤⠤⠤⠤⠤⠤⠤⠤⡄
⠀⡇⠀⠀⠀⠀⠀⠀⠀⡇
⠀⡇⠀⠀⠀⠀⠀⠀⠀⡇
⠀⡇⠀⠀⠀⠀⠀⠀⠀⡇
⠀⡧⠤⠤⠤⠤⠤⠤⠤⡇
⠀⡇⠀⠀⠀⠀⠀⠀⠀⡇
⠀⡇⠀⠀⠀⠀⠀⠀⠀⡇
⠀⡇⠀⠀⠀⠀⠀⠀⠀⡇
⠀⠧⠤⠤⠤⠤⠤⠤⠤⠇

Georg Nees - schotter, plotter on paper, 1968\. Redis ver\. [0-9]+\.[0-9]+\.[0-9]+
'''[1:-1]
    pattern2 = r'''
^⠀⡤⠤⠤⠤⡤⠤⠤⠤⡄
⠀⡇⠀⠀⠀⡇⠀⠀⠀⡇
⠀⡧⠤⠤⠤⡧⠤⠤⠤⡇
⠀⡇⠀⠀⠀⡇⠀⠀⠀⡇
⠀⠧⠤⠤⠤⠧⠤⠤⠤⠇

Georg Nees - schotter, plotter on paper, 1968\. Redis ver\. [0-9]+\.[0-9]+\.[0-9]+
'''[1:-1]
    pattern3 = r'''
^⠀⡤⠤⡤⠤⡤⠤⡤⠤⡄
⠀⡧⠤⡧⠤⡧⠤⡧⠤⡇
⠀⠧⠤⠧⠤⠧⠤⠧⠤⠇

Georg Nees - schotter, plotter on paper, 1968\. Redis ver\. [0-9]+\.[0-9]+\.[0-9]+
'''[1:-1]
    r = connect(redis_host, redis_port)
    res = r.execute_command('LOLWUT', 10, 1, 2)
    assert re.match(pattern1, res)
    res = r.execute_command('LOLWUT', 10, 2, 2)
    assert re.match(pattern2, res)
    res = r.execute_command('LOLWUT', 10, 4, 2)
    assert re.match(pattern3, res)

def test_strlen(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key1 = random_string(10)
    val1 = random_string(10)
    key2 = random_string(10)
    val2 = random_string(1000)
    key3 = random_string(10)

    assert r.set(key1, val1) == True
    assert r.set(key2, val2) == True
    r.delete(key3)
    assert r.strlen(key1) == 10
    assert r.strlen(key2) == 1000
    assert r.strlen(key3) == 0

@pytest.mark.xfail(reason="types on redis does not implemented yet")
def test_strlen_wrongtype(redis_host, redis_port):
    r = connect(redis_host, redis_port)
    key1 = random_string(10)
    val1 = random_string(10)
    val2 = random_string(10)

    assert r.rpush(key1, val1)
    assert r.rpush(key1, val2)
    try:
        r.strlen(key1)
    except redis.exceptions.ResponseError as ex:
        assert str(ex) == 'WRONGTYPE Operation against a key holding the wrong kind of value'
