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
from util import random_string, connect

logger = logging.getLogger('redis-test')

def test_set_get_delete():
    r = connect()
    key = random_string(10)
    val = random_string(10)

    assert r.set(key, val) == True
    assert r.get(key) == val
    assert r.delete(key) == 1
    assert r.get(key) == None 


def test_get():
    r = connect()
    key = random_string(10)
    r.delete(key)

    assert r.get(key) == None 

def test_del_existent_key():
    r = connect()
    key = random_string(10)
    val = random_string(10)

    r.set(key, val)
    assert r.get(key) == val
    assert r.delete(key) == 1

@pytest.mark.xfail(reason="DEL command does not support to return number of deleted keys")
def test_del_non_existent_key():
    r = connect()
    key = random_string(10)
    r.delete(key)
    assert r.delete(key) == 0

def test_set_empty_string():
    r = connect()
    key = random_string(10)
    val = ""
    r.set(key, val)
    assert r.get(key) == val
    r.delete(key)

def test_set_large_string():
    r = connect()
    key = random_string(10)
    val = random_string(4096)
    r.set(key, val)
    assert r.get(key) == val
    r.delete(key)

def test_ping():
    r = connect()
    assert r.ping() == True

def test_echo():
    r = connect()
    assert r.echo('hello world') == 'hello world'

def test_select():
    r = connect()
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

    logger.debug('Try to switch to invalid database 16')
    try:
        r.execute_command('SELECT 16')
        raise Exception('Expect that `SELECT 16` does not work')
    except redis.exceptions.ResponseError as ex:
        assert str(ex) == 'DB index is out of range'

def test_exists_existent_key():
    r = connect()
    key = random_string(10)
    val = random_string(10)

    r.set(key, val)
    assert r.get(key) == val
    assert r.exists(key) == 1

def test_exists_non_existent_key():
    r = connect()
    key = random_string(10)
    r.delete(key)
    assert r.exists(key) == 0

def test_exists_multiple_existent_key():
    r = connect()
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
