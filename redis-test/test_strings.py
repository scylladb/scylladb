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
import requests
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

def test_select_invalid_db():
    r = connect()
    logger.debug('Assume that user will not set redis_database_count to be bigger as 100')
    invalid_db_idx = 100

    logger.debug('Try to switch to invalid database %d' % invalid_db_idx)
    try:
        query = 'SELECT %d' % invalid_db_idx
        r.execute_command(query)
        raise Exception('Expect that `%s` does not work' % query)
    except redis.exceptions.ResponseError as ex:
        assert str(ex) == 'invalid DB index'
