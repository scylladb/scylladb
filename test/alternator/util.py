# Copyright 2019 ScyllaDB
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
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

# Various utility functions which are useful for multiple tests

import string
import random
import collections
import time

def random_string(length=10, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(length))

def random_bytes(length=10):
    return bytearray(random.getrandbits(8) for _ in range(length))

# Utility functions for scan and query into an array of items:
# TODO: add to full_scan and full_query by default ConsistentRead=True, as
# it's not useful for tests without it!
def full_scan(table, **kwargs):
    response = table.scan(**kwargs)
    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], **kwargs)
        items.extend(response['Items'])
    return items

# full_scan_and_count returns both items and count as returned by the server.
# Note that count isn't simply len(items) - the server returns them
# independently. e.g., with Select='COUNT' the items are not returned, but
# count is.
def full_scan_and_count(table, **kwargs):
    response = table.scan(**kwargs)
    items = []
    count = 0
    if 'Items' in response:
        items.extend(response['Items'])
    if 'Count' in response:
        count = count + response['Count']
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], **kwargs)
        if 'Items' in response:
            items.extend(response['Items'])
        if 'Count' in response:
            count = count + response['Count']
    return (count, items)

# Utility function for fetching the entire results of a query into an array of items
def full_query(table, **kwargs):
    response = table.query(**kwargs)
    items = response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.query(ExclusiveStartKey=response['LastEvaluatedKey'], **kwargs)
        items.extend(response['Items'])
    return items

# To compare two lists of items (each is a dict) without regard for order,
# "==" is not good enough because it will fail if the order is different.
# The following function, multiset() converts the list into a multiset
# (set with duplicates) where order doesn't matter, so the multisets can
# be compared.

def freeze(item):
    if isinstance(item, dict):
        return frozenset((key, freeze(value)) for key, value in item.items())
    elif isinstance(item, list):
        return tuple(freeze(value) for value in item)
    return item

def multiset(items):
    return collections.Counter([freeze(item) for item in items])

# NOTE: alternator_Test prefix contains a capital letter on purpose,
#in order to validate case sensitivity in alternator
test_table_prefix = 'alternator_Test_'
def test_table_name():
    current_ms = int(round(time.time() * 1000))
    # In the off chance that test_table_name() is called twice in the same millisecond...
    if test_table_name.last_ms >= current_ms:
        current_ms = test_table_name.last_ms + 1
    test_table_name.last_ms = current_ms
    return test_table_prefix + str(current_ms)
test_table_name.last_ms = 0

def create_test_table(dynamodb, **kwargs):
    name = test_table_name()
    print("fixture creating new table {}".format(name))
    table = dynamodb.create_table(TableName=name,
        BillingMode='PAY_PER_REQUEST', **kwargs)
    waiter = table.meta.client.get_waiter('table_exists')
    # recheck every second instead of the default, lower, frequency. This can
    # save a few seconds on AWS with its very slow table creation, but can
    # more on tests on Scylla with its faster table creation turnaround.
    waiter.config.delay = 1
    waiter.config.max_attempts = 200
    waiter.wait(TableName=name)
    return table

# DynamoDB's ListTables request returns up to a single page of table names
# (e.g., up to 100) and it is up to the caller to call it again and again
# to get the next page. This is a utility function which calls it repeatedly
# as much as necessary to get the entire list.
# We deliberately return a list and not a set, because we want the caller
# to be able to recognize bugs in ListTables which causes the same table
# to be returned twice.
def list_tables(dynamodb, limit=100):
    ret = []
    pos = None
    while True:
        if pos:
            page = dynamodb.meta.client.list_tables(Limit=limit, ExclusiveStartTableName=pos);
        else:
            page = dynamodb.meta.client.list_tables(Limit=limit);
        results = page.get('TableNames', None)
        assert(results)
        ret = ret + results
        newpos = page.get('LastEvaluatedTableName', None)
        if not newpos:
            break;
        # It doesn't make sense for Dynamo to tell us we need more pages, but
        # not send anything in *this* page!
        assert len(results) > 0
        assert newpos != pos
        # Note that we only checked that we got back tables, not that we got
        # any new tables not already in ret. So a buggy implementation might
        # still cause an endless loop getting the same tables again and again.
        pos = newpos
    return ret
