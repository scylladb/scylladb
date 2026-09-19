# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the WarmThroughput table option. Alternator has no throughput caps
# and nothing to pre-warm (issue #21853), so it remembers what a request
# configured and reports it back without enforcing it. That round-trip is what
# these tests check, and DynamoDB behaves the same way. A table which never
# configured a WarmThroughput is covered by test_describe_table.py.

import pytest
import time
from test.alternator.util import new_test_table

# DynamoDB's baseline warm throughput is 12000 read and 4000 write units per
# second and a table cannot ask for less, so these are above those minimums.
READ_UNITS = 20000
WRITE_UNITS = 8000

def wait_for_warm_throughput(table, read_units, write_units):
    # DynamoDB applies the change asynchronously - WarmThroughput.Status is
    # UPDATING until it settles - whereas in Alternator it is immediate.
    got = None
    deadline = time.time() + 600
    while time.time() < deadline:
        got = table.meta.client.describe_table(TableName=table.name)['Table']['WarmThroughput']
        if got['ReadUnitsPerSecond'] == read_units and got['WriteUnitsPerSecond'] == write_units:
            return got
        time.sleep(1)
    pytest.fail(f'WarmThroughput never reached {read_units}/{write_units}, last seen {got}')

# A WarmThroughput given to CreateTable is reported back by DescribeTable.
def test_create_table_warm_throughput(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            WarmThroughput={'ReadUnitsPerSecond': READ_UNITS, 'WriteUnitsPerSecond': WRITE_UNITS}) as table:
        assert wait_for_warm_throughput(table, READ_UNITS, WRITE_UNITS)['Status'] == 'ACTIVE'

# An UpdateTable carrying nothing but WarmThroughput is accepted - it is not the
# "UpdateTable requires one of ..." empty request - and takes effect. This is
# the request shape the Terraform AWS Provider sends for a warm_throughput
# block, which Alternator used to reject with a message that never mentioned
# warm throughput. Reproduces CUSTOMER-705.
def test_update_table_warm_throughput(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        table.meta.client.update_table(TableName=table.name,
            WarmThroughput={'ReadUnitsPerSecond': READ_UNITS, 'WriteUnitsPerSecond': WRITE_UNITS})
        wait_for_warm_throughput(table, READ_UNITS, WRITE_UNITS)
