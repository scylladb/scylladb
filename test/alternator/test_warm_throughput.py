# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Tests for the WarmThroughput table option. Alternator has no throughput caps
# and nothing to pre-warm (issue #21853), so it only remembers what a request
# configured and reports it back. That round-trip is what
# these tests check, and DynamoDB behaves the same way. A table which never
# configured a WarmThroughput is covered by
# test_describe_table.py::test_describe_table_warm_throughput.

import pytest
import time
from botocore.exceptions import ClientError
from test.alternator.util import is_aws, new_test_table, new_test_table_description, wait_for_gsi

# The warm throughput every table starts with in DynamoDB, which is also the
# least it can be configured with - see
# test_create_table_warm_throughput_below_minimum() below. Alternator does not
# enforce this. Every other value in this file is a multiple of these, written
# at the point it is requested: raising a table's warm throughput is a real
# pre-warm on DynamoDB whose duration grows with the size of the increase, so
# how far above the minimum a test goes is part of what the test is saying.
# Each unit of increase is also billed, so the multiples stay at 1.01 and 1.02.
MIN_READ_UNITS = 12000
MIN_WRITE_UNITS = 4000

def warm_throughput(read_multiple, write_multiple):
    return {'ReadUnitsPerSecond': int(MIN_READ_UNITS * read_multiple),
            'WriteUnitsPerSecond': int(MIN_WRITE_UNITS * write_multiple)}

def wait_for_warm_throughput(table, warm, index_name=None):
    # DynamoDB applies the change asynchronously - WarmThroughput.Status is
    # UPDATING until it settles - whereas in Alternator it is immediate.
    if is_aws(table):
        timeout = 1800
        delay = 2
    else:
        timeout = 60
        delay = 0.1
    if index_name:
        # A GSI reports no WarmThroughput until the GSI is ACTIVE.
        wait_for_gsi(table, index_name)
    got = None
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        desc = table.meta.client.describe_table(TableName=table.name)['Table']
        if index_name:
            desc = next(gsi for gsi in desc['GlobalSecondaryIndexes'] if gsi['IndexName'] == index_name)
        got = desc['WarmThroughput']
        if got['Status'] == 'ACTIVE':
            for unit, value in warm.items():
                assert got[unit] == value
            return got
        time.sleep(delay)
    pytest.fail(f'WarmThroughput never became ACTIVE, last seen {got}')

# A WarmThroughput given to CreateTable is reported back by DescribeTable.
def test_create_table_warm_throughput(dynamodb):
    warm = warm_throughput(1.02, 1.02)
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            WarmThroughput=warm) as table:
        assert wait_for_warm_throughput(table, warm)['Status'] == 'ACTIVE'

# An UpdateTable carrying nothing but WarmThroughput is accepted - it is not the
# "UpdateTable requires one of ..." empty request - and takes effect. This is
# the request shape the Terraform AWS Provider sends for a warm_throughput
# block, which Alternator used to reject with a message that never mentioned
# warm throughput. Reproduces CUSTOMER-705.
def test_update_table_warm_throughput(dynamodb):
    warm = warm_throughput(1.02, 1.02)
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        table.meta.client.update_table(TableName=table.name, WarmThroughput=warm)
        wait_for_warm_throughput(table, warm)

# Both WarmThroughput units are optional, and a request may configure just one
# of them. DynamoDB remembers the one it was given, filling the other with the
# baseline every table already has; Alternator has no baseline to report and
# leaves the unconfigured unit at zero, so only the configured one is asserted.
@pytest.mark.parametrize('unit,minimum', [
        ('ReadUnitsPerSecond', MIN_READ_UNITS),
        ('WriteUnitsPerSecond', MIN_WRITE_UNITS)])
def test_create_table_warm_throughput_one_unit(dynamodb, unit, minimum):
    warm = {unit: int(minimum * 1.02)}
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            WarmThroughput=warm) as table:
        assert wait_for_warm_throughput(table, warm)['Status'] == 'ACTIVE'

# The CreateTable response echoes the configured WarmThroughput back with a
# Status member which the request itself does not carry. The status differs
# between the two implementations - DynamoDB reports UPDATING while the change
# settles, Alternator's CreateTable is synchronous and reports ACTIVE - so only
# the member's presence is asserted, which is what a client reading the response
# needs.
def test_create_table_warm_throughput_response(dynamodb):
    warm = warm_throughput(1.02, 1.02)
    with new_test_table_description(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST',
            WarmThroughput=warm) as got:
        assert got['WarmThroughput']['ReadUnitsPerSecond'] == warm['ReadUnitsPerSecond']
        assert got['WarmThroughput']['WriteUnitsPerSecond'] == warm['WriteUnitsPerSecond']
        assert got['WarmThroughput']['Status'] in ['ACTIVE', 'UPDATING']

# A WarmThroughput with neither unit configures nothing: DynamoDB's CreateTable
# accepts one and leaves it out of its response.
def test_create_table_warm_throughput_empty(dynamodb):
    with new_test_table_description(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST',
            WarmThroughput={}) as got:
        assert not 'WarmThroughput' in got

# An update to a WarmThroughput with neither unit is refused, for the table and
# for a GSI alike. DynamoDB refuses the GSI's with the message matched here,
# but fails the table's with an InternalFailure, so only the rejection itself
# is asserted for the table.
def test_update_table_warm_throughput_empty(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'x', 'AttributeType': 'S'}],
            GlobalSecondaryIndexes=[{
                'IndexName': 'gsi',
                'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                'Projection': {'ProjectionType': 'ALL'}}]) as table:
        with pytest.raises(ClientError):
            table.meta.client.update_table(TableName=table.name, WarmThroughput={})
        with pytest.raises(ClientError, match='ValidationException.*WarmThroughput.*ReadUnitsPerSecond or WriteUnitsPerSecond'):
            table.meta.client.update_table(TableName=table.name,
                GlobalSecondaryIndexUpdates=[{'Update': {'IndexName': 'gsi', 'WarmThroughput': {}}}])

# A GSI has a WarmThroughput of its own, and a WarmThroughput given to a GSI by
# CreateTable is reported back for that GSI by DescribeTable.
def test_gsi_create_table_warm_throughput(dynamodb):
    warm = warm_throughput(1.02, 1.02)
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'x', 'AttributeType': 'S'}],
            GlobalSecondaryIndexes=[{
                'IndexName': 'gsi',
                'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                'Projection': {'ProjectionType': 'ALL'},
                'WarmThroughput': warm}]) as table:
        wait_for_warm_throughput(table, warm, 'gsi')

# A GSI added to an existing table by UpdateTable keeps the WarmThroughput it
# was created with.
def test_gsi_created_by_update_table_warm_throughput(dynamodb):
    warm = warm_throughput(1.02, 1.02)
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        table.meta.client.update_table(TableName=table.name,
            AttributeDefinitions=[{'AttributeName': 'x', 'AttributeType': 'S'}],
            GlobalSecondaryIndexUpdates=[{'Create': {
                'IndexName': 'gsi',
                'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                'Projection': {'ProjectionType': 'ALL'},
                'WarmThroughput': warm}}])
        wait_for_warm_throughput(table, warm, 'gsi')

# A GlobalSecondaryIndexUpdates Update carrying nothing but a WarmThroughput
# takes effect. This is the request the Terraform AWS Provider sends for a
# warm_throughput block in a global_secondary_index, which Alternator used to
# reject.
def test_gsi_update_table_warm_throughput(dynamodb):
    warm = warm_throughput(1.02, 1.02)
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'x', 'AttributeType': 'S'}],
            GlobalSecondaryIndexes=[{
                'IndexName': 'gsi',
                'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                'Projection': {'ProjectionType': 'ALL'}}]) as table:
        table.meta.client.update_table(TableName=table.name,
            GlobalSecondaryIndexUpdates=[{'Update': {'IndexName': 'gsi', 'WarmThroughput': warm}}])
        wait_for_warm_throughput(table, warm, 'gsi')

# Once raised, a warm throughput cannot be lowered again - the documentation
# says "After warm throughput is increased, the values can't be decreased", and
# the service rejects the attempt naming the unit and the reason. A table
# created with a warm throughput has it from the moment the table exists, with
# no pre-warm to wait for, so the decrease is refused immediately. Alternator
# enforces no warm-throughput semantics (issue #21853) and simply remembers the
# lower value.
@pytest.mark.xfail(reason="Alternator does not enforce WarmThroughput semantics, issue #21853")
def test_update_table_warm_throughput_cannot_decrease_after_create(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            WarmThroughput=warm_throughput(1.02, 1.02)) as table:
        with pytest.raises(ClientError, match='ValidationException.*lower than current WarmThroughput'):
            table.meta.client.update_table(TableName=table.name,
                WarmThroughput=warm_throughput(1.01, 1.01))

# The same rule for a table which reached its warm throughput by being raised
# from the default rather than created with one. The rule is enforced against
# the settled value, not against a raise still in flight - while the pre-warm is
# running DescribeTable still reports the old units and a lower request is
# accepted - so the increase has to complete before the decrease below means
# anything. Waiting for it takes about ten minutes against DynamoDB; against
# Alternator the increase is immediate.
@pytest.mark.xfail(reason="Alternator does not enforce WarmThroughput semantics, issue #21853")
def test_update_table_warm_throughput_cannot_decrease_after_increase(dynamodb):
    increased = warm_throughput(1.02, 1.02)
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}]) as table:
        table.meta.client.update_table(TableName=table.name, WarmThroughput=increased)
        wait_for_warm_throughput(table, increased)
        with pytest.raises(ClientError, match='ValidationException.*lower than current WarmThroughput'):
            table.meta.client.update_table(TableName=table.name,
                WarmThroughput=warm_throughput(1.01, 1.01))

# DynamoDB refuses to configure a warm throughput below the baseline every table
# already has, reporting which of the two units was too low. Alternator does not
# enforce any warm-throughput semantics (issue #21853), so it accepts these
# requests and remembers the values; this test documents DynamoDB's behaviour
# and fails against Alternator until that changes.
@pytest.mark.xfail(reason="Alternator does not enforce WarmThroughput minimums, issue #21853")
@pytest.mark.parametrize('read_units,write_units,too_low', [
        (MIN_READ_UNITS - 1, MIN_WRITE_UNITS, 'ReadUnitsPerSecond'),
        (MIN_READ_UNITS, MIN_WRITE_UNITS - 1, 'WriteUnitsPerSecond')])
def test_create_table_warm_throughput_below_minimum(dynamodb, read_units, write_units, too_low):
    with pytest.raises(ClientError, match=f'ValidationException.*{too_low}.*lower than'):
        with new_test_table(dynamodb,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                WarmThroughput={'ReadUnitsPerSecond': read_units,
                                'WriteUnitsPerSecond': write_units}) as table:
            pass
