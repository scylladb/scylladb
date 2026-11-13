# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# Test for ProvisionedThroughput
# ProvisionedThroughput is part of a table definition
# The following tests make sure we can get, set and update its value

import pytest
from botocore.exceptions import ClientError
from test.alternator.util import new_test_table, wait_for_gsi, wait_for_gsi_gone, unique_table_name

# When creating a table with PROVISIONED billing mode, ProvisionedThroughput must be explicitly set,
# and the same values should be reflected when the table is described.
def test_create_table(dynamodb):
    KeySchema=[ { 'AttributeName': 'p123', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c4567', 'KeyType': 'RANGE' }
        ]
    AttributeDefinitions=[
                { 'AttributeName': 'p123', 'AttributeType': 'S' },
                { 'AttributeName': 'c4567', 'AttributeType': 'S' },
    ]

    ProvisionedThroughput={
        'ReadCapacityUnits': 2,
        'WriteCapacityUnits': 3
    }
    with new_test_table(dynamodb,
                        KeySchema=KeySchema,
        AttributeDefinitions=AttributeDefinitions,
        BillingMode='PROVISIONED',
        ProvisionedThroughput=ProvisionedThroughput) as table:
        got = table.meta.client.describe_table(TableName=table.name)['Table']
        if 'BillingModeSummary' in got:
            # PROVISIONED BillingMode is the default and can be omitted, only check if it's present
            assert got['BillingModeSummary']['BillingMode'] == 'PROVISIONED'
        assert got['ProvisionedThroughput']['ReadCapacityUnits'] == ProvisionedThroughput['ReadCapacityUnits']
        assert got['ProvisionedThroughput']['WriteCapacityUnits'] == ProvisionedThroughput['WriteCapacityUnits']

# When creating a table with PROVISIONED billing mode, ProvisionedThroughput must be explicitly set,
# and both Read and Write capacity should be present.
def test_create_table_missing_units(dynamodb):
    KeySchema=[ { 'AttributeName': 'p123', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c4567', 'KeyType': 'RANGE' }
        ]
    AttributeDefinitions=[
                { 'AttributeName': 'p123', 'AttributeType': 'S' },
                { 'AttributeName': 'c4567', 'AttributeType': 'S' },
    ]
    for ProvisionedThroughput in [{'WriteCapacityUnits': 1}, {'ReadCapacityUnits': 5}]:
        with pytest.raises(ClientError, match='ValidationException.*provisionedThroughput.*CapacityUnits.*'):
            with new_test_table(dynamodb,
                                KeySchema=KeySchema,
                AttributeDefinitions=AttributeDefinitions,
                BillingMode='PROVISIONED',
                ProvisionedThroughput=ProvisionedThroughput):
                    table.meta.client.describe_table(TableName=table.name)

# DynamoDB's minimum for either of the table's capacities is 1, and it rejects
# anything lower with a protocol-level message Alternator does not reproduce.
@pytest.mark.parametrize('throughput', [
        {'ReadCapacityUnits': 0, 'WriteCapacityUnits': 5},
        {'ReadCapacityUnits': 5, 'WriteCapacityUnits': -1}])
def test_create_table_nonpositive_units(dynamodb, throughput):
    with pytest.raises(ClientError, match='ValidationException'):
        with new_test_table(dynamodb,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
                BillingMode='PROVISIONED',
                ProvisionedThroughput=throughput) as table:
            pass

# When creating a table with PAY_PER_REQUEST billing mode, RCU and WCU should be zero
def test_create_pay_per_request_units(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    assert got['BillingModeSummary']['BillingMode'] == 'PAY_PER_REQUEST'
    assert got['ProvisionedThroughput']['ReadCapacityUnits'] == 0
    assert got['ProvisionedThroughput']['WriteCapacityUnits'] == 0

# A GSI has its own ProvisionedThroughput, separate from the base table's.
# The tests below check that Alternator remembers the configured values and
# reports them back - it does not enforce them, exactly like the base table's.
# Reproduces issue #19718.

# A GSI created together with the table keeps its own ProvisionedThroughput,
# which DescribeTable reports separately from the base table's.
def test_gsi_create_table(dynamodb):
    rcu, wcu, gsi_rcu, gsi_wcu = 2, 3, 4, 5
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'x', 'AttributeType': 'S'}],
            BillingMode='PROVISIONED',
            ProvisionedThroughput={'ReadCapacityUnits': rcu, 'WriteCapacityUnits': wcu},
            GlobalSecondaryIndexes=[{
                'IndexName': 'gsi',
                'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                'Projection': {'ProjectionType': 'ALL'},
                'ProvisionedThroughput': {'ReadCapacityUnits': gsi_rcu, 'WriteCapacityUnits': gsi_wcu}}]) as table:
        got = table.meta.client.describe_table(TableName=table.name)['Table']
        assert got['ProvisionedThroughput']['ReadCapacityUnits'] == rcu
        assert got['ProvisionedThroughput']['WriteCapacityUnits'] == wcu
        gsi = got['GlobalSecondaryIndexes'][0]
        assert gsi['ProvisionedThroughput']['ReadCapacityUnits'] == gsi_rcu
        assert gsi['ProvisionedThroughput']['WriteCapacityUnits'] == gsi_wcu

# A GSI added to an existing table by UpdateTable also keeps the
# ProvisionedThroughput it was created with.
def test_gsi_created_by_update_table(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            BillingMode='PROVISIONED',
            ProvisionedThroughput={'ReadCapacityUnits': 2, 'WriteCapacityUnits': 3}) as table:
        table.meta.client.update_table(TableName=table.name,
            AttributeDefinitions=[{'AttributeName': 'x', 'AttributeType': 'S'}],
            GlobalSecondaryIndexUpdates=[{'Create': {
                'IndexName': 'gsi',
                'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                'Projection': {'ProjectionType': 'ALL'},
                'ProvisionedThroughput': {'ReadCapacityUnits': 8, 'WriteCapacityUnits': 9}}}])
        wait_for_gsi(table, 'gsi')
        got = table.meta.client.describe_table(TableName=table.name)['Table']
        gsi = got['GlobalSecondaryIndexes'][0]
        assert gsi['ProvisionedThroughput']['ReadCapacityUnits'] == 8
        assert gsi['ProvisionedThroughput']['WriteCapacityUnits'] == 9
        # The base table's own capacities are untouched by the UpdateTable.
        assert got['ProvisionedThroughput']['ReadCapacityUnits'] == 2
        assert got['ProvisionedThroughput']['WriteCapacityUnits'] == 3

# A GSI's ProvisionedThroughput must be given when, and only when, the table is
# in PROVISIONED billing mode. Both mismatches below are rejected by DynamoDB,
# with the messages quoted here, so Alternator rejects them too.
# Reproduces issue #19718.
def test_gsi_throughput_required_in_provisioned_mode(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*ProvisionedThroughput.*not specified'):
        with new_test_table(dynamodb,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[
                    {'AttributeName': 'p', 'AttributeType': 'S'},
                    {'AttributeName': 'x', 'AttributeType': 'S'}],
                BillingMode='PROVISIONED',
                ProvisionedThroughput={'ReadCapacityUnits': 2, 'WriteCapacityUnits': 3},
                GlobalSecondaryIndexes=[{
                    'IndexName': 'gsi',
                    'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                    'Projection': {'ProjectionType': 'ALL'}}]) as table:
            pass

def test_gsi_throughput_forbidden_in_pay_per_request_mode(dynamodb):
    with pytest.raises(ClientError, match='ValidationException.*ProvisionedThroughput.*PAY_PER_REQUEST'):
        with new_test_table(dynamodb,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[
                    {'AttributeName': 'p', 'AttributeType': 'S'},
                    {'AttributeName': 'x', 'AttributeType': 'S'}],
                BillingMode='PAY_PER_REQUEST',
                GlobalSecondaryIndexes=[{
                    'IndexName': 'gsi',
                    'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 4, 'WriteCapacityUnits': 5}}]) as table:
            pass

# The same rule applies to a GSI added later by UpdateTable.
def test_gsi_throughput_forbidden_by_update_table_in_pay_per_request_mode(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST') as table:
        with pytest.raises(ClientError, match='ValidationException.*CapacityUnits.*PAY_PER_REQUEST'):
            table.meta.client.update_table(TableName=table.name,
                AttributeDefinitions=[{'AttributeName': 'x', 'AttributeType': 'S'}],
                GlobalSecondaryIndexUpdates=[{'Create': {
                    'IndexName': 'gsi',
                    'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 4, 'WriteCapacityUnits': 5}}}])

# A GSI added by UpdateTable must carry its own ProvisionedThroughput when the
# table is PROVISIONED, exactly as one created together with the table must.
# DynamoDB words this rejection differently from CreateTable's above.
def test_gsi_throughput_required_by_update_table_in_provisioned_mode(dynamodb):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            BillingMode='PROVISIONED',
            ProvisionedThroughput={'ReadCapacityUnits': 2, 'WriteCapacityUnits': 3}) as table:
        with pytest.raises(ClientError, match='ValidationException.*CapacityUnits.*must be specified'):
            table.meta.client.update_table(TableName=table.name,
                AttributeDefinitions=[{'AttributeName': 'x', 'AttributeType': 'S'}],
                GlobalSecondaryIndexUpdates=[{'Create': {
                    'IndexName': 'gsi',
                    'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                    'Projection': {'ProjectionType': 'ALL'}}}])

# A GSI's ProvisionedThroughput, when present at all, must carry both capacities.
# DynamoDB checks this before it looks at the billing mode - it rejects a
# half-filled throughput even on a PAY_PER_REQUEST table, which may carry no
# throughput at all - but it does so with a protocol-level message which
# Alternator does not reproduce, so only the rejection itself is asserted.
@pytest.mark.parametrize('billing_args', [
        {'BillingMode': 'PROVISIONED',
         'ProvisionedThroughput': {'ReadCapacityUnits': 2, 'WriteCapacityUnits': 3}},
        {'BillingMode': 'PAY_PER_REQUEST'}])
@pytest.mark.parametrize('gsi_throughput', [
        {'ReadCapacityUnits': 4}, {'WriteCapacityUnits': 5}, {}])
def test_gsi_partial_throughput_rejected_by_create_table(dynamodb, billing_args, gsi_throughput):
    with pytest.raises(ClientError, match='ValidationException'):
        with new_test_table(dynamodb,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[
                    {'AttributeName': 'p', 'AttributeType': 'S'},
                    {'AttributeName': 'x', 'AttributeType': 'S'}],
                GlobalSecondaryIndexes=[{
                    'IndexName': 'gsi',
                    'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': gsi_throughput}],
                **billing_args) as table:
            pass

# The same rule for a GSI added by UpdateTable.
@pytest.mark.parametrize('gsi_throughput', [
        {'ReadCapacityUnits': 4}, {'WriteCapacityUnits': 5}, {}])
def test_gsi_partial_throughput_rejected_by_update_table(dynamodb, gsi_throughput):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            BillingMode='PROVISIONED',
            ProvisionedThroughput={'ReadCapacityUnits': 2, 'WriteCapacityUnits': 3}) as table:
        with pytest.raises(ClientError, match='ValidationException'):
            table.meta.client.update_table(TableName=table.name,
                AttributeDefinitions=[{'AttributeName': 'x', 'AttributeType': 'S'}],
                GlobalSecondaryIndexUpdates=[{'Create': {
                    'IndexName': 'gsi',
                    'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': gsi_throughput}}])

# Both capacities must be at least 1. DynamoDB checks this where it checks the
# completeness above - before the billing mode, so a PAY_PER_REQUEST table which
# may carry no throughput at all rejects the value rather than its presence -
# and again with a protocol-level message Alternator does not reproduce.
# Alternator deliberately accepts zeros, an incompatibility with DynamoDB, so
# only negative capacities are tested here.
@pytest.mark.parametrize('billing_args', [
        {'BillingMode': 'PROVISIONED',
         'ProvisionedThroughput': {'ReadCapacityUnits': 2, 'WriteCapacityUnits': 3}},
        {'BillingMode': 'PAY_PER_REQUEST'}])
@pytest.mark.parametrize('gsi_throughput', [
        {'ReadCapacityUnits': -1, 'WriteCapacityUnits': 5},
        {'ReadCapacityUnits': 4, 'WriteCapacityUnits': -1}])
def test_gsi_negative_throughput_rejected_by_create_table(dynamodb, billing_args, gsi_throughput):
    with pytest.raises(ClientError, match='ValidationException'):
        with new_test_table(dynamodb,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[
                    {'AttributeName': 'p', 'AttributeType': 'S'},
                    {'AttributeName': 'x', 'AttributeType': 'S'}],
                GlobalSecondaryIndexes=[{
                    'IndexName': 'gsi',
                    'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': gsi_throughput}],
                **billing_args) as table:
            pass

# The same rule for a GSI added by UpdateTable.
@pytest.mark.parametrize('gsi_throughput', [
        {'ReadCapacityUnits': -1, 'WriteCapacityUnits': 5},
        {'ReadCapacityUnits': 4, 'WriteCapacityUnits': -1}])
def test_gsi_negative_throughput_rejected_by_update_table(dynamodb, gsi_throughput):
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}],
            BillingMode='PROVISIONED',
            ProvisionedThroughput={'ReadCapacityUnits': 2, 'WriteCapacityUnits': 3}) as table:
        with pytest.raises(ClientError, match='ValidationException'):
            table.meta.client.update_table(TableName=table.name,
                AttributeDefinitions=[{'AttributeName': 'x', 'AttributeType': 'S'}],
                GlobalSecondaryIndexUpdates=[{'Create': {
                    'IndexName': 'gsi',
                    'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': gsi_throughput}}])

# A deliberate incompatibility with DynamoDB: DynamoDB refuses a GSI's
# capacities of zero, on a PAY_PER_REQUEST table as on a PROVISIONED one, but
# Alternator accepts them, as it only remembers the values. On a
# PAY_PER_REQUEST table they are taken as no throughput at all. Scylla Migrator
# relies on this: it copies the zeros DynamoDB reports for every GSI of a
# PAY_PER_REQUEST table into the GSIs of the PAY_PER_REQUEST table it creates
# for such a source (scylladb/scylla-migrator#413). The GSI is created here by
# CreateTable, as Scylla Migrator does, or by UpdateTable, which validates the
# throughput with the same code.
@pytest.mark.parametrize('billing_args', [
        {'BillingMode': 'PAY_PER_REQUEST'},
        {'BillingMode': 'PROVISIONED',
         'ProvisionedThroughput': {'ReadCapacityUnits': 2, 'WriteCapacityUnits': 3}}],
    ids=['pay_per_request', 'provisioned'])
@pytest.mark.parametrize('creator', ['create_table', 'update_table'])
def test_gsi_zero_throughput_accepted(scylla_only, dynamodb, billing_args, creator):
    gsi = {'IndexName': 'gsi',
           'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
           'Projection': {'ProjectionType': 'ALL'},
           'ProvisionedThroughput': {'ReadCapacityUnits': 0, 'WriteCapacityUnits': 0}}
    x = {'AttributeName': 'x', 'AttributeType': 'S'}
    gsi_args = {'GlobalSecondaryIndexes': [gsi]} if creator == 'create_table' else {}
    with new_test_table(dynamodb,
            KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'p', 'AttributeType': 'S'}] + ([x] if gsi_args else []),
            **gsi_args, **billing_args) as table:
        if creator == 'update_table':
            table.meta.client.update_table(TableName=table.name, AttributeDefinitions=[x],
                GlobalSecondaryIndexUpdates=[{'Create': gsi}])
        got = table.meta.client.describe_table(TableName=table.name)['Table']['GlobalSecondaryIndexes'][0]
        assert got['ProvisionedThroughput']['ReadCapacityUnits'] == 0
        assert got['ProvisionedThroughput']['WriteCapacityUnits'] == 0

# The tolerance is for both capacities zero only: on a PAY_PER_REQUEST table, a
# zero next to a non-zero capacity is a throughput, which that mode refuses.
@pytest.mark.parametrize('gsi_throughput', [
        {'ReadCapacityUnits': 0, 'WriteCapacityUnits': 5},
        {'ReadCapacityUnits': 4, 'WriteCapacityUnits': 0}])
def test_gsi_one_zero_capacity_refused_in_pay_per_request_mode(scylla_only, dynamodb, gsi_throughput):
    with pytest.raises(ClientError, match='ValidationException.*PAY_PER_REQUEST'):
        with new_test_table(dynamodb,
                KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
                AttributeDefinitions=[
                    {'AttributeName': 'p', 'AttributeType': 'S'},
                    {'AttributeName': 'x', 'AttributeType': 'S'}],
                BillingMode='PAY_PER_REQUEST',
                GlobalSecondaryIndexes=[{
                    'IndexName': 'gsi',
                    'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': gsi_throughput}]):
            pass

# A PROVISIONED table with one provisioned GSI, gsi1, for the tests below of a
# switch to PAY_PER_REQUEST.
PROVISIONED_TABLE_WITH_GSI = {
    'KeySchema': [{'AttributeName': 'p', 'KeyType': 'HASH'}],
    'AttributeDefinitions': [
        {'AttributeName': 'p', 'AttributeType': 'S'},
        {'AttributeName': 'x', 'AttributeType': 'S'}],
    'BillingMode': 'PROVISIONED',
    'ProvisionedThroughput': {'ReadCapacityUnits': 2, 'WriteCapacityUnits': 3},
    'GlobalSecondaryIndexes': [{
        'IndexName': 'gsi1',
        'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
        'Projection': {'ProjectionType': 'ALL'},
        'ProvisionedThroughput': {'ReadCapacityUnits': 4, 'WriteCapacityUnits': 5}}]}

# A switch to PAY_PER_REQUEST by UpdateTable takes effect: the table reports
# that billing mode and zero capacities for itself and its GSI, and a GSI can be
# added without a ProvisionedThroughput, as that mode requires. The Terraform
# AWS Provider sends the switch and the new GSI in two requests when one apply
# moves a table to PAY_PER_REQUEST and adds a GSI. DynamoDB also accepts both in
# one request, and validates the new GSI against the new billing mode.
@pytest.mark.parametrize('one_request', [False, True], ids=['two_requests', 'one_request'])
def test_update_table_switch_to_pay_per_request(dynamodb, one_request):
    create_gsi2 = {
        'AttributeDefinitions': [{'AttributeName': 'y', 'AttributeType': 'S'}],
        'GlobalSecondaryIndexUpdates': [{'Create': {
            'IndexName': 'gsi2',
            'KeySchema': [{'AttributeName': 'y', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}}}]}
    with new_test_table(dynamodb, **PROVISIONED_TABLE_WITH_GSI) as table:
        table.meta.client.update_table(TableName=table.name, BillingMode='PAY_PER_REQUEST',
            **(create_gsi2 if one_request else {}))
        # DynamoDB applies the switch asynchronously.
        wait_for_gsi(table, 'gsi1')
        got = table.meta.client.describe_table(TableName=table.name)['Table']
        assert got['BillingModeSummary']['BillingMode'] == 'PAY_PER_REQUEST'
        assert got['ProvisionedThroughput']['ReadCapacityUnits'] == 0
        assert got['ProvisionedThroughput']['WriteCapacityUnits'] == 0
        gsi = next(g for g in got['GlobalSecondaryIndexes'] if g['IndexName'] == 'gsi1')
        assert gsi['ProvisionedThroughput']['ReadCapacityUnits'] == 0
        assert gsi['ProvisionedThroughput']['WriteCapacityUnits'] == 0
        if not one_request:
            table.meta.client.update_table(TableName=table.name, **create_gsi2)
        # DynamoDB cannot delete the table while the new GSI is being built.
        wait_for_gsi(table, 'gsi2')

# DynamoDB also accepts a switch to PAY_PER_REQUEST together with a GSI Delete.
# The view of that GSI, which the same request drops, needs no update.
def test_update_table_switch_to_pay_per_request_with_gsi_delete(dynamodb):
    with new_test_table(dynamodb, **PROVISIONED_TABLE_WITH_GSI) as table:
        table.meta.client.update_table(TableName=table.name, BillingMode='PAY_PER_REQUEST',
            GlobalSecondaryIndexUpdates=[{'Delete': {'IndexName': 'gsi1'}}])
        # DynamoDB applies the change asynchronously.
        wait_for_gsi_gone(table, 'gsi1')
        got = table.meta.client.describe_table(TableName=table.name)['Table']
        assert got['BillingModeSummary']['BillingMode'] == 'PAY_PER_REQUEST'
        assert got['ProvisionedThroughput']['ReadCapacityUnits'] == 0
        assert got['ProvisionedThroughput']['WriteCapacityUnits'] == 0
        assert not got.get('GlobalSecondaryIndexes')

# DynamoDB's CreateTable response reports each GSI's ProvisionedThroughput -
# zeros for a PAY_PER_REQUEST table - but, unlike its DescribeTable response,
# carries no WarmThroughput for a table which configured none. Verified against
# DynamoDB.
def test_gsi_create_table_response(dynamodb):
    name = unique_table_name()
    got = dynamodb.meta.client.create_table(TableName=name,
        KeySchema=[{'AttributeName': 'p', 'KeyType': 'HASH'}],
        AttributeDefinitions=[
            {'AttributeName': 'p', 'AttributeType': 'S'},
            {'AttributeName': 'x', 'AttributeType': 'S'}],
        BillingMode='PAY_PER_REQUEST',
        GlobalSecondaryIndexes=[{
            'IndexName': 'gsi',
            'KeySchema': [{'AttributeName': 'x', 'KeyType': 'HASH'}],
            'Projection': {'ProjectionType': 'ALL'}}])['TableDescription']
    try:
        gsi = got['GlobalSecondaryIndexes'][0]
        assert gsi['ProvisionedThroughput']['ReadCapacityUnits'] == 0
        assert gsi['ProvisionedThroughput']['WriteCapacityUnits'] == 0
        assert gsi['ProvisionedThroughput']['NumberOfDecreasesToday'] == 0
        assert not 'WarmThroughput' in got
        assert not 'WarmThroughput' in gsi
    finally:
        # Unlike Alternator's, DynamoDB's CreateTable is asynchronous, and a
        # table cannot be deleted until it and its GSI finish being created.
        wait_for_gsi(dynamodb.Table(name), 'gsi')
        dynamodb.meta.client.delete_table(TableName=name)
