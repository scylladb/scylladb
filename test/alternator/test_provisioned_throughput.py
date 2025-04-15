# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Test for ProvisionedThroughput
# ProvisionedThroughput is part of a table definition
# The following tests make sure we can get, set and update its value

import pytest
from botocore.exceptions import ClientError
from decimal import Decimal
from test.alternator.util import random_string, random_bytes, new_test_table

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

# When creating a table with PAY_PER_REQUEST billing mode, RCU and WCU should be zero
def test_create_pay_per_request_units(test_table):
    got = test_table.meta.client.describe_table(TableName=test_table.name)['Table']
    assert got['BillingModeSummary']['BillingMode'] == 'PAY_PER_REQUEST'
    assert got['ProvisionedThroughput']['ReadCapacityUnits'] == 0
    assert got['ProvisionedThroughput']['WriteCapacityUnits'] == 0

