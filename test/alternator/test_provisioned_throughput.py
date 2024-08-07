# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# Test for ProvisionedThroughput
# ProvisionedThroughput is part of a table definition
# The following tests make sure we can get, set and update its value

import pytest
from botocore.exceptions import ClientError
from decimal import Decimal
from test.alternator.util import random_string, random_bytes, new_test_table

def test_create_table(dynamodb):
    KeySchema=[ { 'AttributeName': 'p123', 'KeyType': 'HASH' },
                    { 'AttributeName': 'c4567', 'KeyType': 'RANGE' }
        ]
    AttributeDefinitions=[
                { 'AttributeName': 'p123', 'AttributeType': 'S' },
                { 'AttributeName': 'c4567', 'AttributeType': 'S' },
    ]

    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
    with new_test_table(dynamodb,
                        KeySchema=KeySchema,
        AttributeDefinitions=AttributeDefinitions,
        BillingMode='PROVISIONED',
        ProvisionedThroughput=ProvisionedThroughput) as table:
        got = table.meta.client.describe_table(TableName=table.name)['Table']

        assert got['KeySchema'] == KeySchema
        assert sorted(got['AttributeDefinitions'], key=lambda x: x["AttributeName"]) == sorted(AttributeDefinitions, key=lambda x: x["AttributeName"])
        print(got)
        if 'BillingMode' in got:
            # BillingMode can be omitted
            assert got['BillingMode'] == 'PROVISIONED'
        assert got['ProvisionedThroughput']['ReadCapacityUnits'] == 5
        assert got['ProvisionedThroughput']['WriteCapacityUnits'] == 5
