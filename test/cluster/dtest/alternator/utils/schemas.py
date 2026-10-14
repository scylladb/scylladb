#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

HASH_KEY_NAME = "hash_key"
RANGE_KEY_NAME = "range_key"
LSI_KEY_NAME = "lsi_key"
LSI_INDEX_NAME = "lsi_index"
GSI_KEY_NAME = "gsi_key"
GSI_INDEX_NAME = "gsi_index"

HASH_SCHEMA = tuple(dict(
    KeySchema=[
        {'AttributeName': HASH_KEY_NAME, 'KeyType': 'HASH'},
    ],
    AttributeDefinitions=[
        {'AttributeName': HASH_KEY_NAME, 'AttributeType': 'S'},
    ]
).items())

HASH_AND_NUM_RANGE_SCHEMA = tuple(dict(
    KeySchema=[
        {'AttributeName': HASH_KEY_NAME, 'KeyType': 'HASH'},
        {'AttributeName': RANGE_KEY_NAME, 'KeyType': 'RANGE'}],
    AttributeDefinitions=[
        {'AttributeName': HASH_KEY_NAME, 'AttributeType': 'S'},
        {'AttributeName': RANGE_KEY_NAME, 'AttributeType': 'N'}]
).items())


HASH_AND_STR_RANGE_SCHEMA = tuple(dict(
    KeySchema=[
        {'AttributeName': HASH_KEY_NAME, 'KeyType': 'HASH'},
        {'AttributeName': RANGE_KEY_NAME, 'KeyType': 'RANGE'},
    ],
    AttributeDefinitions=[
        {'AttributeName': HASH_KEY_NAME, 'AttributeType': 'S'},
        {'AttributeName': RANGE_KEY_NAME, 'AttributeType': 'S'},
    ]
).items())

HASH_AND_BINARY_RANGE_SCHEMA = tuple(dict(
    KeySchema=[
        {'AttributeName': HASH_KEY_NAME, 'KeyType': 'HASH'},
        {'AttributeName': RANGE_KEY_NAME, 'KeyType': 'RANGE'},
    ],
    AttributeDefinitions=[
        {'AttributeName': HASH_KEY_NAME, 'AttributeType': 'S'},
        {'AttributeName': RANGE_KEY_NAME, 'AttributeType': 'B'},
    ]
).items())

CONDITION_EXPRESSION_SCHEMA = tuple(dict(
    KeySchema=[{'AttributeName': 'pk', 'KeyType': 'HASH'}, {'AttributeName': 'c', 'KeyType': 'RANGE'}],
    AttributeDefinitions=[{'AttributeName': 'pk', 'AttributeType': 'S'}, {'AttributeName': 'c', 'AttributeType': 'N'}]
).items())

HASH_AND_STR_RANGE_GSI_LSI_SCHEMA = tuple(dict(
    KeySchema=[
        {'AttributeName': HASH_KEY_NAME, 'KeyType': 'HASH'},
        {'AttributeName': RANGE_KEY_NAME, 'KeyType': 'RANGE'},
    ],
    AttributeDefinitions=[
        {'AttributeName': HASH_KEY_NAME, 'AttributeType': 'S'},
        {'AttributeName': RANGE_KEY_NAME, 'AttributeType': 'S'},
        {'AttributeName': LSI_KEY_NAME, 'AttributeType': 'S'},
        {'AttributeName': GSI_KEY_NAME, 'AttributeType': 'S'},
    ],
    LocalSecondaryIndexes=[
        {
            'IndexName': LSI_INDEX_NAME,
            'KeySchema': [
                {'AttributeName': HASH_KEY_NAME, 'KeyType': 'HASH'},
                {'AttributeName': LSI_KEY_NAME, 'KeyType': 'RANGE'},
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        }
    ],
    GlobalSecondaryIndexes=[
        {
            'IndexName': GSI_INDEX_NAME,
            'KeySchema': [
                {'AttributeName': GSI_KEY_NAME, 'KeyType': 'HASH'}
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        }
    ]
).items())
