#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

HASH_KEY_NAME = "hash_key"
RANGE_KEY_NAME = "range_key"

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
