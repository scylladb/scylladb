#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import pytest

@pytest.mark.asyncio
async def test_opensearch_basic(opensearch):

    index_name = 'test-index'
    index_body = {
        'settings': {
            'index': {
            'number_of_shards': 4
            }
        }
    }

    response = opensearch.indices.create(index_name, body=index_body)
    print(f"Index creation response: {response}")

    response = opensearch.cat.indices(format='json')
    for index in response:
        print(index['index'])
