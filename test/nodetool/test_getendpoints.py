#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest
from test.nodetool.rest_api_mock import expected_request


@pytest.mark.parametrize("num_endpoints", [1, 2])
def test_getendpoints(nodetool, num_endpoints):
    keyspace = 'ks'
    table = 'cf0'
    key = '42'
    endpoints = [f"127.0.0.{i}" for i in range(num_endpoints)]
    res = nodetool("getendpoints", keyspace, table, key, expected_requests=[
        expected_request("GET", f"/storage_service/natural_endpoints/{keyspace}",
                         params={"cf": table, "key": key},
                         response=endpoints),
    ])
    actual_output = res.stdout

    expected_output = ''.join(f"{endpoint}\n" for endpoint in endpoints)
    assert actual_output == expected_output
