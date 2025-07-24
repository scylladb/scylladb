#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import pytest
from test.nodetool.rest_api_mock import expected_request


@pytest.mark.parametrize("num_endpoints", [1, 2])
@pytest.mark.parametrize("key_delimiter", [None, '|'])
def test_getendpoints(nodetool, num_endpoints, key_delimiter):
    keyspace = 'ks'
    table = 'cf0'
    key = '42'
    endpoints = [f"127.0.0.{i}" for i in range(num_endpoints)]

    params = {"cf": table, "key": key}
    if key_delimiter is not None:
        params["key_delimiter"] = key_delimiter

    expected = expected_request(
        "GET",
        f"/storage_service/natural_endpoints/{keyspace}",
        params=params,
        response=endpoints,
    )

    args = ["getendpoints", keyspace, table, key]
    if key_delimiter is not None:
        args.append(key_delimiter)

    res = nodetool(*args, expected_requests=[expected])
    actual_output = res.stdout
    expected_output = ''.join(f"{endpoint}\n" for endpoint in endpoints)

    assert actual_output == expected_output
