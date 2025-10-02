#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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

@pytest.mark.parametrize("num_endpoints", [1, 2])
@pytest.mark.parametrize("key_components", [[("--key-components", "part1")],
                                   [("--key-components", "part1"), ("--key-components", "part2")]])
def test_getendpoints_key_components_param(nodetool, num_endpoints, key_components):
    keyspace = 'ks'
    table = 'cf0'
    endpoints = [f"127.0.0.{i}" for i in range(num_endpoints)]
    key_components_values = []
    args = [keyspace, table]
    for arg, value in key_components:
        key_components_values.append(value)
        args += [arg, value]
    key_component_param = key_components_values if len(key_components_values) > 1 else key_components_values[0]
    res = nodetool("getendpoints",  *args, expected_requests=[
        expected_request("GET", f"/storage_service/natural_endpoints/v2/{keyspace}",
                         params={"cf": table, "key_component": key_component_param},
                         response=endpoints),
    ])
    actual_output = res.stdout

    expected_output = ''.join(f"{endpoint}\n" for endpoint in endpoints)
    assert actual_output == expected_output
