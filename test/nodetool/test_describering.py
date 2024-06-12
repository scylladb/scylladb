#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.nodetool.rest_api_mock import expected_request

def test_describering(nodetool):
    schema_version = "83541f18-c1bc-11ee-b55e-4d563ca8da4b"
    ring = [
            {
                "start_token": "-9153143965931359657",
                "end_token": "-9105705820509211664",
                "endpoints": [
                    "127.0.0.1",
                    "127.0.0.2"
                ],
                "rpc_endpoints": [
                    "127.0.0.1",
                    "127.0.0.2"
                ],
                "endpoint_details": [
                    {
                        "host": "127.0.0.1",
                        "datacenter": "datacenter1",
                        "rack": "rack1"
                    },
                    {
                        "host": "127.0.0.2",
                        "datacenter": "datacenter2",
                        "rack": "rack2"
                    }
                ]
            },
            {
                "start_token": "9213626581013704850",
                "end_token": "-9153143965931359657",
                "endpoints": [
                    "127.0.0.1"
                ],
                "rpc_endpoints": [
                    "127.0.0.1"
                ],
                "endpoint_details": [
                    {
                        "host": "127.0.0.1",
                        "datacenter": "datacenter1",
                        "rack": "rack1"
                    }
                ]
            }
    ]
    res = nodetool("describering", "ks", expected_requests=[
        expected_request("GET", "/storage_service/schema_version", response=schema_version),
        expected_request("GET", "/storage_service/describe_ring/ks", response=ring)])
    assert res.stdout == f"""Schema Version:{schema_version}
TokenRange: 
\tTokenRange(start_token:-9153143965931359657, end_token:-9105705820509211664, endpoints:[127.0.0.1, 127.0.0.2], rpc_endpoints:[127.0.0.1, 127.0.0.2], endpoint_details:[EndpointDetails(host:127.0.0.1, datacenter:datacenter1, rack:rack1), EndpointDetails(host:127.0.0.2, datacenter:datacenter2, rack:rack2)])
\tTokenRange(start_token:9213626581013704850, end_token:-9153143965931359657, endpoints:[127.0.0.1], rpc_endpoints:[127.0.0.1], endpoint_details:[EndpointDetails(host:127.0.0.1, datacenter:datacenter1, rack:rack1)])
"""

def test_describering_table(nodetool, scylla_only):
    schema_version = "83541f12-c1bc-11ee-b55e-4d563ca8da4b"
    ring = [
            {
                "start_token": "9213626581013704850",
                "end_token": "-9153143965931359657",
                "endpoints": [
                    "127.0.0.1"
                ],
                "rpc_endpoints": [
                    "127.0.0.1"
                ],
                "endpoint_details": [
                    {
                        "host": "127.0.0.1",
                        "datacenter": "datacenter1",
                        "rack": "rack1"
                    }
                ]
            }
    ]
    res = nodetool("describering", "ks", "tbl", expected_requests=[
        expected_request("GET", "/storage_service/schema_version", response=schema_version),
        expected_request("GET", "/storage_service/describe_ring/ks", params={"table": "tbl"}, response=ring)])
    assert res.stdout == f"""Schema Version:{schema_version}
TokenRange: 
\tTokenRange(start_token:9213626581013704850, end_token:-9153143965931359657, endpoints:[127.0.0.1], rpc_endpoints:[127.0.0.1], endpoint_details:[EndpointDetails(host:127.0.0.1, datacenter:datacenter1, rack:rack1)])
"""
