#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest
from textwrap import indent
from uuid import uuid1
from test.nodetool.rest_api_mock import expected_request


def create_schema_version(num_hosts):
    key = uuid1()
    value = [f"127.0.0.{i}" for i in range(num_hosts)]
    return {"key": str(key), "value": value}


def normalize_cluster_info(cluster_info):
    # Cassandra's nodetool uses HashMap under the hood for collecting the map
    # from versions to hosts on that version, and print out them by iterating
    # the keys, but the order is not guaranteed to be ordered or consistent, so
    # let's extract the items in "schema_versions" out and sort them before
    # comparing.
    expecting_schema_versions = False
    normalized = []
    schema_versions = []
    for line in cluster_info.split('\n'):
        if expecting_schema_versions:
            schema_versions.append(line)
        else:
            if line.strip().startswith('Schema versions:'):
                expecting_schema_versions = True
            normalized.append(line)
    normalized += sorted(schema_versions)
    return normalized


@pytest.mark.parametrize("num_schema_versions", [1, 2])
@pytest.mark.parametrize("num_schema_versions_hosts", [1, 2])
def test_describecluster(nodetool, num_schema_versions, num_schema_versions_hosts):
    cluster_name = "Test Cluster"
    snitch_name = "org.apache.cassandra.locator.SimpleSnitch"
    partitioner = "org.apache.cassandra.dht.Murmur3Partitioner"
    dynamic_snitch = "disabled"
    schema_versions = [create_schema_version(num_schema_versions_hosts)
                       for _ in range(num_schema_versions)]

    schema_versions_fmt = ''
    for schema_version in schema_versions:
        version = schema_version["key"]
        hosts = ', '.join(schema_version["value"])
        schema_versions_fmt += f"{version}: [{hosts}]\n\n"
    schema_versions_fmt = indent(schema_versions_fmt, "\t")

    res = nodetool("describecluster", expected_requests=[
        expected_request("GET", "/storage_service/cluster_name", response=cluster_name),
        expected_request("GET", "/snitch/name", response=snitch_name),
        expected_request("GET", "/storage_service/partitioner_name", response=partitioner),
        expected_request("GET", "/storage_proxy/schema_versions", response=schema_versions),
    ])
    actual_output = res.stdout

    expected_cluster_info = indent(f"""\
Name: {cluster_name}
Snitch: {snitch_name}
DynamicEndPointSnitch: {dynamic_snitch}
Partitioner: {partitioner}
Schema versions:
{schema_versions_fmt}""", "\t")

    expected_output = f"""Cluster Information:
{expected_cluster_info}"""

    assert normalize_cluster_info(actual_output) == normalize_cluster_info(expected_output)
