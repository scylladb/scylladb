# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later


# This file provides utility to REST API requests.
# Some metrics cannot be obtained by nodetool, but they are available by API.

import requests
import nodetool
import pytest
from contextlib import contextmanager

# Sends GET request to REST API. Response is returned as JSON.
# If API isn't available, `pytest.skip()` is called.
def get_request(cql, *path):
    if nodetool.has_rest_api(cql):
        response = requests.get(f"{nodetool.rest_api_url(cql)}/{'/'.join(path)}")
        return response.json()
    else:
        pytest.skip("REST API not available")

# Sends POST request to REST API. Response is returned as JSON or None
# if the response body was empty (this is typical).
# If API isn't available, `pytest.skip()` is called.
def post_request(cql, *path):
    if nodetool.has_rest_api(cql):
        response = requests.post(f"{nodetool.rest_api_url(cql)}/{'/'.join(path)}")
        if not response.text:
            return None
        return response.json()
    else:
        pytest.skip("REST API not available")

# Sends DELETE request to REST API. Response is returned as JSON or None
# if the response body was empty (this is typical).
# If API isn't available, `pytest.skip()` is called.
def delete_request(cql, *path):
    if nodetool.has_rest_api(cql):
        response = requests.delete(f"{nodetool.rest_api_url(cql)}/{'/'.join(path)}")
        if not response.text:
            return None
        return response.json()
    else:
        pytest.skip("REST API not available")


# Get column family's metric.
# metric - name of metric
# table - (optional) column family name to add to request's path
#         Expected format: `keyspace.name`
def get_column_family_metric(cql, metric, table=None):
    args = ["column_family", "metrics", metric]
    if table != None:
        ks, cf = table.split('.')
        args.append(f"{ks}:{cf}")
        
    return get_request(cql, *args)

# scylla_inject_error() is a context manager, running a block of code with
# the given error injection enabled - and automatically disabling the
# injection when the block exits.
# This error-injection feature uses Scylla's REST API, so it only works on
# Scylla. Also, only works in specific build modes (dev, debug, sanitize).
# When Cassandra or non-supporting build of Scylla is being tested, using
# this function will cause the calling test to be skipped.
@contextmanager
def scylla_inject_error(cql, err, one_shot=False):
    post_request(cql, f'v2/error_injection/injection/{err}?one_shot={one_shot}')
    response = get_request(cql, f'v2/error_injection/injection')
    print("Enabled error injections:", response)
    if not err in response:
        pytest.skip("Error injection not enabled in Scylla - try compiling in dev/debug/sanitize mode")
    try:
        yield
    finally:
        print("Disabling error injection", err)
        delete_request(cql, f'v2/error_injection/injection/{err}')
