# Copyright 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0


# This file provides utility to REST API requests.
# Some metrics cannot be obtained by nodetool, but they are available by API.

import requests
from . import nodetool
import pytest
import threading
import time
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

# Waits until a fiber enters the given one-shot error injection. A one-shot
# injection reports itself as disabled on the shard which entered it, and it
# is entered before the injected code parks, so once some shard reports the
# injection as disabled the injected code is running (or about to park).
def wait_for_injection_enter(cql, err, timeout=60):
    tick = threading.Event()
    deadline = time.monotonic() + timeout
    while all(shard['enabled'] for shard in get_request(cql, f'v2/error_injection/injection/{err}')):
        assert time.monotonic() < deadline, f'timed out waiting for injection {err}'
        tick.wait(0.01)

# Sends a message to a fiber paused on the given error injection, resuming it.
def message_injection(cql, err):
    post_request(cql, f'v2/error_injection/injection/{err}/message')
