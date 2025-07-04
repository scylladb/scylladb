# -*- coding: utf-8 -*-
# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

from cassandra.cluster import NoHostAvailable
import concurrent.futures
from contextlib import contextmanager
from test.cqlpy.util import cql_session

@contextmanager
def cql_bad_protocol(host_str, port, creds):
    try:
        # Use the default superuser credentials, which work for both Scylla and Cassandra
        with cql_session(
                host=host_str,
                port=port,
                is_ssl=creds["ssl"],
                username=creds["username"],
                password=creds["password"],
                protocol_version=5,
        ) as session:
            yield session
            session.shutdown()
    except NoHostAvailable:
        yield None

def try_connect(args):
    host_str, port, creds = args
    with cql_bad_protocol(host_str, port, creds) as session:
        return 1 if session else 0

def protocol_version_mismatch(request, host, max_attempts, num_parallel):
    count = 0

    host_str = host
    port = request.config.getoption("--port")
    creds = {
        "ssl": request.config.getoption("--ssl"),
        "username": request.config.getoption("--auth_username") or "cassandra",
        "password": request.config.getoption("--auth_password") or "cassandra",
    }

    args = [(host_str, port, creds)] * max_attempts
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_parallel) as executor:
        results = list(executor.map(try_connect, args))
        count = sum(results)

    return count

def test_protocol_version_mismatch(scylla_only, request, host):
    successful_session_count = protocol_version_mismatch(request, host, max_attempts=10, num_parallel=2)
    assert successful_session_count == 0
