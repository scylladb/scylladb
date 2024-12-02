# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for the shedding mechanisms in the CQL layer
#############################################################################

import pytest
from cassandra.cluster import NoHostAvailable
from cassandra.protocol import InvalidRequest
from util import unique_name, new_cql, ScyllaMetrics
from contextlib import contextmanager


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int primary key, t text)")
    yield table
    cql.execute("DROP TABLE " + table)


@contextmanager
def disable_compression():
    import cassandra.connection
    from collections import OrderedDict

    saved = cassandra.connection.locally_supported_compressions
    cassandra.connection.locally_supported_compressions = OrderedDict()
    try:
        yield
    finally:
        cassandra.connection.locally_supported_compressions = saved

# When a too large request comes, it should be rejected in full.
# That means that first of all a client receives an error after sending
# such a request, but also that following correct requests can be successfully
# processed.
# This test depends on the current configuration. The assumptions are:
# 1. Scylla has 1GB memory total
# 2. The memory is split among 2 shards
# 3. Total memory reserved for CQL requests is 10% of the total - 50MiB
# 4. The memory estimate for a request is 2*(raw size) + 8KiB
# 5. Hence, a 30MiB request will be estimated to take around 60MiB RAM,
#    which is enough to trigger shedding.
# See also #8193.
#
# We check that there are no unexpected protocol_errors using Scylla Prometheus API.
# Such errors occurred before, when before closing the connection, the remaining
# bytes of the current request were not read to the end and were treated as
# the beginning of the next request.
def test_shed_too_large_request(cql, table1, scylla_only):
    def get_protocol_errors(metrics):
        return metrics.get('scylla_transport_cql_errors_total', {'type': 'protocol_error'})

    initial_metrics = ScyllaMetrics.query(cql)

    # See comments above
    expected_shard_count = 2
    expected_limit = initial_metrics.get('scylla_memory_total_memory') / expected_shard_count / 10
    request_size_limit = (expected_limit - 8 * 1024) / 2
    request_size = int(request_size_limit + 1024)

    # disable compression since we rely on specific request size
    with disable_compression():
        # Big request causes the entire connection to be closed (see #8800 for the reasons).
        # For some reason the driver doesn't try to reopen the connection to the same host,
        # so we end up with a broken session and cql_test_connection fixture fails the test.
        # We create a separate session to make the test work.
        with new_cql(cql) as ncql:
            prepared = ncql.prepare(f"INSERT INTO {table1} (p,t) VALUES (42,?)")

            # With release builds of Scylla, information that the socket is closed reaches the client driver
            # before it has time to process the error message written by Scylla to the socket.
            # The driver ignores unread bytes from the socket (this looks like a bug),
            # tries to establish a connection with another node, and throws a NoHostAvailable exception if it fails.
            # In the debug builds, the driver can have time to grab the error from the socket,
            # and we get InvalidRequest exception.
            with pytest.raises((NoHostAvailable, InvalidRequest),
                               match="request size too large|Unable to complete the operation against any hosts"):
                ncql.execute(prepared, ['x'*request_size])
    current_metrics = ScyllaMetrics.query(cql)
    # protocol_errors metric is always non-zero, since the
    # cassandra python driver use these errors to negotiate the protocol version
    assert get_protocol_errors(current_metrics) == get_protocol_errors(initial_metrics)

    cql.execute(prepared, ["small_string"])
    res = [row for row in cql.execute(f"SELECT p, t FROM {table1}")]
    assert len(res) == 1 and res[0].p == 42 and res[0].t == "small_string"
