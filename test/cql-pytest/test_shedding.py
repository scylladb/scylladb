# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Tests for the shedding mechanisms in the CQL layer
#############################################################################

import pytest
from cassandra.cluster import NoHostAvailable
from cassandra.protocol import InvalidRequest
from util import unique_name, new_cql
import requests
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


class ScyllaMetrics:
    def __init__(self, lines):
        self._lines = lines
    @staticmethod
    def query(cql):
        url = f'http://{cql.cluster.contact_points[0]}:9180/metrics'
        return ScyllaMetrics(requests.get(url).text.split('\n'))
    def get(self, name, labels = None, shard='total'):
        result = None
        for l in self._lines:
            if not l.startswith(name):
                continue
            labels_start = l.find('{')
            labels_finish = l.find('}')
            if labels_start == -1 or labels_finish == -1:
                raise ValueError(f'invalid metric format [{l}]')
            def match_kv(kv):
                key, val = kv.split('=')
                val = val.strip('"')
                return shard == 'total' or val == shard if key == 'shard' \
                    else labels is None or labels.get(key, None) == val
            match = all(match_kv(kv) for kv in l[labels_start + 1:labels_finish].split(','))
            if match:
                value = float(l[labels_finish + 2:])
                if result is None:
                    result = value
                else:
                    result += value
                if shard != 'total':
                    break
        return result


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
# Big request causes the entire connection to be closed (see #8800 for the reasons),
# this results in NoHostAvailable on the client.
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
        # separate session is needed due to the cql_test_connection fixture,
        # which checks that the session is not broken at the end of the test
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
