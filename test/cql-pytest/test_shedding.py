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


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (p int primary key, t1 text, t2 text, t3 text, t4 text, t5 text, t6 text)")
    yield table
    cql.execute("DROP TABLE " + table)

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
#
# Have no idea why, but on CI the actual memory limit for CQL requests is ~100MiB,
# so we use ~60MiB request (which is estimated to take ~120MiB according to the logic above)
# to guarantee shedding.
def test_shed_too_large_request(cql, table1, scylla_only):
    def get_protocol_errors():
        result = 0
        for line in requests.get(f'http://{cql.cluster.contact_points[0]}:9180/metrics').text.split('\n'):
            if not line.startswith('scylla_transport_cql_errors_total') or 'protocol_error' not in line:
                continue
            result += int(line.split(' ')[1])
        return result

    # protocol_errors metric is always non-zero, since the
    # cassandra python driver use these errors to negotiate the protocol version
    protocol_errors_before = get_protocol_errors()

    # separate session is needed due to the cql_test_connection fixture,
    # which checks that the session is not broken at the end of the test
    with new_cql(cql) as ncql:
        prepared = ncql.prepare(f"INSERT INTO {table1} (p,t1,t2,t3,t4,t5,t6) VALUES (42,?,?,?,?,?,?)")

        # With release builds of Scylla, information that the socket is closed reaches the client driver
        # before it has time to process the error message written by Scylla to the socket.
        # The driver ignores unread bytes from the socket (this looks like a bug),
        # tries to establish a connection with another node, and throws a NoHostAvailable exception if it fails.
        # In the debug builds, the driver can have time to grab the error from the socket,
        # and we get InvalidRequest exception.
        with pytest.raises((NoHostAvailable, InvalidRequest),
                           match="request size too large|Unable to complete the operation against any hosts"):
            a_5mb_string = 'x'*10*1024*1024
            ncql.execute(prepared, [a_5mb_string]*6)
    protocol_errors_after = get_protocol_errors()
    assert protocol_errors_after == protocol_errors_before
    cql.execute(prepared, ["small_string"]*6)
    res = [row for row in cql.execute(f"SELECT p, t3 FROM {table1}")]
    assert len(res) == 1 and res[0].p == 42 and res[0].t3 == "small_string"


