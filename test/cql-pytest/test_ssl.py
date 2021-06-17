# -*- coding: utf-8 -*-
# Copyright 2021-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

#############################################################################
# Tests for CQL over SSL (TLS). These tests are skipped when the tests are
# *not* using SSL, so run the tests with "--ssl" to enable them.
#############################################################################

import pytest

import ssl
import cassandra.cluster

# Test that TLS 1.2 is supported (because this is what "cqlsh --ssl" uses
# by default), and that other TLS version are either supported - or if
# disallowed must result in the expected error message and not in some
# mysterious disconnection. Reproduces #8827.
def test_tls_versions(cql):
    # To reduce code duplication, we let conftest.py set up 'cql', and then
    # learn from cql.cluster whether SSL is used, and if so which contact
    # points, ports, and other parameters, we should use to connect.
    if not cql.cluster.ssl_context:
        pytest.skip("SSL-specific tests are skipped without the '--ssl' option")

    # TLS v1.2 must be supported, because this is the default version that
    # "cqlsh --ssl" uses. If this fact changes in the future, we may need
    # to reconsider this test.
    try_connect(cql.cluster, ssl.TLSVersion.TLSv1_2)
    print(f"{ssl.TLSVersion.TLSv1_2} supported")

    # All other protocol versions should either work (if Scylla is configured
    # to allow them) or fail with the expected error message.
    for ssl_version in [ssl.TLSVersion.TLSv1_3,
                        ssl.TLSVersion.TLSv1_1,
                        ssl.TLSVersion.TLSv1,
                        ssl.TLSVersion.SSLv3]:
        try:
            try_connect(cql.cluster, ssl_version)
            print(f"{ssl_version} supported")
        except cassandra.cluster.NoHostAvailable as e:
            # For the various TLS versions, we get the new TLS alert
            # "protocol version". But for SSL, we get the older
            # "no protocols available" error.
            assert 'protocol version' in str(e) or 'no protocols available' in str(e)
            print(f"{ssl_version} not supported")

def try_connect(orig_cluster, ssl_version):
    ssl_context=ssl.SSLContext(ssl.PROTOCOL_TLS)
    ssl_context.minimum_version = ssl_version
    ssl_context.maximum_version = ssl_version
    cluster = cassandra.cluster.Cluster(
        contact_points=orig_cluster.contact_points,
        port=orig_cluster.port,
        protocol_version=orig_cluster.protocol_version,
        auth_provider=orig_cluster.auth_provider,
        ssl_context=ssl_context)
    cluster.connect()
    cluster.shutdown()
