# -*- coding: utf-8 -*-
# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

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
        ssl_context=ssl_context,
        # The default timeout for new connections is 5 seconds, and for
        # requests made by the control connection is 2 seconds. These should
        # have been more than enough, but in some extreme cases with a very
        # slow debug build running on a very busy machine, they may not be.
        # so let's increase them to 60 seconds. See issue #11289.
        connect_timeout = 60,
        control_connection_timeout = 60)
    cluster.connect()
    cluster.shutdown()

# Test that if we try to connect to an SSL port with *unencrypted* CQL,
# it doesn't work.
# Note that Cassandra can be configured (with "optional: true") to allow both
# SSL and non-SSL on the same port. But Scylla doesn't support this, and
# Cassandra also won't if configured with the recommended (but not default)
# "optional: false" - as we do in the run-cassandra script.
def test_non_tls_on_tls(cql):
    if not cql.cluster.ssl_context:
        pytest.skip("SSL-specific tests are skipped without the '--ssl' option")
    # Copy the configuration of the existing "cql", just not the ssl_context
    cluster = cassandra.cluster.Cluster(
        contact_points=cql.cluster.contact_points,
        port=cql.cluster.port,
        protocol_version=cql.cluster.protocol_version,
        auth_provider=cql.cluster.auth_provider)
    with pytest.raises(cassandra.cluster.NoHostAvailable, match="ProtocolError"):
        cluster.connect()
    cluster.shutdown() # can't be reached
