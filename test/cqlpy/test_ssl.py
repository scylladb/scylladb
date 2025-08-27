# -*- coding: utf-8 -*-
# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Tests for CQL over SSL (TLS). These tests are skipped when the tests are
# *not* using SSL, so run the tests with "--ssl" to enable them.
#############################################################################

import pytest

import cassandra.cluster
from contextlib import contextmanager
import re
import ssl


# This function normalizes the SSL cipher suite name (a string),
# which we need to do because tests use python library and scylla server uses C library,
# and both the python's and the C's library naming conventions are different,
# so we need some translation in order to compare them.
def normalize_cipher(cipher_name: str) -> str:
    if cipher_name.startswith("TLS_"):
        cipher_name = cipher_name[len("TLS_"):] # Remove leading "TLS_" if present.
    cipher_name = cipher_name.replace("_WITH_", "-")
    cipher_name = cipher_name.replace("_", "-")
    # Remove hyphen between letters and digits: e.g. convert "AES-256" to "AES256"
    cipher_name = re.sub(r'([A-Z]+)-(\d+)', r'\1\2', cipher_name)
    return cipher_name


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

# a regression test for #9216
def test_system_clients_stores_tls_info(cql):
    if not cql.cluster.ssl_context:
        pytest.skip("SSL-specific tests are skipped without the '--ssl' option")

    # TLS v1.2 must be supported, because this is the default version that
    # "cqlsh --ssl" uses. If this fact changes in the future, we may need
    # to reconsider this test.
    with try_connect(cql.cluster, ssl.TLSVersion.TLSv1_2) as session:
        table_result = session.execute(f"SELECT * FROM system.clients")
        print(f"system.clients has {len(table_result.current_rows)} rows")
        rows_count = len(table_result.current_rows)
        for row in table_result:
            assert row.ssl_enabled
            assert row.hostname == '127.0.0.1'
            assert row.ssl_protocol == 'TLS1.2', f"system.clients has {len(table_result.current_rows)} rows"
            expected_ciphers = [normalize_cipher(cipher['name']) for cipher in ssl.create_default_context().get_ciphers()]
            actual_cipher = normalize_cipher(row.ssl_cipher_suite)
            assert actual_cipher in expected_ciphers


@contextmanager
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
    try:
        session = cluster.connect()
        yield session
    finally:
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
