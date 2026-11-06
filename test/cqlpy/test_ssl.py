# -*- coding: utf-8 -*-
# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for CQL over SSL (TLS). test.py always runs them over TLS; with
# test/cqlpy/run they are skipped unless "--ssl" is given.
#############################################################################

import pytest

import cassandra.cluster
from contextlib import contextmanager
import signal
import ssl
import time

from test import TEST_RUNNER
from test.pylib.driver_utils import normalize_cipher, safe_driver_shutdown
from test.pylib.skip_types import skip_env
from .util import cql_session


if TEST_RUNNER != "runpy":
    # Under test.py, serve CQL over TLS on an extra port; the framework's
    # readiness check needs the plain one. All four ports must be explicit,
    # or the shard-aware port inherits encryption.
    _TLS_PORT = 9142

    @pytest.fixture(scope="module")
    async def scylla_cluster(request, testpy_cluster_factory, testpy_uname):
        async with testpy_cluster_factory(request.node, testpy_uname) as cluster:
            await cluster.add_server(config={
                'native_transport_port': 9042,
                'native_shard_aware_transport_port': 19042,
                'native_transport_port_ssl': _TLS_PORT,
                'native_shard_aware_transport_port_ssl': 19142,
                'client_encryption_options': {
                    'enabled': True,
                    'certificate': 'conf/scylla.crt',
                    'keyfile': 'conf/scylla.key',
                },
            })
            cluster.take_log_savepoint()
            yield cluster
            processes = [(server, server.cmd) for server in cluster.running.values() if server.cmd]

        # Same server-crash check as test/pylib/runner.py's scylla_cluster.
        if exits := [f"{server} exited with status code {process.returncode}" for server, process in processes
                     if process.returncode not in (0, -signal.SIGKILL)]:
            raise RuntimeError("; ".join(exits))

    @pytest.fixture(scope="module")
    def cql(host):
        try:
            with cql_session(host=host, port=_TLS_PORT, is_ssl=True,
                             username="cassandra", password="cassandra") as session:
                yield session
                session.shutdown()
        except cassandra.cluster.NoHostAvailable:
            # Like conftest's cql: one clear exit instead of every test failing.
            pytest.exit(f"Cannot connect to Scylla over TLS at {host}:{_TLS_PORT}", returncode=pytest.ExitCode.INTERNAL_ERROR)


# Test that TLS 1.2 is supported (because this is what "cqlsh --ssl" uses
# by default), and that other TLS version are either supported - or if
# disallowed must result in the expected error message and not in some
# mysterious disconnection. Reproduces #8827.
def test_tls_versions(cql):
    # To reduce code duplication, we let conftest.py set up 'cql', and then
    # learn from cql.cluster whether SSL is used, and if so which contact
    # points, ports, and other parameters, we should use to connect.
    if not cql.cluster.ssl_context:
        skip_env("SSL-specific tests are skipped without the '--ssl' option")

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
        table_result = cql.execute(f"SELECT * FROM system.clients")
        for row in table_result:
            assert not row.ssl_enabled
            assert row.ssl_protocol is None
            assert row.ssl_cipher_suite is None
    else:
        # TLS v1.2 must be supported, because this is the default version that
        # "cqlsh --ssl" uses. If this fact changes in the future, we may need
        # to reconsider this test.
        with try_connect(cql.cluster, ssl.TLSVersion.TLSv1_2) as session:
            # As of time of writing, python driver spawns 5 to 6 connections for this single session,
            # and some connections may already be past the TLS init phase, while others not, when we query system.clients,
            # so we need to retry until all connections are initialized and have their TLS info recorded in system.clients,
            # otherwise we'd end up with some connections e.g. having their ssl_enabled=True but other fields still None.
            # system.clients also lists the connections of the test's main
            # "cql" session, which doesn't pin the TLS version and may
            # negotiate TLS 1.3. So every connection should have its TLS info
            # recorded with TLS 1.2 or 1.3, and at least one of them (our
            # session's) should be TLS 1.2.
            expected_ciphers = [normalize_cipher(cipher['name']) for cipher in ssl.create_default_context().get_ciphers()]
            deadline = time.time() + 10  # 10 seconds timeout
            while time.time() < deadline:
                # Under test.py the framework also holds plain-text connections; check only this session's.
                ports = {c._socket.getsockname()[1]
                         for h in session.cluster.get_connection_holders()
                         for c in h.get_connections() if c._socket}
                rows = [row for row in session.execute("SELECT * FROM system.clients") if row.port in ports]
                if rows and all(
                    row.ssl_enabled
                    and row.ssl_protocol in ('TLS1.2', 'TLS1.3')
                    and normalize_cipher(row.ssl_cipher_suite) in expected_ciphers
                    for row in rows
                ) and any(row.ssl_protocol == 'TLS1.2' for row in rows):
                    return
                time.sleep(0.01)
            pytest.fail(f"Not all connections have TLS data set correctly in system.clients after 10 seconds")


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
        safe_driver_shutdown(cluster)

# Test that if we try to connect to an SSL port with *unencrypted* CQL,
# it doesn't work.
# Note that Cassandra can be configured (with "optional: true") to allow both
# SSL and non-SSL on the same port. But Scylla doesn't support this, and
# Cassandra also won't if configured with the recommended (but not default)
# "optional: false" - as we do in the run-cassandra script.
def test_non_tls_on_tls(cql):
    if not cql.cluster.ssl_context:
        skip_env("SSL-specific tests are skipped without the '--ssl' option")
    # Copy the configuration of the existing "cql", just not the ssl_context
    cluster = cassandra.cluster.Cluster(
        contact_points=cql.cluster.contact_points,
        port=cql.cluster.port,
        protocol_version=cql.cluster.protocol_version,
        auth_provider=cql.cluster.auth_provider)
    try:
        with pytest.raises(cassandra.cluster.NoHostAvailable, match="ProtocolError"):
            cluster.connect()
    finally:
        safe_driver_shutdown(cluster)
