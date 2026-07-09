#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Tests for CertificateOrPasswordAuthenticator that allows clients to
authenticate either with a client certificate, or with a username/password
on the same CQL port, combining the features of CertificateAuthenticator and
PasswordAuthenticator.

When the server's encryption options include a truststore and set
require_client_auth=optional, the server requests a client certificate during
the TLS handshake, but does not reject clients that do not present one. Still,
the CQL authenticator implementation needs to check whether a certificate
was presented and if not - fall back to the SASL exchange for username/password
authentication. This is exactly what CertificateOrPasswordAuthenticator
implements.

We want to test here that indeed, CertificateOrPasswordAuthenticator allows
both types of clients to connect to the same CQL port, and that the
authentication succeeds or fails as expected in each case.
"""

import os
import ssl
import logging
import pytest

from cassandra.cluster import Cluster, NoHostAvailable       # type: ignore
from cassandra.auth import PlainTextAuthProvider             # type: ignore
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT  # type: ignore
from cassandra.policies import WhiteListRoundRobinPolicy     # type: ignore

from test.pylib.manager_client import ManagerClient
from test.pylib.driver_utils import safe_driver_shutdown

logger = logging.getLogger(__name__)

def system(cmd):
    """Like os.system(), but asserts the command succeeded (exit code 0)."""
    assert os.system(cmd) == 0, f'Command failed: {cmd}'

# TLS CQL port used in the tests.  We also keep the default unencrypted port
# on 9042 because the test framework's manager connects without TLS and needs
# a plain port to reach the server.
_TLS_PORT = 9143


def _make_tls_cluster(host: str, port: int,
                      certfile: str | None = None,
                      keyfile: str | None = None,
                      auth_provider=None) -> Cluster:
    """Return a cassandra-driver Cluster object configured to connect over TLS
    and present a client certificate (certfile, keyfile) if provided.

    The server certificate is not verified, since Scylla uses a self-signed
    test certificate.
    """
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    if certfile:
        ssl_ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)

    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy([host]),
        request_timeout=30,
    )
    return Cluster(
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        contact_points=[host],
        port=port,
        ssl_context=ssl_ctx,
        auth_provider=auth_provider,
        protocol_version=4,
        connect_timeout=30,
        control_connection_timeout=30,
    )


def _gen_certs(tmp_path):
    """Generate a self-signed CA and a client certificate with CN=cassandra.

    Also generates a second CA (ca2) that is *not* trusted by the server, and
    a client certificate signed by it (client2.crt / client2.key).  Used to
    simulate a client presenting an untrusted certificate.
    """
    system(f'openssl genrsa 2048 > "{tmp_path}/ca.key" 2>/dev/null')
    system(f'openssl req -new -x509 -nodes -sha256 -days 365 '
           f'-subj "/CN=TestCA" -key "{tmp_path}/ca.key" '
           f'-out "{tmp_path}/ca.crt" 2>/dev/null')
    system(f'openssl genrsa 2048 > "{tmp_path}/client.key" 2>/dev/null')
    system(f'openssl req -new -sha256 -subj "/CN=cassandra" '
           f'-key "{tmp_path}/client.key" -out "{tmp_path}/client.csr" 2>/dev/null')
    system(f'openssl x509 -req -sha256 -days 365 '
           f'-in "{tmp_path}/client.csr" '
           f'-CA "{tmp_path}/ca.crt" -CAkey "{tmp_path}/ca.key" -CAcreateserial '
           f'-out "{tmp_path}/client.crt" 2>/dev/null')

    # Second (untrusted) CA and a client cert signed by it.
    system(f'openssl genrsa 2048 > "{tmp_path}/ca2.key" 2>/dev/null')
    system(f'openssl req -new -x509 -nodes -sha256 -days 365 '
           f'-subj "/CN=UntrustedCA" -key "{tmp_path}/ca2.key" '
           f'-out "{tmp_path}/ca2.crt" 2>/dev/null')
    system(f'openssl genrsa 2048 > "{tmp_path}/client2.key" 2>/dev/null')
    system(f'openssl req -new -sha256 -subj "/CN=cassandra" '
           f'-key "{tmp_path}/client2.key" -out "{tmp_path}/client2.csr" 2>/dev/null')
    system(f'openssl x509 -req -sha256 -days 365 '
           f'-in "{tmp_path}/client2.csr" '
           f'-CA "{tmp_path}/ca2.crt" -CAkey "{tmp_path}/ca2.key" -CAcreateserial '
           f'-out "{tmp_path}/client2.crt" 2>/dev/null')

    # Third client cert: trusted (signed by ca.crt), but subject has no CN field.
    # The auth_certificate_role_queries rule "CN=([^,]+)" will not match it.
    system(f'openssl genrsa 2048 > "{tmp_path}/client3.key" 2>/dev/null')
    system(f'openssl req -new -sha256 -subj "/O=TestOrg" '
           f'-key "{tmp_path}/client3.key" -out "{tmp_path}/client3.csr" 2>/dev/null')
    system(f'openssl x509 -req -sha256 -days 365 '
           f'-in "{tmp_path}/client3.csr" '
           f'-CA "{tmp_path}/ca.crt" -CAkey "{tmp_path}/ca.key" -CAcreateserial '
           f'-out "{tmp_path}/client3.crt" 2>/dev/null')


async def _start_server(manager, tmp_path):
    """Start a Scylla node with CertificateOrPasswordAuthenticator and TLS settings.

    native_transport_port_ssl adds the TLS port (9143) while keeping the default
    plain port (9042), which the test framework's manager requires since it connects
    without TLS.  All four ports (9042, 9143, 19042, 19142) must be set explicitly:
    without that, the shard-aware port (19042) inherits the encryption setting and
    becomes TLS-only, which makes the manager's plain driver hang on connection.
    On the plain port, CertificateOrPasswordAuthenticator sees no
    client certificate and falls back to SASL; we supply cassandra/cassandra
    credentials via driver_connect_opts.
    """
    servers = await manager.servers_add(1,
        config={
            'authenticator': 'CertificateOrPasswordAuthenticator',
            'authorizer': 'CassandraAuthorizer',
            'auth_certificate_role_queries': [
                {'source': 'SUBJECT', 'query': 'CN=([^,]+)'},
            ],
            # Set explicit non-SSL and SSL ports so the driver can connect to unencrypted port.
            # Without this, native_shard_aware_transport_port (19042) gets encrypted
            # (np=0, nps=0, ceo=1 → encrypted), which prevents the manager's plain driver
            # from connecting and causes servers_add to hang.
            'native_transport_port': 9042,
            'native_shard_aware_transport_port': 19042,
            'native_transport_port_ssl': _TLS_PORT,
            'native_shard_aware_transport_port_ssl': 19142,
            'client_encryption_options': {
                'enabled': True,
                'certificate': 'conf/scylla.crt',
                'keyfile': 'conf/scylla.key',
                'truststore': f'{tmp_path}/ca.crt',
                'require_client_auth': 'optional',
            },
        },
        driver_connect_opts={
            'auth_provider': PlainTextAuthProvider(username='cassandra', password='cassandra'),
        })
    return servers[0].ip_addr


async def test_cql_optional_client_cert(manager: ManagerClient, tmp_path):
    """Test CertificateOrPasswordAuthenticator with require_client_auth=optional.

    This tests that a single TLS CQL port accepts both certificate-bearing
    clients (cert CN as role, no SASL) and password-authenticated clients
    (SASL exchange), depending on whether the client presents a TLS
    certificate.

    We test five scenarios on the same server:

    Test 1: cert client, cert auth succeeds (no SASL, CN used as role).
    Test 2: no-cert client with correct password. password auth succeeds.
    Test 3: no-cert client with wrong password. authentication fails.
    Test 4: client with untrusted cert. TLS handshake fails; password cannot help.
    Test 5: trusted cert matching no role query + valid password. cert auth fails
            and there is no fallback to password auth.
    """
    _gen_certs(tmp_path)
    host = await _start_server(manager, tmp_path)

    # Test 1: cert-bearing client authenticates via cert CN, no SASL needed.
    logger.info("Test 1: cert client should authenticate via cert CN")
    cluster_cert = _make_tls_cluster(
        host, _TLS_PORT,
        certfile=f'{tmp_path}/client.crt',
        keyfile=f'{tmp_path}/client.key',
    )
    try:
        session = cluster_cert.connect()
        rows = list(session.execute('SELECT release_version FROM system.local'))
        assert rows
    finally:
        safe_driver_shutdown(cluster_cert)

    # Test 2: cert-less client with correct password. password auth succeeds.
    # With require_client_auth=optional the TLS handshake completes,
    # then CertificateOrPasswordAuthenticator detects the missing cert and falls
    # back to SASL.
    logger.info("Test 2: no-cert client with correct password should succeed via SASL")
    cluster_pwd = _make_tls_cluster(
        host, _TLS_PORT,
        auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'),
    )
    try:
        session = cluster_pwd.connect()
        rows = list(session.execute('SELECT release_version FROM system.local'))
        assert rows
    finally:
        safe_driver_shutdown(cluster_pwd)

    # Test 3: cert-less client with wrong password. authentication fails.
    logger.info("Test 3: no-cert client with wrong password should fail authentication")
    cluster_bad_pwd = _make_tls_cluster(
        host, _TLS_PORT,
        auth_provider=PlainTextAuthProvider(username='cassandra', password='wrong_password'),
    )
    try:
        with pytest.raises(NoHostAvailable) as exc_info:
            cluster_bad_pwd.connect()
        error_texts = [str(e) for e in exc_info.value.errors.values()]
        assert not any('SSL' in t or 'certificate required' in t.lower() for t in error_texts), \
            f"Unexpected SSL error — should be a password failure: {error_texts}"
    finally:
        safe_driver_shutdown(cluster_bad_pwd)

    # Test 4: client presents a certificate signed by an untrusted CA.
    # Even though the server doesn't require a client certificate, when the
    # client does present one the TLS layer validates it against the truststore.
    # An untrusted certificate causes the TLS handshake to fail before any CQL
    # authentication exchange takes place, so supplying a correct password
    # cannot rescue the connection.
    logger.info("Test 4: client with untrusted cert should fail at TLS handshake")
    cluster_bad_cert = _make_tls_cluster(
        host, _TLS_PORT,
        certfile=f'{tmp_path}/client2.crt',
        keyfile=f'{tmp_path}/client2.key',
        auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'),
    )
    try:
        with pytest.raises(NoHostAvailable) as exc_info:
            cluster_bad_cert.connect()
        error_texts = [str(e) for e in exc_info.value.errors.values()]
        # We would like to see a clean TLS error here, but in some cases the
        # server may close the connection prematurely and the driver reports a
        # generic connection closed message instead. Accept that too.
        # The important thing is that the connection failed.
        assert any('SSL' in t or 'tls' in t.lower() or 'certificate' in t.lower() or 'Broken pipe' in t or 'already closed' in t.lower() for t in error_texts), \
            f"Expected an SSL/TLS error for untrusted cert, got: {error_texts}"
    finally:
        safe_driver_shutdown(cluster_bad_cert)

    # Test 5: client presents a trusted certificate (signed by the trusted CA),
    # but the certificate subject has no CN field, so the role query
    # "CN=([^,]+)" does not match.  Valid username/password credentials are
    # supplied alongside the cert.  CertificateOrPasswordAuthenticator must
    # NOT fall back to password auth once a (trusted) certificate is detected:
    # authentication should fail even though the password would be correct on
    # its own. A client that has bad certs shouldn't send them.
    logger.info("Test 5: trusted cert matching no role query + valid password should fail (no password fallback)")
    cluster_no_rule_cert = _make_tls_cluster(
        host, _TLS_PORT,
        certfile=f'{tmp_path}/client3.crt',
        keyfile=f'{tmp_path}/client3.key',
        auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'),
    )
    try:
        with pytest.raises(NoHostAvailable) as exc_info:
            cluster_no_rule_cert.connect()
        error_texts = [str(e) for e in exc_info.value.errors.values()]
        # The TLS handshake must have succeeded (the cert is signed by the trusted CA),
        # so this must be a CQL-level "Bad credentials" auth failure.
        assert any('Bad credentials' in t for t in error_texts), \
            f"Expected a CQL authentication failure, got: {error_texts}"
    finally:
        safe_driver_shutdown(cluster_no_rule_cert)



