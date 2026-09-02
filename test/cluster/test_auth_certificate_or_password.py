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
import socket
import ssl
import struct
import logging
import pytest

from contextlib import asynccontextmanager

from cassandra import ConsistencyLevel, InvalidRequest        # type: ignore
from cassandra.cluster import Cluster, NoHostAvailable       # type: ignore
from cassandra.auth import PlainTextAuthProvider             # type: ignore
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT  # type: ignore
from cassandra.policies import WhiteListRoundRobinPolicy     # type: ignore
from cassandra.query import SimpleStatement                  # type: ignore

from test.cluster.dtest.dtest_class import wait_for
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.driver_utils import safe_driver_shutdown

logger = logging.getLogger(__name__)

def system(cmd):
    """Like os.system(), but asserts the command succeeded (exit code 0)."""
    assert os.system(cmd) == 0, f'Command failed: {cmd}'

# TLS CQL port used in the tests.  We also keep the default unencrypted port
# on 9042 because the test framework's manager connects without TLS and needs
# a plain port to reach the server.
_TLS_PORT = 9143


_CQL_OPCODE_ERROR = 0x00
_CQL_OPCODE_STARTUP = 0x01
_CQL_FRAME_HEADER_LEN = 9


def _cql_startup_frame(stream: int = 0) -> bytes:
    """Build a raw protocol v4 STARTUP frame.

    The driver closes the connection as soon as a login is refused, so a test
    that has to send a second STARTUP on the same socket cannot use it.  This
    follows the frame building in test/cqlpy/test_protocol_exceptions.py.
    """
    def short_string(value: str) -> bytes:
        encoded = value.encode()
        return struct.pack('!H', len(encoded)) + encoded

    body = struct.pack('!H', 1) + short_string('CQL_VERSION') + short_string('3.0.0')
    return (struct.pack('!B', 0x04)
            + struct.pack('!B', 0x00)
            + struct.pack('!H', stream)
            + struct.pack('!B', _CQL_OPCODE_STARTUP)
            + struct.pack('!I', len(body))
            + body)


def _recv_exactly(sock, count: int) -> bytes:
    buf = b''
    while len(buf) < count:
        chunk = sock.recv(count - len(buf))
        assert chunk, f'connection closed after {len(buf)} of {count} bytes'
        buf += chunk
    return buf


def _recv_cql_opcode(sock) -> int:
    """Read one whole frame off the socket and return its opcode."""
    header = _recv_exactly(sock, _CQL_FRAME_HEADER_LEN)
    opcode = header[4]
    (body_len,) = struct.unpack('!I', header[5:9])
    _recv_exactly(sock, body_len)
    return opcode


def _tls_socket(host: str, port: int, certfile: str, keyfile: str):
    """A raw TLS socket presenting a client certificate.

    The server's certificate is not verified: these tests generate their own CA
    and are only interested in what the server does with the client's.
    """
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    context.load_cert_chain(certfile=certfile, keyfile=keyfile)
    return context.wrap_socket(socket.create_connection((host, port), timeout=30))


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
    """Generate a self-signed CA and the client certificates the tests connect with.

    client.crt   trusted, CN=cassandra - the happy path.
    client2.crt  signed by a second CA (ca2) that the server does *not* trust,
                 to simulate a client presenting an untrusted certificate.
    client3.crt  trusted, but its subject has no CN, so the role query cannot
                 extract a role name from it.
    client4.crt  trusted, CN=certuser - a role that exists and may log in.
    client5.crt  trusted, CN=certghost - a role that is never created.
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

    system(f'openssl genrsa 2048 > "{tmp_path}/client4.key" 2>/dev/null')
    system(f'openssl req -new -sha256 -subj "/CN=certuser" '
           f'-key "{tmp_path}/client4.key" -out "{tmp_path}/client4.csr" 2>/dev/null')
    system(f'openssl x509 -req -sha256 -days 365 '
           f'-in "{tmp_path}/client4.csr" '
           f'-CA "{tmp_path}/ca.crt" -CAkey "{tmp_path}/ca.key" -CAcreateserial '
           f'-out "{tmp_path}/client4.crt" 2>/dev/null')

    system(f'openssl genrsa 2048 > "{tmp_path}/client5.key" 2>/dev/null')
    system(f'openssl req -new -sha256 -subj "/CN=certghost" '
           f'-key "{tmp_path}/client5.key" -out "{tmp_path}/client5.csr" 2>/dev/null')
    system(f'openssl x509 -req -sha256 -days 365 '
           f'-in "{tmp_path}/client5.csr" '
           f'-CA "{tmp_path}/ca.crt" -CAkey "{tmp_path}/ca.key" -CAcreateserial '
           f'-out "{tmp_path}/client5.crt" 2>/dev/null')


def _server_config(tmp_path, extra_config=None):
    """Build the node config the tests in this file share.

    native_transport_port_ssl adds the TLS port (9143) while keeping the default
    plain port (9042), which the test framework's manager requires since it connects
    without TLS.  All four ports (9042, 9143, 19042, 19142) must be set explicitly:
    without that, the shard-aware port (19042) inherits the encryption setting and
    becomes TLS-only, which makes the manager's plain driver hang on connection.

    extra_config, when given, is merged over the defaults, so a test needing a
    node configured differently - a different authenticator, or auditing turned
    on - can say so without affecting the other tests in this file.
    """
    config = {
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
    }
    config.update(extra_config or {})
    return config


async def test_cql_optional_client_cert(manager: ScyllaClusterManager, tmp_path):
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
    servers = await manager.servers_add(1, config=_server_config(tmp_path),
        driver_connect_opts={
            'auth_provider': PlainTextAuthProvider(username='cassandra', password='cassandra'),
        })
    host = servers[0].ip_addr

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


async def test_cql_plain_port(manager: ScyllaClusterManager, tmp_path):
    """Test CertificateOrPasswordAuthenticator on an unencrypted CQL port.

    On a plain (non-TLS) connection there is no TLS handshake and thus no
    client certificate can ever be presented, so CertificateOrPasswordAuthenticator
    always falls back to the SASL exchange for username/password authentication.
    We just check that password authentication behaves normally on this port:
    correct credentials succeed, and incorrect credentials fail.
    """
    _gen_certs(tmp_path)
    server = (await manager.servers_add(1, config=_server_config(tmp_path),
        driver_connect_opts={
            'auth_provider': PlainTextAuthProvider(username='cassandra', password='cassandra'),
        }))[0]

    # Correct password authenticates successfully via SASL.
    logger.info("Correct password should succeed on the plain port")
    # Note that the framework's get_cql_exclusive() helper uses the default
    # plain port (9042) and does not use the TLS port (9143).
    session = await manager.get_cql_exclusive(
        server, auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'))
    rows = list(session.execute('SELECT release_version FROM system.local'))
    assert rows

    # Wrong password is rejected.
    logger.info("Wrong password should fail on the plain port")
    with pytest.raises(NoHostAvailable) as exc_info:
        await manager.get_cql_exclusive(
            server, auth_provider=PlainTextAuthProvider(username='cassandra', password='wrong_password'))
    error_texts = [str(e) for e in exc_info.value.errors.values()]
    assert any('Bad credentials' in t for t in error_texts), \
        f"Expected a CQL authentication failure, got: {error_texts}"


@asynccontextmanager
async def _audit_login_node(manager, tmp_path):
    """A node auditing AUTH, yielding its address and an admin session.

    audit_categories is narrowed to AUTH so the DCL rows for the CREATE ROLE
    statements below stay out of audit.audit_log.
    """
    _gen_certs(tmp_path)
    server = (await manager.servers_add(1,
        config=_server_config(tmp_path, extra_config={
            'audit': 'table',
            'audit_categories': 'AUTH',
        }),
        driver_connect_opts={
            'auth_provider': PlainTextAuthProvider(username='cassandra', password='cassandra'),
        }))[0]
    admin = await manager.get_cql_exclusive(
        server, auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'))
    admin.execute("CREATE ROLE certuser WITH LOGIN = true")
    admin.execute("CREATE ROLE pwduser WITH PASSWORD = 'pwduser' AND LOGIN = true")
    yield server.ip_addr, admin


def _login_records(session):
    """The AUTH/LOGIN records in the audit log."""
    try:
        rows = session.execute(SimpleStatement('SELECT * FROM audit.audit_log',
                                               consistency_level=ConsistencyLevel.ONE))
    except InvalidRequest:
        return []
    return [r for r in rows if r.category == 'AUTH' and r.operation == 'LOGIN']


def _await_login_record(admin, host, username, error, description):
    """Wait for an AUTH/LOGIN record for username, and check the columns it shares.

    Matched by presence, not by count: a driver opens more than one connection per
    cluster, so one logical login can legitimately produce several rows.
    """
    found = []

    def appeared():
        nonlocal found
        found = [r for r in _login_records(admin)
                 if r.username == username and r.error == error]
        return bool(found)

    wait_for(appeared, timeout=60, text=f'audit login record for {description}')
    for row in found:
        assert row.node == host
        assert row.source
        assert row.keyspace_name == ''
        assert row.table_name == ''
    return found


def _attempt_cert_login(host, tmp_path, cert, refused):
    """Open a TLS connection presenting cert, expecting it to be accepted or refused."""
    cluster = _make_tls_cluster(host, _TLS_PORT,
                                certfile=f'{tmp_path}/{cert}.crt',
                                keyfile=f'{tmp_path}/{cert}.key')
    try:
        if refused:
            with pytest.raises(NoHostAvailable):
                cluster.connect()
        else:
            cluster.connect()
    finally:
        safe_driver_shutdown(cluster)


async def test_audit_accepted_certificate_login(manager: ScyllaClusterManager, tmp_path):
    """An accepted certificate login is audited under the role it names (SCYLLADB-3926)."""
    async with _audit_login_node(manager, tmp_path) as (host, admin):
        assert not [r for r in _login_records(admin) if r.username == 'certuser'], \
            'certuser login records exist before certuser has connected'
        _attempt_cert_login(host, tmp_path, 'client4', refused=False)
        _await_login_record(admin, host, 'certuser', error=False,
                            description='certuser certificate login')


async def test_audit_rejected_certificate_login(manager: ScyllaClusterManager, tmp_path):
    """A certificate the role query cannot match is audited under an empty name (SCYLLADB-3926)."""
    async with _audit_login_node(manager, tmp_path) as (host, admin):
        assert not [r for r in _login_records(admin) if r.username == '' and r.error], \
            'an identity-less login failure was recorded before one was attempted'
        _attempt_cert_login(host, tmp_path, 'client3', refused=True)
        _await_login_record(admin, host, '', error=True, description='rejected certificate')


async def test_audit_certificate_for_missing_role(manager: ScyllaClusterManager, tmp_path):
    """A certificate naming a role that does not exist is audited as an error (SCYLLADB-3926).

    authenticate() resolves a name from the certificate without checking that the
    role exists, so the record has to state the outcome of the whole login.
    """
    async with _audit_login_node(manager, tmp_path) as (host, admin):
        assert not [r for r in _login_records(admin) if r.username == 'certghost'], \
            'certghost login records exist before certghost has connected'
        _attempt_cert_login(host, tmp_path, 'client5', refused=True)
        _await_login_record(admin, host, 'certghost', error=True,
                            description='certghost refused login')
        assert not [r for r in _login_records(admin)
                    if r.username == 'certghost' and not r.error], \
            'a refused certificate login must not also produce a success record'


async def test_audit_password_login_on_tls_port(manager: ScyllaClusterManager, tmp_path):
    """A password login on the TLS port keeps producing its SASL-path record (SCYLLADB-3926)."""
    async with _audit_login_node(manager, tmp_path) as (host, admin):
        cluster = _make_tls_cluster(
            host, _TLS_PORT,
            auth_provider=PlainTextAuthProvider(username='pwduser', password='pwduser'))
        try:
            cluster.connect()
        finally:
            safe_driver_shutdown(cluster)
        _await_login_record(admin, host, 'pwduser', error=False,
                            description='pwduser password login')


@asynccontextmanager
async def _certificate_authenticator_node(manager, tmp_path):
    """A CertificateAuthenticator node, yielding its address and an admin session.

    CertificateOrPasswordAuthenticator falls back to SASL rather than refusing, so
    a test for a refused login needs this authenticator instead.  The manager's own
    driver cannot log into it on the plain port, hence connect_driver=False and an
    admin connection over TLS retried until the listener answers.
    """
    _gen_certs(tmp_path)
    server = await manager.server_add(
        config=_server_config(tmp_path, extra_config={
            'authenticator': 'CertificateAuthenticator',
            'audit': 'table',
            'audit_categories': 'AUTH',
        }),
        connect_driver=False)

    def connect_admin():
        cluster = _make_tls_cluster(
            server.ip_addr, _TLS_PORT,
            certfile=f'{tmp_path}/client.crt',
            keyfile=f'{tmp_path}/client.key',
        )
        try:
            return cluster, cluster.connect()
        except NoHostAvailable:
            safe_driver_shutdown(cluster)
            return None

    admin_cluster, admin = wait_for(connect_admin, timeout=60,
                                    text='the TLS port to accept a certificate login')
    try:
        yield server.ip_addr, admin
    finally:
        safe_driver_shutdown(admin_cluster)


def _refused_login_records(session):
    """Audit records for a login refused before any role name existed."""
    return [r for r in _login_records(session) if r.username == '' and r.error]


async def test_audit_login_without_certificate(manager: ScyllaClusterManager, tmp_path):
    """A client presenting no certificate at all must be audited (SCYLLADB-3926).

    Such a login is refused before any name exists, so it is recorded under an
    empty user name: the client address and the node are what the attempt has to
    say for itself.
    """
    async with _certificate_authenticator_node(manager, tmp_path) as (host, admin):
        assert not _refused_login_records(admin), \
            'an identity-less login failure was recorded before one was attempted'

        cluster_no_cert = _make_tls_cluster(host, _TLS_PORT)
        try:
            with pytest.raises(NoHostAvailable):
                cluster_no_cert.connect()
        finally:
            safe_driver_shutdown(cluster_no_cert)

        found = []

        def appeared():
            nonlocal found
            found = _refused_login_records(admin)
            return bool(found)

        wait_for(appeared, timeout=60, text='audit login record for a certificate-less client')
        for row in found:
            assert row.node == host
            assert row.source
            assert row.keyspace_name == ''
            assert row.table_name == ''


async def test_audit_repeated_refused_startup(manager: ScyllaClusterManager, tmp_path):
    """Every rejected STARTUP on one connection must be audited (SCYLLADB-3926).

    A refused STARTUP is answered with an ERROR but leaves the connection in
    UNINITIALIZED, which accepts STARTUP again, so one socket can repeat the
    attempt.  The driver closes as soon as a login is refused and cannot send the
    second one, which is why this test - alone in this file - speaks CQL frames
    directly.
    """
    async with _certificate_authenticator_node(manager, tmp_path) as (host, admin):
        assert not _refused_login_records(admin), \
            'an identity-less login failure was recorded before one was attempted'

        sock = _tls_socket(host, _TLS_PORT,
                           certfile=f'{tmp_path}/client3.crt',
                           keyfile=f'{tmp_path}/client3.key')
        try:
            for attempt in range(2):
                sock.sendall(_cql_startup_frame(stream=attempt))
                opcode = _recv_cql_opcode(sock)
                assert opcode == _CQL_OPCODE_ERROR, \
                    f'attempt {attempt} answered with opcode {opcode:#x}, expected ERROR'
        finally:
            sock.close()

        def both_appeared():
            return len(_refused_login_records(admin)) >= 2

        wait_for(both_appeared, timeout=60, text='an audit record for each rejected STARTUP')
        assert len(_refused_login_records(admin)) == 2, \
            'two rejected STARTUPs produced more than two audit records'
