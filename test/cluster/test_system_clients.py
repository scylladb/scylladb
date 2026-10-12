#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
system.clients on nodes with PasswordAuthenticator serving CQL over TLS on
native_transport_port_ssl next to the plain port: sessions of TLS users are
listed while open and gone once closed, and clients without the required
certificate are refused.

Ported from scylla-dtest cql_system_clients_test.py. Like the dtests'
nodes, they use vnodes (256 per node), the SimpleSnitch,
PasswordAuthenticator with CassandraAuthorizer and no permission or role
caching, and are stopped gracefully at the end of the test, as the dtests'
teardown did.
"""

import asyncio
import logging
import ssl
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT, NoHostAvailable
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import dict_factory

from test.cluster.util import stop_servers_gracefully
from test.pylib.certificates import CertificateAuthority
from test.pylib.driver_utils import safe_driver_shutdown
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256}
# ccm's populate(n): every node in datacenter1/rack1 of the SimpleSnitch.
SIMPLE_SNITCH = {'endpoint_snitch': 'SimpleSnitch'}
TEST_USERS = [("user1", "password1"), ("user2", "password2"), ("user3", "password3"),
              ("user4", "password4"), ("user5", "password5")]
SUPERUSER = PlainTextAuthProvider(username='cassandra', password='cassandra')


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def get_system_clients_records(session, protocol_version=None, user=None, ssl_context=None) -> list:
    """The dtests' get_system_clients_records(): the rows of system.clients
    matching the session's TLS state, user and protocol version ("*" = any)."""
    filters = []
    if ssl_context is None:
        filters.append("ssl_enabled=null")
    elif ssl_context != "*":
        filters.append("ssl_enabled=true")
    if user is None:
        filters.append("username='anonymous'")
    elif user != "*":
        filters.append(f"username='{user}'")
    if protocol_version is None:
        filters.append("protocol_version=null")
    elif protocol_version != "*":
        filters.append(f"protocol_version='{protocol_version}'")
    where = "WHERE " + " AND ".join(filters) + " ALLOW FILTERING" if filters else ""
    result = list(session.execute(f"SELECT * FROM system.clients {where}"))
    logger.debug("system.clients: %s", result)
    return result


async def get_allowed_client_records(session, allow_anonymous=True, step=0.2, timeout=30) -> list:
    """All rows of system.clients; without allow_anonymous, once no
    connection is left unauthenticated ("anonymous")."""
    end_time = time.time() + timeout
    records = get_system_clients_records(session, protocol_version="*", user="*", ssl_context="*")
    if not allow_anonymous:
        while any(r["username"] == "anonymous" if isinstance(r, dict) else r.username == "anonymous" for r in records):
            assert time.time() < end_time, "Timed out waiting for no anonymous records in system_clients"
            await asyncio.sleep(step)
            records = get_system_clients_records(session, protocol_version="*", user="*", ssl_context="*")
    return records


async def get_total_records_in_system_clients(session, allow_anonymous=True) -> int:
    return len(await get_allowed_client_records(session, allow_anonymous))


async def wait_total_records_in_system_clients(session, expected, timeout=30) -> None:
    end_time = time.time() + timeout
    while (last_value := await get_total_records_in_system_clients(session)) != expected:
        assert time.time() < end_time, \
            f"Timed out waiting for number of records in system_clients to get to {expected}, last value was {last_value}"
        await asyncio.sleep(0.2)


class Certificates:
    """The dtests' generate_ssl_stores(): the node's certificate and key
    (ccm_node.pem/.key), signed by a CA whose certificate is ccm_node.cer."""
    def __init__(self, tmp_path):
        ca = CertificateAuthority()
        self.pem, self.key = str(tmp_path / "ccm_node.pem"), str(tmp_path / "ccm_node.key")
        ca.issue("scylla", self.pem, self.key)
        self.cer = ca.write_cert(tmp_path / "ccm_node.cer")


async def prepare(manager: ScyllaClusterManager, tmp_path, ssl_optional=False, require_ssl_auth=False,
                  nodes=1, superuser=False, ssl_enabled=True, extra_config=None):
    """The dtests' prepare(): nodes with client encryption (optional or
    with required client certificates), password authentication and five
    test roles. As in the dtests, system_auth isn't altered: auth-v2
    replicates it via raft."""
    certs = Certificates(tmp_path)
    ssl_options = {'enabled': ssl_enabled}
    if ssl_optional:
        ssl_options['optional'] = ssl_optional
    ssl_options.update({'certificate': certs.pem, 'keyfile': certs.key})
    if require_ssl_auth:
        ssl_options.update({'truststore': certs.cer, 'require_client_auth': True})
    config = {**CONFIG, **SIMPLE_SNITCH,
              'client_encryption_options': ssl_options,
              'authenticator': 'org.apache.cassandra.auth.PasswordAuthenticator',
              'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
              'role_manager': 'org.apache.cassandra.auth.CassandraRoleManager',
              'permissions_validity_in_ms': 0, 'roles_validity_in_ms': 0,
              'native_transport_port': 9042, 'native_transport_port_ssl': 9142,
              **(extra_config or {})}
    servers = await manager.servers_add(nodes, config=config, driver_connect_opts={'auth_provider': SUPERUSER})
    cql = await manager.get_cql_exclusive(servers[0], auth_provider=SUPERUSER)
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ALL
    for user, password in TEST_USERS:
        await cql.run_async(f"CREATE ROLE '{user}' WITH PASSWORD = '{password}' AND LOGIN = true AND  SUPERUSER = {superuser}")
    return servers, certs


def verifying_ssl_context(certs: Certificates, load_cert_chain=False) -> ssl.SSLContext:
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    # The node certificate has no address in it.
    ssl_context.check_hostname = False
    if load_cert_chain:
        ssl_context.load_cert_chain(certfile=certs.pem, keyfile=certs.key)
        ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.load_verify_locations(cafile=certs.cer)
    return ssl_context


def node_cluster(server, user, password, port, ssl_context=None, row_factory=None) -> Cluster:
    """The driver cluster of the dtests' node_session(): a session to one
    node as the given user."""
    profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr]))
    if row_factory:
        profile.row_factory = row_factory
    return Cluster(contact_points=[server.ip_addr], port=port, ssl_context=ssl_context,
                   auth_provider=PlainTextAuthProvider(username=user, password=password),
                   execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                   # The driver only finds the TLS shard-aware port when
                   # configured with ssl_options, not ssl_context.
                   shard_aware_options={'disable_shardaware_port': True})


class TlsUserSession:
    """The dtests' CQLSession with its SessionStore, for one session of a
    user over TLS, as the dtests use them:

    - Inside the session's `with`, expect_system_clients() reads
      system.clients through the user's own session. The user (not a
      superuser) has no SELECT on system.clients, so the read fails with
      Unauthorized; in the dtests the failure is swallowed, because
      CQLSession.__exit__() returns a true value, which also skips the rest
      of the `with` body. Here it is swallowed and logged.
    - The `with` shuts down the session but not its driver cluster, whose
      control connection over TLS stays open, so the superuser's later
      reads of system.clients list a TLS client of the user.
    - After the `with`, the dtests' SessionStore still holds the closed
      session's hash as a key (with an empty list), so it skips the "not in
      system.clients" check: only the superuser's session is opened.
    """
    def __init__(self, manager: ScyllaClusterManager, server, user, password, ssl_context, inspected_server):
        self.manager = manager
        self.user = user
        self.ssl_context = ssl_context
        self.inspected_server = inspected_server
        self.cluster = node_cluster(server, user, password, 9142, ssl_context)
        self.session = None

    def __enter__(self):
        self.session = self.cluster.connect()
        return self

    def __exit__(self, *exc):
        # The dtests' CQLSession.__exit__() shuts down the session only: the
        # driver's cluster, with its control connection over TLS, stays open
        # (it is closed by close() at the end of the test here).
        self.session.shutdown()
        self.session = None

    def close(self):
        safe_driver_shutdown(self.cluster)

    async def expect_system_clients(self):
        if self.session:
            try:
                records = get_system_clients_records(self.session, user=self.user, ssl_context=self.ssl_context)
                assert len(records) > 0, f"Can't find record in system.clients for cql session of {self.user}"
            except Exception as e:  # noqa: BLE001
                # The dtest's `with` body ends here, before its superuser session.
                logger.info(f"system.clients check of {self.user}'s session failed, as in the dtest: {e}")
                return
        await self.manager.get_cql_exclusive(self.inspected_server, auth_provider=SUPERUSER)


# Ported from scylla-dtest cql_system_clients_test.py::TestSystemClients::test_system_clients.
# On three nodes with optional client encryption, a TLS session of user1 to
# the second node is listed in system.clients while open and gone after.
# Then, inspected from the second and from the first node, the number of
# records in system.clients stays as it was, also across three failing
# connection attempts (TLS to the plain port, plain to the TLS port, a wrong
# user).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_system_clients(manager: ScyllaClusterManager, tmp_path):
    servers, certs = await prepare(manager, tmp_path, nodes=3, ssl_optional=True, require_ssl_auth=False)
    ssl_context = verifying_ssl_context(certs)
    user, password = TEST_USERS[0]

    session = TlsUserSession(manager, servers[1], user, password, ssl_context, servers[0])
    with session:
        await session.expect_system_clients()
    await session.expect_system_clients()

    for node in [servers[1], servers[0]]:
        inspector_session = await manager.get_cql_exclusive(node, auth_provider=SUPERUSER)
        original_sessions_count = await get_total_records_in_system_clients(inspector_session, allow_anonymous=False)
        # The dtest's failing node_session() calls to the second node: TLS
        # to the non-TLS port, plain to the TLS port, and a wrong user.
        for user_, password_, port, context in [(user, password, 9042, ssl_context),
                                                (user, password, 9142, None),
                                                ("user-to-fail", "password-to-fail", 9042, None)]:
            await asyncio.to_thread(patient_connect_fails, servers[1], user_, password_, port, context)
            await wait_total_records_in_system_clients(inspector_session, original_sessions_count)
    session.close()


def patient_connect_fails(server, user, password, port, ssl_context, timeout=10):
    """The dtests' patient_cql_connection(..., timeout=10) of a session that
    cannot be opened: it retries until the timeout, then fails."""
    deadline = time.time() + timeout
    while True:
        cluster = node_cluster(server, user, password, port, ssl_context)
        try:
            cluster.connect()
        except Exception as e:  # noqa: BLE001
            if time.time() >= deadline:
                logger.info(f"connection of {user} to port {port} failed, as expected: {e}")
                return
            time.sleep(1)
        else:
            raise AssertionError(f"connection of {user} to {server.ip_addr}:{port} unexpectedly succeeded")
        finally:
            safe_driver_shutdown(cluster)


# Ported from scylla-dtest cql_system_clients_test.py::TestSystemClients::test_system_clients_ssl_authentication.
# With required client certificates, a TLS session of user1 presenting the
# node's certificate is listed in system.clients while open and gone after;
# a TLS client without a certificate is refused with "certificate
# required" and leaves the number of records as it was.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_system_clients_ssl_authentication(manager: ScyllaClusterManager, tmp_path):
    # Unlike ccm, the test framework's driver connects to the shard-aware
    # port, which would otherwise be TLS-only too.
    servers, certs = await prepare(manager, tmp_path, ssl_optional=False, require_ssl_auth=True,
                                   extra_config={'native_shard_aware_transport_port': 19042,
                                                 'native_shard_aware_transport_port_ssl': 19142})
    [server] = servers
    user, password = TEST_USERS[0]

    session = TlsUserSession(manager, server, user, password, verifying_ssl_context(certs, load_cert_chain=True), server)
    with session:
        await session.expect_system_clients()
    await session.expect_system_clients()

    cql = await manager.get_cql_exclusive(server, auth_provider=SUPERUSER)
    original_sessions_count = await get_total_records_in_system_clients(cql, allow_anonymous=False)
    cluster = node_cluster(server, user, password, 9142, verifying_ssl_context(certs))
    try:
        with pytest.raises(NoHostAvailable) as exp:
            cluster.connect()
    finally:
        safe_driver_shutdown(cluster)
    assert "certificate required" in str(exp.value)
    await wait_total_records_in_system_clients(cql, original_sessions_count)
    session.close()


async def system_client_content(manager: ScyllaClusterManager, tmp_path, fields_with_value, ssl_optional=True, ssl_enabled=True):
    """The dtests' _system_client_content(): the first system.clients row
    seen by a superuser's session, plain or over TLS, has the fields filled."""
    _, certs = await prepare(manager, tmp_path, ssl_optional=ssl_optional, require_ssl_auth=False,
                             superuser=True, ssl_enabled=ssl_enabled)
    [server] = await manager.running_servers()
    if ssl_optional:
        port, ssl_context = 9142, verifying_ssl_context(certs)
    else:
        port, ssl_context = 9042, None
    user, password = TEST_USERS[0]
    cluster = node_cluster(server, user, password, port, ssl_context, row_factory=dict_factory)
    try:
        session = cluster.connect()
        rows = await get_allowed_client_records(session, allow_anonymous=False)
        row = rows[0]
        empty = {f: row.get(f) for f in fields_with_value if row.get(f) is None}
    finally:
        safe_driver_shutdown(cluster)
    assert not empty, f"expect fields Value with content, got {empty}"


# Ported from scylla-dtest cql_system_clients_test.py::TestSystemClients::test_system_client_not_none.
# A superuser's TLS session on native_transport_port_ssl appears in
# system.clients (once the driver's anonymous connections are
# authenticated) with its address, port, client type, connection stage,
# protocol version, shard, user name and driver name and version filled.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_system_client_not_none(manager: ScyllaClusterManager, tmp_path):
    fields = ["address", "port", "client_type", "connection_stage", "protocol_version", "shard_id", "username", "driver_name", "driver_version"]
    await system_client_content(manager, tmp_path, fields)


# Ported from scylla-dtest cql_system_clients_test.py::TestSystemClients::test_system_client_not_none_non_ssl.
# The same as test_system_client_not_none, with client encryption off and
# the session on the plain native_transport_port.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_system_client_not_none_non_ssl(manager: ScyllaClusterManager, tmp_path):
    fields = ["address", "port", "client_type", "connection_stage", "protocol_version", "shard_id", "username", "driver_name", "driver_version"]
    await system_client_content(manager, tmp_path, fields, ssl_optional=False, ssl_enabled=False)
