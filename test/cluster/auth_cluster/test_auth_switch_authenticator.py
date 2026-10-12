#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Restarting a node with another authenticator and authorizer: what
# anonymous, authenticated and wrongly authenticated clients may do under
# each, and that the auth data survives the switches.

import asyncio
import contextlib
import re
import time

import pytest
from cassandra import AuthenticationFailed, Unauthorized  # type: ignore
from cassandra.auth import PlainTextAuthProvider  # type: ignore
from cassandra.cluster import NoHostAvailable  # type: ignore
from cassandra.policies import RoundRobinPolicy, WhiteListRoundRobinPolicy  # type: ignore

from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from test.cluster.dtest_helpers import start_node_wait_cql
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import unique_name

ALLOW_ALL = {"authenticator": "AllowAllAuthenticator", "authorizer": "AllowAllAuthorizer"}
TRANSITIONAL = {"authenticator": "com.scylladb.auth.TransitionalAuthenticator",
                "authorizer": "com.scylladb.auth.TransitionalAuthorizer"}
STRICT = {"authenticator": "PasswordAuthenticator", "authorizer": "CassandraAuthorizer"}
ANONYMOUS = "You have to be logged in and not anonymous to perform this request"


@contextlib.contextmanager
def session_as(manager: ScyllaClusterManager, server, user=None, password=None):
    auth = PlainTextAuthProvider(username=user, password=password) if user else None
    cluster = manager.con_gen([server.ip_addr], manager.port, manager.use_ssl, auth)
    try:
        yield cluster.connect()
    finally:
        cluster.shutdown()


def assert_login_fails(manager, server, user=None, password=None):
    with pytest.raises(NoHostAvailable) as e:
        with session_as(manager, server, user, password):
            pass
    assert all(isinstance(err, AuthenticationFailed) for err in e.value.errors.values()), e.value


async def switch_auth(manager: ScyllaClusterManager, server, config):
    await manager.server_update_config(server.server_id, config_options=config, reload=False)
    await manager.server_restart(server.server_id)
    await manager.driver_connect()


# A role named "anonymous" is not the anonymous user: it may run LIST USERS.
# With AllowAll authentication, every client is anonymous, whatever the
# credentials it sends, and may not.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_anonymous(manager: ScyllaClusterManager):
    server = await manager.server_add(config=auth_config)
    cql = manager.get_cql()
    cql.execute("CREATE USER anonymous WITH PASSWORD '12345' NOSUPERUSER")
    with session_as(manager, server, "anonymous", "12345") as session:
        session.execute("LIST USERS")
    cql.execute("GRANT AUTHORIZE ON ALL KEYSPACES TO anonymous")

    await switch_auth(manager, server, ALLOW_ALL)
    with session_as(manager, server, "anonymous", "12345") as session:
        for stmt in ["LIST USERS", "GRANT SELECT ON ALL KEYSPACES TO anonymous"]:
            with pytest.raises(Unauthorized, match=ANONYMOUS):
                session.execute(stmt)


# Roles and their permissions survive restarts with AllowAll and then
# password authentication again.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_restart_node_doesnt_lose_auth_data(manager: ScyllaClusterManager):
    server = await manager.server_add(config=auth_config)
    cql = manager.get_cql()
    ks = unique_name()
    cql.execute("CREATE USER cathy WITH PASSWORD '12345'")
    cql.execute("CREATE USER philip WITH PASSWORD 'strongpass'")
    cql.execute(f"CREATE KEYSPACE {ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}")
    cql.execute(f"CREATE TABLE {ks}.cf (id int PRIMARY KEY)")
    cql.execute(f"GRANT ALL ON {ks}.cf TO philip")

    await switch_auth(manager, server, ALLOW_ALL)
    await switch_auth(manager, server, STRICT)
    with session_as(manager, server, "philip", "strongpass") as session:
        session.execute(f"SELECT * FROM {ks}.cf")
    with session_as(manager, server, "cathy", "12345") as session:
        with pytest.raises(Unauthorized, match=f"User cathy has no SELECT permission on <table {ks}.cf>"):
            session.execute(f"SELECT * FROM {ks}.cf")
    manager.get_cql().execute(f"DROP KEYSPACE {ks}")


# The TestAuth dtests' cluster: populate(3) (SimpleSnitch, datacenter1/rack1)
# with vnodes, and no permission caching.
DTEST_PROPERTY_FILE = {"dc": "datacenter1", "rack": "rack1"}
DTEST_CONFIG = {"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256, "endpoint_snitch": "SimpleSnitch",
                "permissions_validity_in_ms": 0, "permissions_update_interval_in_ms": 0}
DTEST_ALLOW_ALL = {"authenticator": "AllowAllAuthenticator", "authorizer": "AllowAllAuthorizer"}
DTEST_STRICT = {"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator",
                "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer"}


async def wait_for_any_log(manager: ScyllaClusterManager, servers, entries: list[str], timeout: float = 30) -> None:
    """The dtests' wait_for_any_log(..., dispersed=True): each entry is
    logged by some node."""
    deadline = time.time() + timeout
    logs = [await manager.server_open_log(s.server_id) for s in servers]
    for entry in entries:
        while not any([await log.grep(entry) for log in logs]):
            assert time.time() < deadline, f"None of the nodes logged {entry!r}"
            await asyncio.sleep(0.5)


async def dtest_auth_prepare(manager: ScyllaClusterManager, nodes=3, enable_auth=True, wait_for_superuser=False,
                             config=None, **server_add_opts):
    """TestAuth.prepare(): the nodes with password authentication or the
    default AllowAll, waiting until a node created the default superuser."""
    config = {**DTEST_CONFIG, **(DTEST_STRICT if enable_auth else DTEST_ALLOW_ALL), **(config or {})}
    servers = [await manager.server_add(config=config, property_file=DTEST_PROPERTY_FILE, **server_add_opts)
               for _ in range(nodes)]
    if enable_auth or wait_for_superuser:
        entries = ["Created default superuser role"]
        if enable_auth:
            entries.append("Created default superuser authentication record")
        await wait_for_any_log(manager, servers, entries)
    return servers


async def dtest_auth_switch(manager: ScyllaClusterManager, servers, config) -> None:
    """set_configuration_options(), then each node stopped (gracefully) and
    started again, one after another."""
    for s in servers:
        await manager.server_update_config(s.server_id, config_options=config, reload=False)
    for s in servers:
        await manager.server_stop_gracefully(s.server_id)
        await start_node_wait_cql(manager, s)
    await manager.driver_connect()


class DtestSessions:
    """TestAuth.get_session(): sessions to a node as the given user; exclusive
    (only that node), patient (retrying NoHostAvailable for 30 s) by default.
    Their clusters are shut down by close()."""
    def __init__(self, manager: ScyllaClusterManager):
        self.manager = manager
        self.clusters = []

    def get(self, server, user=None, password=None, exclusive=True, patient=True):
        auth = PlainTextAuthProvider(username=user, password=password) if user is not None else None
        lbp = WhiteListRoundRobinPolicy([server.ip_addr]) if exclusive else RoundRobinPolicy()
        deadline = time.time() + 30
        while True:
            cluster = self.manager.con_gen([server.ip_addr], self.manager.port, self.manager.use_ssl, auth, lbp)
            try:
                session = cluster.connect()
                self.clusters.append(cluster)
                return session
            except NoHostAvailable:
                cluster.shutdown()
                if not patient or time.time() > deadline:
                    raise
                time.sleep(0.5)

    def close(self):
        for cluster in self.clusters:
            cluster.shutdown()


def assert_unauthorized(message, session, query):
    with pytest.raises(Unauthorized) as cm:
        session.execute(query)
    assert re.search(message, str(cm.value)), f"Expected '{message}', but got '{cm.value!s}'"


def assert_auth_failed(exc_info):
    assert isinstance(next(iter(exc_info.value.errors.values())), AuthenticationFailed), exc_info.value


# Ported from scylla-dtest auth_test.py::TestAuth::test_transitional_auth_from_default.
# Three nodes, from the default AllowAll through transitional to strict
# authentication, each a rolling restart. Under transitional
# authentication a wrong password logs in as anonymous, with access to data
# but no auth statements, while logging in without credentials fails;
# under strict authentication a wrong password fails and the user needs
# permissions.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_transitional_auth_from_default(manager: ScyllaClusterManager):
    servers = await dtest_auth_prepare(manager, enable_auth=False, wait_for_superuser=True)
    node1 = servers[0]
    sessions = DtestSessions(manager)
    try:
        await dtest_auth_switch(manager, servers, TRANSITIONAL)
        cassandra = sessions.get(node1, "cassandra", "cassandra")
        cassandra.execute("CREATE USER normal WITH PASSWORD '123456' NOSUPERUSER")

        session = sessions.get(node1, "normal", "wrongpwd")
        assert_unauthorized(ANONYMOUS, session, "LIST USERS")
        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        session.execute("CREATE TABLE ks.cf (id int primary key)")
        session.execute("SELECT * FROM ks.cf")
        assert_unauthorized(ANONYMOUS, session, "GRANT SELECT ON ks.cf TO normal")
        assert_unauthorized(ANONYMOUS, session, "REVOKE SELECT ON ks.cf from normal")

        with pytest.raises(NoHostAvailable) as exc:
            sessions.get(node1, patient=False)
        assert_auth_failed(exc)

        await dtest_auth_switch(manager, servers, DTEST_STRICT)
        with pytest.raises(NoHostAvailable) as exc:
            sessions.get(node1, patient=False)
        assert_auth_failed(exc)
        with pytest.raises(NoHostAvailable) as exc:
            sessions.get(node1, "normal", "wrongpwd", patient=False)
        assert_auth_failed(exc)

        session = sessions.get(node1, "normal", "123456")
        assert_unauthorized("User normal has no SELECT permission on <table ks.cf> or any of its parents", session, "SELECT * FROM ks.cf")
        assert_unauthorized("User normal has no AUTHORIZE permission on <table ks.cf> or any of its parents", session, "REVOKE SELECT ON ks.cf from normal")
    finally:
        sessions.close()


# Ported from scylla-dtest auth_test.py::TestAuth::test_transitional_auth_from_pwdauth.
# Three nodes, from strict through transitional to no authentication (not
# the intended order, but it must work too), each a rolling restart. Under
# transitional authentication the user keeps its identity and lacks
# AUTHORIZE, a wrong password logs in as anonymous and no credentials fail;
# with AllowAll, everyone is anonymous.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_transitional_auth_from_pwdauth(manager: ScyllaClusterManager):
    servers = await dtest_auth_prepare(manager, enable_auth=True)
    await wait_for_any_log(manager, servers, ["Created default superuser"])
    node1 = servers[0]
    sessions = DtestSessions(manager)
    try:
        session = sessions.get(node1, "cassandra", "cassandra")
        session.execute("CREATE USER normal WITH PASSWORD '123456' NOSUPERUSER")

        session = sessions.get(node1, "normal", "123456")
        rows = list(session.execute("LIST USERS"))
        assert len(rows) == 1, f"Expect to see `normal`, actual: {rows}"

        with pytest.raises(NoHostAvailable) as exc:
            sessions.get(node1, "normal", "wrongpwd", patient=False)
        assert_auth_failed(exc)

        await dtest_auth_switch(manager, servers, TRANSITIONAL)
        session = sessions.get(node1, "normal", "123456")
        session.execute("LIST USERS")
        session.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        session.execute("CREATE TABLE ks.cf (id int primary key)")
        assert_unauthorized("User normal has no AUTHORIZE permission on <table ks.cf> or any of its parents", session, "GRANT ALTER ON ks.cf TO normal")
        assert_unauthorized("User normal has no AUTHORIZE permission on <table ks.cf> or any of its parents", session, "REVOKE SELECT ON ks.cf from normal")

        session = sessions.get(node1, "normal", "wrongpwd")
        assert_unauthorized(ANONYMOUS, session, "LIST USERS")

        with pytest.raises(NoHostAvailable) as exc:
            sessions.get(node1, patient=False)
        assert_auth_failed(exc)

        await dtest_auth_switch(manager, servers, ALLOW_ALL)
        session = sessions.get(node1, "cassandra", "cassandra")
        assert_unauthorized(ANONYMOUS, session, "LIST USERS")
        session = sessions.get(node1, "normal", "123456")
        assert_unauthorized(ANONYMOUS, session, "LIST USERS")
    finally:
        sessions.close()
