#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Service levels created, altered, dropped and attached to roles and users
# with CQL, invalid shares and timeouts, and the scheduling group that a
# role's connections and queries run in. Ported from scylla-dtest
# sla_per_user_test.py: like the dtests' nodes, the nodes run with one shard
# (unless the dtest asks for more) and PasswordAuthenticator,
# CassandraAuthorizer and CassandraRoleManager, use the SimpleSnitch with
# every node in datacenter1/rack1, and are stopped gracefully at the end of
# each test, as in the dtests' teardown.

import asyncio
import itertools
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import requests
from cassandra import ConsistencyLevel, InvalidRequest, OperationTimedOut
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import EXEC_PROFILE_DEFAULT, NoHostAvailable
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.protocol import SyntaxException
from cassandra.util import Duration

from test import DEBUG_MODES
from test.cluster.util import stop_servers_gracefully
from test.pylib.driver_utils import safe_driver_shutdown
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for

logger = logging.getLogger(__name__)

AUTH_CONFIG = {
    "authenticator": "org.apache.cassandra.auth.PasswordAuthenticator",
    "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer",
    "role_manager": "org.apache.cassandra.auth.CassandraRoleManager",
    "tablets_mode_for_new_keyspaces": "disabled",
    # ccm's populate(n): the SimpleSnitch, every node in datacenter1/rack1.
    "endpoint_snitch": "SimpleSnitch",
}
CASSANDRA = PlainTextAuthProvider(username="cassandra", password="cassandra")


async def prepare(manager: ScyllaClusterManager, nodes=1, smp=1, additional_config=None):
    servers = await manager.servers_add(nodes, config=AUTH_CONFIG | (additional_config or {}), cmdline=["--smp", str(smp)],
                                        property_file=[{"dc": "datacenter1", "rack": "rack1"}] * nodes,
                                        driver_connect_opts={"auth_provider": CASSANDRA})
    return servers, manager.get_cql()


def connect(manager: ScyllaClusterManager, server, user, password, exclusive=False):
    """A session logged in as the user, whose statements default to
    consistency level ONE, as dtest sessions do."""
    if exclusive:
        cluster = manager.con_gen([server.ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr]),
                                  auth_provider=PlainTextAuthProvider(username=user, password=password))
    else:
        cluster = manager.con_gen([server.ip_addr], auth_provider=PlainTextAuthProvider(username=user, password=password))
    session = cluster.connect()
    session.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    return cluster, session


def populate_data(cql, number_of_keys, replication_factor=1):
    """The dtests' populate_data(): create_ks(), create_c1c2_table() and
    insert_c1c2() of the keys k0, k1... at QUORUM."""
    cql.execute(f"CREATE KEYSPACE ks WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{replication_factor}}}")
    cql.execute("USE ks")
    cql.execute("CREATE COLUMNFAMILY cf (key varchar PRIMARY KEY, c1 text, c2 text) WITH comment='test cf' "
                "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {}")
    cql.execute("SELECT * FROM cf LIMIT 1")
    insert = cql.prepare("INSERT INTO ks.cf (key, c1, c2) VALUES (?, 'value1', 'value2')")
    insert.consistency_level = ConsistencyLevel.QUORUM
    execute_concurrent_with_args(cql, insert, [[f"k{k}"] for k in range(number_of_keys)], concurrency=20)


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


class ServiceLevel:
    """The dtests' tools/sla.py ServiceLevel: the name is quoted, attributes
    are set WITH shares/timeout/workload_type. The timeout is the duration
    string; timeout_duration is the Duration LIST SERVICE_LEVEL shows for it."""

    def __init__(self, cql, name, shares=1000, timeout=None, timeout_duration=None):
        self.cql = cql
        self.name = f'"{name}"'
        self.shares = shares
        self.timeout = timeout
        self.timeout_duration = timeout_duration

    @staticmethod
    def attributes(shares=None, timeout=None):
        attrs = []
        if shares is not None:
            attrs.append(f"shares = {shares}")
        if timeout is not None:
            attrs.append(f"timeout = '{timeout}'")
        return (" WITH " + " AND ".join(attrs)) if attrs else ""

    def create(self):
        self.cql.execute(f"CREATE SERVICE_LEVEL IF NOT EXISTS {self.name}{self.attributes(self.shares, self.timeout)}")
        return self

    def alter(self, new_shares=None):
        self.cql.execute(f"ALTER SERVICE_LEVEL {self.name} {self.attributes(new_shares)}")
        self.shares = new_shares

    def drop(self):
        self.cql.execute(f"DROP SERVICE_LEVEL IF EXISTS {self.name}")

    def matches(self, row):
        return (row.shares or 1000) == (self.shares or 1000) and row.timeout == self.timeout_duration and row.workload_type is None

    def listed(self):
        rows = self.cql.execute(f"LIST SERVICE_LEVEL {self.name}").all()
        assert len(rows) <= 1
        return rows[0] if rows else None


class Role:
    """CREATE ROLE with the dtests' option rendering (password quoted,
    booleans as True)."""

    def __init__(self, cql, name, password=None, login=False, superuser=False):
        self.cql, self.name, self.password, self.login, self.superuser = cql, name, password, login, superuser

    def create(self):
        options = []
        if self.password:
            options.append(f"password = '{self.password}'")
        if self.login:
            options.append(f"login = {self.login}")
        if self.superuser:
            options.append(f"superuser = {self.superuser}")
        self.cql.execute(f"CREATE ROLE {self.name}" + ((" WITH " + " AND ".join(options)) if options else ""))
        return self


class User(Role):
    """CREATE USER WITH PASSWORD '...' [SUPERUSER|NOSUPERUSER]."""

    def __init__(self, cql, name, password=None, login=None, superuser=None):
        super().__init__(cql, name, password, login, superuser)

    def create(self):
        superuser = "" if self.superuser is None else " SUPERUSER" if self.superuser else " NOSUPERUSER"
        self.cql.execute(f"CREATE USER {self.name} WITH PASSWORD '{self.password or ''}'{superuser}")
        return self


def attach(entity, sl):
    entity.cql.execute(f"ATTACH SERVICE_LEVEL {sl.name} TO {entity.name}")


def detach(entity):
    entity.cql.execute(f"DETACH SERVICE_LEVEL FROM {entity.name}")


def create_entity_with_service_level(entity, sl):
    sl.create()
    entity.create()
    attach(entity, sl)
    return entity


def validate_sl_list(cql, expected):
    """All the service levels, except the driver's, are the expected ones."""
    if not expected:
        ServiceLevel(cql, "dummy").create()
        names = [r.service_level for r in cql.execute("LIST ALL SERVICE_LEVELS") if r.service_level not in ("driver", "dummy")]
        assert not names, names
        return
    rows = [r for r in cql.execute("LIST ALL SERVICE_LEVELS") if r.service_level != "driver"]
    assert len(rows) == len(expected), rows
    for sl in expected:
        assert sl.matches(sl.listed()), sl.name


def validate_attached(cql, entity, expected):
    rows = cql.execute(f"LIST ATTACHED SERVICE_LEVEL OF {entity.name}").all()
    assert len(rows) == len(expected), rows
    for row in rows:
        matching = [sl for sl in expected if sl.name == f'"{row.service_level}"']
        assert len(matching) == 1, row
        assert matching[0].matches(ServiceLevel(cql, row.service_level).listed())


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("sla_name", ["sla1", "Sla1"])
async def test_sla(manager: ScyllaClusterManager, sla_name):
    """A service level with 100 shares, named in lower or mixed case, is
    listed with its shares."""
    servers, cql = await prepare(manager)
    sl = ServiceLevel(cql, sla_name, shares=100).create()
    validate_sl_list(cql, [sl])


ENTITIES = {
    "attach_to_role": (Role, "role1", None, False),
    "attach_to_user": (User, "user1", None, None),
    "attach_to_auth_role": (Role, "auth_role", "auth", True),
    "attach_to_auth_user": (User, "auth_user", None, None),
}


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("entity", list(ENTITIES))
async def test_sla_attached_to_entity(manager: ScyllaClusterManager, entity):
    """A service level attached to a role or a user, with and without
    login and password, is listed as attached to it."""
    servers, cql = await prepare(manager)
    cls, name, password, login = ENTITIES[entity]
    sl = ServiceLevel(cql, "sla1", shares=100)
    e = create_entity_with_service_level(cls(cql, name, password=password, login=login), sl)
    validate_sl_list(cql, [sl])
    validate_attached(cql, e, [sl])


async def stress(server, user, password):
    """The dtest's node.stress() of "write cl=ALL n=10 -mode cql3 native
    user=... password=...": cassandra-stress against the node, logged in as
    the given user (cassandra_stress() in test/cluster/util.py always logs
    in as cassandra)."""
    proc = await asyncio.create_subprocess_exec(
        "cassandra-stress", "write", "cl=ALL", "n=10", "-mode", "cql3", "native", f"user={user}", f"password={password}",
        "-node", server.ip_addr, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    out, _ = await proc.communicate()
    assert proc.returncode == 0, f"cassandra-stress as {user} failed: {out.decode()}"


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("entity", ["attach_to_role", "attach_to_user"])
async def test_sla_usage(manager: ScyllaClusterManager, entity):
    """
    A superuser role or user with a service level attached can run a
    cassandra-stress workload, like the default superuser. As in the dtest
    (ccm's node.stress(), vnodes), cassandra-stress creates keyspace1 itself.
    """
    servers, cql = await prepare(manager)
    sl = ServiceLevel(cql, "sla1", shares=100)
    if entity == "attach_to_role":
        e = Role(cql, "auth_role", password="password", login=True, superuser=True)
    else:
        e = User(cql, "auth_user", password="password", superuser=True)
    create_entity_with_service_level(e, sl)
    validate_sl_list(cql, [sl])
    validate_attached(cql, e, [sl])
    await stress(servers[0], "cassandra", "cassandra")
    await stress(servers[0], e.name, e.password)


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("entity", ["with_role", "with_user"])
async def test_replace_sla(manager: ScyllaClusterManager, entity):
    """Detaching a role's or user's service level and attaching another one
    replaces it; both service levels still exist."""
    servers, cql = await prepare(manager)
    sl_50 = ServiceLevel(cql, "sla50", shares=50).create()
    sl_300 = ServiceLevel(cql, "sla300", shares=300).create()
    e = (Role(cql, "test_role") if entity == "with_role" else User(cql, "test_user")).create()
    attach(e, sl_50)
    validate_sl_list(cql, [sl_50, sl_300])
    validate_attached(cql, e, [sl_50])
    detach(e)
    attach(e, sl_300)
    validate_sl_list(cql, [sl_50, sl_300])
    validate_attached(cql, e, [sl_300])


# Ported from scylla-dtest sla_per_user_test.py::TestSLA::test_attach_2_slas_to_role.
# Attaching a second service level to a role or a user with one attached
# replaces the attached one; both service levels still exist.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("entity", ["with_role", "with_user"])
async def test_attach_2_slas_to_role(manager: ScyllaClusterManager, entity):
    servers, cql = await prepare(manager)
    sl100 = ServiceLevel(cql, "sla1", shares=100)
    e = Role(cql, "test_role") if entity == "with_role" else User(cql, "test_user")
    create_entity_with_service_level(e, sl100)
    validate_sl_list(cql, [sl100])
    validate_attached(cql, e, [sl100])
    sl200 = ServiceLevel(cql, "sla2", shares=200).create()
    attach(e, sl200)
    validate_sl_list(cql, [sl100, sl200])
    validate_attached(cql, e, [sl200])


def retry_till_success(fun, *args, timeout=60, bypassed_exception=Exception, **kwargs):
    """The dtests' retry_till_success()."""
    deadline = time.time() + timeout
    while True:
        try:
            return fun(*args, **kwargs)
        except bypassed_exception:
            if time.time() > deadline:
                raise
            time.sleep(0.25)


def create_sl_in_loop(session, random_range, name_iter, service_level_name=None, timeout=300):
    """The dtest's chaos loop: for the given time, create a service level
    (named after service_level_name, or a fresh one), wait for it with a
    read barrier, alter it and drop it, tolerating the errors of racing
    with the other loops."""
    end_time = time.time() + timeout
    i = 0
    while time.time() < end_time:
        sl_name = f"sl{service_level_name or next(name_iter)}"
        i += 1
        sl = None
        try:
            logger.info(f"Iteration {i}")
            # Prevent service level creation in exactly same time from all threads.
            time.sleep(random.randint(1, 10))
            new_sl = ServiceLevel(session, sl_name, shares=random.randint(*random_range))
            # Under this concurrency a group0 write can retry past the client timeout, see SCYLLADB-4652.
            sl = retry_till_success(new_sl.create, timeout=120, bypassed_exception=OperationTimedOut)
            # Wait for the service level to be propagated to all the nodes: the dtests' read_barrier().
            session.execute("drop table if exists nosuchkeyspace.nosuchtable")
            retry_till_success(sl.alter, new_shares=random.randint(*random_range), timeout=120, bypassed_exception=OperationTimedOut)
        except NoHostAvailable as exc:
            if "no more scheduling groups exist" in str(exc.errors) or "concurrent modification" in str(exc.errors):
                logger.warning(str(exc))
            else:
                raise
        except InvalidRequest as exc:
            if f"The service level '{sl_name}' doesn't exist" in str(exc) or "no more scheduling groups exist" in str(exc):
                logger.warning(str(exc))
            else:
                raise
        finally:
            if sl:
                try:
                    retry_till_success(sl.drop, timeout=120, bypassed_exception=OperationTimedOut)
                except NoHostAvailable as exc:
                    if "concurrent modification" not in str(exc.errors):
                        raise
                    logger.warning(str(exc))
                except InvalidRequest as exc:
                    if f"The service level '{sl_name}' doesn't exist" not in str(exc):
                        raise
                    logger.warning(str(exc))


# Ported from scylla-dtest sla_per_user_test.py::TestSLA::test_chaos_sl_creation_altering_deletion.
# For five minutes, ten loops on the four nodes (of four shards) create,
# alter and drop service levels concurrently, two of them re-creating the
# same name; the only errors are those of the races.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.tier2
async def test_chaos_sl_creation_altering_deletion(manager: ScyllaClusterManager):
    servers, cql = await prepare(manager, nodes=4, smp=4)
    connections = [connect(manager, s, "cassandra", "cassandra", exclusive=True) for s in servers]
    try:
        session_n1, session_n2, session_n3, session_n4 = [session for _, session in connections]

        def chaos():
            with ThreadPoolExecutor(max_workers=10) as tp:
                name_iter = itertools.count(start=3000)
                threads = [
                    # Create and drop the same named service level.
                    tp.submit(create_sl_in_loop, session=session_n4, random_range=(10, 1000), name_iter=name_iter, service_level_name="_reuse_1"),
                    # Create and drop the same named service level.
                    tp.submit(create_sl_in_loop, session=session_n3, random_range=(10, 1000), name_iter=name_iter, service_level_name="_reuse_2"),
                    tp.submit(create_sl_in_loop, session=session_n2, random_range=(1, 20), name_iter=name_iter),
                    tp.submit(create_sl_in_loop, session=session_n1, random_range=(1, 20), name_iter=name_iter),
                    tp.submit(create_sl_in_loop, session=session_n3, random_range=(50, 500), name_iter=name_iter),
                    tp.submit(create_sl_in_loop, session=session_n4, random_range=(500, 1000), name_iter=name_iter),
                    tp.submit(create_sl_in_loop, session=session_n1, random_range=(1, 20), name_iter=name_iter),
                    tp.submit(create_sl_in_loop, session=session_n3, random_range=(50, 500), name_iter=name_iter),
                    tp.submit(create_sl_in_loop, session=session_n4, random_range=(500, 1000), name_iter=name_iter),
                    tp.submit(create_sl_in_loop, session=session_n4, random_range=(500, 1000), name_iter=name_iter),
                ]
                for thread in threads:
                    thread.result(timeout=480)

        await asyncio.to_thread(chaos)
    finally:
        for cluster, _ in connections:
            safe_driver_shutdown(cluster)


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("entity", ["with_role", "with_user"])
async def test_drop_sl_and_attach_new_to_role(manager: ScyllaClusterManager, entity):
    """Dropping the attached service level detaches it; another one can then
    be attached."""
    servers, cql = await prepare(manager)
    sl100 = ServiceLevel(cql, "sla1", shares=100)
    e = Role(cql, "test_role") if entity == "with_role" else User(cql, "test_user")
    create_entity_with_service_level(e, sl100)
    validate_sl_list(cql, [sl100])
    validate_attached(cql, e, [sl100])
    sl100.drop()
    validate_attached(cql, e, [])
    sl200 = ServiceLevel(cql, "sla2", shares=200).create()
    attach(e, sl200)
    validate_sl_list(cql, [sl200])
    validate_attached(cql, e, [sl200])


@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_update_not_existing_sla(manager: ScyllaClusterManager):
    """ALTER of a service level that doesn't exist fails."""
    servers, cql = await prepare(manager)
    with pytest.raises(InvalidRequest, match="The service level 'sla1' doesn't exist."):
        ServiceLevel(cql, "sla1").alter(new_shares=100)


async def wrong_shares(manager: ScyllaClusterManager, shares):
    """Creating a service level with shares out of 1-1000 fails."""
    servers, cql = await prepare(manager)
    with pytest.raises(SyntaxException, match=r"'SHARES' can only take values of 1-1000 \(given %d\)" % shares):
        ServiceLevel(cql, "sla1", shares=shares).create()


# Ported from scylla-dtest sla_per_user_test.py::TestSLANegativeTests::test_create_sla_with_more_1000_shares.
# A service level with 1001 shares is rejected.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_create_sla_with_more_1000_shares(manager: ScyllaClusterManager):
    await wrong_shares(manager, 1001)


# Ported from scylla-dtest sla_per_user_test.py::TestSLANegativeTests::test_create_sla_with_0_shares.
# A service level with 0 shares is rejected.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_create_sla_with_0_shares(manager: ScyllaClusterManager):
    await wrong_shares(manager, 0)


# Ported from scylla-dtest sla_per_user_test.py::TestSLANegativeTests::test_create_sla_with_negative_shares.
# A service level with -1 shares is rejected.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_create_sla_with_negative_shares(manager: ScyllaClusterManager):
    await wrong_shares(manager, -1)


# Ported from scylla-dtest sla_per_user_test.py::TestSLATimeouts::test_timeout_valid_values.
# A service level with a timeout given as a large number of milliseconds or
# as a compound duration is listed with it, and a role it is attached to
# can read a table of 1000 rows.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("timeout,nanoseconds", [
    ("1000000ms", 1000000 * 10**6),
    ("5h", 5 * 3600 * 10**9),
    ("23h2m2s2ms", ((23 * 60 + 2) * 60 + 2) * 10**9 + 2 * 10**6),
], ids=["1000000ms", "5h", "23h2m2s2ms"])
async def test_timeout_valid_values(manager: ScyllaClusterManager, timeout, nanoseconds):
    servers, cql = await prepare(manager)
    populate_data(cql, 1000)
    role = Role(cql, "test_role", password="test_role", login=True).create()
    sl = ServiceLevel(cql, "sl1", shares=None, timeout=timeout, timeout_duration=Duration(0, 0, nanoseconds)).create()
    attach(role, sl)
    cql.execute(f"GRANT SELECT ON KEYSPACE ks TO {role.name};")
    assert sl.matches(sl.listed())

    cluster, session = connect(manager, servers[0], role.name, role.password)
    try:
        assert session.execute("SELECT * FROM ks.cf").all()
    finally:
        safe_driver_shutdown(cluster)


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("timeout,message", [
    ("1d", "Timeout values cannot be expressed in days/months"),
    ("2mo", "Timeout values cannot be expressed in days/months"),
    ("1000ns", "Timeout values must be expressed in millisecond granularity"),
    ("-1200ms", "Timeout values must be nonnegative"),
], ids=["using_days_as_values", "using_months_as_values", "nanosecond_value", "negative_timeout_value"])
async def test_invalid_timeout_values(manager: ScyllaClusterManager, timeout, message):
    """Service level timeouts in days or months, finer than milliseconds, or
    negative, are rejected."""
    servers, cql = await prepare(manager)
    with pytest.raises(InvalidRequest, match=f".*{message}.*"):
        ServiceLevel(cql, "sl1", shares=None, timeout=timeout).create()


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_sla_configuration(manager: ScyllaClusterManager):
    """
    On every node of three, the connections of a role with a service level
    attached switch to the service level's scheduling group, and its queries
    run in that scheduling group and use its reader concurrency semaphore.
    """
    servers, cql = await prepare(manager, nodes=3, additional_config={"service_levels_interval_ms": 500})
    populate_data(cql, 10)

    role = Role(cql, "test_role", password="test_role", login=True, superuser=True)
    sl = ServiceLevel(cql, "test_sl", shares=500)
    create_entity_with_service_level(role, sl)
    sg_name = "sl:test_sl"
    time.sleep(1)

    connections = [connect(manager, s, role.name, role.password, exclusive=True) for s in servers]
    clusters = [c for c, _ in connections]
    try:
        sessions = [session for _, session in connections]

        for server, session in zip(servers, sessions):
            async def in_scheduling_group():
                session.execute("select * from ks.cf")
                connections = requests.get(f"http://{server.ip_addr}:10000/service_levels/count_connections").json()
                return True if role.name in connections.get(sg_name, {}) else None
            await wait_for(in_scheduling_group, time.time() + 30, label=f"{role.name} connections in {sg_name}")

        for session in sessions:
            async def query_in_scheduling_group():
                trace = session.execute("select * from ks.cf", trace=True).get_query_trace()
                semaphore_events = 0
                for e in trace.events:
                    if sg_name not in e.thread_name:
                        return None
                    if "[reader concurrency semaphore" in e.description:
                        semaphore_events += 1
                        assert f"[reader concurrency semaphore {sg_name}]" in e.description
                return True if semaphore_events > 0 else None
            await wait_for(query_in_scheduling_group, time.time() + 30, label=f"query under {sg_name}")
    finally:
        for c in clusters:
            safe_driver_shutdown(c)
