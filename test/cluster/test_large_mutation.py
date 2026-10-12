#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Size limits of requests and schema changes, ported from scylla-dtest
limits_test.py (TestLimits), one test per dtest, on a single node like the
dtests' populate(1) (SimpleSnitch, datacenter1/rack1): a mutation is
accepted as long as it (with its overhead) fits into the commitlog, even if
it is larger than a commitlog segment, and is rejected cleanly if it
doesn't fit, over plain and TLS client connections; tables of up to 65535
columns can be created and written, and one of 131071 columns is refused
because its schema change is too large.
"""

import asyncio
import logging
import math

import pytest
from cassandra import InvalidRequest, WriteFailure
from cassandra.cluster import EXEC_PROFILE_DEFAULT, NoHostAvailable

from test.cluster.test_client_encryption import certificates_for_ip, client_encryption, session_to, wait_for_binary_proto
from test.cluster.util import stop_servers_gracefully
from test.pylib.driver_utils import safe_driver_shutdown
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# ccm's populate(1): the SimpleSnitch; vnodes, like the dtests.
CONFIG = {"tablets_mode_for_new_keyspaces": "disabled", "endpoint_snitch": "SimpleSnitch"}
MAX_COLUMNS = 128 * 1024
# Timeout in seconds for long-running DDL operations (CREATE TABLE with many columns).
TIMEOUT = 90


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


async def start_node(manager: ScyllaClusterManager, tmp_path, mode: str, cmdline: list[str]):
    """The dtests' populate(1).start(jvm_args=...), with client encryption
    for the "SSL" mode (populate_cluster_with_ssl_enabled()). Returns the
    server and a function opening a new session to it (the dtests'
    patient_cql_connection() / create_cql_session_with_ssl())."""
    if mode == "SSL":
        names = {"certificate": str(tmp_path / "ccm_node.pem"), "keyfile": str(tmp_path / "ccm_node.key")}
        server = await manager.server_add(start=False, cmdline=cmdline, config={**CONFIG, "client_encryption_options": client_encryption(names)})
        certs, _, _ = certificates_for_ip(tmp_path, server.ip_addr)
        await manager.server_start(server.server_id, connect_driver=False)
        await wait_for_binary_proto(manager, server)
    else:
        certs = None
        server = await manager.server_add(cmdline=cmdline, config=CONFIG)
    clusters = []

    async def create_session():
        # The SSL session checks the server's certificate and host name, like
        # create_cql_session_with_ssl().
        cluster, session = await session_to(server.ip_addr, 9042, use_ssl=mode == "SSL", certs=certs, ca_certs_required=mode == "SSL")
        clusters.append(cluster)
        return session

    return server, create_session, clusters


async def create_ks(session, name: str, rf: int):
    """The dtests' create_ks()."""
    await session.run_async(f"CREATE KEYSPACE {name} WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}")
    await session.run_async(f"USE {name}")


# Ported from scylla-dtest limits_test.py::TestLimits::test_accepted_large_request.
# With 2 shards and a 1 GB commitlog, a 30 MB insert, larger than a 32 MB
# commitlog segment can hold in one entry, is accepted and read back through
# a new session, and a 6 MB insert after it too. Reproduces #19472.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("mode", ["SSL", "non-SSL"])
async def test_accepted_large_request(manager: ScyllaClusterManager, tmp_path, mode):
    _, create_session, clusters = await start_node(manager, tmp_path, mode, ["--smp", "2", "--commitlog-total-space-in-mb", "1024", "-m", "2G"])
    try:
        session = await create_session()
        await create_ks(session, "test_keyspace", 1)
        await session.run_async("create table test_keyspace.test_table(id int primary key, test_string text);")
        long_string = "scylla" * 5 * 1024 * 1024
        id_value = 17
        await session.run_async(f"insert into test_keyspace.test_table (id, test_string) values ({id_value}, '{long_string}');")

        session = await create_session()
        output = await session.run_async(f"select test_string from test_keyspace.test_table where id = {id_value};")
        assert long_string == output[0].test_string, "Expected the table contains very long string"

        short_string = "scylla" * 1024 * 1024
        await session.run_async(f"insert into test_keyspace.test_table (id, test_string) values ({id_value}, '{short_string}');")
        output = await session.run_async(f"select test_string from test_keyspace.test_table where id = {id_value};")
        assert short_string == output[0].test_string, "Expected to get the regular string inserted, but did not find it in the table!"
    finally:
        for cluster in clusters:
            safe_driver_shutdown(cluster)


# Ported from scylla-dtest limits_test.py::TestLimits::test_request_too_large.
# With 2 shards sharing a 60 MB commitlog of 6 MB segments, a 30 MB insert
# can't fit: it fails, writes nothing (checked through a new session), and a
# 6 MB insert afterwards succeeds.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
@pytest.mark.parametrize("mode", ["SSL", "non-SSL"])
async def test_request_too_large(manager: ScyllaClusterManager, tmp_path, mode):
    _, create_session, clusters = await start_node(manager, tmp_path, mode, ["--smp", "2", "--commitlog-total-space-in-mb", "60",
                                                                             "--commitlog-segment-size-in-mb", "6"])
    try:
        session = await create_session()
        await create_ks(session, "test_keyspace", 1)
        await session.run_async("create table test_keyspace.test_table(id int primary key, test_string text);")
        long_string = "scylla" * 5 * 1024 * 1024
        id_value = 17
        manager.ignore_log_patterns.append("Could not write mutation test_keyspace:test_table.*std::invalid_argument.*Mutation.*is too large")
        with pytest.raises((NoHostAvailable, WriteFailure, InvalidRequest)):
            await session.run_async(f"insert into test_keyspace.test_table (id, test_string) values ({id_value}, '{long_string}');")

        session = await create_session()
        output = await session.run_async(f"select test_string from test_keyspace.test_table where id = {id_value};")
        assert not output, "Expected the table was empty, but id had rows inserted!"

        short_string = "scylla" * 1024 * 1024
        await session.run_async(f"insert into test_keyspace.test_table (id, test_string) values ({id_value}, '{short_string}');")
        output = await session.run_async(f"select test_string from test_keyspace.test_table where id = {id_value};")
        assert short_string == output[0].test_string, "Expected to get the regular string inserted, but did not find it in the table!"
    finally:
        for cluster in clusters:
            safe_driver_shutdown(cluster)


def do_test_max_columns(session, count: int, expect_failure: bool, manager: ScyllaClusterManager):
    """The dtests' _do_test_max_columns(): a table test1 of `count` columns
    (key0.., and the primary key blub), a row written to all of them, the
    table dropped; or, when expect_failure, the CREATE refused because the
    schema change is too large."""
    count = max(count - 1, 0)
    keys = "".join(f"key{i}, " for i in range(count))
    keys_create = "".join(f"key{i} int, " for i in range(count))
    values = "1, " * count
    create = f"CREATE TABLE test1 ({keys_create} blub int PRIMARY KEY,)"
    if expect_failure:
        expected_error = r"Command size \d+ is greater than the configured limit \d+"
        manager.ignore_log_patterns.append(expected_error)
        with pytest.raises(Exception, match=expected_error):
            session.execute(create)
        return
    session.execute(create)
    session.execute(f"insert into ks.test1  ({keys} blub) values ({values} 1);")
    session.execute("DROP TABLE test1")
    session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)


# Ported from scylla-dtest limits_test.py::TestLimits::test_max_columns_and_query_parameters.
# Tables of 1, 3, 7, ... 65535 columns are created, written to and dropped;
# one of 131071 columns is refused: "Command size ... is greater than the
# configured limit". The node has 4 GB and the dtest's long timeouts.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest runs it nightly in release mode only (the client times out in debug mode)")
async def test_max_columns_and_query_parameters(manager: ScyllaClusterManager):
    config = {**CONFIG, "query_tombstone_page_limit": 9999999, "request_timeout_in_ms": int(TIMEOUT * 1000),
              "group0_raft_op_timeout_in_ms": int(TIMEOUT * 2 * 1000),
              # Scylla's default, as on the dtest nodes; test.py turns it off.
              "auto_snapshot": True}
    server = await manager.server_add(cmdline=["-m", "4G"], config=config)
    cluster, session = await session_to(server.ip_addr, 9042, use_ssl=False)
    try:
        session.get_execution_profile(EXEC_PROFILE_DEFAULT).request_timeout = TIMEOUT
        await create_ks(session, "ks", 1)
        session.execute("UPDATE system.config SET value='100000' WHERE name='query_tombstone_page_limit'")
        count = 1
        for _ in range(int(math.log(MAX_COLUMNS, 2))):
            count <<= 1
            await asyncio.to_thread(do_test_max_columns, session, count - 1, count == MAX_COLUMNS, manager)
    finally:
        safe_driver_shutdown(cluster)
