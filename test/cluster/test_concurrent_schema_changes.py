#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Many tables, alters, indexes and materialized views created concurrently,
# and schema changes made on different nodes, while nodes are down or
# decommissioned, and under cassandra-stress write and compaction load,
# ported from scylla-dtest concurrent_schema_changes_test.py. After the changes, every
# node reports a single schema version in "nodetool describecluster". As on
# dtest nodes, keyspaces use vnodes and auto_snapshot is on, so dropped
# tables and keyspaces are snapshotted.

import asyncio
import re
import subprocess
import time
from random import randrange

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement

from test.cluster.util import cassandra_stress, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

CONFIG = {"tablets_mode_for_new_keyspaces": "disabled", "auto_snapshot": True}
# Clusters the dtests populate with a node count instead of a topology use
# the SimpleSnitch, with every node in datacenter1/rack1.
SIMPLE_SNITCH = {"endpoint_snitch": "SimpleSnitch"}


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def racks(n, dc="datacenter1"):
    return [{"dc": dc, "rack": f"rack{i}"} for i in range(1, n + 1)]


async def nodetool(manager: ScyllaClusterManager, server, *args):
    """Runs "scylla nodetool" against the node, like ccm's node.nodetool()."""
    exe = await manager.server_get_exe(server.server_id)
    return await asyncio.to_thread(subprocess.run, [exe, "nodetool", "-h", server.ip_addr, *args],
                                   check=True, capture_output=True, text=True)


def connect(manager: ScyllaClusterManager, server, exclusive=False):
    """A session whose statements default to consistency level ONE, as dtest sessions do."""
    if exclusive:
        cql = manager.con_gen([server.ip_addr], load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()
    else:
        cql = manager.con_gen([server.ip_addr]).connect()
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    return cql


def create_ks(cql, name, rf):
    cql.execute(f"CREATE KEYSPACE {name} WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}")


async def validate_schema_consistent(manager: ScyllaClusterManager, node, num_attempts=60):
    """Waits until "nodetool describecluster" on the node lists one schema version."""
    for attempt in range(num_attempts):
        out = (await nodetool(manager, node, "describecluster")).stdout
        schemas = out.split("Schema versions:")[1].strip()
        num_schemas = len(re.findall(r"\[.*?\]", schemas))
        if num_schemas == 1:
            return
        assert attempt < num_attempts - 1, f"There were multiple schema versions: {schemas}"
        await asyncio.sleep(1)


def prepare_for_changes(cql, namespace="ns1", rf=2):
    """A keyspace with a table with an index and a row, a table and a
    keyspace to be dropped later."""
    create_ks(cql, f"ks_{namespace}", rf)
    cql.execute(f"USE ks_{namespace}")
    cql.execute(f"""
        CREATE TABLE cf_{namespace} (
            col1 text PRIMARY KEY,
            col2 text,
            col3 text
        );
    """)
    cql.execute(f"INSERT INTO cf_{namespace} (col1, col2, col3) VALUES ('a', 'b', 'c');")
    cql.execute(f"CREATE INDEX index_{namespace} ON cf_{namespace}(col2)")
    cql.execute(f"""
        CREATE TABLE cf2_{namespace} (
            col1 uuid PRIMARY KEY,
            col2 text,
            col3 text
        );
    """)
    create_ks(cql, f"ks2_{namespace}", rf)


def make_schema_changes(cql, namespace="ns1", rf=2):
    """Drops and creates a keyspace and a table, alters a table, and creates
    and drops an index."""
    cql.execute(f"USE ks_{namespace}")
    cql.execute(f"DROP KEYSPACE ks2_{namespace}")
    create_ks(cql, f"ks3_{namespace}", rf)
    cql.execute(f"USE ks_{namespace}")
    cql.execute(f"DROP COLUMNFAMILY cf2_{namespace}")
    cql.execute(f"""
        CREATE TABLE cf3_{namespace} (
            col1 uuid PRIMARY KEY,
            col2 text,
            col3 text,
            col4 text
        );
    """)
    cql.execute(f"""
       ALTER COLUMNFAMILY cf_{namespace}
       ADD col4 text;
    """)
    cql.execute(f"CREATE INDEX index2_{namespace} ON cf_{namespace}(col3)")
    cql.execute(f"DROP INDEX index_{namespace}")


async def wait_for_view_built(manager: ScyllaClusterManager, cql, ks, view, servers, timeout=600):
    """The dtests' wait_for_view(): polls system_distributed.view_build_status
    until every live node reports the view built."""
    host_ids = {str(await manager.get_host_id(s.server_id)) for s in servers}
    query = SimpleStatement(f"SELECT host_id,status FROM system_distributed.view_build_status WHERE keyspace_name = '{ks}' AND view_name = '{view}'",
                            consistency_level=ConsistencyLevel.ONE if len(servers) == 1 else ConsistencyLevel.QUORUM)
    deadline = time.time() + timeout
    while True:
        done = {str(r.host_id) for r in cql.execute(query) if r.status == "SUCCESS"}
        if host_ids <= done:
            return
        assert time.time() < deadline, f"view {ks}.{view} not built on every node: {done}"
        await asyncio.sleep(1)


async def index_is_built(manager: ScyllaClusterManager, cql, servers, ks_name, table_name, index_name):
    """The dtests' index_is_built(): the index's view is built and the index is in the schema."""
    await wait_for_view_built(manager, cql, ks_name, f"{index_name}_index", servers)
    return len(list(cql.execute(f"SELECT * FROM system_schema.indexes WHERE keyspace_name = '{ks_name}' and table_name ='{table_name}' AND index_name='{index_name}'"))) == 1


def do_lots_of_schema_actions(cql):
    """Tables to alter and to index, then concurrent table creations, column
    drops and adds, and index creations on them."""
    for n in range(20):
        cql.execute(f"create table alter_me_{n} (id uuid primary key, s1 int, s2 int, s3 int, s4 int, s5 int, s6 int, s7 int);")
        cql.execute(f"create table index_me_{n} (id uuid primary key, c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int);")
    cmds = []
    for n in range(20):
        cmds.append((f"create table new_table_{n} (id uuid primary key, c1 int, c2 int, c3 int, c4 int);", ()))
        for a in range(1, 8):
            cmds.append((f"alter table alter_me_{n} drop s{a};", ()))
            cmds.append((f"alter table alter_me_{n} add c{a} int;", ()))
            cmds.append((f"create index ix_index_me_{n}_c{a} on index_me_{n} (c{a});", ()))
    for success, result in execute_concurrent(cql, cmds, concurrency=100, raise_on_first_error=True):
        assert success, f"didn't get success: {result}"


async def verify_lots_of_schema_actions(manager: ScyllaClusterManager, cql, servers):
    """One schema version on every node, with every new table."""
    cql.cluster.control_connection.wait_for_schema_agreement()
    await validate_schema_consistent(manager, servers[0], num_attempts=120)
    for node in servers[1:]:
        await validate_schema_consistent(manager, node, num_attempts=1)
    cql.cluster.refresh_schema_metadata()
    tables = cql.cluster.metadata.keyspaces["lots_o_churn"].tables
    for n in range(20):
        assert f"new_table_{n}" in tables


# Ported from scylla-dtest concurrent_schema_changes_test.py::TestConcurrentSchemaChanges::test_create_lots_of_tables_concurrently.
# 250 tables created concurrently through one node end in one schema version
# on every node, with all the tables.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_create_lots_of_tables_concurrently(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config={**CONFIG, **SIMPLE_SNITCH}, property_file=racks(1) * 3)
    node1, node2, node3 = servers
    cql = connect(manager, node1)
    cql.execute("create keyspace lots_o_tables WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};")
    cql.execute("use lots_o_tables")

    cmds = [(f"create table t_{n} (id uuid primary key, c1 text, c2 text, c3 text, c4 text)", ()) for n in range(250)]
    for success, result in execute_concurrent(cql, cmds, raise_on_first_error=True, concurrency=200):
        assert success, f"didn't get success on table create: {result}"

    await validate_schema_consistent(manager, node1)
    await validate_schema_consistent(manager, node2, num_attempts=1)
    await validate_schema_consistent(manager, node3, num_attempts=1)

    cql.cluster.refresh_schema_metadata()
    table_meta = cql.cluster.metadata.keyspaces["lots_o_tables"].tables
    assert 250 == len(table_meta), f"expected 250, got len(table_meta)={len(table_meta)} "


# Ported from scylla-dtest concurrent_schema_changes_test.py::TestConcurrentSchemaChanges::test_create_lots_of_alters_concurrently.
# 500 ALTER TABLE ... ADD on 10 tables, run concurrently through one node, all
# land: the tables have 510 columns, in one schema version on every node.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_create_lots_of_alters_concurrently(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config={**CONFIG, **SIMPLE_SNITCH}, property_file=racks(1) * 3)
    node1 = servers[0]
    cql = connect(manager, node1)
    cql.execute("create keyspace lots_o_alters WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};")
    cql.execute("use lots_o_alters")
    for n in range(10):
        cql.execute(f"create table base_{n} (id uuid primary key)")

    cmds = [(f"alter table base_{randrange(0, 10)} add c_{n} int", ()) for n in range(500)]
    for success, result in execute_concurrent(cql, cmds, raise_on_first_error=True, concurrency=150):
        assert success, f"didn't get success on table create: {result}"

    # All nodes must converge before the metadata is read.
    await validate_schema_consistent(manager, node1)

    cql.cluster.refresh_schema_metadata()
    table_meta = cql.cluster.metadata.keyspaces["lots_o_alters"].tables
    column_ct = sum([len(table.columns) for table in table_meta.values()])
    # The primary keys and the added columns.
    assert 510 == column_ct, f"expected 510, column_ct = {column_ct}"


# Ported from scylla-dtest concurrent_schema_changes_test.py::TestConcurrentSchemaChanges::test_create_lots_of_indexes_concurrently.
# 10 indexes created concurrently on 5 tables of 1000 rows each are all in
# the schema, and once built every row is found through each of them.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_create_lots_of_indexes_concurrently(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config={**CONFIG, **SIMPLE_SNITCH}, property_file=racks(1) * 2)
    node1, node2 = servers
    cql = connect(manager, node1)
    cql.execute("create keyspace lots_o_indexes WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};")
    cql.execute("use lots_o_indexes")
    for n in range(5):
        cql.execute(f"create table base_{n} (id uuid primary key, c1 int, c2 int)")
        for ins in range(1000):
            cql.execute(f"insert into base_{n} (id, c1, c2) values (uuid(), {ins}, {ins})")

    cmds = []
    for n in range(5):
        cmds.append((f"create index ix_base_{n}_c1 on base_{n} (c1)", ()))
        cmds.append((f"create index ix_base_{n}_c2 on base_{n} (c2)", ()))
    for success, result in execute_concurrent(cql, cmds, raise_on_first_error=True):
        assert success, f"didn't get success on table create: {result}"

    cql.cluster.control_connection.wait_for_schema_agreement()
    cql.cluster.refresh_schema_metadata()
    index_meta = cql.cluster.metadata.keyspaces["lots_o_indexes"].indexes
    await validate_schema_consistent(manager, node1)
    await validate_schema_consistent(manager, node2, num_attempts=1)
    assert 10 == len(index_meta), f"expect 10 , got len(index_meta)={len(index_meta)}"
    for n in range(5):
        assert f"ix_base_{n}_c1" in index_meta, f"ix_base_{n}_c1 not found in {index_meta}"
        assert f"ix_base_{n}_c2" in index_meta, f"ix_base_{n}_c2 not found in in {index_meta}"

    # Wait for the indexes to be built.
    for n in range(5):
        for c in ("c1", "c2"):
            assert await index_is_built(manager, cql, servers, "lots_o_indexes", f"base_{n}", f"ix_base_{n}_{c}")
    for n in range(5):
        for ins in range(1000):
            assert len(list(cql.execute(f"select * from base_{n} where c1 = {ins}"))) == 1
            assert len(list(cql.execute(f"select * from base_{n} where c2 = {ins}"))) == 1


# Ported from scylla-dtest concurrent_schema_changes_test.py::TestConcurrentSchemaChanges::test_create_lots_of_mv_concurrently.
# 10 materialized views created one after the other on a table of 4000 rows
# are all in the schema, and each has all the rows.
# Not gating in scylla-dtest (scylladb#14934).
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_create_lots_of_mv_concurrently(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config={**CONFIG, **SIMPLE_SNITCH}, property_file=racks(1) * 3)
    cql = connect(manager, servers[0])
    cql.execute("create keyspace lots_o_views WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};")
    cql.execute("use lots_o_views")
    cql.execute("create table source_data (id uuid primary key, c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, c10 int);")
    insert_stmt = cql.prepare("insert into source_data (id, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) values (uuid(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")
    for n in range(4000):
        cql.execute(insert_stmt, [n] * 10)

    for n in range(1, 11):
        cql.execute(f"CREATE MATERIALIZED VIEW src_by_c{n} AS SELECT * FROM source_data WHERE c{n} IS NOT NULL AND id IS NOT NULL PRIMARY KEY (c{n}, id)")
        cql.cluster.control_connection.wait_for_schema_agreement()

    # Wait for the views to be built.
    for n in range(1, 11):
        await wait_for_view_built(manager, cql, "lots_o_views", f"src_by_c{n}", servers)
    result = list(cql.execute("SELECT * FROM system_schema.views WHERE keyspace_name='lots_o_views' AND base_table_name='source_data' ALLOW FILTERING"))
    assert 10 == len(result), f"missing some mv from source_data table, expected 10, got len(result)={len(result)}"

    for n in range(1, 11):
        result_count = len(list(cql.execute(f"select * from src_by_c{n}")))
        assert 4000 == result_count, f"expect 4000, got len(result)={result_count}"


# Ported from scylla-dtest concurrent_schema_changes_test.py::TestConcurrentSchemaChanges::test_create_lots_of_schema_churn.
# Tables, alters and indexes created concurrently end in one schema version
# on every node, with every new table.
# Not gating in scylla-dtest.
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_create_lots_of_schema_churn(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config={**CONFIG, **SIMPLE_SNITCH}, property_file=racks(1) * 3)
    cql = connect(manager, servers[0])
    cql.execute("create keyspace lots_o_churn WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};")
    cql.execute("use lots_o_churn")

    do_lots_of_schema_actions(cql)
    await verify_lots_of_schema_actions(manager, cql, servers)


# Ported from scylla-dtest concurrent_schema_changes_test.py::TestConcurrentSchemaChanges::test_create_lots_of_schema_churn_with_node_down.
# The same schema churn while the second node is down: once started again,
# it catches up, and every node ends in one schema version.
# Not gating in scylla-dtest (scylla-enterprise#3231).
@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_create_lots_of_schema_churn_with_node_down(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config={**CONFIG, **SIMPLE_SNITCH}, property_file=racks(1) * 3)
    node1, node2, _node3 = servers
    cql = connect(manager, node1)
    cql.execute("create keyspace lots_o_churn WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};")
    cql.execute("use lots_o_churn")

    await manager.server_stop_gracefully(node2.server_id)
    do_lots_of_schema_actions(cql)
    await manager.server_start(node2.server_id)
    await manager.servers_see_each_other(servers)
    await verify_lots_of_schema_actions(manager, cql, servers)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_basic(manager: ScyllaClusterManager):
    """Several schema changes made on the same node."""
    servers = await manager.servers_add(2, config=CONFIG, property_file=racks(2))
    cql = connect(manager, servers[0])
    prepare_for_changes(cql, namespace="ns1")
    make_schema_changes(cql, namespace="ns1")


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_changes_to_different_nodes(manager: ScyllaClusterManager):
    """Schema changes made through one node, then through the other, end in
    one schema version."""
    servers = await manager.servers_add(2, config=CONFIG, property_file=racks(2))
    node1, node2 = servers
    cql = connect(manager, node1)
    prepare_for_changes(cql, namespace="ns1")
    make_schema_changes(cql, namespace="ns1")
    await validate_schema_consistent(manager, node1)

    cql = connect(manager, node2)
    prepare_for_changes(cql, namespace="ns2")
    make_schema_changes(cql, namespace="ns2")
    await validate_schema_consistent(manager, node1)


# Ported from scylla-dtest concurrent_schema_changes_test.py::TestConcurrentSchemaChanges::test_changes_while_node_down.
# Schema changes made through node 1 while node 2 is down reach node 2 when it
# starts again, and the index created meanwhile gets built on every node.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_changes_while_node_down(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, property_file=racks(3))
    node1, node2, _node3 = servers
    cql = connect(manager, node1, exclusive=True)

    await manager.server_stop_gracefully(node2.server_id)
    await manager.others_not_see_server(node2.ip_addr)
    prepare_for_changes(cql, namespace="ns1", rf=3)
    make_schema_changes(cql, namespace="ns1", rf=3)

    await manager.server_start(node2.server_id)
    await manager.servers_see_each_other(servers)
    await validate_schema_consistent(manager, node2)

    assert await index_is_built(manager, cql, servers, ks_name="ks_ns1", table_name="cf_ns1", index_name="index2_ns1")


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_changes_while_node_toggle(manager: ScyllaClusterManager):
    """Changes made on node 2 while node 1 is down reach node 1 when it comes
    back up after nodes 2 and 3 went down."""
    servers = await manager.servers_add(3, config=CONFIG, property_file=racks(3))
    node1, node2, node3 = servers
    cql = connect(manager, node2, exclusive=True)
    prepare_for_changes(cql, namespace="ns2", rf=3)
    await manager.server_stop_gracefully(node1.server_id)
    make_schema_changes(cql, namespace="ns2", rf=3)

    await manager.server_stop_gracefully(node2.server_id)
    await manager.server_stop_gracefully(node3.server_id)
    for node in [node1, node2, node3]:
        await manager.server_start(node.server_id)
    await validate_schema_consistent(manager, node1)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_decommission_node(manager: ScyllaClusterManager):
    """Schema changes made after a node is decommissioned, and before a new
    node joins, end in one schema version."""
    # Like in the dtest, the second node is not a seed.
    node1 = await manager.server_add(config=CONFIG, property_file={"dc": "datacenter1", "rack": "rack1"})
    node_to_decommission = await manager.server_add(config=CONFIG, property_file={"dc": "datacenter1", "rack": "rack1"})
    cql = connect(manager, node1)
    prepare_for_changes(cql)

    await manager.decommission_node(node_to_decommission.server_id)
    await validate_schema_consistent(manager, node1)
    make_schema_changes(cql, namespace="ns1")

    await manager.server_add(config=CONFIG, property_file={"dc": "datacenter1", "rack": "rack2"})
    await validate_schema_consistent(manager, node1)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_load(manager: ScyllaClusterManager):
    """
    A table is dropped and created again while the node compacts the data
    cassandra-stress wrote.
    """
    node1 = await manager.server_add(config={**CONFIG, **SIMPLE_SNITCH})
    cql = connect(manager, node1)

    # Put some data into the cluster. Like ccm's node.stress(), cassandra-stress
    # creates keyspace1 itself.
    await cassandra_stress(manager, node1, "write", "n=30000", "-rate", "threads=8", create_keyspace=False)

    compact = asyncio.create_task(nodetool(manager, node1, "compact"))
    await asyncio.sleep(1)

    # The node is busy compacting. Make some schema changes.
    cql.execute("USE keyspace1")
    cql.execute("DROP TABLE standard1")
    cql.execute("CREATE TABLE standard1 (KEY text PRIMARY KEY)")

    await compact
