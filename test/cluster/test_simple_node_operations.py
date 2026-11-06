#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Basic operations on nodes of their own: schema and data surviving a
restart, inserts and selects by composite and compound keys, consistency
levels with nodes down, and data written by cassandra-stress read back after
a restart.

Ported from scylla-dtest. Like the dtests' nodes, the nodes use vnodes (256
per node), and are stopped gracefully at the end of each test, as the
dtests' teardown did.
"""

import asyncio
import logging

import pytest
from cassandra import ConsistencyLevel, Unavailable
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import SimpleStatement

from test.cluster.util import run_nodetool, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

# Like the dtests' nodes, new keyspaces use vnodes by default, 256 per node.
CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256}


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def ks_opts(rf: int) -> str:
    return f"WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}"


async def check_k1_and_not_k2(cql) -> None:
    res = list(await cql.run_async("SELECT * FROM ks.test1 WHERE k=1"))
    assert len(res) == 1, f"expected length=1 got {res}"
    res = list(await cql.run_async("SELECT * FROM ks.test1 WHERE k=2"))
    assert len(res) == 0, f"expected length=0 got {res}"


############################################################################
# Ported from simple_boot_shutdown_test.py.

async def boot_with_table(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts(1)}")
    await cql.run_async("CREATE TABLE ks.test1 (k int PRIMARY KEY, c int)")
    return server, cql


async def restart(manager: ScyllaClusterManager, server, gracefully: bool = True) -> None:
    if gracefully:
        await manager.server_stop_gracefully(server.server_id)
    else:
        await manager.server_stop(server.server_id, convict=False)
    await manager.server_start(server.server_id)
    await manager.driver_connect(server=server)


# A table created (and flushed) before a restart takes a row after it.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_boot_create_keyspace_table_shutdown_boot_insert_select(manager: ScyllaClusterManager):
    server, cql = await boot_with_table(manager)
    await run_nodetool(manager, server, "flush")
    await restart(manager, server)
    cql = manager.get_cql()
    await cql.run_async("insert into ks.test1  (k,c) values (1,2);")
    await check_k1_and_not_k2(cql)


# A row written and flushed before a graceful restart is read after it.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_boot_create_keyspace_table_insert_shutdown_select(manager: ScyllaClusterManager):
    server, cql = await boot_with_table(manager)
    await cql.run_async("insert into ks.test1  (k,c) values (1,2);")
    await run_nodetool(manager, server, "flush")
    await restart(manager, server)
    await check_k1_and_not_k2(manager.get_cql())


# The same with the node killed after the flush (the dtest's "commitlog
# replay" variant).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_boot_create_keyspace_table_insert_shutdown_commitlog_replay_select(manager: ScyllaClusterManager):
    server, cql = await boot_with_table(manager)
    await cql.run_async("insert into ks.test1  (k,c) values (1,2);")
    await run_nodetool(manager, server, "flush")
    await restart(manager, server, gracefully=False)
    await check_k1_and_not_k2(manager.get_cql())


############################################################################
# Ported from simple_driver_test.py: each test on a node with one and with
# two shards, as the dtests' SMP fixture.

SMP = pytest.mark.parametrize("smp", ["1", "2"], ids=["SMP=1", "SMP=2"])


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@SMP
async def test_simple_composite_partition_key_create_insert_select(manager: ScyllaClusterManager, smp):
    await manager.server_add(config=CONFIG, cmdline=["--smp", smp])
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts(1)}")
    await cql.run_async("CREATE TABLE ks.test1 (k1 int, k2 int, c int, PRIMARY KEY ((k1,k2)))")
    await cql.run_async("insert into ks.test1 (k1,k2,c) values (1,1,3);")
    await cql.run_async("insert into ks.test1 (k1,k2,c) values (1,2,4);")
    for k1, k2, n in [(1, 1, 1), (1, 2, 1), (2, 1, 0), (1, 3, 0)]:
        res = list(await cql.run_async(f"SELECT * FROM ks.test1 WHERE k1={k1} and k2={k2}"))
        assert len(res) == n, res


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@SMP
async def test_simple_compound_primary_key_create_insert_select(manager: ScyllaClusterManager, smp):
    await manager.server_add(config=CONFIG, cmdline=["--smp", smp])
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts(1)}")
    await cql.run_async("CREATE TABLE ks.test1 (k1 int, c1 int, c2 int, c3 int, PRIMARY KEY (k1,c1,c2))")
    await cql.run_async("insert into ks.test1 (k1,c1,c2,c3) values (1,1,1,1);")
    await cql.run_async("insert into ks.test1 (k1,c1,c2,c3) values (1,1,2,2);")
    assert len(list(await cql.run_async("SELECT * FROM ks.test1 WHERE k1=1"))) == 2
    assert len(list(await cql.run_async("SELECT * FROM ks.test1 WHERE k1=1 and c1=1 and c2=1"))) == 1
    await cql.run_async("SELECT * FROM ks.test1 WHERE k1=1 and c1=2")
    assert len(list(await cql.run_async("SELECT * FROM ks.test1 WHERE k1=1 and c1=1 and c2=3"))) == 0


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@SMP
async def test_simple_create_insert_select(manager: ScyllaClusterManager, smp):
    await manager.server_add(config=CONFIG, cmdline=["--smp", smp])
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts(1)}")
    await cql.run_async("CREATE TABLE ks.test1 (k int PRIMARY KEY, c int)")
    await cql.run_async("insert into ks.test1  (k,c) values (1,2);")
    await check_k1_and_not_k2(cql)


############################################################################
# Ported from simple_cluster_driver_test.py.

# Three nodes (one per rack), RF=3: a table created through the second node
# takes a row written at QUORUM through the third, which each node reads at
# QUORUM; a key never written is found by none.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_simple_cluster_create_insert_select(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, property_file=[{"dc": "datacenter1", "rack": f"rack{i}"} for i in range(1, 4)])
    sessions = [await manager.get_cql_exclusive(s) for s in servers]
    await sessions[0].run_async(f"CREATE KEYSPACE ks {ks_opts(3)}")
    await sessions[1].run_async("CREATE TABLE ks.test1 (k int PRIMARY KEY, c int)")
    await sessions[2].run_async(SimpleStatement("insert into ks.test1  (k,c) values (1,2);", consistency_level=ConsistencyLevel.QUORUM))
    for key, n in ((1, 1), (2, 0)):
        query = SimpleStatement(f"SELECT * FROM ks.test1 WHERE k={key}", consistency_level=ConsistencyLevel.QUORUM)
        for session in sessions:
            res = list(await session.run_async(query))
            assert len(res) == n, res

async def consistency_level_validate(cql, read_keys, read_cls_pass, read_cls_fail, read_cls_pass_all,
                                     write_keys, write_cls_pass, write_cls_fail, write_cls_pass_all) -> None:
    """The dtest's simple_consistency_level_validate(): reads and writes at
    each CL either all pass, or (with some replicas down) some fail; CLs
    needing more replicas than there are fail with Unavailable."""
    for cl in read_cls_pass:
        failed = 0
        for key in read_keys:
            try:
                res = list(await cql.run_async(SimpleStatement(f"SELECT * FROM ks.test1 WHERE k={key}", consistency_level=cl)))
                assert len(res) == 1, res
            except Exception:  # noqa: BLE001
                failed += 1
        assert failed == 0 or not read_cls_pass_all, f"expected all reads at {cl} to pass, {failed} failed"
        assert failed > 0 or read_cls_pass_all, f"expected some reads at {cl} to fail"
    for cl in read_cls_fail:
        with pytest.raises(Unavailable):
            await cql.run_async(SimpleStatement("SELECT * FROM ks.test1 WHERE k=1", consistency_level=cl, retry_policy=FallthroughRetryPolicy()), timeout=1)
    for cl in write_cls_pass:
        failed = 0
        for key in write_keys:
            try:
                await cql.run_async(SimpleStatement(f"insert into ks.test1  (k,c) values ({key},{key})", consistency_level=cl))
            except Exception:  # noqa: BLE001
                failed += 1
        assert failed == 0 or not write_cls_pass_all, f"expected all writes at {cl} to pass, {failed} failed"
        assert failed > 0 or write_cls_pass_all, f"expected some writes at {cl} to fail"
    for cl in write_cls_fail:
        with pytest.raises(Unavailable):
            await cql.run_async(SimpleStatement("insert into ks.test1 (k,c) values (101,101)", consistency_level=cl, retry_policy=FallthroughRetryPolicy()), timeout=1)


# Three nodes (one per rack), RF=1: every read and write at ONE, QUORUM and
# ALL passes from each node, TWO and THREE are unavailable; with one and then
# two nodes down, some requests fail and TWO and THREE stay unavailable.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_simple_rf_1_consistency_level_no_racklist(manager: ScyllaClusterManager):
    servers = await manager.servers_add(3, config=CONFIG, property_file=[{"dc": "datacenter1", "rack": f"rack{i}"} for i in range(1, 4)])
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts(1)}")
    await cql.run_async("CREATE TABLE ks.test1 (k int PRIMARY KEY, c int)")
    sessions = [await manager.get_cql_exclusive(s) for s in servers]
    keys = range(1, 100)
    for val in keys:
        await sessions[0].run_async(SimpleStatement(f"insert into ks.test1  (k,c) values ({val},{val})", consistency_level=ConsistencyLevel.ALL))
    CL = ConsistencyLevel
    reads, unavailable = [CL.ONE, CL.QUORUM, CL.ALL], [CL.TWO, CL.THREE]
    for i, s in enumerate(sessions):
        await consistency_level_validate(s, keys, reads, unavailable, True, range(101 + 100 * i, 200 + 100 * i),
                                         [CL.ANY, CL.ONE, CL.QUORUM, CL.ALL], unavailable, True)
    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.others_not_see_server(servers[0].ip_addr)
    for i, s in enumerate(sessions[1:]):
        await consistency_level_validate(s, keys, reads, unavailable, False, range(401 + 100 * i, 500 + 100 * i),
                                         [CL.ONE, CL.QUORUM, CL.ALL], unavailable, False)
    await manager.server_stop_gracefully(servers[1].server_id)
    await manager.others_not_see_server(servers[1].ip_addr)
    await consistency_level_validate(sessions[2], keys, reads, unavailable, False, range(601, 700),
                                     [CL.ONE, CL.QUORUM, CL.ALL], unavailable, False)


############################################################################
# Ported from persistence_test.py.

# Five rows with a 1-byte blob column, written with the driver into a
# table made by the dtests' create_cf() (keyspace1 with RF=1), read back
# equal after a restart.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_persist_small_columns(manager: ScyllaClusterManager):
    server = await manager.server_add(config=CONFIG)
    cql = manager.get_cql()
    size = 1
    cql.execute(f"CREATE KEYSPACE keyspace1 {ks_opts(1)}")
    # The dtests' create_cf(session, "standard1", key_type="blob", columns={"c0": "blob"}).
    cql.execute("CREATE COLUMNFAMILY keyspace1.standard1 (key blob PRIMARY KEY, c0 blob) WITH comment='test cf' "
                "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {}")
    cql.execute(SimpleStatement("SELECT * FROM keyspace1.standard1 LIMIT 1", consistency_level=ConsistencyLevel.ONE))
    rows = {f"{i}".encode(): bytes([i]) * size for i in range(1, 6)}
    for key, val in rows.items():
        cql.execute(SimpleStatement("INSERT INTO keyspace1.standard1 (key, c0) VALUES (%s, %s)", consistency_level=ConsistencyLevel.ONE), (key, val))
    # restart_cluster(): stop, 0.5 s, start, 0.5 s.
    await manager.server_stop_gracefully(server.server_id)
    await asyncio.sleep(0.5)
    await manager.server_start(server.server_id)
    await asyncio.sleep(0.5)
    await manager.driver_connect(server=server)
    cql = manager.get_cql()
    for key, val in rows.items():
        res = [list(r) for r in cql.execute(SimpleStatement(f"SELECT c0 FROM keyspace1.standard1 WHERE key = 0x{key.hex()}",
                                                            consistency_level=ConsistencyLevel.ONE))]
        assert res == [[val]], f"Expected [[{val!r}]] for key 0x{key.hex()}, got {res}"


############################################################################
# Ported from sstable_generation_loading_test.py.

# A small partition with a static row followed by a large one (#1567): with
# the cache off, the large partition's promoted index, written by the
# shutdown flush, serves a read of its first row after a restart.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_promoted_index_generation_with_small_partition_followed_by_a_large_partition(manager: ScyllaClusterManager):
    server = await manager.server_add(config={**CONFIG, 'enable_cache': False})
    cql = manager.get_cql()
    await cql.run_async(f"CREATE KEYSPACE ks {ks_opts(1)}")
    await cql.run_async("CREATE TABLE ks.test (pk int, ck text, s1 int static, v int, PRIMARY KEY (pk, ck));")
    await cql.run_async("insert into ks.test (pk, s1) values (1, 7);")
    await cql.run_async("insert into ks.test (pk, s1) values (0, 7);")
    for idx in range(2000):
        await cql.run_async(f"insert into ks.test  (pk, ck, v) values (0, 'ck_{idx}', {idx});")
    await restart(manager, server)
    rows = list(await manager.get_cql().run_async("select * from ks.test where pk = 0 and ck = 'ck_0';"))
    assert len(rows) == 1
    assert list(rows[0]) == [0, "ck_0", 7, 0]


############################################################################
# Ported from schema_everywhere_test.py.

EVERYWHERE_KS = "distributed_everywhere"


# A table in an EverywhereStrategy keyspace on two nodes, written at ALL,
# is read at ONE from each node while the other one is down.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_creating_new_table(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, config=CONFIG)
    session = await manager.get_cql_exclusive(servers[0])
    await session.run_async(f"CREATE KEYSPACE IF NOT EXISTS {EVERYWHERE_KS} WITH REPLICATION = {{'class': 'EverywhereStrategy'}} AND TABLETS = {{'enabled': false}}")
    await session.run_async(f"CREATE TABLE {EVERYWHERE_KS}.test1 (key varchar PRIMARY KEY, c1 varchar, c2 varchar)")
    await session.run_async(SimpleStatement(f"INSERT INTO {EVERYWHERE_KS}.test1 (key, c1, c2) VALUES ('key1', 'txt1', 'txt2')",
                                            consistency_level=ConsistencyLevel.ALL))

    async def verify(server):
        session = await manager.get_cql_exclusive(server)
        res = list(await session.run_async(SimpleStatement(f"Select * from {EVERYWHERE_KS}.test1;", consistency_level=ConsistencyLevel.ONE)))
        assert len(res) == 1, f"Wrong number of rows: {res}"
        assert res[0].c1 == "txt1" and res[0].c2 == "txt2", f"Row {res[0]} has wrong column values"

    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.others_not_see_server(servers[0].ip_addr)
    await verify(servers[1])
    await manager.server_start(servers[0].server_id)
    await manager.server_stop_gracefully(servers[1].server_id)
    await verify(servers[0])


############################################################################
# Ported from persistence_test.py.

# Ported from scylla-dtest persistence_test.py::TestPersistence::test_data_persistence_of_map_with_empty_keys.
# On one node, a row whose map has an empty key, flushed and compacted,
# is the same after the node restarts (scylla-enterprise#909).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_data_persistence_of_map_with_empty_keys(manager: ScyllaClusterManager):
    server = await manager.server_add(config={**CONFIG, "endpoint_snitch": "SimpleSnitch"}, property_file={"dc": "datacenter1", "rack": "rack1"})
    cql = manager.get_cql()
    table_name = "keyspace1.user_stats3"
    cql.execute("CREATE KEYSPACE keyspace1 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}  AND durable_writes = true;")
    cql.execute(f"CREATE TABLE {table_name} ( user_id text PRIMARY KEY, clients_usage map<text, text>, last_seen timestamp );")
    cql.execute(f"Insert into {table_name} (user_id, clients_usage) values ('Piotr', {{'': '2019-05-05T04:14:16.954407'}});")
    row_before_reboot = cql.execute(f"Select * from {table_name};").current_rows[0]
    await run_nodetool(manager, server, "flush")
    await run_nodetool(manager, server, "compact")
    # restart_cluster(): stop, 0.5 s, start, 0.5 s.
    await manager.server_stop_gracefully(server.server_id)
    await asyncio.sleep(0.5)
    await manager.server_start(server.server_id)
    await asyncio.sleep(0.5)
    await manager.driver_connect(server=server)
    row_after_reboot = manager.get_cql().execute(f"Select * from {table_name};").current_rows[0]
    assert row_before_reboot == row_after_reboot, \
        f"The data before reboot ('{row_before_reboot}') should be equal to data after reboot ('{row_after_reboot}')"

