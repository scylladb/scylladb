#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import logging
import sys

import time
from typing import List

import pytest
from cassandra.policies import WhiteListRoundRobinPolicy

from test.cluster.util import new_materialized_view, new_test_keyspace, new_test_table
from test.cqlpy import nodetool
from cassandra import ConsistencyLevel
from cassandra.protocol import InvalidRequest
from cassandra.query import SimpleStatement
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables, TextType, Column
from test.pylib.rest_client import read_barrier
from test.pylib.util import unique_name, wait_for
from test.cluster.conftest import cluster_con

logger = logging.getLogger(__name__)
CONFIG = {"endpoint_snitch": "GossipingPropertyFileSnitch"}


# Checks a cluster boot/operations in multi-dc environment with 5 nodes each in a separate DC
@pytest.mark.asyncio
async def test_multidc(request: pytest.FixtureRequest, manager: ManagerClient) -> None:
    logger.info("Creating a new cluster")
    for i in range(5):
        s_info = await manager.server_add(
            config=CONFIG,
            property_file={'dc': f'dc{i}', 'rack': 'myrack1'}
        )
        logger.info(s_info)
    random_tables = RandomTables(request.node.name, manager, unique_name(), 1)
    logger.info("Creating new tables")
    await random_tables.add_tables(ntables=3, ncolumns=3)
    await random_tables.verify_schema()


cluster_config = [
    ([1, 2], 1),
    ([1, 1, 2, 2], 2)
]


# Simple put-get test for 2 DC with a different amount of nodes and different replication factors
@pytest.mark.asyncio
@pytest.mark.parametrize("nodes_list, rf", cluster_config)
async def test_putget_2dc_with_rf(
        request: pytest.FixtureRequest, manager: ManagerClient, nodes_list: list[int], rf: int
) -> None:
    ks = "test_ks"
    cf = "test_cf"
    table_name = "test_table_name"
    columns = [Column("name", TextType), Column("value", TextType)]
    logger.info("Create two servers in different DC's")
    for i in nodes_list:
        s_info = await manager.server_add(
            config=CONFIG,
            property_file={"dc": f"dc{i}", "rack": "myrack"},
        )
        logger.info(s_info)
    conn = manager.get_cql()
    random_tables = RandomTables(request.node.name, manager, ks, rf)
    logger.info("Add table")
    await random_tables.add_table(ncolumns=2, columns=columns, pks=1, name=table_name)
    conn.execute(f"USE {ks}")
    conn.execute(
        f"CREATE COLUMNFAMILY {cf} ( key varchar, c varchar, v varchar, PRIMARY KEY(key, c)) WITH comment='test cf'"
    )

    logger.info("Perform insert/overwrite")
    # create 100 records with values from 0 to 99
    update_query = f"UPDATE {cf} SET v='value%d' WHERE key='k0' AND c='c%02d'"
    query_batch = "BEGIN BATCH %s APPLY BATCH"
    kvs = [update_query % (i, i) for i in range(100)]
    query = SimpleStatement(
        query_batch % ";".join(kvs), consistency_level=ConsistencyLevel.QUORUM
    )
    conn.execute(query)
    nodetool.flush_keyspace(conn, ks)
    # overwrite each second value
    kvs = [update_query % (i * 4, i * 2) for i in range(50)]
    query = SimpleStatement(
        query_batch % "; ".join(kvs), consistency_level=ConsistencyLevel.QUORUM
    )
    conn.execute(query)
    nodetool.flush_keyspace(conn, ks)
    # overwrite each fifth value
    kvs = [update_query % (i * 20, i * 5) for i in range(20)]
    query = SimpleStatement(
        query_batch % "; ".join(kvs), consistency_level=ConsistencyLevel.QUORUM
    )
    conn.execute(query)
    nodetool.flush_keyspace(conn, ks)

    logger.info("Check written data is correct")
    query = SimpleStatement(f"SELECT * FROM {cf} WHERE key='k0'", consistency_level=ConsistencyLevel.QUORUM)
    rows = list(conn.execute(query))
    assert len(rows) == 100
    for i, row in enumerate(rows):
        if i % 5 == 0:
            assert row[2] == f"value{i * 4}"
        elif i % 2 == 0:
            assert row[2] == f"value{i * 2}"
        else:
            assert row[2] == f"value{i}"


@pytest.mark.asyncio
async def test_query_dc_with_rf_0_does_not_crash_db(request: pytest.FixtureRequest, manager: ManagerClient):
    """Test querying dc with CL=LOCAL_QUORUM when RF=0 for this dc, does not crash the node and returns None
    Covers https://github.com/scylladb/scylla/issues/8354"""
    servers = []
    ks = "test_ks"
    table_name = "test_table_name"
    expected = ["k1", "value1"]
    dc_replication = {'dc2': 0}
    columns = [Column("name", TextType), Column("value", TextType)]

    for i in [1, 2]:
        servers.append(await manager.server_add(
            config=CONFIG,
            property_file={"dc": f"dc{i}", "rack": "myrack"},
        ))

    dc1_connection = cluster_con([servers[0].ip_addr], 9042, False,
                                 load_balancing_policy=WhiteListRoundRobinPolicy([servers[0].ip_addr])).connect()
    dc2_connection = cluster_con([servers[1].ip_addr], 9042, False,
                                 load_balancing_policy=WhiteListRoundRobinPolicy([servers[1].ip_addr])).connect()

    random_tables = RandomTables(request.node.name, manager, ks, 1, dc_replication)
    await random_tables.add_table(ncolumns=2, columns=columns, pks=1, name=table_name)
    dc1_connection.execute(
        f"INSERT INTO  {ks}.{table_name} ({columns[0].name}, {columns[1].name}) VALUES ('{expected[0]}', '{expected[1]}');")
    select_query = SimpleStatement(f"SELECT * from {ks}.{table_name};",
                                   consistency_level=ConsistencyLevel.LOCAL_QUORUM)
    nodetool.flush(dc1_connection, "{ks}.{table_name}")
    first_node_results = list(dc1_connection.execute(select_query).one())
    second_node_result = dc2_connection.execute(select_query).one()

    assert first_node_results == expected, \
        f"Expected {expected} from {select_query.query_string}, but got {first_node_results}"
    assert second_node_result is None, \
        f"Expected no results from {select_query.query_string}, but got {second_node_result}"

@pytest.mark.asyncio
async def test_create_and_alter_keyspace_with_altering_rf_and_racks(manager: ManagerClient):
    """
    This test verifies that creating and altering a keyspace keeps it RF-rack-valid.
    If an operation would make it RF-rack-invalid, it should fail.
    We can add a new rack or a data center and the existing keyspaces must still work fine.
    """

    cql = None
    cfg = {"rf_rack_valid_keyspaces": "true"}

    async def create_ok(rfs: List[int]) -> str:
        ks = unique_name()
        dcs = ", ".join([f"'dc{i + 1}': {rf}" for i, rf in enumerate(rfs)])
        await cql.run_async(f"CREATE KEYSPACE {ks} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', {dcs}}} AND tablets = {{'enabled': true}}")
        return ks

    async def create_fail(rfs: List[int], failed_dc: int, rack_count: int) -> None:
        ks = unique_name()
        dcs = ", ".join([f"'dc{i + 1}': {rf}" for i, rf in enumerate(rfs)])

        dc = f"dc{failed_dc}"
        rf = rfs[failed_dc - 1]

        err = r"The option `rf_rack_valid_keyspaces` is enabled. It requires that keyspaces are RF-rack-valid. " \
              f"Your query would violate that: keyspace '{ks}' doesn't satisfy the condition for DC '{dc}': RF={rf} vs. rack count={rack_count}."

        with pytest.raises(InvalidRequest, match=err):
            await cql.run_async(f"CREATE KEYSPACE {ks} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', {dcs}}} AND tablets = {{'enabled': true}}")

    async def alter_ok(ks: str, rfs: List[int]) -> None:
        dcs = ", ".join([f"'dc{i + 1}': {rf}" for i, rf in enumerate(rfs)])
        await cql.run_async(f"ALTER KEYSPACE {ks} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', {dcs}}}")

    async def alter_fail(ks: str, rfs: List[int], failed_dc: int, rack_count: int) -> None:
        dcs = ", ".join([f"'dc{i + 1}': {rf}" for i, rf in enumerate(rfs)])

        dc = f"dc{failed_dc}"
        rf = rfs[failed_dc - 1]

        err = r"The option `rf_rack_valid_keyspaces` is enabled. It requires that keyspaces remain RF-rack-valid. " \
              f"Your query would violate that: keyspace '{ks}' uses tablets and the condition is not satisfied for DC '{dc}': " \
              f"RF={rf} vs. rack count={rack_count}."

        with pytest.raises(InvalidRequest, match=err):
            await cql.run_async(f"ALTER KEYSPACE {ks} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', {dcs}}}")

    # dc1: r1=1.
    _ = await manager.server_add(property_file={"dc": "dc1", "rack": "r1"}, config=cfg)
    cql = manager.get_cql()

    ks1 = await create_ok([1])
    await create_fail([2], 1, 1)

    # Edge case: global RF = 0.
    await alter_ok(ks1, [0])
    await alter_ok(ks1, [1])
    # Check if it works if the RF doesn't change.
    await alter_ok(ks1, [1])

    await alter_fail(ks1, [2], 1, 1)

    # dc1: r1=1.
    # dc2: r1=1.
    _ = await manager.server_add(property_file={"dc": "dc2", "rack": "r1"}, config=cfg)

    ks2 = await create_ok([1, 1])
    await create_ok([0, 1])
    await create_ok([0, 0])
    await create_ok([1, 0])

    await create_fail([2, 1], 1, 1)
    await create_fail([1, 2], 2, 1)
    await create_fail([2, 0], 1, 1)

    # Edge case: global RF = 0.
    await alter_ok(ks1, [0, 0])
    await alter_ok(ks1, [1, 0])
    # Check if it works if the RF doesn't change.
    await alter_ok(ks1, [1, 0])

    # Edge case: global RF = 0.
    await alter_ok(ks1, [0])
    await alter_ok(ks1, [1])
    # Check if it works if the RF doesn't change.
    await alter_ok(ks1, [1])

    await alter_fail(ks1, [2, 0], 1, 1)
    await alter_fail(ks1, [2], 1, 1)

    await alter_ok(ks1, [1, 1])
    await alter_ok(ks2, [0, 1])
    await alter_ok(ks2, [1, 1])
    await alter_ok(ks2, [1, 0])
    await alter_ok(ks2, [1, 1])

    await alter_fail(ks2, [2, 1], 1, 1)
    await alter_fail(ks2, [1, 2], 2, 1)

    # dc1: r1=1, r2=1.
    # dc2: r1=1.
    _ = await manager.server_add(property_file={"dc": "dc1", "rack": "r2"}, config=cfg)

    ks3 = await create_ok([2, 1])
    # RF = 1 is always OK!
    ks4 = await create_ok([1, 1])

    await create_fail([1, 2], 2, 1)
    await create_fail([2, 2], 2, 1)
    await create_ok([2, 0])
    await create_ok([0, 1])

    await alter_ok(ks1, [2, 1])
    await alter_fail(ks1, [2, 2], 2, 1)

    await alter_ok(ks2, [2, 1])
    await alter_ok(ks3, [2, 1])
    await alter_ok(ks4, [2, 1])
    # RF = 1 is always OK!
    await alter_ok(ks3, [1, 1])

@pytest.mark.asyncio
async def test_create_mv_with_racks(manager: ManagerClient):
    """
    This test verifies that creating a materialized view is only possible in RF-rack-valid keyspaces.

    For more context, see: scylladb/scylladb#23030.
    """

    cmd = ["--experimental-features=views-with-tablets"]
    cfg = {"rf_rack_valid_keyspaces": "true"}

    s1 = await manager.server_add(cmdline=cmd, property_file={"dc": "dc1", "rack": "r1"}, config=cfg)
    _ = await manager.server_add(cmdline=cmd, property_file={"dc": "dc1", "rack": "r2"}, config=cfg)
    _ = await manager.server_add(cmdline=cmd, property_file={"dc": "dc2", "rack": "r1"}, config=cfg)

    async def set_rf_rack_valid_keyspaces(value: bool):
        servers = await manager.running_servers()
        cfg_name = "rf_rack_valid_keyspaces"

        for s in servers:
            await manager.server_update_config(s.server_id, cfg_name, str(value).lower())

        for s in servers:
            async def config_value_equal():
                await read_barrier(manager.api, s.ip_addr)
                response = await manager.api.get_config(s.ip_addr, cfg_name)
                logging.info(f"Obtained the value of option '{cfg_name}' for {s.ip_addr} via REST API: {response}")
                if response == value:
                    return True
                return None
            await wait_for(config_value_equal, deadline=time.time() + 60)

    async def try_pass(replication_class: str, replication_details: str, tablets: str):
        async with new_test_keyspace(manager, f"WITH REPLICATION = {{'class': '{replication_class}', {replication_details}}} AND tablets = {{'enabled': {tablets}}}") as ks:
            async with new_test_table(manager, ks, "p int PRIMARY KEY, v int") as table:
                async with new_materialized_view(manager, table, "*", "p, v", "p IS NOT NULL AND v IS NOT NULL"):
                    pass

    async def try_fail(replication_class: str, replication_details: str, tablets: str, regex: str):
        # We need to artificially turn off the restriction to be able to create an RF-rack-invalid keyspace
        # so we can verify that Scylla refuses to create a materialized view in it.
        await set_rf_rack_valid_keyspaces(False)

        async with new_test_keyspace(manager, f"WITH REPLICATION = {{'class': '{replication_class}', {replication_details}}} AND tablets = {{'enabled': {tablets}}}") as ks:
            await set_rf_rack_valid_keyspaces(True)

            async with new_test_table(manager, ks, "p int PRIMARY KEY, v int") as table:
                with pytest.raises(InvalidRequest, match=regex.format(ks=ks)):
                    async with new_materialized_view(manager, table, "*", "p, v", "p IS NOT NULL AND v IS NOT NULL"):
                        pass

    # Below, we test each case twice: with tablets on and off.
    # Note that we only use NetworkTopologyStrategy. That's because of this fragment of our documentation:
    #
    # "When creating a new keyspace with tablets enabled (the default), you can still disable them on a per-keyspace basis.
    #  The recommended NetworkTopologyStrategy for keyspaces remains REQUIRED when using tablets."
    #
    # --- "Data Distribution with Tablets"

    # Part 1: Test the current state of the cluster. Note that every rack currently consists of one node.

    # RF = #racks for every DC.
    await try_pass("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 1", "true")
    await try_pass("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 1", "false")

    # RF != #racks for dc1, but we accept RF = 1.
    await try_pass("NetworkTopologyStrategy", "'dc1': 1, 'dc2': 1", "true")
    await try_pass("NetworkTopologyStrategy", "'dc1': 1, 'dc2': 1", "false")

    # RF != #racks for dc2, but we accept RF = 0.
    await try_pass("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 0", "true")
    await try_pass("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 0", "false")
    # Ditto, just for dc1.
    await try_pass("NetworkTopologyStrategy", "'dc1': 0, 'dc2': 1", "true")
    await try_pass("NetworkTopologyStrategy", "'dc1': 0, 'dc2': 1", "false")

    # Note: in case these checks start failing or causing issues, feel free to get rid of them.
    #       We don't care about it (just like we don't care about EverywhereStrategy and LocalStrategy),
    #       so these are more of sanity checks than something we really want to test.
    for rf in [1, 2, 3]:
        await try_pass("SimpleStrategy", f"'replication_factor': {rf}", "false")

    # Part 2: We extend the cluster by one node in dc1/r2. We no longer have a bijection: nodes -> racks.

    _ = await manager.server_add(cmdline=cmd, property_file={"dc": "dc1", "rack": "r2"}, config=cfg)

    # RF = #racks for every DC.
    await try_pass("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 1", "true")
    await try_pass("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 1", "false")

    # RF < #racks for dc1, but we accept RF = 1.
    await try_pass("NetworkTopologyStrategy", "'dc1': 1, 'dc2': 1", "true")
    await try_pass("NetworkTopologyStrategy", "'dc1': 1, 'dc2': 1", "false")

    # RF > #racks for dc1.
    await try_fail("NetworkTopologyStrategy", "'dc1': 3, 'dc2': 1", "true",
                   "The option `rf-rack-valid-keyspaces` is enabled, which forbids creating " \
                   "a materialized view in an RF-rack-invalid keyspace: the mismatch occurs for " \
                   r"keyspace='{ks}', data center='dc1': RF=3 vs. rack count=2")
    await try_pass("NetworkTopologyStrategy", "'dc1': 3, 'dc2': 1", "false")

    # RF != #racks for dc2, but we accept RF = 0.
    await try_pass("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 0", "true")
    await try_pass("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 0", "false")
    # Ditto, just for dc1.
    await try_pass("NetworkTopologyStrategy", "'dc1': 0, 'dc2': 1", "true")
    await try_pass("NetworkTopologyStrategy", "'dc1': 0, 'dc2': 1", "false")

    # Note: ditto, same as in part 1.
    for rf in [1, 2, 3, 4]:
        await try_pass("SimpleStrategy", f"'replication_factor': {rf}", "false")

    # Part 3: We get rid of dc1/r1. This way, we have two nodes in dc1, but only one rack.

    await manager.decommission_node(s1.server_id)

    # RF = #racks for every DC.
    await try_pass("NetworkTopologyStrategy", "'dc1': 1, 'dc2': 1", "true")
    await try_pass("NetworkTopologyStrategy", "'dc1': 1, 'dc2': 1", "false")

    # RF > #racks for dc1.
    await try_fail("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 1", "true",
                   "The option `rf-rack-valid-keyspaces` is enabled, which forbids creating " \
                   "a materialized view in an RF-rack-invalid keyspace: the mismatch occurs for " \
                   r"keyspace='{ks}', data center='dc1': RF=2 vs. rack count=1")
    await try_pass("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 1", "false")

    # RF != #racks for dc2, but we accept RF = 0.
    await try_pass("NetworkTopologyStrategy", "'dc1': 1, 'dc2': 0", "true")
    await try_pass("NetworkTopologyStrategy", "'dc1': 1, 'dc2': 0", "false")
    # Ditto, just for dc1.
    await try_pass("NetworkTopologyStrategy", "'dc1': 0, 'dc2': 1", "true")
    await try_pass("NetworkTopologyStrategy", "'dc1': 0, 'dc2': 1", "false")

    # Note: ditto, same as in part 1.
    for rf in [1, 2, 3]:
        await try_pass("SimpleStrategy", f"'replication_factor': {rf}", "false")
