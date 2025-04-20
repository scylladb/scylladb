#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import asyncio
import logging
import sys

from typing import List, Union

import pytest
from cassandra.policies import WhiteListRoundRobinPolicy

from test.cqlpy import nodetool
from cassandra import ConsistencyLevel
from cassandra.protocol import InvalidRequest
from cassandra.query import SimpleStatement
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables, TextType, Column
from test.pylib.util import unique_name
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

    For context, see: scylladb/scylladb#23276.
    """

    cql = None
    cfg = {"rf_rack_valid_keyspaces": "true"}
    cfg_zero_token = {"rf_rack_valid_keyspaces": "true", "join_ring": "false"}

    async def create_aux(ks: str, rfs: Union[List[int], int]):
        if type(rfs) is int:
            rep_opts = f"'replication_factor': {rfs}"
        else:
            rep_opts = ", ".join([f"'dc{i + 1}': {rf}" for i, rf in enumerate(rfs)])
        opts = f"replication = {{'class': 'NetworkTopologyStrategy', {rep_opts}}} AND tablets = {{'enabled': true}}"
        await cql.run_async(f"CREATE KEYSPACE {ks} WITH {opts}")

    async def create_ok(rfs: Union[List[int], int]) -> str:
        ks = unique_name()
        await create_aux(ks, rfs)
        return ks

    async def create_fail(rfs: Union[List[int], int], failed_dc: int, rf: int, rack_count: int):
        ks = unique_name()
        err = r"The option `rf_rack_valid_keyspaces` is enabled. It requires that all keyspaces are RF-rack-valid. " \
              f"That condition is violated: keyspace '{ks}' doesn't satisfy it for DC 'dc{failed_dc}': RF={rf} vs. rack count={rack_count}."
        with pytest.raises(InvalidRequest, match=err):
            await create_aux(ks, rfs)

    async def alter_ok(ks: str, rfs: List[int]) -> None:
        dcs = ", ".join([f"'dc{i + 1}': {rf}" for i, rf in enumerate(rfs)])
        await cql.run_async(f"ALTER KEYSPACE {ks} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', {dcs}}}")

    async def alter_fail(ks: str, rfs: List[int], failed_dc: int, rack_count: int) -> None:
        rf = rfs[failed_dc - 1]
        err = r"The option `rf_rack_valid_keyspaces` is enabled. It requires that all keyspaces are RF-rack-valid. " \
              f"That condition is violated: keyspace '{ks}' doesn't satisfy it for DC 'dc{failed_dc}': RF={rf} vs. rack count={rack_count}."

        with pytest.raises(InvalidRequest, match=err):
            await alter_ok(ks, rfs)

    # Step 1.
    # -------
    # dc1: r1=1, rzerotoken=1 (Note: zero-token nodes have NO impact. In this case, this rack should behave as if it never existed).
    _ = await manager.server_add(property_file={"dc": "dc1", "rack": "r1"}, config=cfg)
    _ = await manager.server_add(property_file={"dc": "dc1", "rack": "rzerotoken"}, config=cfg_zero_token)
    cql = manager.get_cql()

    ks1 = await create_ok([1])
    await create_ok([0])
    await create_ok(0)
    await create_ok(1)
    await create_fail([2], 1, 2, 1)
    await create_fail(2, 1, 2, 1)

    await alter_ok(ks1, [0])
    await alter_ok(ks1, [1])
    # Check if it works if the RF doesn't change.
    await alter_ok(ks1, [1])

    await alter_fail(ks1, [2], 1, 1)

    # Step 2.
    # -------
    # dc1: r1=1, rzerotoken=1.
    # dc2: r1=1.
    _ = await manager.server_add(property_file={"dc": "dc2", "rack": "r1"}, config=cfg)

    ks2 = await create_ok([1, 1])

    tasks = [
        create_ok([0, 1]),
        create_ok([0, 0]),
        create_ok([1, 0]),
        create_ok(0),
        create_ok(1),
        create_fail([2, 1], 1, 2, 1),
        create_fail([1, 2], 2, 2, 1),
        create_fail([2, 0], 1, 2, 1)
    ]

    # To reduce the latency.
    async with asyncio.TaskGroup() as tg:
        for task in tasks:
            _ = tg.create_task(task)

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

    # Step 3.
    # -------
    # dc1: r1=1, r2=1, rzerotoken=1.
    # dc2: r1=1.
    _ = await manager.server_add(property_file={"dc": "dc1", "rack": "r2"}, config=cfg)

    ks3 = await create_ok([2, 1])
    # RF = 1 is always OK!
    ks4 = await create_ok([1, 1])

    tasks = [
        create_ok([2, 0]),
        create_ok([0, 1]),
        create_ok(0),
        create_ok(1),
        create_fail([1, 2], 2, 2, 1),
        create_fail([2, 2], 2, 2, 1),
        create_fail(2, 2, 2, 1)
    ]

    # To reduce the latency.
    async with asyncio.TaskGroup() as tg:
        for task in tasks:
            _ = tg.create_task(task)

    await alter_ok(ks1, [2, 1])
    await alter_fail(ks1, [2, 2], 2, 1)

    await alter_ok(ks2, [2, 1])
    await alter_ok(ks3, [2, 1])
    await alter_ok(ks4, [2, 1])
    # RF = 1 is always OK!
    await alter_ok(ks3, [1, 1])

@pytest.mark.asyncio
async def test_arbiter_dc_rf_rack_valid_keyspaces(manager: ManagerClient):
    """
    This test verifies that Scylla rejects RF-rack-invalid keyspaces
    in presence of an arbiter data center.

    For context, see: scylladb/scylladb#23276 and scylladb/scylladb#20356.
    """

    cfg = {"rf_rack_valid_keyspaces": "true"}
    zero_token_cfg = {"rf_rack_valid_keyspaces": "true", "join_ring": "false"}

    # Regular DC.
    _ = await manager.server_add(config=cfg, property_file={"dc": "dc1", "rack": "r1"})
    _ = await manager.server_add(config=cfg, property_file={"dc": "dc1", "rack": "r2"})
    _ = await manager.server_add(config=cfg, property_file={"dc": "dc1", "rack": "r3"})
    # Zero-token node. This rack should behave as if it didn't exist.
    _ = await manager.server_add(config=zero_token_cfg, property_file={"dc": "dc1", "rack": "r4"})

    # Arbiter DC.
    _ = await manager.server_add(config=zero_token_cfg, property_file={"dc": "dc2", "rack": "r1"})
    _ = await manager.server_add(config=zero_token_cfg, property_file={"dc": "dc2", "rack": "r2"})
    _ = await manager.server_add(config=zero_token_cfg, property_file={"dc": "dc2", "rack": "r3"})

    cql = manager.get_cql()

    async def create_aux(ks: str, rfs: Union[List[int], int]):
        if type(rfs) is int:
            rep_opts = f"'replication_factor': {rfs}"
        else:
            rep_opts = f"'dc1': {rfs[0]}, 'dc2': {rfs[1]}"
        opts = f"replication = {{'class': 'NetworkTopologyStrategy', {rep_opts}}} AND tablets = {{'enabled': true}}"
        try:
            await cql.run_async(f"CREATE KEYSPACE {ks} WITH {opts}")
        finally:
            await cql.run_async(f"DROP KEYSPACE IF EXISTS {ks}")

    async def create_ok(rfs: Union[List[int], int]):
        ks = unique_name()
        await create_aux(ks, rfs)

    async def create_fail(rfs: Union[List[int], int], failed_dc: int, rf: int, rack_count: int):
        ks = unique_name()
        err = r"The option `rf_rack_valid_keyspaces` is enabled. It requires that all keyspaces are RF-rack-valid. " \
              f"That condition is violated: keyspace '{ks}' doesn't satisfy it for DC 'dc{failed_dc}': RF={rf} vs. rack count={rack_count}."
        with pytest.raises(InvalidRequest, match=err):
            await create_aux(ks, rfs)

    valid_keyspaces = [
        create_ok([0, 0]),
        create_ok([1, 0]),
        create_ok([3, 0]),
        create_ok(0)
    ]

    # We don't consider cases where the RF for dc1 is 2 and the RF for dc2 is a positive number
    # because then we can't predict what error will say.
    invalid_keyspaces = [
        create_fail([4, 0], 1, 4, 3),
        create_fail([2, 0], 1, 2, 3),
        create_fail([0, 1], 2, 1, 0),
        create_fail([0, 2], 2, 2, 0),
        create_fail([0, 3], 2, 3, 0),
        create_fail([1, 1], 2, 1, 0),
        create_fail([1, 2], 2, 2, 0),
        create_fail([1, 3], 2, 3, 0),
        create_fail([3, 1], 2, 1, 0),
        create_fail([3, 2], 2, 2, 0),
        create_fail([3, 3], 2, 3, 0),
        create_fail(1, 2, 1, 0),
        create_fail(3, 2, 3, 0)
    ]

    async with asyncio.TaskGroup() as tg:
        for task in [*valid_keyspaces, *invalid_keyspaces]:
            _ = tg.create_task(task)

async def test_startup_with_keyspaces_violating_rf_rack_valid_keyspaces(manager: ManagerClient):
    """
    This test verifies that starting a Scylla node fails when there's an RF-rack-invalid keyspace.
    We aim to simulate the behavior of a node when upgrading to 2025.*.

    For more context, see: scylladb/scylladb#23300.
    """

    cfg_false = {"rf_rack_valid_keyspaces": "false"}

    s1 = await manager.server_add(config=cfg_false, property_file={"dc": "dc1", "rack": "r1"})
    _ = await manager.server_add(config=cfg_false, property_file={"dc": "dc1", "rack": "r2"})
    _ = await manager.server_add(config=cfg_false, property_file={"dc": "dc1", "rack": "r3"})
    _ = await manager.server_add(config=cfg_false, property_file={"dc": "dc2", "rack": "r4"})
    # Note: This rack should behave as if it never existed.
    _ = await manager.server_add(config={"join_ring": "false", "rf_rack_valid_keyspaces": "false"}, property_file={"dc": "dc1", "rack": "rzerotoken"})

    # Current situation:
    # DC1: {r1, r2, r3}, DC2: {r4}

    cql = manager.get_cql()

    async def create_keyspace(rfs: List[int], tablets: bool) -> str:
        dcs = ", ".join([f"'dc{i + 1}': {rf}" for i, rf in enumerate(rfs)])
        name = unique_name()
        tablets = str(tablets).lower()
        await cql.run_async(f"CREATE KEYSPACE {name} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', {dcs}}} "
                            f"AND tablets = {{'enabled': {tablets}}}")
        logger.info(f"Created keyspace {name} with {rfs} and tablets={tablets}")
        return name

    valid_keyspaces = [
        # For each DC: RF \in {0, 1, #racks}.
        ([0, 0], True),
        ([1, 0], True),
        ([3, 0], True),
        ([1, 1], True),
        ([3, 1], True),
        # Reminder: Keyspaces not using tablets are all valid.
        ([0, 0], False),
        ([1, 0], False),
        ([2, 0], False),
        ([3, 0], False),
        ([4, 0], False),
        ([0, 1], False),
        ([1, 1], False),
        ([2, 1], False),
        ([3, 1], False),
        ([4, 1], False),
        ([0, 2], False),
        ([1, 2], False),
        ([2, 2], False),
        ([3, 2], False),
        ([4, 2], False)
    ]

    # Populate RF-rack-valid keyspaces.
    async with asyncio.TaskGroup() as tg:
        for rfs, tablets in valid_keyspaces:
            _ = tg.create_task(create_keyspace(rfs, tablets))

    await manager.server_stop_gracefully(s1.server_id)
    await manager.server_update_config(s1.server_id, "rf_rack_valid_keyspaces", "true")

    async def try_fail(rfs: List[int], dc: str, rf: int, rack_count: int):
        ks = await create_keyspace(rfs, True)
        err = r"The option `rf_rack_valid_keyspaces` is enabled. It requires that all keyspaces are RF-rack-valid. " \
              f"That condition is violated: keyspace '{ks}' doesn't satisfy it for DC '{dc}': RF={rf} vs. rack count={rack_count}."
        _ = await manager.server_start(s1.server_id, expected_error=err)
        await cql.run_async(f"DROP KEYSPACE {ks}")

    # Test RF-rack-invalid keyspaces.
    await try_fail([2, 0], "dc1", 2, 3)
    await try_fail([3, 2], "dc2", 2, 1)
    await try_fail([4, 1], "dc1", 4, 3)

    _ = await manager.server_start(s1.server_id)

@pytest.mark.asyncio
async def test_restart_with_prefer_local(request: pytest.FixtureRequest, manager: ManagerClient) -> None:
    logger.info("Creating a new cluster")
    for i in range(3):
        s_info = await manager.server_add(
            config=CONFIG,
            property_file={'dc': f'dc{i}', 'rack': 'myrack1', 'prefer_local': 'true'}
        )
        logger.info(s_info)

    await manager.server_stop_gracefully(s_info.server_id)
    await manager.server_start(s_info.server_id)
