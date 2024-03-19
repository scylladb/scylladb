#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import logging
import sys

import pytest
from cassandra.policies import WhiteListRoundRobinPolicy


sys.path.insert(0, sys.path[0] + "/test/cql-pytest")
import nodetool
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables, TextType, Column
from test.pylib.util import unique_name
from test.topology.conftest import cluster_con

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
    random_tables = RandomTables(request.node.name, manager, unique_name(), 3)
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
