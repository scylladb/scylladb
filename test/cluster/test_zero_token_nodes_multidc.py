#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import logging
import pytest

from cassandra import ConsistencyLevel
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.query import SimpleStatement
from test.pylib.manager_client import ManagerClient

from test.pylib.util import unique_name
from test.cluster.conftest import cluster_con
from test.cluster.util import create_new_test_keyspace


@pytest.mark.asyncio
@pytest.mark.parametrize('zero_token_nodes', [1, 2])
async def test_zero_token_nodes_multidc_basic(manager: ManagerClient, zero_token_nodes: int):
    """
    Test the basic functionality of a DC with zero-token nodes:
    - adding zero-token nodes to a new DC succeeds
    - with tablets, ensuring enough replicas for tables depends on the number of token-owners in a DC, not all nodes
    - client requests in the presence of zero-token nodes succeed (also when zero-token nodes coordinate)
    """
    normal_cfg = {'endpoint_snitch': 'GossipingPropertyFileSnitch'}
    zero_token_cfg = {'endpoint_snitch': 'GossipingPropertyFileSnitch', 'join_ring': False}
    property_file_dc1 = {'dc': 'dc1', 'rack': 'rack'}
    property_file_dc2 = {'dc': 'dc2', 'rack': 'rack'}

    logging.info('Creating dc1 with 2 token-owning nodes')
    servers = await manager.servers_add(2, config=normal_cfg, property_file=property_file_dc1)

    normal_nodes_in_dc2 = 2 - zero_token_nodes
    logging.info(f'Creating dc2 with {normal_nodes_in_dc2} token-owning and {zero_token_nodes} zero-token nodes')
    servers += await manager.servers_add(zero_token_nodes, config=zero_token_cfg, property_file=property_file_dc2)
    if zero_token_nodes == 1:
        servers.append(await manager.server_add(config=normal_cfg, property_file=property_file_dc2))

    logging.info('Creating connections to dc1 and dc2')
    dc1_cql = cluster_con([servers[0].ip_addr], 9042, False,
                          load_balancing_policy=WhiteListRoundRobinPolicy([servers[0].ip_addr])).connect()
    dc2_cql = cluster_con([servers[2].ip_addr], 9042, False,
                          load_balancing_policy=WhiteListRoundRobinPolicy([servers[2].ip_addr])).connect()

    ks_names = list[str]()
    logging.info('Trying to create tables for different replication factors')
    for rf in range(3):
        failed = False
        ks_name = await create_new_test_keyspace(dc2_cql, f"""WITH replication =
                                     {{'class': 'NetworkTopologyStrategy', 'replication_factor': 2, 'dc2': {rf}}}
                                     AND tablets = {{ 'enabled': true }}""")
        ks_names.append(ks_name)
        try:
            await dc2_cql.run_async(f'CREATE TABLE {ks_names[rf]}.tbl (pk int PRIMARY KEY, v int)')
        except Exception:
            failed = True
        assert failed == (rf > normal_nodes_in_dc2)

    logging.info('Sending requests with different consistency levels')
    for rf in range(normal_nodes_in_dc2 + 1):
        # FIXME: we may add LOCAL_QUORUM to the list below once scylladb/scylladb#20028 is fixed.
        cls = [
            ConsistencyLevel.ONE,
            ConsistencyLevel.TWO,
            ConsistencyLevel.QUORUM,
            ConsistencyLevel.EACH_QUORUM,
            ConsistencyLevel.ALL
        ]
        if rf == 1:
            cls.append(ConsistencyLevel.LOCAL_ONE)

        for cl in cls:
            logging.info('Testing with rf=%s, consistency_level=%s', rf, cl)

            insert_query = SimpleStatement(f'INSERT INTO {ks_names[rf]}.tbl (pk, v) VALUES (1, 1)',
                                           consistency_level=cl)
            await dc1_cql.run_async(insert_query)
            await dc2_cql.run_async(insert_query)

            if cl == ConsistencyLevel.EACH_QUORUM:
                continue  # EACH_QUORUM is supported only for writes

            select_query = SimpleStatement(f'SELECT * FROM {ks_names[rf]}.tbl', consistency_level=cl)
            dc1_result_set = await dc1_cql.run_async(select_query)
            dc2_result_set = await dc2_cql.run_async(select_query)
            assert dc1_result_set
            assert list(dc1_result_set[0]) == [1, 1]
            assert dc2_result_set
            assert list(dc2_result_set[0]) == [1, 1]
