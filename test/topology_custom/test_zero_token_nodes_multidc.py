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
from test.topology.conftest import cluster_con
from test.topology.util import create_new_test_keyspace


@pytest.mark.asyncio
@pytest.mark.parametrize('zero_token_nodes', [1, 2])
@pytest.mark.parametrize('rf_rack_valid_keyspaces', [False, True])
async def test_zero_token_nodes_multidc_basic(manager: ManagerClient, zero_token_nodes: int, rf_rack_valid_keyspaces: bool):
    """
    Test the basic functionality of a DC with zero-token nodes:
    - adding zero-token nodes to a new DC succeeds
    - with tablets, ensuring enough replicas for tables depends on the number of token-owners in a DC, not all nodes
    - client requests in the presence of zero-token nodes succeed (also when zero-token nodes coordinate)
    """
    normal_cfg = {'endpoint_snitch': 'GossipingPropertyFileSnitch', 'rf_rack_valid_keyspaces': rf_rack_valid_keyspaces}
    zero_token_cfg = normal_cfg | {'join_ring': False}
    property_file_dc2 = {'dc': 'dc2', 'rack': 'rack'}

    logging.info('Creating dc1 with 2 token-owning nodes')
    servers = await manager.servers_add(2, config=normal_cfg, auto_rack_dc='dc1')

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

    # With `rf_rack_valid_keyspaces` set to true, we cannot create a keyspace with RF > #racks.
    # Because of that, the test will fail not at the stage when a TABLE is created, but when
    # the KEYSPACE is. We want to avoid that and hence this statement.
    #
    # rf_rack_valid_keyspaces == False: We'll attempt to create tables in keyspaces with too few normal token owners.
    # rf_rack_valid_keyspaces == True:  We'll only create RF-rack-valid keyspaces and so all of the created tables
    #                                   will have enough normal token owners.
    rf_range = (normal_nodes_in_dc2 + 1) if rf_rack_valid_keyspaces else 3

    for rf in range(rf_range):
        failed = False
        ks_name = await create_new_test_keyspace(dc2_cql, f"""WITH replication =
                                     {{'class': 'NetworkTopologyStrategy', 'replication_factor': 2, 'dc2': {rf}}}
                                     AND tablets = {{ 'enabled': true }}""")
        ks_names.append(ks_name)
        try:
            await dc2_cql.run_async(
                f'CREATE TABLE {ks_names[rf]}.tbl (cl int, zero_token boolean, v int, PRIMARY KEY (cl, zero_token))')
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

            insert_queries = [
                SimpleStatement(
                    f'INSERT INTO {ks_names[rf]}.tbl (cl, zero_token, v) VALUES ({cl}, {zero_token_coordinator}, {cl})',
                    consistency_level=cl
                ) for zero_token_coordinator in [False, True]
            ]
            await dc1_cql.run_async(insert_queries[0])
            await dc2_cql.run_async(insert_queries[1])

            if cl == ConsistencyLevel.EACH_QUORUM:
                continue  # EACH_QUORUM is supported only for writes

            select_queries = [
                SimpleStatement(
                    f'SELECT * FROM {ks_names[rf]}.tbl WHERE cl = {cl} AND zero_token = {zero_token_coordinator}',
                    consistency_level=cl
                ) for zero_token_coordinator in [False, True]
            ]
            dc1_result_set = await dc1_cql.run_async(select_queries[0])
            dc2_result_set = await dc2_cql.run_async(select_queries[1])
            # With CL=ONE we don't have a guarantee that the replicas written to and read from have a non-empty
            # intersection. Hence, reads could miss the written rows.
            assert cl == ConsistencyLevel.ONE or (dc1_result_set and dc2_result_set)
            if dc1_result_set:
                assert list(dc1_result_set[0]) == [cl, False, cl]
            if dc2_result_set:
                assert list(dc2_result_set[0]) == [cl, True, cl]
