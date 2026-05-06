#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import pytest
import logging
from collections import defaultdict

from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.protocol import ConfigurationException

from test.pylib.manager_client import ManagerClient, ServerUpState
from test.cluster.conftest import cluster_con
from test.cluster.util import create_new_test_keyspace, get_replication, get_replica_count


@pytest.mark.parametrize("tablets_enabled", [True, False])
@pytest.mark.parametrize("rf_rack_valid_keyspaces", [False, True])
async def test_create_keyspace_with_default_replication_factor(manager: ManagerClient, tablets_enabled: bool, rf_rack_valid_keyspaces: bool, build_mode):
    def get_pf(dc: str, rack: str) -> dict:
        return {'dc': dc, 'rack': rack}

    normal_cfg = {
        'tablets_mode_for_new_keyspaces': 'enabled' if tablets_enabled else 'disabled',
        'rf_rack_valid_keyspaces': rf_rack_valid_keyspaces
    }
    zero_token_cfg = normal_cfg | {'join_ring': False}

    expected_rf = defaultdict(set)

    logging.info("Adding the dc1/r1/s1 server")
    server = await manager.server_add(config=normal_cfg, property_file=get_pf("dc1", "r1"))

    logging.info("Adding dc1/rz rack server as zero-token")
    await manager.server_add(
        config=zero_token_cfg,
        property_file=get_pf("dc1", "rz"),
        expected_server_up_state=ServerUpState.CQL_ALTERNATOR_CONNECTED,
        connect_driver=True,
    )

    logging.info("Adding dc1/r2 rack server")
    await manager.server_add(config=normal_cfg, property_file=get_pf("dc1", "r2"))

    expected_rf["dc1"] = {"r1", "r2"}

    logging.info("Adding dc2/r1 rack server")
    await manager.server_add(config=normal_cfg, property_file=get_pf("dc2", "r1"))

    logging.info("Adding dc2/r2 rack server")
    await manager.server_add(config=normal_cfg, property_file=get_pf("dc2", "r2"))

    expected_rf["dc2"] = {"r1", "r2"}

    if build_mode != "debug":
        logging.info("Adding dc2/r3 rack server")
        await manager.server_add(config=normal_cfg, property_file=get_pf("dc2", "r3"))

        logging.info("Adding dc2/r4 rack server")
        await manager.server_add(config=normal_cfg, property_file=get_pf("dc2", "r4"))

        expected_rf["dc2"] |= {"r3", "r4"}

    if rf_rack_valid_keyspaces == False:
        logging.info("Adding dc3/rz1 rack server as zero-token")
        await manager.server_add(
            config=zero_token_cfg,
            property_file=get_pf("dc3", "rz"),
            expected_server_up_state=ServerUpState.CQL_ALTERNATOR_CONNECTED,
            connect_driver=True,
        )

        logging.info("Adding dc3/rz2 rack server as zero-token")
        await manager.server_add(
            config=zero_token_cfg,
            property_file=get_pf("dc3", "r2"),
            expected_server_up_state=ServerUpState.CQL_ALTERNATOR_CONNECTED,
            connect_driver=True,
        )

    def verify_rf(cql, ks_name: str):
        rep = get_replication(cql, ks_name)
        assert len(rep) == 3
        assert rep['class'] == 'org.apache.cassandra.locator.NetworkTopologyStrategy'

        def verify_dc_rf(dc: str):
            assert get_replica_count(rep[dc]) == len(expected_rf[dc])
            if tablets_enabled and rf_rack_valid_keyspaces:
                assert set(rep[dc]) == expected_rf[dc]

        verify_dc_rf('dc1')
        verify_dc_rf('dc2')

    cql = cluster_con([server.ip_addr], 9042, False,
                        load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()

    logging.info("Create NetworkTopologyStrategy keyspace with default replication factor")
    ks_name = await create_new_test_keyspace(cql, f"""WITH replication =
                                                {{'class': 'NetworkTopologyStrategy'}}
                                                AND tablets = {{ 'enabled': {str(tablets_enabled).lower()} }}""")
    verify_rf(cql, ks_name)

    logging.info("Try to create SimpleStrategy keyspace with default replication factor")
    with pytest.raises(ConfigurationException, match="SimpleStrategy requires a replication_factor strategy option."):
        await create_new_test_keyspace(cql, f"""WITH replication =
                                                {{'class': 'SimpleStrategy'}}
                                                AND tablets = {{ 'enabled': {str(tablets_enabled).lower()} }}""")

    logging.info("Create keyspace with default replication options")
    ks_name = await create_new_test_keyspace(cql, f"""WITH replication =
                                                {{}}
                                                AND tablets = {{ 'enabled': {str(tablets_enabled).lower()} }}""")
    verify_rf(cql, ks_name)
