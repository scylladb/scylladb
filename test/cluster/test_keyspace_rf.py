#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
import pytest
import logging

from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.protocol import ConfigurationException

from test.pylib.manager_client import ManagerClient
from test.cluster.conftest import cluster_con
from test.cluster.util import create_new_test_keyspace

@pytest.mark.asyncio
@pytest.mark.parametrize("tablets_enabled", [True, False])
@pytest.mark.parametrize("rf_rack_valid_keyspaces", [False, True])
async def test_create_keyspace_with_default_replication_factor(manager: ManagerClient, tablets_enabled: bool, rf_rack_valid_keyspaces: bool):
    def get_pf(dc: str, rack: str) -> dict:
        return {'dc': dc, 'rack': rack}

    logging.info("Trying to add a zero-token server in the gossip-based topology")
    await manager.server_add(config={'join_ring': False,
                                     'force_gossip_topology_changes': True,
                                     'tablets_mode_for_new_keyspaces': 'disabled'},
                             property_file={'dc': 'dc1', 'rack': 'rz'},
                             expected_error='the raft-based topology is disabled')

    normal_cfg = {
        'tablets_mode_for_new_keyspaces': 'enabled' if tablets_enabled else 'disabled',
        'rf_rack_valid_keyspaces': rf_rack_valid_keyspaces
    }
    zero_token_cfg = normal_cfg | {'join_ring': False}

    logging.info("Adding the dc1/r1/s1 server")
    server = await manager.server_add(config=normal_cfg, property_file=get_pf("dc1", "r1"))

    logging.info("Adding dc1/rz rack server as zero-token")
    await manager.server_add(config=zero_token_cfg, property_file=get_pf("dc1", "rz"))

    logging.info("Adding dc1/r2 rack server")
    await manager.server_add(config=normal_cfg, property_file=get_pf("dc1", "r2"))

    logging.info("Adding dc2/r1 rack server")
    await manager.server_add(config=normal_cfg, property_file=get_pf("dc2", "r1"))

    logging.info("Adding dc2/r2 rack server")
    await manager.server_add(config=normal_cfg, property_file=get_pf("dc2", "r2"))

    if rf_rack_valid_keyspaces == False:
        logging.info("Adding dc3/rz1 rack server as zero-token")
        await manager.server_add(config=zero_token_cfg, property_file=get_pf("dc3", "rz"))

        logging.info("Adding dc3/rz2 rack server as zero-token")
        await manager.server_add(config=zero_token_cfg, property_file=get_pf("dc3", "r2"))

    cql = cluster_con([server.ip_addr], 9042, False,
                        load_balancing_policy=WhiteListRoundRobinPolicy([server.ip_addr])).connect()

    logging.info("Create NetworkTopologyStrategy keyspace with default replication factor")
    ks_name = await create_new_test_keyspace(cql, f"""WITH replication =
                                                {{'class': 'NetworkTopologyStrategy'}}
                                                AND tablets = {{ 'enabled': {str(tablets_enabled).lower()} }}""")
    rows = await cql.run_async(f"SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '{ks_name}'")
    assert len(rows) == 1
    assert rows[0].keyspace_name == ks_name
    rep = rows[0].replication
    assert len(rep) == 3
    assert rep['class'] == 'org.apache.cassandra.locator.NetworkTopologyStrategy'
    assert rep['dc1'] == '2'
    assert rep['dc2'] == '2'

    logging.info("Try to create SimpleStrategy keyspace with default replication factor")
    with pytest.raises(ConfigurationException, match="SimpleStrategy requires a replication_factor strategy option."):
        await create_new_test_keyspace(cql, f"""WITH replication =
                                                {{'class': 'SimpleStrategy'}}
                                                AND tablets = {{ 'enabled': {str(tablets_enabled).lower()} }}""")

    logging.info("Create keyspace with default replication options")
    ks_name = await create_new_test_keyspace(cql, f"""WITH replication =
                                                {{}}
                                                AND tablets = {{ 'enabled': {str(tablets_enabled).lower()} }}""")
    rows = await cql.run_async(f"SELECT * FROM system_schema.keyspaces WHERE keyspace_name = '{ks_name}'")
    assert len(rows) == 1
    assert rows[0].keyspace_name == ks_name
    rep = rows[0].replication
    assert len(rep) == 3
    assert rep['class'] == 'org.apache.cassandra.locator.NetworkTopologyStrategy'
    assert rep['dc1'] == '2'
    assert rep['dc2'] == '2'
