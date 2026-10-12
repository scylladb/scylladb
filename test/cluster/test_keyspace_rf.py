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

from test import DEBUG_MODES
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.internal_types import ServerUpState
from test.cluster.util import create_new_test_keyspace, get_replication, get_replica_count
from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT


@pytest.mark.parametrize("tablets_enabled", [True, False])
@pytest.mark.parametrize("rf_rack_valid_keyspaces", [False, True])
async def test_create_keyspace_with_default_replication_factor(manager: ScyllaClusterManager, tablets_enabled: bool, rf_rack_valid_keyspaces: bool, build_mode):
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

    if build_mode not in DEBUG_MODES:
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

    cql = manager.con_gen([server.ip_addr], 9042, False,
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


# The dtests below run on vnodes, with Scylla's default (off) for
# rf_rack_valid_keyspaces, which test.py turns on.
DTEST_CONFIG = {"tablets_mode_for_new_keyspaces": "disabled", "rf_rack_valid_keyspaces": False}
NTS = "org.apache.cassandra.locator.NetworkTopologyStrategy"
def connect(manager: ScyllaClusterManager, server):
    """A session to the node, like the dtests' patient_cql_connection():
    statements default to consistency level ONE."""
    cql = manager.con_gen([server.ip_addr]).connect()
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
    return cql
def create_ks(cql, name, rf, replication_class="NetworkTopologyStrategy", tablets=None):
    """Like the dtests' create_ks(): an int rf is the replication_factor
    option, a dict gives the factor per DC."""
    if isinstance(rf, int):
        options = f"'class':'{replication_class}', 'replication_factor':{rf}"
    else:
        options = "'class':'NetworkTopologyStrategy', " + ", ".join(f"'{dc}':'{dc_rf}'" for dc, dc_rf in rf.items())
    query = f"CREATE KEYSPACE {name} WITH replication={{{options}}}"
    if tablets is not None:
        query += f" and tablets={{'initial':{tablets}}}" if tablets else " and tablets={'enabled':false}"
    cql.execute(query)
    cql.execute(f"USE {name}")
# Ported from scylla-dtest schema_test.py::TestSchema::test_alter_keyspace_then_table_with_udt.
# Reproduces scylladb/scylla-enterprise#2989: after a keyspace with a UDT and
# a table using it had its replication changed, altering the table failed on
# the nodes missing the type with "Unknown type".
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.usefixtures("stop_nodes_gracefully")
async def test_alter_keyspace_then_table_with_udt(manager: ScyllaClusterManager):
    # generate_cluster_topology(rack_num=2): datacenter1 with rack1 and rack2.
    servers = await manager.servers_add(2, config=DTEST_CONFIG,
                                        property_file=[{"dc": "datacenter1", "rack": r} for r in ["rack1", "rack2"]])
    cql = connect(manager, servers[0])
    # It is forbidden to re-add a column with client-side timestamps (the
    # dtests' prepare()).
    cql.use_client_timestamp = False
    create_ks(cql, "ks", {"datacenter1": 1})

    cql.execute("CREATE TYPE IF NOT EXISTS ks.my_type (md5 text, basename text)")
    # The dtests' create_cf().
    cql.execute("CREATE COLUMNFAMILY ks.cf (key varchar PRIMARY KEY, c1 text, c2 ks.my_type ) WITH comment='test cf'"
                " AND compaction={'class': 'SizeTieredCompactionStrategy'} AND compression = {}")
    cql.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    cql.execute("SELECT * FROM ks.cf LIMIT 1")

    # get_replication_options(): a rack list RF is flattened into
    # "datacenter1:<index>" keys; it is replaced by both racks, a number by 2.
    [row] = cql.execute("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = 'ks'")
    options = getattr(row, "replication_v2", None) or row.replication
    rf = "'2'" if "datacenter1" in options else "['rack1', 'rack2']"
    cql.execute(f"ALTER KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'datacenter1': {rf}}};")
    cql.execute("ALTER TABLE ks.cf WITH compaction={'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 1}")
    cql.shutdown()
