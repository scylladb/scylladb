import bisect
import itertools
import logging
import random
import time
from collections import defaultdict, namedtuple
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import IntEnum
from itertools import product
from threading import Event
from uuid import UUID

import pytest
from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.cluster import Session
from cassandra.connection import ConnectionException
from cassandra.metadata import Murmur3Token
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import SimpleStatement
from cassandra.util import datetime_from_uuid1
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode

from dtest_class import Tester, wait_for
from dtest_setup import DTestSetup
from dtest_setup_overrides import DTestSetupOverrides
from tools.cdc_utils import (
    CDC_GENERATIONS_TABLE,
    CDC_STREAMS_TABLE_TABLETS,
    CDC_STREAMS_TABLE_VNODES,
    CDC_TIMESTAMPS_TABLE,
    consistency_for_cdc_streams_query,
)
from tools.cluster_topology import generate_cluster_topology
from tools.data import keyspace_has_tablets
from tools.marks import issue_open
from tools.misc import ImmutableMapping

TOKENS_PER_NODE = 256

logger = logging.getLogger(__name__)


@dataclass
class GenerationId:
    time: datetime
    uuid: UUID


class CdcLogOperations(IntEnum):
    PREIMAGE = 0
    UPDATE = 1
    INSERT = 2
    ROW_DELETE = 3
    PARTITION_DELETE = 4
    RANGE_DELETE_START_INCLUSIVE = 5
    RANGE_DELETE_START_EXCLUSIVE = 6
    RANGE_DELETE_END_INCLUSIVE = 7
    RANGE_DELETE_END_EXCLUSIVE = 8
    POSTIMAGE = 9


class CdcStreamState(IntEnum):
    CURRENT = 0
    CLOSED = 1
    OPENED = 2


CDCDesc = namedtuple("CDCDesc", ["time", "streams"])


class CDCInitializeHelper:
    def populate_sequentially(self, nodes_per_dc: list | int, wait_other_notice: bool = True, custom_args: list[str] | None = None, topo: dict | None = None, rack_per_node: bool = True):
        """
        Populate the cluster and start its nodes one by one, in order.

        :param nodes_per_dc: number of nodes per datacenter.
        :param wait_other_notice: wait notice each started node.
        :param custom_args: first node's extra JVM arguments.
        :param topo: explicit {dc: {rack: node_count}} topology.
        :param rack_per_node: place every node in its own rack.

        The rack layout comes from either topo or rack_per_node - set one or none,
        never both. rack_per_node is only considered when topo is None:

        1. topo is not None - build the cluster with exactly this topology;
            rack_per_node and nodes_per_dc ignored.
        2. topo is None and rack_per_node is true - each dc gets nodes_per_dc
            racks with one node each.
        3. topo is None and rack_per_node is false - each dc gets a single rack
            holding nodes_per_dc nodes.
        """
        if custom_args is None:
            custom_args = []
        cluster: ScyllaCluster = self.cluster
        jvm_args = ["--blocked-reactor-notify-ms", "100" if cluster.scylla_mode != "debug" else "1000000"]
        jvm_args += custom_args
        nodes_per_dc = [nodes_per_dc] if isinstance(nodes_per_dc, int) else nodes_per_dc
        if topo is not None:
            topology = topo
        elif rack_per_node:
            topology = {f"dc{i + 1}": {f"rack{j + 1}": 1 for j in range(node_count)} for i, node_count in enumerate(nodes_per_dc)}
        else:
            topology = {f"dc{i + 1}": {"rack1": node_count} for i, node_count in enumerate(nodes_per_dc)}
        # populate() writes each node's scylla.yaml here seeded with that node
        # and the ones created before it rather than leaving them with
        # scylla's default seed, 127.0.0.1, which might be wrong, because
        # in some environments the first node might listen for gossip on a
        # different address.
        cluster.populate(topology)
        nodes = cluster.nodelist()
        logger.debug(f"Starting node {nodes[0].name}")
        nodes[0].start(wait_for_binary_proto=True, wait_other_notice=wait_other_notice, jvm_args=jvm_args)
        for node in nodes[1:]:
            logger.debug(f"Starting node {node.name}")
            node.start(wait_for_binary_proto=True, wait_other_notice=wait_other_notice)

    def uses_tablets(self, session, table_name):
        keyspace_name = table_name.split(".")[0]
        return keyspace_has_tablets(session, keyspace_name)

    # Retrieve the ID of the last known generation from the local tables of the node `session` is connected to.
    # The ID is a (timestamp, uuid) pair.
    def get_local_generation_id(self, session) -> GenerationId:
        rs = list(session.execute("SELECT streams_timestamp, uuid FROM system.cdc_local WHERE key = 'cdc_local'"))
        assert len(rs) == 1
        return GenerationId(time=rs[0].streams_timestamp, uuid=rs[0].uuid)

    def get_cdc_description_rows(self, session, table_name):
        return self.get_all_cdc_description_rows(session, table_name)

    def get_last_generation_timestamp(self, session):
        timestamps = list(self.get_cdc_generation_timestamps(session))
        assert len(timestamps) > 0, "No CDC generations"
        return max(row.time for row in timestamps)

    def wait_for_last_generation_to_be_active(self, session):
        last_timestamp = self.get_last_generation_timestamp(session)
        # Add one second to account for clock differences
        self.sleep_until(last_timestamp + timedelta(seconds=1))
        logger.debug(f"Current generation timestamp: {last_timestamp}")
        return last_timestamp

    def get_all_cdc_description_rows(self, session, table_name):
        cl = consistency_for_cdc_streams_query(len(self.cluster.nodes))
        ks_name, cf_name = table_name.split(".")
        if self.uses_tablets(session, table_name):
            query = session.prepare(f"SELECT timestamp, stream_id FROM {CDC_STREAMS_TABLE_TABLETS} WHERE keyspace_name = ? AND table_name = ? AND stream_state = ? ALLOW FILTERING")
            query.consistency_level = cl
            rows = session.execute(query, (ks_name, cf_name, CdcStreamState.OPENED))
            streams_by_ts = defaultdict(list)
            for row in rows:
                streams_by_ts[row.timestamp].append(row.stream_id)
            return [CDCDesc(time=ts, streams=streams) for ts, streams in streams_by_ts.items()]
        else:
            query = SimpleStatement(f"SELECT * FROM {CDC_STREAMS_TABLE_VNODES}", consistency_level=cl)
            rows = session.execute(query)
            return [CDCDesc(time=row.time, streams=row.streams) for row in rows]

    def get_single_cdc_description_rows(self, session, gen_ts):
        query = session.prepare(f"SELECT * FROM {CDC_STREAMS_TABLE_VNODES} WHERE time = ?")
        query.consistency_level = consistency_for_cdc_streams_query(len(self.cluster.nodes))
        return session.execute(query, (gen_ts,))

    def get_cdc_generation_timestamps(self, session):
        cl = consistency_for_cdc_streams_query(len(self.cluster.nodes))
        query = SimpleStatement(f"SELECT time FROM {CDC_TIMESTAMPS_TABLE} WHERE key = 'timestamps'", consistency_level=cl)
        return session.execute(query)

    def wait_for_metadata_update(self, session, cluster_size):
        # Cluster metadata is updated asynchronously, so we need to wait
        def check_metadata():
            ring = self.get_vnode_ring(session)
            logger.debug(f"Token ring length: {len(ring)}")
            return len(ring) == cluster_size * 256

        wait_for(check_metadata, timeout=60, text="Waiting until metadata is updated")

    def sleep_until(self, timestamp):
        secs = (timestamp - datetime.utcnow()).total_seconds()
        if secs > 0:
            logger.debug(f"Sleeping for {secs} seconds")
            time.sleep(secs)

    def get_vnode_ring(self, session):
        return list(session.cluster.metadata.token_map.ring)


@pytest.mark.scylla_cdc
@pytest.mark.dtest_full
class TestCdc(Tester, CDCInitializeHelper):
    @pytest.fixture(scope="function", autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        assert dtest_config.is_scylla, "CDC tests are intended for Scylla only"

        ring_delay_sec = 5
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping({"experimental_features": ["cdc"], "ring_delay_ms": ring_delay_sec * 1000, "num_tokens": TOKENS_PER_NODE, "hinted_handoff_enabled": False})
        return dtest_setup_overrides

    @pytest.fixture(
        params=["Single_cluster", "Multi_DC_cluster"],
        ids=("Single_cluster", "Multi_DC_cluster"),
    )
    def cluster_config(self, request):
        ClusterConfig = namedtuple("ClusterConfig", "topology replication")
        if request.param == "Single_cluster":
            nodes = 3
            DC_TOPO = generate_cluster_topology(dc_num=1, rack_num=nodes, nodes_per_rack=1)
            DC_REPL = f"{{'class': 'NetworkTopologyStrategy', 'replication_factor': {nodes}}}"
        else:
            if type(self.cluster) is ScyllaCluster and self.cluster.scylla_mode == "debug":
                dcs = 2
                nodes = 2
            else:
                dcs = 3
                nodes = 3
            DC_TOPO = generate_cluster_topology(dc_num=dcs, rack_num=nodes, nodes_per_rack=1)
            replication = ", ".join(f"'{dc_name}': {nodes}" for dc_name in DC_TOPO.keys())
            DC_REPL = f"{{'class': 'NetworkTopologyStrategy', {replication}}}"
        ret = ClusterConfig(DC_TOPO, DC_REPL)
        logger.debug(f"cluster_config={ret}")
        return ret

    def simple_cdc_template(self, request, topology, replication, with_preimage):
        logger.debug("Setup a cluster")
        cluster: ScyllaCluster = self.cluster
        cluster.populate(topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1: ScyllaNode = cluster.nodes["node1"]
        session = self.patient_cql_connection(node1)

        logger.debug("Wait for the last generation to become active")
        gen_timestamp = self.wait_for_last_generation_to_be_active(session)
        total_nodes = sum(sum(racks.values()) for racks in topology.values())
        self.wait_for_metadata_update(session, cluster_size=total_nodes)
        ring = self.get_vnode_ring(session)

        self.generation_quality_check(session, gen_timestamp, ring)

        logger.debug("Create a table with CDC enabled, and start writing to it")
        finish_writing = self.run_writes_with_counting(request, node1, replication, with_preimage=with_preimage)

        logger.debug("Write for 15 more seconds, and stop writing")
        time.sleep(15)
        write_count = finish_writing()

        base_rows, log_rows = self.get_base_and_log_rows(session, "ks.cf")
        update_rows = self.get_sorted_update_rows(session, "ks.cf", log_rows)

        logger.debug("Check invariants on written data")
        self.check_common_invariants(session, "ks.cf", base_rows, log_rows, update_rows, write_count, with_preimage)
        if not self.uses_tablets(session, "ks.cf"):
            self.check_that_log_entries_and_their_streams_are_in_the_same_vnode(session, base_rows, update_rows, ring)

        logger.debug("Test finished")

    def test_simple_cdc(self, request, cluster_config):
        self.simple_cdc_template(request=request, topology=cluster_config.topology, replication=cluster_config.replication, with_preimage=False)

    @pytest.mark.next_gating
    def test_simple_cdc_with_preimage(self, request, cluster_config):
        self.simple_cdc_template(request=request, topology=cluster_config.topology, replication=cluster_config.replication, with_preimage=True)

    def cluster_expansion_with_cdc_template(self, request, topology, replication, with_preimage):
        logger.debug(f"Setup a cluster: topology={topology} replication={replication} with_preimage={with_preimage}")
        cluster: ScyllaCluster = self.cluster
        cluster.populate(topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        # choose random node from random dc in multidc configuration
        node1: ScyllaNode = random.choice(cluster.nodelist())
        session = self.patient_cql_connection(node1)

        logger.debug("Wait for the last generation to become active")
        gen_timestamp = self.wait_for_last_generation_to_be_active(session)
        total_nodes = sum(sum(racks.values()) for racks in topology.values())
        self.wait_for_metadata_update(session, cluster_size=total_nodes)
        ring_before_expansion = self.get_vnode_ring(session)

        self.generation_quality_check(session, gen_timestamp, ring_before_expansion)

        logger.debug("Create a table with CDC enabled, and start writing to it")
        finish_writing = self.run_writes_with_counting(request, node1, replication, with_preimage=with_preimage)

        logger.debug("Add new node to the cluster")
        expansion_start_time = datetime.utcnow()
        new_node = cluster.new_node(total_nodes + 1, auto_bootstrap=True, data_center=node1.data_center)

        new_node.start(wait_for_binary_proto=True)

        logger.debug("Wait until new generation starts")
        gen_timestamp = self.wait_for_last_generation_to_be_active(session)

        logger.debug("Write for 15 more seconds, and stop writing")
        time.sleep(15)
        write_count = finish_writing()

        self.wait_for_metadata_update(session, cluster_size=total_nodes + 1)
        ring_after_expansion = self.get_vnode_ring(session)
        assert ring_before_expansion != ring_after_expansion

        self.generation_quality_check(session, gen_timestamp, ring_after_expansion)

        base_rows, log_rows = self.get_base_and_log_rows(session, "ks.cf")
        update_rows = self.get_sorted_update_rows(session, "ks.cf", log_rows)

        logger.debug("Check invariants on written data")
        self.check_common_invariants(session, "ks.cf", base_rows, log_rows, update_rows, write_count, with_preimage)

        # Check before expansion
        self.check_that_log_entries_and_their_streams_are_in_the_same_vnode(session, base_rows, update_rows, ring=ring_before_expansion, time_range_end=expansion_start_time)

        # Check after expansion
        after_expansion_timestamp = self.get_timestamp_of_first_generation_after(session, "ks.cf", expansion_start_time)
        self.check_that_log_entries_and_their_streams_are_in_the_same_vnode(session, base_rows, update_rows, ring=ring_after_expansion, time_range_begin=after_expansion_timestamp)

        logger.debug("Test finished")

    @pytest.mark.no_boot_speedups
    @pytest.mark.next_gating
    # the test is not relevant for tablets - cluster expansion doesn't affect CDC streams of tablets-based keyspaces
    @pytest.mark.required_features("!tablets")
    def test_cluster_expansion_with_cdc(self, request, cluster_config):
        self.cluster_expansion_with_cdc_template(request, topology=cluster_config.topology, replication=cluster_config.replication, with_preimage=False)

    @pytest.mark.no_boot_speedups
    # the test is not relevant for tablets - cluster expansion doesn't affect CDC streams of tablets-based keyspaces
    @pytest.mark.required_features("!tablets")
    def test_cluster_expansion_with_cdc_and_preimage(self, request, cluster_config):
        self.cluster_expansion_with_cdc_template(request=request, topology=cluster_config.topology, replication=cluster_config.replication, with_preimage=True)

    def cluster_reduction_with_cdc_template(self, request, topology, replication, with_preimage):
        logger.debug(f"Setup a cluster: topology={topology} replication={replication} with_preimage={with_preimage}")
        cluster: ScyllaCluster = self.cluster
        # increase cluster size by 1 node in each DC, so
        # CL=ALL not failed after node decommission
        expanded_topology = {}
        for dc, racks in topology.items():
            # add a node to the first rack in each DC
            expanded_racks = {}
            for i, (rack, nodes) in enumerate(racks.items()):
                expanded_racks[rack] = nodes + 1 if i == 0 else nodes
            expanded_topology[dc] = expanded_racks
        cluster.populate(expanded_topology).start(wait_for_binary_proto=True, wait_other_notice=True)
        node1 = cluster.nodes["node1"]
        session = self.patient_cql_connection(node1)

        logger.debug("Wait for the last generation to become active")
        gen_timestamp = self.wait_for_last_generation_to_be_active(session)
        total_nodes = sum(sum(racks.values()) for racks in expanded_topology.values())
        self.wait_for_metadata_update(session, cluster_size=total_nodes)
        ring = self.get_vnode_ring(session)

        self.generation_quality_check(session, gen_timestamp, ring)

        logger.debug("Create a table with CDC enabled, and start writing to it")
        finish_writing = self.run_writes_with_counting(request, node1, replication, with_preimage=with_preimage)
        time.sleep(15)

        # pick a node to decommission from an expanded rack
        first_dc = next(iter(expanded_topology.keys()))
        first_rack_name = next(iter(expanded_topology[first_dc].keys()))
        decommission_node = next(node for node in cluster.nodelist() if node.rack == first_rack_name and node.data_center == first_dc and node != node1)

        logger.debug(f"Downsize the cluster by decommissioning {decommission_node.name}")
        reduction_start_time = datetime.utcnow()
        decommission_node.decommission()
        reduction_end_time = datetime.utcnow()

        logger.debug(f"Decommission of node {decommission_node.name} took {reduction_end_time - reduction_start_time}")
        logger.debug("Write for 15 more seconds, and stop writing")
        time.sleep(15)
        write_count = finish_writing()

        base_rows, log_rows = self.get_base_and_log_rows(session, "ks.cf")
        update_rows = self.get_sorted_update_rows(session, "ks.cf", log_rows)

        logger.debug("Check invariants on written data")
        self.check_common_invariants(session, "ks.cf", base_rows, log_rows, update_rows, write_count, with_preimage)
        self.check_that_log_entries_and_their_streams_are_in_the_same_vnode(session, base_rows, update_rows, ring)

        logger.debug("Test finished")

    @pytest.mark.next_gating
    # Test had history of timing out in debug, see: https://github.com/scylladb/scylla-dtest/issues/3275
    @pytest.mark.scylla_mode("!debug")
    # the test is not relevant for tablets - cluster reduction doesn't affect CDC streams of tablets-based keyspaces
    @pytest.mark.required_features("!tablets")
    def test_cluster_reduction_with_cdc(self, request, cluster_config):
        self.cluster_reduction_with_cdc_template(request=request, topology=cluster_config.topology, replication=cluster_config.replication, with_preimage=False)

    # the test is not relevant for tablets - cluster reduction doesn't affect CDC streams of tablets-based keyspaces
    @pytest.mark.required_features("!tablets")
    def test_cluster_reduction_with_cdc_and_preimage(self, request, cluster_config):
        self.cluster_reduction_with_cdc_template(request=request, topology=cluster_config.topology, replication=cluster_config.replication, with_preimage=True)

    # checkAndRepairCdcStreams is not relevant for streams of tablets-based keyspaces
    @pytest.mark.required_features("!tablets")
    def test_check_and_repair_after_cluster_reduction(self):
        # After a decommission, streams no longer match the new token ring structure.
        # In such a case `nodetool checkAndRepairCdcStreams` should trigger regeneration.
        logger.debug("Setup a cluster")
        cluster = self.cluster
        self.populate_sequentially(nodes_per_dc=3)
        node1 = cluster.nodes["node1"]
        node3 = cluster.nodes["node3"]
        session = self.patient_cql_connection(node1)

        gen_timestamp = self.get_last_generation_timestamp(session)
        self.wait_for_metadata_update(session, cluster_size=3)
        ring = self.get_vnode_ring(session)

        logger.debug("Check that the initial generation is OK")
        self.generation_quality_check(session, gen_timestamp, ring)

        logger.debug("Downsize the cluster by one node")
        node3.decommission()

        self.wait_for_metadata_update(session, cluster_size=2)

        logger.debug("Running checkAndRepairCdcStreams...")
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(node1.nodetool, "checkAndRepairCdcStreams")
            try:
                future.result(timeout=60)
            except TimeoutError:
                pytest.fail("checkAndRepairCdcStreams did not terminate in time")

            old_gen_timestamp = gen_timestamp

            def new_gen_appeared():
                gen_timestamp = self.get_last_generation_timestamp(session)
                return gen_timestamp > old_gen_timestamp

            wait_for(new_gen_appeared, 1, "Waiting for new generation to appear", 60)
            gen_timestamp = self.get_last_generation_timestamp(session)

            ring = self.get_vnode_ring(session)
            ring_tokens = set(token.value for token in ring)
            gen_description = list(self.get_single_cdc_description_rows(session, gen_timestamp))
            gen_tokens = set(entry.range_end for entry in gen_description)
            assert gen_tokens == ring_tokens, "New generation should match the token ring"

            logger.debug("Run checkAndRepairCdcStreams again")
            future = executor.submit(node1.nodetool, "checkAndRepairCdcStreams")
            try:
                future.result(timeout=60)
            except TimeoutError:
                pytest.fail("checkAndRepairCdcStreams did not terminate in time")

            gen_timestamp2 = self.get_last_generation_timestamp(session)
            assert gen_timestamp2 == gen_timestamp, "2nd run of checkAndRepairCdcStreams should not regenerate"

            logger.debug("Test finished")

    def schema_change_template(self, request, alter_query, topology, replication, with_preimage=False, additional_fields=None):  # noqa: PLR0913
        if additional_fields is None:
            additional_fields = []
        logger.debug(f"Setup a cluster: topology={topology} replication={replication} with_preimage={with_preimage}")
        cluster = self.cluster
        # Enable migration_manager=trace for debugging scylladb/scylladb#15357
        cluster.populate(topology).start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=["--logger-log-level", "migration_manager=trace"])
        node1 = cluster.nodes["node1"]
        session = self.patient_cql_connection(node1)

        logger.debug("Wait for the last generation to become active")
        gen_timestamp = self.wait_for_last_generation_to_be_active(session)
        total_nodes = sum(sum(racks.values()) for racks in topology.values())
        self.wait_for_metadata_update(session, cluster_size=total_nodes)
        ring = self.get_vnode_ring(session)

        self.generation_quality_check(session, gen_timestamp, ring)

        logger.debug("Create a table with CDC enabled, and start writing to it")
        finish_writing = self.run_writes_with_counting(request=request, node=node1, replication=replication, with_preimage=with_preimage, additional_fields=additional_fields)
        time.sleep(15)

        logger.debug(f"Alter schema: {alter_query}")
        session.execute(alter_query)

        logger.debug("Write for 15 more seconds, and stop writing")
        time.sleep(15)
        write_count = finish_writing()

        base_rows, log_rows = self.get_base_and_log_rows(session, "ks.cf")
        update_rows = self.get_sorted_update_rows(session, "ks.cf", log_rows)

        logger.debug("Check invariants on written data")
        self.check_common_invariants(session, "ks.cf", base_rows, log_rows, update_rows, write_count, with_preimage)
        if not self.uses_tablets(session, "ks.cf"):
            self.check_that_log_entries_and_their_streams_are_in_the_same_vnode(session, base_rows, update_rows, ring)

        logger.debug("Test finished")

    @pytest.mark.next_gating
    @pytest.mark.skip_if(issue_open("scylladb/scylladb#14401"))
    def test_change_field_type_with_cdc(self, request, cluster_config):
        self.schema_change_template(request, "ALTER TABLE ks.cf ALTER b TYPE blob", topology=cluster_config.topology, replication=cluster_config.replication)

    @pytest.mark.skip_if(issue_open("scylladb/scylladb#14401"))
    def test_change_field_type_with_cdc_and_preimage(self, request, cluster_config):
        self.schema_change_template(request, "ALTER TABLE ks.cf ALTER b TYPE blob", topology=cluster_config.topology, replication=cluster_config.replication, with_preimage=True)

    @pytest.mark.next_gating
    def test_add_field_with_cdc(self, request, cluster_config):
        self.schema_change_template(request, "ALTER TABLE ks.cf ADD c int", topology=cluster_config.topology, replication=cluster_config.replication)

    def test_add_field_with_cdc_and_preimage(self, request, cluster_config):
        self.schema_change_template(request, "ALTER TABLE ks.cf ADD c int", topology=cluster_config.topology, replication=cluster_config.replication, with_preimage=True)

    @pytest.mark.next_gating
    def test_remove_field_with_cdc(self, request, cluster_config):
        self.schema_change_template(request, "ALTER TABLE ks.cf DROP c", topology=cluster_config.topology, replication=cluster_config.replication, additional_fields=["c int"])

    def test_remove_field_with_cdc_and_preimage(self, request, cluster_config):
        self.schema_change_template(request, "ALTER TABLE ks.cf DROP c", topology=cluster_config.topology, replication=cluster_config.replication, additional_fields=["c int"], with_preimage=True)

    # Regression test for Scylla issue #7127
    # deselect test from running with consistent-topology-changes
    # the internal table system.cdc_local is not used with raft topology.
    # checkAndRepairCdcStreams is not relevant for streams of tablets-based keyspaces
    @pytest.mark.required_features("!consistent-topology-changes", "!tablets")
    def test_check_and_repair_cdc_streams_liveness(self, fixture_dtest_setup: DTestSetup):
        # During the test, error "Could not find CDC generation" appears as part of the test logic.
        # The teardown fails because it expects a cluster doesn't contain errors if the test is passed.

        fixture_dtest_setup.ignore_log_patterns += [
            "Could not find CDC generation with timestamp .*in distributed system tables.*even though some node gossiped about it.",
        ]

        logger.debug("Setup a single node cluster")
        self.populate_sequentially(nodes_per_dc=1)
        node = self.cluster.nodes["node1"]
        session = self.patient_cql_connection(node)

        logger.debug("Wait for the last generation to become active")
        gen_timestamp = self.wait_for_last_generation_to_be_active(session)

        # Get the UUID of this generation, which is used as the partition key in the GENERATIONS table
        gen_id = self.get_local_generation_id(session)

        # Sanity check: the timestamp stored by the node in system.cdc_local
        # is the timestamp of the last generation, i.e. gen_timestamp
        assert gen_timestamp, gen_id.time

        logger.debug(f"Deleting generation ({gen_timestamp}, {gen_id.uuid})")
        query = session.prepare(f"DELETE FROM {CDC_GENERATIONS_TABLE} WHERE id = ?")
        session.execute(query, (gen_id.uuid,))

        self.ignore_log_patterns += ["Could not find CDC generation"]

        logger.debug("Running checkAndRepairCdcStreams...")
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(node.nodetool, "checkAndRepairCdcStreams")
            try:
                future.result(timeout=60)
            except TimeoutError:
                pytest.fail("checkAndRepairCdcStreams did not terminate in time")

        # Ok, let's also check if the command actually created a generation just in case
        # and perform some sanity checks for the generation's consistency
        logger.debug("Retrieving generation data")
        rows = list(session.execute(f"SELECT id, num_ranges FROM {CDC_GENERATIONS_TABLE}"))
        assert len(rows) > 0, "No CDC generations"

        uuid = rows[0].id
        assert all(r.id == uuid for r in rows), "More than one generation IDs detected, but there should be exactly one"

        num_ranges = rows[0].num_ranges
        assert len(rows) == num_ranges, f"Expected {num_ranges} number of rows, got {len(rows)}"

        logger.debug("Waiting for the generation to appear in the client table...")
        # It should appear pretty much instantaneously, but with those Jenkins machines nobody knows...
        old_gen_timestamp = gen_timestamp

        def new_gen_appeared():
            gen_timestamp = self.get_last_generation_timestamp(session)
            assert gen_timestamp > old_gen_timestamp
            return gen_timestamp > old_gen_timestamp

        wait_for(new_gen_appeared, 1, "waiting for new generation to appear in client table", 60)

        gen_timestamp = self.get_last_generation_timestamp(session)
        assert gen_timestamp > old_gen_timestamp

        logger.debug(f"New generation timestamp: {gen_timestamp}")
        ring = self.get_vnode_ring(session)
        self.generation_quality_check(session, gen_timestamp, ring)

        logger.debug("Test finished")

    def run_writes_with_counting(self, request, node, replication, with_preimage=False, additional_fields=None):  # noqa: PLR0915
        if additional_fields is None:
            additional_fields = []
        cdc_options = "'enabled': true"
        if with_preimage:
            cdc_options += ", 'preimage': true"
            workers_count = 1
        else:
            workers_count = 10

        logger.info(str(replication))
        session = self.patient_cql_connection(node)
        session.execute(f"CREATE KEYSPACE ks WITH replication = {replication!s}")
        fields = ["a int", "b text", *additional_fields]
        session.execute("CREATE TABLE ks.cf ({fields}, PRIMARY KEY(a)) WITH cdc = {{{cdc_options}}}".format(fields=", ".join(fields), cdc_options=cdc_options))

        worker_executor = ThreadPoolExecutor(max_workers=workers_count)
        stop_event = Event()
        request.addfinalizer(lambda: stop_event.set())
        start_time = time.time()

        def print_fatal_exception(worker_id, operation_start_time, e):
            ago = time.time() - operation_start_time
            logger.error(("Worker #{}: Got a critical error for a write that started {} seconds ago: {}. " + "The worker will not continue and the test will fail").format(worker_id, ago, e))

        def run_writes(worker_id):
            confirmed_writes = 0
            unconfirmed_writes = 0
            i = 0
            stmt = session.prepare("INSERT INTO ks.cf (a, b) VALUES (?, ?)")
            stmt.consistency_level = ConsistencyLevel.QUORUM
            # We don't want the driver to retry writes, because this would cause us to count writes incorrectly.
            stmt.retry_policy = FallthroughRetryPolicy()
            while not stop_event.is_set():
                operation_start_time = time.time()
                try:
                    session.execute(stmt, (i, str(worker_id)))
                    confirmed_writes += 1
                except ConnectionException as e:
                    logger.debug(("Worker #{}: Got ConnectionException, probably because the cluster is being downsized, retrying; " + "exception was {}").format(worker_id, e))
                    # We cannot determine if the write was successful.
                    unconfirmed_writes += 1
                except InvalidRequest as e:
                    if "cdc: attempted to get a stream from an earlier generation than the currently used" in str(e):
                        logger.debug(("Worker #{}: Attempted to write with a timestamp older than the current generation. " + "This is a non critical error, continuing; exception was: {}").format(worker_id, e))
                    else:
                        print_fatal_exception(worker_id, operation_start_time, e)
                        raise e
                except Exception as e:
                    print_fatal_exception(worker_id, operation_start_time, e)
                    raise e
                i += 1
            return confirmed_writes, unconfirmed_writes

        futs = [worker_executor.submit(run_writes, i) for i in range(workers_count)]

        def finisher():
            stop_event.set()
            total_confirmed = 0
            total_unconfirmed = 0
            for i, fut in enumerate(futs):
                (confirmed, unconfirmed) = fut.result()
                logger.debug(f"Worker #{i} did {confirmed} successful writes, and {unconfirmed} unconfirmed writes")
                total_confirmed += confirmed
                total_unconfirmed += unconfirmed
            worker_executor.shutdown(wait=True)
            duration = time.time() - start_time
            rows_per_second = float(total_confirmed + total_unconfirmed) / duration
            logger.debug(f"Made ({total_confirmed} confirmed, {total_unconfirmed} unconfirmed) inserts in total over {duration} seconds: {rows_per_second} rows per second")
            return total_confirmed, total_unconfirmed

        return finisher

    def check_common_invariants(self, session, table_name, base_rows, log_rows, update_rows, write_count, with_preimage):  # noqa: PLR0913
        if with_preimage:
            self.check_preimage(log_rows)
        self.check_that_all_writes_were_recorded(session, write_count, update_rows)
        self.check_that_log_corresponds_to_current_state(session, base_rows, update_rows)
        self.check_that_every_log_entry_of_one_partition_is_in_one_stream(session, table_name, update_rows)
        self.check_that_log_entries_are_not_earlier_than_their_stream(session, table_name, update_rows)

    def check_preimage(self, log_rows):
        logger.debug("Check that preimage reflects the previous state of the row")

        latest_rows = {}
        for _, write in itertools.groupby(log_rows, key=lambda r: r.cdc_time):
            writes = list(write)
            if len(writes) > 1:
                # The row was updated
                preimage_row, update_row = writes
                assert preimage_row.cdc_operation == CdcLogOperations.PREIMAGE
                assert update_row.cdc_operation == CdcLogOperations.INSERT
                assert preimage_row.a == update_row.a
                assert preimage_row.b is not None
                old_row = latest_rows[update_row.a]
                assert preimage_row.b == old_row.b, "Preimage did not contain previous state of the row"
            else:
                # This is a new row - no preimage
                update_row = writes[0]
                assert update_row.cdc_operation == CdcLogOperations.INSERT

            latest_rows[update_row.a] = update_row

    def check_that_all_writes_were_recorded(self, session, write_count, update_rows):
        logger.debug('Check that there are as many "insert" operation rows in log table as there were writes')
        confirmed_count, unconfirmed_count = write_count
        assert len(update_rows) >= confirmed_count
        assert len(update_rows) <= confirmed_count + unconfirmed_count

    def check_that_log_corresponds_to_current_state(self, session, base_rows, update_rows):
        logger.debug("Check that most recent records in log table correspond to current state of base table")

        latest_rows = {}
        for row in update_rows:
            if row.a in latest_rows:
                assert datetime_from_uuid1(row.cdc_time) >= datetime_from_uuid1(latest_rows[row.a].cdc_time), f"Update rows are not sorted, pk: {row.a}"
            latest_rows[row.a] = row

        assert len(latest_rows) == len(base_rows)
        for row in base_rows:
            assert row.a in latest_rows
            latest = latest_rows[row.a]
            assert row.a == latest.a
            assert row.b == latest.b

    def check_that_every_log_entry_of_one_partition_is_in_one_stream(self, session, table_name, update_rows):
        logger.debug("Check that, within a generation, a particular partition key may be written to one stream only")

        cdc_desciption_rows = sorted(desc.time for desc in self.get_cdc_description_rows(session, table_name))

        stream_for_partition = {}
        for row in update_rows:
            generation_number = bisect.bisect(cdc_desciption_rows, datetime_from_uuid1(row.cdc_time))
            idx = (generation_number, row.a)
            if idx not in stream_for_partition:
                stream_for_partition[idx] = row.cdc_stream_id
            else:
                assert stream_for_partition[idx] == row.cdc_stream_id, "A partition was written to more than one stream within one generation"

    def check_that_log_entries_are_not_earlier_than_their_stream(self, session, table_name, update_rows):
        logger.debug("Check that log entries do not have earlier timestamp than their stream")
        stream_to_timestamp = self.get_stream_id_to_timestamp_assignment(session, table_name)

        for row in update_rows:
            assert row.cdc_stream_id in stream_to_timestamp
            timestamp = stream_to_timestamp[row.cdc_stream_id]
            assert timestamp <= datetime_from_uuid1(row.cdc_time)

    def check_that_log_entries_and_their_streams_are_in_the_same_vnode(  # noqa: PLR0913
        self,
        session,
        base_rows,
        update_rows,
        ring,
        time_range_begin=None,
        time_range_end=None,
    ):
        logger.debug("Check that log entries and corresponding streams are from the same vnode")

        pk_to_vnode = {r.a: self.get_vnode_for_partition_token(ring, Murmur3Token(r.tok)) for r in base_rows}
        log_vnodes = list(self.get_vnode_for_partition_token(ring, Murmur3Token(r.tok)) for r in update_rows)
        good_vnodes = set(log_vnodes)

        # Check that vnode for base row and vnode for log row match
        for row, log_row_vnode in zip(update_rows, log_vnodes):
            time = datetime_from_uuid1(row.cdc_time)
            if time_range_begin is not None and time < time_range_begin:
                continue
            if time_range_end is not None and time_range_end <= time:
                continue

            base_row_vnode = pk_to_vnode[row.a]
            if base_row_vnode not in good_vnodes:
                continue

            if base_row_vnode != log_row_vnode:
                logger.debug(f"Timestamp of the offending log write: {time}")
            assert base_row_vnode == log_row_vnode

    def generation_quality_check(self, session, gen_timestamp, ring):
        logger.debug("Checking invariants on generation")
        logger.debug("Checking if generation token ranges refine vnodes")
        gen_description = list(self.get_single_cdc_description_rows(session, gen_timestamp))
        gen_tokens = set(entry.range_end for entry in gen_description)
        ring_tokens = set(token.value for token in ring)
        assert ring_tokens <= gen_tokens, "Vnodes should contain generation token ranges"

        logger.debug("Checking that all vnodes have a stream")
        prev_range_end = gen_description[-1].range_end
        for entry in gen_description:
            range_end = entry.range_end
            if range_end > prev_range_end:
                vnode_size = range_end - prev_range_end
            else:
                vnode_size = 2**63 - 1 - prev_range_end + range_end
            if vnode_size > 1:
                assert any(int.from_bytes(stream[0:8], byteorder="big", signed=True) != range_end for stream in entry.streams)
            prev_range_end = range_end

    def get_sorted_update_rows(self, session, table_name, log_rows):
        update_rows = [r for r in log_rows if r.cdc_operation == CdcLogOperations.INSERT]
        assignment = self.get_stream_id_to_timestamp_assignment(session, table_name)
        return sorted(update_rows, key=lambda r: assignment[r.cdc_stream_id])

    def get_stream_id_to_timestamp_assignment(self, session, table_name):
        cdc_descriptions = self.get_all_cdc_description_rows(session, table_name)

        assignment = {}
        for desc in cdc_descriptions:
            for stream_id in desc.streams:
                assert stream_id not in assignment
                assignment[stream_id] = desc.time

        return assignment

    def get_timestamp_of_first_generation_after(self, session, table_name, timestamp):
        cdc_descriptions = list(self.get_cdc_description_rows(session, table_name))
        return min(desc.time for desc in cdc_descriptions if desc.time > timestamp)

    def get_base_and_log_rows(self, session, base_table_name):
        logger.debug("Fetch table data")
        base_rows = list(self.get_base_table_rows(session, base_table_name))

        logger.debug("Fetch cdc log table data")
        log_rows = list(self.get_log_table_rows(session, self.log_table_name(base_table_name)))

        logger.debug(f"There are {len(base_rows)} base table rows and {len(log_rows)} cdc log rows")
        return base_rows, log_rows

    def get_base_table_rows(self, session, base_table_name):
        query = f"SELECT a, b, token(a) AS tok FROM {base_table_name}"
        return session.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.ALL))

    def get_log_table_rows(self, session, log_table_name):
        query = ('SELECT "cdc$stream_id", ' + '"cdc$time", "cdc$batch_seq_no", a, b, "cdc$operation", "cdc$ttl", ' + 'token("cdc$stream_id") AS tok FROM {}').format(log_table_name)
        return session.execute(SimpleStatement(query, consistency_level=ConsistencyLevel.ALL))

    def get_vnode_for_partition_token(self, ring, token):
        idx = bisect.bisect_left(ring, token)
        if idx == 0 or idx == len(ring):
            return (ring[-1], ring[0])
        return (ring[idx - 1], ring[idx])

    def log_table_name(self, base_table_name):
        return base_table_name + "_scylla_cdc_log"


def generate_test_params():
    preimage = ["'full'", "true", "false"]
    others = [False, True]

    test_configs = []
    for v1 in preimage:
        for v2, v3 in product(others, repeat=2):
            test_config = {"preimage": v1, "postimage": v2, "use_regular_column": v3}
            test_configs.append(test_config)

    return test_configs


def generate_test_id(param):
    return "-".join([f"{key}:{value}".lower() for key, value in param.items()]).replace("'", "")


@pytest.mark.scylla_cdc
@pytest.mark.dtest_full
class TestCdcWithCompactStorage(Tester, CDCInitializeHelper):
    expected_fields = ["cdc_stream_id", "cdc_time", "cdc_batch_seq_no", "cdc_end_of_batch", "cdc_operation", "cdc_ttl", "pk", "ck", "reg_column", "cdc_deleted_reg_column"]
    use_reg_column = False
    preimage = False
    postimage = False

    @property
    def create_table_query(self):
        create_table = "CREATE TABLE ks1.cf1 (pk int, ck int, "
        if self.use_reg_column:
            create_table += "reg_column text, "
            self.use_reg_column = True
        create_table += "PRIMARY KEY (pk, ck)) with COMPACT STORAGE and cdc = {'enabled': true"
        if self.preimage:
            create_table += f", 'preimage': {self.preimage}"
        if self.postimage:
            create_table += f", 'postimage': {self.postimage}"
        create_table += " };"

        return create_table

    @property
    def insert_query(self):
        statement = "INSERT INTO ks1.cf1 (pk, ck) VALUES (1, 1);"
        statement_with_reg_column = "INSERT INTO ks1.cf1 (pk, ck, reg_column) VALUES (1, 1, '1');"

        return [statement_with_reg_column] if self.use_reg_column else [statement]

    @property
    def delete_query(self):
        detete = ["DELETE FROM ks1.cf1 WHERE ck = 1 AND pk = 1"]
        delete_with_reg_column = ["DELETE reg_column FROM ks1.cf1 WHERE ck = 1 AND pk = 1", "DELETE FROM ks1.cf1 WHERE ck = 1 AND pk = 1"]

        return delete_with_reg_column if self.use_reg_column else detete

    @property
    def batch_insert_query(self):
        queries = []
        for i in range(5):
            for j in range(10):
                if self.use_reg_column:
                    queries.append(f"INSERT INTO ks1.cf1 (pk, ck, reg_column) VALUES ({i}, {j}, '{j + i}');")
                else:
                    queries.append(f"INSERT INTO ks1.cf1 (pk, ck) VALUES ({i}, {j});")

        return [f"BEGIN BATCH {''.join(queries)} APPLY BATCH;"]

    @property
    def delete_range_query(self):
        delete_range = ["DELETE FROM ks1.cf1 WHERE pk = 1 AND ck > 3 and ck < 6"]
        delete_range_with_reg_column = ["DELETE FROM ks1.cf1 WHERE pk = 1 AND ck > 3 and ck < 6", "DELETE reg_column FROM ks1.cf1 WHERE pk = 1 AND ck > 6 and ck < 9"]

        return delete_range_with_reg_column if self.use_reg_column else delete_range

    def check_cdc_log_rows_with_compact_storage(self, preimage=False, postimage=False, use_regular_column=None):
        self.preimage = preimage
        self.postimage = postimage
        self.use_reg_column = use_regular_column
        create_ks_query = "CREATE KEYSPACE ks1 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 2};"

        self.cluster.populate(generate_cluster_topology(rack_num=2)).start(wait_for_binary_proto=True, wait_other_notice=True)
        cluster: ScyllaCluster = self.cluster
        node: ScyllaNode = cluster.nodelist()[0]
        session: Session = self.patient_cql_connection(node)

        session.execute(create_ks_query)
        session.execute(self.create_table_query)

        for query in self.insert_query:
            logger.debug(query)
            session.execute(query)

        cdc_logs = self.get_cdc_log_rows(session)
        self.assert_columns(cdc_logs)

        for query in self.delete_query:
            logger.debug(query)
            session.execute(query)
        cdc_logs = self.get_cdc_log_rows(session)
        self.assert_columns(cdc_logs)

        for query in self.batch_insert_query:
            logger.debug(query)
            session.execute(query)
        cdc_logs = self.get_cdc_log_rows(session)
        self.assert_columns(cdc_logs)

        with pytest.raises(InvalidRequest, match='Range deletions on "compact storage" schemas are not supported'):
            for query in self.delete_range_query:
                logger.debug(query)
                session.execute(query)
            cdc_logs = self.get_cdc_log_rows(session)
            self.assert_columns(cdc_logs)

    def get_cdc_log_rows(self, session):
        query_statement = SimpleStatement("SELECT * FROM ks1.cf1_scylla_cdc_log")
        query_statement.consistency_level = ConsistencyLevel.ALL
        return list(session.execute(query_statement))

    def assert_columns(self, rows):
        if not self.use_reg_column:
            expected_fields = [field for field in self.expected_fields if field != "v"]
        else:
            expected_fields = self.expected_fields
        for row in rows:
            for column in row._fields:
                assert column in expected_fields, f"CDC log row doesn't have column {column}"

    @pytest.mark.parametrize("test_config", generate_test_params(), ids=generate_test_id)
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_artificial_column_with_type_empty_is_missing(self, test_config):
        self.check_cdc_log_rows_with_compact_storage(preimage=test_config["preimage"], postimage=test_config["postimage"], use_regular_column=test_config["use_regular_column"])
