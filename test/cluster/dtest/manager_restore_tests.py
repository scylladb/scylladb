import logging
import os
import re
import shutil
import time
from glob import glob

import pytest

from dtest_class import Tester
from dtest_scylla_manager import (
    C1_PREFIX,
    C2_PREFIX,
    ScyllaManagerError,
    ScyllaManagerMixin,
    TaskStatus,
)
from encryption_at_rest_test import EncryptionAtRestBase, KeyProviderEnum
from manager_backup_tests import ManagerBackupMixin, fake_gcs_docker, minio_docker
from tools.cluster_topology import generate_cluster_topology, generate_cluster_topology_based_rf
from tools.files import get_list_of_sstables

CLUSTER_NAME = "cluster1"
DESTINATION_BUCKET = "backup-bucket"
DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE = {"ks": {"cf1": (1, 21)}}

logger = logging.getLogger(__name__)


class TestScyllaMgmtRestoreBase(Tester, ManagerBackupMixin, ScyllaManagerMixin):
    @pytest.fixture(params=["rclone", "native"], scope="function", autouse=True)
    def setup_manager_method(self, request):
        self.method = request.param  # --method parameter is not implemented in Scylla Manager yet for restore

    @pytest.fixture(params=["s3", "gcs"], scope="function", autouse=True)
    def setup_backend(self, request):
        self.backend = request.param

    def verify_c1c2(self, node, keyspace_table_and_key_range=None):
        if keyspace_table_and_key_range is None:
            keyspace_table_and_key_range = DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE
        super().verify_c1c2(keyspace_table_and_key_range=keyspace_table_and_key_range, node=node)

    def _backup_and_cleanup(self, healthy_node, mgr_cluster, keyspace_table_and_key_range):
        backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=list(keyspace_table_and_key_range.keys()))
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        self.clean_up_tables(node=healthy_node, keyspace_and_tables_dict=keyspace_table_and_key_range)
        return backup_task

    def insert_data_backup_and_cleanup(self, healthy_node, mgr_cluster, keyspace_table_and_key_range=None, rf=2):
        if keyspace_table_and_key_range is None:
            keyspace_table_and_key_range = DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE
        self.insert_data_from_ranges(healthy_node=healthy_node, keyspace_table_and_key_range=keyspace_table_and_key_range, rf=rf)
        backup_task = self._backup_and_cleanup(healthy_node=healthy_node, mgr_cluster=mgr_cluster, keyspace_table_and_key_range=keyspace_table_and_key_range)
        return backup_task

    def restore_and_verify(self, mgr_cluster, backup_task, healthy_node):
        restore_task = mgr_cluster.run_restore_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], restore_data=True, snapshot_tag=backup_task.get_snapshot_tag())
        final_status = restore_task.wait_and_get_final_status(timeout=1200, step=5)
        assert final_status == TaskStatus.DONE, f"Restore task failed: {restore_task.full_progress_string()}"
        self.verify_c1c2(node=healthy_node)

    def restore_schema(self, mgr_cluster, backup_task, cluster=None):
        target_cluster = cluster if cluster else self.cluster
        restore_task = mgr_cluster.run_restore_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], restore_schema=True, snapshot_tag=backup_task.get_snapshot_tag())
        final_status = restore_task.wait_and_get_final_status(step=5)
        assert final_status == TaskStatus.DONE, f"Restore task failed: {restore_task.full_progress_string()}"
        for node in target_cluster.nodelist():
            node.stop(wait_other_notice=True)
            node.start(wait_other_notice=True, wait_for_binary_proto=True)
            self.configure_agent(node)

    def restore_and_verify_using_stress(  # noqa: PLR0913
        self,
        mgr_cluster,
        backup_task,
        healthy_node,
        number_of_rows,
        threads=5,
        batch_size=None,
    ):
        restore_task = mgr_cluster.run_restore_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], restore_data=True, snapshot_tag=backup_task.get_snapshot_tag(), batch_size=batch_size)
        final_status = restore_task.wait_and_get_final_status(step=5)
        assert final_status == TaskStatus.DONE, f"Restore task failed: {restore_task.full_progress_string()}"
        self.cluster.stress(["read", f"n={number_of_rows}", "-rate", f"threads={threads}"])


@pytest.mark.scylla_manager
class TestScyllaMgmtRestore(TestScyllaMgmtRestoreBase):
    def test_basic_restore(self):
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(node1, mgr_cluster)
        self.restore_and_verify(mgr_cluster, backup_task, node1)

    def test_restore_removed_table(self):
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster)
        self._drop_table_and_delete_table_dir("ks", "cf1", node1)
        try:
            mgr_cluster.run_restore_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], restore_data=True, snapshot_tag=backup_task.get_snapshot_tag())
        except ScyllaManagerError as err:
            assert "table" in err.args[0].lower() and "is missing in the restored cluster" in err.args[0].lower(), f"Trying to restore a dropped table failed with an improper error message: {err.args[0]}"

    def _compare_single_column(self, node, column_name, prefix):
        keyspace_name = next(iter(DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE.keys()))
        table_name = next(iter(DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE[keyspace_name].keys()))
        key_range = DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE[keyspace_name][table_name]

        expected_values = sorted([prefix % i for i in range(*key_range)])
        session = self.patient_cql_connection(node)
        query_result = session.execute(f"select {column_name} from {keyspace_name}.{table_name}")
        value_list = []
        for row in query_result:
            value_list.append(getattr(row, column_name))
        value_list.sort()
        assert value_list == expected_values

    def _validate_all_column_values_none(self, node, column_name):
        keyspace_name = next(iter(DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE.keys()))
        table_name = next(iter(DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE[keyspace_name].keys()))
        session = self.patient_cql_connection(node)
        query_result = session.execute(f"select {column_name} from {keyspace_name}.{table_name}")
        value_list = []
        for row in query_result:
            value_list.append(getattr(row, column_name))
        is_all_values_none = map(lambda x: x is None, value_list)
        assert all(is_all_values_none), f"Some of the values of the column {column_name} are not None: {value_list}"

    # Apparently load and stream is really error resistant and changes in the schema would not cause it to fail
    # Hence, the restore will not fail when the schema is altered.
    def test_restore_after_adding_column(self):
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster)
        session = self.patient_cql_connection(node1)
        session.execute("ALTER TABLE ks.cf1 ADD c3 int")
        restore_task = mgr_cluster.run_restore_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], restore_data=True, snapshot_tag=backup_task.get_snapshot_tag())
        final_status = restore_task.wait_and_get_final_status(step=10)
        assert final_status == TaskStatus.DONE, f"The restore task should not fail when the schema has been altered, but the restore task has reached the status of {final_status} after a column was added to the target table"
        self._compare_single_column(node=node1, column_name="key", prefix="k%d")
        self._compare_single_column(node=node1, column_name="c1", prefix=C1_PREFIX)
        self._compare_single_column(node=node1, column_name="c2", prefix=C2_PREFIX)
        self._validate_all_column_values_none(node=node1, column_name="c3")

    def test_restore_after_removing_column(self):
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster)
        session = self.patient_cql_connection(node1)
        session.execute("ALTER TABLE ks.cf1 DROP c2")
        restore_task = mgr_cluster.run_restore_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], restore_data=True, snapshot_tag=backup_task.get_snapshot_tag())
        final_status = restore_task.wait_and_get_final_status(step=10)
        # Apparently load and stream is really error resistant and changes in the schema would not cause it to fail
        assert final_status == TaskStatus.DONE, f"The restore task should not fail when the schema has been altered, but the restore task has reached the status of {final_status} after a column from the target table was removed"
        self._compare_single_column(node=node1, column_name="key", prefix="k%d")
        self._compare_single_column(node=node1, column_name="c1", prefix=C1_PREFIX)

    def test_restore_after_replacing_column(self):
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster)
        session = self.patient_cql_connection(node1)
        session.execute("ALTER TABLE ks.cf1 DROP c2")
        session.execute("ALTER TABLE ks.cf1 ADD c2 int")
        restore_task = mgr_cluster.run_restore_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], restore_data=True, snapshot_tag=backup_task.get_snapshot_tag())
        final_status = restore_task.wait_and_get_final_status(step=10)
        # Apparently load and stream is really error resistant and changes in the schema would not cause it to fail
        assert final_status == TaskStatus.DONE, f"The restore task should not fail when the schema has been altered, but the restore task has reached the status of {final_status} after a column from the target table was removed"
        self._compare_single_column(node=node1, column_name="key", prefix="k%d")
        self._compare_single_column(node=node1, column_name="c1", prefix=C1_PREFIX)
        self._validate_all_column_values_none(node=node1, column_name="c2")

    def test_restore_after_decommission(self):
        topology_layout = {"dc1": {"rack1": 1, "rack2": 2}}
        node1, _node2, node3 = self.config_and_create_cluster(topology=topology_layout)

        # Make sure we're decommissioning a node from the right rack.
        # There must remain at least node in it afterwards.
        assert node3.rack == "rack2"

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster)
        node3.nodetool("decommission")
        self.restore_and_verify(mgr_cluster, backup_task, node1)

    def _add_new_node_and_wait_up_normal(self, healthy_node, datacenter=None, rack=None, cluster=None):
        if cluster is None:
            cluster = self.cluster

        node_index = len(cluster.nodelist()) + 1
        new_node = cluster.new_node(node_index, auto_bootstrap=True, add_node=True, is_seed=False, data_center=datacenter, rack=rack)
        new_node.start()

        self._wait_until_node_reaches_status(new_node, healthy_node, "UN", tolerate_missing=True)
        self.configure_agent(new_node)  # the new node's manager agent needs to be configured to have same object storage settings
        return new_node

    def test_restore_after_adding_new_node(self):
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster, rf=2)
        node3 = self._add_new_node_and_wait_up_normal(healthy_node=node1, datacenter=node1.get_datacenter_name(), rack=node1.rack)
        node3.nodetool("repair")
        self.restore_and_verify(mgr_cluster, backup_task, node1)

    def test_restore_after_adding_new_dc(self):
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2, dc_name_prefix="dc", rack_name_prefix="rack")
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster)
        self._add_new_node_and_wait_up_normal(healthy_node=node1, datacenter="dc2", rack="rack3")
        self.restore_and_verify(mgr_cluster, backup_task, node1)

    @pytest.mark.xfail(reason="https://github.com/scylladb/scylla-manager/issues/3896")
    def test_restore_after_remove_dc(self):
        topology_layout = {"dc1": {"rack1": 1, "rack2": 1}, "dc2": {"rack3": 1}}
        node1, _node2, node3 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster, rf={"dc1": 2, "dc2": 1})
        node3.nodetool("decommission")
        self.restore_and_verify(mgr_cluster, backup_task, node1)

    @pytest.mark.xfail(reason="https://github.com/scylladb/scylladb/issues/19504")
    def test_restore_different_dc(self, secondary_cluster):
        # manager_backend_cluster
        backend_cluster_topology = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2, dc_name_prefix="dc")
        self.config_and_create_cluster(topology=backend_cluster_topology)
        # test cluster
        test_cluster_topology = generate_cluster_topology_based_rf(dc_num=1, nodes=3, rf=2, dc_name_prefix="dc")
        first_dc_nodes = self.config_and_create_cluster(topology=test_cluster_topology, cluster=secondary_cluster)
        mgr_cluster = self._create_mgr_cluster(node=first_dc_nodes[0], name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=first_dc_nodes[0], mgr_cluster=mgr_cluster)
        second_dc_nodes = []
        for i in range(3):
            new_node = self._add_new_node_and_wait_up_normal(healthy_node=first_dc_nodes[0], datacenter="dc2", cluster=secondary_cluster)
            second_dc_nodes.append(new_node)
            new_node.nodetool("repair")
            self.configure_agent(new_node)

        mgr_cluster.update_cluster_host(second_dc_nodes[0].address())

        for node in first_dc_nodes:
            node.nodetool("decommission")
            secondary_cluster.remove(node, other_nodes=second_dc_nodes)

        self.restore_and_verify(mgr_cluster, backup_task, second_dc_nodes[0])

    @pytest.mark.parametrize(argnames=("backed_up_cluster_size", "target_cluster_size"), argvalues=[(2, 3), (3, 2), (2, 4), (5, 3)])
    def test_restore_different_size_cluster(self, backed_up_cluster_size, target_cluster_size, secondary_cluster):
        backed_up_topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=backed_up_cluster_size, rf=2)
        self.config_and_create_cluster(topology=backed_up_topology_layout)
        mgr_cluster1 = self._create_mgr_cluster(node=self.cluster.nodelist()[0], name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(self.cluster.nodelist()[0], mgr_cluster1)

        target_topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=target_cluster_size, rf=2)
        second_cluster_nodes = self.config_and_create_cluster(topology=target_topology_layout, cluster=secondary_cluster)
        mgr_cluster2 = self._create_mgr_cluster(node=second_cluster_nodes[0], name=CLUSTER_NAME + "2")
        self.restore_schema(mgr_cluster=mgr_cluster2, backup_task=backup_task, cluster=secondary_cluster)
        self.restore_and_verify(mgr_cluster=mgr_cluster2, backup_task=backup_task, healthy_node=second_cluster_nodes[0])

    def test_restore_using_nonexistent_snapshot_tag(self):
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2, dc_name_prefix="dc")
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        try:
            mgr_cluster.run_restore_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], restore_data=True, snapshot_tag="sm_20190126161112UTC")
        except ScyllaManagerError as err:
            expected_error_string = "no snapshot with tag sm_20190126161112utc"
            assert expected_error_string in err.args[0].lower(), f"Create a restore task with a nonexistent snapshot tag failed, as expected, but with an improper error message: {err.args[0]}\n\nExpected: '{expected_error_string}'"
        else:
            raise ScyllaManagerError("No error occurred when creating a restore task with a nonexistent snapshot tag")

    def test_restore_only_specific_keyspace(self):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}, "ks_1": {"cf1": (44, 69), "cf2": (25, 35)}, "ks_a": {"cf1": (100, 123)}}
        backed_up_keyspaces = ["ks_1", "ks_a"]
        ks_not_backed_up = ["ks"]

        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster, keyspace_table_and_key_range=keyspace_table_and_key_range)
        restore_task = mgr_cluster.run_restore_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], restore_data=True, snapshot_tag=backup_task.get_snapshot_tag(), keyspace_list=["ks_*"])
        restore_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        self.verify_c1c2(node=node1, keyspace_table_and_key_range={ks: keyspace_table_and_key_range[ks] for ks in backed_up_keyspaces})
        self.verify_lack_of_keys(keyspace_table_and_key_range={ks: keyspace_table_and_key_range[ks] for ks in ks_not_backed_up}, node=node1)

    def test_restore_using_nonexistent_keyspace(self):
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster)
        try:
            mgr_cluster.run_restore_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], restore_data=True, snapshot_tag=backup_task.get_snapshot_tag(), keyspace_list=["ShlomoWasHere2023"])
        except ScyllaManagerError as err:
            expected_error_string = "no data in backup locations match given keyspace pattern"
            assert expected_error_string in err.args[0].lower(), f"Create a restore task with a nonexistent keyspace to restore failed, as expected, but with an improper error message: {err.args[0]}\n\nExpected: '{expected_error_string}'"
        else:
            raise ScyllaManagerError("No error occurred when creating a restore task with a nonexistent keyspace")

    def test_restore_using_different_batch_sizes(self):
        number_of_rows = "1500K"
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        self.cluster.stress(["write", f"n={number_of_rows}", "-rate", "threads=50", "-schema", "compaction(strategy=SizeTieredCompactionStrategy)"])
        backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=["keyspace1"])
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        batch_size_list = [None, 1, 3, 5]
        for batch_size in batch_size_list:
            self.clean_up_tables(node=node1, keyspace_and_tables_dict={"keyspace1": ["standard1"]})
            self.restore_and_verify_using_stress(mgr_cluster=mgr_cluster, backup_task=backup_task, healthy_node=node1, number_of_rows=number_of_rows, threads=50, batch_size=batch_size)

    def _get_tombstone_gc_mode(self, healthy_node, keyspace, table):
        session = self.patient_cql_connection(healthy_node)
        result = session.execute(f"select extensions from system_schema.tables where keyspace_name = '{keyspace}' and table_name = '{table}';")
        tombstone_gc_mode = "N\\A"
        if "tombstone_gc" in result.current_rows[0].extensions:
            tombstone_gc_raw_string = result.current_rows[0].extensions["tombstone_gc"].decode()
            tombstone_gc_mode = re.search(r"(repair|timeout|immediate|disabled)", tombstone_gc_raw_string)[0]
        return tombstone_gc_mode

    @pytest.mark.parametrize("initial_gc_mode", ["repair", "timeout", "immediate", "disabled"])
    def test_restore_check_tombstone_gc_value(self, initial_gc_mode):
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2, dc_name_prefix="dc")
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        self.cluster.stress(["write", "n=2500K", "-rate", "threads=50", "-schema", "replication(strategy=NetworkTopologyStrategy,dc1=2)", "compaction(strategy=SizeTieredCompactionStrategy)"])
        session = self.patient_cql_connection(node1)
        session.execute("ALTER TABLE keyspace1.standard1 WITH tombstone_gc = {'mode':'%s'}" % initial_gc_mode)
        backup_task = self._backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster, keyspace_table_and_key_range={"keyspace1": ["standard1"]})
        restore_task = mgr_cluster.run_restore_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], restore_data=True, snapshot_tag=backup_task.get_snapshot_tag())
        restore_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=35, step=1)  # Letting the restore start

        # Give Manager up to 10 extra seconds to change tombstone_gc mode
        start_time = time.time()
        while time.time() - start_time < 10:
            current_tombstone_gc_mode = self._get_tombstone_gc_mode(node1, "keyspace1", "standard1")
            if current_tombstone_gc_mode == "disabled":
                break
            time.sleep(1)
        else:
            raise TimeoutError("Timed out waiting for tombstone_gc mode to change to 'disabled'")

        restore_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        current_tombstone_gc_mode = self._get_tombstone_gc_mode(node1, "keyspace1", "standard1")
        assert current_tombstone_gc_mode == initial_gc_mode, (
            f"After the restore was completed, the value tombstone_gc mode of the restored table did not went back to original '{initial_gc_mode}' value, and instead remained at {current_tombstone_gc_mode}"
        )

    def test_restore_alter_batch_size(self):
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        self.cluster.stress(["write", "n=3500K", "-rate", "threads=50", "-schema", "compaction(strategy=SizeTieredCompactionStrategy)"])
        backup_task = self._backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster, keyspace_table_and_key_range={"keyspace1": ["standard1"]})
        restore_task = mgr_cluster.run_restore_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], restore_data=True, batch_size=3, snapshot_tag=backup_task.get_snapshot_tag())
        restore_task.wait_for_status(list_status=[TaskStatus.RUNNING], step=2)
        restore_task.stop()
        restore_task.update(batch_size=1)
        restore_task.start(continue_task=True)
        final_status = restore_task.wait_and_get_final_status(step=5)
        assert final_status == TaskStatus.DONE, f"Restore task failed after altering the batch size: {restore_task.progress_details()}"

    def test_delete_keyspace_while_restore_is_paused(self):
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        self.cluster.stress(["write", "n=3500K", "-rate", "threads=50", "-schema", "compaction(strategy=SizeTieredCompactionStrategy)"])
        backup_task = self._backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster, keyspace_table_and_key_range={"keyspace1": ["standard1"]})
        restore_task = mgr_cluster.run_restore_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], restore_data=True, snapshot_tag=backup_task.get_snapshot_tag())
        restore_task.wait_for_status(list_status=[TaskStatus.RUNNING], step=2)
        restore_task.stop()
        self._drop_table_and_delete_table_dir(keyspace_name="keyspace1", table_name="standard1", up_normal_node=node1)
        restore_task.start(continue_task=True)
        final_status = restore_task.wait_and_get_final_status(step=5)
        full_progress_string = restore_task.full_progress_string()
        assert final_status == TaskStatus.ERROR, f"Even though the restored keyspace was dropped while the restore task was paused, the task did not fail, but it instead reached the status of {final_status}: {full_progress_string}"
        error_messages = ["not found", "is missing in the restored cluster"]  # any of two since depends on Scylla version
        assert "keyspace1.standard1" in full_progress_string and any(msg in full_progress_string for msg in error_messages), (
            f"The expected message - one of {error_messages} - did not appear in the output of task progress: {full_progress_string}"
        )

    def test_restore_after_deleting_file_from_s3(self):
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        self.cluster.stress(["write", "n=3500K", "-rate", "threads=50", "-schema", "compaction(strategy=SizeTieredCompactionStrategy)"])

        keyspace, table = "keyspace1", "standard1"
        backup_task = self._backup_and_cleanup(healthy_node=node1, mgr_cluster=mgr_cluster, keyspace_table_and_key_range={keyspace: [table]})

        prefix = f"backup/sst/cluster/{mgr_cluster.id}/dc/datacenter1/node/{node1.hostid()}/keyspace/{keyspace}/table/{table}/"
        self._delete_file_from_bucket(prefix=prefix, suffix="Data.db")

        restore_task = mgr_cluster.run_restore_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], restore_data=True, snapshot_tag=backup_task.get_snapshot_tag())
        final_status = restore_task.wait_and_get_final_status(step=5)
        assert final_status == TaskStatus.ERROR, f"After deleting sstable component from the s3 snapshot directory, the restore task was expected to fail. However, it did not fail, and instead reached the status {final_status}"
        assert "not present in listed versioned files" in restore_task.full_progress_string(), f"The restore task has failed as expected, but printed unexpected error message:\n{restore_task.full_progress_string()}"

    def test_restore_data_after_purge(self):
        key_ranges = [(1, 21), (21, 101), (101, 251), (251, 388)]
        complete_key_range = (key_ranges[0][0], key_ranges[-1][1])
        keyspace_name = "ks"
        table_name = "cf1"

        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        self.insert_data_from_ranges(healthy_node=node1, keyspace_table_and_key_range={keyspace_name: {table_name: key_ranges[0]}})
        backup_task = mgr_cluster.run_backup_command(keyspace_list=[keyspace_name], location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], retention=2)
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        for key_range in key_ranges[1:]:
            self.insert_data_from_ranges(healthy_node=node1, keyspace_table_and_key_range={keyspace_name: {table_name: key_range}})
            backup_task.start(continue_task=False)
            backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        self.clean_up_tables(node=node1, keyspace_and_tables_dict={keyspace_name: [table_name]})
        restore_task = mgr_cluster.run_restore_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], restore_data=True, snapshot_tag=backup_task.get_snapshot_tag())
        final_status = restore_task.wait_and_get_final_status(step=5)
        assert final_status == TaskStatus.DONE
        self.verify_c1c2(keyspace_table_and_key_range={keyspace_name: {table_name: complete_key_range}}, node=node1)

    def test_restore_schema_with_mv(self, secondary_cluster):
        view_name = "view_specific_rows"

        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        self.insert_data_from_ranges(healthy_node=node1, keyspace_table_and_key_range=DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE)
        keyspace_name = next(iter(DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE.keys()))
        table_name = next(iter(DEFAULT_KEYSPACE_TABLE_AND_KEY_RANGE[keyspace_name].keys()))
        with self.patient_cql_cluster_session(node1) as session:
            session.execute(f"CREATE MATERIALIZED VIEW {keyspace_name}.{view_name} AS SELECT * FROM {keyspace_name}.{table_name} WHERE key = 'k1'PRIMARY KEY (key);")
        backup_task = mgr_cluster.run_backup_command(keyspace_list=[keyspace_name], location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], retention=2)
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)

        cluster2_node1, _ = self.config_and_create_cluster(topology=topology_layout, cluster=secondary_cluster)
        mgr_cluster2 = self._create_mgr_cluster(node=cluster2_node1, name=CLUSTER_NAME + "2")
        self.restore_schema(mgr_cluster=mgr_cluster2, backup_task=backup_task, cluster=secondary_cluster)
        with self.patient_cql_cluster_session(cluster2_node1) as session:
            result = session.execute(f"select * from system_schema.views where view_name='{view_name}'")
            assert len(list(result)) == 1, "After the schema restoration, the view was not created in the new cluster"
            self.restore_and_verify(mgr_cluster=mgr_cluster2, backup_task=backup_task, healthy_node=cluster2_node1)
            result = session.execute(f"select * from {keyspace_name}.{view_name}")
            assert len(list(result)) == 1, f"There was suppose to be only one row in the Mview after the restore, but instead there were {len(list(result))} lines"
        self.verify_c1c2(node=cluster2_node1, keyspace_table_and_key_range={keyspace_name: {view_name: (1, 2)}})

    @staticmethod
    def _corrupt_data(node, keyspace_name, table_name):
        table_glob_path = os.path.join(node.get_path(), "data", keyspace_name, f"{table_name}-*")
        table_path = glob(table_glob_path)[0]
        shutil.rmtree(path=table_path)

    def _template_post_restore_repair_only_restored_table_is_repaired(self, second_cluster, key_range):
        keyspace_name = next(iter(key_range.keys()))
        table_name = next(iter(key_range[keyspace_name].keys()))
        restore_key_range = {"ks": {"cf1": (1, 21)}}

        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2)
        node1, _ = self.config_and_create_cluster(topology=topology_layout)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = self.insert_data_backup_and_cleanup(node1, mgr_cluster, keyspace_table_and_key_range=restore_key_range)

        cluster2_node1, cluster2_node2 = self.config_and_create_cluster(topology=topology_layout, cluster=second_cluster)
        mgr_cluster2 = self._create_mgr_cluster(node=cluster2_node1, name=CLUSTER_NAME + "2")
        self.restore_schema(mgr_cluster=mgr_cluster2, backup_task=backup_task, cluster=second_cluster)

        self.insert_data_from_ranges(healthy_node=cluster2_node1, keyspace_table_and_key_range=key_range, rf=2)
        for node in second_cluster.nodelist():
            node.flush()
        self._corrupt_data(cluster2_node2, keyspace_name, table_name)
        self.restore_and_verify(mgr_cluster2, backup_task, cluster2_node1)
        return not get_list_of_sstables(cluster2_node2, keyspace_name, table_name)

    def test_post_restore_different_keyspace_not_repaired(self, secondary_cluster):
        """
        Since version 3.2, the manager starts a repair automatically after a data restore task.
        In this test, we:
        1. create one cluster and create a keyspace "ks" in it
        2. back the first cluster up
        3. create a second cluster and create a different keyspace in it, "ks_2", and intentionally create a fault in it
           on one of the nodes
        4. restore the backup from the first cluster into the second one (both schema and data)
        5. the test verifies that "ks_2" was NOT repaired
        """
        assert self._template_post_restore_repair_only_restored_table_is_repaired(secondary_cluster, key_range={"ks_2": {"cf_2": (1, 21)}}), "Restoring one keyspace caused a different keyspace to be repaired"

    def test_post_restore_different_table_not_repaired(self, secondary_cluster):
        """
        Since version 3.2, the manager starts a repair automatically after a data restore task.
        In this test, we:
        1. create one cluster and create a keyspace "ks" in it
        2. back the first cluster up
        3. create a second cluster
        4. restore the schema from the backup of the first cluster into the second one
        5. create a different table in "ks", "cf_2", and intentionally create a fault in it on one of the nodes
        4. restore the data from the backup from the first cluster into the second one
        5. the test verifies that "cf_2" was NOT repaired
        """
        assert self._template_post_restore_repair_only_restored_table_is_repaired(secondary_cluster, key_range={"ks": {"cf_2": (1, 21)}}), "Restoring the data of one table caused a different table in the same keyspace to be repaired"


@pytest.mark.dtest_full
@pytest.mark.scylla_manager
class TestRestoreWithEaR(EncryptionAtRestBase, TestScyllaMgmtRestoreBase):
    def config_and_create_cluster(self, nodes, extra_config_options=None, cluster=None, kss=None, restart=False):
        if cluster is not None:
            raise Exception("this class doesn't support multiple cluster test")
        if kss:
            extra_args = dict(kss=kss)
        else:
            extra_args = {}
        self.setup_object_storage()
        self.prepare(n=nodes, restart=restart, **extra_args)
        node_list = self.cluster.nodelist()
        for node in node_list:
            self.configure_agent(node)
        return node_list

    def test_basic_restore_with_kms_rotate_key(self):
        key_provider = KeyProviderEnum.kms
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        with self.get_key_provider(key_provider) as kp:
            node1, node2 = self.config_and_create_cluster(nodes=2, kss=["ks"], restart=kp.require_restart())
            session = self.get_session()
            self.create_encrypted_cf(session, name="ks.cf1", additional_options=kp.additional_cf_options())

            self.insert_data_from_ranges(healthy_node=node1, keyspace_table_and_key_range=keyspace_table_and_key_range, rf=2)

            mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

            logger.debug("Attempting to create a backup task with a location value, expecting it to success")
            backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=list(keyspace_table_and_key_range.keys()))
            backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)

            logger.debug("Rotate KMS key")
            kp.create_new_key_replace_alias()

            logger.debug("Restart cluster")
            self.rolling_restart()
            self.configure_agent(node1)
            self.configure_agent(node2)

            for node in self.cluster.nodelist():
                node.nodetool("upgradesstables -a")

            self.restore_and_verify(mgr_cluster, backup_task, node1)
