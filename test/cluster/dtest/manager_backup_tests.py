import logging
import os
import random
import re
import shutil
import time
import uuid
from datetime import UTC, datetime, timedelta
from glob import glob
from pathlib import Path
from pprint import pformat
from time import sleep
from typing import Literal

import boto3
import pytest
import yaml
from cassandra import ConsistencyLevel
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode
from google.api_core.exceptions import Conflict
from google.api_core.page_iterator import HTTPIterator
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from mypy_boto3_s3 import S3Client

from dtest_class import Tester, WaitTimeoutExpiredError, create_cf, create_ks, wait_for
from dtest_scylla_manager import (
    C1_PREFIX,
    C2_PREFIX,
    ScyllaManagerError,
    ScyllaManagerMixin,
    ScyllaManagerTool,
    TaskStatus,
)
from encryption_at_rest_test import EncryptionAtRestBase, KeyProviderEnum, all_providers
from tools.cluster_topology import generate_cluster_topology, generate_cluster_topology_based_rf
from tools.docker_versions import get_docker_version
from tools.fake_gcs_server import FakeGCSDocker
from tools.files import get_sstables_files
from tools.minio import MinioDocker

CLUSTER_NAME = "cluster1"
DESTINATION_BUCKET = "backup-bucket"
FALSE_BUCKET = "nonexistent-bucket"

logger = logging.getLogger(__name__)


@pytest.fixture(scope="class")
def minio_docker():
    with MinioDocker(name=f"minio-{str(uuid.uuid4())[:8]}", image=get_docker_version("minio")) as minio:
        yield minio


@pytest.fixture(scope="class")
def fake_gcs_docker():
    with FakeGCSDocker(name=f"fake-gcs-{str(uuid.uuid4())[:8]}", image=get_docker_version("fake-gcs-server")) as fake_gcs:
        yield fake_gcs


class ManagerBackupMixin:
    backend: Literal["s3", "gcs"] = None
    method: Literal["native", "rclone"] = None

    @pytest.fixture(scope="class")
    def boto_client(self, minio_docker: MinioDocker):
        return boto3.client(service_name="s3", aws_access_key_id=minio_docker.access_key, aws_secret_access_key=minio_docker.secret_key, endpoint_url=minio_docker.endpoint_url)

    @pytest.fixture(scope="class")
    def google_client(self, fake_gcs_docker: FakeGCSDocker):
        return storage.Client(
            credentials=AnonymousCredentials(),
            project="test",
            client_options={"api_endpoint": f"http://{fake_gcs_docker.address}:{fake_gcs_docker.port}"},
        )

    @pytest.fixture(scope="function", autouse=True)
    def append_endpoint(self, boto_client: S3Client, google_client: storage.Client, minio_docker: MinioDocker, fake_gcs_docker: FakeGCSDocker, setup_backend):
        if self.backend == "s3":
            self.storage_endpoint_client = boto_client
            self.storage_endpoint_docker: MinioDocker = minio_docker
        elif self.backend == "gcs":
            self.storage_endpoint_client = google_client
            self.storage_endpoint_docker: FakeGCSDocker = fake_gcs_docker
        self.endpoint_create_bucket(DESTINATION_BUCKET)

    def endpoint_create_bucket(self, bucket: str):
        if self.backend == "s3":
            try:
                self.storage_endpoint_client.create_bucket(Bucket=bucket)
            except self.storage_endpoint_client.exceptions.BucketAlreadyOwnedByYou:
                pass
        elif self.backend == "gcs":
            try:
                self.storage_endpoint_client.create_bucket(bucket)
            except Conflict:
                pass

    def endpoint_list_objects(self, bucket: str, prefix: str) -> list[dict] | list[storage.Blob]:
        if self.backend == "s3":
            objects = self.storage_endpoint_client.list_objects(Bucket=bucket, Prefix=prefix)
            if "Contents" in objects:
                return objects["Contents"]
            return []
        else:
            return list(self.storage_endpoint_client.list_blobs(bucket, prefix=prefix))

    def endpoint_delete_object(self, bucket: str, file_object: storage.Blob | dict | str):
        if self.backend == "s3":
            if isinstance(file_object, dict):
                file_name = file_object["Key"]
            else:
                file_name = file_object

            self.storage_endpoint_client.delete_object(Bucket=bucket, Key=file_name)
        elif self.backend == "gcs":
            if isinstance(file_object, storage.Blob):
                file_name = file_object.name
            else:
                file_name = file_object
            self.storage_endpoint_client.bucket(bucket).blob(file_name).delete()

    def endpoint_download_file(self, bucket: str, source: str, destination: str):
        if self.backend == "s3":
            self.storage_endpoint_client.download_file(bucket, source, destination)
        elif self.backend == "gcs":
            self.storage_endpoint_client.bucket(bucket).blob(source).download_to_filename(destination)

    def endpoint_upload_file(self, bucket: str, source: str, destination: str):
        if self.backend == "s3":
            self.storage_endpoint_client.upload_file(source, bucket, destination)
        elif self.backend == "gcs":
            self.storage_endpoint_client.bucket(bucket).blob(destination).upload_from_filename(source)

    def _get_object_name(self, file_object) -> str:
        if self.backend == "s3":
            return file_object["Key"]
        return file_object.name

    def configure_agent(self, node: ScyllaNode):
        agent_config = {
            "s3": {"endpoint": self.storage_endpoint_docker.endpoint_url, "access_key_id": self.storage_endpoint_docker.access_key, "secret_access_key": self.storage_endpoint_docker.secret_key, "provider": "Minio"},
            "gcs": {"endpoint": self.storage_endpoint_docker.endpoint_url, "anonymous": "true"},
        }
        node.update_agent_config(new_settings={self.backend: agent_config[self.backend]}, restart_agent_after_change=True)

    def setup_object_storage(self, cluster: ScyllaCluster = None):
        cluster: ScyllaCluster = cluster or self.cluster
        # endpoints are setup differently based on the backend, see https://github.com/scylladb/scylladb/issues/26570
        endpoint_config = {
            "s3": {"name": self.storage_endpoint_docker.address, "port": int(self.storage_endpoint_docker.port), "aws_region": "local"},
            "gcs": {"name": self.storage_endpoint_docker.endpoint_url, "type": "gs", "credentials_file": "none"},
        }
        cluster.set_configuration_options(values={"object_storage_endpoints": [endpoint_config[self.backend]]})
        if self.backend == "s3":
            os.environ["SCYLLA_EXT_ENV"] = ";".join([os.getenv("SCYLLA_EXT_ENV", ""), f"AWS_ACCESS_KEY_ID={self.storage_endpoint_docker.access_key}", f"AWS_SECRET_ACCESS_KEY={self.storage_endpoint_docker.secret_key}"]).lstrip(";")

    def config_and_create_cluster(self, *args, **kwargs):
        self.setup_object_storage(cluster=kwargs.get("cluster"))
        node_list = super().config_and_create_cluster(*args, **kwargs)
        for node in node_list:
            self.configure_agent(node)
        return node_list

    def _drop_table_and_delete_table_dir(self, keyspace_name: str, table_name: str, up_normal_node: ScyllaNode):
        # Due to the fact that ccm does not delete the table's directory, to avoid confusion we'll delete it manually
        session = self.patient_cql_connection(node=up_normal_node)
        session.execute(f"drop table {keyspace_name}.{table_name};")
        for node in self.cluster.nodelist():
            keyspace_path = os.path.join(node.get_path(), "data", keyspace_name)
            table_path = glob(os.path.join(keyspace_path, table_name + "-*"))[0]
            shutil.rmtree(path=table_path)

    @staticmethod
    def _get_node_status(node_address: str, functioning_node: ScyllaNode, tolerate_missing: bool):
        """
        The function receives a node address and a ScyllaNode object of a node that is certifiably UN, and returns
        the the nodetool status of the node (address).
        """
        output_string, err = functioning_node.nodetool("status", capture_output=True)
        assert not err, "nodetool status execution failed"
        print(output_string)
        if node_address not in output_string:
            if tolerate_missing:
                logger.debug("node %s was not found in nodetool status, retrying", node_address)
                return "Nonexistent"
            assert False, f"Could not find requested node ({node_address}) in nodetool status"
        output_lines = output_string.split("\n")
        relevant_line = next(line for line in output_lines if node_address in line)
        return relevant_line[:2]

    def _is_node_at_status(self, node_address, functioning_node, desirable_status, tolerate_missing):
        return self._get_node_status(node_address, functioning_node, tolerate_missing) == desirable_status

    def _wait_until_node_reaches_status(  # noqa: PLR0913
        self,
        node: ScyllaNode,
        functioning_node: ScyllaNode,
        desirable_status: str,
        tolerate_missing: bool,
        timeout: int = 100,
        step: int = 1,
    ):
        """
        The function receives a ScyllaNode object, another node that is certain to be UN, and a nodetool status
        and waits until the node has reached the expected nodetool status.
        """
        text = f"Waiting until node {node.name} reaches status of: {desirable_status}"
        is_status_reached = wait_for(
            func=self._is_node_at_status, step=step, text=text, timeout=timeout, throw_exc=True, node_address=node.address(), functioning_node=functioning_node, desirable_status=desirable_status, tolerate_missing=tolerate_missing
        )
        return is_status_reached

    def compare_c1c2_rows_to_expected_results(self, row_list: list, key_range: tuple, table_name: str):
        """
        The method receives a row list, key range and a table name and confirms that the row list matches the
        expected row list (that is generated by the key range).
        """
        key_values = []
        c1_values = []
        c2_values = []
        for row in row_list:
            key_values.append(row.key)
            c1_values.append(row.c1)
            c2_values.append(row.c2)
        result_dict = {"key": sorted(key_values), "c1": sorted(c1_values), "c2": sorted(c2_values)}

        expected_result_dict = {"key": sorted([f"k{i}" for i in range(*key_range)]), "c1": sorted([C1_PREFIX % i for i in range(*key_range)]), "c2": sorted([C2_PREFIX % i for i in range(*key_range)])}
        for column, value in expected_result_dict.items():
            assert value == result_dict[column], f"""post backup table {table_name} does not match expected data:
                      mismatched_column:{column}
                      pre backup values:{value}
                      post backup values:{result_dict[column]}"""

    def verify_c1c2(self, keyspace_table_and_key_range: dict, node: ScyllaNode):
        """
        The method receives a dictionary of keyspaces that contain a dictionary of tables, and for each a key range.
        The method then confirms that all of the keys in the key range exist in each table
        """
        session = self.patient_cql_connection(node, consistency_level=ConsistencyLevel.QUORUM)
        for keyspace in keyspace_table_and_key_range:
            for table_name, key_range in keyspace_table_and_key_range.get(keyspace, {}).items():
                results = session.execute(f"select * from {keyspace}.{table_name}")
                self.compare_c1c2_rows_to_expected_results(results, key_range, table_name)

    def verify_lack_of_keys(self, keyspace_table_and_key_range: dict, node: ScyllaNode, key_name="key"):
        """
        The method receives a dictionary of keyspaces that contain a dictionary of tables, and for each a key range.
        The method then confirms that none of the keys in the key range does not exist in each table.
        """
        session = self.patient_cql_connection(node)
        for keyspace in keyspace_table_and_key_range:
            for table_name, key_range in keyspace_table_and_key_range.get(keyspace, {}).items():
                results = session.execute(f"select {key_name} from {keyspace}.{table_name}")
                existing_key_set = {str(getattr(row, key_name)).replace("k", "") for row in results}
                missing_key_set = set([str(n) for n in range(*key_range)])
                wrongfully_existing_keys = existing_key_set.intersection(missing_key_set)
                assert not wrongfully_existing_keys, f"The table {'.'.join([keyspace, table_name])} contains the keys {wrongfully_existing_keys} in column {key_name}, even though they are not suppose to exist in it"

    def _delete_file_from_bucket(self, prefix: str, suffix: str | None = None):
        file_objects = self.endpoint_list_objects(DESTINATION_BUCKET, prefix)
        if suffix:
            file_objects = [f for f in file_objects if self._get_object_name(f).endswith(suffix)]
        assert file_objects, f"No files found with prefix={prefix!r} and suffix={suffix!r}"
        random_file_object = random.choice(file_objects)
        logger.info(f"Removing file {random_file_object} from bucket {DESTINATION_BUCKET}")
        self.endpoint_delete_object(DESTINATION_BUCKET, random_file_object)

    def _prepare_cluster_with_data(self, keyspace_table_and_key_range, rf=2, topology=None):
        if topology is None:
            topology = generate_cluster_topology_based_rf(dc_num=1, nodes=rf, rf=rf)

        node_list = self.config_and_create_cluster(topology=topology)
        self.insert_data_from_ranges(healthy_node=node_list[0], keyspace_table_and_key_range=keyspace_table_and_key_range, rf=rf)
        return node_list

    def delete_range(  # noqa: PLR0913
        self,
        healthy_node,
        keyspace,
        table,
        key_range,
        clustering_key_name="ckey",
        partition_key_name="pkey",
        partition_key_set_value=1,
    ):
        """
        Only works on a table that contains a clustering key
        :param healthy_node: node in UN state
        :param key_range: range of the clustering key values, the rows of which will be deleted
        :param partition_key_set_value: the permanent value of the partition key
        """
        session = self.patient_cql_connection(healthy_node)
        query = f"DELETE from {keyspace}.{table} where {clustering_key_name} >= {key_range[0]} and {clustering_key_name} <= {key_range[1]} and {partition_key_name} = {partition_key_set_value}"
        session.execute(query)

    def download_files_from_endpoint(self, destination, file_list, bucket_name=DESTINATION_BUCKET):
        for file_path in file_list:
            path = file_path.replace(f"{self.backend}://{bucket_name}/", "")
            file_name = file_path[file_path.rfind("/") + 1 :]
            self.endpoint_download_file(bucket_name, path, os.path.join(destination, file_name))

    def restore_backup(self, node_list, mgr_cluster, snapshot_tag, keyspace_and_table_list):
        """
        At the moment, the function only supports the restoration of the LATEST backup of a specific backup task
        """
        per_node_backup_file_paths = mgr_cluster.get_backup_files_dict(snapshot_tag)
        for node in node_list:
            node_id = node.hostid()
            if node_id not in per_node_backup_file_paths:
                continue
            node_data_path = os.path.join(node.get_path(), "data")
            for keyspace, tables in keyspace_and_table_list.items():
                keyspace_path = os.path.join(node_data_path, keyspace)
                for table in tables:
                    table_upload_path = glob(os.path.join(keyspace_path, table + "-*", "upload"))[0]
                    self.download_files_from_endpoint(destination=table_upload_path, file_list=per_node_backup_file_paths[node_id][keyspace][table])
                    node.nodetool(f"refresh -- {keyspace} {table}")

    def restore_backup_from_backup_task(self, node_list, mgr_cluster, backup_task, keyspace_and_table_list):
        snapshot_tag = backup_task.get_snapshot_tag()
        self.restore_backup(node_list, mgr_cluster, snapshot_tag, keyspace_and_table_list)

    def clean_restore_and_verify_backup(
        self,
        backup_task,
        node_list,
        mgr_cluster,
        healthy_node,
        keyspace_table_and_key_range,
    ):
        per_keyspace_table_dict = {keyspace: list(table_dict.keys()) for keyspace, table_dict in keyspace_table_and_key_range.items()}
        self.clean_up_tables(healthy_node, per_keyspace_table_dict)
        self.restore_backup_from_backup_task(node_list, mgr_cluster, backup_task, per_keyspace_table_dict)
        self.verify_c1c2(keyspace_table_and_key_range, healthy_node)

    def clean_restore_and_verify_backup_with_stress(  # noqa: PLR0913
        self,
        backup_task,
        node_list,
        mgr_cluster,
        healthy_node,
        number_of_rows,
        per_keyspace_table_dict=None,
        threads=5,
    ):
        if not per_keyspace_table_dict:
            per_keyspace_table_dict = {"keyspace1": ["standard1"]}
        self.clean_up_tables(healthy_node, per_keyspace_table_dict)
        self.restore_backup_from_backup_task(node_list, mgr_cluster, backup_task, per_keyspace_table_dict)
        healthy_node.stress(["read", f"n={number_of_rows}", "-rate", f"threads={threads}"])


@pytest.mark.scylla_manager
class TestScyllaMgmtBackup(Tester, ManagerBackupMixin, ScyllaManagerMixin):
    @pytest.fixture(params=["native", "rclone"], scope="function", autouse=True)
    def setup_manager_method(self, request):
        self.method = request.param

    @pytest.fixture(params=["s3", "gcs"], scope="function", autouse=True)
    def setup_backend(self, request):
        self.backend = request.param

    def test_basic_backup(self):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        node1, _node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        logger.debug("Attempting to create a backup task with a location value, expecting it to success")
        backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=list(keyspace_table_and_key_range.keys()))
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), mgr_cluster, node1, keyspace_table_and_key_range)

    def test_backup_rate_limit_invalid(self):
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        logger.debug("Attempting to create a backup task with an invalid rate limit value, expecting it to fail")
        try:
            mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], rate_limit_list=["a"])
        except ScyllaManagerError as err:
            assert "invalid" in err.args[0] and "limit" in err.args[0], f"Unexpected error: {err.args[0]}"
        else:
            assert False, "No error occurred when an invalid rate-limit is used in the sctool backup command"

    def test_backup_cron_date(self):
        """
        The test starts a backup task with a certain start time using the cron flag,
        wait until the task has started and then makes sure that it
        indeed has started on the correct time.
        """
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        node1, _node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        command_execution_time = datetime.now()
        backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=list(keyspace_table_and_key_range.keys()), cron=create_cron_list_from_timedelta(minutes=2))
        next_run_delta = backup_task.next_run - datetime.now(UTC)
        assert next_run_delta < timedelta(seconds=120), "The next run time is as requested"

        backup_task.wait_for_status(list_status=[TaskStatus.RUNNING, TaskStatus.DONE], timeout=180, step=2)
        start_time_string = backup_task.start_time
        start_time = datetime.strptime(start_time_string, "%d %b %y %H:%M:%S %Z")
        assert abs((start_time - command_execution_time).seconds) < 121, "In practice, the start time of the backup task did not match the requested time"
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=100, step=5)
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), mgr_cluster, node1, keyspace_table_and_key_range)

    def test_backup_multiple_keyspaces_and_tables(self):
        keyspace_table_and_key_range = {"ks1": {"cf1": (1, 21), "cf2": (1, 21)}, "ks2": {"cf1": (1, 21), "cf2": (1, 21)}, "ks3": {"cf1": (1, 21), "cf2": (1, 21)}}
        node1, _node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        logger.debug("Attempting to create a backup task for a cluster with several keyspaces and column families, expecting it to succeed")
        backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=list(keyspace_table_and_key_range.keys()))
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), mgr_cluster, node1, keyspace_table_and_key_range)

    def test_backup_a_single_keyspace_and_glob_pattern(self):
        keyspace_table_and_key_range = {
            "ks1": {"cf1": (1, 21), "cf2": (1, 21)},
            "ks2": {"cf1": (1, 21), "cf2": (1, 21)},
            "ks3": {"cf1": (1, 21), "cf2": (1, 21)},
            "keyspace_for_glob": {"cf1": (1, 21), "cf2": (1, 21)},
            "other_keyspace": {"cf1": (1, 21), "cf2": (1, 21)},
        }
        node1, _node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        logger.debug("Attempting to create a backup task for a cluster with several keyspaces and column families, expecting it to succeed")
        backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=["ks1", "*_for_glob"])
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
        keyspaces_ranges_to_verify = {"ks1": keyspace_table_and_key_range["ks1"], "keyspace_for_glob": keyspace_table_and_key_range["keyspace_for_glob"]}
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), mgr_cluster, node1, keyspaces_ranges_to_verify)

    def test_backup_nonexistent_bucket(self):
        node1, _node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range={"ks": {"cf1": (1, 21)}})

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        logger.debug("Attempting to create a backup task with a nonexistent bucket in the location value, expecting it to fail")
        try:
            mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{FALSE_BUCKET}"])
        except ScyllaManagerError as err:
            assert "specified bucket does not exist" in err.args[0] or "location is not accessible" in err.args[0], f"Unexpected error: {err.args[0]}"
            # Backwards compatibility
            # TODO: Remove when branching 3.1
        else:
            raise ScyllaManagerError("No error occurred when a nonexistent bucket was used as a location in a manager backup command")

    def _backup_nonexistent_keyspace_template(self, keyspace_filter_string):
        node1, _node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range={"ks": {"cf1": (1, 21)}})

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        logger.debug("Attempting to create a backup task with a nonexistent keyspace in the keyspace value, expecting it to fail")
        try:
            mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=[keyspace_filter_string])
        except ScyllaManagerError as err:
            assert "no keyspace matched" in err.args[0], "The manager justifiably failed to backup a nonexistent keyspace, but the error message does not describe the error properly"
        else:
            raise ScyllaManagerError("No error occurred when a non existent keyspace was used in a keyspace flag in a manager backup command")

    def test_backup_nonexistent_keyspace(self):
        self._backup_nonexistent_keyspace_template("Nonexistent_keyspace")

    def test_backup_nonexistent_keyspace_glob(self):
        self._backup_nonexistent_keyspace_template("Nonexistent*")

    def _backup_nonexistent_datacenter_template(self, dc_filter_string):
        topology_layout = generate_cluster_topology(dc_num=2, rack_num=2, nodes_per_rack=1)
        nodes = self._prepare_cluster_with_data(keyspace_table_and_key_range={"ks": {"cf1": (1, 21)}}, topology=topology_layout)

        mgr_cluster = self._create_mgr_cluster(node=nodes[0], name=CLUSTER_NAME)

        logger.debug("Attempting to create a backup task with a nonexistent keyspace in the keyspace value, expecting it to fail")
        try:
            mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], dc_list=[dc_filter_string])
        except ScyllaManagerError as err:
            assert "no matching DCs" in err.args[0]
        else:
            raise ScyllaManagerError("No error occurred when a non existent database was used in a database flag in a manager backup command")

    def test_backup_nonexistent_datacenter(self):
        self._backup_nonexistent_datacenter_template("nonexistent")

    def test_backup_nonexistent_datacenter_glob(self):
        self._backup_nonexistent_datacenter_template("nonexistent*")

    def test_backup_task_progress(self):
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        self.cluster.stress(["write", "n=5000K", "-rate", "threads=50", "-schema", "compaction(strategy=SizeTieredCompactionStrategy)"])

        backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"])

        backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=100, step=1)
        progress_percentage = backup_task.progress
        assert progress_percentage != "N/A", "couldn't read the progress of the backup test"

        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
        progress_percentage = backup_task.progress
        assert progress_percentage == "100%", "The percentage of the backup task at its end was not 100%"
        self.clean_restore_and_verify_backup_with_stress(backup_task=backup_task, node_list=self.cluster.nodelist(), mgr_cluster=mgr_cluster, healthy_node=node1, number_of_rows="5000K", threads=50)

    @staticmethod
    def extract_all_snapshot_names(output, ignore_manager_snapshots=True):
        values_only_lines = output.split("\n")[2:-4]
        # Remove unnecessary lines from:

        # Snapshot Details:
        # Snapshot name                      Keyspace name  Column family name   True size Size on disk
        # 1646147496434-repair_unit          scylla_manager repair_unit          0 bytes   0 bytes
        # 1646147501653-repair_run_progress  scylla_manager repair_run_progress  0 bytes   0 bytes
        # 1646147498513-repair_run           scylla_manager repair_run           0 bytes   0 bytes
        # 1646147550539-repair_job_execution scylla_manager repair_job_execution 0 bytes   0 bytes
        # 1646147545534-repair_run_progress  scylla_manager repair_run_progress  0 bytes   0 bytes
        # 1646147491424-repair_config        scylla_manager repair_config        40 KB     40 KB
        # 1646147496416-scheduler_task       scylla_manager scheduler_task       0 bytes   0 bytes
        # 1646147542401-repair_run           scylla_manager repair_run           0 bytes   0 bytes
        # sm_manual_snapshot                 system         local                17.66 KB  17.66 KB
        # ...
        #
        # Total TrueDiskSpaceUsed: 34.38 K
        snapshot_names = []
        for line in values_only_lines:
            name, keyspace = line.split()[:2]
            if not ignore_manager_snapshots or not keyspace == "scylla_manager":
                snapshot_names.append(name)
        return snapshot_names

    def test_backup_nodetool_snapshots_before_backup(self):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        node_list = node1, node2

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        manual_snapshot_name = "sm_manual_snapshot"
        self.cluster.nodetool(f"snapshot -t {manual_snapshot_name}")
        per_node_pre_backup_snapshot_lists = {node.name: sorted(self.extract_all_snapshot_names(node.nodetool("listsnapshots", capture_output=True)[0])) for node in node_list}

        backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=list(keyspace_table_and_key_range.keys()))
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
        per_node_post_backup_snapshot_lists = {node.name: sorted(self.extract_all_snapshot_names(node.nodetool("listsnapshots", capture_output=True)[0])) for node in node_list}

        for node in node_list:
            assert per_node_post_backup_snapshot_lists == per_node_pre_backup_snapshot_lists, (
                f"The list of snapshots in {node.name} changed after the manager backup:\nPre backup snapshot list:{per_node_pre_backup_snapshot_lists[node.name]}\nPost backup_snapshot list:{per_node_post_backup_snapshot_lists[node.name]}"
            )
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), mgr_cluster, node1, keyspace_table_and_key_range)

    def test_shutting_down_node_during_backup(self, is_issue_open):
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=4, rf=4)
        node1, _node2, _node3, node4 = self.config_and_create_cluster(topology=topology_layout)
        self.cluster.stress(["write", "cl=ALL", "n=1000K", "-rate", "threads=50", "-schema", "replication(factor=4)"])

        manager_tool = ScyllaManagerTool(scylla_manager=self.cluster._scylla_manager)
        mgr_cluster = manager_tool.add_cluster(node=node1, name=CLUSTER_NAME)

        # Update scylla-manager configuration to limit the amount of retries to speed up backup task getting into
        # ERROR state. See details https://github.com/scylladb/scylladb/issues/22391#issuecomment-2612211525
        agent_client_config = {"agent_client": {"backoff": {"max_retries": 1}}}
        self.update_scylla_manager_yaml(append_data=agent_client_config)
        manager_tool.restart_manager_server(gently=True)

        backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=["keyspace1"])
        backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=600, step=1)

        if is_issue_open("scylladb/scylladb#24642"):
            self.ignore_log_patterns.extend([r"snapshots - Error uploading .*: storage_io_error[ :]+\(?S3 error \(seastar::abort_requested_exception[ :]+\(?abort requested\)?\)\)?"])
            self.ignore_log_patterns.extend([r"snapshots - Error uploading .*: std::runtime_error[ :]+\(?Failed to parse ETag list. Aborting multipart upload.\)?"])
        node4.stop(wait_other_notice=True)

        backup_task.wait_for_status(list_status=[TaskStatus.ERROR], step=5, timeout=200)

        node4.start(wait_other_notice=True, wait_for_binary_proto=True)
        self.configure_agent(node4)

        backup_task.start()
        backup_task.wait_and_get_final_status(step=5)
        assert backup_task.status != TaskStatus.ERROR, "After starting the nodes again, the task still failed"
        self.clean_restore_and_verify_backup_with_stress(backup_task=backup_task, node_list=self.cluster.nodelist(), mgr_cluster=mgr_cluster, healthy_node=node1, number_of_rows="1000K", threads=50)

    def test_restart_node_during_backup(self):
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=3, rf=3)
        node1, _node2, node3 = self.config_and_create_cluster(topology=topology_layout)
        self.cluster.stress(["write", "cl=ALL", "n=1000K", "-rate", "threads=50", "-schema", "replication(factor=3)"])

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=["keyspace1"])
        backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=100, step=1)
        node3.stop(wait_other_notice=True)
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        self.configure_agent(node3)
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=600, step=5)

        self.clean_restore_and_verify_backup_with_stress(backup_task=backup_task, node_list=self.cluster.nodelist(), mgr_cluster=mgr_cluster, healthy_node=node1, number_of_rows="1000K", threads=50)

    def test_shutting_down_node_before_backup(self):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=3, rf=3)
        node1, _node2, node3 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range, rf=3, topology=topology_layout)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        node3.stop(wait_other_notice=True)
        backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=["ks"])
        backup_task.wait_and_get_final_status(step=5)
        assert backup_task.status == TaskStatus.DONE, f"The backup task did not end in the given time, current progress:\n{backup_task.full_progress_string()}"
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), mgr_cluster, node1, keyspace_table_and_key_range)

    def test_backup_while_node_is_drained(self):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=3, rf=3)
        node1, _node2, node3 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range, topology=topology_layout, rf=3)

        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        node3.nodetool("drain")

        self._wait_until_node_reaches_status(node=node3, functioning_node=node1, desirable_status="DN", tolerate_missing=False)
        backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=["ks"])
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=600, step=5)
        node3.stop(wait_other_notice=False)
        node3.start(wait_other_notice=True, wait_for_binary_proto=True)
        self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), mgr_cluster, node1, keyspace_table_and_key_range)

    def test_backup_files_command_with_many_sstable_files(self):
        """
        Added a test that creates a large amount of sstable files by continuously executing
        Nodetool flush during c-s, and afterwards creates a backup task and restore the keyspace
        using the backup
        """
        self.cluster.set_configuration_options(values={"compaction_enforce_min_threshold": True})
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        session = self.patient_cql_connection(node1)
        create_ks(session=session, name="ks", rf=2)
        create_cf(
            session=session,
            name="ks.cf",
            read_repair=0.0,
            columns={"c1": "text", "c2": "text"},
            dclocal_read_repair_chance=0.1,
            speculative_retry="99.0PERCENTILE",
            compaction={"class": "SizeTieredCompactionStrategy", "min_threshold": 99999},
        )

        table_path = glob(os.path.join(node1.get_path(), "data", "ks", "cf-*"))[0]
        for fill_attempt in range(1, 400):
            session.execute(f"INSERT INTO ks.cf (key, c1, c2) VALUES ('k{fill_attempt}', '{C1_PREFIX % fill_attempt}', '{C2_PREFIX % fill_attempt}')")
            print(f"Flush No. {fill_attempt}")
            self.cluster.nodetool("flush")

            if len(get_sstables_files(cf_dir=table_path)) >= 2500:
                break
        else:
            assert False, "Failed to fill the cluster with enough files"

        backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=["ks"])
        backup_task.wait_and_get_final_status()
        assert backup_task.status == TaskStatus.DONE, "Backup task failed!"
        self.clean_restore_and_verify_backup(backup_task=backup_task, node_list=self.cluster.nodelist(), mgr_cluster=mgr_cluster, healthy_node=node1, keyspace_table_and_key_range={"ks": {"cf": (1, fill_attempt + 1)}})

    def insert_data_over_multiple_queries(
        self,
        healthy_node,
        keyspace_table_and_key_range,
        num_of_queries=10,
        use_clustering_key=False,
        partition_key_value=1,
    ):
        for keyspace in keyspace_table_and_key_range:
            for table, key_range in keyspace_table_and_key_range.get(keyspace, {}).items():
                split_key_ranges = list(range(key_range[0], key_range[1], (key_range[1] - key_range[0]) // num_of_queries))
                split_key_ranges.append(key_range[1])
                for n in range(len(split_key_ranges[:-1])):
                    self.insert_data_from_ranges(
                        healthy_node=healthy_node, keyspace_table_and_key_range={keyspace: {table: (split_key_ranges[n], split_key_ranges[n + 1])}}, use_clustering_key=use_clustering_key, partition_key_value=partition_key_value
                    )
                    healthy_node.nodetool("flush")

    def test_restore_after_purge(self):
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        snapshot_tags = []

        self.insert_data_over_multiple_queries(healthy_node=node1, keyspace_table_and_key_range={"ks": {"cf1": (1, 1001)}}, use_clustering_key=True)
        backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=["ks"], num_retries="0", retention="3")
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        snapshot_tags.append(backup_task.get_snapshot_tag())
        self.delete_range(node1, keyspace="ks", table="cf1", key_range=(1, 1000))

        for i in range(1, 4):
            self.insert_data_over_multiple_queries(healthy_node=node1, keyspace_table_and_key_range={"ks": {"cf1": (i * 1000 + 1, i * 1000 + 1001)}}, use_clustering_key=True)
            backup_task.start(continue_task=False)
            backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
            snapshot_tags.append(backup_task.get_snapshot_tag())

        self.clean_up_tables(node1, {"ks": ["cf1"]})
        self.restore_backup(node_list=self.cluster.nodelist(), mgr_cluster=mgr_cluster, snapshot_tag=snapshot_tags[1], keyspace_and_table_list={"ks": ["cf1"]})

        self.verify_lack_of_keys(keyspace_table_and_key_range={"ks": {"cf1": (1, 1001)}}, node=node1, key_name="ckey")

    def _get_total_snapshot_set(self):
        current_snapshot_set = set()
        for node in self.cluster.nodelist():
            current_snapshot_set.update(self.extract_all_snapshot_names(node.nodetool("listsnapshots", capture_output=True)[0]))

        return current_snapshot_set

    def test_delete_nonexisting_backup(self):
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        # Have to run a backup before the deletion, so that the manager will know about the s3/gcs location and the bucket
        backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"])
        backup_task.wait_and_get_final_status(step=5)
        print(backup_task.get_snapshot_tag())

        try:
            mgr_cluster.delete_backup(snapshot_tag="thisdoesnotexist")
        except ScyllaManagerError as err:
            if "not found" not in err.args[0]:
                logger.warning("When trying to delete a nonexistent snapshot, there was no proper error message")
                raise

    def test_delete_backup_twice(self):
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"])
        backup_task.wait_and_get_final_status(step=5)
        snapshot_tag = backup_task.get_snapshot_tag()

        mgr_cluster.delete_backup(snapshot_tag=snapshot_tag)
        try:
            mgr_cluster.delete_backup(snapshot_tag=snapshot_tag)
        except ScyllaManagerError as err:
            if "not found" not in err.args[0]:
                logger.warning("When trying to delete an already deleted snapshot, there was no proper error message")
                raise

    def test_delete_all_backups(self):
        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        # inserting data and creating a backup three times
        snapshot_tag_list = list()
        self.cluster.stress(["write", "n=50K", "-rate", "threads=50", "-pop", "seq=1..100000"])
        backup_task = mgr_cluster.run_backup_command(keyspace_list=["keyspace1"], location_list=[f"{self.backend}:{DESTINATION_BUCKET}"])
        backup_task.wait_and_get_final_status(step=5)
        snapshot_tag_list.append(backup_task.get_snapshot_tag())

        for i in range(1, 3):
            self.cluster.stress(["write", "n=50K", "-rate", "threads=50", "-pop", f"seq={100000 * i + 1}..{100000 * (i + 1)}"])
            backup_task.start(continue_task=False)
            backup_task.wait_and_get_final_status(step=5)
            snapshot_tag_list.append(backup_task.get_snapshot_tag())

        for tag in snapshot_tag_list:
            mgr_cluster.delete_backup(snapshot_tag=tag)

        # Trying to receive the backed up file list of each of the backup tasks, expecting an empty list
        for tag in snapshot_tag_list:
            backup_files = mgr_cluster.get_backup_files_dict(snapshot_tag=tag)
            assert not backup_files, "There are still backed up files left even after the tag was deleted"

    def _delete_run_and_restore_others_template(self, backup_run_to_delete):
        key_ranges = [(1, 11), (11, 21), (21, 31)]
        keyspace_name = "ks"
        table_name = "cf1"

        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

        snapshot_tag_list = list()
        self.insert_data_from_ranges(healthy_node=node1, keyspace_table_and_key_range={keyspace_name: {table_name: key_ranges[0]}})
        backup_task = mgr_cluster.run_backup_command(keyspace_list=[keyspace_name], location_list=[f"{self.backend}:{DESTINATION_BUCKET}"])
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        snapshot_tag_list.append(backup_task.get_snapshot_tag())

        for key_range in key_ranges[1:]:
            self.insert_data_from_ranges(healthy_node=node1, keyspace_table_and_key_range={keyspace_name: {table_name: key_range}})
            backup_task.start(continue_task=False)
            backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
            snapshot_tag_list.append(backup_task.get_snapshot_tag())

        # deleting the chosen backup and making sure there are no files oh it left afterwards
        mgr_cluster.delete_backup(snapshot_tag=snapshot_tag_list[backup_run_to_delete])
        backup_files_deleted_snapshot_files = mgr_cluster.get_backup_files_dict(snapshot_tag=snapshot_tag_list[backup_run_to_delete])
        assert not backup_files_deleted_snapshot_files, f"Even after deletion, there are still files of the snapshot {snapshot_tag_list[backup_run_to_delete]} in {self.backend}:\n{backup_files_deleted_snapshot_files}"
        session = self.patient_cql_connection(node=node1)
        for run_num in range(len(snapshot_tag_list)):
            if run_num == backup_run_to_delete:
                continue
            snapshot_tag = snapshot_tag_list[run_num]
            # Could not use clean_up_tables, since running truncate table twice causes scylla to crash
            self._drop_table_and_delete_table_dir(keyspace_name, table_name, node1)
            create_cf(session=session, name=f"{keyspace_name}.{table_name}", read_repair=0.0, columns={"c1": "text", "c2": "text"}, dclocal_read_repair_chance=0.0, speculative_retry="NONE")
            self.restore_backup(node_list=self.cluster.nodelist(), mgr_cluster=mgr_cluster, snapshot_tag=snapshot_tag, keyspace_and_table_list={keyspace_name: [table_name]})
            expected_key_range = [key_ranges[0][0], None]
            for r in range(run_num + 1):
                expected_key_range[1] = key_ranges[r][1]
            self.verify_c1c2(keyspace_table_and_key_range={keyspace_name: {table_name: expected_key_range}}, node=node1)

    def test_delete_first_run_and_restore_others(self):
        self._delete_run_and_restore_others_template(backup_run_to_delete=0)

    def test_delete_second_run_and_restore_others(self):
        self._delete_run_and_restore_others_template(backup_run_to_delete=1)

    def test_delete_third_run_and_restore_others(self):
        self._delete_run_and_restore_others_template(backup_run_to_delete=2)

    def test_compare_backup_list_size(self):
        """
        The test runs a backup and let it run until its completion,
        and afterwards checks that the size of the backup the manager reports on in the backup list command
        matches the actual size of the backup in s3/gcs
        """
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2)
        node1, _node2 = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        self.insert_data_from_ranges(healthy_node=node1, keyspace_table_and_key_range={"keyspace1": {"table1": [1, 10001]}})
        backup_task = mgr_cluster.run_backup_command(keyspace_list=["keyspace1"], location_list=[f"{self.backend}:{DESTINATION_BUCKET}"])
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        backup_size_under_test = self.get_backup_size_from_backup_list(mgr_cluster=mgr_cluster, snapshot_tag=backup_task.get_snapshot_tag())
        actual_backup_size = self.get_backup_size_in_practice(cluster_id=mgr_cluster.id)
        # backup_size_under_test reflects only data size, while actual_backup_size includes some auxiliary files
        # such as manifest.json, schema.json. We allow up to 2% overhead from such files.
        assert 0.98 <= backup_size_under_test / actual_backup_size < 1, (
            f"The size reported by the manager is not in the expected range (98%-100%) of the actual backup size in "
            f"{self.backend}:\n\tSize of the backup as reported by the manager: {backup_size_under_test} KiB\n\t"
            f"Size of the backup as seen in {self.backend}: {actual_backup_size} KiB"
        )

    def get_backup_size_in_practice(self, cluster_id):
        total_size_in_bytes = 0
        sst_files = self.endpoint_list_objects(DESTINATION_BUCKET, f"backup/sst/cluster/{cluster_id}/dc/datacenter1/node")

        def _get_object_size(obj):
            if self.backend == "s3":
                return obj["Size"]
            elif self.backend == "gcs":
                return obj.size

        total_size_in_bytes += sum([_get_object_size(obj) for obj in sst_files])
        complete_kib = total_size_in_bytes / 1024.0
        return complete_kib

    @staticmethod
    def get_backup_size_from_backup_list(mgr_cluster, snapshot_tag):
        backup_list_output = mgr_cluster.sctool.run(f" -c {mgr_cluster.id} backup list")[0]
        relevant_line = next(line[0] for line in backup_list_output if snapshot_tag in line[0])
        result = re.search(r"\(.+\)", relevant_line)[0][1:-1]  # Getting rid of parentheses
        if "K" in result:
            return float(result[: result.find("K")])
        if "M" in result:
            return float(result[: result.find("M")]) * 1024
        if "G" in result:
            return float(result[: result.find("G")]) * 1024**2
        raise ValueError("The size string does not contain any known file size unit")

    def test_disable_backup_task_before_run_before_executed(self):
        """
        Create a backup task that will run in the near future, and disable it.
        Expected: The task will not be executed
        """
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2)
        node1, *_ = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        keyspace_name = "keyspace1"
        self.insert_data_from_ranges(healthy_node=node1, keyspace_table_and_key_range={keyspace_name: {"cf1": (1, 10)}})
        location = f"{self.backend}:{DESTINATION_BUCKET}"
        cron_time_to_run = 1
        cron_start_time = create_cron_list_from_timedelta(minutes=cron_time_to_run)

        logger.info(f"Creating a backup task with following values:\nLocation: '{location}\nKeyspace: '{keyspace_name}\ncron time: '{cron_start_time}")
        backup_task = mgr_cluster.backup_api.backup(keyspace_list=keyspace_name, location_list=location, cron=cron_start_time, cluster_name=mgr_cluster.id, method=self.method)
        start_time = time.time()
        logger.info(f"Disabling the backup task '{backup_task.id}'")
        backup_task.enabled(is_enabled=False)
        logger.info(f"Verifying the backup task '{backup_task.id}' is disabled")
        backup_task.is_task_disabled()
        sleep_time = 60 * cron_time_to_run
        logger.info(f"Sleeping '{sleep_time}' seconds before verifying the status of back is '{TaskStatus.NEW}'")
        sleep(sleep_time)
        backup_task.wait_for_status(list_status=[TaskStatus.NEW], timeout=100, step=1)

    def test_update_backup_parameters(self):
        """
        Executing a backup task, waiting for it to end, and using sctool backup update to update the task` parameters
         (target table, keyspace, etc.) and rerunning the task afterwards, to completion.
        Expected:
         The task will be updated and its second run will be executed using the updated args rather than the ones it
          received in its creation.
        """
        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=2, rf=2)
        node1, *_ = self.config_and_create_cluster(topology=topology_layout)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        keyspace_name = "keyspace1"
        new_keyspace_name = f"new_{keyspace_name}"
        keyspace_table_and_key_range = {keyspace_name: {"cf1": (1, 21)}}
        new_keyspace_table_and_key_range = {new_keyspace_name: {"cf1": (1, 21)}}
        location = f"{self.backend}:{DESTINATION_BUCKET}"
        new_bucket_name = f"new{DESTINATION_BUCKET}"
        new_location = location.replace(DESTINATION_BUCKET, new_bucket_name)
        num_retries = 11
        rate_limit_list = 1
        retention = 12
        snapshot_parallel_list = "1,2,3"
        upload_parallel_list = "4,5,6"

        logger.info(f"Creating a new table with following values: '{pformat(keyspace_table_and_key_range)}")
        self.insert_data_from_ranges(healthy_node=node1, keyspace_table_and_key_range=keyspace_table_and_key_range)
        logger.info(f"Creating a new table with following values: '{pformat(new_keyspace_table_and_key_range)}")
        self.insert_data_from_ranges(healthy_node=node1, keyspace_table_and_key_range=new_keyspace_table_and_key_range)
        logger.info(f"Creating a new bucket: {new_bucket_name}")
        self.endpoint_create_bucket(new_bucket_name)
        logger.info(f"Creating a backup task with following values:\nLocation: '{location}\nKeyspace: '{keyspace_name}")
        backup_task = mgr_cluster.backup_api.backup(keyspace_list=keyspace_name, location_list=location, cluster_name=mgr_cluster.id, method=self.method)

        logger.info("Waiting until backup task is done")
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)

        logger.info(f"Changing the backup table name to '{new_keyspace_name}' from '{keyspace_name}'")
        backup_task.update(
            keyspace_list=new_keyspace_name, location_list=new_location, num_retries=num_retries, rate_limit_list=rate_limit_list, retention=retention, snapshot_parallel_list=snapshot_parallel_list, upload_parallel_list=upload_parallel_list
        )

        logger.info(f"Validating the 'keyspace', 'location', 'retention', 'rate_limit', 'retention', 'snapshot-parallel' and 'upload-parallel' fields are updated")
        properties = backup_task.properties
        err_msg = "The expected '{}' value should to be '{}' and not '{}'"
        assert properties["keyspace"] == new_keyspace_name, err_msg.format("keyspace", properties["keyspace"], new_keyspace_name)
        assert properties["location"] == new_location, err_msg.format("location", properties["location"], new_location)
        assert properties["retention"] == retention, err_msg.format("retention", properties["retention"], retention)
        assert properties["rate-limit"] == rate_limit_list, err_msg.format("rate_limit", properties["rate-limit"], rate_limit_list)
        assert properties["snapshot-parallel"] == list(map(int, snapshot_parallel_list.split(","))), err_msg.format("snapshot_parallel_list", properties["snapshot-parallel"], snapshot_parallel_list)
        assert properties["upload-parallel"] == list(map(int, upload_parallel_list.split(","))), err_msg.format("upload_parallel_list", properties["upload-parallel"], "upload_parallel_list")

    def _get_endpoint_files(  # noqa: PLR0913
        self,
        cluster_id,
        category="sst",
        datacenter=None,
        node_id=None,
        keyspace=None,
        table=None,
        suffix=None,
    ):
        prefix_string = f"backup/{category}/cluster/{cluster_id}{'/dc/' + datacenter if datacenter else ''}{'/node/' + node_id if node_id else ''}{'/keyspace/' + keyspace if keyspace else ''}{'/table/' + table if table else ''}"
        file_list = []
        file_objects = self.endpoint_list_objects(DESTINATION_BUCKET, prefix=prefix_string)
        if self.backend == "s3":
            try:
                file_list = [item["Key"] for item in file_objects]
                if suffix:
                    file_list = [item for item in file_list if item.endswith(suffix)]
            except KeyError as err:
                if err.args[0] == "Contents":
                    pass  # No snapshot files of the requested prefix exists
                else:
                    raise
        elif self.backend == "gcs":
            file_list = [item.name for item in file_objects]
            if suffix:
                file_list = [item for item in file_list if item.endswith(suffix)]
        return file_list

    def test_purge_removed_node_data(self):
        """
        The test creates a backup task with retention=1, and after the task has ended decommissions one of the nodes.
        Afterwards, the test restarts the backup task, and when the task completes its run the test
        makes sure that all snapshot files of the decommissioned node were removed (purged) from the bucket
        """
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        topology_layout = {"dc1": {"rack1": 1, "rack2": 2}}
        node1, _node2, node3 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range, topology=topology_layout, rf=2)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = mgr_cluster.run_backup_command(keyspace_list=["ks"], location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], retention=1)
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=10, timeout=300)
        hosts_status = mgr_cluster.get_hosts_health()
        decommissioned_node_id = node3.hostid()
        current_snapshots = self._get_endpoint_files(cluster_id=mgr_cluster.id, datacenter=hosts_status[node3.address()].datacenter["data_center"], node_id=decommissioned_node_id)
        assert current_snapshots, "Could not find backup files for destined node in bucket after backup task ended"

        node3.nodetool("decommission")
        backup_task.start(continue_task=False)
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=10, timeout=300)
        current_snapshots = self._get_endpoint_files(cluster_id=mgr_cluster.id, datacenter=hosts_status[node3.address()].datacenter["data_center"], node_id=decommissioned_node_id)
        assert not current_snapshots, f"After node {decommissioned_node_id} was decommissioned, and the backup task was rerun, snapshot files of the decommissioned node were not removed from the destination bucket"

    def _rename_files_with_older_timestamps(self, object_path_list):
        """
        The function takes the timestamps from the names of the manifest files it receives,
        changes them to be a year older than they originally were,
        and then replaces the timestamps in the manifests' names with the altered, older timestamps.

        For example:

        task_25929b31-c0b4-452a-a80f-85793aaa6d33_tag_sm_20210519075928UTC_manifest.json.gz
        Changes to
        task_25929b31-c0b4-452a-a80f-85793aaa6d33_tag_sm_20200519075928UTC_manifest.json.gz
        """
        datetime_format = "%Y%m%d%H%M%S"
        regex_pattern = "([0-9]+)UTC"
        temp_location = Path("/tmp/s3_objects/")
        temp_location.mkdir(exist_ok=True)

        for object_path in object_path_list:
            object_location, file_name = object_path.rsplit("/", maxsplit=1)
            self.endpoint_download_file(DESTINATION_BUCKET, object_path, str(temp_location / file_name))
            self.endpoint_delete_object(DESTINATION_BUCKET, object_path)
            file_date_string = re.findall(regex_pattern, file_name)[0]
            file_date_object = datetime.strptime(file_date_string, datetime_format)
            file_date_object = file_date_object.replace(year=file_date_object.year - 1)
            new_file_date_string = file_date_object.strftime(datetime_format)
            new_file_name = file_name.replace(file_date_string, new_file_date_string)
            self.endpoint_upload_file(DESTINATION_BUCKET, str(temp_location / file_name), "/".join([object_location, new_file_name]))

    def _purge_deleted_backup_task_template(self, use_purge_only):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        node1, node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = mgr_cluster.run_backup_command(keyspace_list=["ks"], location_list=[f"{self.backend}:{DESTINATION_BUCKET}"])
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=10, timeout=300)

        hosts_status = mgr_cluster.get_hosts_health()
        snapshot_file_paths_before_purge = set(self._get_endpoint_files(cluster_id=mgr_cluster.id, datacenter=hosts_status[node2.address()].datacenter["data_center"]))
        manifest_file_paths = self._get_endpoint_files(cluster_id=mgr_cluster.id, category="meta", datacenter=hosts_status[node2.address()].datacenter["data_center"], suffix=".gz")
        self._rename_files_with_older_timestamps(object_path_list=manifest_file_paths)
        backup_task.delete_task()

        for node in self.cluster.nodelist():
            # So new snapshot files will have different names,
            # and will not replace the existing files of the previous task
            node.nodetool("compact")
        if use_purge_only:
            new_backup_task = mgr_cluster.run_backup_command(purge_only=True, location_list=[f"{self.backend}:{DESTINATION_BUCKET}"])
        else:
            new_backup_task = mgr_cluster.run_backup_command(keyspace_list=["ks"], location_list=[f"{self.backend}:{DESTINATION_BUCKET}"])
        new_backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5, timeout=300)
        snapshot_file_paths_after_purge = set(self._get_endpoint_files(cluster_id=mgr_cluster.id, datacenter=hosts_status[node2.address()].datacenter["data_center"]))
        remaining_old_snapshot_files = snapshot_file_paths_before_purge.intersection(snapshot_file_paths_after_purge)
        assert not remaining_old_snapshot_files, "Snapshot files from a (old) deleted backup task were not purged"
        if use_purge_only:
            assert not snapshot_file_paths_after_purge, "The purge-only backup task has in fact ran a backup and uploaded snapshots to the destination bucket"

    def test_purge_deleted_backup_task(self):
        """
        At first, the test runs a backup task to completion and then deletes the task.

        Afterwards, the test alters the name of the deleted task's manifest to fool the manager to think
        that the task is over a month old, and therefore should be purged.
        (The manager will remove the files of a deleted task from the bucket only once it's over a month old)

        At last, the test starts another backup task, and upon its completion makes sure that the files of
        the deleted backup task were removed from the bucket.
        """
        self._purge_deleted_backup_task_template(use_purge_only=False)

    def test_purge_only_backup_task(self):
        """
        The test runs a normal backup task to completion.

        Afterwards the test deletes said backup task, and rename its manifest so that it appears to be a year old
        (and because of that, the manager will consider it an old enough backup to purge).

        At last, we start a backup task using the --purge-only param, and upon its completion
        the test makes sure that the snapshot files of the deleted task were purged from the bucket,
        and that the purge-only backup did not upload any file to the bucket, and therefore,
        did not run an actual backup.
        """
        self._purge_deleted_backup_task_template(use_purge_only=True)

    def _get_table_id(self, node, table_name):
        session = self.patient_cql_connection(node)
        result = session.execute(f"select id from system_schema.tables where table_name = '{table_name}';")
        table_id = str(result.current_rows[0].id).replace("-", "")
        return table_id

    def _upload_spam_file_to_bucket(self, cluster_id, node, keyspace_name, table_name, file_name="unrelated_file.txt"):
        table_id = self._get_table_id(node=node, table_name=table_name)
        object_location = f"backup/sst/cluster/{cluster_id}/dc/datacenter1/node/{node.hostid()}/keyspace/{keyspace_name}/table/{table_name}/{table_id}"
        open(f"/tmp/{file_name}", "w").close()
        self.endpoint_upload_file(DESTINATION_BUCKET, f"/tmp/{file_name}", "/".join([object_location, file_name]))

    def _does_spam_file_exist_in_bucket(
        self,
        cluster_id,
        node,
        keyspace_name,
        table_name,
        file_name="unrelated_file.txt",
    ):
        table_id = self._get_table_id(node=node, table_name=table_name)
        object_path = f"backup/sst/cluster/{cluster_id}/dc/datacenter1/node/{node.hostid()}/keyspace/{keyspace_name}/table/{table_name}/{table_id}/{file_name}"
        file_object = self.endpoint_list_objects(DESTINATION_BUCKET, object_path)
        return bool(file_object)

    def test_backup_validate_delete_orphaned_files(self):
        """
        The test runs a backup task until completion.

        Afterwards, the test manually inserts an unrelated file (that won't me mentioned in the manifest)
        to the bucket, that the manager will consider an orphan file (and therefore should be deleted).

        At the end, the test initiates a backup validate task, and upon its completion makes sure that the task
        reported on the orphan file and indeed deleted it from the bucket
        """
        keyspace_name, table_name = "ks", "cf1"
        keyspace_table_and_key_range = {keyspace_name: {table_name: (1, 21)}}
        node1, _node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = mgr_cluster.run_backup_command(keyspace_list=[keyspace_name], location_list=[f"{self.backend}:{DESTINATION_BUCKET}"])
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=10, timeout=300)

        self._upload_spam_file_to_bucket(cluster_id=mgr_cluster.id, node=node1, keyspace_name=keyspace_name, table_name=table_name)
        successful_backup_validate_task = mgr_cluster.run_backup_validate_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], delete_orphaned_files=True)
        file_status_dict = successful_backup_validate_task.get_file_status_summary(wait_for_task_ending=True)

        assert file_status_dict["Orphaned files"] == 1, f"The backup validate task was supposed to report about one orphan file, but instead reported on {file_status_dict['Orphaned files']} orphan files"
        assert file_status_dict["Deleted files"] == 1, f"The backup validate task was supposed to delete only one orphan file, but instead deleted {file_status_dict['Deleted files']} orphan files"
        assert not self._does_spam_file_exist_in_bucket(cluster_id=mgr_cluster.id, node=node1, keyspace_name=keyspace_name, table_name=table_name), (
            "Even though the backup validate task reported that it has deleted the orphan file, the file still exists in the bucket"
        )

    def test_backup_files_multiple_clusters(self, secondary_cluster, fixture_dtest_setup):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}

        topology_layout = generate_cluster_topology_based_rf(dc_num=1, nodes=3, rf=2)
        primary_cluster_nodes = self.config_and_create_cluster(topology=topology_layout)

        self.insert_data_from_ranges(healthy_node=primary_cluster_nodes[0], keyspace_table_and_key_range=keyspace_table_and_key_range, rf=2)
        primary_mgr_cluster = self._create_mgr_cluster(node=primary_cluster_nodes[0], name=CLUSTER_NAME)
        primary_backup_task = primary_mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=list(keyspace_table_and_key_range.keys()))
        primary_backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=10, timeout=300)

        secondary_cluster_nodes = self.config_and_create_cluster(topology=topology_layout, cluster=secondary_cluster)
        self.insert_data_from_ranges(healthy_node=secondary_cluster_nodes[0], keyspace_table_and_key_range=keyspace_table_and_key_range, rf=2)
        secondary_mgr_cluster = self._create_mgr_cluster(node=secondary_cluster_nodes[0], name=CLUSTER_NAME + "_second")
        secondary_backup_task = secondary_mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=list(keyspace_table_and_key_range.keys()))
        secondary_backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=10, timeout=300)

        primary_backup_task_snapshot_tag = primary_backup_task.get_snapshot_tag()
        snapshot_files = primary_mgr_cluster.get_backup_files_dict(snapshot_tag=primary_backup_task_snapshot_tag, all_clusters=True)

        misplaced_files = [snapshot for snapshot in snapshot_files if primary_mgr_cluster.id not in snapshot]
        assert misplaced_files, f"backup files command of the snapshot tag {primary_backup_task_snapshot_tag} contains unrelated files: {misplaced_files}"

    def test_agent_check_location(self, is_issue_open):
        if self.backend == "gcs" and is_issue_open("scylladb/scylla-manager#4626"):
            pytest.skip("With GCS backend, check location never finishes.")
        correct_config_file_path = os.path.join(self.cluster.get_path(), "node1/conf/scylla-manager-agent.yaml")
        wrong_config_file_location = os.path.join(self.cluster._scylla_manager._get_path(), "TEMP_CONFIG.yaml")
        wrong_config_dict = {
            "s3": {"s3": {"endpoint": "127.0.0.1:1", "provider": "Minio"}},
            "gcs": {"gcs": {"endpoint": "127.0.0.1:1", "anonymous": "true"}},
        }[self.backend]
        with open(wrong_config_file_location, "w") as temp_conf:
            yaml.dump(wrong_config_dict, temp_conf, default_flow_style=False)

        topology_layout = generate_cluster_topology(dc_num=1, rack_num=1, nodes_per_rack=3)
        self.config_and_create_cluster(topology=topology_layout)
        self._create_mgr_cluster(self.cluster.nodelist()[0], name="cluster1")
        # Running with the correct config file, expecting success.
        self.cluster._scylla_manager.agent_check_location(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], extra_config_file_list=[correct_config_file_path])
        # Running with the wrong config file, expecting failure.
        try:
            self.cluster._scylla_manager.agent_check_location(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], extra_config_file_list=[correct_config_file_path, wrong_config_file_location])
        except Exception as err:  # noqa: BLE001
            assert "connection refused" in err.args[-1], f"using an additional faulty {self.backend} config did cause the check-location command fail, but with an unexpected error message: {err.args!s}"
        else:
            raise Exception(f"using an additional faulty {self.backend} config did not cause the check-location command to fail")

    def test_backup_specific_keyspaces_includes_system_keyspaces(self):
        """
        The following test makes sure that even when "system_schema" is no included keyspace list of a backup task,
        it will still be backed up.
        Introduced in manager 2.4
        """
        node1, _node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range={"keyspace1": {"table1": [1, 11]}})
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = mgr_cluster.run_backup_command(keyspace_list=["keyspace1"], location_list=[f"{self.backend}:{DESTINATION_BUCKET}"])
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)
        snapshot_tag = backup_task.get_snapshot_tag()
        backup_files_dict = mgr_cluster.get_backup_files_dict(snapshot_tag=snapshot_tag)

        node_id = node1.hostid()
        backed_up_keyspaces_list = list(backup_files_dict[node_id].keys())

        assert "system_schema" in backed_up_keyspaces_list, "system_schema was not backed up as part of the task, even though this keyspace should always be backed up, even when it is not stated in the keyspace list"

    def test_backup_restore_with_agent_download_files(self):
        """
        The test creates a backup task, truncate the table that has been backed up,
        and eventually restores the backup using the agent's download-files command.
        """
        keyspace_name, table_name = "keyspace1", "table1"
        keyspace_table_and_key_range = {keyspace_name: {table_name: [1, 11]}}
        node1, _node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = mgr_cluster.run_backup_command(keyspace_list=[keyspace_name], location_list=[f"{self.backend}:{DESTINATION_BUCKET}"])
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)

        self.clean_up_tables(node=node1, keyspace_and_tables_dict={keyspace_name: [table_name]})
        for node in self.cluster.nodelist():
            self.cluster._scylla_manager.agent_download_files(node=node, location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], snapshot_tag=backup_task.get_snapshot_tag())
            node.nodetool(f"refresh -- {keyspace_name} {table_name}")
        self.verify_c1c2(keyspace_table_and_key_range=keyspace_table_and_key_range, node=node1)

    def test_execute_download_files_on_nonexistent_keyspaces(self):
        """
        The test creates a backup, runs it to completion, and afterwards attempts to execute the
        download-files agent command with a keyspace filter that has no matches, expecting failure.
        """
        keyspace_name, table_name = "keyspace1", "table1"
        keyspace_table_and_key_range = {keyspace_name: {table_name: [1, 11]}}
        node1, _node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = mgr_cluster.run_backup_command(keyspace_list=[keyspace_name], location_list=[f"{self.backend}:{DESTINATION_BUCKET}"])
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], step=5)

        self.clean_up_tables(node=node1, keyspace_and_tables_dict={keyspace_name: [table_name]})
        try:
            self.cluster._scylla_manager.agent_download_files(node=node1, location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], snapshot_tag=backup_task.get_snapshot_tag(), keyspace_filter_list=["NONEXISTENT_KEYSPACE"])
        except Exception as err:  # noqa: BLE001
            assert "no data matching filters" in err.args[-1], "The download-files justifiably failed to restore a nonexistent keyspace, but the error message does not describe the error properly"
        else:
            raise ScyllaManagerError("No error occurred when a non existent keyspace was used in the keyspace filter flag in a download-files command")

    def _get_table_set(self, node, keyspace_name):
        session = self.patient_cql_connection(node)
        result_rows = session.execute(f"select table_name from system_schema.tables where keyspace_name = '{keyspace_name}';")
        table_names = {f"{keyspace_name}.{row.table_name!s}" for row in result_rows}
        return table_names

    def _get_table_set_from_dry_run_output(self, node, location_list, snapshot_tag):
        output, _ = self.cluster._scylla_manager.agent_download_files(node=node, location_list=location_list, snapshot_tag=snapshot_tag, dry_run=True)
        table_name_set = set()
        for row in output.splitlines():
            if row.startswith("  - "):
                table_full_name = row[4 : row.find(" (")]
                table_name_set.add(table_full_name)
        return table_name_set

    def test_execute_download_files_dry_run(self):
        """
        The test runs a backup task until completion.
        Afterwards, using the backup task's snapshot tag, the test runs the download-files command
        with the --dry-run attribute, which means that the manager will not download the snapshot
        files to the upload directories of the backed up tables, but instead will print a list of
        the backed up tables.
        The test will verify that the agent listed the exact list of tables that were backed up
        and that no files were downloaded to the tables' upload directories.
        """
        keyspace_name, table_name = "keyspace1", "table1"
        keyspace_table_and_key_range = {keyspace_name: {table_name: [1, 11]}}
        node1, _node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = mgr_cluster.run_backup_command(keyspace_list=[keyspace_name], location_list=[f"{self.backend}:{DESTINATION_BUCKET}"])
        backup_task.wait_and_get_final_status(step=5)
        assert backup_task.status == TaskStatus.DONE, "Backup task failed due to an unexpected issue"

        backed_up_table_set = self._get_table_set(node=node1, keyspace_name=keyspace_name)
        # As of manager 2.4, system_schema keyspace is always backed up, even when not specified
        backed_up_table_set.update(self._get_table_set(node=node1, keyspace_name="system_schema"))
        output_table_set = self._get_table_set_from_dry_run_output(node=node1, location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], snapshot_tag=backup_task.get_snapshot_tag())

        basic_error_message = "The output of the agent's 'download-files --dry-run' command "
        assert not backed_up_table_set.difference(output_table_set), f"{basic_error_message} did not include the following table/s: {backed_up_table_set.difference(output_table_set)}"
        assert not output_table_set.difference(backed_up_table_set), f"{basic_error_message} did not include the following table/s: {output_table_set.difference(backed_up_table_set)}"

        for table_full_name in backed_up_table_set:
            keyspace, table = table_full_name.split(".")
            table_upload_directory = os.path.join(node1.get_path(), f"data/{keyspace}/{table}*/upload/*")
            downloaded_snapshot_files = glob(table_upload_directory, recursive=True)
            assert not downloaded_snapshot_files, f"Even though the download-files command uses the dry-run argument, the snapshot files were still downloaded to the upload directory of {table_full_name}"

    def test_validate_backup_after_deleting_file(self):
        """
        The test creates a backup, runs it to completion,
        and afterwards deletes one of the files of the backup from s3/gcs (minio/fake-gcs-server).
        Afterwards, we create a backup validate task, expecting it to report about the missing file
        and for the task to fail.
        """
        keyspace_name, table_name = "keyspace1", "table1"
        keyspace_table_and_key_range = {keyspace_name: {table_name: [1, 11]}}
        node1, _node2 = self._prepare_cluster_with_data(keyspace_table_and_key_range=keyspace_table_and_key_range)
        mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)
        backup_task = mgr_cluster.run_backup_command(keyspace_list=[keyspace_name], location_list=[f"{self.backend}:{DESTINATION_BUCKET}"])
        backup_task.wait_and_get_final_status(step=5)
        assert backup_task.status == TaskStatus.DONE, "Backup task failed due to an unexpected issue"

        successful_backup_validate_task = mgr_cluster.run_backup_validate_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"])
        file_status_dict = successful_backup_validate_task.get_file_status_summary(wait_for_task_ending=True)
        assert file_status_dict["Missing files"] == 0, f"The backup validate task reported on an incorrect number of missing files: it reported on {file_status_dict['Missing files']} files instead of 0"
        assert successful_backup_validate_task.status == TaskStatus.DONE, "Since there are no missing files, the task was supposed to end in success, but it did not"

        self._delete_file_from_bucket(prefix=f"backup/sst/cluster/{mgr_cluster.id}/")

        failing_backup_validate_task = mgr_cluster.run_backup_validate_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"])
        file_status_dict = failing_backup_validate_task.get_file_status_summary(wait_for_task_ending=True)
        assert file_status_dict["Missing files"] == 1, f"The validate task did not report on the correct number of missing files: reported {file_status_dict['Missing files']} files instead of 1"
        assert failing_backup_validate_task.status == TaskStatus.ERROR, "Since there are missing files, the task was supposed to end in failure, but it did not"


def create_cron_list_from_timedelta(minutes=0, hours=0):
    destined_time = datetime.now() + timedelta(hours=hours, minutes=minutes)
    cron_list = [str(destined_time.minute), str(destined_time.hour), "*", "*", "*"]
    return cron_list


@pytest.mark.dtest_full
@pytest.mark.scylla_manager
class TestBackupWithEaR(EncryptionAtRestBase, ManagerBackupMixin, ScyllaManagerMixin):
    @pytest.fixture(params=["native", "rclone"], scope="function", autouse=True)
    def setup_manager_method(self, request):
        self.method = request.param

    @pytest.fixture(params=["s3", "gcs"], scope="function", autouse=True)
    def setup_backend(self, request):
        self.backend = request.param

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

    @pytest.mark.parametrize(argnames="key_provider", argvalues=all_providers(), ids=lambda x: x.name)
    def test_basic_backup_with_encryption_at_rest(self, key_provider):
        keyspace_table_and_key_range = {"ks": {"cf1": (1, 21)}}
        with self.get_key_provider(key_provider) as kp:
            node1, _node2 = self.config_and_create_cluster(nodes=2, kss=["ks"], restart=kp.require_restart())
            session = self.get_session()
            self.create_encrypted_cf(session, name="ks.cf1", additional_options=kp.additional_cf_options())

            self.insert_data_from_ranges(healthy_node=node1, keyspace_table_and_key_range=keyspace_table_and_key_range, rf=2)

            mgr_cluster = self._create_mgr_cluster(node=node1, name=CLUSTER_NAME)

            logger.debug("Attempting to create a backup task with a location value, expecting it to success")
            backup_task = mgr_cluster.run_backup_command(location_list=[f"{self.backend}:{DESTINATION_BUCKET}"], keyspace_list=list(keyspace_table_and_key_range.keys()))
            backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1000, step=5)
            self.clean_restore_and_verify_backup(backup_task, self.cluster.nodelist(), mgr_cluster, node1, keyspace_table_and_key_range)
