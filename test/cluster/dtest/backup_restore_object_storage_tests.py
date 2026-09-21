"""
Test cases for ScyllaDB backup and restore operations using the API.

From manager-3.4, scylla manager uses Scylla API to do backups and restore,
so most use cases are covered by manager_backup_tests and manager_restore tests.
These tests serve as an extension to those, and mostly take care of the use case of
running backup/restore from nodetool.
"""

import logging
import os
import uuid
from collections.abc import Iterable

import pytest
import yaml
from ccmlib.node import NodetoolError
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.scylla_node import ScyllaNode

from dtest_class import Tester
from dtest_scylla_manager import ScyllaManagerMixin
from manager_backup_tests import DESTINATION_BUCKET, ManagerBackupMixin, fake_gcs_docker, minio_docker
from tools.cluster_topology import generate_cluster_topology
from tools.files import get_node_cf_dir, get_sstables_files

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestBackupRestoreObjectStorage(Tester, ManagerBackupMixin, ScyllaManagerMixin):
    @pytest.fixture(params=["s3", "gcs"], scope="function", autouse=True)
    def setup_backend(self, request):
        self.backend = request.param

    @pytest.fixture(scope="function", autouse=True)
    def setup_and_teardown(self):
        self.cluster: ScyllaCluster
        self.cluster_topology = generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)
        self.node1 = self.cluster.populate(self.cluster_topology).nodelist()[0]
        self.rf = 2
        # with default `task_ttl_in_seconds=0`, when backup/restore fail due to bad arguments
        # the error thrown would be `task not found` because failed tasks had been deleted
        # with a non-0 value, the task will still exist and it can be queried for the proper error
        self.cluster.set_configuration_options(values={"task_ttl_in_seconds": 60})
        # endpoints are setup differently based on the backend, see https://github.com/scylladb/scylladb/issues/26570
        if self.backend == "s3":
            self.endpoint = self.storage_endpoint_docker.address
        elif self.backend == "gcs":
            self.endpoint = self.storage_endpoint_docker.endpoint_url
        self.setup_object_storage()
        logger.debug(f"{self.cluster._config_options['object_storage_endpoints']=}")
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        self.prefix = f"backup/{uuid.uuid1()}"
        logger.debug(f"{self.prefix=}")
        self.bucket = DESTINATION_BUCKET

        self.ks = "ks"
        self.cf = "cf1"
        self.keyspace_table_and_key_range = {self.ks: {self.cf: (1, 21)}}
        self.snapshot = "snapshot"

        self.insert_data_from_ranges(healthy_node=self.node1, keyspace_table_and_key_range=self.keyspace_table_and_key_range, rf=self.rf)
        # NOTE: In the future, the backup command may do its own snapshot
        # See: https://github.com/scylladb/scylladb/blob/32f508d4502a565af38d5fa8fea38b9249653741/api/storage_service.cc#L1795-L1797
        # Test cases should check both manual and automatic snapshots then
        self.take_snapshot()

        yield
        # Teardown
        self.clean_bucket()

    def truncate_table(self):
        """
        Truncate the table and check that it is empty afterwards
        """
        self.clean_up_tables(node=self.node1, keyspace_and_tables_dict=self.keyspace_table_and_key_range)
        self.verify_lack_of_keys(keyspace_table_and_key_range=self.keyspace_table_and_key_range, node=self.node1)

    def take_snapshot(self, ks: str | None = None, cf: str | None = None, snapshot: str | None = None):
        ks = ks or self.ks
        cf = cf or self.cf
        snapshot = snapshot or self.snapshot
        out, err = self.node1.nodetool(f"snapshot -t {snapshot} -cf {cf} -- {ks}")
        logger.debug(f"\nOutput:\n{out}\nError:\n{err}")

    def backup_with_nodetool(self, endpoint: str | None = None, bucket: str | None = None, ks: str | None = None, cf: str | None = None, prefix: str | None = None, snapshot: str | None = None):  # noqa: PLR0913
        """
        Do a snapshot then a backup. Return the list of TOC (Table of Contents) components of the SSTables

        :param endpoint: the name of the endpoint as defined in `object_storage.yaml`
        :param bucket: the name of the minio bucket
        :param ks: the name of the keyspace to backup
        :param cf: the name of the table (column family) to backup
        :param prefix: the prefix under which the keyspace and table will be backed up
        :param snapshot: the name of the snapshot from which to take sstables
        """
        endpoint = endpoint or self.endpoint
        bucket = bucket or self.bucket
        ks = ks or self.ks
        cf = cf or self.cf
        snapshot = snapshot or self.snapshot
        prefix = prefix or self.prefix
        out, err = self.node1.nodetool(f"backup --endpoint {endpoint} --bucket {bucket} --prefix {prefix} --keyspace {ks} --table {cf} --snapshot {snapshot}")
        logger.debug(f"\nOutput:\n{out}\nError:\n{err}")
        return get_sstables_files(get_node_cf_dir(self.node1, ks, cf), f_type="TOC")

    def restore_with_nodetool(self, tocs: Iterable[str], endpoint: str | None = None, bucket: str | None = None, ks: str | None = None, cf: str | None = None, prefix: str | None = None):  # noqa: PLR0913
        """
        Restore sstables based on a list of TOC files

        :param tocs: a list of TOC files to restore
        :param endpoint: the name of the endpoint as defined in `object_storage.yaml`
        :param bucket: the name of the minio bucket
        :param ks: the name of the keyspace to restore
        :param cf: the name of the table (column family) to restore
        :param prefix: the prefix under which the keyspace and table were backed up
        """
        endpoint = endpoint or self.endpoint
        bucket = bucket or self.bucket
        ks = ks or self.ks
        cf = cf or self.cf
        prefix = prefix or self.prefix
        out, err = self.node1.nodetool(f"restore --endpoint {endpoint} --bucket {bucket} --keyspace {ks} --table {cf} --prefix {prefix} {' '.join(tocs)}")
        logger.debug(f"\nOutput:\n{out}\nError:\n{err}")

    def check_bucket_contents(self, bucket: str | None = None, prefix: str | None = None):
        """
        Verify that the contents of a `bucket` under a certain `prefix` are not empty
        """
        bucket = bucket or self.bucket
        prefix = prefix or self.prefix
        endpoint_objects = self.endpoint_list_objects(bucket, prefix)
        assert endpoint_objects, f"{endpoint_objects=}"

    def check_bucket_empty(self, bucket: str | None = None, prefix: str | None = None):
        """
        Verify that the contents of a `bucket` under a certain `prefix` are not empty
        """
        bucket = bucket or self.bucket
        prefix = prefix or self.prefix
        endpoint_objects = self.endpoint_list_objects(bucket, prefix)
        assert "" not in endpoint_objects, f"{endpoint_objects=}"

    def clean_bucket(self, bucket: str | None = None, prefix: str | None = None):
        """
        Delete all objects in a `bucket` tha have a certain `prefix`
        """
        bucket = bucket or self.bucket
        prefix = prefix or self.prefix
        for file_object in self.endpoint_list_objects(bucket, prefix):
            self.endpoint_delete_object(bucket, file_object)

    def test_backup_restore_with_nodetool(self):
        """
        Do a backup, truncate, then restore using nodetool
        Check that the data after restore is the same as before backup
        """
        # Do a backup and get the list of sstable TOCs.
        # We get it now because after cleanup there will be no sstables
        tocs = self.backup_with_nodetool()
        # check data something has been uploaded to the bucket
        self.check_bucket_contents()
        # truncate the table before restore
        self.truncate_table()
        self.restore_with_nodetool(tocs)
        # check that the data is the same as before the backup
        self.verify_c1c2(keyspace_table_and_key_range=self.keyspace_table_and_key_range, node=self.node1)

    def test_restore_into_different_table(self):
        tocs = self.backup_with_nodetool()
        self.check_bucket_contents()
        self.truncate_table()
        # create another table with less data
        keyspace_table_and_key_range = {"ks": {"cf2": (1, 10)}}
        self.insert_data_from_ranges(healthy_node=self.node1, keyspace_table_and_key_range=keyspace_table_and_key_range, rf=self.rf)
        self.restore_with_nodetool(tocs, cf="cf2")
        # check that the data is the same as before the backup
        keyspace_table_and_key_range["ks"]["cf2"] = self.keyspace_table_and_key_range["ks"]["cf1"]
        self.verify_c1c2(keyspace_table_and_key_range=keyspace_table_and_key_range, node=self.node1)

    def test_restore_after_emptying_bucket(self):
        tocs = self.backup_with_nodetool()
        self.check_bucket_contents()
        self.truncate_table()
        self.clean_bucket()
        nodetool_error_regex = {
            "s3": r"failed: storage_io_error[ :]+\(?S3 request failed. Code: 117. Reason:\s+HTTP code: 404 Not Found\)?.*",
            "gcs": r"failed: storage_io_error[ :]+\(?GCP object doesn't exist \(404 Not Found\)\)?.*",
        }
        with pytest.raises(NodetoolError, match=nodetool_error_regex[self.backend]):
            self.restore_with_nodetool(tocs)
        self.verify_lack_of_keys(keyspace_table_and_key_range=self.keyspace_table_and_key_range, node=self.node1)

    @pytest.mark.parametrize(
        ("argument", "error", "ignore"),
        [
            pytest.param(
                {"endpoint": "does_not_exist"}, r"(?s).*std::invalid_argument[ :]+\(?endpoint does_not_exist not found\)?.*", [r"sstables_manager - unable to find does_not_exist in configured object-storage endpoints"], id="endpoint"
            ),
            pytest.param(
                {"bucket": "does_not_exist"},
                r"(?s).*failed: storage_io_error[ :]+.*(S3|GCP) (object doesn't exist|request failed).*",
                [
                    r"snapshots - Error uploading .* storage_io_error[ :]+.*S3 (object doesn't exist|request failed).*",
                    r"snapshots - Error uploading .* storage_io_error[ :]+\(?GCP object doesn't exist \(404 Not Found\)\)?.*",
                    r"default_retry_strategy - S3 client encountered non-retryable error\. Reason: The specified bucket is not valid\.\. Code: 100\.",
                ],
                id="bucket",
            ),
            # Since scylladb/scylladb#30838 the backup API locates the snapshot
            # on disk without resolving the live schema (snapshots survive
            # DROP), so a nonexistent keyspace, table or snapshot all fail the
            # same way: the snapshot is not found on disk and the request is
            # rejected synchronously with "snapshot ... not found for table".
            # Accept both the old and the new errors until the fix (and its
            # backports) are merged, then drop the old alternatives.
            pytest.param(
                {"ks": "does_not_exist"},
                r"data_dictionary::no_such_column_family[ :]+\(?Can't find a column family cf1 in keyspace does_not_exist\)?|std::invalid_argument[ :]+\(?snapshot snapshot not found for table does_not_exist\.cf1\)?",
                [],
                id="keyspace",
            ),
            pytest.param(
                {"cf": "does_not_exist"},
                r"data_dictionary::no_such_column_family[ :]+\(?Can't find a column family does_not_exist in keyspace ks\)?|std::invalid_argument[ :]+\(?snapshot snapshot not found for table ks\.does_not_exist\)?",
                [],
                id="table",
            ),
            pytest.param(
                {"snapshot": "does_not_exist"},
                r"(?s)failed: std::invalid_argument[ :]+\(?snapshot does not exist at .*/does_not_exist\)?|std::invalid_argument[ :]+\(?snapshot does_not_exist not found for table ks\.cf1\)?",
                [],
                id="snapshot",
            ),
        ],
    )
    def test_backup_nonexistent_argument(self, argument, error, ignore):
        self.ignore_log_patterns.extend(ignore)
        with pytest.raises(NodetoolError, match=error):
            self.backup_with_nodetool(**argument)
        self.check_bucket_empty()

    @pytest.mark.parametrize(
        ("argument", "error", "ignore"),
        [
            pytest.param({"endpoint": "does_not_exist"}, r"(?s).*std::invalid_argument[ :]+\(?endpoint does_not_exist not found\)?.*", [], id="endpoint"),
            pytest.param(
                {"bucket": "does_not_exist"},
                r"(?s).*failed: storage_io_error[ :]+.*(S3|GCP) (object doesn't exist|request failed).*",
                [r"default_retry_strategy - S3 client encountered non-retryable error. Reason: Unknown server error has been encountered. HTTP code: 400 Bad Request\. Code: 100\."],
                id="bucket",
            ),
            pytest.param({"ks": "does_not_exist"}, r"(?s).*data_dictionary::no_such_column_family[ :]+\(?Can't find a column family cf1 in keyspace does_not_exist\)?.*", [], id="keyspace"),
            pytest.param({"cf": "does_not_exist"}, r"(?s).*data_dictionary::no_such_column_family[ :]+\(?Can't find a column family does_not_exist in keyspace ks\)?.*", [], id="table"),
            pytest.param(
                {"prefix": "does_not_exist"},
                r"(?s).*failed: storage_io_error[ :]+.*(S3|GCP) (object doesn't exist|request failed).*",
                [r"default_retry_strategy - S3 client encountered non-retryable error. Reason:\s+HTTP code: 404 Not Found\. Code: 117\."],
                id="prefix",
            ),
            pytest.param(
                {"tocs": ["me-does_not_exist1-TOC.txt", "me-does_not_exist2-TOC.txt"]}, r"(?s).*failed: sstables::malformed_sstable_exception[ :]+\(?invalid version for file .*. Name doesn't match any known version.\)?.*", [], id="tocs"
            ),
        ],
    )
    @pytest.mark.cluster_options(abort_on_malformed_sstable_error=False)
    def test_restore_nonexistent_argument(self, argument, error, ignore):
        tocs = self.backup_with_nodetool()
        self.check_bucket_contents()
        self.truncate_table()
        arguments = {"tocs": tocs}
        arguments.update(argument)
        self.ignore_log_patterns.extend(ignore)
        with pytest.raises(NodetoolError, match=error):
            self.restore_with_nodetool(**arguments)
        # check that the table was not populated
        self.verify_lack_of_keys(keyspace_table_and_key_range=self.keyspace_table_and_key_range, node=self.node1)
