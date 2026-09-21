import logging
import os
import pathlib
import re
import shutil
from functools import lru_cache

import pytest
from ccmlib.common import get_java_home_path

from dtest_config import DTestConfig
from dtest_setup import DTestSetup, copy_logs
from dtest_setup_overrides import DTestSetupOverrides
from tools.files import copy_files_to, get_cf_dir

logger = logging.getLogger(__name__)


class CassandraCluster:
    """Class provides interface to create Cassandra cluster and migrate the data from Scylla"""

    def __init__(self, cassandra_version, request, test_instance):
        self.cassandra_version = cassandra_version
        self.request: pytest.FixtureRequest = request
        self.dtest_config = DTestConfig()
        self.dtest_config.setup(self.request)
        self.dtest_config.cassandra_version = cassandra_version
        self.dtest_setup = DTestSetup(dtest_config=self.dtest_config, setup_overrides=DTestSetupOverrides(), cluster_name="test")
        self.test_instance = test_instance
        self.test_path = None
        self.cluster = None
        self.scylla_data_tmp_folder = None
        self.scylla_schema_ddl = None
        self.ddl_obj = None
        self.folders_tree = None
        self.scylla_cluster = None

        unsupported_schema_opts = ["tombstone_gc", "paxos_grace_seconds", "tablets"]
        self.unsupported_schema_opts_re = re.compile(rf"\s*(AND\s*)?({'|'.join(unsupported_schema_opts)})\s*=.*?(?P<term>[;\n])", flags=re.IGNORECASE)

        logger.debug("\n=============== Create Cassandra cluster ====================\n")

    def create_and_start_cluster(self, nodes=1, config_options=None):
        # Stop Scylla cluster before create new Cassandra cluster because of it's impossible to run two clusters simultaneously
        if self.scylla_cluster:
            self.scylla_cluster.stop(wait_other_notice=True)
        # Set up Cassandra cluster
        self.dtest_setup.initialize_cluster(DTestSetup.create_ccm_cluster)
        self.request.addfinalizer(self.tear_down)
        self.cluster = self.dtest_setup.cluster
        # remove experimental_features parameter that cassandra doesn't support
        self.cluster._config_options.pop("experimental_features", None)
        self.cluster.set_configuration_options(values=config_options)
        logger.debug(f"Starting a Cassandra cluster of {nodes} node(s) with options {config_options}...")
        self.cluster.populate(nodes)
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        self.test_path = self.dtest_setup.test_path
        return self.cluster.nodelist()[0]

    def get_scylla_test_schema_ddl(self, keyspace_names_list=None, table_names_list=None, get_system_keyspaces=None):
        self.ddl_obj = SchemaDDL(node=self.scylla_cluster.nodelist()[0], keyspace_names_list=keyspace_names_list, table_names_list=table_names_list, get_system_keyspaces=get_system_keyspaces)
        self.scylla_schema_ddl = self.ddl_obj.get_schemas_ddl()

    def create_entites_list(self, obj, func_for_empty):
        """
        :param obj: the object should be converted to list if not empty
        :type obj: any
        :param func_for_empty: if obj is empty, run this function to receive the value. Expected list, where
                            first element is function, and second is parameter for the function
        :type  func_for_empty: list
        :return:
        """
        out = obj
        if obj and not isinstance(obj, list):
            out = [obj]
        if not obj:
            if func_for_empty[1]:
                out = func_for_empty[0](func_for_empty[1])
            else:
                out = func_for_empty[0]()
        return out

    def create_data_folders_tree(self, keyspace_names_list=None, table_names_list=None):
        """
        create dictionary with keyspace(s) and their table(s) that its data will be migrated
        """
        self.folders_tree = {}
        keyspace_names_list = self.create_entites_list(obj=keyspace_names_list, func_for_empty=[self.ddl_obj.get_keyspaces, None])

        for keyspace_name in keyspace_names_list:
            self.folders_tree[keyspace_name] = []
            table_names_list = self.create_entites_list(obj=table_names_list, func_for_empty=[self.ddl_obj.get_entity_list, "select table_name from system_schema.tables where keyspace_name='{}'".format(keyspace_name.replace('"', ""))])
            for table_name in table_names_list:
                self.folders_tree[keyspace_name].append(table_name)

    def get_table_folder(self, base_path, node, keyspace_name, table_name, create=False):
        keyspace_name = keyspace_name.replace('"', "")
        table_name = table_name.replace('"', "")
        ks_dir = os.path.join(base_path, "test", node.name, "data", keyspace_name)
        if create:
            the_folder = os.path.join(ks_dir, f"{table_name}-tmp")
            os.makedirs(the_folder)
        else:
            the_folder = get_cf_dir(ks_dir, table_name)
        return the_folder

    def copy_scylla_test_data_to_tmp(self, scylla_test_path, keyspace_names_list=None, table_names_list=None, nodes=None):
        self.scylla_cluster.flush()
        self.create_data_folders_tree(keyspace_names_list, table_names_list)
        self.scylla_data_tmp_folder = os.path.join("/tmp", scylla_test_path.split("/")[-1])
        os.makedirs(self.scylla_data_tmp_folder)
        logger.debug(f"Create {self.scylla_data_tmp_folder} test folder")
        self.copy_table_data_all_nodes(from_base_path=scylla_test_path, to_base_path=self.scylla_data_tmp_folder, create_to_folder=True, nodes=nodes)

    def copy_table_data_all_nodes(self, from_base_path, to_base_path, nodes=None, create_to_folder=False):
        logger.debug("Copy Scylla test data files")
        for node in nodes:
            for ks, tables in self.folders_tree.items():
                for table in tables:
                    copy_from = self.get_table_folder(base_path=from_base_path, node=node, keyspace_name=ks, table_name=table)
                    copy_to = self.get_table_folder(base_path=to_base_path, node=node, keyspace_name=ks, table_name=table, create=create_to_folder)
                    logger.debug(f"Copy data files for {ks}.{table} table: from {copy_from} to {copy_to}")
                    copy_files_to(from_dir=copy_from, to_dir=copy_to, files_only=True)

    def copy_scylla_data_to_cassandra(self, nodes=None):
        self.copy_table_data_all_nodes(from_base_path=self.scylla_data_tmp_folder, to_base_path=self.test_path, nodes=nodes)

    def create_test_schema(self, node):
        for ks, cmds in self.scylla_schema_ddl.items():
            logger.debug("Create keyspace %s with all entities", ks)
            # filter scylla only parameters, that cassandra can't grok, for example:
            # "AND tombstone_gc = {'mode': 'timeout', 'propagation_delay_in_seconds': '3600'};"
            clean_cmds = [self.unsupported_schema_opts_re.sub(r"\g<term>", cmd) for cmd in cmds]

            with self.test_instance.patient_cql_connection(node) as session:
                for cmd in clean_cmds:
                    session.execute(cmd)

    def migrate_data_to_cassandra(self, nodes):
        for node in nodes:
            for ks, tables in self.folders_tree.items():
                for table in tables:
                    logger.debug(f"Start data migration from Scylla to Cassandra for {ks}.{table} table")
                    # If the keyspace/table names are case sensitive, we have to use double quotes. And nodetool refresh
                    # can't recognize it. So we need to remove double quotes to be able to run the refresh
                    node.nodetool("refresh -- {} {}".format(ks.replace('"', ""), table.replace('"', "")))

        for node in nodes:
            node.flush()

    def run_migration(self, scylla_cluster, scylla_test_path, keyspace_names_list=None, table_names=None, nodes="ALL"):
        self.scylla_cluster = scylla_cluster
        if not self.scylla_cluster:
            logger.debug("Missed Scylla cluster. Migration cant be run")
            return
        self.get_scylla_test_schema_ddl(keyspace_names_list=keyspace_names_list, table_names_list=table_names)

        # Node(s) for Scylla cluster
        nodes_list = self.scylla_cluster.nodes.values() if nodes == "ALL" else [self.scylla_cluster.nodes.values()[0]]

        self.copy_scylla_test_data_to_tmp(scylla_test_path=scylla_test_path, keyspace_names_list=keyspace_names_list, table_names_list=table_names, nodes=nodes_list)

        node1 = self.create_and_start_cluster(nodes=len(self.scylla_cluster.nodes.values()), config_options={"hinted_handoff_enabled": False})

        # Node(s) for Cassandra cluster
        nodes_list = self.cluster.nodelist() if nodes == "ALL" else [node1]

        self.create_test_schema(node=nodes_list[0])
        self.copy_scylla_data_to_cassandra(nodes=nodes_list)
        self.migrate_data_to_cassandra(nodes=nodes_list)
        return node1

    def tear_down(self):
        logger.debug("Remove temporary folder with Scylla data")
        if self.scylla_data_tmp_folder and os.path.exists(self.scylla_data_tmp_folder):
            shutil.rmtree(self.scylla_data_tmp_folder)

        dtest_setup = self.dtest_setup
        for con in dtest_setup.connections:
            con.cluster.shutdown()
        dtest_setup.connections = []

        rep_setup = getattr(self.request.node, "rep_setup", None)
        rep_call = getattr(self.request.node, "rep_call", None)
        failed = getattr(rep_setup, "failed", False) or getattr(rep_call, "failed", False)
        try:
            if not dtest_setup.allow_log_errors:
                try:
                    dtest_setup.check_errors_all_nodes()
                except AssertionError:
                    failed = True
                    raise
        finally:
            try:
                # save the logs for inspection
                if (failed and self.dtest_config.delete_logs == "passed") or self.dtest_config.delete_logs == "none":
                    copy_logs(self.request, dtest_setup)
            except Exception as e:  # noqa: BLE001
                logger.error("Error saving log: %s", str(e))
            finally:
                dtest_setup.cleanup_cluster()


class SchemaDDL:
    """Class provides interface to fetch schema DDL"""

    def __init__(self, node, keyspace_names_list="", table_names_list="", get_system_keyspaces=False):
        """
        :param node: node object to run the statements on
        :param keyspace_names_list: keyspace name to receive the DDL schema for
        :param table_name: table name or list of table names, for those (this) table/s the DDLs will be received.
                           In case DDl of all keyspace entites need to be created - remain it None
        :param get_system_keyspaces:
        """
        self.node = node
        self.get_system_keyspaces = get_system_keyspaces
        self.keyspace_names_list = keyspace_names_list
        self.table_names_list = table_names_list

    @property
    def keyspace_names_list(self):
        return self._keyspace_names_list

    @keyspace_names_list.setter
    def keyspace_names_list(self, value):
        self._keyspace_names_list = self.get_keyspaces() if not value else [self.wrap_case_sensitive_string(ks) for ks in value]

    def get_schemas_ddl(self):
        test_ddl = {}
        keyspace = ""
        for keyspace_name in self._keyspace_names_list:
            ks_ddl = self.get_ddl(entities=keyspace_name, _type="KEYSPACE", keyspace=keyspace)
            # The view of the secondary indexes shouldn't be created explicitly.
            # It'll be created automatically during secondary indexes creation
            test_ddl[keyspace_name] = self.remove_view_of_indexes(keyspace=keyspace_name, ks_ddl=ks_ddl)

            # If self.table_names_list is not None, just the tables in the list should be remain in the DDL
            if self.table_names_list:
                test_ddl[keyspace_name] = self.remain_expected_tables_only(keyspace=keyspace_name, ks_ddl=ks_ddl)
        return test_ddl

    def remain_expected_tables_only(self, keyspace, ks_ddl):
        if self.table_names_list:
            for table_name in self.table_names_list:
                for cmd in ks_ddl:
                    if f" {keyspace}.{table_name} " not in cmd and f"KEYSPACE {keyspace} " not in cmd:
                        ks_ddl.remove(cmd)
        return ks_ddl

    def remove_view_of_indexes(self, keyspace, ks_ddl):
        indexes = self.get_entity_list(cmd=f"select index_name from system_schema.indexes where keyspace_name='{keyspace}'")
        if indexes:
            for index in indexes:
                for cmd in ks_ddl:
                    if f"{keyspace}.{index}_index" in cmd:
                        ks_ddl.remove(cmd)
        return ks_ddl

    def get_ddl(self, entities, _type, keyspace=""):
        ddl = []
        keyspace = f"{keyspace}." if keyspace else keyspace
        entities = [entities] if not isinstance(entities, list) else entities
        for entity in entities:
            entity_ddl = self.node.run_cqlsh(cmds=f"DESC {_type} {keyspace}{entity}", return_output=True)
            splitted = [f"CREATE {e}" for e in entity_ddl[0].split("\nCREATE") if e]
            ddl.extend(splitted)
        return ddl

    def get_entity_list(self, cmd):
        out = self.node.run_cqlsh(cmds=cmd, return_output=True)
        if [i for i in out if "error" in i]:
            assert False, f'Failed to run command "{cmd}". Error: {out}'
        return [self.wrap_case_sensitive_string(entity.strip()) for entity in out[0].split("\n")[3:-3]]

    def get_keyspaces(self):
        if hasattr(self, "_keyspace_names_list") and self._keyspace_names_list:
            return self._keyspace_names_list
        out = self.node.run_cqlsh(cmds="select keyspace_name from system_schema.keyspaces", return_output=True)
        keyspaces = []
        for ks in out[0].split("\n")[3:-3]:
            if not self.get_system_keyspaces and "system" in ks:
                continue
            keyspaces.append(self.wrap_case_sensitive_string(ks.strip()))
        return keyspaces

    @staticmethod
    def wrap_case_sensitive_string(string):
        if '"' in string:
            return string

        for l in string:
            if l.isupper():
                return f'"{string}"'
        return string


@lru_cache(maxsize=2)
def java_version_exist(java_version: str | int) -> bool:
    """
    Checks if the specified Java version exists in the known JVM names and is available in the system.

    :param java_version: The Java version to check for existence. Supported versions are '8' and '11'.
    :type java_version: str
    :raises AssertionError: If the specified Java version is not supported.
    :return: True if the specified Java version is found in the system, False otherwise.
    :rtype: bool
    """
    java_version = str(java_version)
    known_jvm_names = {"8": ["1.8", "8"], "11": ["11"]}
    jvm_root_path = "/usr/lib/jvm/"
    assert java_version in known_jvm_names, f"java_version={java_version} not supported in:\n{known_jvm_names}"

    java_home_path = get_java_home_path(pathlib.Path(jvm_root_path), known_jvm_names[java_version])
    return java_home_path is not None
