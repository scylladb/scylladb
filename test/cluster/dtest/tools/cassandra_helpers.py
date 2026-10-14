#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import os
import re
import shutil
import tempfile

import pytest

from tools.files import copy_files_to, get_cf_dir

logger = logging.getLogger(__name__)


class CassandraCluster:
    """Class provides interface to create Cassandra cluster and migrate the data from Scylla"""

    def __init__(self, cassandra_version, request, test_instance):
        self.cassandra_version = cassandra_version
        self.request: pytest.FixtureRequest = request
        self.test_instance = test_instance
        self.test_path = tempfile.mkdtemp(prefix="dtest-cassandra-")
        # The sstables are bind-mounted into the containers, so the work
        # directory has to be reachable by the container's uid as well.
        os.chmod(self.test_path, 0o755)
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
        # Imported here, not at module scope: this module is imported by every
        # migration test, and only the handful that need a real Cassandra
        # should have to have the docker stack installed.
        from tools.cassandra_docker import CassandraDockerCluster

        # Set up Cassandra cluster
        self.cluster = CassandraDockerCluster(
            version=self.cassandra_version,
            workdir=self.test_path,
            datacenter=self.scylla_cluster.nodelist()[0].data_center if self.scylla_cluster else None,
        )
        self.request.addfinalizer(self.tear_down)
        config_options = dict(config_options or {})
        # remove experimental_features parameter that cassandra doesn't support
        config_options.pop("experimental_features", None)
        self.cluster.set_configuration_options(values=config_options)
        logger.debug(f"Starting a Cassandra cluster of {nodes} node(s) with options {config_options}...")
        self.cluster.populate(nodes)
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
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

    @staticmethod
    def get_table_folder(base_path, node, keyspace_name, table_name, create=False):
        """Return a node's directory for one table.

        `base_path` is the root to build it under; when it is None the node's
        own work directory is used, which is where both a Scylla node (through
        the cluster manager) and a Cassandra container keep their data.
        """
        keyspace_name = keyspace_name.replace('"', "")
        table_name = table_name.replace('"', "")
        node_dir = node.get_path() if base_path is None else os.path.join(base_path, node.name)
        ks_dir = os.path.join(node_dir, "data", keyspace_name)
        if create:
            the_folder = os.path.join(ks_dir, f"{table_name}-tmp")
            os.makedirs(the_folder)
        else:
            the_folder = get_cf_dir(ks_dir, table_name)
        return the_folder

    def copy_scylla_test_data_to_tmp(self, keyspace_names_list=None, table_names_list=None, nodes=None):
        self.scylla_cluster.flush()
        self.create_data_folders_tree(keyspace_names_list, table_names_list)
        self.scylla_data_tmp_folder = tempfile.mkdtemp(prefix="dtest-scylla-data-")
        logger.debug(f"Create {self.scylla_data_tmp_folder} test folder")
        self.copy_table_data_all_nodes(from_base_path=None, to_base_path=self.scylla_data_tmp_folder, create_to_folder=True, nodes=nodes)

    def copy_table_data_all_nodes(self, from_base_path, to_base_path, nodes=None, create_to_folder=False):
        logger.debug("Copy Scylla test data files")
        for node in nodes:
            for ks, tables in self.folders_tree.items():
                for table in tables:
                    copy_from = self.get_table_folder(base_path=from_base_path, node=node, keyspace_name=ks, table_name=table)
                    copy_to = self.get_table_folder(base_path=to_base_path, node=node, keyspace_name=ks, table_name=table, create=create_to_folder)
                    # get_cf_dir() returns None when the table has no directory
                    # yet, which used to turn the whole migration into a silent
                    # no-op and only showed up as an empty table at the far end.
                    assert copy_from, f"No directory for {ks}.{table} on {node.name} under {from_base_path or node.get_path()}"
                    assert copy_to, f"No directory for {ks}.{table} on {node.name} under {to_base_path or node.get_path()}"
                    files = sorted(os.listdir(copy_from))
                    logger.info(f"Copy {len(files)} data files for {ks}.{table} on {node.name}: from {copy_from} to {copy_to}: {files}")
                    copy_files_to(from_dir=copy_from, to_dir=copy_to, files_only=True)

    def copy_scylla_data_to_cassandra(self, nodes=None):
        self.copy_table_data_all_nodes(from_base_path=self.scylla_data_tmp_folder, to_base_path=None, nodes=nodes)

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

    def run_migration(self, scylla_cluster, keyspace_names_list=None, table_names=None, nodes="ALL"):
        self.scylla_cluster = scylla_cluster
        if not self.scylla_cluster:
            logger.debug("Missed Scylla cluster. Migration cant be run")
            return
        self.get_scylla_test_schema_ddl(keyspace_names_list=keyspace_names_list, table_names_list=table_names)

        # Node(s) for Scylla cluster
        nodes_list = list(self.scylla_cluster.nodes.values()) if nodes == "ALL" else [self.scylla_cluster.nodelist()[0]]

        self.copy_scylla_test_data_to_tmp(keyspace_names_list=keyspace_names_list, table_names_list=table_names, nodes=nodes_list)

        node1 = self.create_and_start_cluster(nodes=len(nodes_list), config_options={"hinted_handoff_enabled": False})

        # Node(s) for Cassandra cluster
        nodes_list = self.cluster.nodelist() if nodes == "ALL" else [node1]

        self.create_test_schema(node=nodes_list[0])
        self.copy_scylla_data_to_cassandra(nodes=nodes_list)
        self.migrate_data_to_cassandra(nodes=nodes_list)
        return node1

    def tear_down(self):
        """Stop the Cassandra cluster and clean up after it.

        The containers and their bind-mounted work directory always go, even
        when the test failed; the node logs are kept with the test's own logs
        first, so a failure can still be read afterwards.
        """
        logger.debug("Remove temporary folder with Scylla data")
        if self.scylla_data_tmp_folder and os.path.exists(self.scylla_data_tmp_folder):
            shutil.rmtree(self.scylla_data_tmp_folder, ignore_errors=True)

        if self.cluster is None:
            return
        try:
            self.save_logs()
        finally:
            self.cluster.remove()
            self.cluster = None

    def save_logs(self):
        """Copy each Cassandra node's system.log next to the test's own logs."""

        log_dir = os.environ.get("LOG_SAVED_DIR", "logs")
        test_name = re.sub(r"[^\w.-]", "_", self.request.node.name)
        dest = os.path.join(log_dir, f"cassandra-{test_name}")
        try:
            os.makedirs(dest, exist_ok=True)
            for node in self.cluster.nodelist():
                log = os.path.join(node.get_path(), "logs", "system.log")
                if os.path.exists(log):
                    shutil.copyfile(log, os.path.join(dest, f"{node.name}.log"))
        except OSError as e:
            logger.error("Error saving Cassandra logs: %s", e)


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
