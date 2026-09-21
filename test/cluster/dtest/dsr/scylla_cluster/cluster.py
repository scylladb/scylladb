import collections
import json
import os.path
import random
import re
import time

import pytest
from cassandra import ConsistencyLevel

from dsr.base.dsr_entity import DSREntity
from dsr.loaders.base import LoaderBase
from dsr.scylla_cluster.states import ClusterState
from dtest_class import Tester


class ScyllaClusterTest(DSREntity):
    debug = False
    initial_node_count = None
    initial_seed_count = None
    min_node_count = 0
    max_node_count = None
    create_keyspace_stmt = None
    create_table_stmt = None
    validation_consistency = ConsistencyLevel.QUORUM  # Consistency level of the workload
    validation_serial_consistency = None
    loader: LoaderBase = None
    db_configuration: dict = None  # Configuration that is passed down to scylla
    actions = None  # pregenerated/generated actions
    action_variants = None  # list of actions to pick from
    action_count = None  # number of actions to generate
    sleep_time = 0  # Sleep time between running next action
    _expected_results: dict = None
    _loaders: dict = None
    _next_node_index = 0
    _keyspace_info = None

    def _generate_actions(self, current_actions, variants, length):
        for action in current_actions:
            action.chain = self
            action.reapply_cluster_state()
        for n in range(length - len(current_actions)):
            options = []
            for variant in variants:
                if isinstance(variant, type):
                    variant_instance = variant(chain=self)
                else:
                    variant_instance = variant.copy()
                    variant_instance.chain = self
                options.extend([variant_instance] * variant_instance.get_probability_coeff())
            if not options:
                break
            action_instance = random.choice(options)
            action_instance.randomize()
            current_actions.append(action_instance)
        return current_actions

    def _set_validation_serial_consistency(self):
        if self.validation_serial_consistency is not None:
            return
        if not self.loader:
            return
        self.validation_serial_consistency = self.loader.factual_serial_consistency()

    def _validate_data_in_cluster(self, tester):  # noqa: PLR0912, PLR0915
        ks_name = next(iter(self._keyspace_info.keys()))
        table_name = next(iter(self._keyspace_info[ks_name]["__tables__"].keys()))

        errors = collections.OrderedDict(
            {
                "dont_match": [],
                "dont_exists": [],
                "extra_rows": [],
                "cant_read": [],
                "cant_delete": [],
                "io_errors": [],
            }
        )
        tester.cluster.show(True)
        if self.debug:
            print(f"Correctness of {len(self._expected_results)} records is going to be verified")
        for cluster_node in tester.cluster.nodelist():
            if not cluster_node.is_running():
                cluster_node.start(wait_for_binary_proto=True, wait_other_notice=True)
            cluster_node.nodetool("rebuild")
        session_params = {"request_timeout": 600, "consistency_level": self.validation_consistency}
        if self.validation_serial_consistency is not None:
            session_params["serial_consistency_level"] = self.validation_serial_consistency
        with tester.patient_cql_connection(tester.cluster.nodelist()[0], **session_params) as session:
            select_stmt = session.prepare(f"SELECT * FROM {ks_name}.{table_name} WHERE k = ?")
            delete_stmt = session.prepare(f"DELETE FROM {ks_name}.{table_name} WHERE k = ?")
            for key, expected_value in sorted(self._expected_results.items(), key=lambda x: x[0]):
                to_be_deleted = True
                try:
                    results = list(session.execute(select_stmt.bind((key,))))
                    if not results:
                        errors["dont_exists"].append({"key": key, "expected_value": expected_value})
                    else:
                        _, db_value = results[0]
                        if expected_value != db_value:
                            errors["dont_match"].append({"key": key, "db_value": db_value, "expected_value": expected_value})
                except Exception as exc:  # noqa: BLE001
                    errors["cant_read"].append({"key": key, "expected_value": expected_value, "error": str(exc)})
                    to_be_deleted = False

                if to_be_deleted:
                    try:
                        session.execute(delete_stmt.bind((key,)))
                    except Exception as exc:  # noqa: BLE001
                        errors["cant_delete"].append({"key": key, "expected_value": expected_value, "error": str(exc)})

        with tester.patient_cql_connection(tester.cluster.nodelist()[0], request_timeout=600, consistency_level=self.validation_consistency) as session:
            extra_rows = []
            try:
                extra_rows = list(session.execute(f"SELECT * FROM {ks_name}.{table_name}"))
            except Exception as exc:  # noqa: BLE001
                errors["io_errors"].append(f'Error occurred while executing "SELECT * FROM {ks_name}.{table_name}": {exc!s}')
            for key, db_value in extra_rows:
                errors["extra_rows"].append(
                    {
                        "key": key,
                        "db_value": db_value,
                    }
                )
        error_output = ""
        for name, error_list in errors.items():
            if self.debug:
                if not os.path.exists("/tmp/errors"):
                    os.mkdir("/tmp/errors")
                with open(f"/tmp/errors/{name}.json", "w") as f:
                    f.write(json.dumps(error_list))
            if error_list:
                if name == "dont_match":
                    error_output += f"{len(error_list)} of {len(self._expected_results)} records does not match expected values\n"
                elif name == "dont_exists":
                    error_output += f"{len(error_list)} of {len(self._expected_results)} records does not exist in database\n"
                elif name == "extra_rows":
                    error_output += f"{len(error_list)} of {len(self._expected_results)} records exists in database that should not be there\n"
                elif name == "cant_read":
                    error_output += f"{len(error_list)} of {len(self._expected_results)} could not be read due to the error\n"
                elif name == "io_errors":
                    error_output += "\n".join(error_list)
        if error_output:
            pytest.fail(f"Following errors found:\n{error_output}")

    def _prepare_cluster(self, tester: Tester):
        if self.db_configuration:
            tester.cluster.set_configuration_options(values=self.db_configuration)
        if self.initial_seed_count:
            initial_seed_count = self.initial_seed_count
        else:
            initial_seed_count = self.initial_node_count
        initial_node_count = min(self.initial_node_count, initial_seed_count)
        tester.cluster.populate(self.initial_node_count).start(wait_for_binary_proto=True, wait_other_notice=True)
        for node_id in range(initial_seed_count, initial_node_count):
            new_node = tester.cluster.new_node(node_id + 1, auto_bootstrap=True, is_seed=False)
            new_node.start(wait_for_binary_proto=True, wait_other_notice=True)
        time.sleep(0.2)
        if self.debug:
            tester.cluster.show(True)
        with tester.patient_cql_connection(tester.cluster.nodelist()[0], request_timeout=600, consistency_level=ConsistencyLevel.ALL) as session:
            session.execute(self.create_keyspace_stmt)
            session.execute(self.create_table_stmt)

    def _parse_create_keyspace_stmt(self, create_keyspace_stmt: str, output=None):
        if output is None:
            output = {}
        try:
            groups = re.match(r"CREATE[ \t]+KEYSPACE[ \t]+(?P<ks_name>[^ ]+)[ \t]+" r"WITH[ \t]+replication[ \t]*=[ \t]*(?P<replication_stmt>{[^}]+})", create_keyspace_stmt).groupdict()
            ks_data = json.loads(groups["replication_stmt"].replace("'", '"'))
            ks_name = groups["ks_name"]
            ks_bucket = output.get(ks_name, None)
            if ks_bucket is None:
                output[ks_name] = ks_data
                return output
            output[ks_name].update(ks_data)
            return output
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Failed to parse keyspace statement (create_keyspace_stmt): {exc!s}")

    def _parse_create_table_stmt(self, create_table_stmt: str, output=None):
        if output is None:
            output = {}
        try:
            # TBD: Add support for table options and columns_definition
            groups = re.match(r"CREATE[ \t]+TABLE[ \t]+(?P<ks_name>[^ ]+)\.(?P<table_name>[^ ]+)[ \t]+" r"(IF[ \t]+NOT[ \t]+EXISTS[ \t]+){0,1}\((?P<column_definition>[^\)]+)\)", create_table_stmt).groupdict()
            ks_name = groups["ks_name"]
            table_name = groups["table_name"]
            ks_bucket = output.get(ks_name, None)
            if ks_bucket is None:
                output[ks_name] = {"__tables__": {table_name: {"__columns__": groups["column_definition"]}}}
                return output
            tables_bucket = ks_bucket.get("__tables__", None)
            if tables_bucket is None:
                ks_bucket["__tables__"] = {table_name: {"__columns__": groups["column_definition"]}}
                return output
            tables_bucket[table_name] = {"__columns__": groups["column_definition"]}
            return output
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Failed to parse table statement (create_table_stmt) : {exc!s}")

    def randomize(self):
        # TBD: Add some logic on generating these parameters
        self.check_validity()
        self._set_validation_serial_consistency()
        self._keyspace_info = self._parse_create_keyspace_stmt(self.create_keyspace_stmt)
        self._parse_create_table_stmt(self.create_table_stmt, self._keyspace_info)
        ks_name = next(iter(self._keyspace_info.keys()))
        self.state = ClusterState(
            initial_node_count=self.initial_node_count,
            rf=self._keyspace_info[ks_name]["replication_factor"],
            min_node_count=self.min_node_count,
            max_node_count=self.max_node_count,
            loaders_consistency_level=self.loader.consistency_level,
            db_configuration=self.db_configuration,
        )
        if self.actions is None:
            self.actions = []
        self.actions = self._generate_actions(self.actions, self.action_variants, self.action_count)

    def execute(self, tester):
        """
        Create cluster, run actions on it and validate data at the end
        """
        self._loaders = {}
        self._expected_results = {}
        self._next_node_index = self.initial_node_count + 1
        self._prepare_cluster(tester)
        for node in tester.cluster.nodelist():
            self.start_loader(node, tester)
        time.sleep(self.sleep_time)
        for action in self.actions:
            action.execute(tester)
            time.sleep(self.sleep_time)
        for _, loader in self._loaders.items():
            loader.stop()
            loader.merge_result(self._expected_results)
        for node in list(self._loaders.keys()):
            del self._loaders[node]
        self._validate_data_in_cluster(tester)

    def stop_loader(self, node):
        self._loaders[node].stop()

    def start_loader(self, node, tester):
        # self._loaders[node] = self.loaders_class(test, node, consistency_level=self.loaders_consistency_level)
        loader_instance = self.loader.copy()
        loader_instance.bind(tester, node)
        self._loaders[node] = loader_instance
        self._loaders[node].start()

    def get_nodes(self, tester):
        return sorted(tester.cluster.nodelist(), key=lambda node: int(node.name[4:]))

    def get_node_by_id(self, node_id, tester):
        for node in tester.cluster.nodelist():
            tmp_id = int(node.name[4:])
            if tmp_id == node_id:
                return node
        raise RuntimeError("There is no such node in the cluster")

    def check_validity(self):
        if self.create_keyspace_stmt is None:
            raise ValueError(f"{self.__class__.__name__}: create_keyspace_stmt is required")
        if self.create_table_stmt is None:
            raise ValueError(f"{self.__class__.__name__}: create_table_stmt is required")
        if self.loader is None:
            raise ValueError(f"{self.__class__.__name__}: loader is required")
        self.loader.check_validity()
        if self.actions:
            for action in self.actions:
                action.check_validity()

    def cleanup(self):
        for loader in self._loaders.values():
            loader.stop()
