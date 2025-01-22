#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import logging
import os
import pprint
import re
from functools import partial, partialmethod
from typing import TYPE_CHECKING

import requests
from cassandra.cluster import EXEC_PROFILE_DEFAULT, NoHostAvailable, default_lbp_factory
from cassandra.cluster import Cluster as PyCluster
from cassandra.policies import ExponentialReconnectionPolicy, WhiteListRoundRobinPolicy

from test.cluster.dtest.dtest_class import (
    get_auth_provider,
    get_ip_from_node,
    get_port_from_node,
    make_execution_profile,
)
from test.cluster.dtest.ccmlib.scylla_cluster import ScyllaCluster
from test.cluster.dtest.tools.context import log_filter
from test.cluster.dtest.tools.log_utils import DisableLogger, remove_control_chars
from test.cluster.dtest.tools.misc import retry_till_success

if TYPE_CHECKING:
    from typing import Any

    from test.cluster.dtest.ccmlib.scylla_node import ScyllaNode
    from test.cluster.dtest.dtest_config import DTestConfig
    from test.cluster.dtest.dtest_setup_overrides import DTestSetupOverrides
    from test.pylib.manager_client import ManagerClient


DEFAULT_PROTOCOL_VERSION = 4

logger = logging.getLogger(__name__)

# Add custom TRACE level, for development print we don't want on debug level
logging.TRACE = 5
logging.addLevelName(logging.TRACE, "TRACE")
logging.Logger.trace = partialmethod(logging.Logger.log, logging.TRACE)
logging.trace = partial(logging.log, logging.TRACE)


class DTestSetup:
    def __init__(self,
                 dtest_config: DTestConfig | None = None,
                 setup_overrides: DTestSetupOverrides | None = None,
                 manager: ManagerClient | None = None,
                 scylla_mode: str | None = None,
                 cluster_name: str = "test"):
        self.dtest_config = dtest_config
        self.setup_overrides = setup_overrides
        self.cluster_name = cluster_name
        self.ignore_log_patterns = []
        self.ignore_cores_log_patterns = []
        self.ignore_cores = []
        self.cluster = ScyllaCluster(manager=manager, scylla_mode=scylla_mode)
        self.cluster_options: dict[str, Any] = {}
        self.replacement_node = None
        self.allow_log_errors = False
        self.connections = []
        self.jvm_args = []
        self.base_cql_timeout = 10  # seconds
        self.cql_request_timeout = None
        self.scylla_features: set[str] = self.dtest_config.scylla_features

    def find_cores(self):
        cores = []
        ignored_cores = []
        nodes = []
        for node in self.cluster.nodelist():
            try:
                pids = node.all_pids
                if not pids:
                    pids = [node.pid]
            except AttributeError:
                pids = [node.pid]
            nodes += [(node, pids)]
        for f in os.listdir("."):
            if not f.endswith(".core"):
                continue
            for node, pids in nodes:
                """Look for this cluster's coredumps"""
                for p in pids:
                    if f.find(f".{p}.") >= 0:
                        path = os.path.join(os.getcwd(), f)
                        if not node in self.ignore_cores:
                            cores += [(node.name, path)]
                        else:
                            logger.debug(f"Ignoring core file {path} belonging to {node.name} due to ignore_cores_log_patterns")
                            ignored_cores += [(node.name, path)]
        # returns empty list if no core files found
        return cores, ignored_cores

    def cql_connection(  # noqa: PLR0913
        self,
        node,
        keyspace=None,
        user=None,
        password=None,
        compression=True,
        protocol_version=None,
        port=None,
        ssl_opts=None,
        **kwargs,
    ):
        return self._create_session(node, keyspace, user, password, compression, protocol_version, port=port, ssl_opts=ssl_opts, **kwargs)

    def cql_cluster_session(  # noqa: PLR0913
        self,
        node,
        keyspace=None,
        user=None,
        password=None,
        compression=True,
        protocol_version=None,
        port=None,
        ssl_opts=None,
        topology_event_refresh_window=10,
        request_timeout=None,
        exclusive=False,
        **kwargs,
    ):
        if exclusive:
            node_ip = get_ip_from_node(node)
            topology_event_refresh_window = -1
            load_balancing_policy = WhiteListRoundRobinPolicy([node_ip])
        else:
            load_balancing_policy = default_lbp_factory()

        session = self._create_session(
            node,
            keyspace,
            user,
            password,
            compression,
            protocol_version,
            port=port,
            ssl_opts=ssl_opts,
            topology_event_refresh_window=topology_event_refresh_window,
            load_balancing_policy=load_balancing_policy,
            request_timeout=request_timeout,
            keep_session=False,
            **kwargs,
        )

        class ClusterSession:
            def __init__(self, session):
                self.session = session

            def __del__(self):
                self.__cleanup()

            def __enter__(self):
                return self.session

            def __exit__(self, _type, value, traceback):
                self.__cleanup()

            def __cleanup(self):
                if self.session:
                    self.session.cluster.shutdown()
                    self.session = None

        return ClusterSession(session)

    def patient_cql_cluster_session(  # noqa: PLR0913
        self,
        node,
        keyspace=None,
        user=None,
        password=None,
        request_timeout=None,
        compression=True,
        timeout=60,
        protocol_version=None,
        port=None,
        ssl_opts=None,
        topology_event_refresh_window=10,
        exclusive=False,
        **kwargs,
    ):
        """
        Returns a connection after it stops throwing NoHostAvailables due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        return retry_till_success(
            self.cql_cluster_session,
            node,
            keyspace=keyspace,
            user=user,
            password=password,
            timeout=timeout,
            request_timeout=request_timeout,
            compression=compression,
            protocol_version=protocol_version,
            port=port,
            ssl_opts=ssl_opts,
            topology_event_refresh_window=topology_event_refresh_window,
            exclusive=exclusive,
            bypassed_exception=NoHostAvailable,
            **kwargs,
        )

    def exclusive_cql_connection(  # noqa: PLR0913
        self,
        node,
        keyspace=None,
        user=None,
        password=None,
        compression=True,
        protocol_version=None,
        port=None,
        ssl_opts=None,
        **kwargs,
    ):
        node_ip = get_ip_from_node(node)
        wlrr = WhiteListRoundRobinPolicy([node_ip])

        return self._create_session(node, keyspace, user, password, compression, protocol_version, port=port, ssl_opts=ssl_opts, load_balancing_policy=wlrr, **kwargs)

    def _create_session(  # noqa: PLR0913
        self,
        node,
        keyspace,
        user,
        password,
        compression,
        protocol_version,
        port=None,
        ssl_opts=None,
        execution_profiles=None,
        topology_event_refresh_window=10,
        request_timeout=None,
        keep_session=True,
        ssl_context=None,
        load_balancing_policy=None,
        **kwargs,
    ):
        nodes = []
        if type(node) is list:
            nodes = node
            node = nodes[0]
        else:
            nodes = [node]
        node_ips = [get_ip_from_node(node) for node in nodes]
        if not port:
            port = get_port_from_node(node)

        if protocol_version is None:
            protocol_version = DEFAULT_PROTOCOL_VERSION

        if user is not None:
            auth_provider = get_auth_provider(user=user, password=password)
        else:
            auth_provider = None

        if request_timeout is None:
            request_timeout = self.cql_request_timeout

        if load_balancing_policy is None:
            load_balancing_policy = default_lbp_factory()

        profiles = {EXEC_PROFILE_DEFAULT: make_execution_profile(request_timeout=request_timeout, load_balancing_policy=load_balancing_policy, **kwargs)}
        if execution_profiles is not None:
            profiles.update(execution_profiles)

        cluster = PyCluster(
            node_ips,
            auth_provider=auth_provider,
            compression=compression,
            protocol_version=protocol_version,
            port=port,
            ssl_options=ssl_opts,
            connect_timeout=5,
            max_schema_agreement_wait=60,
            control_connection_timeout=6.0,
            allow_beta_protocol_version=True,
            topology_event_refresh_window=topology_event_refresh_window,
            execution_profiles=profiles,
            ssl_context=ssl_context,
            # The default reconnection policy has a large maximum interval
            # between retries (600 seconds). In tests that restart/replace nodes,
            # where a node can be unavailable for an extended period of time,
            # this can cause the reconnection retry interval to get very large,
            # longer than a test timeout.
            reconnection_policy=ExponentialReconnectionPolicy(1.0, 4.0),
        )
        session = cluster.connect(wait_for_all_pools=True)

        if keyspace is not None:
            session.set_keyspace(keyspace)

        if keep_session:
            self.connections.append(session)

        return session

    def patient_cql_connection(  # noqa: PLR0913
        self,
        node,
        keyspace=None,
        user=None,
        password=None,
        timeout=30,
        compression=True,
        protocol_version=None,
        port=None,
        ssl_opts=None,
        **kwargs,
    ):
        """
        Returns a connection after it stops throwing NoHostAvailables due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        expected_log_lines = ("Control connection failed to connect, shutting down Cluster:", "[control connection] Error connecting to ")
        with log_filter("cassandra.cluster", expected_log_lines):
            session = retry_till_success(
                self.cql_connection,
                node,
                keyspace=keyspace,
                user=user,
                password=password,
                timeout=timeout,
                compression=compression,
                protocol_version=protocol_version,
                port=port,
                ssl_opts=ssl_opts,
                bypassed_exception=NoHostAvailable,
                **kwargs,
            )

        return session

    def patient_exclusive_cql_connection(  # noqa: PLR0913
        self,
        node,
        keyspace=None,
        user=None,
        password=None,
        timeout=30,
        compression=True,
        protocol_version=None,
        port=None,
        ssl_opts=None,
        **kwargs,
    ):
        """
        Returns a connection after it stops throwing NoHostAvailables due to not being ready.

        If the timeout is exceeded, the exception is raised.
        """
        return retry_till_success(
            self.exclusive_cql_connection,
            node,
            keyspace=keyspace,
            user=user,
            password=password,
            timeout=timeout,
            compression=compression,
            protocol_version=protocol_version,
            port=port,
            ssl_opts=ssl_opts,
            bypassed_exception=NoHostAvailable,
            **kwargs,
        )

    def check_errors(self,
                     node: ScyllaNode,
                     exclude_errors: str | tuple[str, ...] | list[str] | None = None,
                     search_str: None = None,  # not used in scylla-dtest
                     from_mark: int | None = None,  # not used in scylla-dtest
                     regex: bool = False,
                     return_errors: bool = False) -> list[str]:
        assert search_str is None, "argument `search_str` is not supported"
        assert from_mark is None, "argument `from_mark` is not supported"

        match exclude_errors:
            case tuple():
                exclude_errors = list(exclude_errors)
            case list():
                pass
            case str():
                exclude_errors = [exclude_errors]
            case None:
                exclude_errors = []
            case _:
                raise TypeError(f"Unsupported type for `exlude_errors` argument: {type(exclude_errors)}")

        if not regex:
            exclude_errors = [re.escape(error) for error in exclude_errors]

        # Yep, we have such side effect in scylla-dtest.
        self.ignore_log_patterns += exclude_errors

        exclude_errors_pattern = re.compile("|".join(f"{p}" for p in {
            *self.ignore_log_patterns,
            *self.ignore_cores_log_patterns,

            r"Compaction for .* deliberately stopped",
            r"update compaction history failed:.*ignored",

            # We may stop nodes that have not finished starting yet.
            r"(Startup|start) failed:.*(seastar::sleep_aborted|raft::request_aborted)",
            r"Timer callback failed: seastar::gate_closed_exception",

            # Ignore expected RPC errors when nodes are stopped.
            r"rpc - client .*(connection dropped|fail to connect)",

            # We see benign RPC errors when nodes start/stop.
            # If they cause system malfunction, it should be detected using higher-level tests.
            r"rpc::unknown_verb_error",
            r"raft_rpc - Failed to send",
            r"raft_topology.*(seastar::broken_promise|rpc::closed_error)",

            # Expected tablet migration stream failure where a node is stopped.
            # Refs: https://github.com/scylladb/scylladb/issues/19640
            r"Failed to handle STREAM_MUTATION_FRAGMENTS.*rpc::stream_closed",

            # Expected Raft errors on decommission-abort or node restart with MV.
            r"raft_topology - raft_topology_cmd.*failed with: raft::request_aborted",
        }))

        errors = node.grep_log_for_errors(distinct_errors=True)
        errors = [remove_control_chars(error) for error in errors if not exclude_errors_pattern.search(error)]

        if return_errors:
            return errors

        assert not errors, "\n".join(errors)

    def check_errors_all_nodes(self,
                               nodes: list[ScyllaNode] | None = None,  # not used in scylla-dtest
                               exclude_errors: str | tuple[str, ...] | list[str] | None = None,
                               search_str: str | None = None,  # not used in scylla-dtest
                               regex: bool = False) -> None:
        assert search_str is None, "argument `search_str` is not supported"
        assert nodes is None, "argument `nodes` is not supported"

        critical_errors = []
        found_errors = []

        logger.debug("exclude_errors: %s", exclude_errors)

        for node in self.cluster.nodelist():
            try:
                critical_errors_pattern = r"Assertion.*failed|AddressSanitizer"
                if self.ignore_cores_log_patterns:
                    if matches := node.grep_log("|".join(f"({p})" for p in set(self.ignore_cores_log_patterns))):
                        logger.debug("Will ignore cores on %s. Found the following log messages: %s", node.name, matches)
                        self.ignore_cores.append(node)
                if node not in self.ignore_cores:
                    critical_errors_pattern += "|Aborting on shard"
                if matches := node.grep_log(critical_errors_pattern, filter_expr="|".join(self.ignore_log_patterns)):
                    critical_errors.append((node.name, [m[0].strip() for m in matches]))
            except FileNotFoundError:
                pass

            if errors := self.check_errors(node=node, exclude_errors=exclude_errors, regex=regex, return_errors=True):
                found_errors.append((node.name, errors))

        assert not critical_errors, f"Critical errors found: {critical_errors}\nOther errors: {found_errors}"

        if found_errors:
            logger.error("Unexpected errors found: %s", found_errors)
            errors_summary = "\n".join(
                f"{node}: {len(errors)} errors\n{"\n".join(errors[:5])}" for node, errors in found_errors
            )
            raise AssertionError(f"Unexpected errors found:\n{errors_summary}")

        found_cores, _ = self.find_cores()

        assert not found_cores, "Core file(s) found. Marking test as failed."

    def init_default_config(self):  # noqa: PLR0912,PLR0915
        # the failure detector can be quite slow in such tests with quick start/stop
        timeout = self.cql_timeout() * 1000
        range_timeout = 3 * timeout
        self.cql_request_timeout = 3 * self.cql_timeout()
        # count(*) queries are particularly slow in debug mode
        # need to adjust the session or query timeout respectively
        self.count_request_timeout = self.cql_timeout(400)

        logger.debug(f"Scylla mode is '{self.cluster.scylla_mode}'")
        logger.debug(f"Cluster *_request_timeout_in_ms={timeout}, range_request_timeout_in_ms={range_timeout}, cql request_timeout={self.cql_request_timeout}")

        values: dict[str, Any] = self.cluster_options | {
            "phi_convict_threshold": 5,
            "task_ttl_in_seconds": 0,
            "read_request_timeout_in_ms": timeout,
            "range_request_timeout_in_ms": range_timeout,
            "write_request_timeout_in_ms": timeout,
            "truncate_request_timeout_in_ms": range_timeout,
            "counter_write_request_timeout_in_ms": timeout * 2,
            "cas_contention_timeout_in_ms": timeout,
            "request_timeout_in_ms": timeout,
            "num_tokens": None,
        }

        if self.setup_overrides is not None and self.setup_overrides.cluster_options:
            values.update(self.setup_overrides.cluster_options)

        if self.dtest_config.use_vnodes:
            values.update({
                "initial_token": None,
                "num_tokens": self.dtest_config.num_tokens,
            })

        experimental_features = values.setdefault("experimental_features", [])
        if "views-with-tablets" not in experimental_features:
            experimental_features.append("views-with-tablets")

        if self.dtest_config.experimental_features:
            for f in self.dtest_config.experimental_features:
                if f not in experimental_features:
                    experimental_features.append(f)
        self.scylla_features |= set(values.get("experimental_features", []))

        if self.dtest_config.force_gossip_topology_changes:
            logger.debug("Forcing gossip topology changes")
            values["force_gossip_topology_changes"] = True

        logger.debug("Setting 'enable_tablets' to %s", self.dtest_config.tablets)
        values["enable_tablets"] = self.dtest_config.tablets
        values["tablets_mode_for_new_keyspaces"] = "enabled" if self.dtest_config.tablets else "disabled"
        if self.dtest_config.tablets:
            self.scylla_features.add("tablets")

            # Avoid having too many tablets per shard by default as this slows down node operations like
            # decommission, due to concurrency limit of parallel migrations per shard, and
            # because with small tablets group0 transition latency dominates migration time,
            # which is pronounced in debug mode. All of this may cause timeouts of node operations
            # with higher tablet count.
            # Set to more than 1 to exercise having many compaction groups.
            values["tablets_initial_scale_factor"] = 1
            values["tablets_per_shard_goal"] = 1000

        self.cluster.set_configuration_options(values)
        logger.debug("Done setting configuration options:\n" + pprint.pformat(self.cluster._config_options, indent=4))

    def cql_timeout(self, seconds=None):
        if not seconds:
            seconds = self.base_cql_timeout
        factor = 1
        if isinstance(self.cluster, ScyllaCluster):
            if self.cluster.scylla_mode == "debug":
                factor = 3
            elif self.cluster.scylla_mode != "release":
                factor = 2
        return seconds * factor

    def disable_error(self, name, node):
        """Disable error injection
        Args:
            name (str): name of error injection to be disabled.
            node (ScyllaNode|int): either instance of scylla node or node number.
        """
        with DisableLogger("urllib3.connectionpool"):
            if isinstance(node, int):
                node = self.cluster.nodelist()[node]
            node_ip = get_ip_from_node(node)
            logger.trace(f'Disabling error injection "{name}" on node {node_ip}')

            response = requests.delete(f"http://{node_ip}:10000/v2/error_injection/injection/{name}")
            response.raise_for_status()

    def check_error(self, name, node):
        """Get status of error injection

        Args:
            name (str): name of error injection.
            node (ScyllaNode|int): either instance of scylla node or node number.

        """
        with DisableLogger("urllib3.connectionpool"):
            if isinstance(node, int):
                node = self.cluster.nodelist()[node]
            node_ip = get_ip_from_node(node)
            response = requests.get(f"http://{node_ip}:10000/v2/error_injection/injection/{name}")
            response.raise_for_status()

    def list_errors(self, node):
        """List enabled error injections

        Args:
            node (ScyllaNode|int): either instance of scylla node or node number.

        """
        with DisableLogger("urllib3.connectionpool"):
            if isinstance(node, int):
                node = self.cluster.nodelist()[node]
            node_ip = get_ip_from_node(node)
            response = requests.get(f"http://{node_ip}:10000/v2/error_injection/injection")
            response.raise_for_status()
            return response.json()

    def disable_errors(self, node):
        """Disable all error injections

        Args:
            node (ScyllaNode|int): either instance of scylla node or node number.

        """
        with DisableLogger("urllib3.connectionpool"):
            if isinstance(node, int):
                node = self.cluster.nodelist()[node]
            node_ip = get_ip_from_node(node)
            logger.trace(f"Disable all error injections on node {node_ip}")
            response = requests.delete(f"http://{node_ip}:10000/v2/error_injection/injection")
            response.raise_for_status()

    def enable_error(self, name, node, one_shot=False):
        """Enable error injection

        Args:
            name (str): name of error injection to be enabled.
            node (ScyllaNode|int): either instance of scylla node or node number.
            one_shot (bool): indicates whether the injection is one-shot
                             (resets enabled state after triggering the injection).

        """
        with DisableLogger("urllib3.connectionpool"):
            if isinstance(node, int):
                node = self.cluster.nodelist()[node]
            node_ip = get_ip_from_node(node)
            logger.trace(f'Enabling error injection "{name}" on node {node_ip}')
            response = requests.post(f"http://{node_ip}:10000/v2/error_injection/injection/{name}", params={"one_shot": one_shot})
            response.raise_for_status()
