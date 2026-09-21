#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import glob
import logging
import operator
import os
import pprint
import re
import shutil
import subprocess
import threading
from functools import partial, partialmethod, reduce
from pathlib import Path
from typing import TYPE_CHECKING

import requests
from cassandra import AuthenticationFailed
from cassandra.cluster import EXEC_PROFILE_DEFAULT, NoHostAvailable, default_lbp_factory
from cassandra.cluster import Cluster as PyCluster
from cassandra.policies import ExponentialReconnectionPolicy, WhiteListRoundRobinPolicy

from test.pylib.driver_utils import safe_driver_shutdown

from test.cluster.dtest.dtest_class import (
    get_auth_provider,
    get_ip_from_node,
    get_port_from_node,
    make_execution_profile,
)
from test.cluster.dtest.ccmlib.common import is_win
from test.cluster.dtest.ccmlib.scylla_cluster import ScyllaCluster
from test.cluster.dtest.tools.context import log_filter
from test.cluster.dtest.tools.log_utils import DisableLogger, get_test_log_name, remove_control_chars
from test.cluster.dtest.tools.misc import retry_till_success

if TYPE_CHECKING:
    from typing import Any

    from test.cluster.dtest.ccmlib.scylla_node import ScyllaNode
    from test.cluster.dtest.dtest_config import DTestConfig
    from test.cluster.dtest.dtest_setup_overrides import DTestSetupOverrides
    from test.pylib.scylla_cluster_manager import ScyllaClusterManager


DEFAULT_PROTOCOL_VERSION = 4

KEEP_CORES = os.environ.get("KEEP_CORES", "true").lower() in ("yes", "true")
DTEST_CORE_COMPRESS_TOOL = os.environ.get("DTEST_CORE_COMPRESS_TOOL", "gzip")
DTEST_CORE_COMPRESS_EXT = os.environ.get("DTEST_CORE_COMPRESS_EXT", "gz")

logger = logging.getLogger(__name__)

# Add custom TRACE level, for development print we don't want on debug level
logging.TRACE = 5
logging.addLevelName(logging.TRACE, "TRACE")
logging.Logger.trace = partialmethod(logging.Logger.log, logging.TRACE)
logging.trace = partial(logging.log, logging.TRACE)


def _should_retry_no_host(e):
    """Don't retry NoHostAvailable if it wraps AuthenticationFailed."""
    return not any(isinstance(err, AuthenticationFailed) for err in e.errors.values())


# NOTE: restored verbatim (imports aside) from scylla-dtest's dtest_setup.py;
# it was trimmed when this module was first ported in-tree, but
# not-yet-adapted dtest/unported test modules still import it. It relies on
# `dtest_config.cluster`/`dtest_config.find_cores()`, which are part of the
# ccm-based DTestConfig from the original dtest, not the
# test.pylib.scylla_cluster_manager-based one used in-tree; it is kept as-is
# for import purposes only, not expected to work at runtime until the
# consuming test modules are adapted.
def copy_logs(request, dtest_config, directory=None, name=None, cores=None):  # noqa: PLR0912, PLR0915
    """Copy the current cluster's log files somewhere, by default to LOG_SAVED_DIR with a name of 'last'"""
    log_saved_dir = os.environ.get("LOG_SAVED_DIR", "logs")
    try:
        os.mkdir(log_saved_dir)
    except OSError:
        pass

    if directory is None:
        directory = log_saved_dir
    if name is None:
        name = os.path.join(log_saved_dir, "last")
    else:
        name = os.path.join(directory, name)
    if not os.path.exists(directory):
        os.mkdir(directory)

    # Use shared helper function to ensure consistency with per-test log file naming
    # Note: no extension chars reserved here since this is a directory name
    basedir = get_test_log_name(request, directory=directory, reserve_extension_chars=0)
    logdir = os.path.join(directory, basedir)
    os.mkdir(logdir)

    cluster_path = dtest_config.cluster.get_path()

    for log in glob.glob(os.path.join(cluster_path, "**/logs/*"), recursive=True):
        n = re.search(r"node\d+", log).group(0)
        logname = os.path.basename(log)
        # for backward compatibility, rename the logs:
        #   nodeX/logs/system.log to nodeX.log
        #   nodeX/logs/debug.log to nodeX_debug.log
        if logname == "system.log":
            dest = n + ".log"
        else:
            dest = f"{n}_{logname}"
        shutil.copyfile(log, os.path.join(logdir, dest))

    jmx_core_files = reduce(operator.iadd, [glob.glob(match) for match in ("core", "core.*", "hs_err_*", "replay_*")], [])
    for jmx_core_file in jmx_core_files:
        shutil.copyfile(jmx_core_file, Path(logdir) / Path(jmx_core_file).name)

    for pcap in glob.glob(str(Path(cluster_path) / "tcpdump_*.pcap")):
        shutil.copyfile(pcap, Path(logdir) / Path(pcap).name)

    if hasattr(dtest_config.cluster, "_scylla_manager") and dtest_config.cluster._scylla_manager:
        log = os.path.join(dtest_config.cluster._scylla_manager._get_path(), "scylla-manager.log")
        if os.path.exists(log):
            shutil.copyfile(log, os.path.join(logdir, "scylla-manager.log"))

        logs = [(node.name, node.logfilename() + ".manager_agent") for node in dtest_config.cluster.nodes.values()]
        if logs:
            for node_name, agent_log in logs:
                if os.path.exists(agent_log):
                    shutil.copyfile(agent_log, os.path.join(logdir, node_name + ".manager_agent.log"))

    if KEEP_CORES:
        if cores is None:
            cores, ignored_cores = dtest_config.find_cores()
            cores += ignored_cores
        if cores:
            for n, src in cores:
                dst = os.path.join(logdir, f"{n}-{os.path.basename(src)}")
                logger.warning(f"Moving core file {src} to {dst}")
                try:
                    if DTEST_CORE_COMPRESS_TOOL == "":
                        cmd = f"mv {src} {dst}"
                        shutil.move(src, dst)
                    else:
                        cmd = f"{DTEST_CORE_COMPRESS_TOOL} < {src} > {dst}.{DTEST_CORE_COMPRESS_EXT} && rm {src}"
                        subprocess.check_call(cmd, shell=True)
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"`{cmd}` failed: {e}. Keeping directory.")

    if os.path.exists(logdir):
        if os.path.exists(name):
            os.unlink(name)
        if not is_win():
            os.symlink(basedir, name)


class _Runner:
    """Run `func(i)` with an incrementing `i` in a background thread until stopped.

    Any exception `func` raises is stashed rather than propagated, so the
    background thread never crashes the process; `check()`/`stop()` re-raise
    it in the caller instead, at a point of the caller's choosing.
    """

    def __init__(self, func, sleep=1.0):
        self._func = func
        self._sleep = sleep
        self._exception = None
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        i = 0
        while not self._stop_event.is_set():
            try:
                self._func(i)
            except Exception as e:  # noqa: BLE001
                self._exception = e
                return
            i += 1
            # Pause between calls, as scylla-dtest's Runner: without it the function runs
            # thousands of times instead of once a second.
            self._stop_event.wait(self._sleep)

    def check(self):
        """Re-raise `func`'s exception, if it has raised one so far."""

        if self._exception is not None:
            raise self._exception

    def stop(self):
        """Stop the background thread and re-raise any exception it hit."""

        self._stop_event.set()
        self._thread.join()
        self.check()


class DTestSetup:
    def __init__(self,
                 dtest_config: DTestConfig | None = None,
                 setup_overrides: DTestSetupOverrides | None = None,
                 manager: ScyllaClusterManager | None = None,
                 scylla_mode: str | None = None,
                 cluster_name: str = "test"):
        self.dtest_config = dtest_config
        self.setup_overrides = setup_overrides
        self.cluster_name = cluster_name
        self.ignore_log_patterns = []
        self.ignore_cores_log_patterns = []
        self.ignore_cores = []
        # Upgrade tests override the dtest_config fixture to name the version the
        # cluster must *start* on (the oldest one in their upgrade path); every
        # other test leaves it at the build under test.
        self.cluster = ScyllaCluster(
            manager=manager,
            scylla_mode=scylla_mode,
            scylla_version=getattr(dtest_config, "scylla_version", None),
            # scylla-dtest built every cluster this way (its dtest_setup.py).
            # It makes a bare cluster.start() wait for CQL and for the other
            # nodes to notice the new one, which is what the ported tests
            # assume when they call start() with no arguments.
            force_wait_for_cluster_start=True,
        )
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
                    safe_driver_shutdown(self.session.cluster)
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
            # The base delay decides how long a reconnect is delayed after a node is
            # already back up; max_attempts keeps the overall budget at ~251s.
            reconnection_policy=ExponentialReconnectionPolicy(0.1, 1.0, 250),
        )
        try:
            session = cluster.connect(wait_for_all_pools=True)

            if keyspace is not None:
                session.set_keyspace(keyspace)
        except BaseException:
            # The Cluster constructor already started the driver's "Task Scheduler" thread,
            # and Cluster has no __del__, so a half-built Cluster would keep that thread
            # running forever and eventually crash the pytest worker with
            # "cannot schedule new futures after shutdown".
            safe_driver_shutdown(cluster)
            raise

        if keep_session:
            self.connections.append(session)

        return session

    def go(self, func):
        """Run `func(i)`, with an incrementing `i`, in a background thread until stopped.

        Returns a `_Runner`: call `.check()` to re-raise anything `func` has
        thrown so far without stopping it, or `.stop()` to stop it and raise.
        """

        return _Runner(func)

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
                should_retry=_should_retry_no_host,
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
            should_retry=_should_retry_no_host,
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

            # Expected when a node is stopped while raft topology is waiting for an IP.
            r"raft_topology - raft_topology_cmd.*failed with: seastar::sleep_aborted",
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

        # The test's own cluster_options go last: a @pytest.mark.cluster_options
        # is how a test asks for something other than the defaults below, and
        # merging it first meant sstable_format, say, could not be asked for.
        values: dict[str, Any] = {
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
            "sstable_format": "mt",
            # test.py's scylla.yaml sets strict_allow_filtering: true, which rejects queries that
            # scylla-dtest ran against Scylla's default ("warn": run them, with a warning).
            "strict_allow_filtering": "warn",
        } | self.cluster_options

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

        logger.debug("Setting 'enable_tablets' to %s", self.dtest_config.tablets)
        values.update(self.get_tablets_config(self.dtest_config.tablets))
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

    @staticmethod
    def get_tablets_config(enable_tablets: bool) -> dict[str, Any]:
        """The scylla.yaml options that turn tablets on or off.

        Both spellings, because the boolean was the only one older versions knew
        and the upgrade tests set this per version.
        """
        return {
            "enable_tablets": enable_tablets,
            "tablets_mode_for_new_keyspaces": "enabled" if enable_tablets else "disabled",
        }

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
