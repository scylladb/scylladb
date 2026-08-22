#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Manager client.
   A facade in front of ScyllaClusterManager: calls it directly as a Python
   object and provides helper methods to test cases.
   Manages driver refresh when cluster is cycled.
"""
from collections import defaultdict
import pathlib
import re
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from collections.abc import Coroutine
from typing import List, Optional, Callable, Any, Awaitable, Dict, Union
from time import time
import logging
from test.pylib.log_browsing import ScyllaLogFile
from test.pylib.rest_client import ScyllaRESTAPIClient, ScyllaMetricsClient
from test.pylib.util import gather_safely, wait_for, wait_for_cql_and_get_hosts, universalasync_typed_wrap, \
    Host, graceful_stop_timeout
from test.pylib.internal_types import ServerNum, IPAddress, HostID, ServerInfo, ServerUpState
from test.pylib.scylla_cluster import ReplaceConfig, ScyllaClusterManager, ScyllaServer, ScyllaVersionDescription, bind_to_current_loop
from test.pylib.driver_utils import safe_driver_shutdown
from cassandra.cluster import Session as CassandraSession, \
    ExecutionProfile, EXEC_PROFILE_DEFAULT  # type: ignore # pylint: disable=no-name-in-module
from cassandra.policies import LoadBalancingPolicy, RoundRobinPolicy, WhiteListRoundRobinPolicy
from cassandra.cluster import Cluster as CassandraCluster  # type: ignore # pylint: disable=no-name-in-module
from cassandra.auth import AuthProvider
import asyncio
import allure


logger = logging.getLogger(__name__)


class NoSuchProcess(Exception):
    ...


@universalasync_typed_wrap
class ManagerClient:
    """Helper Manager API client
    Args:
        cluster_manager (ScyllaClusterManager): the manager to drive
        manager_loop: the event loop the manager runs on
        con_gen (Callable): generator function for CQL driver connection to a cluster
        mode (str): test.py build mode, for scaling timeouts to match the manager's
    """
    # pylint: disable=too-many-public-methods

    def __init__(self, cluster_manager: ScyllaClusterManager, manager_loop: asyncio.AbstractEventLoop,
                 port: int, use_ssl: bool, auth_provider: Any|None,
                 con_gen: Callable[[List[IPAddress], int, bool, Any, LoadBalancingPolicy], CassandraCluster],
                 mode: str) \
                         -> None:
        self.mode = mode
        self.port = port
        self.use_ssl = use_ssl
        self.auth_provider = auth_provider
        self.load_balancing_policy = RoundRobinPolicy()
        self.con_gen = con_gen
        self.ccluster: Optional[CassandraCluster] = None
        self.cql: Optional[CassandraSession] = None
        self.exclusive_clusters: List[CassandraCluster] = []
        self._cluster_manager = cluster_manager
        self._manager_loop = manager_loop
        self._test_finished = False
        self.api = ScyllaRESTAPIClient()
        self.metrics = ScyllaMetricsClient()
        self.thread_pool = ThreadPoolExecutor()
        self.ignore_log_patterns = []  # patterns to ignore in server logs when checking for errors
        self.ignore_cores_log_patterns = []  # patterns to ignore in server logs when checking for core files

    @property
    def _manager(self) -> ScyllaClusterManager:
        """The manager, guarded: fail loudly when a task leaked by the test
        touches the manager after the test finished."""
        if self._test_finished:
            raise Exception("ManagerClient is not accessible after the test finished")
        return self._cluster_manager

    async def _call(self, op: Coroutine, timeout: float | None = None) -> Any:
        """Run a manager operation on the manager's own event loop.

        The manager owns loop-bound state -- the Scylla subprocess transports
        and the per-server asyncio locks -- so its coroutines must run on its
        loop, not on whichever loop the caller happens to have.  Callers get
        their loop from pytest, or, when a synchronous dtest test calls in from
        a worker thread, from universalasync, which creates a fresh one per
        thread; awaiting a manager coroutine there would fail with "got Future
        attached to a different loop".

        The operation is capped at 300 s unless the caller passes a timeout,
        the same ceiling the HTTP client used to apply to every request.  A
        timed-out or cancelled caller does not abort the operation: manager_op
        shields it, so it runs to completion and after_test() drains it, just
        as an abandoned HTTP request used to leave its handler running.
        """
        future = asyncio.run_coroutine_threadsafe(op, self._manager_loop)
        async with asyncio.timeout(300 if timeout is None else timeout):
            return await asyncio.wrap_future(future, loop=asyncio.get_running_loop())

    async def stop(self):
        """Close driver"""
        self.driver_close()

    async def driver_connect(self, server: Optional[ServerInfo] = None, auth_provider: Optional[AuthProvider] = None) -> None:
        """Connect to cluster"""
        targets = [server] if server else await self.running_servers()
        servers = [s_info.rpc_address for s_info in targets]
        # avoids leaking connections if driver wasn't closed before
        self.driver_close()
        logger.debug("driver connecting to %s", servers)
        self.ccluster = self.con_gen(servers, self.port, self.use_ssl,
                                     auth_provider if auth_provider else self.auth_provider, self.load_balancing_policy)
        self.cql = self.ccluster.connect()

    def driver_close(self) -> None:
        """Disconnect from cluster"""
        for cluster in self.exclusive_clusters:
            safe_driver_shutdown(cluster)
        self.exclusive_clusters.clear()
        if self.ccluster is not None:
            logger.debug("shutting down driver")
            safe_driver_shutdown(self.ccluster)
            self.ccluster = None
        self.cql = None

    def get_cql(self) -> CassandraSession:
        """Precondition: driver is connected"""
        assert self.cql
        return self.cql

    # More robust version of get_cql, when topology changes
    # or cql statement is executed immediately after driver_connect
    # it may fail unless we perform additional readiness checks
    async def get_ready_cql(self, servers: List[ServerInfo]) -> tuple[CassandraSession, list[Host]]:
        """Precondition: driver is connected"""
        cql = self.get_cql()
        await self.servers_see_each_other(servers)
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time() + 60)
        return cql, hosts

    async def get_cql_exclusive(self, server: ServerInfo, auth_provider: Optional[AuthProvider] = None):
        cluster = self.con_gen([server.ip_addr], self.port, self.use_ssl,
                               auth_provider if auth_provider else self.auth_provider,
                               WhiteListRoundRobinPolicy([server.ip_addr]))
        self.exclusive_clusters.append(cluster)
        cql = cluster.connect()
        await wait_for_cql_and_get_hosts(cql, [server], time() + 60)
        return cql

    # Make driver update endpoints from remote connection
    def _driver_update(self) -> None:
        if self.ccluster is not None:
            logger.debug("refresh driver node list")
            self.ccluster.control_connection.refresh_node_list_and_token_map()

    async def check_all_errors(self, check_all_errors=False) -> dict[ServerInfo, dict[str, Union[list[str], list[str], Path, list[str]]]]:
        
        errors = defaultdict(dict)
        # find errors in logs
        for server in await self.all_servers():
            log_file = await self.server_open_log(server_id=server.server_id)
            # check if we should ignore cores on this server
            ignore_cores = []
            if self.ignore_cores_log_patterns:
                if matches := await log_file.grep("|".join(f"({p})" for p in set(self.ignore_cores_log_patterns))):
                    logger.debug(f"Will ignore cores on {server}. Found the following log messages: {matches}")
                    ignore_cores.append(server)
            critical_error_pattern = r"Assertion.*failed|AddressSanitizer"
            if server not in ignore_cores:
                critical_error_pattern += "|Aborting on shard"
            if found_critical := await log_file.grep(critical_error_pattern):
                errors[server]["critical"] = [e[0] for e in found_critical]
                # Find the backtraces for the critical errors
                if found_backtraces := await log_file.find_backtraces():
                    errors[server]["backtraces"] = found_backtraces
            if check_all_errors:
                if found_errors := await log_file.grep_for_errors(distinct_errors=True):
                    if filtered_errors := await self.filter_errors(found_errors):
                        errors[server]["error"] = filtered_errors
        # find core files
        for server, cores in (await self.find_cores()).items():
            errors[server]["cores"] = cores
        # add log file path to the report for servers that had errors or cores
        for server in await self.all_servers():
            log_file = await self.server_open_log(server_id=server.server_id)
            if server in errors:
                errors[server]["log"] = log_file.file.name

        return errors
    
    async def filter_errors(self, errors: list[str] | list[list[str]]):
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
        # Support both list[str] (from distinct_errors=True) and
        # list[list[str]] (from distinct_errors=False), matching against
        # the first line of each error group.
        def match_line(e):
            line = e[0] if isinstance(e, list) else e
            return not exclude_errors_pattern.search(line)
        return [e for e in errors if match_line(e)]

    async def find_cores(self) -> dict[ServerInfo, list[str]]:
        """Find core files on all servers"""
        # find *.core files in current dir
        cores = [str(core_file.absolute()) for core_file in pathlib.Path('.').glob('*.core')]
        server_cores = dict()
        # match core files to servers by pid
        for server in await self.all_servers():
            if found_cores := [core for core in cores if f".{server.pid}." in core]:
                server_cores[server] = found_cores
        return server_cores

    async def gather_related_logs(self, failed_test_path_dir: Path, logs: Dict[str, Path]) -> None:
        for server in await self.all_servers():
            log_file = await self.server_open_log(server_id=server.server_id)
            shutil.copyfile(log_file.file, failed_test_path_dir / f"{pathlib.Path(log_file.file).name}")
            allure.attach(log_file.file.read_bytes(), name=log_file.file.name, attachment_type=allure.attachment_type.TEXT)
        for name, log in logs.items():
            # A log is missing when the handler that writes it was never installed, e.g. the
            # test failed before before-test ran. Skip it: the caller collects the rest of the
            # artifacts after this returns, so raising here would lose them all.
            if not log.is_file():
                logger.warning("Log %s (%s) does not exist, not attaching it", name, log)
                continue
            allure.attach(log.read_bytes(), name=name, attachment_type=allure.attachment_type.TEXT)
            shutil.copyfile(log, failed_test_path_dir / name)

    async def before_test(self, test_case_name: str) -> str:
        """Before-test hook of the manager fixture: lease a cluster for the test."""
        return await self._call(self._manager.before_test(test_case_name), timeout=600)

    async def after_test(self, success: bool) -> dict[str, Any]:
        """After-test hook of the manager fixture.

        Marks this client as finished first, so a task leaked by the test
        cannot reach the manager anymore, then reports the test result to
        the manager (bypassing that very guard).
        """
        self._test_finished = True
        return await self._call(self._cluster_manager.after_test(success))

    async def is_dirty(self) -> bool:
        """Check if current cluster dirty."""
        return await self._call(self._manager.is_dirty())

    async def running_servers(self) -> list[ServerInfo]:
        """Get List of server info (id and IP address) of running servers"""
        return await self._call(self._manager.running_servers())

    async def all_servers(self) -> list[ServerInfo]:
        """Get List of server info (id and IP address) of all servers"""
        return await self._call(self._manager.all_servers())

    async def all_servers_by_host_id(self) -> Dict[HostID, ServerInfo]:
        result = dict()
        servers = await self.all_servers()
        for s in servers:
            result[await self.get_host_id(s.server_id)] = s
        return result

    async def find_server_by_host_id(self, servers: List[ServerInfo], host_id: HostID) -> ServerInfo:
        for s in servers:
            try:
                if await self.get_host_id(s.server_id) == host_id:
                    return s
            except Exception:
                logger.warning(f"Failed to get host ID of server {s} while looking for a server with host ID {host_id}")
        raise Exception(f"Host ID {host_id} not found in {servers}")

    async def starting_servers(self) -> list[ServerInfo]:
        """Get List of server info (id and IP address) of servers currently
           starting. Can be useful for killing (with server_stop()) a server
           which a test started in the background but now doesn't expect to
           ever finish booting successfully.
        """
        return await self._call(self._manager.starting_servers())

    async def mark_dirty(self) -> None:
        """Manually mark current cluster dirty.
           To be used when a server was modified outside of this API."""
        await self._call(self._manager.mark_dirty())

    async def mark_clean(self) -> None:
        """Manually mark current cluster not dirty.
           To be used when a current cluster wants to be reused."""
        await self._call(self._manager.mark_clean())

    async def server_stop(self, server_id: ServerNum, convict: bool) -> None:
        """Stop specified server.

        convict: If True, immediately marks the node as DOWN on all live nodes,
                 bypassing the natural failure detection delay (~20s).
                 Set to True when the test waits for other nodes to notice the down node
                 to speed up the test (e.g. before
                 remove_node, replace, or waiting for failure detection).
                 Set to False when conviction is pointless (single node, all stopped, immediate restart)
                 or the test wants to exercise natural failure detection.
        """
        host_id = None
        if convict:
            try:
                host_id = await self.get_host_id(server_id)
            except Exception:
                # Server may have never completed bootstrap, so host_id is unknown.
                # Skip conviction in this case.
                pass
        logger.debug("ManagerClient stopping %s", server_id)
        await self._call(self._manager.server_stop(server_id))
        if host_id is not None:
            try:
                await self.convict_on_all(host_id)
            except Exception:
                # It's a best-effort attempt, ignore errors
                # In some scenarios errors are expected, e.g. when server_stop() is called concurrently on many servers.
                pass

    async def server_stop_gracefully(self, server_id: ServerNum, timeout: float | None = None) -> None:
        """Stop specified server gracefully

        With no timeout given, outlast the manager's own graceful-stop timeout
        for this mode, so that a slow stop is reported by the manager -- which
        knows why it was slow -- instead of timing out here first."""
        if timeout is None:
            timeout = graceful_stop_timeout(self.mode) + 60
        logger.debug("ManagerClient stopping gracefully %s", server_id)
        await self._call(self._manager.server_stop_gracefully(server_id), timeout=timeout)

    async def disable_tablet_balancing(self):
        """
        Disables background tablet load-balancing.
        If there are already active migrations, it waits for them to finish before returning.
        Doesn't block migrations on behalf of node operations like decommission, removenode or replace.
        :return:
        """
        servers = await self.running_servers()
        if not servers:
            raise Exception("No running servers")
        # Any server will do, it's a group0 operation
        await self.api.disable_tablet_balancing(servers[0].ip_addr)

    async def enable_tablet_balancing(self):
        """
        Enables background tablet load-balancing.
        """
        servers = await self.running_servers()
        if not servers:
            raise Exception("No running servers")
        # Any server will do, it's a group0 operation
        await self.api.enable_tablet_balancing(servers[0].ip_addr)

    async def convict(self, convict_on: ServerInfo, host: HostID):
        """Convicts a given host on a live server.
        convict_on will mark "host" as DOWN and drop connections to it.
        """
        logger.debug(f"Convicting {host} on {convict_on.ip_addr}")
        await self.api.convict(convict_on.ip_addr, host)

    async def convict_on_all(self, host: HostID):
        """Convicts a given host on all live servers.
        Each live server will mark "host" as DOWN and drop connections to it.
        """
        await gather_safely(*(self.api.convict(server.ip_addr, host) for server in await self.running_servers()))

    async def server_start(self,
                           server_id: ServerNum,
                           expected_error: str | None = None,
                           expected_crash: bool = False,
                           wait_others: int = 0,
                           wait_interval: float = 45,
                           seeds: list[IPAddress] | None = None,
                           timeout: float | None = None,
                           connect_driver: bool = True,
                           expected_server_up_state: ServerUpState = ServerUpState.SERVING,
                           cmdline_options_override: list[str] | None = None,
                           append_env_override: dict[str, str] | None = None,
                           auth_provider: dict[str, str] | None = None) -> None:
        """Start specified server.

        When expected_error is None, waits until Scylla reports that all
        configured listeners (including any non-default ports) are ready
        (ServerUpState.SERVING). Pass a lower expected_server_up_state to
        return earlier. When expected_error is set, no readiness wait is
        performed. When connect_driver=False, the effective wait state is
        capped at HOST_ID_QUERIED regardless of expected_server_up_state.

        Optionally waits for it to learn of other servers (wait_others).
        Replace CLI options and environment variables with `cmdline_options_override` and `append_env_override`
        if provided.

        If `expected_crash` is True, ignore cores and backtraces related to `expected_error`.
        """
        if expected_error is not None:
            self.ignore_log_patterns.append(re.escape(expected_error))
            if expected_crash:
                self.ignore_cores_log_patterns.append(re.escape(expected_error))

        if not connect_driver:
            expected_server_up_state = min(expected_server_up_state, ServerUpState.HOST_ID_QUERIED)

        logger.debug("ManagerClient starting %s", server_id)
        await self._call(self._manager.server_start(
            server_id,
            expected_error=expected_error,
            seeds=seeds,
            expected_server_up_state=expected_server_up_state,
            cmdline_options_override=cmdline_options_override,
            append_env_override=append_env_override,
            auth_provider=auth_provider,
        ), timeout=timeout)
        await self.server_sees_others(server_id, wait_others, interval = wait_interval)
        if expected_error is None and connect_driver:
            if self.cql:
                self._driver_update()
            else:
                await self.driver_connect()

    async def server_restart(self, server_id: ServerNum, wait_others: int = 0,
                             wait_interval: float = 45) -> None:
        """Restart specified server and optionally wait for it to learn of other servers"""
        await self.server_stop_gracefully(server_id)
        await self.server_start(server_id=server_id, wait_others=wait_others, wait_interval=wait_interval)

    async def rolling_restart(self, servers: List[ServerInfo], with_down: Optional[Callable[[ServerInfo], Awaitable[Any]]] = None, wait_for_cql = True, cmdline_options_override: list[str] | None = None):
        # `servers` might not include all the running servers, but we want to check against all of them
        servers_running = await self.running_servers()

        for s in servers:
            await self.server_stop_gracefully(s.server_id)

            # Wait for other servers to see the server to be stopped
            # so that the later server_sees_other_server() call will not
            # exit immediately, making it moot.
            for s2 in servers_running:
                if s2.server_id != s.server_id:
                    await self.server_not_sees_other_server(s2.ip_addr, s.ip_addr)

            if with_down:
                up_servers = [u for u in servers if u.server_id != s.server_id]
                if wait_for_cql:
                    await wait_for_cql_and_get_hosts(self.cql, up_servers, time() + 60)
                await with_down(s)

            await self.server_start(s.server_id, connect_driver=wait_for_cql, cmdline_options_override=cmdline_options_override)

            # Wait for other servers to see the restarted server.
            # Otherwise, the next server we are going to restart may not yet see "s" as restarted
            # and will not send graceful shutdown message to it. Server "s" may learn about the
            # restart from gossip later and close connections while we already sent CQL requests
            # to it, which will cause them to time out. Refs #14746.
            for s2 in servers_running:
                if s2.server_id != s.server_id:
                    await self.server_sees_other_server(s2.ip_addr, s.ip_addr)
        if wait_for_cql:
            await wait_for_cql_and_get_hosts(self.cql, servers_running, time() + 60)

    async def server_pause(self, server_id: ServerNum) -> None:
        """Pause the specified server."""
        logger.debug("ManagerClient pausing %s", server_id)
        await self._call(self._manager.server_pause(server_id))

    async def server_unpause(self, server_id: ServerNum) -> None:
        """Unpause the specified server."""
        logger.debug("ManagerClient unpausing %s", server_id)
        await self._call(self._manager.server_unpause(server_id))

    async def server_switch_executable(self, server_id: ServerNum, path: str) -> None:
        """Switch the executable path of a stopped server"""
        logger.debug("ManagerClient switching executable of %s to %s", server_id, path)
        await self._call(self._manager.server_switch_executable(server_id, path))

    async def server_change_version(self, server_id: ServerNum, exe: str):
        """ Upgrades a running Scylla node by switching it to a new binary version 
            specified by the 'exe' parameter.
        """
        await self.server_stop_gracefully(server_id)
        await self.server_switch_executable(server_id, exe)
        await self.server_start(server_id)

    async def server_wipe_sstables(self, server_id: ServerNum, keyspace: str, table: str) -> None:
        """Delete all files for the given table from the data directory"""
        logger.debug("ManagerClient wiping sstables on %s, keyspace=%s, table=%s", server_id, keyspace, table)
        await self._call(self._manager.server_wipe_sstables(server_id, keyspace, table))

    async def server_get_sstables_disk_usage(self, server_id: ServerNum, keyspace: str, table: str) -> int:
        """Get the total size of all sstable files for the given table"""
        return await self._call(self._manager.server_get_sstables_disk_usage(server_id, keyspace, table))

    async def _get_ignored_ip_addresses(self, ignore_dead: List[IPAddress | HostID]) -> List[IPAddress]:
        """
        Get IP addresses of nodes ignored in the replace and removenode operations.

        FIXME: Simplify the code once we disallow specifying ignored nodes through IP addresses in Scylla.
        """
        servers = await self.all_servers()
        ignored_ips = []
        for ignored in ignore_dead:
            # IPAddress and HostID are both NewType over str, so isinstance() cannot distinguish them at runtime.
            if '.' in ignored:
                ignored_ips.append(ignored)
            else:
                ignored_server = await self.find_server_by_host_id(servers, ignored)
                ignored_ips.append(ignored_server.ip_addr)
        return ignored_ips

    async def add_teardown_callback(self, callback: Callable[[], Any], name: str | None = None) -> None:
        """Register a callback to run once the cluster used by this test has
        been stopped.

        The callbacks belong to the cluster and fire from its
        run_teardown_callbacks(), which recycle() calls right after the servers
        are stopped.  They therefore run against a cluster that is down, and
        may dispose of a resource it was using without racing against it: an
        object storage bucket that an in-flight tablet migration could
        otherwise still read from (SCYLLADB-2471).

        Two consequences of firing that late are worth knowing.  The resource
        the callback disposes of has to stay alive past the fixture that
        created it, and a failure inside a callback is reported against the
        test case that recycled the cluster, not necessarily the one that
        registered the callback.

        Callbacks fire in LIFO order; both plain callables and coroutine
        functions are accepted.  Register them from a fixture rather than from
        a test body, so that a coroutine is handed back to a loop that is still
        alive when it is awaited.  `name` is what the cluster log calls this
        callback, and defaults to the callable's qualified name.
        """
        await self._call(self._manager.add_teardown_callback(
            bind_to_current_loop(callback),
            name or getattr(callback, "__qualname__", repr(callback))))

    async def server_add(self,
                         replace_cfg: Optional[ReplaceConfig] = None,
                         cmdline: Optional[List[str]] = None,
                         config: Optional[dict[str, Any]] = None,
                         version: Optional[ScyllaVersionDescription] = None,
                         property_file: Optional[dict[str, Any]] = None,
                         start: bool = True,
                         expected_error: Optional[str] = None,
                         seeds: Optional[List[IPAddress]] = None,
                         timeout: Optional[float] = ScyllaServer.TOPOLOGY_TIMEOUT,
                         server_encryption: str = "none",
                         expected_server_up_state: ServerUpState = ServerUpState.SERVING,
                         connect_driver: bool = True) -> ServerInfo:
        """Add a new server.

        When start=True and expected_error is None, waits until Scylla reports
        that all configured listeners (including any non-default ports) are ready
        (ServerUpState.SERVING). Pass a lower expected_server_up_state to return
        earlier. When start=False or expected_error is set, no readiness wait is
        performed. When connect_driver=False, the effective wait state is capped
        at HOST_ID_QUERIED regardless of expected_server_up_state."""
        if expected_error is not None:
            self.ignore_log_patterns.append(re.escape(expected_error))

        if not connect_driver:
            expected_server_up_state = min(expected_server_up_state, ServerUpState.HOST_ID_QUERIED)

        try:
            # We should wait until all running nodes see the node being replaced
            # and all ignored nodes as dead. Replace could be rejected otherwise.
            # We make this waiting default and optional to allow testing expected
            # replace failures.
            if replace_cfg and replace_cfg.wait_dead:
                replaced_ip = await self.get_host_ip(replace_cfg.replaced_id)
                ignored_ips = await self._get_ignored_ip_addresses(replace_cfg.ignore_dead_nodes)
                dead_ips = [replaced_ip] + ignored_ips
                await gather_safely(*(self.others_not_see_server(ip) for ip in dead_ips))

            s_info = await self._call(self._manager.server_add(
                replace_cfg=replace_cfg,
                cmdline=cmdline,
                config=config,
                version=version,
                property_file=property_file,
                start=start,
                seeds=seeds,
                server_encryption=server_encryption,
                expected_error=expected_error,
                expected_server_up_state=expected_server_up_state,
            ), timeout=timeout)
        except Exception as exc:
            raise Exception("Failed to add server") from exc
        logger.debug("ManagerClient added %s", s_info)
        if expected_error is None and connect_driver:
            if self.cql:
                self._driver_update()
            elif start:
                await self.driver_connect()
        return s_info

    async def servers_add(self, servers_num: int = 1,
                          cmdline: Optional[List[str]] = None,
                          config: Optional[dict[str, Any]] = None,
                          version: Optional[ScyllaVersionDescription] = None,
                          property_file: Union[List[dict[str, Any]], dict[str, Any], None] = None,
                          start: bool = True,
                          seeds: Optional[List[IPAddress]] = None,
                          driver_connect_opts: dict[str, Any] = {},
                          expected_error: Optional[str] = None,
                          server_encryption: str = "none",
                          auto_rack_dc: Optional[str] = None) -> List[ServerInfo]:
        """Add new servers concurrently.

        When start=True and expected_error is None, waits until Scylla reports
        that all configured listeners (including any non-default ports) are ready
        (ServerUpState.SERVING). When start=False or expected_error is set, no
        readiness wait is performed.

        This function can be called only if the cluster uses consistent topology changes, which support
        concurrent bootstraps. If your test does not fulfill this condition and you want to add multiple
        servers, you should use multiple server_add calls."""
        assert servers_num > 0, f"servers_add: cannot add {servers_num} servers, servers_num must be positive"
        assert not (property_file and auto_rack_dc), f"Either property_file or auto_rack_dc can be provided, but not both"

        if expected_error is not None:
            self.ignore_log_patterns.append(re.escape(expected_error))

        if auto_rack_dc:
            property_file = [{"dc":auto_rack_dc, "rack":f"rack{i+1}"} for i in range(servers_num)]

        try:
            s_infos = await self._call(self._manager.servers_add(
                servers_num=servers_num,
                cmdline=cmdline,
                config=config,
                version=version,
                property_file=property_file,
                start=start,
                seeds=seeds,
                server_encryption=server_encryption,
                expected_error=expected_error,
            ), timeout=ScyllaServer.TOPOLOGY_TIMEOUT * servers_num)
        except Exception as exc:
            raise Exception("Failed to add servers") from exc

        assert len(s_infos) == servers_num, f"servers_add requested adding {servers_num} servers but " \
                                    f"got server data about {len(s_infos)} servers: {s_infos}"
        logger.debug("ManagerClient added %s", s_infos)
        if expected_error is None:
            if self.cql:
                self._driver_update()
            elif start:
                await self.driver_connect(**driver_connect_opts)
        return s_infos

    async def remove_node(self, initiator_id: ServerNum, server_id: ServerNum,
                          ignore_dead: List[IPAddress] | List[HostID] = list[IPAddress](),
                          expected_error: str | None = None,
                          wait_dead: bool = True,
                          timeout: Optional[float] = ScyllaServer.TOPOLOGY_TIMEOUT) -> None:
        """Invoke remove node Scylla REST API for a specified server"""
        if expected_error is not None:
            self.ignore_log_patterns.append(re.escape(expected_error))

        logger.debug("ManagerClient remove node %s on initiator %s", server_id, initiator_id)

        # We should wait until all running nodes see the node being removed
        # and all ignored nodes as dead. Removenode could be rejected
        # otherwise. We make this waiting default and optional to allow testing
        # expected removenode failures.
        if wait_dead:
            removed_ip = await self.get_host_ip(server_id)
            ignored_ips = await self._get_ignored_ip_addresses(ignore_dead)
            dead_ips = [removed_ip] + ignored_ips
            await gather_safely(*(self.others_not_see_server(ip) for ip in dead_ips))

        await self._call(self._manager.remove_node(
            initiator_id, server_id, ignore_dead=ignore_dead, expected_error=expected_error,
        ), timeout=timeout)
        self._driver_update()

    async def decommission_node(self, server_id: ServerNum,
                                expected_error: str | None = None,
                                timeout: Optional[float] = ScyllaServer.TOPOLOGY_TIMEOUT) -> None:
        """Tell a node to decommission with Scylla REST API"""
        if expected_error is not None:
            self.ignore_log_patterns.append(re.escape(expected_error))

        logger.debug("ManagerClient decommission %s", server_id)
        await self._call(self._manager.decommission_node(server_id, expected_error=expected_error),
                         timeout=timeout)
        self._driver_update()

    async def rebuild_node(self, server_id: ServerNum,
                           expected_error: str | None = None,
                           timeout: Optional[float] = ScyllaServer.TOPOLOGY_TIMEOUT) -> None:
        """Tell a node to rebuild with Scylla REST API"""
        logger.debug("ManagerClient rebuild %s", server_id)
        await self._call(self._manager.rebuild_node(server_id, expected_error=expected_error),
                         timeout=timeout)
        self._driver_update()

    async def server_get_config(self, server_id: ServerNum) -> dict[str, Any]:
        return await self._call(self._manager.server_get_config(server_id))

    async def server_update_config(self,
                                   server_id: ServerNum,
                                   key: str | None = None,
                                   value: Any = None,
                                   *,
                                   config_options: dict[str, Any] | None = None) -> None:
        """
        Update the server's configuration file.

        You can update a single option by providing the (key, value) pair, or multiple options using config_options.
        """
        if key is not None:
            if value is None:
                raise RuntimeError("`value` is required if `key` is not None")
            if config_options is not None:
                raise RuntimeError("`key: value` pair and `config_options` dict can't be used simultaneously")
            config_options = {key: value}
        elif not isinstance(config_options, dict):
            raise RuntimeError(f"`config_options` is expected to be a dict, not {type(config_options)}")
        await self._call(self._manager.server_update_config(server_id, config_options))

    async def server_remove_config_option(self, server_id: ServerNum, key: str) -> None:
        """Remove the provided option from the server's configuration file."""
        await self._call(self._manager.server_remove_config_option(server_id, key))

    async def server_update_cmdline(self, server_id: ServerNum, cmdline_options: List[str]) -> None:
        await self._call(self._manager.server_update_cmdline(server_id, cmdline_options))

    async def server_change_ip(self, server_id: ServerNum) -> IPAddress:
        """Change server IP address. Applicable only to a stopped server"""
        return await self._call(self._manager.server_change_ip(server_id))

    async def server_change_rpc_address(self, server_id: ServerNum) -> IPAddress:
        """Change server RPC IP address.

        Applicable only to a stopped server.
        """
        rpc_address = await self._call(self._manager.server_change_rpc_address(server_id))
        logger.debug("ManagerClient has changed RPC IP for server %s to %s", server_id, rpc_address)
        return rpc_address

    async def wait_for_host_known(self, dst_server_ip: IPAddress, expect_host_id: HostID,
                                  deadline: Optional[float] = None) -> None:
        """Waits until dst_server_id knows about expect_host_id, with timeout"""
        async def host_is_known():
            host_id_map = await self.api.get_host_id_map(dst_server_ip)
            return True if any(entry for entry in host_id_map if entry['value'] == expect_host_id) else None

        return await wait_for(host_is_known, deadline or (time() + 30))

    async def wait_for_scylla_process_status(self,
                                             server_id: ServerNum,
                                             expected_statuses: list[str],
                                             deadline: Optional[float] = None) -> str:
        """Wait for Scylla's process status for server_id will be as expected, with timeout."""
        async def process_status_is_as_expected() -> str | None:
            current_status = await self._call(self._manager.server_get_process_status(server_id))
            if current_status in expected_statuses:
                return current_status

        return await wait_for(process_status_is_as_expected, deadline or (time() + 30))

    async def get_host_ip(self, server_id: ServerNum) -> IPAddress:
        """Get host IP Address"""
        try:
            return await self._call(self._manager.get_host_ip(server_id))
        except Exception as exc:
            raise Exception(f"Failed to get host IP address for server {server_id}") from exc

    async def get_host_id(self, server_id: ServerNum) -> HostID:
        """Get local host id of a server"""
        try:
            return await self._call(self._manager.get_host_id(server_id))
        except Exception as exc:
            raise Exception(f"Failed to get local host id address for server {server_id}") from exc

    async def get_table_id(self, keyspace: str, table: str):
        rows = await self.cql.run_async(f"select id from system_schema.tables where keyspace_name = '{keyspace}' and table_name = '{table}'")
        return rows[0].id

    async def get_view_id(self, keyspace: str, view: str):
        rows = await self.cql.run_async(f"select id from system_schema.views where keyspace_name = '{keyspace}' and view_name = '{view}'")
        return rows[0].id

    async def get_table_or_view_id(self, keyspace: str, table: str):
        rows = await self.cql.run_async(f"select id from system_schema.tables where keyspace_name = '{keyspace}' and table_name = '{table}'")
        if len(rows) > 0:
            return rows[0].id
        rows = await self.cql.run_async(f"select id from system_schema.views where keyspace_name = '{keyspace}' and view_name = '{table}'")
        return rows[0].id

    async def server_sees_others(self, server_id: ServerNum, count: int, interval: float = 45.):
        """Wait till a server sees a minimum given count of other servers"""
        if count < 1:
            return
        server_ip = await self.get_host_ip(server_id)
        async def _sees_min_others():
            alive_nodes = await self.api.get_alive_endpoints(server_ip)
            if len(alive_nodes) > count:
                return True
        await wait_for(_sees_min_others, time() + interval, period=.5)

    async def server_sees_other_server(self, server_ip: IPAddress, other_ip: IPAddress,
                                       interval: float = 45.):
        """Wait till a server sees another specific server IP as alive"""
        async def _sees_another_server():
            alive_nodes = await self.api.get_alive_endpoints(server_ip)
            if other_ip in alive_nodes:
                return True
        await wait_for(_sees_another_server, time() + interval, period=.5)

    async def servers_see_each_other(self, servers: List[ServerInfo], interval: float = 45.):
        """Wait till all servers see all other servers in the list"""
        others = [self.server_sees_others(srv.server_id, len(servers) - 1, interval) for srv in servers]
        await gather_safely(*others)

    async def server_not_sees_other_server(self, server_ip: IPAddress, other_ip: IPAddress,
                                           interval: float = 45.):
        """Wait till a server sees another specific server IP as dead"""
        async def _not_sees_another_server():
            alive_nodes = await self.api.get_alive_endpoints(server_ip)
            if not other_ip in alive_nodes:
                return True
        await wait_for(_not_sees_another_server, time() + interval, period=.5)

    async def others_not_see_server(self, server_ip: IPAddress, interval: float = 45.):
        """Wait till a server is seen as dead by all other running servers in the cluster"""
        others_ips = [srv.ip_addr for srv in await self.running_servers() if srv.ip_addr != server_ip]
        await gather_safely(*(self.server_not_sees_other_server(ip, server_ip, interval) for ip in others_ips))

    async def server_open_log(self, server_id: ServerNum) -> ScyllaLogFile:
        logger.debug("ManagerClient getting log filename for %s", server_id)
        log_filename = await self._call(self._manager.server_get_log_filename(server_id))
        return ScyllaLogFile(self.thread_pool, log_filename)

    async def server_get_workdir(self, server_id: ServerNum) -> str:
        return await self._call(self._manager.server_get_workdir(server_id))

    async def server_get_maintenance_socket_path(self, server_id: ServerNum) -> str:
        return await self._call(self._manager.server_get_maintenance_socket_path(server_id))

    async def server_get_exe(self, server_id: ServerNum) -> str:
        return await self._call(self._manager.server_get_exe(server_id))

    async def server_get_returncode(self, server_id: ServerNum) -> int | None:
        match await self._call(self._manager.server_get_returncode(server_id)):
            case "NO_SUCH_PROCESS":
                raise NoSuchProcess(f"No process found for {server_id=}")
            case "RUNNING":
                return None
            case returncode:
                return int(returncode)
