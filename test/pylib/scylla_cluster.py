#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Scylla clusters for testing.
   A cluster of Scylla servers owned by one test (or one module), and the
   async API the test uses to change it.
"""
import asyncio
import copy
import importlib
import inspect
import logging
import pathlib
import re
import shutil
import ssl
import threading
import traceback
import uuid
from collections import ChainMap, defaultdict
from collections.abc import AsyncIterator, Awaitable, Callable
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import asynccontextmanager
from functools import reduce, wraps
from time import time
from typing import Any, Concatenate, NamedTuple, cast, overload

import allure
import psutil
from cassandra import ConsistencyLevel
from cassandra.auth import AuthProvider
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT,
    Cluster as CassandraCluster,
    ExecutionProfile,
    Session as CassandraSession,
)
from cassandra.connection import EndPoint
from cassandra.policies import (
    ExponentialReconnectionPolicy,
    LoadBalancingPolicy,
    RoundRobinPolicy,
    TokenAwarePolicy,
    WhiteListRoundRobinPolicy,
)
from cassandra.pool import Host as CassandraHost

from test.pylib.driver_utils import control_connection_query_fallback_options, safe_driver_shutdown
from test.pylib.host_registry import Host, HostRegistry
from test.pylib.internal_types import ServerNum, IPAddress, HostID, ServerInfo, ServerUpState, SeastarIOMetricName
from test.pylib.log_browsing import ScyllaLogFile
from test.pylib.rest_client import HTTPError, ScyllaMetricsClient, ScyllaRESTAPIClient
from test.pylib.scylla_server import (
    SCYLLA_CMDLINE_OPTIONS,
    ScyllaServer,
    ScyllaVersionDescription,
    get_current_version_description,
    make_scylla_conf,
    merge_cmdline_options,
)
from test.pylib.util import (
    gather_safely,
    graceful_stop_timeout,
    wait_for,
    wait_for_cql_and_get_hosts,
)


class ReplaceConfig(NamedTuple):
    replaced_id: ServerNum
    reuse_ip_addr: bool
    use_host_id: bool
    ignore_dead_nodes: list[IPAddress | HostID] = []
    wait_dead: bool = True


conn_logger = logging.getLogger("conn_messages")
conn_logger.setLevel(logging.INFO)


class CustomConnection(CassandraCluster.connection_class):
    """Driver connection which logs every message, for debugging."""

    def send_msg(self, *args, **argv):
        conn_logger.debug("send_msg: (%s): %s %s", id(self), args, argv)
        return super().send_msg(*args, **argv)

    def process_msg(self, msg, protocol_version):
        conn_logger.debug("process_msg: (%s): %s", id(self), msg)
        return super().process_msg(msg, protocol_version)


# Cap on a cluster operation, the same ceiling the HTTP client used to apply
# to every request.  An op whose caller controls the timeout is decorated with
# cluster_op(timeout=None) and wrapped in asyncio.timeout() on the caller's
# side of the bridge instead.
DEFAULT_OP_TIMEOUT = 300


type ClusterFn[**P, R] = Callable[Concatenate[ScyllaCluster, P], R]
type ClusterAsyncFn[**P, R] = ClusterFn[P, Awaitable[R]]


@overload
def cluster_op[**P, R](entry_point: ClusterAsyncFn[P, R]) -> ClusterAsyncFn[P, R]: ...
@overload
def cluster_op[**P, R](*,
                       timeout: float | None = ...) -> Callable[[ClusterAsyncFn[P, R]], ClusterAsyncFn[P, R]]: ...
def cluster_op[**P, R](entry_point: ClusterAsyncFn[P, R] | None = None,
                       *,
                       timeout: float | None = DEFAULT_OP_TIMEOUT,
                       ) -> ClusterAsyncFn[P, R] | Callable[[ClusterAsyncFn[P, R]], ClusterAsyncFn[P, R]]:
    """Run a ScyllaCluster entry point on the cluster's loop, as its own tracked task.

    Usable bare, as @cluster_op, or with arguments, as @cluster_op(timeout=600).

    The cluster's state -- subprocess transports, per-server locks -- belongs to
    its loop, so an operation always runs there, whichever loop or thread calls
    it.  The caller waits across the bridge under `timeout`; None means no cap,
    for operations whose caller applies its own asyncio.timeout().

    A waiter that times out or is cancelled abandons the operation instead of
    killing it: the operation is shielded, stays in tasks_history and
    after_test() drains it from there.  Failures are logged with their
    traceback through the cluster's logger.

    No entry point works on a stopped cluster, so that is asserted here.
    """
    def decorator(fn: ClusterAsyncFn[P, R]) -> ClusterAsyncFn[P, R]:
        # The bridge does not consume a timeout argument; a timeout is either
        # this decorator's cap or the caller's own asyncio.timeout() around
        # the call.  Fail loudly on the old pattern.
        if "timeout" in inspect.signature(fn).parameters and timeout is not None:
            raise TypeError(f"{fn.__name__} declares its own timeout parameter;"
                            f" decorate it with cluster_op(timeout=None) and apply"
                            f" the timeout on the caller's side")

        async def run_op(self: ScyllaCluster, *args: P.args, **kwargs: P.kwargs) -> R:
            """The operation itself; always runs on the cluster's loop."""
            assert self.is_running, "ScyllaCluster is stopped"
            descr = f"{fn.__name__}{args}"
            op = asyncio.ensure_future(fn(self, *args, **kwargs))

            def op_finished(task: asyncio.Task) -> None:
                """Drop the finished operation from tasks_history and log its
                failure, if any.  Retrieving the exception here also keeps an
                operation orphaned by a cancelled caller from warning
                "exception was never retrieved".
                """
                self.tasks_history.pop(task, None)
                if task.cancelled():
                    return
                exc = task.exception()
                if exc is not None:
                    self.logger.error("Exception when executing %s: %s\n%s",
                                      descr, exc, "".join(traceback.format_exception(exc)))

            self.logger.info("[ScyllaCluster][%s] %s", op.get_name(), descr)
            self.tasks_history[op] = descr
            op.add_done_callback(op_finished)
            return await asyncio.shield(op)

        @wraps(fn)
        async def wrapper(self: ScyllaCluster, *args: P.args, **kwargs: P.kwargs) -> R:
            assert self._loop is not None, "ScyllaCluster is not running on its thread"
            if asyncio.get_running_loop() is self._loop:
                # Already on the cluster's loop, i.e. one operation calling
                # another, as server_pause() does through its process-status
                # wait.  There is nothing to hand over, so call in place.  The
                # cap goes with the bridge: it bounds how long a caller waits
                # on the far side, and here there is no far side.
                return await run_op(self, *args, **kwargs)
            future = asyncio.run_coroutine_threadsafe(run_op(self, *args, **kwargs), self._loop)
            async with asyncio.timeout(timeout):    # None = no cap
                return await asyncio.wrap_future(future)
        # The bridge adds no parameters, so the decorated entry point keeps
        # its exact signature.
        return wrapper
    return decorator if entry_point is None else decorator(entry_point)


class ScyllaCluster:
    """A cluster of Scylla servers for one test (or one module), and the test's API on it.

    Parallel requests are not supported.

    test/cluster runs the cluster on its own thread for the test's duration
    (run_in_thread()), which stops the servers when it ends; recycle() then
    disposes of the cluster.  after_test() drains what the test left running
    and reports what it leaked.  The module-scoped cluster of the
    single-server suites (cqlpy, alternator, scylla_gdb, ...) never starts
    the thread: its fixture adds the one server with add_server() on its own
    loop.

    A method that awaits cluster or server internals must be a @cluster_op --
    those objects belong to the cluster's loop -- and the CQL driver, the REST
    helpers and anything composing operations must not be.  The exceptions are
    add_server() and _stop_server(), which operations call; the module-scoped
    fixtures also call add_server(), on their own loop.  stop() is one as
    well: it runs on the cluster's thread as run_in_thread() exits, or inside
    recycle() for a module-scoped cluster, and the rest of recycle() awaits no
    cluster internals.  An operation body runs entirely on the cluster's loop,
    so an entry point needing caller-side work too (the caller's timeout, a
    driver refresh, a gossip wait) is a private _x operation behind a public
    wrapper holding that work.

    Args:
        test_name: unique name of the test case (or module) the cluster
            serves, e.g. test_topology.1::test_add_server_add_column
    """
    # pylint: disable=too-many-instance-attributes

    def __init__(self,
                 test_name: str,
                 logger: logging.Logger | logging.LoggerAdapter,
                 vardir: pathlib.Path,
                 mode: str,
                 cmdline_options: str | list[str],
                 cmdline_options_override: list[str],
                 config_options: dict[str, Any],
                 append_env: dict[str, str],
                 scylla_exe: str,
                 save_log_on_success: bool = False) -> None:
        self.test_name = test_name
        self.logger = logger
        self.vardir = vardir
        self.mode = mode
        self.cmdline_options = [cmdline_options] if isinstance(cmdline_options, str) else cmdline_options
        self.cmdline_options_override = cmdline_options_override
        self.config_options = config_options
        self.append_env = append_env
        self.scylla_exe = scylla_exe
        self.save_log_on_success = save_log_on_success
        self.host_registry = HostRegistry()
        self.leased_ips = set[IPAddress]()
        self.name = str(uuid.uuid1())
        # Every ScyllaServer is in one of self.running, self.stopped.
        # These dicts are disjoint.
        # A server ID present in self.removed may be either in self.running or in self.stopped.
        self.running: dict[ServerNum, ScyllaServer] = {}        # started servers
        self.stopped: dict[ServerNum, ScyllaServer] = {}        # servers no longer running but present
        self.servers = ChainMap(self.running, self.stopped)
        self.removed: set[ServerNum] = set()                    # removed servers (might be running)
        self.starting: dict[ServerNum, ScyllaServer] = {}       # servers starting right now, not yet running (and not included in "servers").
        # The first IP assigned to a server added to the cluster.
        self.initial_seed: IPAddress | None = None
        # cluster is started (but it might not have running servers);
        # cleared by stop(), which is a no-op when it is False
        self.is_running: bool = True
        self.api = ScyllaRESTAPIClient(build_mode=self.mode)
        self.metrics = ScyllaMetricsClient()

        # Driver options; test/cluster's manager fixture sets them from the command line.
        self.port = 9042
        self.use_ssl = False
        self.auth_provider: AuthProvider | None = None
        self.thread_pool = ThreadPoolExecutor()
        self.ccluster: CassandraCluster | None = None
        self.cql: CassandraSession | None = None
        self.exclusive_clusters: list[CassandraCluster] = []
        self.ignore_log_patterns: list[str] = []  # patterns to ignore in server logs when checking for errors
        self.ignore_cores_log_patterns: list[str] = []  # patterns to ignore in server logs when checking for core files

        # The cluster's own loop, claimed by run_in_thread(): its operations
        # run there (see cluster_op), whichever loop or thread they are
        # called from.
        self._loop: asyncio.AbstractEventLoop | None = None

        # tasks_history holds the running test's operations: after_test()
        # drains it, and whatever is left over is the leak it reports.
        self.tasks_history: dict[asyncio.Task, str] = {}

        self.logger.info("Created new cluster %s", self.name)

    def repr_tasks_history(self) -> str:
        out = "Cluster_history"
        for key, val in self.tasks_history.items():
            out += f"\n{val}:\t\t{repr(key)}"
        return out

    async def recycle(self) -> None:
        """Dispose of the cluster once it's no longer needed: stop it, close
           the server log files and sockets and release the used IPs.  The
           servers' directories and log files stay.
        """
        await self.stop()
        for srv in self.servers.values():
            if srv.log_file is not None:
                srv.log_file.close()
            srv.maintenance_socket_dir.cleanup()

        # Close API client to release connector resources
        if self.api is not None:
            self.api.close()
            self.api = None
        await gather_safely(*(self.host_registry.release_host(Host(ip)) for ip in self.leased_ips))
        self.leased_ips.clear()

    async def stop(self) -> None:
        """Close the driver, shut down the thread pool and stop all running servers ASAP.

        Only the first call does anything.  This is the loop-bound part of the
        disposal: the server subprocess transports belong to the cluster's
        loop, so they have to be awaited there.  run_in_thread() calls it as
        it exits; recycle() calls it too, then disposes of the rest.
        """
        if self.is_running:
            self.is_running = False
            self.logger.info("Cluster %s stopping after %s", self, self.test_name)
            self.driver_close()
            self.thread_pool.shutdown(wait=False)
            # If self.running is empty, no-op
            await gather_safely(*(server.stop() for server in self.running.values()))
            self.stopped.update(self.running)
            self.running.clear()

    @asynccontextmanager
    async def run_in_thread(self) -> AsyncIterator[ScyllaCluster]:
        """Run this cluster on its own thread and event loop.

        The cluster owns loop-bound state -- the Scylla subprocess transports
        and the per-server asyncio locks -- so all of its coroutines have to
        run on one loop, and that loop has to keep running: tests reach the
        cluster from pytest's event loops and, in dtest, from plain worker
        threads.  Hence, a dedicated thread whose loop is idle but alive
        until the context exits.
        """
        ready: Future[None] = Future()
        stop_event = threading.Event()

        async def run_cluster() -> None:
            self.logger.info("Setting up %s", self.test_name)
            self._loop = asyncio.get_running_loop()
            ready.set_result(None)
            try:
                await asyncio.get_running_loop().run_in_executor(None, stop_event.wait)
            finally:
                await self.stop()

        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="scylla-cluster") as executor:
            future = executor.submit(asyncio.run, run_cluster())
            # Fail instead of waiting forever when the thread dies before it
            # signals readiness.  The callback fires only once the future is
            # done, so reaching it without a result always means a startup
            # failure.
            future.add_done_callback(
                lambda f: None if ready.done() else ready.set_exception(
                    f.exception() or RuntimeError("ScyllaCluster thread exited before signaling readiness")))
            # ready.result() blocks, so hand it to a thread rather than
            # stalling the caller's loop.
            await asyncio.get_running_loop().run_in_executor(None, ready.result)
            try:
                yield self
            finally:
                stop_event.set()
                # Wait for the cluster thread off the event loop.
                try:
                    await asyncio.get_running_loop().run_in_executor(None, future.result)
                finally:
                    # The loop is gone with the thread; let cluster_op's
                    # not-running assert catch a leaked caller instead of a
                    # cryptic "Event loop is closed".
                    self._loop = None

    def _seeds(self) -> list[IPAddress]:
        # If the cluster is empty, all servers must use self.initial_seed to not start separate clusters.
        if not self.running:
            return [self.initial_seed] if self.initial_seed else []
        return [server.ip_addr for server in self.running.values()]

    async def add_server(self,
                         replace_cfg: ReplaceConfig | None = None,
                         cmdline: list[str] | None = None,
                         config: dict[str, Any] | None = None,
                         version: ScyllaVersionDescription | None = None,
                         property_file: dict[str, Any] | None = None,
                         start: bool = True,
                         seeds: list[IPAddress] | None = None,
                         server_encryption: str = "none",
                         expected_error: str | None = None,
                         expected_server_up_state: ServerUpState = ServerUpState.SERVING,
                         launcher: list[str] | None = None) -> ServerInfo:
        """Add a new server to the cluster"""

        assert start or not expected_error, \
            f"add_server: cannot add a stopped server and expect an error"

        # Deep copy: the test keeps the dict (and _servers_add() shares it between
        # servers), while ScyllaServer.__init__ writes to it and the server keeps
        # mutating nested values (e.g. change_seeds()).
        extra_config: dict[str, Any] = copy.deepcopy(config) if config else {}
        if replace_cfg:
            replaced_id = replace_cfg.replaced_id
            assert expected_error or replaced_id in self.servers, \
                f"add_server: replaced id {replaced_id} not found in existing servers"

            replaced_srv = self.servers[replaced_id]
            if replace_cfg.use_host_id:
                extra_config['replace_node_first_boot'] = await replaced_srv.get_host_id(self.api)
            else:
                extra_config['replace_address_first_boot'] = replaced_srv.ip_addr

            if replace_cfg.ignore_dead_nodes:
                extra_config['ignore_dead_nodes_for_replace'] = ','.join(replace_cfg.ignore_dead_nodes)

            assert expected_error or replaced_id not in self.removed, \
                f"add_server: cannot replace removed server {replaced_srv}"
            assert expected_error or replaced_id in self.stopped, \
                f"add_server: cannot replace running server {replaced_srv}"

        if replace_cfg and replace_cfg.reuse_ip_addr:
            ip_addr = replaced_srv.ip_addr
        else:
            self.logger.info("Cluster %s waiting for new IP...", self.name)
            ip_addr = IPAddress(await self.host_registry.lease_host())
            self.logger.info("Cluster %s obtained new IP: %s", self.name, ip_addr)
            self.leased_ips.add(ip_addr)

        if not self.initial_seed and not expected_error and start:
            self.initial_seed = ip_addr

        if not seeds:
            seeds = self._seeds()
            if not seeds:
                seeds = [ip_addr]

        server = None

        async def handle_join_failure():
            if not replace_cfg or not replace_cfg.reuse_ip_addr:
                self.leased_ips.remove(ip_addr)
                await self.host_registry.release_host(Host(ip_addr))
            # Assembling the configuration below can fail before there is a
            # server to remember; releasing the host is all that is left to do.
            if server is not None:
                self.stopped[server.server_id] = server

        try:
            if version is None:
                version = get_current_version_description(self.scylla_exe)

            # Every source of cmdline options in one chain, in increasing
            # order of priority.
            cmdline_options = reduce(merge_cmdline_options, [
                SCYLLA_CMDLINE_OPTIONS,
                version.argv,
                self.cmdline_options,
                cmdline or [],
                self.cmdline_options_override,
            ])

            # Sum of the basic server configuration and the user-provided
            # config options, with increasing priority (if two sources provide
            # the same option, the higher priority one wins):
            # 1. the defaults
            # 2. version-specific options
            # 3. cluster-wide options (the suite's "extra_scylla_config_options")
            # 4. options from the test (when servers are added during a test)
            config_options = make_scylla_conf(
                mode=self.mode,
                host_addr=ip_addr,
                seed_addrs=seeds,
                cluster_name=self.name,
                server_encryption=server_encryption,
            ) | version.config | self.config_options | extra_config

            # make_scylla_conf() names no snitch of its own, so a snitch here
            # is one the version, the suite or the test asked for.
            if property_file and "endpoint_snitch" not in config_options:
                config_options["endpoint_snitch"] = "GossipingPropertyFileSnitch"

            server = ScyllaServer(
                logger=self.logger,
                vardir=self.vardir,
                version=version,
                cmdline_options=cmdline_options,
                config_options=config_options,
                property_file=property_file,
                append_env=self.append_env,
                launcher=launcher,
            )
            server._per_server_cmdline_options = list(cmdline or [])
            self.starting[server.server_id] = server
            self.logger.info("Cluster %s adding server...", self)
            if start:
                await server.install_and_start(self.api, expected_error, expected_server_up_state)
            else:
                await server.install()
        except asyncio.CancelledError:
            if server is not None:
                # Cancelled mid-start, e.g. by the cluster's loop shutting down
                # on an interrupted run, possibly after the cluster's stop()
                # already ran: kill the process here, and keep the server in
                # the cluster so recycle() releases its IP and its files are
                # deleted with the cluster's.
                if server.cmd is not None and server.cmd.returncode is None:
                    try:
                        server.cmd.kill()
                    except ProcessLookupError:
                        pass
                self.running[server.server_id] = server
            raise
        except Exception as exc:
            workdir = '<unknown>' if server is None else server.workdir.name
            self.logger.error("Failed to start Scylla server at host %s in %s: %s",
                          ip_addr, workdir, str(exc))
            await handle_join_failure()
            raise
        finally:
            # _stop_server() may have already removed the starting server
            # if it interrupted this add_server() operation.
            if server is not None:
                self.starting.pop(server.server_id, None)

        if expected_error:
            await handle_join_failure()
        else:
            if start:
                self.running[server.server_id] = server
            else:
                self.stopped[server.server_id] = server
            self.logger.info("Cluster %s added %s", self, server)

        return server.server_info()

    def endpoint(self) -> str:
        """Get a server id (IP) from running servers"""
        return next(server.ip_addr for server in self.running.values())

    def take_log_savepoint(self) -> None:
        """Save the log size on all running servers"""
        for server in self.running.values():
            server.take_log_savepoint()

    def __str__(self):
        running = ", ".join(str(server) for server in self.running.values())
        stopped = ", ".join(str(server) for server in self.stopped.values())
        return f"ScyllaCluster(name: {self.name}, running: {running}, stopped: {stopped})"

    def server(self, server_id: ServerNum) -> ScyllaServer:
        """Get the given server, running or stopped.
           Fails if the server cannot be found."""
        assert server_id in self.servers, f"Server {server_id} unknown"
        return self.servers[server_id]

    def _running_server_infos(self) -> list[ServerInfo]:
        return [server.server_info() for server in self.running.values()
                if server.server_id not in self.removed]

    @cluster_op
    async def running_servers(self) -> list[ServerInfo]:
        """Return server info for running servers (and not removed)."""

        return self._running_server_infos()

    @cluster_op
    async def all_servers(self) -> list[ServerInfo]:
        """Return server info for all servers."""

        return [server.server_info() for server in self.servers.values()]

    @cluster_op
    async def starting_servers(self) -> list[ServerInfo]:
        """Return server info for servers which are currently starting (not yet running)."""

        return [server.server_info() for server in self.starting.values()]

    @cluster_op
    async def get_host_ip(self, server_id: ServerNum) -> IPAddress:
        """IP address of a server."""

        try:
            return self.servers[server_id].ip_addr
        except Exception as exc:
            raise RuntimeError(f"Failed to get host IP address for server {server_id}") from exc

    @cluster_op
    async def get_host_id(self, server_id: ServerNum) -> HostID:
        """Host ID of a server."""

        try:
            return await self.servers[server_id].get_host_id(self.api)
        except Exception as exc:
            raise RuntimeError(f"Failed to get local host id address for server {server_id}") from exc

    # A wedged or paused server still counts as running, and the REST client's
    # default budget is 300s -- five minutes of teardown per such server.  Cap
    # the scrape instead, generously enough that a debug-mode node rendering
    # /metrics for many shards is not dropped from the sum.
    SEASTAR_IO_SCRAPE_TIMEOUT = 15.0

    async def _scrape_seastar_io(self) -> dict[str, int]:
        """Sum the reactor IO counters of every running server, across shards.

        The cluster is created per test, so the counters start at zero with it
        and their current value is this test's total -- no baseline needed.

        Best effort, erring towards an undercount rather than charging a test
        for IO it did not do: a server the test stopped takes its counters
        with it, and one that does not answer the scrape is left out.
        """
        totals: dict[str, int] = dict.fromkeys(SeastarIOMetricName, 0)
        for srv in self._running_server_infos():
            try:
                metrics = await self.metrics.query(srv.ip_addr,
                                                   timeout=self.SEASTAR_IO_SCRAPE_TIMEOUT)
            except Exception as e:
                self.logger.debug("Could not scrape IO metrics from %s: %s", srv.ip_addr, e)
                continue
            for name in SeastarIOMetricName:
                totals[name] += int(metrics.get(name) or 0)
        return totals

    # timeout=None: the drain below carries its own budget, which scales with
    # the build mode and so can exceed DEFAULT_OP_TIMEOUT.  A bridge cap
    # shorter than the drain would abandon the cleanup half-way through.
    @cluster_op(timeout=None)
    async def after_test(self, success: bool, gather_io: bool = True) -> dict[str, Any]:
        """After-test hook of the manager fixture.

        Drains the operations the test left running and returns what the test
        leaked, plus this test's IO totals unless gather_io says otherwise.
        """
        tasks_leaked = ""
        self.logger.info(self.repr_tasks_history())
        # Drop our own entry: the drain below must not wait for itself.
        current = asyncio.current_task()
        assert current is not None  # ops always run as a task (see cluster_op)
        self.tasks_history.pop(current, None)
        # Wait for the operations the test left running.  The budget has to
        # outlast the longest of them, because asyncio.wait_for() cancels the
        # task when it expires -- and the task here is the operation itself,
        # not the shield the caller awaited.  The longest is a graceful stop,
        # and cancelling one mid-flight is not merely a lost result: the
        # CancelledError skips stop_gracefully()'s SIGKILL path while its
        # finally clause still clears self.cmd, disowning a process that is
        # still shutting down, after which stop() finds nothing left to kill.
        drain_timeout = graceful_stop_timeout(self.mode) + 60
        for task, descr in list(self.tasks_history.items()):
            if not task.done():
                self.logger.info("wait for task:%s, op:%s", task, descr)
                try:
                    await asyncio.wait_for(task, timeout=drain_timeout)
                except asyncio.TimeoutError:
                    tasks_leaked = f"timed out waiting for coro {task.get_name()}"
                    break
                except Exception:
                    # The operation failed after its caller went away.  It was
                    # already logged by op_finished; completing is all the
                    # drain needs.
                    pass

        # check on tasks leakage: finished operations remove themselves from
        # tasks_history, so anything left after the drain is a new operation
        # started behind our back while the test was already over.
        await asyncio.sleep(0.1)
        if not tasks_leaked and self.tasks_history:
            tasks_leaked = f"tasks leakage found  {self.tasks_history}"
        if tasks_leaked:
            self.logger.error("%s, test case %s", tasks_leaked, self.test_name)
        self.logger.info("Test %s %s, cluster: %s", self.test_name, "SUCCEEDED" if success else "FAILED", self)

        # Scrape before the fixture tears the cluster down: stopped servers
        # take their counters with them.  Skipped under --no-gather-metrics,
        # which is there to keep measurement off the test's critical path.
        seastar_io = await self._scrape_seastar_io() if gather_io else None

        return {
            "cluster_str": str(self),
            "tasks_leaked": bool(tasks_leaked),
            "message": tasks_leaked,
            "seastar_io": seastar_io,
        }

    async def _stop_server(self, server_id: ServerNum, gracefully: bool) -> None:
        """Stop a server. No-op if already stopped."""
        self.logger.info("Cluster %s stopping server %s", self, server_id)
        if server_id in self.stopped:
            return
        assert server_id in self.running or server_id in self.starting, f"Server {server_id} unknown"
        if server_id in self.running:
            server = self.running[server_id]
        else:
            server = self.starting[server_id]
        # Remove the server from `running` only after we successfully stop it.
        # Stopping may fail and if we removed it from `running` now it might leak.
        if gracefully:
            await server.stop_gracefully(graceful_stop_timeout(self.mode))
        else:
            await server.stop()
        if server_id in self.running:
            self.running.pop(server_id)
            self.stopped[server_id] = server
        else:
            # Starting servers are removed from self.starting by add_server()
            # in its cleanup path. This is a fallback if _stop_server() wins the race.
            self.starting.pop(server_id, None)

    @cluster_op
    async def _server_stop(self, server_id: ServerNum) -> None:
        """Stop a server. No-op if already stopped."""

        await self._stop_server(server_id, gracefully=False)

    @cluster_op(timeout=None)
    async def _server_stop_gracefully(self, server_id: ServerNum) -> None:
        await self._stop_server(server_id, gracefully=True)

    @cluster_op(timeout=None)
    async def _server_start(self,
                            server_id: ServerNum,
                            *,
                            expected_error: str | None = None,
                            seeds: list[IPAddress] | None = None,
                            expected_server_up_state: ServerUpState = ServerUpState.SERVING,
                            cmdline_options_override: list[str] | None = None,
                            append_env_override: dict[str, str] | None = None,
                            auth_provider: dict[str, str] | None = None,
                            replace_cmdline_options: bool = False) -> None:
        """Start a server.

        `cmdline_options_override` is merged into the server's command line for
        this start only and `append_env_override` replaces its environment, see
        `ScyllaServer.start()`.

        No-op if already running.
        """
        if server_id in self.running:
            return
        assert server_id in self.stopped, f"Server {server_id} unknown"
        server = self.stopped.pop(server_id)
        self.logger.info("Cluster %s starting server %s ip %s", self,
                         server_id, server.ip_addr)
        if not seeds:
            seeds = self._seeds()
            if not seeds:
                seeds = [server.ip_addr]
        server.change_seeds(seeds)
        # Put the server in `running` before starting it.
        # Starting may fail and if we didn't add it now it might leak.
        self.running[server_id] = server

        def instance_auth_provider(desc: dict):
            module_path, class_name = desc["authenticator"].rsplit('.', 1)
            module = importlib.import_module(module_path)
            auth_class = getattr(module, class_name)
            return auth_class(**desc["kwargs"])

        if auth_provider is not None:
            server.auth_provider = instance_auth_provider(auth_provider)
        await server.start(
            api=self.api,
            expected_error=expected_error,
            expected_server_up_state=expected_server_up_state,
            cmdline_options_override=cmdline_options_override,
            append_env_override=append_env_override,
            replace_cmdline_options=replace_cmdline_options,
        )
        if expected_error is not None:
            self.running.pop(server_id)
            self.stopped[server_id] = server

    @cluster_op
    async def server_pause(self, server_id: ServerNum) -> None:
        """Pause the specified server and wait until its process is stopped."""

        self.logger.info("Cluster %s pausing server %s", self.name, server_id)
        assert server_id in self.running
        server = self.running[server_id]
        server.pause()
        await self.wait_for_scylla_process_status(server_id, [psutil.STATUS_STOPPED])

    @cluster_op
    async def server_unpause(self, server_id: ServerNum) -> None:
        """Unpause the specified server and wait until its process is no longer stopped."""

        self.logger.info("Cluster %s unpausing server %s", self.name, server_id)
        assert server_id in self.running
        server = self.running[server_id]
        server.unpause()

        async def process_is_not_stopped() -> bool | None:
            status = await self.server_get_process_status(server_id)
            return True if status != psutil.STATUS_STOPPED else None

        await wait_for(process_is_not_stopped, time() + 30)

    @cluster_op(timeout=None)
    async def _server_add(self,
                          replace_cfg: ReplaceConfig | None = None,
                          cmdline: list[str] | None = None,
                          config: dict[str, Any] | None = None,
                          version: ScyllaVersionDescription | None = None,
                          property_file: dict[str, Any] | None = None,
                          start: bool = True,
                          seeds: list[IPAddress] | None = None,
                          server_encryption: str = "none",
                          expected_error: str | None = None,
                          expected_server_up_state: ServerUpState = ServerUpState.SERVING,
                          launcher: list[str] | None = None) -> ServerInfo:
        """Add a new server."""

        return await self.add_server(
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
            launcher=launcher,
        )

    @cluster_op(timeout=None)
    async def _servers_add(self,
                           servers_num: int = 1,
                           cmdline: list[str] | None = None,
                           config: dict[str, Any] | None = None,
                           version: ScyllaVersionDescription | None = None,
                           property_file: list[dict[str, Any]] | dict[str, Any] | None = None,
                           start: bool = True,
                           seeds: list[IPAddress] | None = None,
                           server_encryption: str = "none",
                           expected_error: str | None = None) -> list[ServerInfo]:
        """Add new servers concurrently."""
        assert servers_num > 0, f"_servers_add: cannot add {servers_num} servers"

        def get_property_file(i) -> dict[str, Any] | None:
            if property_file is None:
                return None
            elif type(property_file) is dict:
                return property_file
            else:
                assert type(property_file) is list and len(property_file) == servers_num
                return property_file[i]

        return await gather_safely(*(self.add_server(None, cmdline, config, version, get_property_file(i), start, seeds, server_encryption, expected_error)
                                      for i in range(servers_num)))

    @cluster_op(timeout=None)
    async def _remove_node(self,
                           initiator_id: ServerNum,
                           server_id: ServerNum,
                           ignore_dead: list[IPAddress | HostID],
                           expected_error: str | None) -> None:
        """Run remove node on Scylla REST API for a specified server."""

        assert initiator_id in self.running, f"Initiator {initiator_id} is not running"
        if server_id in self.running:
            self.logger.warning("remove_node %s is a running node", server_id)
        else:
            assert server_id in self.stopped, f"remove_node: {server_id} unknown"
        to_remove = self.servers[server_id]
        initiator = self.servers[initiator_id]
        self.logger.info("remove_node %s with initiator %s", to_remove, initiator)

        details = f"initiator: {initiator}, to_remove: {to_remove}, ignore_dead: {ignore_dead}"
        try:
            await self.api.remove_node(initiator.ip_addr,
                                       await to_remove.get_host_id(self.api),
                                       ignore_dead,
                                       timeout=ScyllaServer.TOPOLOGY_TIMEOUT)
        except (RuntimeError, HTTPError) as exc:
            self._check_failure_expected("removenode", details, initiator.log_filename, exc, expected_error)
            return
        self.logger.debug("Cluster %s marking server %s as removed", self, server_id)
        self.removed.add(server_id)
        self._check_success_expected("removenode", details, initiator.log_filename, expected_error)

    @cluster_op(timeout=None)
    async def _decommission_node(self, server_id: ServerNum, expected_error: str | None) -> None:
        """Run decommission node on Scylla REST API for a specified server."""

        self.logger.info("decommission_node %s", server_id)
        assert server_id in self.running, "Can't decommission not running node"
        if len(self.running) == 1:
            self.logger.warning("decommission_node %s is only running node left", server_id)
        server = self.running[server_id]
        details = f"server: {server}"
        try:
            await self.api.decommission_node(server.ip_addr, timeout=ScyllaServer.TOPOLOGY_TIMEOUT)
        except (RuntimeError, HTTPError) as exc:
            self._check_failure_expected("decommission", details, server.log_filename, exc, expected_error)
            return
        await self._stop_server(server_id, gracefully=True)
        self._check_success_expected("decommission", details, server.log_filename, expected_error)

    @cluster_op(timeout=None)
    async def _rebuild_node(self, server_id: ServerNum, expected_error: str | None) -> None:
        """Run rebuild node on Scylla REST API for a specified server."""

        self.logger.info("rebuild_node %s", server_id)
        assert server_id in self.running, "Can't rebuild not running node"
        server = self.running[server_id]
        details = f"server: {server}"
        try:
            await self.api.rebuild_node(server.ip_addr, timeout=ScyllaServer.TOPOLOGY_TIMEOUT)
        except (RuntimeError, HTTPError) as exc:
            self._check_failure_expected("rebuild", details, server.log_filename, exc, expected_error)
            return
        self._check_success_expected("rebuild", details, server.log_filename, expected_error)
        await self._stop_server(server_id, gracefully=True)

    def _check_failure_expected(self,
                                request: str,
                                details: str,
                                log_filename: pathlib.Path,
                                exc: Exception,
                                expected_error: str | None) -> None:
        """Raise unless the topology request failed with the expected error."""

        if not expected_error:
            raise RuntimeError(
                f"{request} failed ({details}), check log file at {log_filename},"
                f" error: \"{exc}\""
            )
        if expected_error not in str(exc):
            raise RuntimeError(
                f"{request} failed ({details}) but did not contain expected error"
                f" (\"{expected_error}\"), check log file at {log_filename}, error: \"{exc}\""
            )
        self.logger.info("%s (%s) failed as expected: %s", request, details, exc)

    def _check_success_expected(self,
                                request: str,
                                details: str,
                                log_filename: pathlib.Path,
                                expected_error: str | None) -> None:
        """Raise if the topology request, which succeeded, was expected to fail."""

        if expected_error:
            raise RuntimeError(
                f"{request} succeeded when it should have failed ({details},"
                f" expected error: \"{expected_error}\"), check log file at {log_filename}"
            )
        self.logger.info("%s (%s) succeeded", request, details)

    @cluster_op
    async def server_get_config(self, server_id: ServerNum) -> dict[str, object]:
        """Get conf/scylla.yaml of the given server as a dictionary.
           Fails if the server cannot be found."""
        assert server_id in self.servers, f"Server {server_id} unknown"
        return self.servers[server_id].get_config()

    @cluster_op
    async def server_update_config(self,
                                   server_id: ServerNum,
                                   key: str | None = None,
                                   value: Any = None,
                                   *,
                                   config_options: dict[str, Any] | None = None) -> None:
        """Update conf/scylla.yaml of the given server.

        You can update a single option by providing the (key, value) pair, or multiple options
        using config_options.
        If the server is running, reload the config with a SIGHUP.
        Fail if the server cannot be found.
        """
        if key is not None:
            if value is None:
                raise RuntimeError("`value` is required if `key` is not None")
            if config_options is not None:
                raise RuntimeError("`key: value` pair and `config_options` dict can't be used simultaneously")
            config_options = {key: value}
        elif not isinstance(config_options, dict):
            raise RuntimeError(f"`config_options` is expected to be a dict, not {type(config_options)}")
        assert server_id in self.servers, f"Server {server_id} unknown"
        self.servers[server_id].update_config(config_options=config_options)

    @cluster_op
    async def server_remove_config_option(self, server_id: ServerNum, key: str) -> None:
        """Remove an option from conf/scylla.yaml of the given server.

        If the server is running, reload the config with a SIGHUP.
        Fail if the server cannot be found.
        """
        assert server_id in self.servers, f"Server {server_id} unknown"
        self.servers[server_id].remove_config_option(key=key)

    @cluster_op
    async def server_update_cmdline(self, server_id: ServerNum, cmdline_options: list[str]) -> None:
        """Update the command-line options of the given server by merging the new options into the existing ones.
           The update only takes effect after restart.
           Fails if the server cannot be found."""
        assert server_id in self.servers, f"Server {server_id} unknown"
        self.servers[server_id].update_cmdline(cmdline_options)

    @cluster_op
    async def server_switch_executable(self,
                                       server_id: ServerNum,
                                       path: str,
                                       version: ScyllaVersionDescription | None = None) -> None:
        """Switch the executable path of a stopped server.

        If `version` is given, also recompute the server's cmdline_options for that
        version, so version-specific argv from the previous version (e.g. a
        --logger-log-level for a logger unknown to the new executable) doesn't leak
        into the new one.
        """
        self.logger.info("Cluster %s upgrading server %s to executable %s", self.name, server_id, path)
        server = self.servers[server_id]
        assert not server.is_running, f"Server {server_id} is running: stop it first and then change its executable"
        server.exe = pathlib.Path(path).resolve()
        server.check_scylla_executable()
        if version is not None:
            server.switch_version(version, self.cmdline_options, self.cmdline_options_override)

    @cluster_op
    async def server_change_ip(self, server_id: ServerNum) -> IPAddress:
        """Lease a new IP address and update conf/scylla.yaml with it. The
        original IP is released at the end of the test to avoid an
        immediate recycle within the same cluster. The server must be
        stopped before its ip is changed."""
        server, ip_addr = await self._lease_address(server_id)
        logging.info("Cluster %s changed server %s IP from %s to %s", self.name,
                     server_id, server.ip_addr, ip_addr)
        server.change_ip(ip_addr)
        return ip_addr

    @cluster_op
    async def server_change_rpc_address(self, server_id: ServerNum) -> IPAddress:
        """Like server_change_ip(), for the server's RPC address."""
        server, rpc_address = await self._lease_address(server_id)
        logging.info("Cluster %s changed server %s RPC IP from %s to %s", self.name,
                     server_id, server.config["rpc_address"], rpc_address)
        server.change_rpc_address(rpc_address)
        return rpc_address

    async def _lease_address(self, server_id: ServerNum) -> tuple[ScyllaServer, IPAddress]:
        """Lease a new address for a stopped server; recycle() releases it."""
        assert server_id in self.servers, f"Server {server_id} unknown"
        server = self.servers[server_id]
        assert not server.is_running, f"Server {server_id} is running: stop it first and then change its ip"
        address = IPAddress(await self.host_registry.lease_host())
        self.leased_ips.add(address)
        return server, address

    @cluster_op
    async def server_get_log_filename(self, server_id: ServerNum) -> str:
        return str(self.server(server_id).log_filename)

    @cluster_op
    async def server_get_workdir(self, server_id: ServerNum) -> str:
        return str(self.server(server_id).workdir)

    @cluster_op
    async def server_get_maintenance_socket_path(self, server_id: ServerNum) -> str:
        return self.server(server_id).maintenance_socket_path

    @cluster_op
    async def server_get_exe(self, server_id: ServerNum) -> str:
        return str(self.server(server_id).exe)

    @cluster_op
    async def server_is_alive(self, server_id: ServerNum) -> bool:
        """Whether the server has a process which has not exited yet.

        Unlike ScyllaServer.is_running, which only tells a started server from
        a stopped one, this notices a server whose process died on its own.
        """
        cmd = self.server(server_id).cmd
        return cmd is not None and cmd.returncode is None

    @cluster_op
    async def server_wipe_sstables(self, server_id: ServerNum, keyspace: str, table: str) -> None:
        """Delete all sstable files for the given <node, keyspace, table>."""
        assert server_id in self.servers, f"Server {server_id} unknown"
        server = self.servers[server_id]
        assert not server.is_running, f"Server {server_id} is running: stop it first and then delete its files"
        server.wipe_sstables(keyspace, table)

    @cluster_op
    async def server_get_sstables_disk_usage(self, server_id: ServerNum, keyspace: str, table: str) -> int:
        """Measure the disk usage of sstables for the given <node, keyspace, table>."""
        assert server_id in self.servers, f"Server {server_id} unknown"
        server = self.servers[server_id]
        return server.get_sstables_disk_usage(keyspace, table)

    @cluster_op
    async def server_get_process_status(self, server_id: ServerNum) -> str:
        assert server_id in self.running

        self.logger.info("Cluster %s get process status for server %s", self.name, server_id)
        server = self.running[server_id]
        status = server.process_status()

        self.logger.info("Cluster %s process status for server %s is %s", self.name, server_id, status)
        return status

    def con_gen(self,
                hosts: list[IPAddress | EndPoint],
                port: int = 9042,
                use_ssl: bool = False,
                auth_provider: AuthProvider | None = None,
                load_balancing_policy: LoadBalancingPolicy | None = None,
                control_connection_fallback: bool = False,
                queries_only_via_control_connection: bool = False) -> CassandraCluster:
        """Create a CQL Cluster connection object according to configuration.

        It does not .connect() yet. control_connection_fallback uses the control
        connection when no query pool is usable. queries_only_via_control_connection
        always uses it. Both are harness flags translated to a driver option when
        the installed driver supports it.
        """
        assert hosts, "python driver connection needs at least one host to connect to"
        ssl_context = None
        if use_ssl:
            # Test clusters use self-signed certificates.
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
        # Policies own mutable host state, so each Cluster needs its own default.
        if load_balancing_policy is None:
            load_balancing_policy = RoundRobinPolicy()
        profile = ExecutionProfile(
            load_balancing_policy=load_balancing_policy,
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            # The default timeouts should have been more than enough, but in some
            # extreme cases with a very slow debug build running on a slow or very busy
            # machine, they may not be. Observed tests reach 160 seconds. So it's
            # incremented to 200 seconds.
            # See issue #11289.
            # NOTE: request_timeout is the main cause of timeouts, even if logs say heartbeat
            request_timeout=200,
        )
        whitelist_profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(WhiteListRoundRobinPolicy(hosts)),
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            request_timeout=200,
        )
        if control_connection_fallback and queries_only_via_control_connection:
            raise ValueError("Select fallback or control-connection-only mode, not both")

        cluster_kwargs = dict(
            execution_profiles={
                EXEC_PROFILE_DEFAULT: profile,
                "whitelist": whitelist_profile,
            },
            contact_points=hosts,
            port=port,
            # TODO: make the protocol version an option, to allow testing with
            # different versions. If we drop this setting completely, it will
            # mean pick the latest version supported by the client and the server.
            protocol_version=4,
            # NOTE: No auth provider as auth keysppace has RF=1 and topology will take
            # down nodes, causing errors. If auth is needed in the future for topology
            # tests, they should bump up auth RF and run repair.
            ssl_context=ssl_context,
            # The default timeouts should have been more than enough, but in some
            # extreme cases with a very slow debug build running on a slow or very busy
            # machine, they may not be. Observed tests reach 160 seconds. So it's
            # incremented to 200 seconds.
            # See issue #11289.
            connect_timeout=200,
            control_connection_timeout=200,
            # NOTE: max_schema_agreement_wait must be 2x or 3x smaller than request_timeout
            # else the driver can't handle a server being down
            max_schema_agreement_wait=20,
            idle_heartbeat_timeout=200,
            # The default reconnection policy has a large maximum interval
            # between retries (600 seconds). In tests that restart/replace nodes,
            # where a node can be unavailable for an extended period of time,
            # this can cause the reconnection retry interval to get very large,
            # longer than a test timeout.
            # The base delay decides how long a reconnect is delayed after a node is
            # already back up; max_attempts keeps the overall budget at ~251s.
            reconnection_policy=ExponentialReconnectionPolicy(0.1, 1.0, 250),
            auth_provider=auth_provider,
            # Capture messages for debugging purposes.
            connection_class=CustomConnection,
        )
        if control_connection_fallback or queries_only_via_control_connection:
            cluster_kwargs.update(control_connection_query_fallback_options(
                skip_pool_creation=queries_only_via_control_connection))
        return CassandraCluster(**cluster_kwargs)

    async def driver_connect(
            self,
            server: ServerInfo | None = None,
            auth_provider: AuthProvider | None = None,
            control_connection_fallback: bool = False,
    ) -> None:
        """Connect to cluster."""

        targets = [server] if server else await self.running_servers()
        servers = [s_info.rpc_address for s_info in targets]
        # avoids leaking connections if driver wasn't closed before
        self.driver_close()
        self.logger.debug("driver connecting to %s", servers)
        self.ccluster = self.con_gen(
            hosts=servers,
            port=self.port,
            use_ssl=self.use_ssl,
            auth_provider=auth_provider if auth_provider else self.auth_provider,
            control_connection_fallback=control_connection_fallback,
        )
        self.cql = self.ccluster.connect()

    def driver_close(self) -> None:
        """Disconnect from cluster."""

        for cluster in self.exclusive_clusters:
            safe_driver_shutdown(cluster)
        self.exclusive_clusters.clear()
        if self.ccluster is not None:
            self.logger.debug("shutting down driver")
            safe_driver_shutdown(self.ccluster)
            self.ccluster = None
        self.cql = None

    def get_cql(self) -> CassandraSession:
        """Precondition: driver is connected."""

        assert self.cql
        return self.cql

    async def get_ready_cql(self,
                            servers: list[ServerInfo],
                            timeout: float = 60,
                            see_others_interval: float = 45.) -> tuple[CassandraSession, list[CassandraHost]]:
        """Precondition: driver is connected."""

        cql = self.get_cql()
        await self.servers_see_each_other(servers, interval=see_others_interval)
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time() + timeout)
        return cql, hosts

    async def get_cql_exclusive(self,
                                server: ServerInfo,
                                auth_provider: AuthProvider | None = None) -> CassandraSession:
        cluster = self.con_gen([server.ip_addr], self.port, self.use_ssl,
                               auth_provider if auth_provider else self.auth_provider,
                               WhiteListRoundRobinPolicy([server.ip_addr]))
        self.exclusive_clusters.append(cluster)
        cql = cluster.connect()
        await wait_for_cql_and_get_hosts(cql, [server], time() + 60)
        return cql

    def _driver_update(self) -> None:
        if self.ccluster is not None:
            self.logger.debug("refresh driver node list")
            self.ccluster.control_connection.refresh_node_list_and_token_map()

    async def check_all_errors(self, all_errors: bool = False) -> dict[ServerInfo, dict[str, list[str] | str]]:
        errors = defaultdict(dict)
        # find errors in logs
        for server in await self.all_servers():
            log_file = await self.server_open_log(server_id=server.server_id)
            # check if we should ignore cores on this server
            ignore_cores = []
            if self.ignore_cores_log_patterns:
                if matches := await log_file.grep("|".join(f"({p})" for p in set(self.ignore_cores_log_patterns))):
                    self.logger.debug("Will ignore cores on %s. Found the following log messages: %s", server, matches)
                    ignore_cores.append(server)
            critical_error_pattern = r"Assertion.*failed|AddressSanitizer"
            if server not in ignore_cores:
                critical_error_pattern += "|Aborting on shard"
            if found_critical := await log_file.grep(critical_error_pattern):
                errors[server]["critical"] = [e[0] for e in found_critical]
                # Find the backtraces for the critical errors
                if found_backtraces := await log_file.find_backtraces():
                    errors[server]["backtraces"] = found_backtraces
            if all_errors:
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

    async def filter_errors(self, errors: list[str] | list[list[str]]) -> list[str] | list[list[str]]:
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
        def match_line(e: str | list[str]) -> bool:
            line = e[0] if isinstance(e, list) else e
            return not exclude_errors_pattern.search(line)
        return [e for e in errors if match_line(e)]

    async def find_cores(self) -> dict[ServerInfo, list[str]]:
        """Find core files on all servers."""

        # find *.core files in current dir
        cores = [str(core_file.absolute()) for core_file in pathlib.Path('.').glob('*.core')]
        server_cores = dict()
        # match core files to servers by pid
        for server in await self.all_servers():
            if found_cores := [core for core in cores if f".{server.pid}." in core]:
                server_cores[server] = found_cores
        return server_cores

    async def gather_related_logs(self, failed_test_path_dir: pathlib.Path) -> None:
        for server in await self.all_servers():
            log_file = await self.server_open_log(server_id=server.server_id)
            shutil.copyfile(log_file.file, failed_test_path_dir / f"{pathlib.Path(log_file.file).name}")
            allure.attach(log_file.file.read_bytes(), name=log_file.file.name, attachment_type=allure.attachment_type.TEXT)

    async def all_servers_by_host_id(self) -> dict[HostID, ServerInfo]:
        result = dict()
        servers = await self.all_servers()
        for s in servers:
            result[await self.get_host_id(s.server_id)] = s
        return result

    async def find_server_by_host_id(self, servers: list[ServerInfo], host_id: HostID) -> ServerInfo:
        for s in servers:
            try:
                if await self.get_host_id(s.server_id) == host_id:
                    return s
            except Exception:
                self.logger.warning("Failed to get host ID of server %s while looking for a server with host ID %s",
                                    s, host_id)
        raise RuntimeError(f"Host ID {host_id} not found in {servers}")

    async def server_stop(self, server_id: ServerNum, convict: bool, seen_by: list[ServerInfo] | None = None) -> None:
        """Stop specified server.

        convict: If True, immediately marks the node as DOWN on all live nodes,
                 bypassing the natural failure detection delay (~20s).
                 Set to True when the test waits for other nodes to notice the down node
                 to speed up the test (e.g. before
                 remove_node, replace, or waiting for failure detection).
                 Set to False when conviction is pointless (single node, all stopped, immediate restart)
                 or the test wants to exercise natural failure detection.
        seen_by: Servers which must see the server as dead before returning.
        """
        host_id = None
        if convict:
            try:
                host_id = await self.get_host_id(server_id)
            except Exception:
                # Server may have never completed bootstrap, so host_id is unknown.
                # Skip conviction in this case.
                pass
        await self._server_stop(server_id)
        if host_id is not None:
            try:
                await self.convict_on_all(host_id)
            except Exception:
                # It's a best-effort attempt, ignore errors
                # In some scenarios errors are expected, e.g. when server_stop() is called concurrently on many servers.
                pass
        await self._server_seen_dead_by(server_id, seen_by)

    async def _server_seen_dead_by(self, server_id: ServerNum, seen_by: list[ServerInfo] | None, interval: float = 45.) -> None:
        """Wait till the given servers see a stopped server as dead."""
        if seen_by:
            server_ip = await self.get_host_ip(server_id)
            await gather_safely(*(self.server_not_sees_other_server(s.ip_addr, server_ip, interval=interval) for s in seen_by))

    async def _server_seen_live_by(self, server_id: ServerNum, seen_by: list[ServerInfo] | None, interval: float = 45.) -> None:
        """Wait till the given servers see a started server as alive."""
        if seen_by:
            server_ip = await self.get_host_ip(server_id)
            await gather_safely(*(self.server_sees_other_server(s.ip_addr, server_ip, interval=interval) for s in seen_by))

    async def server_stop_gracefully(self,
                                     server_id: ServerNum,
                                     timeout: float | None = None,
                                     seen_by: list[ServerInfo] | None = None) -> None:
        """Stop specified server gracefully.

        With no timeout given, outlast the server's own graceful-stop timeout
        for this build mode, so that a slow stop is reported by the server --
        which knows why it was slow -- instead of timing out here first.

        Optionally waits for the servers in seen_by to see it as dead.
        """
        assert self.is_running, "ScyllaCluster is stopped"
        if timeout is None:
            timeout = graceful_stop_timeout(self.mode) + 60

        async with asyncio.timeout(timeout):
            await self._server_stop_gracefully(server_id)
        await self._server_seen_dead_by(server_id, seen_by)

    async def disable_tablet_balancing(self) -> None:
        """Disables background tablet load-balancing.

        If there are already active migrations, it waits for them to finish before returning.
        Doesn't block migrations on behalf of node operations like decommission, removenode or replace.
        """
        await self.api.disable_tablet_balancing(await self._group0_server_ip())

    async def enable_tablet_balancing(self) -> None:
        """Enables background tablet load-balancing."""

        await self.api.enable_tablet_balancing(await self._group0_server_ip())

    async def _group0_server_ip(self) -> IPAddress:
        """Any running server will do for a group0 operation."""
        servers = await self.running_servers()
        if not servers:
            raise RuntimeError("No running servers")
        return servers[0].ip_addr

    async def convict_on_all(self, host: HostID) -> None:
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
                           seen_by: list[ServerInfo] | None = None,
                           seeds: list[IPAddress] | None = None,
                           timeout: float | None = ScyllaServer.TOPOLOGY_TIMEOUT,
                           connect_driver: bool = True,
                           expected_server_up_state: ServerUpState = ServerUpState.SERVING,
                           cmdline_options_override: list[str] | None = None,
                           append_env_override: dict[str, str] | None = None,
                           auth_provider: dict[str, str] | None = None,
                           replace_cmdline_options: bool = False) -> None:
        """Start specified server.

        When expected_error is None, waits until Scylla reports that all
        configured listeners (including any non-default ports) are ready
        (ServerUpState.SERVING). Pass a lower expected_server_up_state to
        return earlier. When expected_error is set, no readiness wait is
        performed. When connect_driver=False, the effective wait state is
        capped at HOST_ID_QUERIED regardless of expected_server_up_state.

        Optionally waits for it to learn of other servers (wait_others),
        and for the servers in seen_by to see it as alive, e.g. the one that
        coordinates the requests which follow. When restarting a server, pass
        them as seen_by when stopping it too, or they may still see it as alive
        from before it was stopped.

        `cmdline_options_override` is merged into the server's command line for
        this start only, like `server_update_cmdline()` but without persisting:
        `--logger-log-level` appends, any other option replaces its current
        value, and the harness defaults (`--smp 2 -m 1G --overprovisioned ...`)
        stay. `replace_cmdline_options=True` makes it the whole argv instead.
        `append_env_override` replaces the server's environment.

        If `expected_crash` is True, ignore cores and backtraces related to `expected_error`.
        """
        if expected_error is not None:
            self.ignore_log_patterns.append(re.escape(expected_error))
            if expected_crash:
                self.ignore_cores_log_patterns.append(re.escape(expected_error))

        if not connect_driver:
            expected_server_up_state = min(expected_server_up_state, ServerUpState.HOST_ID_QUERIED)

        async with asyncio.timeout(timeout):
            await self._server_start(
                server_id,
                expected_error=expected_error,
                seeds=seeds,
                expected_server_up_state=expected_server_up_state,
                cmdline_options_override=cmdline_options_override,
                append_env_override=append_env_override,
                auth_provider=auth_provider,
                replace_cmdline_options=replace_cmdline_options,
            )
        await self.server_sees_others(server_id, wait_others, interval = wait_interval)
        await self._server_seen_live_by(server_id, seen_by, interval=wait_interval)
        if expected_error is None and connect_driver:
            if self.cql:
                self._driver_update()
            else:
                await self.driver_connect()

    async def server_restart(self,
                             server_id: ServerNum,
                             wait_others: int = 0,
                             wait_interval: float = 45) -> None:
        """Restart specified server and optionally wait for it to learn of other servers."""

        await self.server_stop_gracefully(server_id)
        await self.server_start(server_id=server_id, wait_others=wait_others, wait_interval=wait_interval)

    async def rolling_restart(self,
                              servers: list[ServerInfo],
                              with_down: Callable[[ServerInfo], Awaitable[Any]] | None = None,
                              wait_for_cql: bool = True,
                              cmdline_options_override: list[str] | None = None) -> None:
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
                    await wait_for_cql_and_get_hosts(self.get_cql(), up_servers, time() + 60)
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
            await wait_for_cql_and_get_hosts(self.get_cql(), servers_running, time() + 60)

    async def server_change_version(self,
                                     server_id: ServerNum,
                                     exe: str,
                                     version: ScyllaVersionDescription | None = None) -> None:
        """Upgrade a running Scylla node by switching it to a new binary version specified by the 'exe' parameter.

        If `version` is given, it is forwarded to server_switch_executable() so the
        server's cmdline_options are recomputed for that version, rather than carrying
        over version-specific argv from whatever version the server was previously
        running (see server_switch_executable's docstring).
        """

        await self.server_stop_gracefully(server_id)
        await self.server_switch_executable(server_id, exe, version)
        await self.server_start(server_id)

    async def _get_ignored_ip_addresses(self, ignore_dead: list[IPAddress | HostID]) -> list[IPAddress]:
        """Get IP addresses of nodes ignored in the replace and removenode operations.

        FIXME: Simplify the code once we disallow specifying ignored nodes through IP addresses in Scylla.
        """
        servers = await self.all_servers()
        ignored_ips = []
        for ignored in ignore_dead:
            # IPAddress and HostID are both NewType over str, so isinstance() cannot distinguish them at runtime.
            if '.' in ignored:
                ignored_ips.append(ignored)
            else:
                ignored_server = await self.find_server_by_host_id(servers, cast(HostID, ignored))
                ignored_ips.append(ignored_server.ip_addr)
        return ignored_ips

    async def server_add(self,
                         replace_cfg: ReplaceConfig | None = None,
                         cmdline: list[str] | None = None,
                         config: dict[str, Any] | None = None,
                         version: ScyllaVersionDescription | None = None,
                         property_file: dict[str, Any] | None = None,
                         start: bool = True,
                         expected_error: str | None = None,
                         seeds: list[IPAddress] | None = None,
                         timeout: float | None = ScyllaServer.TOPOLOGY_TIMEOUT,
                         server_encryption: str = "none",
                         expected_server_up_state: ServerUpState = ServerUpState.SERVING,
                         connect_driver: bool = True,
                         launcher: list[str] | None = None) -> ServerInfo:
        """Add a new server.

        When start=True and expected_error is None, waits until Scylla reports
        that all configured listeners (including any non-default ports) are ready
        (ServerUpState.SERVING). Pass a lower expected_server_up_state to return
        earlier. When start=False or expected_error is set, no readiness wait is
        performed. When connect_driver=False, the effective wait state is capped
        at HOST_ID_QUERIED regardless of expected_server_up_state.

        launcher is a command prefix the server's executable is started through
        on every start, e.g. to run it in another namespace. It must exec the
        executable rather than fork it. By default the executable is started
        directly.
        """
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

            async with asyncio.timeout(timeout):
                s_info = await self._server_add(
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
                    launcher=launcher,
                )
        except Exception as exc:
            raise RuntimeError("Failed to add server") from exc
        self.logger.debug("ScyllaCluster added %s", s_info)
        if expected_error is None and connect_driver:
            if self.cql:
                self._driver_update()
            elif start:
                await self.driver_connect()
        return s_info

    async def servers_add(self,
                          servers_num: int = 1,
                          cmdline: list[str] | None = None,
                          config: dict[str, Any] | None = None,
                          version: ScyllaVersionDescription | None = None,
                          property_file: list[dict[str, Any]] | dict[str, Any] | None = None,
                          start: bool = True,
                          seeds: list[IPAddress] | None = None,
                          driver_connect_opts: dict[str, Any] | None = None,
                          expected_error: str | None = None,
                          server_encryption: str = "none",
                          auto_rack_dc: str | None = None) -> list[ServerInfo]:
        """Add new servers concurrently.

        When start=True and expected_error is None, waits until Scylla reports
        that all configured listeners (including any non-default ports) are ready
        (ServerUpState.SERVING). When start=False or expected_error is set, no
        readiness wait is performed.

        This function can be called only if the cluster uses consistent topology changes, which support
        concurrent bootstraps. If your test does not fulfill this condition, and you want to add multiple
        servers, you should use multiple server_add calls.
        """
        assert servers_num > 0, f"servers_add: cannot add {servers_num} servers, servers_num must be positive"
        assert not (property_file and auto_rack_dc), "Either property_file or auto_rack_dc can be provided, but not both"

        if expected_error is not None:
            self.ignore_log_patterns.append(re.escape(expected_error))

        if auto_rack_dc:
            property_file = [{"dc":auto_rack_dc, "rack":f"rack{i+1}"} for i in range(servers_num)]

        try:
            async with asyncio.timeout(ScyllaServer.TOPOLOGY_TIMEOUT * servers_num):
                s_infos = await self._servers_add(
                    servers_num=servers_num,
                    cmdline=cmdline,
                    config=config,
                    version=version,
                    property_file=property_file,
                    start=start,
                    seeds=seeds,
                    server_encryption=server_encryption,
                    expected_error=expected_error,
                )
        except Exception as exc:
            raise RuntimeError("Failed to add servers") from exc

        assert len(s_infos) == servers_num, f"servers_add requested adding {servers_num} servers but " \
                                    f"got server data about {len(s_infos)} servers: {s_infos}"
        self.logger.debug("ScyllaCluster added %s", s_infos)
        if expected_error is None:
            if self.cql:
                self._driver_update()
            elif start:
                await self.driver_connect(**(driver_connect_opts or {}))
        return s_infos

    async def remove_node(self,
                          initiator_id: ServerNum,
                          server_id: ServerNum,
                          ignore_dead: list[IPAddress | HostID] | None = None,
                          expected_error: str | None = None,
                          wait_dead: bool = True,
                          timeout: float | None = ScyllaServer.TOPOLOGY_TIMEOUT) -> None:
        """Invoke remove node Scylla REST API for a specified server."""

        ignore_dead = ignore_dead or []
        if expected_error is not None:
            self.ignore_log_patterns.append(re.escape(expected_error))


        # We should wait until all running nodes see the node being removed
        # and all ignored nodes as dead. Removenode could be rejected
        # otherwise. We make this waiting default and optional to allow testing
        # expected removenode failures.
        if wait_dead:
            removed_ip = await self.get_host_ip(server_id)
            ignored_ips = await self._get_ignored_ip_addresses(ignore_dead)
            dead_ips = [removed_ip] + ignored_ips
            await gather_safely(*(self.others_not_see_server(ip) for ip in dead_ips))

        async with asyncio.timeout(timeout):
            await self._remove_node(
                initiator_id, server_id, ignore_dead=ignore_dead, expected_error=expected_error,
            )
        self._driver_update()

    async def decommission_node(self,
                                server_id: ServerNum,
                                expected_error: str | None = None,
                                timeout: float | None = ScyllaServer.TOPOLOGY_TIMEOUT) -> None:
        """Tell a node to decommission with Scylla REST API."""

        if expected_error is not None:
            self.ignore_log_patterns.append(re.escape(expected_error))

        async with asyncio.timeout(timeout):
            await self._decommission_node(server_id, expected_error=expected_error)
        self._driver_update()

    async def rebuild_node(self,
                           server_id: ServerNum,
                           expected_error: str | None = None,
                           timeout: float | None = ScyllaServer.TOPOLOGY_TIMEOUT) -> None:
        """Tell a node to rebuild with Scylla REST API."""

        async with asyncio.timeout(timeout):
            await self._rebuild_node(server_id, expected_error=expected_error)
        self._driver_update()

    async def wait_for_host_known(self,
                                  dst_server_ip: IPAddress,
                                  expect_host_id: HostID,
                                  deadline: float | None = None) -> None:
        """Waits until dst_server_id knows about expect_host_id, with timeout."""

        async def host_is_known() -> bool | None:
            host_id_map = await self.api.get_host_id_map(dst_server_ip)
            return True if any(entry for entry in host_id_map if entry["value"] == expect_host_id) else None

        await wait_for(host_is_known, deadline or (time() + 30))

    async def wait_for_scylla_process_status(self,
                                             server_id: ServerNum,
                                             expected_statuses: list[str],
                                             deadline: float | None = None) -> str:
        """Wait for Scylla's process status for server_id will be as expected, with timeout."""

        async def process_status_is_as_expected() -> str | None:
            current_status = await self.server_get_process_status(server_id)
            if current_status in expected_statuses:
                return current_status
            return None

        return await wait_for(process_status_is_as_expected, deadline or (time() + 30))

    async def get_table_id(self, keyspace: str, table: str) -> uuid.UUID:
        return (await self._schema_rows("table", keyspace, table))[0].id

    async def get_view_id(self, keyspace: str, view: str) -> uuid.UUID:
        return (await self._schema_rows("view", keyspace, view))[0].id

    async def get_table_or_view_id(self, keyspace: str, table: str) -> uuid.UUID:
        rows = await self._schema_rows("table", keyspace, table) or await self._schema_rows("view", keyspace, table)
        return rows[0].id

    async def _schema_rows(self, kind: str, keyspace: str, name: str) -> Any:
        """The ID rows of the keyspace's table or view (kind) of that name."""
        return await self.get_cql().run_async(
            f"select id from system_schema.{kind}s where keyspace_name = '{keyspace}' and {kind}_name = '{name}'"
        )

    async def server_sees_others(self, server_id: ServerNum, count: int, interval: float = 45.) -> None:
        """Wait till a server sees a minimum given count of other servers."""

        if count < 1:
            return
        server_ip = await self.get_host_ip(server_id)
        await self._wait_for_alive_endpoints(server_ip, lambda alive: len(alive) > count, interval, "_sees_min_others")

    async def server_sees_other_server(self,
                                       server_ip: IPAddress,
                                       other_ip: IPAddress,
                                       interval: float = 45.) -> None:
        """Wait till a server sees another specific server IP as alive."""

        await self._wait_for_alive_endpoints(
            server_ip,
            lambda alive: other_ip in alive,
            interval,
            "_sees_another_server",
        )

    async def servers_see_each_other(self, servers: list[ServerInfo], interval: float = 45.) -> None:
        """Wait till all servers see all other servers in the list."""

        others = [self.server_sees_others(srv.server_id, len(servers) - 1, interval) for srv in servers]
        await gather_safely(*others)

    async def server_not_sees_other_server(self,
                                           server_ip: IPAddress,
                                           other_ip: IPAddress,
                                           interval: float = 45.) -> None:
        """Wait till a server sees another specific server IP as dead."""

        await self._wait_for_alive_endpoints(
            server_ip,
            lambda alive: other_ip not in alive,
            interval,
            "_not_sees_another_server",
        )

    async def _wait_for_alive_endpoints(self,
                                        server_ip: IPAddress,
                                        condition: Callable[[list], bool],
                                        interval: float,
                                        label: str) -> None:
        """Wait till the server's list of alive endpoints satisfies the condition."""

        async def check() -> bool | None:
            return True if condition(await self.api.get_alive_endpoints(server_ip)) else None

        await wait_for(check, time() + interval, period=.5, label=label)

    async def others_not_see_server(self, server_ip: IPAddress, interval: float = 45.) -> None:
        """Wait till a server is seen as dead by all other running servers in the cluster."""

        others_ips = [srv.ip_addr for srv in await self.running_servers() if srv.ip_addr != server_ip]
        await gather_safely(*(self.server_not_sees_other_server(ip, server_ip, interval) for ip in others_ips))

    async def server_open_log(self, server_id: ServerNum) -> ScyllaLogFile:
        log_filename = await self.server_get_log_filename(server_id)
        return ScyllaLogFile(self.thread_pool, log_filename)
