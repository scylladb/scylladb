#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Manager for a Scylla cluster used by test cases.
   Provides an async API for tests to request changes in the cluster
   and helper methods on top of it.
   Manages driver refresh when the cluster is cycled.
"""
import asyncio
import inspect
import logging
import pathlib
import re
import shutil
import ssl
import traceback
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from pathlib import Path
from time import time
from typing import Any, Awaitable, Callable, Concatenate, cast, overload

import allure
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

from test.pylib.driver_utils import safe_driver_shutdown
from test.pylib.internal_types import ServerNum, IPAddress, HostID, ServerInfo, ServerUpState
from test.pylib.log_browsing import ScyllaLogFile
from test.pylib.rest_client import HTTPError, ScyllaMetricsClient, ScyllaRESTAPIClient
from test.pylib.scylla_cluster import ClusterFactory, ReplaceConfig, ScyllaCluster, bind_to_current_loop
from test.pylib.scylla_server import ScyllaServer, ScyllaVersionDescription
from test.pylib.util import (
    Host,
    LogPrefixAdapter,
    gather_safely,
    graceful_stop_timeout,
    universalasync_typed_wrap,
    wait_for,
    wait_for_cql_and_get_hosts,
)


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


# Cap on a manager operation, the same ceiling the HTTP client used to apply
# to every request.  An op whose caller controls the timeout is decorated with
# manager_op(timeout=None) and wrapped in asyncio.timeout() on the caller's
# side of the bridge instead.
DEFAULT_OP_TIMEOUT = 300


type ManagerFn[**P, R] = Callable[Concatenate[ScyllaClusterManager, P], R]
type ManagerAsyncFn[**P, R] = ManagerFn[P, Awaitable[R]]


def fenced[**P, R](fn: ManagerFn[P, R]) -> ManagerFn[P, R]:
    """Refuse the call once the test that leased the manager has finished.

    manager_op applies it to operations (fence=True); the driver methods carry
    it directly, since the session lives on this module-scoped manager and a
    leaked caller could close the one the running test is using.  The check
    happens on the caller's side, before a coroutine is even created.
    """
    @wraps(fn)
    def wrapper(self: ScyllaClusterManager, *args: P.args, **kwargs: P.kwargs) -> R:
        if self._test_finished:
            raise RuntimeError("ScyllaClusterManager is not accessible after the test finished")
        return fn(self, *args, **kwargs)

    if inspect.iscoroutinefunction(fn):
        # The wrapper is a plain function returning fn's coroutine; universalasync
        # keys off iscoroutinefunction to make operations callable from a thread.
        inspect.markcoroutinefunction(wrapper)

    return wrapper


@overload
def manager_op[**P, R](entry_point: ManagerAsyncFn[P, R]) -> ManagerAsyncFn[P, R]: ...
@overload
def manager_op[**P, R](*,
                       blockable: bool = ...,
                       fence: bool = ...,
                       timeout: float | None = ...) -> Callable[[ManagerAsyncFn[P, R]], ManagerAsyncFn[P, R]]: ...
def manager_op[**P, R](entry_point: ManagerAsyncFn[P, R] | None = None,
                       *,
                       blockable: bool = False,
                       fence: bool = True,
                       timeout: float | None = DEFAULT_OP_TIMEOUT,
                       ) -> ManagerAsyncFn[P, R] | Callable[[ManagerAsyncFn[P, R]], ManagerAsyncFn[P, R]]:
    """Run a ScyllaClusterManager entry point on the manager's loop, as its own tracked task.

    Usable bare, as @manager_op, or with arguments, as @manager_op(blockable=True).

    The manager's state -- subprocess transports, per-server locks -- belongs to
    its loop, so an operation always runs there, whichever loop or thread calls
    it.  The caller waits across the bridge under `timeout`; None means no cap,
    for operations whose caller applies its own asyncio.timeout().

    A waiter that times out or is cancelled abandons the operation instead of
    killing it: the operation is shielded, stays in tasks_history and
    after_test() drains it from there.  Failures are logged with their
    traceback into the per-test cluster log.

    Every entry point works on the current cluster, so that is asserted here.
    fence=True refuses the call once the test finished (see fenced),
    blockable=True refuses it on a manager an earlier test broke.
    """
    def decorator(fn: ManagerAsyncFn[P, R]) -> ManagerAsyncFn[P, R]:
        # The bridge does not consume a timeout argument; a timeout is either
        # this decorator's cap or the caller's own asyncio.timeout() around
        # the call.  Fail loudly on the old pattern.
        if "timeout" in inspect.signature(fn).parameters and timeout is not None:
            raise TypeError(f"{fn.__name__} declares its own timeout parameter;"
                            f" decorate it with manager_op(timeout=None) and apply"
                            f" the timeout on the caller's side")

        async def run_op(self: ScyllaClusterManager, *args: P.args, **kwargs: P.kwargs) -> R:
            """The operation itself; always runs on the manager's loop."""
            assert self.cluster, "ScyllaClusterManager is not running"
            if blockable:
                self.check_not_broken()
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

            self.logger.info("[ScyllaClusterManager][%s] %s", op.get_name(), descr)
            self.tasks_history[op] = descr
            op.add_done_callback(op_finished)
            return await asyncio.shield(op)

        @wraps(fn)
        async def wrapper(self: ScyllaClusterManager, *args: P.args, **kwargs: P.kwargs) -> R:
            if asyncio.get_running_loop() is self._loop:
                # Already on the manager's loop, i.e. one operation calling
                # another -- which no operation does today.  There is nothing
                # to hand over, so call in place.  The cap goes with the
                # bridge: it bounds how long a caller waits on the far side,
                # and here there is no far side.
                return await run_op(self, *args, **kwargs)
            future = asyncio.run_coroutine_threadsafe(run_op(self, *args, **kwargs), self._loop)
            async with asyncio.timeout(timeout):    # None = no cap
                return await asyncio.wrap_future(future)
        # The bridge adds no parameters, so the decorated entry point keeps
        # its exact signature.
        return fenced(wrapper) if fence else wrapper
    return decorator if entry_point is None else decorator(entry_point)


@universalasync_typed_wrap
class ScyllaClusterManager:
    """Manages a Scylla cluster for a test module and serves the test API.

    Parallel requests are not supported.

    The manager outlives every test in the module, so state belonging to one
    test -- the driver session, the log patterns to ignore -- is reset by
    before_test(), while after_test() raises the leaked-task fence and drains
    what the test left running.  That fence only covers the teardown window,
    since the next before_test() lifts it; what keeps a leaked caller out of a
    later test is the per-test event loop cancelling its tasks, after_test()'s
    drain plus break_manager(), and per-process-unique server ids failing an
    assertion.

    A method that awaits cluster or server internals must be a @manager_op --
    those objects belong to the manager's loop -- and the CQL driver, the REST
    helpers and anything composing operations must not be.  An operation body
    runs entirely on that loop, so an entry point needing caller-side work too
    (the caller's timeout, a driver refresh, a gossip wait) is a private _x
    operation behind a public wrapper holding that work.

    Args:
        test_uname: name of the test module the manager serves
        create_cluster: factory of Scylla clusters
        base_dir: directory for the per-test-case cluster logs
        port: CQL port for driver connections
        use_ssl: use SSL for driver connections
        auth_provider: authentication provider for driver connections
    """
    # Per test, created by before_test() and removed by after_test().
    test_case_log_file: pathlib.Path
    test_case_log_fh: logging.FileHandler

    def __init__(self,
                 test_uname: str,
                 create_cluster: ClusterFactory,
                 base_dir: str,
                 port: int,
                 use_ssl: bool,
                 auth_provider: Any | None) -> None:
        # The manager must be constructed on its own, always-running loop:
        # its operations run there (see manager_op), whichever loop or thread
        # they are called from.
        self._loop = asyncio.get_running_loop()
        self.test_uname = test_uname
        self.base_dir = base_dir
        logger = logging.getLogger(self.test_uname)
        self.logger = LogPrefixAdapter(logger, {"prefix": self.test_uname})
        self.cluster: ScyllaCluster | None = None
        self.create_cluster = create_cluster
        self.is_running = False
        # tasks_history holds the running test's operations: after_test()
        # drains it, and whatever is left over is the leak it reports.
        self.tasks_history: dict[asyncio.Task, str] = {}
        # Sticky on purpose: a manager broken by one test keeps failing the
        # rest of the module, so nothing resets these between tests.
        self.server_broken_event = asyncio.Event()
        self.server_broken_reason = ""
        # Per-module: configuration and helpers that outlive every test.
        self.port = port
        self.use_ssl = use_ssl
        # The suite's provider, restored per test: tests override
        # self.auth_provider to connect as someone else.
        self._suite_auth_provider = auth_provider
        self.api = ScyllaRESTAPIClient()
        self.metrics = ScyllaMetricsClient()
        self.thread_pool = ThreadPoolExecutor()

        # Per-test: before_test() resets the fence, the patterns and the
        # policy, and the fixture's driver_close() drops the session.  State
        # added here that a test can change has to be reset there too, or it
        # leaks into the next test -- the manager itself outlives them.
        self._test_finished = False
        # The currently running test case with self.test_uname prepended, e.g.
        # test_topology.1::test_add_server_add_column
        self.current_test_case_full_name = ""
        self.load_balancing_policy = RoundRobinPolicy()
        self.auth_provider = self._suite_auth_provider
        self.ignore_log_patterns: list[str] = []  # patterns to ignore in server logs when checking for errors
        self.ignore_cores_log_patterns: list[str] = []  # patterns to ignore in server logs when checking for core files
        self.ccluster: CassandraCluster | None = None
        self.cql: CassandraSession | None = None
        self.exclusive_clusters: list[CassandraCluster] = []

    def repr_tasks_history(self) -> str:
        out = "Cluster_history"
        for key, val in self.tasks_history.items():
            out += f"\n{val}:\t\t{repr(key)}"
        return out

    async def start(self) -> None:
        """Get first cluster."""

        if self.is_running:
            self.logger.warning("ScyllaClusterManager already running")
            return
        self.cluster = await self.create_cluster(self.logger)
        self.logger.info("First Scylla cluster: %s", self.cluster)
        self.cluster.setLogger(self.logger)
        self.is_running = True

    # fence=False: the fence is still up from the previous test when this runs,
    # and lifting it is this operation's own job.  The manager fixture is the
    # only caller.
    @manager_op(blockable=True, fence=False, timeout=600)
    async def before_test(self, test_case_name: str) -> str:
        """Before-test hook of the manager fixture: reset the per-test state
        and lease a cluster for the test.

        The driver is connected by the fixture afterwards: the session is used
        from the test's loop, and connecting blocks for as long as the driver's
        connect timeout, so it has no business running on the manager's.
        """
        self.ignore_log_patterns = []
        self.ignore_cores_log_patterns = []
        self.load_balancing_policy = RoundRobinPolicy()
        self.auth_provider = self._suite_auth_provider
        self.current_test_case_full_name = f"{self.test_uname}::{test_case_name}"
        root_logger = logging.getLogger()
        parent_test_name = pathlib.Path(self.test_uname.replace("/", "_")).stem
        self.test_case_log_file = pathlib.Path(self.base_dir) / f"{parent_test_name}.{test_case_name}_cluster.log"
        self.test_case_log_fh = logging.FileHandler(self.test_case_log_file)
        self.test_case_log_fh.setLevel(root_logger.getEffectiveLevel())
        # to have the custom formatter with a timestamp that used in a test.py but for each testcase's log, we need to
        # extract it from the root logger and apply to the handler
        self.test_case_log_fh.setFormatter(root_logger.handlers[0].formatter)
        root_logger.addHandler(self.test_case_log_fh)
        self.logger.info("Setting up %s", self.current_test_case_full_name)
        if self.cluster.is_dirty:
            self.logger.info("Current cluster %s is dirty after test %s, replacing with a new one...",
                             self.cluster.name, self.current_test_case_full_name)
            await self.cluster.recycle()
            self.cluster = await self.create_cluster(self.logger)
            self.logger.info("Got new Scylla cluster: %s", self.cluster.name)
        self.cluster.setLogger(self.logger)
        self.logger.info("Leasing Scylla cluster %s for test %s", self.cluster, self.current_test_case_full_name)
        self.cluster.before_test(self.current_test_case_full_name)
        self.cluster.take_log_savepoint()
        # Lower the fence last: recycling above replaces self.cluster, and a
        # task the previous test leaked must not reach the one being disposed.
        self._test_finished = False
        return str(self.cluster)

    async def stop(self) -> None:
        """Dispose of the last cluster if present."""

        self.logger.info("ScyllaManager stopping for test %s", self.test_uname)
        self._driver_close()
        self.thread_pool.shutdown(wait=False)
        if self.cluster:
            self.logger.info("ScyllaManager: stopping Scylla cluster %s after %s",
                             self.cluster, self.test_uname)
            await self.cluster.recycle()
            self.cluster = None
        self.is_running = False

    @manager_op
    async def is_dirty(self) -> bool:
        """Report if current cluster is dirty."""

        return self.cluster.is_dirty

    @manager_op
    async def running_servers(self) -> list[ServerInfo]:
        """Return server info for running servers."""

        return self.cluster.running_servers()

    @manager_op
    async def all_servers(self) -> list[ServerInfo]:
        """Return server info for all servers."""

        return self.cluster.all_servers()

    @manager_op
    async def starting_servers(self) -> list[ServerInfo]:
        """Return server info for servers which are currently starting."""

        return self.cluster.starting_servers()

    @manager_op
    async def get_host_ip(self, server_id: ServerNum) -> IPAddress:
        """IP address of a server."""

        try:
            return self.cluster.servers[server_id].ip_addr
        except Exception as exc:
            raise RuntimeError(f"Failed to get host IP address for server {server_id}") from exc

    @manager_op
    async def get_host_id(self, server_id: ServerNum) -> HostID:
        """Host ID of a server."""

        try:
            return await self.cluster.servers[server_id].get_host_id(self.cluster.api)
        except Exception as exc:
            raise RuntimeError(f"Failed to get local host id address for server {server_id}") from exc

    # fence=False: this operation raises the fence itself, and has to stay
    # reachable afterwards to report what the test left behind.
    # timeout=None: the drain below carries its own budget, which scales with
    # the build mode and so can exceed DEFAULT_OP_TIMEOUT.  A bridge cap
    # shorter than the drain would abandon the cleanup half-way through.
    @manager_op(fence=False, timeout=None)
    async def after_test(self, success: bool) -> dict[str, Any]:
        """After-test hook of the manager fixture.

        Fences the manager off, so a task leaked by the test cannot reach it
        anymore (see manager_op), then drains the operations the test left
        running and reports the result to the cluster.

        The fence goes up even when the manager is broken, which is why the
        broken check is here and not in the decorator: a test that cannot be
        torn down still must not leave its tasks a way in.
        """
        self._test_finished = True
        self.check_not_broken()
        assert self.current_test_case_full_name
        self.logger.info(self.repr_tasks_history())
        # Drop our own entry: the drain below must not wait for itself.
        current = asyncio.current_task()
        assert current is not None  # ops always run as a task (see manager_op)
        self.tasks_history.pop(current, None)
        # Wait for the operations the test left running.  The budget has to
        # outlast the longest of them, because asyncio.wait_for() cancels the
        # task when it expires -- and the task here is the operation itself,
        # not the shield the caller awaited.  The longest is a graceful stop,
        # and cancelling one mid-flight is not merely a lost result: the
        # CancelledError skips stop_gracefully()'s SIGKILL path while its
        # finally clause still clears self.cmd, disowning a process that is
        # still shutting down, after which stop() finds nothing left to kill.
        drain_timeout = graceful_stop_timeout(self.cluster.mode) + 60
        for task, descr in list(self.tasks_history.items()):
            if not task.done():
                self.logger.info("wait for task:%s, op:%s", task, descr)
                try:
                    await asyncio.wait_for(task, timeout=drain_timeout)
                except asyncio.TimeoutError:
                    self.break_manager(f"error on waiting coro {task.get_name()}", self.current_test_case_full_name)
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
        if self.tasks_history:
            self.break_manager(f"tasks leakage found  {self.tasks_history}", self.current_test_case_full_name)

        self.logger.info("Test %s %s, cluster: %s",
                         self.current_test_case_full_name, "SUCCEEDED" if success else "FAILED", self.cluster)
        try:
            self.cluster.after_test(self.current_test_case_full_name, success)
        finally:
            logging.getLogger().removeHandler(self.test_case_log_fh)
            if success:
                self.test_case_log_file.unlink()
            self.current_test_case_full_name = ""
        cluster_str = str(self.cluster)

        return {
            "cluster_str": cluster_str,
            "server_broken": self.server_broken_event.is_set(),
            "message": self.server_broken_reason,
        }

    def check_not_broken(self) -> None:
        """Refuse to run when a previous test broke the manager."""

        if self.server_broken_event.is_set():
            raise RuntimeError(f"ScyllaClusterManager BROKEN, Previous test broke ScyllaClusterManager server,"
                               f" server_broken_reason: {self.server_broken_reason}")

    def break_manager(self, reason: str, test: str) -> None:
        """Make ScyllaClusterManager not operable from client side."""

        self.server_broken_reason = f"{reason}, test case {test} BROKE ScyllaClusterManager"
        self.logger.error(self.server_broken_reason)
        self.server_broken_event.set()

    @manager_op(blockable=True)
    async def mark_dirty(self) -> None:
        """Mark current cluster dirty."""

        self.cluster.is_dirty = True

    @manager_op(blockable=True)
    async def _server_stop(self, server_id: ServerNum) -> None:
        """Stop a server. No-op if already stopped."""

        await self.cluster.server_stop(server_id, gracefully=False)

    @manager_op(blockable=True, timeout=None)
    async def _server_stop_gracefully(self, server_id: ServerNum) -> None:
        await self.cluster.server_stop(server_id, gracefully=True)

    @manager_op(blockable=True, timeout=None)
    async def _server_start(self,
                            server_id: ServerNum,
                            *,
                            expected_error: str | None = None,
                            seeds: list[IPAddress] | None = None,
                            expected_server_up_state: ServerUpState = ServerUpState.SERVING,
                            cmdline_options_override: list[str] | None = None,
                            append_env_override: dict[str, str] | None = None,
                            auth_provider: dict[str, str] | None = None) -> None:
        """Start a specified server (must be stopped.)"""

        await self.cluster.server_start(
            server_id=server_id,
            expected_error=expected_error,
            seeds=seeds,
            expected_server_up_state=expected_server_up_state,
            cmdline_options_override=cmdline_options_override,
            append_env_override=append_env_override,
            auth_provider=auth_provider,
        )

    @manager_op(blockable=True)
    async def server_pause(self, server_id: ServerNum) -> None:
        """Pause the specified server."""

        self.cluster.server_pause(server_id)

    @manager_op(blockable=True)
    async def server_unpause(self, server_id: ServerNum) -> None:
        """Unpause the specified server."""

        self.cluster.server_unpause(server_id)

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
        # bind_to_current_loop() has to run here, on the caller's loop, not
        # inside the operation, which runs on the manager's.
        await self._add_teardown_callback(bind_to_current_loop(callback),
                                          name or getattr(callback, "__qualname__", repr(callback)))

    @manager_op(blockable=True)
    async def _add_teardown_callback(self, callback: Callable[[], Awaitable[None]], name: str) -> None:
        self.cluster.add_teardown_callback(callback, name)

    @manager_op(blockable=True, timeout=None)
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
                         expected_server_up_state: ServerUpState = ServerUpState.SERVING) -> ServerInfo:
        """Add a new server."""

        return await self.cluster.add_server(
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
        )

    @manager_op(blockable=True, timeout=None)
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

        return await self.cluster.add_servers(servers_num, cmdline, config, version, property_file,
                                              start, seeds, server_encryption, expected_error)

    @manager_op(blockable=True, timeout=None)
    async def _remove_node(self,
                           initiator_id: ServerNum,
                           server_id: ServerNum,
                           ignore_dead: list[IPAddress | HostID],
                           expected_error: str | None) -> None:
        """Run remove node on Scylla REST API for a specified server."""

        assert initiator_id in self.cluster.running, f"Initiator {initiator_id} is not running"
        if server_id in self.cluster.running:
            self.logger.warning("remove_node %s is a running node", server_id)
        else:
            assert server_id in self.cluster.stopped, f"remove_node: {server_id} unknown"
        to_remove = self.cluster.servers[server_id]
        initiator = self.cluster.servers[initiator_id]
        self.logger.info("remove_node %s with initiator %s", to_remove, initiator)

        # initiate remove
        try:
            await self.cluster.api.remove_node(initiator.ip_addr,
                                               await to_remove.get_host_id(self.cluster.api),
                                               ignore_dead,
                                               timeout=ScyllaServer.TOPOLOGY_TIMEOUT)
        except (RuntimeError, HTTPError) as exc:
            if expected_error:
                if expected_error not in str(exc):
                    raise RuntimeError(
                        f"removenode failed (initiator: {initiator}, to_remove: {to_remove},"
                        f" ignore_dead: {ignore_dead}) but did not contain expected error (\"{expected_error}\"),"
                        f"check log file at {initiator.log_filename}, error: \"{exc}\"")
                else:
                    self.logger.info("removenode (initiator: %s, to_remove: %s, ignore_dead: %s) failed as expected: %s",
                                     initiator, to_remove, ignore_dead, exc)
            else:
                raise RuntimeError(
                    f"removenode failed (initiator: {initiator}, to_remove: {to_remove},"
                    f" ignore_dead: {ignore_dead}), check log file at {initiator.log_filename},"
                    f" error: \"{exc}\"")
        else:
            self.cluster.server_mark_removed(server_id)
            if expected_error:
                raise RuntimeError(
                    f"removenode succeeded when it should have failed (initiator: {initiator},"
                    f"to_remove: {to_remove}, ignore_dead: {ignore_dead}, expected error: \"{expected_error}\"),"
                    f" check log file at {initiator.log_filename}")
            self.logger.info("removenode (initiator: %s, to_remove: %s, ignore_dead: %s) succeeded",
                             initiator, to_remove, ignore_dead)

    @manager_op(blockable=True, timeout=None)
    async def _decommission_node(self, server_id: ServerNum, expected_error: str | None) -> None:
        """Run decommission node on Scylla REST API for a specified server."""

        self.logger.info("decommission_node %s", server_id)
        assert server_id in self.cluster.running, "Can't decommission not running node"
        if len(self.cluster.running) == 1:
            self.logger.warning("decommission_node %s is only running node left", server_id)
        server = self.cluster.running[server_id]
        try:
            await self.cluster.api.decommission_node(server.ip_addr, timeout=ScyllaServer.TOPOLOGY_TIMEOUT)
        except (RuntimeError, HTTPError) as exc:
            if expected_error:
                if expected_error not in str(exc):
                    raise RuntimeError(
                        f"decommission failed (server: {server}) but did not contain expected error"
                        f"(\"{expected_error}\", check log file at {server.log_filename}, error: \"{exc}\"")
                else:
                    return
            else:
                raise RuntimeError(
                    f"decommission failed (server: {server}), check log at {server.log_filename},"
                    f" error: \"{exc}\"")
        else:
            if expected_error:
                await self.cluster.server_stop(server_id, gracefully=True)
                raise RuntimeError(
                    f"decommission succeeded when it should have failed (server: {server},"
                    f" expected_error: \"{expected_error}\"), check log file at {server.log_filename}")

        await self.cluster.server_stop(server_id, gracefully=True)

    @manager_op(blockable=True, timeout=None)
    async def _rebuild_node(self, server_id: ServerNum, expected_error: str | None) -> None:
        """Run rebuild node on Scylla REST API for a specified server."""

        self.logger.info("rebuild_node %s", server_id)
        assert server_id in self.cluster.running, "Can't rebuild not running node"
        server = self.cluster.running[server_id]
        try:
            await self.cluster.api.rebuild_node(server.ip_addr, timeout=ScyllaServer.TOPOLOGY_TIMEOUT)
        except (RuntimeError, HTTPError) as exc:
            if expected_error:
                if expected_error not in str(exc):
                    raise RuntimeError(
                            f"rebuild failed (server: {server}) but did not contain expected error"
                            f"(\"{expected_error}\", check log file at {server.log_filename}, error: \"{exc}\"")
                else:
                    return
            else:
                raise RuntimeError(
                    f"rebuild failed (server: {server}), check log at {server.log_filename},"
                    f" error: \"{exc}\"")
        else:
            if expected_error:
                raise RuntimeError(
                    f"rebuild succeeded when it should have failed (server: {server},"
                    f" expected_error: \"{expected_error}\"), check log file at {server.log_filename}")

        await self.cluster.server_stop(server_id, gracefully=True)

    @manager_op
    async def server_get_config(self, server_id: ServerNum) -> dict[str, object]:
        """Get conf/scylla.yaml of the given server as a dictionary."""

        return self.cluster.get_config(server_id)

    @manager_op(blockable=True)
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
        Mark the cluster as dirty.
        """
        if key is not None:
            if value is None:
                raise RuntimeError("`value` is required if `key` is not None")
            if config_options is not None:
                raise RuntimeError("`key: value` pair and `config_options` dict can't be used simultaneously")
            config_options = {key: value}
        elif not isinstance(config_options, dict):
            raise RuntimeError(f"`config_options` is expected to be a dict, not {type(config_options)}")
        self.cluster.update_config(server_id=server_id, config_options=config_options)

    @manager_op(blockable=True)
    async def server_remove_config_option(self, server_id: ServerNum, key: str) -> None:
        """Remove an option from conf/scylla.yaml of the given server.

        If the server is running, reload the config with a SIGHUP.
        Mark the cluster as dirty.
        """
        self.cluster.remove_config_option(server_id=server_id, key=key)

    @manager_op(blockable=True)
    async def server_update_cmdline(self, server_id: ServerNum, cmdline_options: list[str]) -> None:
        """Update the command-line options of the given server by merging the new options into the existing ones.

        The update only takes effect after restart.
        Marks the cluster as dirty.
        """
        self.cluster.update_cmdline(server_id, cmdline_options)

    @manager_op(blockable=True)
    async def server_switch_executable(self, server_id: ServerNum, path: str) -> None:
        """Switch the executable of the server to the one specified by 'path'.

        Marks the cluster as dirty.
        """
        self.cluster.server_switch_executable(server_id, path)

    @manager_op(blockable=True)
    async def server_change_ip(self, server_id: ServerNum) -> IPAddress:
        """Pass change_ip command for the given server to the cluster."""

        return await self.cluster.change_ip(server_id)

    @manager_op(blockable=True)
    async def server_change_rpc_address(self, server_id: ServerNum) -> IPAddress:
        """Pass change_rpc_address command for the given server to the cluster."""

        return await self.cluster.change_rpc_address(server_id)

    @manager_op
    async def server_get_log_filename(self, server_id: ServerNum) -> str:
        return str(self.cluster.server(server_id).log_filename)

    @manager_op
    async def server_get_workdir(self, server_id: ServerNum) -> str:
        return str(self.cluster.server(server_id).workdir)

    @manager_op
    async def server_get_maintenance_socket_path(self, server_id: ServerNum) -> str:
        return self.cluster.server(server_id).maintenance_socket_path

    @manager_op
    async def server_get_exe(self, server_id: ServerNum) -> str:
        return str(self.cluster.server(server_id).exe)

    @manager_op
    async def server_is_alive(self, server_id: ServerNum) -> bool:
        """Whether the server has a process which has not exited yet.

        Unlike ScyllaServer.is_running, which only tells a started server from
        a stopped one, this notices a server whose process died on its own.
        """
        cmd = self.cluster.server(server_id).cmd
        return cmd is not None and cmd.returncode is None

    @manager_op(blockable=True)
    async def server_wipe_sstables(self, server_id: ServerNum, keyspace: str, table: str) -> None:
        return self.cluster.wipe_sstables(server_id, keyspace, table)

    @manager_op
    async def server_get_sstables_disk_usage(self, server_id: ServerNum, keyspace: str, table: str) -> int:
        return self.cluster.get_sstables_disk_usage(server_id, keyspace, table)

    @manager_op
    async def server_get_process_status(self, server_id: ServerNum) -> str | None:
        return self.cluster.server_get_process_status(server_id)

    def con_gen(self,
                hosts: list[IPAddress | EndPoint],
                port: int = 9042,
                use_ssl: bool = False,
                auth_provider: AuthProvider | None = None,
                load_balancing_policy: LoadBalancingPolicy = RoundRobinPolicy()) -> CassandraCluster:
        """Create a CQL Cluster connection object according to configuration.

        It does not .connect() yet.
        """
        assert hosts, "python driver connection needs at least one host to connect to"
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
        return CassandraCluster(
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
            ssl_context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT) if use_ssl else None,
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
            reconnection_policy=ExponentialReconnectionPolicy(1.0, 4.0),
            auth_provider=auth_provider,
            # Capture messages for debugging purposes.
            connection_class=CustomConnection,
        )

    @fenced
    async def driver_connect(self, server: ServerInfo | None = None, auth_provider: AuthProvider | None = None) -> None:
        """Connect to cluster."""

        targets = [server] if server else await self.running_servers()
        servers = [s_info.rpc_address for s_info in targets]
        # avoids leaking connections if driver wasn't closed before
        self._driver_close()
        self.logger.debug("driver connecting to %s", servers)
        self.ccluster = self.con_gen(
            servers,
            self.port,
            self.use_ssl,
            auth_provider if auth_provider else self.auth_provider,
            self.load_balancing_policy,
        )
        self.cql = self.ccluster.connect()

    @fenced
    def driver_close(self) -> None:
        """Disconnect from cluster."""

        self._driver_close()

    def _driver_close(self) -> None:
        """Disconnect from cluster, also when no test is running (see stop())."""

        for cluster in self.exclusive_clusters:
            safe_driver_shutdown(cluster)
        self.exclusive_clusters.clear()
        if self.ccluster is not None:
            self.logger.debug("shutting down driver")
            safe_driver_shutdown(self.ccluster)
            self.ccluster = None
        self.cql = None

    @fenced
    def get_cql(self) -> CassandraSession:
        """Precondition: driver is connected."""

        assert self.cql
        return self.cql

    async def get_ready_cql(self, servers: list[ServerInfo]) -> tuple[CassandraSession, list[Host]]:
        """Precondition: driver is connected."""

        cql = self.get_cql()
        await self.servers_see_each_other(servers)
        hosts = await wait_for_cql_and_get_hosts(cql, servers, time() + 60)
        return cql, hosts

    @fenced
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

    async def gather_related_logs(self, failed_test_path_dir: Path, logs: dict[str, Path]) -> None:
        for server in await self.all_servers():
            log_file = await self.server_open_log(server_id=server.server_id)
            shutil.copyfile(log_file.file, failed_test_path_dir / f"{pathlib.Path(log_file.file).name}")
            allure.attach(log_file.file.read_bytes(), name=log_file.file.name, attachment_type=allure.attachment_type.TEXT)
        for name, log in logs.items():
            # A log is missing when the handler that writes it was never installed, e.g. the
            # test failed before before-test ran. Skip it: the caller collects the rest of the
            # artifacts after this returns, so raising here would lose them all.
            if not log.is_file():
                self.logger.warning("Log %s (%s) does not exist, not attaching it", name, log)
                continue
            allure.attach(log.read_bytes(), name=name, attachment_type=allure.attachment_type.TEXT)
            shutil.copyfile(log, failed_test_path_dir / name)

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
        await self._server_stop(server_id)
        if host_id is not None:
            try:
                await self.convict_on_all(host_id)
            except Exception:
                # It's a best-effort attempt, ignore errors
                # In some scenarios errors are expected, e.g. when server_stop() is called concurrently on many servers.
                pass

    async def server_stop_gracefully(self, server_id: ServerNum, timeout: float | None = None) -> None:
        """Stop specified server gracefully.

        With no timeout given, outlast the server's own graceful-stop timeout
        for this build mode, so that a slow stop is reported by the server --
        which knows why it was slow -- instead of timing out here first.
        """
        assert self.cluster, "ScyllaClusterManager is not running"
        if timeout is None:
            timeout = graceful_stop_timeout(self.cluster.mode) + 60

        async with asyncio.timeout(timeout):
            await self._server_stop_gracefully(server_id)

    async def disable_tablet_balancing(self) -> None:
        """Disables background tablet load-balancing.

        If there are already active migrations, it waits for them to finish before returning.
        Doesn't block migrations on behalf of node operations like decommission, removenode or replace.
        """
        servers = await self.running_servers()
        if not servers:
            raise RuntimeError("No running servers")
        # Any server will do, it's a group0 operation
        await self.api.disable_tablet_balancing(servers[0].ip_addr)

    async def enable_tablet_balancing(self) -> None:
        """Enables background tablet load-balancing."""

        servers = await self.running_servers()
        if not servers:
            raise RuntimeError("No running servers")
        # Any server will do, it's a group0 operation
        await self.api.enable_tablet_balancing(servers[0].ip_addr)

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
                           seeds: list[IPAddress] | None = None,
                           timeout: float | None = ScyllaServer.TOPOLOGY_TIMEOUT,
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

        async with asyncio.timeout(timeout):
            await self._server_start(
                server_id,
                expected_error=expected_error,
                seeds=seeds,
                expected_server_up_state=expected_server_up_state,
                cmdline_options_override=cmdline_options_override,
                append_env_override=append_env_override,
                auth_provider=auth_provider,
            )
        await self.server_sees_others(server_id, wait_others, interval = wait_interval)
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

    async def server_change_version(self, server_id: ServerNum, exe: str) -> None:
        """Upgrade a running Scylla node by switching it to a new binary version specified by the 'exe' parameter."""

        await self.server_stop_gracefully(server_id)
        await self.server_switch_executable(server_id, exe)
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
                         connect_driver: bool = True) -> ServerInfo:
        """Add a new server.

        When start=True and expected_error is None, waits until Scylla reports
        that all configured listeners (including any non-default ports) are ready
        (ServerUpState.SERVING). Pass a lower expected_server_up_state to return
        earlier. When start=False or expected_error is set, no readiness wait is
        performed. When connect_driver=False, the effective wait state is capped
        at HOST_ID_QUERIED regardless of expected_server_up_state.
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
                )
        except Exception as exc:
            raise RuntimeError("Failed to add server") from exc
        self.logger.debug("ScyllaClusterManager added %s", s_info)
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
        assert not (property_file and auto_rack_dc), f"Either property_file or auto_rack_dc can be provided, but not both"

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
        self.logger.debug("ScyllaClusterManager added %s", s_infos)
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
        rows = await self.get_cql().run_async(
            f"select id from system_schema.tables where keyspace_name = '{keyspace}' and table_name = '{table}'"
        )
        return rows[0].id

    async def get_view_id(self, keyspace: str, view: str) -> uuid.UUID:
        rows = await self.get_cql().run_async(
            f"select id from system_schema.views where keyspace_name = '{keyspace}' and view_name = '{view}'"
        )
        return rows[0].id

    async def get_table_or_view_id(self, keyspace: str, table: str) -> uuid.UUID:
        rows = await self.get_cql().run_async(
            f"select id from system_schema.tables where keyspace_name = '{keyspace}' and table_name = '{table}'"
        )
        if not rows:
            rows = await self.get_cql().run_async(
                f"select id from system_schema.views where keyspace_name = '{keyspace}' and view_name = '{table}'"
            )
        return rows[0].id

    async def server_sees_others(self, server_id: ServerNum, count: int, interval: float = 45.) -> None:
        """Wait till a server sees a minimum given count of other servers."""

        if count < 1:
            return
        server_ip = await self.get_host_ip(server_id)

        async def _sees_min_others() -> bool | None:
            alive_nodes = await self.api.get_alive_endpoints(server_ip)
            if len(alive_nodes) > count:
                return True
            return None

        await wait_for(_sees_min_others, time() + interval, period=.5)

    async def server_sees_other_server(self,
                                       server_ip: IPAddress,
                                       other_ip: IPAddress,
                                       interval: float = 45.) -> None:
        """Wait till a server sees another specific server IP as alive."""

        async def _sees_another_server() -> bool | None:
            alive_nodes = await self.api.get_alive_endpoints(server_ip)
            if other_ip in alive_nodes:
                return True
            return None

        await wait_for(_sees_another_server, time() + interval, period=.5)

    async def servers_see_each_other(self, servers: list[ServerInfo], interval: float = 45.) -> None:
        """Wait till all servers see all other servers in the list."""

        others = [self.server_sees_others(srv.server_id, len(servers) - 1, interval) for srv in servers]
        await gather_safely(*others)

    async def server_not_sees_other_server(self,
                                           server_ip: IPAddress,
                                           other_ip: IPAddress,
                                           interval: float = 45.) -> None:
        """Wait till a server sees another specific server IP as dead."""

        async def _not_sees_another_server() -> bool | None:
            alive_nodes = await self.api.get_alive_endpoints(server_ip)
            if other_ip not in alive_nodes:
                return True
            return None

        await wait_for(_not_sees_another_server, time() + interval, period=.5)

    async def others_not_see_server(self, server_ip: IPAddress, interval: float = 45.) -> None:
        """Wait till a server is seen as dead by all other running servers in the cluster."""

        others_ips = [srv.ip_addr for srv in await self.running_servers() if srv.ip_addr != server_ip]
        await gather_safely(*(self.server_not_sees_other_server(ip, server_ip, interval) for ip in others_ips))

    async def server_open_log(self, server_id: ServerNum) -> ScyllaLogFile:
        log_filename = await self.server_get_log_filename(server_id)
        return ScyllaLogFile(self.thread_pool, log_filename)
