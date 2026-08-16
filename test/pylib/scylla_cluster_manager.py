#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Manager for a Scylla cluster used by test cases.
   Provides an async API for tests to request changes in the cluster.
"""
import asyncio
import logging
import pathlib
import traceback
from functools import partial, wraps
from typing import Any, Awaitable, Callable

from test.pylib.internal_types import ServerNum, IPAddress, HostID, ServerInfo, ServerUpState
from test.pylib.rest_client import HTTPError
from test.pylib.scylla_cluster import ClusterFactory, ReplaceConfig, ScyllaCluster
from test.pylib.scylla_server import ScyllaServer, ScyllaVersionDescription
from test.pylib.util import LogPrefixAdapter


def manager_op(entry_point: Callable | None = None, *, blockable: bool = False):
    """Run a ScyllaClusterManager entry point as its own task and track it.

    Usable bare, as @manager_op, or with arguments, as @manager_op(blockable=True).

    Every entry point works on the current cluster, so the check that there is
    one lives here instead of being repeated in each of them.

    The rest mirrors what the aiohttp server used to do for request handlers:
    - refuse the operation on a broken manager (blockable=True, which the
      server applied to every mutating route);
    - register the operation in tasks_history, so after_test() can drain
      operations a test left running;
    - shield the operation from the caller's cancellation: a timed-out or
      cancelled caller orphans the operation -- like an HTTP client
      disconnect used to, since the server never cancelled handlers -- and
      after_test() picks the orphan up;
    - log failures with their traceback into the per-test cluster log
      (see _op_finished), which the server's catching handler used to do.
    """
    def decorator(fn: Callable) -> Callable:
        @wraps(fn)
        async def wrapper(self, *args, **kwargs):
            assert self.cluster, "ScyllaClusterManager is not running"
            if blockable and self.server_broken_event.is_set():
                raise RuntimeError(f"ScyllaClusterManager BROKEN, Previous test broke ScyllaClusterManager server,"
                                   f" server_broken_reason: {self.server_broken_reason}")
            descr = f"{fn.__name__}{args}"
            op = asyncio.ensure_future(fn(self, *args, **kwargs))
            self.logger.info("[ScyllaClusterManager][%s] %s", op.get_name(), descr)
            self.tasks_history[op] = descr
            op.add_done_callback(partial(self._op_finished, descr=descr))
            return await asyncio.shield(op)
        return wrapper
    return decorator if entry_point is None else decorator(entry_point)


class ScyllaClusterManager:
    """Manages a Scylla cluster for running test cases
       Provides an async API for tests to request changes in the Cluster.
       Parallel requests are not supported.
    """
    # pylint: disable=too-many-instance-attributes
    cluster: ScyllaCluster
    is_after_test_ok: bool

    def __init__(self,
                 test_uname: str,
                 create_cluster: ClusterFactory,
                 base_dir: str) -> None:
        self.test_uname: str = test_uname
        self.base_dir: str = base_dir
        logger = logging.getLogger(self.test_uname)
        self.logger = LogPrefixAdapter(logger, {'prefix': self.test_uname})
        # The currently running test case with self.test_uname prepended, e.g.
        # test_topology.1::test_add_server_add_column
        self.current_test_case_full_name: str = ''
        self.cluster: ScyllaCluster | None = None
        self.create_cluster: ClusterFactory = create_cluster
        self.is_running: bool = False
        self.is_before_test_ok: bool = False
        self.is_after_test_ok: bool = False
        self.tasks_history = dict()
        self.server_broken_event = asyncio.Event()
        self.server_broken_reason = ""

    def repr_tasks_history(self):
        out = "Cluster_history"
        for key, val in self.tasks_history.items():
            out += f"\n{val}:\t\t{repr(key)}"
        return out

    async def start(self) -> None:
        """Get first cluster"""
        if self.is_running:
            self.logger.warning("ScyllaClusterManager already running")
            return
        self.cluster = await self.create_cluster(self.logger)
        self.logger.info("First Scylla cluster: %s", self.cluster)
        self.cluster.setLogger(self.logger)
        self.is_running = True

    @manager_op(blockable=True)
    async def before_test(self, test_case_name: str) -> str:
        self.current_test_case_full_name = f'{self.test_uname}::{test_case_name}'
        root_logger = logging.getLogger()
        # file handler file name should be consistent with topology/conftest.py:manager test_py_log_test variable
        parent_test_name = pathlib.Path(self.test_uname.replace('/', '_')).stem
        self.test_case_log_fh = logging.FileHandler(f"{self.base_dir}/{parent_test_name}.{test_case_name}_cluster.log")
        self.test_case_log_fh.setLevel(root_logger.getEffectiveLevel())
        # to have the custom formatter with a timestamp that used in a test.py but for each testcase's log, we need to
        # extract it from the root logger and apply to the handler
        self.test_case_log_fh.setFormatter(root_logger.handlers[0].formatter)
        root_logger.addHandler(self.test_case_log_fh)
        self.logger.info("Setting up %s", self.current_test_case_full_name)
        if self.cluster.is_dirty:
            self.logger.info(f"Current cluster %s is dirty after test %s, replacing with a new one...",
                             self.cluster.name, self.current_test_case_full_name)
            await self.cluster.recycle()
            self.cluster = await self.create_cluster(self.logger)
            self.logger.info("Got new Scylla cluster: %s", self.cluster.name)
        self.cluster.setLogger(self.logger)
        self.logger.info("Leasing Scylla cluster %s for test %s", self.cluster, self.current_test_case_full_name)
        self.cluster.before_test(self.current_test_case_full_name)
        self.is_before_test_ok = True
        self.cluster.take_log_savepoint()
        return str(self.cluster)

    async def stop(self) -> None:
        """Dispose of the last cluster if present"""
        self.logger.info("ScyllaManager stopping for test %s", self.test_uname)
        if self.cluster:
            self.logger.info("ScyllaManager: stopping Scylla cluster %s after %s",
                             self.cluster, self.test_uname)
            await self.cluster.recycle()
            self.cluster = None
        self.is_running = False

    @manager_op
    async def is_dirty(self) -> bool:
        """Report if current cluster is dirty"""
        return self.cluster.is_dirty

    @manager_op
    async def running_servers(self) -> list[ServerInfo]:
        """Return server info for running servers"""
        return self.cluster.running_servers()

    @manager_op
    async def all_servers(self) -> list[ServerInfo]:
        """Return server info for all servers"""
        return self.cluster.all_servers()

    @manager_op
    async def starting_servers(self) -> list[ServerInfo]:
        """Return server info for servers which are currently starting"""
        return self.cluster.starting_servers()

    @manager_op
    async def get_host_ip(self, server_id: ServerNum) -> IPAddress:
        """IP address of a server"""
        return self.cluster.servers[server_id].ip_addr

    @manager_op
    async def get_host_id(self, server_id: ServerNum) -> HostID:
        """Host ID of a server."""
        return await self.cluster.servers[server_id].get_host_id(self.cluster.api)

    @manager_op(blockable=True)
    async def after_test(self, success: bool) -> dict[str, Any]:
        assert self.current_test_case_full_name
        self.logger.info(self.repr_tasks_history())
        # Drop our own entry: the drain below must not wait for itself.
        self.tasks_history.pop(asyncio.current_task(), None)
        # wait for the operations the test left running
        for task, descr in list(self.tasks_history.items()):
            if not task.done():
                self.logger.info("wait for task:%s, op:%s", task, descr)
                try:
                    await asyncio.wait_for(task, timeout=120)
                except asyncio.TimeoutError:
                    self.break_manager(f"error on waiting coro {task.get_name()}", self.current_test_case_full_name)
                    break
                except Exception:
                    # The operation failed after its caller went away.  It was
                    # already logged by _op_finished; completing is all the
                    # drain needs.
                    pass

        # check on tasks leakage: finished operations remove themselves from
        # tasks_history, so anything left after the drain is a new operation
        # started behind our back while the test was already over.
        await asyncio.sleep(0.1)
        if self.tasks_history:
            self.break_manager(f"tasks leakage found  {self.tasks_history}", self.current_test_case_full_name)

        self.logger.info("Test %s %s, cluster: %s", self.current_test_case_full_name,
                         "SUCCEEDED" if success else "FAILED", self.cluster)
        try:
            self.cluster.after_test(self.current_test_case_full_name, success)
        finally:
            logging.getLogger().removeHandler(self.test_case_log_fh)
            if success:
                pathlib.Path(self.test_case_log_fh.baseFilename).unlink()
            self.current_test_case_full_name = ''
        self.is_after_test_ok = True
        cluster_str = str(self.cluster)

        return {"cluster_str":cluster_str, "server_broken":self.server_broken_event.is_set(), "message": self.server_broken_reason }

    def break_manager(self, reason, test):
        # make ScyllaClusterManager not operatable from client side
        self.server_broken_reason = f"{reason}, test case {test} BROKE ScyllaClusterManager"
        self.logger.error(self.server_broken_reason)
        self.server_broken_event.set()

    def _op_finished(self, op: asyncio.Task, descr: str) -> None:
        """Done callback of manager_op operations.

        Drops the finished operation from tasks_history and logs its
        failure, if any.  Retrieving the exception here also keeps an
        operation orphaned by a cancelled caller from warning "exception
        was never retrieved".
        """
        self.tasks_history.pop(op, None)
        if op.cancelled():
            return
        if (exc := op.exception()) is not None:
            self.logger.error("Exception when executing %s: %s\n%s",
                              descr, exc, "".join(traceback.format_exception(exc)))

    @manager_op(blockable=True)
    async def mark_dirty(self) -> None:
        """Mark current cluster dirty"""
        self.cluster.is_dirty = True

    @manager_op(blockable=True)
    async def mark_clean(self) -> None:
        """Mark current cluster clean"""
        self.cluster.is_dirty = False
        self.cluster.keyspace_count = self.cluster._get_keyspace_count()

    @manager_op(blockable=True)
    async def server_stop(self, server_id: ServerNum) -> None:
        """Stop a server. No-op if already stopped."""
        await self.cluster.server_stop(server_id, gracefully=False)

    @manager_op(blockable=True)
    async def server_stop_gracefully(self, server_id: ServerNum) -> None:
        """Stop a server gracefully. No-op if already stopped."""
        await self.cluster.server_stop(server_id, gracefully=True)

    @manager_op(blockable=True)
    async def server_start(self, server_id: ServerNum, *,
                           expected_error: str | None = None,
                           seeds: list[IPAddress] | None = None,
                           expected_server_up_state: ServerUpState = ServerUpState.SERVING,
                           cmdline_options_override: list[str] | None = None,
                           append_env_override: dict[str, str] | None = None,
                           auth_provider: dict[str, str] | None = None) -> None:
        """Start a specified server (must be stopped)"""
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

    @manager_op(blockable=True)
    async def add_teardown_callback(self, callback: Callable[[], Awaitable[None]], name: str) -> None:
        """Register a cleanup to run when the current cluster is recycled."""
        self.cluster.add_teardown_callback(callback, name)

    @manager_op(blockable=True)
    async def server_add(self,
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

    @manager_op(blockable=True)
    async def servers_add(self,
                          servers_num: int = 1,
                          cmdline: list[str] | None = None,
                          config: dict[str, Any] | None = None,
                          version: ScyllaVersionDescription | None = None,
                          property_file: list[dict[str, Any]] | dict[str, Any] | None = None,
                          start: bool = True,
                          seeds: list[IPAddress] | None = None,
                          server_encryption: str = "none",
                          expected_error: str | None = None) -> list[ServerInfo]:
        """Add new servers concurrently"""
        return await self.cluster.add_servers(servers_num, cmdline, config, version, property_file,
                                              start, seeds, server_encryption, expected_error)

    @manager_op(blockable=True)
    async def remove_node(self, initiator_id: ServerNum, server_id: ServerNum,
                          ignore_dead: list[IPAddress], expected_error: str | None) -> None:
        """Run remove node on Scylla REST API for a specified server"""
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
                    self.logger.info(f"removenode (initiator: {initiator}, to_remove: {to_remove}, ignore_dead: {ignore_dead})"
                                     f" failed as expected: {exc}")
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
            self.logger.info(f"removenode (initiator: {initiator}, to_remove: {to_remove}, ignore_dead: {ignore_dead}) succeeded")

    @manager_op(blockable=True)
    async def decommission_node(self, server_id: ServerNum, expected_error: str | None) -> None:
        """Run decommission node on Scylla REST API for a specified server"""
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

    @manager_op(blockable=True)
    async def rebuild_node(self, server_id: ServerNum, expected_error: str | None) -> None:
        """Run rebuild node on Scylla REST API for a specified server"""
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
    async def server_update_config(self, server_id: ServerNum, config_options: dict[str, Any]) -> None:
        """Update conf/scylla.yaml of the given server with `config_options` dict.

        If the server is running, reload the config with a SIGHUP.
        Mark the cluster as dirty.
        """
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
           Marks the cluster as dirty."""
        self.cluster.update_cmdline(server_id, cmdline_options)

    @manager_op(blockable=True)
    async def server_switch_executable(self, server_id: ServerNum, path: str) -> None:
        """Switch the executable of the server to the one specified by 'path'
           Marks the cluster as dirty."""
        self.cluster.server_switch_executable(server_id, path)

    @manager_op(blockable=True)
    async def server_change_ip(self, server_id: ServerNum) -> IPAddress:
        """Pass change_ip command for the given server to the cluster"""
        return await self.cluster.change_ip(server_id)

    @manager_op(blockable=True)
    async def server_change_rpc_address(self, server_id: ServerNum) -> IPAddress:
        """Pass change_rpc_address command for the given server to the cluster"""
        return await self.cluster.change_rpc_address(server_id)

    def _server_get_attribute(self, server_id: ServerNum, attribute: str):
        """Get a particular attribute of a ScyllaServer instance

        To be used to implement concrete entry points, not for direct use.
        """
        assert self.cluster
        assert server_id in self.cluster.servers, f"Server {server_id} unknown"
        return getattr(self.cluster.servers[server_id], attribute)

    @manager_op
    async def server_get_log_filename(self, server_id: ServerNum) -> str:
        return str(self._server_get_attribute(server_id, "log_filename"))

    @manager_op
    async def server_get_workdir(self, server_id: ServerNum) -> str:
        return str(self._server_get_attribute(server_id, "workdir"))

    @manager_op
    async def server_get_maintenance_socket_path(self, server_id: ServerNum) -> str:
        return str(self._server_get_attribute(server_id, "maintenance_socket_path"))

    @manager_op
    async def server_get_exe(self, server_id: ServerNum) -> str:
        return str(self._server_get_attribute(server_id, "exe"))

    @manager_op
    async def server_is_alive(self, server_id: ServerNum) -> bool:
        """Whether the server has a process which has not exited yet.

        Unlike ScyllaServer.is_running, which only tells a started server from
        a stopped one, this notices a server whose process died on its own."""
        cmd = self._server_get_attribute(server_id, "cmd")
        return cmd is not None and cmd.returncode is None

    @manager_op(blockable=True)
    async def server_wipe_sstables(self, server_id: ServerNum, keyspace: str, table: str):
        return self.cluster.wipe_sstables(server_id, keyspace, table)

    @manager_op
    async def server_get_sstables_disk_usage(self, server_id: ServerNum, keyspace: str, table: str) -> int:
        return self.cluster.get_sstables_disk_usage(server_id, keyspace, table)

    @manager_op
    async def server_get_process_status(self, server_id: ServerNum) -> str | None:
        return self.cluster.server_get_process_status(server_id)
