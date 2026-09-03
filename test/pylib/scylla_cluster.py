#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Scylla clusters for testing.
   Provides helpers to setup and manage clusters of Scylla servers for testing.
"""
import asyncio
import copy
import importlib
import itertools
import logging
import pathlib
import uuid
from collections import ChainMap
from functools import reduce
from typing import Any, Awaitable, Callable, Dict, List, NamedTuple, Optional, Set, Tuple, Union

import psutil

from test.pylib.host_registry import Host, HostRegistry
from test.pylib.internal_types import ServerNum, IPAddress, HostID, ServerInfo, ServerUpState
from test.pylib.rest_client import ScyllaRESTAPIClient
from test.pylib.scylla_server import (
    SCYLLA_CMDLINE_OPTIONS,
    ScyllaServer,
    ScyllaVersionDescription,
    get_current_version_description,
    make_scylla_conf,
    merge_cmdline_options,
)
from test.pylib.util import gather_safely, graceful_stop_timeout


type ClusterFactory = Callable[[logging.Logger | logging.LoggerAdapter], Awaitable[ScyllaCluster]]


def bind_to_current_loop(callback: Callable[[], Any]) -> Callable[[], Awaitable[None]]:
    """Wrap `callback` so that it can be awaited from any loop.

    A cluster is recycled on the ScyllaClusterManager's own loop, in its own
    thread, while the teardown callbacks own objects the fixtures created on a
    different loop -- a subprocess handle awaited from the wrong loop hangs
    instead of failing.  Binding here lets the cluster keep a plain awaitable
    and fire it without knowing which loop is underneath.
    """
    owner = asyncio.get_running_loop()

    async def invoke() -> None:
        res = callback()
        if asyncio.iscoroutine(res):
            if asyncio.get_running_loop() is owner:
                await res
            else:
                await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(res, owner))

    return invoke


class ReplaceConfig(NamedTuple):
    replaced_id: ServerNum
    reuse_ip_addr: bool
    use_host_id: bool
    ignore_dead_nodes: list[IPAddress | HostID] = []
    wait_dead: bool = True


class ScyllaCluster:
    """A cluster of Scylla servers providing an API for changes"""
    # pylint: disable=too-many-instance-attributes

    def __init__(self,
                 logger: Union[logging.Logger, logging.LoggerAdapter],
                 vardir: pathlib.Path,
                 replicas: int,
                 mode: str,
                 cmdline_options: str | list[str],
                 cmdline_options_override: list[str],
                 config_options: dict[str, Any],
                 append_env: dict[str, str],
                 scylla_exe: str,
                 save_log_on_success: bool = False) -> None:
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
        self.replicas = replicas
        # Every ScyllaServer is in one of self.running, self.stopped.
        # These dicts are disjoint.
        # A server ID present in self.removed may be either in self.running or in self.stopped.
        self.running: Dict[ServerNum, ScyllaServer] = {}        # started servers
        self.stopped: Dict[ServerNum, ScyllaServer] = {}        # servers no longer running but present
        self.servers = ChainMap(self.running, self.stopped)
        self.removed: Set[ServerNum] = set()                    # removed servers (might be running)
        self.starting: Dict[ServerNum, ScyllaServer] = {}       # servers starting right now, not yet running (and not included in "servers").
        # The first IP assigned to a server added to the cluster.
        self.initial_seed: Optional[IPAddress] = None
        # cluster is started (but it might not have running servers)
        self.is_running: bool = False
        # cluster was modified in a way it should not be used in subsequent tests
        self.is_dirty: bool = False
        self.start_exception: Optional[Exception] = None
        self.keyspace_count = 0
        self.api = ScyllaRESTAPIClient()
        self.stop_lock = asyncio.Lock()
        # Cleanups a test registered through ScyllaClusterManager.add_teardown_callback(),
        # as (callback, name) pairs.  They are fired by run_teardown_callbacks()
        # when this cluster is recycled.
        self.teardown_callbacks: List[Tuple[Callable[[], Awaitable[None]], str]] = []
        self.logger.info("Created new cluster %s", self.name)

    async def install_and_start(self) -> None:
        """Setup initial servers and start them.
           Catch and save any startup exception"""
        try:
            if self.replicas > 0:
                await self.add_servers(self.replicas)
                self.keyspace_count = self._get_keyspace_count()
        except Exception as exc:
            # If start fails, swallow the error to throw later,
            # at test time.
            self.start_exception = exc
        self.is_running = True
        self.logger.info("Created cluster %s", self)
        self.is_dirty = False

    async def uninstall(self) -> None:
        """Stop running servers and uninstall all servers"""
        self.is_dirty = True
        self.logger.info("Uninstalling cluster %s", self)
        await self.stop()
        await gather_safely(*(srv.uninstall() for srv in self.stopped.values()))
        # Close API client to release connector resources
        if self.api is not None:
            self.api.close()
            self.api = None
        await gather_safely(*(self.host_registry.release_host(Host(ip))
                               for ip in self.leased_ips))

    async def recycle(self) -> None:
        """Dispose of the cluster once it's no longer needed: stop it, close the
           server log files and sockets and release the used IPs. We don't
           necessarily uninstall() it, which would delete the log file and
           directory - we might want to preserve these if the cluster was used
           by a failed test.
        """
        await self.stop()
        # The servers are down, so a callback may now dispose of a resource
        # they were using without racing against them.
        await self.run_teardown_callbacks()
        for srv in self.servers.values():
            if srv.log_file is not None:
                srv.log_file.close()
            srv.maintenance_socket_dir.cleanup()
        # Close API client to release connector resources
        if self.api is not None:
            self.api.close()
            self.api = None
        await self.release_ips()
        if not self.save_log_on_success:
            # The cluster has served its purpose: a failed test's logs were
            # copied to failed_test/ when it failed, and nothing here is read
            # again.  Delete now rather than at exit, so that a long run
            # doesn't keep the directories of thousands of clusters on disk.
            await gather_safely(*(srv.uninstall() for srv in self.servers.values()))

    def add_teardown_callback(self, callback: Callable[[], Awaitable[None]], name: str) -> None:
        """Register a cleanup to run when this cluster is recycled.

        The callback arrives already bound to the loop it was registered on, so
        this end only tracks which callbacks belong to this cluster and in what
        order they were registered.
        """
        self.logger.info("Cluster %s registers teardown callback %s", self, name)
        self.teardown_callbacks.append((callback, name))

    async def run_teardown_callbacks(self) -> None:
        """Fire the registered teardown callbacks in LIFO order.

        Called from recycle(), once the servers are stopped, so that a callback
        disposing of a resource they were using cannot race with them: an
        object storage bucket an in-flight tablet migration could otherwise
        still read from (SCYLLADB-2471).

        An exception in one callback is logged and swallowed, so that it
        neither hides the others nor aborts the rest of the teardown.
        """
        assert not self.running, \
            f"Cluster {self} still has running servers, teardown callbacks must fire after it is stopped"
        while self.teardown_callbacks:
            callback, name = self.teardown_callbacks.pop()
            self.logger.info("Cluster %s running teardown callback %s", self, name)
            try:
                await callback()
            except Exception as e:
                self.logger.warning("Cluster %s teardown callback %s failed: %s", self, name, e)
            else:
                self.logger.info("Cluster %s teardown callback %s done", self, name)

    async def release_ips(self) -> None:
        """Release all IPs leased from the host registry by this cluster.
        Call this function only if the cluster is stopped and will not be started again."""
        assert not self.running
        self.logger.info("Cluster %s releases ips %s", self, self.leased_ips)
        while self.leased_ips:
            ip = self.leased_ips.pop()
            await self.host_registry.release_host(Host(ip))

    async def stop(self) -> None:
        """Stop all running servers ASAP"""
        # FIXME: the lock is necessary because test.py calls `stop()` and `uninstall()` concurrently
        # (from exit artifacts), which leads to issues (#15755). A more elegant solution would be
        # to prevent that instead of using a lock here.
        async with self.stop_lock:
            if self.is_running:
                self.is_running = False
                self.logger.info("Cluster %s stopping", self)
                self.is_dirty = True
                # If self.running is empty, no-op
                await gather_safely(*(server.stop() for server in self.running.values()))
                self.stopped.update(self.running)
                self.running.clear()

    async def stop_gracefully(self) -> None:
        """Stop all running servers in a clean way"""
        if self.is_running:
            self.is_running = False
            self.logger.info("Cluster %s stopping gracefully", self)
            self.is_dirty = True
            # If self.running is empty, no-op
            await gather_safely(*(server.stop_gracefully(graceful_stop_timeout(self.mode))
                                  for server in self.running.values()))
            self.stopped.update(self.running)
            self.running.clear()

    def _seeds(self) -> List[IPAddress]:
        # If the cluster is empty, all servers must use self.initial_seed to not start separate clusters.
        if not self.running:
            return [self.initial_seed] if self.initial_seed else []
        return [server.ip_addr for server in self.running.values()]

    async def add_server(self, replace_cfg: Optional[ReplaceConfig] = None,
                         cmdline: Optional[List[str]] = None,
                         config: Optional[dict[str, Any]] = None,
                         version: Optional[ScyllaVersionDescription] = None,
                         property_file: Optional[dict[str, Any]] = None,
                         start: bool = True,
                         seeds: Optional[List[IPAddress]] = None,
                         server_encryption: str = "none",
                         expected_error: Optional[str] = None,
                         expected_server_up_state: ServerUpState = ServerUpState.SERVING) -> ServerInfo:
        """Add a new server to the cluster"""
        self.is_dirty = True

        assert start or not expected_error, \
            f"add_server: cannot add a stopped server and expect an error"

        # Deep copy: the test keeps the dict (and add_servers() shares it between
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
            )
            self.starting[server.server_id] = server
            self.logger.info("Cluster %s adding server...", self)
            if start:
                await server.install_and_start(self.api, expected_error, expected_server_up_state)
            else:
                await server.install()
        except Exception as exc:
            workdir = '<unknown>' if server is None else server.workdir.name
            self.logger.error("Failed to start Scylla server at host %s in %s: %s",
                          ip_addr, workdir, str(exc))
            await handle_join_failure()
            raise
        finally:
            # server_stop() may have already removed the starting server
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

    async def add_servers(self, servers_num: int = 1,
                          cmdline: Optional[List[str]] = None,
                          config: Optional[dict[str, Any]] = None,
                          version: Optional[ScyllaVersionDescription] = None,
                          property_file: Union[list[dict[str, Any]], dict[str, Any], None] = None,
                          start: bool = True,
                          seeds: Optional[List[IPAddress]] = None,
                          server_encryption: str = "none",
                          expected_error: Optional[str] = None) -> List[ServerInfo]:
        """Add multiple servers to the cluster concurrently"""
        assert servers_num > 0, f"add_servers: cannot add {servers_num} servers"

        def get_property_file(i) -> Optional[dict[str, Any]]:
            if property_file is None:
                return None
            elif type(property_file) is dict:
                return property_file
            else:
                assert type(property_file) is list and len(property_file) == servers_num
                return property_file[i]

        return await gather_safely(*(self.add_server(None, cmdline, config, version, get_property_file(i), start, seeds, server_encryption, expected_error)
                                      for i in range(servers_num)))

    def endpoint(self) -> str:
        """Get a server id (IP) from running servers"""
        return next(server.ip_addr for server in self.running.values())

    def take_log_savepoint(self) -> None:
        """Save the log size on all running servers"""
        for server in self.running.values():
            server.take_log_savepoint()

    def read_server_log(self) -> str:
        """Read log data of failed server"""
        # FIXME: pick failed server
        if self.running:
            return next(iter(self.running.values())).read_log()
        else:
            return ""

    def server_log_filename(self) -> Optional[pathlib.Path]:
        """The log file name of the failed server"""
        # FIXME: pick failed server
        if self.running:
            return next(server for server in self.running.values()).log_filename
        else:
            return None

    def __str__(self):
        running = ", ".join(str(server) for server in self.running.values())
        stopped = ", ".join(str(server) for server in self.stopped.values())
        return f"ScyllaCluster(name: {self.name}, running: {running}, stopped: {stopped})"

    def running_servers(self) -> list[ServerInfo]:
        """Get a list of tuples of server id and IP address of running servers (and not removed)"""
        return [server.server_info() for server in self.running.values()
                if server.server_id not in self.removed]

    def all_servers(self) -> list[ServerInfo]:
        """Get a list of tuples of server id and IP address of all servers"""
        return [server.server_info() for server in self.servers.values()]

    def starting_servers(self) -> list[ServerInfo]:
        """Get a list of tuples of server ids and IP address of servers which are currently starting (not yet running)"""
        return [server.server_info() for server in self.starting.values()]

    def _get_keyspace_count(self) -> int:
        """Get the current keyspace count"""
        assert self.start_exception is None
        assert self.running, "No active nodes left"
        server = next(iter(self.running.values()))
        self.logger.debug("_get_keyspace_count() using server %s", server)
        assert server.control_connection is not None
        rows = server.control_connection.execute(
               "select count(*) as c from system_schema.keyspaces")
        keyspace_count = int(rows.one()[0])
        return keyspace_count

    def before_test(self, name) -> None:
        """Check that  the cluster is ready for a test. If
        there was a start error, throw it here - the cluster is started
        outside of any specific test, so throwing it at start time
        wouldn't be attributed to a test."""
        if self.start_exception:
            # Mark as dirty so further test cases don't try to reuse this cluster.
            self.is_dirty = True
            raise Exception(f'Exception when starting cluster {self}:\n{self.start_exception}')

        for server in self.running.values():
            server.write_log_marker(f"------ Starting test {name} ------\n")

    def after_test(self, name: str, success: bool | None = None) -> None:
        """Mark the cluster as dirty after a failed test.
        If the cluster is not dirty, check that it's still alive and the test
        hasn't left any garbage."""
        assert self.start_exception is None
        if not success:
            if success is not None:
                self.logger.debug(f"Test failed using cluster {self.name}, marking the cluster as dirty")
            self.is_dirty = True
        if self.is_dirty:
            self.logger.info(f"The cluster {self.name} is dirty, not checking"
                             f" keyspace count post-condition")
        else:
            if self.running and self._get_keyspace_count() != self.keyspace_count:
                raise RuntimeError(f"Test post-condition on cluster {self.name} failed, "
                                   f"the test must drop all keyspaces it creates.")
        for server in itertools.chain(self.running.values(), self.stopped.values()):
            server.write_log_marker(f"------ Ending test {name} ------\n")
            # Only close log files when the cluster is dirty (will be destroyed).
            # If the cluster is clean and will be reused, keep the log file open
            # so that write_log_marker() and take_log_savepoint() work in the
            # next test's before_test().
            if self.is_dirty and not server.log_file.closed:
                server.log_file.close()

    async def server_stop(self, server_id: ServerNum, gracefully: bool) -> None:
        """Stop a server. No-op if already stopped."""
        self.logger.info("Cluster %s stopping server %s", self, server_id)
        if server_id in self.stopped:
            return
        assert server_id in self.running or server_id in self.starting, f"Server {server_id} unknown"
        self.is_dirty = True
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
            # in its cleanup path. This is a fallback if server_stop() wins the race.
            self.starting.pop(server_id, None)

    def server_mark_removed(self, server_id: ServerNum) -> None:
        """Mark server as removed."""
        self.logger.debug("Cluster %s marking server %s as removed", self, server_id)
        self.removed.add(server_id)

    async def server_start(self,
                           server_id: ServerNum,
                           expected_error: str | None = None,
                           seeds: list[IPAddress] | None = None,
                           expected_server_up_state: ServerUpState = ServerUpState.SERVING,
                           cmdline_options_override: list[str] | None = None,
                           append_env_override: dict[str, str] | None = None,
                           auth_provider: dict[str, str] | None = None) -> None:
        """Start a server.

        Replace CLI options and environment variables with `cmdline_options_override` and `append_env_override`
        if provided.

        No-op if already running.
        """
        if server_id in self.running:
            return
        assert server_id in self.stopped, f"Server {server_id} unknown"
        self.is_dirty = True
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
        )
        if expected_error is not None:
            self.running.pop(server_id)
            self.stopped[server_id] = server

    def server_pause(self, server_id: ServerNum) -> None:
        """Pause a running server process."""
        self.logger.info("Cluster %s pausing server %s", self.name, server_id)
        assert server_id in self.running
        self.is_dirty = True
        server = self.running[server_id]
        server.pause()

    def server_unpause(self, server_id: ServerNum) -> None:
        """Unpause a paused server process."""
        self.logger.info("Cluster %s unpausing server %s", self.name, server_id)
        assert server_id in self.running
        server = self.running[server_id]
        server.unpause()

    def server_switch_executable(self, server_id: ServerNum, path: str) -> None:
        """Switch the executable path of a stopped server"""
        self.logger.info("Cluster %s upgrading server %s to executable %s", self.name, server_id, path)
        server = self.servers[server_id]
        assert not server.is_running, f"Server {server_id} is running: stop it first and then change its executable"
        self.is_dirty = True
        server.exe = pathlib.Path(path).resolve()
        server.check_scylla_executable()

    def server_get_process_status(self, server_id: ServerNum) -> str:
        assert server_id in self.running

        self.logger.info("Cluster %s get process status for server %s", self.name, server_id)
        server = self.running[server_id]
        try:
            process = psutil.Process(server.cmd.pid)
            status = process.status()
        except psutil.NoSuchProcess:
            status = psutil.STATUS_DEAD

        self.logger.info("Cluster %s process status for server %s is %s", self.name, server_id, status)
        return status

    def server(self, server_id: ServerNum) -> ScyllaServer:
        """Get the given server, running or stopped.
           Fails if the server cannot be found."""
        assert server_id in self.servers, f"Server {server_id} unknown"
        return self.servers[server_id]

    def get_config(self, server_id: ServerNum) -> dict[str, object]:
        """Get conf/scylla.yaml of the given server as a dictionary.
           Fails if the server cannot be found."""
        assert server_id in self.servers, f"Server {server_id} unknown"
        return self.servers[server_id].get_config()

    def update_config(self, server_id: ServerNum, config_options: dict[str, Any]) -> None:
        """Update conf/scylla.yaml of the given server with `config_options` dict.

        If the server is running, reload the config with a SIGHUP.
        Mark the cluster as dirty.
        Fail if the server cannot be found.
        """
        assert server_id in self.servers, f"Server {server_id} unknown"
        self.is_dirty = True
        self.servers[server_id].update_config(config_options=config_options)

    def remove_config_option(self, server_id: ServerNum, key: str) -> None:
        """Remove an option from conf/scylla.yaml of the given server.

        If the server is running, reload the config with a SIGHUP.
        Mark the cluster as dirty.
        Fail if the server cannot be found.
        """
        assert server_id in self.servers, f"Server {server_id} unknown"
        self.is_dirty = True
        self.servers[server_id].remove_config_option(key=key)

    def update_cmdline(self, server_id: ServerNum, cmdline_options: List[str]) -> None:
        """Update the command-line options of the given server by merging the new options into the existing ones.
           The update only takes effect after restart.
           Marks the cluster as dirty.
           Fails if the server cannot be found."""
        assert server_id in self.servers, f"Server {server_id} unknown"
        self.is_dirty = True
        self.servers[server_id].update_cmdline(cmdline_options)

    def setLogger(self, logger: logging.LoggerAdapter):
        """Change the logger used by the cluster.
           Called when a cluster is reused between tests so that logs during the new test
           are prefixed appropriately with the corresponding test's name.
        """
        self.logger = logger
        for srv in self.servers.values():
            srv.setLogger(self.logger)

    async def change_ip(self, server_id: ServerNum) -> IPAddress:
        """Lease a new IP address and update conf/scylla.yaml with it. The
        original IP is released at the end of the test to avoid an
        immediate recycle within the same cluster. The server must be
        stopped before its ip is changed."""
        assert server_id in self.servers, f"Server {server_id} unknown"
        server = self.servers[server_id]
        assert not server.is_running, f"Server {server_id} is running: stop it first and then change its ip"
        self.is_dirty = True
        ip_addr = IPAddress(await self.host_registry.lease_host())
        self.leased_ips.add(ip_addr)
        logging.info("Cluster %s changed server %s IP from %s to %s", self.name,
                     server_id, server.ip_addr, ip_addr)
        server.change_ip(ip_addr)
        return ip_addr

    async def change_rpc_address(self, server_id: ServerNum) -> IPAddress:
        """Lease a new IP address and update conf/scylla.yaml with it. The
        original IP is released at the end of the test to avoid an
        immediate recycle within the same cluster. The server must be
        stopped before its ip is changed."""
        assert server_id in self.servers, f"Server {server_id} unknown"
        server = self.servers[server_id]
        assert not server.is_running, f"Server {server_id} is running: stop it first and then change its ip"
        self.is_dirty = True
        rpc_address = IPAddress(await self.host_registry.lease_host())
        self.leased_ips.add(rpc_address)
        logging.info("Cluster %s changed server %s RPC IP from %s to %s", self.name,
                     server_id, server.config["rpc_address"], rpc_address)
        server.change_rpc_address(rpc_address)
        return rpc_address

    def wipe_sstables(self, server_id: ServerNum, keyspace: str, table: str):
        """Delete all sstable files for the given <node, keyspace, table>."""
        assert server_id in self.servers, f"Server {server_id} unknown"
        server = self.servers[server_id]
        assert not server.is_running, f"Server {server_id} is running: stop it first and then delete its files"
        self.is_dirty = True
        server.wipe_sstables(keyspace, table)

    def get_sstables_disk_usage(self, server_id: ServerNum, keyspace: str, table: str) -> int:
        """Measure the disk usage of sstables for the given <node, keyspace, table>."""
        assert server_id in self.servers, f"Server {server_id} unknown"
        server = self.servers[server_id]
        return server.get_sstables_disk_usage(keyspace, table)
