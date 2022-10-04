#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Scylla clusters for testing.
   Provides helpers to setup and manage clusters of Scylla servers for testing.
"""
import asyncio
from asyncio.subprocess import Process
from contextlib import asynccontextmanager
from collections import ChainMap
import itertools
import logging
import os
import pathlib
import shutil
import tempfile
import time
from typing import Optional, Dict, List, Set, Callable, AsyncIterator, NamedTuple
import uuid
from io import BufferedWriter
from test.pylib.pool import Pool
from test.pylib.rest_client import ScyllaRESTAPIClient
import aiohttp
import aiohttp.web
import yaml
import signal

from cassandra import InvalidRequest                    # type: ignore
from cassandra import OperationTimedOut                 # type: ignore
from cassandra.auth import PlainTextAuthProvider        # type: ignore
from cassandra.cluster import Cluster           # type: ignore # pylint: disable=no-name-in-module
from cassandra.cluster import NoHostAvailable   # type: ignore # pylint: disable=no-name-in-module
from cassandra.cluster import Session           # pylint: disable=no-name-in-module
from cassandra.cluster import ExecutionProfile  # pylint: disable=no-name-in-module
from cassandra.cluster import EXEC_PROFILE_DEFAULT  # pylint: disable=no-name-in-module
from cassandra.policies import WhiteListRoundRobinPolicy  # type: ignore


def make_scylla_conf(workdir: pathlib.Path, host_addr: str, seed_addrs: List[str], cluster_name: str) -> dict[str, object]:
    return {
        'cluster_name': cluster_name,
        'workdir': str(workdir.resolve()),
        'listen_address': host_addr,
        'rpc_address': host_addr,
        'api_address': host_addr,
        'prometheus_address': host_addr,
        'alternator_address': host_addr,
        'seed_provider': [{
            'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
            'parameters': [{
                'seeds': '{}'.format(','.join(seed_addrs))
                }]
            }],

        'developer_mode': True,

        # Allow testing experimental features. Following issue #9467, we need
        # to add here specific experimental features as they are introduced.
        'enable_user_defined_functions': True,
        'experimental': True,
        'experimental_features': ['raft', 'udf'],

        'skip_wait_for_gossip_to_settle': 0,
        'ring_delay_ms': 0,
        'num_tokens': 16,
        'flush_schema_tables_after_modification': False,
        'auto_snapshot': False,

        # Significantly increase default timeouts to allow running tests
        # on a very slow setup (but without network losses). Note that these
        # are server-side timeouts: The client should also avoid timing out
        # its own requests - for this reason we increase the CQL driver's
        # client-side timeout in conftest.py.

        'range_request_timeout_in_ms': 300000,
        'read_request_timeout_in_ms': 300000,
        'counter_write_request_timeout_in_ms': 300000,
        'cas_contention_timeout_in_ms': 300000,
        'truncate_request_timeout_in_ms': 300000,
        'write_request_timeout_in_ms': 300000,
        'request_timeout_in_ms': 300000,

        'strict_allow_filtering': True,

        'permissions_update_interval_in_ms': 100,
        'permissions_validity_in_ms': 100,
    }

# Seastar options can not be passed through scylla.yaml, use command line
# for them. Keep everything else in the configuration file to make
# it easier to restart. Sic: if you make a typo on the command line,
# Scylla refuses to boot.
SCYLLA_CMDLINE_OPTIONS = [
    '--smp', '2',
    '-m', '1G',
    '--collectd', '0',
    '--overprovisioned',
    '--max-networking-io-control-blocks', '100',
    '--unsafe-bypass-fsync', '1',
    '--kernel-page-cache', '1',
    '--abort-on-lsa-bad-alloc', '1',
    '--abort-on-seastar-bad-alloc',
    '--abort-on-internal-error', '1',
    '--abort-on-ebadf', '1'
]


class ScyllaServer:
    """Starts and handles a single Scylla server, managing logs, checking if responsive,
       and cleanup when finished."""
    # pylint: disable=too-many-instance-attributes
    START_TIMEOUT = 300     # seconds
    start_time: float
    sleep_interval: float
    workdir: pathlib.Path
    log_filename: pathlib.Path
    config_filename: pathlib.Path
    log_file: BufferedWriter

    def __init__(self, exe: str, vardir: str,
                 host_registry,
                 cluster_name: str, seeds: List[str],
                 cmdline_options: List[str],
                 config_options: Dict[str, str]) -> None:
        # pylint: disable=too-many-arguments
        self.exe = pathlib.Path(exe).resolve()
        self.vardir = pathlib.Path(vardir)
        self.host_registry = host_registry
        self.cmdline_options = cmdline_options
        self.cluster_name = cluster_name
        self.hostname = ""
        self.seeds = seeds
        self.cmd: Optional[Process] = None
        self.log_savepoint = 0
        self.control_cluster: Optional[Cluster] = None
        self.control_connection: Optional[Session] = None
        self.config_options = config_options
        # Sum of basic server configuration and the user-provided config options (self.config_options).
        # Calculated in `install` as only then we know the seed servers.
        self.config: Dict[str, object] = {}

        async def stop_server() -> None:
            if self.is_running:
                await self.stop()

        async def uninstall_server() -> None:
            await self.uninstall()

        self.stop_artifact = stop_server
        self.uninstall_artifact = uninstall_server

    async def install_and_start(self) -> None:
        """Setup and start this server"""
        await self.install()

        logging.info("starting server at host %s in %s...", self.hostname,
                     self.workdir.name)

        await self.start()

        if self.cmd:
            logging.info("started server at host %s in %s, pid %d", self.hostname,
                         self.workdir.name, self.cmd.pid)

    @property
    def is_running(self) -> bool:
        """Check the server subprocess is up"""
        return self.cmd is not None

    @property
    def host(self) -> str:
        """Server host name"""
        return str(self.hostname)

    def check_scylla_executable(self) -> None:
        """Check if executable exists and can be run"""
        if not os.access(self.exe, os.X_OK):
            raise RuntimeError(f"{self.exe} is not executable")

    async def install(self) -> None:
        """Create a working directory with all subdirectories, initialize
        a configuration file."""

        self.check_scylla_executable()

        # Scylla assumes all instances of a cluster use the same port,
        # so each instance needs an own IP address.
        self.hostname = await self.host_registry.lease_host()
        if not self.seeds:
            self.seeds = [self.hostname]
        # Use the last part in host IP 127.151.3.27 -> 27
        # There can be no duplicates within the same test run
        # thanks to how host registry registers subnets, and
        # different runs use different vardirs.
        shortname = pathlib.Path(f"scylla-{self.host.rsplit('.', maxsplit=1)[-1]}")
        self.workdir = self.vardir / shortname

        logging.info("installing Scylla server in %s...", self.workdir)

        self.log_filename = self.vardir / shortname.with_suffix(".log")

        self.config_filename = self.workdir / "conf/scylla.yaml"

        # Delete the remains of the previous run

        # Cleanup any remains of the previously running server in this path
        shutil.rmtree(self.workdir, ignore_errors=True)

        self.workdir.mkdir(parents=True, exist_ok=True)
        self.config_filename.parent.mkdir(parents=True, exist_ok=True)
        # Create a configuration file.
        self.config = make_scylla_conf(
                workdir = self.workdir,
                host_addr = self.hostname,
                seed_addrs = self.seeds,
                cluster_name = self.cluster_name) \
            | self.config_options
        self._write_config_file()

        self.log_file = self.log_filename.open("wb")

    def get_config(self) -> dict[str, object]:
        """Return the contents of conf/scylla.yaml as a dict."""
        return self.config

    def update_config(self, key: str, value: object) -> None:
        """Update conf/scylla.yaml by setting `value` under `key`.
           If we're running, reload the config with a SIGHUP."""
        self.config[key] = value
        self._write_config_file()
        if self.cmd:
            self.cmd.send_signal(signal.SIGHUP)

    def take_log_savepoint(self) -> None:
        """Save the server current log size when a test starts so that if
        the test fails, we can only capture the relevant lines of the log"""
        self.log_savepoint = self.log_file.tell()

    def read_log(self) -> str:
        """ Return first 3 lines of the log + everything that happened
        since the last savepoint. Used to diagnose CI failures, so
        avoid a nessted exception."""
        try:
            with self.log_filename.open("r") as log:
                # Read the first 5 lines of the start log
                lines: List[str] = []
                for _ in range(3):
                    lines.append(log.readline())
                # Read the lines since the last savepoint
                if self.log_savepoint and self.log_savepoint > log.tell():
                    log.seek(self.log_savepoint)
                return "".join(lines + log.readlines())
        except Exception as exc:    # pylint: disable=broad-except
            return f"Exception when reading server log {self.log_filename}: {exc}"

    async def cql_is_up(self) -> bool:
        """Test that CQL is serving (a check we use at start up)."""
        caslog = logging.getLogger('cassandra')
        oldlevel = caslog.getEffectiveLevel()
        # Be quiet about connection failures.
        caslog.setLevel('CRITICAL')
        auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
        # auth::standard_role_manager creates "cassandra" role in an
        # async loop auth::do_after_system_ready(), which retries
        # role creation with an exponential back-off. In other
        # words, even after CQL port is up, Scylla may still be
        # initializing. When the role is ready, queries begin to
        # work, so rely on this "side effect".
        profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([self.hostname]),
                                   request_timeout=self.START_TIMEOUT)
        try:
            # In a cluster setup, it's possible that the CQL
            # here is directed to a node different from the initial contact
            # point, so make sure we execute the checks strictly via
            # this connection
            with Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                         contact_points=[self.hostname],
                         # This is the latest version Scylla supports
                         protocol_version=4,
                         auth_provider=auth) as cluster:
                with cluster.connect() as session:
                    session.execute("SELECT * FROM system.local")
                    self.control_cluster = Cluster(execution_profiles=
                                                        {EXEC_PROFILE_DEFAULT: profile},
                                                   contact_points=[self.hostname],
                                                   auth_provider=auth)
                    self.control_connection = self.control_cluster.connect()
                    return True
        except (NoHostAvailable, InvalidRequest, OperationTimedOut) as exc:
            logging.debug("Exception when checking if CQL is up: %s", exc)
            return False
        finally:
            caslog.setLevel(oldlevel)
        # Any other exception may indicate a problem, and is passed to the caller.

    async def rest_api_is_up(self) -> bool:
        """Test that the Scylla REST API is serving. Can be used as a
        checker function at start up."""
        try:
            async with aiohttp.ClientSession() as session:
                url = f"http://{self.hostname}:10000/"
                async with session.get(url):
                    return True
        except aiohttp.ClientConnectionError:
            return False
        # Any other exception may indicate a problem, and is passed to the caller.

    async def start(self) -> None:
        """Start an installed server. May be used for restarts."""

        # Add suite-specific command line options
        scylla_args = SCYLLA_CMDLINE_OPTIONS + self.cmdline_options
        env = os.environ.copy()
        env.clear()     # pass empty env to make user user's SCYLLA_HOME has no impact
        self.cmd = await asyncio.create_subprocess_exec(
            self.exe,
            *scylla_args,
            cwd=self.workdir,
            stderr=self.log_file,
            stdout=self.log_file,
            env=env,
            preexec_fn=os.setsid,
        )

        self.start_time = time.time()
        sleep_interval = 0.1

        while time.time() < self.start_time + self.START_TIMEOUT:
            if self.cmd.returncode:
                with self.log_filename.open('r') as log_file:
                    logging.error("failed to start server at host %s in %s",
                                  self.hostname, self.workdir.name)
                    logging.error("last line of %s:", self.log_filename)
                    log_file.seek(0, 0)
                    logging.error(log_file.readlines()[-1].rstrip())
                    log_handler = logging.getLogger().handlers[0]
                    if hasattr(log_handler, 'baseFilename'):
                        logpath = log_handler.baseFilename   # type: ignore
                    else:
                        logpath = "?"
                    raise RuntimeError(f"Failed to start server at host {self.hostname}.\n"
                                       "Check the log files:\n"
                                       f"{logpath}\n"
                                       f"{self.log_filename}")

            if await self.rest_api_is_up():
                if await self.cql_is_up():
                    return

            # Sleep and retry
            await asyncio.sleep(sleep_interval)

        raise RuntimeError(f"failed to start server {self.host}, "
                           f"check server log at {self.log_filename}")

    async def force_schema_migration(self) -> None:
        """This is a hack to change schema hash on an existing cluster node
        which triggers a gossip round and propagation of entire application
        state. Helps quickly propagate tokens and speed up node boot if the
        previous state propagation was missed."""
        auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
        profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy(self.seeds),
                                   request_timeout=self.START_TIMEOUT)
        with Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                     contact_points=self.seeds,
                     auth_provider=auth,
                     # This is the latest version Scylla supports
                     protocol_version=4,
                     ) as cluster:
            with cluster.connect() as session:
                session.execute("CREATE KEYSPACE IF NOT EXISTS k WITH REPLICATION = {" +
                                "'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
                session.execute("DROP KEYSPACE k")

    async def shutdown_control_connection(self) -> None:
        """Shut down driver connection"""
        if self.control_connection is not None:
            self.control_connection.shutdown()
            self.control_connection = None
        if self.control_cluster is not None:
            self.control_cluster.shutdown()
            self.control_cluster = None

    async def stop(self) -> None:
        """Stop a running server. No-op if not running. Uses SIGKILL to
        stop, so is not graceful. Waits for the process to exit before return."""
        # Preserve for logging
        hostname = self.hostname
        logging.info("stopping server at host %s in %s", hostname,
                     self.workdir.name)
        if not self.cmd:
            return

        await self.shutdown_control_connection()
        try:
            self.cmd.kill()
        except ProcessLookupError:
            pass
        else:
            await self.cmd.wait()
        finally:
            if self.cmd:
                logging.info("stopped server at host %s in %s", hostname,
                             self.workdir.name)
            self.cmd = None

    async def stop_gracefully(self) -> None:
        """Stop a running server. No-op if not running. Uses SIGTERM to
        stop, so it is graceful. Waits for the process to exit before return."""
        # Preserve for logging
        hostname = self.hostname
        logging.info("gracefully stopping server at host %s", hostname)
        if not self.cmd:
            return

        await self.shutdown_control_connection()
        try:
            self.cmd.terminate()
        except ProcessLookupError:
            pass
        else:
            # FIXME: add timeout, fail the test and mark cluster as dirty
            # if we timeout.
            await self.cmd.wait()
        finally:
            if self.cmd:
                logging.info("gracefully stopped server at host %s", hostname)
            self.cmd = None

    async def uninstall(self) -> None:
        """Clear all files left from a stopped server, including the
        data files and log files."""

        if not self.hostname:
            return
        logging.info("Uninstalling server at %s", self.workdir)

        shutil.rmtree(self.workdir)
        self.log_filename.unlink(missing_ok=True)

        await self.host_registry.release_host(self.hostname)
        self.hostname = ""

    def write_log_marker(self, msg) -> None:
        """Write a message to the server's log file (e.g. separator/marker)"""
        self.log_file.seek(0, 2)  # seek to file end
        self.log_file.write(msg.encode())
        self.log_file.flush()

    def __str__(self):
        return self.hostname

    def _write_config_file(self) -> None:
        with self.config_filename.open('w') as config_file:
            yaml.dump(self.config, config_file)


class ScyllaCluster:
    """A cluster of Scylla servers providing an API for changes"""
    # pylint: disable=too-many-instance-attributes

    class ActionReturn(NamedTuple):
        """Return status, message, and data (where applicable, otherwise empty) for API requests."""
        success: bool
        msg: str = ""
        data: dict = {}

    def __init__(self, replicas: int,
                 create_server: Callable[[str, List[str]], ScyllaServer]) -> None:
        self.name = str(uuid.uuid1())
        self.replicas = replicas
        self.create_server = create_server
        # Every ScyllaServer is in one of self.running, self.stopped, self.decommissioned.
        # These dicts are disjoint.
        # A server ID present in self.removed may be either in self.running or in self.stopped.
        self.running: Dict[str, ScyllaServer] = {}        # started servers
        self.stopped: Dict[str, ScyllaServer] = {}        # servers no longer running but present
        self.decommissioned: Dict[str, ScyllaServer] = {} # decommissioned servers
        self.removed: Set[str] = set()                    # removed servers (might be running)
        # cluster is started (but it might not have running servers)
        self.is_running: bool = False
        # cluster was modified in a way it should not be used in subsequent tests
        self.is_dirty: bool = False
        self.start_exception: Optional[Exception] = None
        self.keyspace_count = 0
        self.api = ScyllaRESTAPIClient()

    async def install_and_start(self) -> None:
        """Setup initial servers and start them.
           Catch and save any startup exception"""
        try:
            for _ in range(self.replicas):
                await self.add_server()
            self.keyspace_count = self._get_keyspace_count()
        except Exception as exc:
            # If start fails, swallow the error to throw later,
            # at test time.
            self.start_exception = exc
        self.is_running = True
        logging.info("Created cluster %s", self)
        self.is_dirty = False

    async def uninstall(self) -> None:
        """Stop running servers, uninstall all servers, and remove API socket"""
        self.is_dirty = True
        logging.info("Uninstalling cluster")
        await self.stop()
        await asyncio.gather(*(server.uninstall() for server in self.stopped.values()))

    async def stop(self) -> None:
        """Stop all running servers ASAP"""
        if self.is_running:
            logging.info("Cluster %s stopping", self)
            self.is_dirty = True
            # If self.running is empty, no-op
            await asyncio.gather(*(server.stop() for server in self.running.values()))
            self.stopped.update(self.running)
            self.running.clear()
            self.is_running = False

    async def stop_gracefully(self) -> None:
        """Stop all running servers in a clean way"""
        if self.is_running:
            logging.info("Cluster %s stopping gracefully", self)
            self.is_dirty = True
            # If self.running is empty, no-op
            await asyncio.gather(*(server.stop_gracefully() for server in self.running.values()))
            self.stopped.update(self.running)
            self.running.clear()
            self.is_running = False

    def _seeds(self) -> List[str]:
        return list(self.running.keys())

    async def add_server(self) -> str:
        """Add a new server to the cluster"""
        server = self.create_server(self.name, self._seeds())
        self.is_dirty = True
        try:
            logging.info("Cluster %s adding server...", self)
            await server.install_and_start()
        except Exception as exc:
            logging.error("Failed to start Scylla server at host %s in %s: %s",
                          server.hostname, server.workdir.name, str(exc))
            raise
        self.running[server.host] = server
        logging.info("Cluster %s added server %s", self, server)
        return server.host

    def endpoint(self) -> str:
        """Get a server id (IP) from running servers"""
        return next(iter(self.running))

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
            return next(iter(self.running.values())).log_filename
        else:
            return None

    def __str__(self):
        running = f"{{{', '.join(str(c) for c in self.running)}}}"
        stopped = f"{{{', '.join(str(c) for c in self.stopped)}}}"
        return f"ScyllaCluster(name: {self.name}, running: {running}, stopped: {stopped})"

    def running_servers(self) -> List[str]:
        return list(set(self.running.keys()) - self.removed)

    def _get_keyspace_count(self) -> int:
        """Get the current keyspace count"""
        assert self.start_exception is None
        assert self.running, "No active nodes left"
        server = next(iter(self.running.values()))
        logging.debug("_get_keyspace_count() using server %s", server)
        assert server.control_connection is not None
        rows = server.control_connection.execute(
               "select count(*) as c from system_schema.keyspaces")
        keyspace_count = int(rows.one()[0])
        return keyspace_count

    def before_test(self, name) -> None:
        """Check that  the cluster is ready for a test. If
        there was a start error, throw it here - the server is
        running when it's added to the pool, which can't be attributed
        to any specific test, throwing it here would stop a specific
        test."""
        if self.start_exception:
            raise self.start_exception

        for server in self.running.values():
            server.write_log_marker(f"------ Starting test {name} ------\n")

    def after_test(self, name) -> None:
        """Check that the cluster is still alive and the test
        hasn't left any garbage."""
        assert self.start_exception is None
        if self._get_keyspace_count() != self.keyspace_count:
            raise RuntimeError("Test post-condition failed, "
                               "the test must drop all keyspaces it creates.")
        for server in itertools.chain(self.running.values(), self.stopped.values()):
            server.write_log_marker(f"------ Ending test {name} ------\n")

    async def server_stop(self, server_ip: str, gracefully: bool) -> ActionReturn:
        """Stop a server. No-op if already stopped."""
        logging.info("Cluster %s stopping server %s", self, server_ip)
        if server_ip in self.stopped:
            return ScyllaCluster.ActionReturn(success=True,
                                              msg=f"Server {server_ip} already stopped")
        if server_ip not in self.running:
            return ScyllaCluster.ActionReturn(success=False, msg=f"Server {server_ip} unknown")
        self.is_dirty = True
        server = self.running.pop(server_ip)
        if gracefully:
            await server.stop_gracefully()
        else:
            await server.stop()
        self.stopped[server_ip] = server
        return ScyllaCluster.ActionReturn(success=True, msg=f"Server {server_ip} stopped")

    def server_mark_decommissioned(self, server_ip: str) -> None:
        """Mark a stopped server as decommissioned"""
        assert server_ip in self.stopped, "Server must be stopped when marking as decommissioned"""
        logging.debug("Cluster %s marking server %s as decommissioned", self, server_ip)
        self.decommissioned[server_ip] = self.stopped.pop(server_ip)

    def server_mark_removed(self, server_ip: str) -> None:
        """Mark server as removed."""
        logging.debug("Cluster %s marking server %s as removed", self, server_ip)
        self.removed.add(server_ip)

    async def server_start(self, server_ip: str) -> ActionReturn:
        """Start a stopped server"""
        logging.info("Cluster %s starting server", self)
        if server_ip in self.running:
            return ScyllaCluster.ActionReturn(success=True,
                                              msg=f"Server {server_ip} already started")
        if server_ip not in self.stopped:
            return ScyllaCluster.ActionReturn(success=False, msg=f"Server {server_ip} unknown")
        self.is_dirty = True
        server = self.stopped.pop(server_ip)
        server.seeds = self._seeds()
        await server.start()
        self.running[server_ip] = server
        return ScyllaCluster.ActionReturn(success=True, msg=f"Server {server_ip} started")

    async def server_restart(self, server_ip: str) -> ActionReturn:
        """Restart a running server"""
        logging.info("Cluster %s restarting server %s", self, server_ip)
        ret = await self.server_stop(server_ip, gracefully=True)
        if not ret.success:
            logging.error("Cluster %s failed to stop server %s", self, server_ip)
            return ret
        return await self.server_start(server_ip)

    def get_config(self, server_ip: str) -> ActionReturn:
        """Get conf/scylla.yaml of the given server as a dictionary.
           Fails if the server cannot be found."""
        server = self._find_server(server_ip)
        if not server:
            return ScyllaCluster.ActionReturn(success=False, msg=f"Server {server_ip} unknown")
        return ScyllaCluster.ActionReturn(success=True, data=server.get_config())

    def update_config(self, server_ip: str, key: str, value: object) -> ActionReturn:
        """Update conf/scylla.yaml of the given server by setting `value` under `key`.
           If the server is running, reload the config with a SIGHUP.
           Marks the cluster as dirty.
           Fails if the server cannot be found."""
        server = self._find_server(server_ip)
        if not server:
            return ScyllaCluster.ActionReturn(success=False, msg=f"Server {server_ip} unknown")
        self.is_dirty = True
        server.update_config(key, value)
        return ScyllaCluster.ActionReturn(success=True)

    def _find_server(self, id: str) -> Optional[ScyllaServer]:
        return ChainMap(self.running, self.stopped).get(id)


class ScyllaClusterManager:
    """Manages a Scylla cluster for running test cases
       Provides an async API for tests to request changes in the Cluster.
       Parallel requests are not supported.
    """
    # pylint: disable=too-many-instance-attributes
    cluster: ScyllaCluster
    site: aiohttp.web.UnixSite
    is_after_test_ok: bool

    def __init__(self, test_uname: str, clusters: Pool[ScyllaCluster], base_dir: str) -> None:
        self.test_uname: str = test_uname
        # The currently running test case with self.test_uname prepended, e.g.
        # test_topology.1::test_add_server_add_column
        self.current_test_case_full_name: str = ''
        self.clusters: Pool[ScyllaCluster] = clusters
        self.is_running: bool = False
        self.is_before_test_ok: bool = False
        self.is_after_test_ok: bool = False
        # API
        # NOTE: need to make a safe temp dir as tempfile can't make a safe temp sock name
        self.manager_dir: str = tempfile.mkdtemp(prefix="manager-", dir=base_dir)
        self.sock_path: str = f"{self.manager_dir}/api"
        app = aiohttp.web.Application()
        self._setup_routes(app)
        self.runner = aiohttp.web.AppRunner(app)

    async def start(self) -> None:
        """Get first cluster, setup API"""
        if self.is_running:
            logging.warning("ScyllaClusterManager already running")
            return
        await self._get_cluster()
        await self.runner.setup()
        self.site = aiohttp.web.UnixSite(self.runner, path=self.sock_path)
        await self.site.start()
        self.is_running = True

    async def _before_test(self, test_case_name: str) -> None:
        if self.cluster.is_dirty:
            await self.clusters.steal()
            await self.cluster.stop()
            await self._get_cluster()
        self.current_test_case_full_name = f'{self.test_uname}::{test_case_name}'
        logging.info("Leasing Scylla cluster %s for test %s", self.cluster, self.current_test_case_full_name)
        self.cluster.before_test(self.current_test_case_full_name)
        self.is_before_test_ok = True
        self.cluster.take_log_savepoint()

    async def stop(self) -> None:
        """Stop, cycle last cluster if not dirty and present"""
        logging.info("ScyllaManager stopping for test %s", self.test_uname)
        await self.site.stop()
        if not self.cluster.is_dirty:
            logging.info("Returning Scylla cluster %s for test %s", self.cluster, self.test_uname)
            await self.clusters.put(self.cluster)
        else:
            logging.info("ScyllaManager: Scylla cluster %s is dirty after %s, stopping it",
                            self.cluster, self.test_uname)
            await self.clusters.steal()
            await self.cluster.stop()
        del self.cluster
        if os.path.exists(self.manager_dir):
            shutil.rmtree(self.manager_dir)
        self.is_running = False

    async def _get_cluster(self) -> None:
        self.cluster = await self.clusters.get()
        logging.info("Getting new Scylla cluster %s", self.cluster)


    def _setup_routes(self, app: aiohttp.web.Application) -> None:
        app.router.add_get('/up', self._manager_up)
        app.router.add_get('/cluster/up', self._cluster_up)
        app.router.add_get('/cluster/is-dirty', self._is_dirty)
        app.router.add_get('/cluster/replicas', self._cluster_replicas)
        app.router.add_get('/cluster/running-servers', self._cluster_running_servers)
        app.router.add_get('/cluster/before-test/{test_case_name}', self._before_test_req)
        app.router.add_get('/cluster/after-test', self._after_test)
        app.router.add_get('/cluster/mark-dirty', self._mark_dirty)
        app.router.add_get('/cluster/server/{id}/stop', self._cluster_server_stop)
        app.router.add_get('/cluster/server/{id}/stop_gracefully',
                           self._cluster_server_stop_gracefully)
        app.router.add_get('/cluster/server/{id}/start', self._cluster_server_start)
        app.router.add_get('/cluster/server/{id}/restart', self._cluster_server_restart)
        app.router.add_get('/cluster/addserver', self._cluster_server_add)
        # TODO: only pass UUID
        app.router.add_put('/cluster/remove-node/{initiator}', self._cluster_remove_node)
        app.router.add_get('/cluster/decommission-node/{ip}', self._cluster_decommission_node)
        app.router.add_get('/cluster/server/{id}/get_config', self._server_get_config)
        app.router.add_put('/cluster/server/{id}/update_config', self._server_update_config)

    async def _manager_up(self, _request) -> aiohttp.web.Response:
        return aiohttp.web.Response(text=f"{self.is_running}")

    async def _cluster_up(self, _request) -> aiohttp.web.Response:
        """Is cluster running"""
        return aiohttp.web.Response(text=f"{self.cluster is not None and self.cluster.is_running}")

    async def _is_dirty(self, _request) -> aiohttp.web.Response:
        """Report if current cluster is dirty"""
        if self.cluster is None:
            return aiohttp.web.Response(status=500, text="No cluster active")
        return aiohttp.web.Response(text=f"{self.cluster.is_dirty}")

    async def _cluster_replicas(self, _request) -> aiohttp.web.Response:
        """Return cluster's configured number of replicas (replication factor)"""
        if self.cluster is None:
            return aiohttp.web.Response(status=500, text="No cluster active")
        return aiohttp.web.Response(text=f"{self.cluster.replicas}")

    async def _cluster_running_servers(self, _request) -> aiohttp.web.Response:
        """Return a list of running server ids (IPs)"""
        return aiohttp.web.Response(text=f"{','.join(sorted(self.cluster.running_servers()))}")

    async def _before_test_req(self, _request) -> aiohttp.web.Response:
        await self._before_test(_request.match_info['test_case_name'])
        return aiohttp.web.Response(text="OK")

    async def _after_test(self, _request) -> aiohttp.web.Response:
        assert self.cluster is not None
        assert self.current_test_case_full_name
        logging.info("Finished test %s, cluster: %s", self.current_test_case_full_name, self.cluster)
        try:
            self.cluster.after_test(self.current_test_case_full_name)
        finally:
            self.current_test_case_full_name = ''
        self.is_after_test_ok = True
        return aiohttp.web.Response(text="True")

    async def _mark_dirty(self, _request) -> aiohttp.web.Response:
        """Mark current cluster dirty"""
        assert self.cluster
        self.cluster.is_dirty = True
        return aiohttp.web.Response(text="OK")

    async def _server_stop(self, _request: aiohttp.web.Request, gracefully: bool) \
                        -> aiohttp.web.Response:
        """Stop a server. No-op if already stopped."""
        assert self.cluster
        ret = await self.cluster.server_stop(_request.match_info['id'], gracefully)
        return aiohttp.web.Response(status=200 if ret[0] else 500, text=ret[1])

    async def _cluster_server_stop(self, _request) -> aiohttp.web.Response:
        """Stop a specified server"""
        assert self.cluster
        return await self._server_stop(_request, gracefully = False)

    async def _cluster_server_stop_gracefully(self, _request) -> aiohttp.web.Response:
        """Stop a specified server gracefully"""
        assert self.cluster
        return await self._server_stop(_request, gracefully = True)

    async def _cluster_server_start(self, _request) -> aiohttp.web.Response:
        """Start a specified server (must be stopped)"""
        assert self.cluster
        ret = await self.cluster.server_start(_request.match_info['id'])
        return aiohttp.web.Response(status=200 if ret[0] else 500, text=ret[1])

    async def _cluster_server_restart(self, _request) -> aiohttp.web.Response:
        """Restart a specified server (must be already started)"""
        assert self.cluster
        ret = await self.cluster.server_restart(_request.match_info['id'])
        return aiohttp.web.Response(status=200 if ret[0] else 500, text=ret[1])

    async def _cluster_server_add(self, _request) -> aiohttp.web.Response:
        """Add a new server"""
        assert self.cluster
        server_ip = await self.cluster.add_server()
        return aiohttp.web.Response(text=server_ip)

    async def _cluster_remove_node(self, _request: aiohttp.web.Request) -> aiohttp.web.Response:
        """Run remove node on Scylla REST API for a specified server"""
        assert self.cluster
        # TODO: only pass UUID
        data = await _request.json()
        initiator_ip = _request.match_info["initiator"]
        to_remove_ip = data["to_remove_ip"]
        to_remove_host_id = data["to_remove_host_id"]
        ignore_dead = data["ignore_dead"]
        if to_remove_ip in self.cluster.running:
            logging.warning("_cluster_remove_node %s is a running node", to_remove_ip)
        logging.info("_cluster_remove_node initiator %s server %s %s ignore_dead %s",
                     initiator_ip, to_remove_ip, to_remove_host_id, ignore_dead)

        # initate remove
        try:
            await self.cluster.api.remove_node(initiator_ip, to_remove_host_id, ignore_dead)
        except RuntimeError as exc:
            logging.error("_cluster_remove_node failed initiator %s server %s %s ignore_dead %s, check log at %s",
                          initiator_ip, to_remove_ip, to_remove_host_id, ignore_dead,
                          self.cluster.running[initiator_ip].log_filename)
            return aiohttp.web.Response(status=500,
                                        text=f"Error removing {to_remove_ip} {to_remove_host_id} {exc}")
        self.cluster.server_mark_removed(to_remove_ip)
        return aiohttp.web.Response(text="OK")

    async def _cluster_decommission_node(self, _request) -> aiohttp.web.Response:
        """Run remove node on Scylla REST API for a specified server"""
        assert self.cluster
        to_decommission_ip = _request.match_info["ip"]
        logging.info("_cluster_decommission_node %s", to_decommission_ip)
        assert to_decommission_ip in self.cluster.running, "Can't decommission not running node"
        if len(self.cluster.running) == 1:
            logging.warning("_cluster_decommission_node %s is only running node left",
                            to_decommission_ip)

        # initate decommission
        try:
            await self.cluster.api.decommission_node(to_decommission_ip)
        except RuntimeError as exc:
            logging.error("_cluster_decommission_node %s, check log at %s", to_decommission_ip,
                          self.cluster.running[to_decommission_ip].log_filename)
            return aiohttp.web.Response(status=500,
                                        text=f"Error decommissioning {to_decommission_ip}: {exc}")
        await self.cluster.server_stop(to_decommission_ip, gracefully=True)
        self.cluster.server_mark_decommissioned(to_decommission_ip)
        return aiohttp.web.Response(text="OK")

    async def _server_get_config(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """Get conf/scylla.yaml of the given server as a dictionary."""
        assert self.cluster
        ret = self.cluster.get_config(request.match_info['id'])
        if not ret.success:
            return aiohttp.web.Response(status=404, text=ret.msg)
        return aiohttp.web.json_response(ret.data)

    async def _server_update_config(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        """Update conf/scylla.yaml of the given server by setting `value` under `key`.
           If the server is running, reload the config with a SIGHUP.
           Marks the cluster as dirty."""
        assert self.cluster
        data = await request.json()
        ret = self.cluster.update_config(request.match_info['id'], data['key'], data['value'])
        if not ret.success:
            return aiohttp.web.Response(status=404, text=ret.msg)
        return aiohttp.web.Response()


@asynccontextmanager
async def get_cluster_manager(test_uname: str, clusters: Pool[ScyllaCluster], test_path: str) \
        -> AsyncIterator[ScyllaClusterManager]:
    """Create a temporary manager for the active cluster used in a test
       and provide the cluster to the caller."""
    manager = ScyllaClusterManager(test_uname, clusters, test_path)
    try:
        yield manager
    finally:
        await manager.stop()
