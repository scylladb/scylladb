#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
from contextlib import asynccontextmanager
import itertools
import logging
import os
import pathlib
import shutil
import tempfile
import time
import uuid
from typing import Optional, Dict, List, Set, Callable, AsyncIterator, NamedTuple
from test.pylib.artifact_registry import ArtifactRegistry
from test.pylib.host_registry import HostRegistry
from test.pylib.pool import Pool
from cassandra import InvalidRequest                    # type: ignore
from cassandra import OperationTimedOut                 # type: ignore
from cassandra.auth import PlainTextAuthProvider        # type: ignore
from cassandra.cluster import Cluster, NoHostAvailable  # type: ignore
from cassandra.cluster import Session                   # type: ignore
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT     # type: ignore
from cassandra.policies import WhiteListRoundRobinPolicy  # type: ignore
import aiohttp
import aiohttp.web

#
# Put all Scylla options in a template file. Sic: if you make a typo in the
# configuration file, Scylla will boot fine and ignore the setting.
# Always check the error log after modifying the template.
#
SCYLLA_CONF_TEMPLATE = """cluster_name: {cluster_name}
developer_mode: true

# Allow testing experimental features. Following issue #9467, we need
# to add here specific experimental features as they are introduced.

enable_user_defined_functions: true
experimental: true
experimental_features:
    - raft
    - udf

data_file_directories:
    - {workdir}/data
commitlog_directory: {workdir}/commitlog
hints_directory: {workdir}/hints
view_hints_directory: {workdir}/view_hints

listen_address: {host}
rpc_address: {host}
api_address: {host}
prometheus_address: {host}
alternator_address: {host}

seed_provider:
    - class_name: org.apache.cassandra.locator.simple_seed_provider
      parameters:
          - seeds: {seeds}

skip_wait_for_gossip_to_settle: 0
ring_delay_ms: 0
num_tokens: 16
flush_schema_tables_after_modification: false
auto_snapshot: false

# Significantly increase default timeouts to allow running tests
# on a very slow setup (but without network losses). Note that these
# are server-side timeouts: The client should also avoid timing out
# its own requests - for this reason we increase the CQL driver's
# client-side timeout in conftest.py.

range_request_timeout_in_ms: 300000
read_request_timeout_in_ms: 300000
counter_write_request_timeout_in_ms: 300000
cas_contention_timeout_in_ms: 300000
truncate_request_timeout_in_ms: 300000
write_request_timeout_in_ms: 300000
request_timeout_in_ms: 300000

# Set up authentication in order to allow testing this module
# and other modules dependent on it: e.g. service levels

authenticator: {authenticator}
authorizer: {authorizer}
strict_allow_filtering: true

permissions_update_interval_in_ms: 100
permissions_validity_in_ms: 100
"""

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
]


class ScyllaServer:
    START_TIMEOUT = 300     # seconds

    def __init__(self, exe: str, vardir: str,
                 host_registry,
                 cluster_name: str, seeds: List[str],
                 cmdline_options: List[str],
                 config_options: Dict[str, str]) -> None:
        self.exe = pathlib.Path(exe).resolve()
        self.vardir = pathlib.Path(vardir)
        self.host_registry = host_registry
        self.cmdline_options = cmdline_options
        self.cluster_name = cluster_name
        self.hostname = ""
        self.seeds = seeds
        self.cmd: Optional[asyncio.subprocess.Process] = None
        self.log_savepoint = 0
        self.control_cluster: Optional[Cluster] = None
        self.control_connection: Optional[Session] = None
        self.authenticator: str = config_options["authenticator"]
        self.authorizer: str = config_options["authorizer"]

        async def stop_server() -> None:
            if self.is_running:
                await self.stop()

        async def uninstall_server() -> None:
            await self.uninstall()

        self.stop_artifact = stop_server
        self.uninstall_artifact = uninstall_server

    async def install_and_start(self) -> None:
        await self.install()

        logging.info("starting server at host %s in %s...", self.hostname,
                     self.workdir.name)

        await self.start()

        if self.cmd:
            logging.info("started server at host %s in %s, pid %d", self.hostname,
                         self.workdir.name, self.cmd.pid)

    @property
    def is_running(self) -> bool:
        return self.cmd is not None

    @property
    def host(self) -> str:
        return str(self.hostname)

    def find_scylla_executable(self) -> None:
        if not os.access(self.exe, os.X_OK):
            raise RuntimeError("{} is not executable", self.exe)

    async def install(self) -> None:
        """Create a working directory with all subdirectories, initialize
        a configuration file."""

        self.find_scylla_executable()

        # Scylla assumes all instances of a cluster use the same port,
        # so each instance needs an own IP address.
        self.hostname = await self.host_registry.lease_host()
        if not self.seeds:
            self.seeds = [self.hostname]
        # Use the last part in host IP 127.151.3.27 -> 27
        # There can be no duplicates within the same test run
        # thanks to how host registry registers subnets, and
        # different runs use different vardirs.
        shortname = pathlib.Path("scylla-" + self.host.split(".")[-1])
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
        fmt = {
              "cluster_name": self.cluster_name,
              "host": self.hostname,
              "seeds": ",".join(self.seeds),
              "workdir": self.workdir,
              "authenticator": self.authenticator,
              "authorizer": self.authorizer
        }
        with self.config_filename.open('w') as config_file:
            config_file.write(SCYLLA_CONF_TEMPLATE.format(**fmt))

        self.log_file = self.log_filename.open("wb")

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
                for i in range(3):
                    lines.append(log.readline())
                # Read the lines since the last savepoint
                if self.log_savepoint and self.log_savepoint > log.tell():
                    log.seek(self.log_savepoint)
                return "".join(lines + log.readlines())
        except Exception as e:
            return "Exception when reading server log {}: {}".format(self.log_filename, str(e))

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
                    session.execute("CREATE KEYSPACE IF NOT EXISTS k WITH REPLICATION = {" +
                                    "'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
                    session.execute("DROP KEYSPACE k")
                    self.control_cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                                                   contact_points=[self.hostname],
                                                   auth_provider=auth)
                    self.control_connection = self.control_cluster.connect()
                    return True
        except (NoHostAvailable, InvalidRequest, OperationTimedOut) as e:
            logging.debug("Exception when checking if CQL is up: {}".format(e))
            return False
        finally:
            caslog.setLevel(oldlevel)
        # Any other exception may indicate a problem, and is passed to the caller.

    async def rest_api_is_up(self) -> bool:
        """Test that the Scylla REST API is serving. Can be used as a
        checker function at start up."""
        try:
            async with aiohttp.ClientSession() as s:
                url = "http://{}:10000/".format(self.hostname)
                async with s.get(url):
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
                    logging.error("last line of {}:".format(self.log_filename))
                    log_file.seek(0, 0)
                    logging.error(log_file.readlines()[-1].rstrip())
                    h = logging.getLogger().handlers[0]
                    logpath = h.baseFilename if hasattr(h, 'baseFilename') else "?"  # type: ignore
                    raise RuntimeError("""Failed to start server at host {}.
Check the log files:
{}
{}""".format(self.hostname, logpath, self.log_filename))

            if await self.rest_api_is_up():
                if await self.cql_is_up():
                    return

            # Sleep and retry
            await asyncio.sleep(sleep_interval)

        raise RuntimeError("failed to start server {}, check server log at {}".format(
            self.host, self.log_filename))

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
        self.log_file.seek(0, 2)  # seek to file end
        self.log_file.write(msg.encode())
        self.log_file.flush()

    def __str__(self):
        return self.hostname


class ScyllaCluster:

    class ActionReturn(NamedTuple):
        success: bool
        msg: str

    def __init__(self, replicas: int,
                 create_server: Callable[[str, Optional[List[str]]], ScyllaServer]) -> None:
        self.name = str(uuid.uuid1())
        self.replicas = replicas
        self.create_server = create_server
        self.running: Dict[str, ScyllaServer] = {}  # started servers
        self.stopped: Dict[str, ScyllaServer] = {}  # servers no longer running but present
        self.removed: Set[str] = set()              # servers stopped and uninstalled (can't return)
        # cluster is started (but it might not have running servers)
        self.is_running: bool = False
        # cluster was modified in a way it should not be used in subsequent tests
        self.is_dirty: bool = False
        self.start_exception: Optional[Exception] = None
        self.keyspace_count = 0

    async def install_and_start(self) -> None:
        try:
            for i in range(self.replicas):
                await self.add_server()
            self.keyspace_count = self._get_keyspace_count()
        except (RuntimeError, NoHostAvailable, InvalidRequest, OperationTimedOut) as e:
            # If start fails, swallow the error to throw later,
            # at test time.
            self.start_exception = e
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
        return [server for server in self.running.keys()]

    async def add_server(self) -> str:
        """Add a new server to the cluster"""
        server = self.create_server(self.name, self._seeds())
        self.is_dirty = True
        try:
            logging.info("Cluster %s adding server", server)
            await server.install_and_start()
        except Exception as e:
            logging.error("Failed to start Scylla server at host %s in %s: %s",
                          server.hostname, server.workdir.name, str(e))
            raise
        self.running[server.host] = server
        return server.host

    def __getitem__(self, i: int) -> ScyllaServer:
        assert i >= 0, "ScyllaCluster: cluster sub-index must be positive"
        return list(self.running.values())[i]

    def __str__(self):
        return f"{{{', '.join(str(c) for c in self.running)}}}"

    def _get_keyspace_count(self) -> int:
        """Get the current keyspace count"""
        assert(self.start_exception is None)
        assert self[0].control_connection is not None
        rows = self[0].control_connection.execute(
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
        assert(self.start_exception is None)
        if self._get_keyspace_count() != self.keyspace_count:
            raise RuntimeError("Test post-condition failed, "
                               "the test must drop all keyspaces it creates.")
        for server in itertools.chain(self.running.values(), self.stopped.values()):
            server.write_log_marker(f"------ Ending test {name} ------\n")

    async def server_stop(self, server_id: str, gracefully: bool) -> ActionReturn:
        """Stop a server. No-op if already stopped."""
        logging.info("Cluster %s stopping server %s", self, server_id)
        if server_id in self.stopped:
            return ScyllaCluster.ActionReturn(success=True, msg=f"Server {server_id} already stopped")
        if server_id in self.removed:
            return ScyllaCluster.ActionReturn(success=False, msg=f"Server {server_id} removed")
        if server_id not in self.running:
            return ScyllaCluster.ActionReturn(success=False, msg=f"Server {server_id} unknown")
        self.is_dirty = True
        server = self.running.pop(server_id)
        if gracefully:
            await server.stop_gracefully()
        else:
            await server.stop()
        self.stopped[server_id] = server
        return ScyllaCluster.ActionReturn(success=True, msg=f"Server {server_id} stopped")

    async def server_start(self, server_id: str) -> ActionReturn:
        """Start a stopped server"""
        logging.info("Cluster %s starting server", self)
        if server_id in self.running:
            return ScyllaCluster.ActionReturn(success=True, msg=f"Server {server_id} already started")
        if server_id in self.removed:
            return ScyllaCluster.ActionReturn(success=False, msg=f"Server {server_id} removed")
        if server_id not in self.stopped:
            return ScyllaCluster.ActionReturn(success=False, msg=f"Server {server_id} unknown")
        self.is_dirty = True
        server = self.stopped.pop(server_id)
        server.seeds = self._seeds()
        await server.start()
        self.running[server_id] = server
        return ScyllaCluster.ActionReturn(success=True, msg=f"Server {server_id} started")

    async def server_restart(self, server_id: str) -> ActionReturn:
        """Restart a running server"""
        ret = await self.server_stop(server_id, gracefully=True)
        if not ret.success:
            return ret
        return await self.server_start(server_id)

    async def server_remove(self, server_id: str) -> ActionReturn:
        """Remove a specified server"""
        self.is_dirty = True
        logging.info("Cluster %s removing server %s", self, server_id)
        if server_id in self.running:
            server = self.running.pop(server_id)
            await server.stop_gracefully()
        elif server_id in self.stopped:
            server = self.stopped.pop(server_id)
        else:
            return ScyllaCluster.ActionReturn(success=False, msg=f"Server {server_id} unknown")
        await server.uninstall()
        self.removed.add(server_id)
        return ScyllaCluster.ActionReturn(success=True, msg=f"Server {server_id} removed")


class ScyllaClusterManager:
    """Manages a Scylla cluster for running test cases
       Provides an async API for tests to request changes in the Cluster.
       Parallel requests are not supported.
    """
    cluster: ScyllaCluster
    site: aiohttp.web.UnixSite

    def __init__(self, test_name: str, clusters: Pool[ScyllaCluster], base_dir: str) -> None:
        self.test_name: str = test_name
        self.clusters: Pool[ScyllaCluster] = clusters
        self.is_running: bool = False
        # API
        # NOTE: need to make a safe temp dir as tempfile can't make a safe temp sock name
        self.manager_dir: str = tempfile.mkdtemp(prefix="manager-", dir=base_dir)
        self.sock_path: str = f"{self.manager_dir}/api"
        self.app = aiohttp.web.Application()
        self._setup_routes()
        self.runner = aiohttp.web.AppRunner(self.app)

    async def start(self) -> None:
        """Get first cluster, setup API"""
        await self._get_cluster()
        await self.runner.setup()
        self.site = aiohttp.web.UnixSite(self.runner, path=self.sock_path)
        await self.site.start()
        self.is_running = True

    async def stop(self) -> None:
        """Stop, cycle last cluster if not dirty and present"""
        await self.site.stop()
        self.cluster.after_test(self.test_name)
        await self._return_cluster()
        if os.path.exists(self.manager_dir):
            shutil.rmtree(self.manager_dir)

    async def _get_cluster(self) -> None:
        self.cluster = await self.clusters.get()
        logging.info("Getting new Scylla cluster %s", self.cluster)

    async def _return_cluster(self) -> None:
        logging.info("Returning Scylla cluster %s", self.cluster)
        await self.clusters.put(self.cluster)
        del self.cluster

    def _setup_routes(self) -> None:
        self.app.router.add_get('/up', self._manager_up)
        self.app.router.add_get('/cluster/up', self._cluster_up)
        self.app.router.add_get('/cluster/is-dirty', self._is_dirty)
        self.app.router.add_get('/cluster/replicas', self._cluster_replicas)
        self.app.router.add_get('/cluster/servers', self._cluster_servers)

    async def _manager_up(self, request) -> aiohttp.web.Response:
        return aiohttp.web.Response(text=f"{self.is_running}")

    async def _cluster_up(self, request) -> aiohttp.web.Response:
        """Is cluster running"""
        return aiohttp.web.Response(text=f"{self.cluster is not None and self.cluster.is_running}")

    async def _is_dirty(self, request) -> aiohttp.web.Response:
        """Report if current cluster is dirty"""
        if self.cluster is None:
            return aiohttp.web.Response(status=500, text="No cluster active")
        return aiohttp.web.Response(text=f"{self.cluster.is_dirty}")

    async def _cluster_replicas(self, request) -> aiohttp.web.Response:
        """Return cluster's configured number of replicas (replication factor)"""
        if self.cluster is None:
            return aiohttp.web.Response(status=500, text="No cluster active")
        return aiohttp.web.Response(text=f"{self.cluster.replicas}")

    async def _cluster_servers(self, request) -> aiohttp.web.Response:
        """Return a list of active server ids (IPs)"""
        return aiohttp.web.Response(text=f"{','.join(sorted(self.cluster.running.keys()))}")

@asynccontextmanager
async def get_cluster_manager(test_name: str, clusters: Pool[ScyllaCluster], test_path: str) \
        -> AsyncIterator[ScyllaClusterManager]:
    """Create a temporary manager for the active cluster used in a test
       and provide the cluster to the caller."""
    manager = ScyllaClusterManager(test_name, clusters, test_path)
    await manager.start()
    try:
        yield manager
    finally:
        await manager.stop()
