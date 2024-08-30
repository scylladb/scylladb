#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""Scylla clusters for testing.
   Provides helpers to setup and manage clusters of Scylla servers for testing.
"""
from functools import wraps
import asyncio
import concurrent.futures
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
import traceback
from typing import Any, Optional, Dict, List, Set, Tuple, Callable, AsyncIterator, NamedTuple, Union
import uuid
from enum import Enum
from io import BufferedWriter
from test.pylib.host_registry import Host, HostRegistry
from test.pylib.pool import Pool
from test.pylib.rest_client import ScyllaRESTAPIClient, HTTPError
from test.pylib.util import LogPrefixAdapter, read_last_line
from test.pylib.internal_types import ServerNum, IPAddress, HostID, ServerInfo
from functools import partial
import aiohttp
import aiohttp.web
import yaml
import signal
import glob
import errno
import re

from cassandra import InvalidRequest                    # type: ignore
from cassandra import OperationTimedOut                 # type: ignore
from cassandra.auth import PlainTextAuthProvider        # type: ignore
from cassandra.cluster import Cluster           # type: ignore # pylint: disable=no-name-in-module
from cassandra.cluster import NoHostAvailable   # type: ignore # pylint: disable=no-name-in-module
from cassandra.cluster import Session           # pylint: disable=no-name-in-module
from cassandra.cluster import ExecutionProfile  # pylint: disable=no-name-in-module
from cassandra.cluster import EXEC_PROFILE_DEFAULT  # pylint: disable=no-name-in-module
from cassandra.policies import WhiteListRoundRobinPolicy  # type: ignore
from cassandra.connection import UnixSocketEndPoint


io_executor = concurrent.futures.ThreadPoolExecutor(max_workers=20)

async def async_rmtree(directory, *args, **kwargs):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(io_executor, partial(shutil.rmtree, directory, *args, **kwargs))


class ReplaceConfig(NamedTuple):
    replaced_id: ServerNum
    reuse_ip_addr: bool
    use_host_id: bool
    ignore_dead_nodes: list[IPAddress | HostID] = []
    wait_replaced_dead: bool = True


def make_scylla_conf(mode: str, workdir: pathlib.Path, host_addr: str, seed_addrs: List[str], cluster_name: str,
                     socket_path: str, server_encryption: str) -> dict[str, object]:
    # We significantly increase default timeouts to allow running tests on a very slow
    # setup (but without network losses). These timeouts can impact the running time of
    # topology tests. For example, the barrier_and_drain topology command waits until
    # background writes' handlers time out. We don't want to slow down tests for no
    # reason, so we increase the timeouts according to each mode's needs. The client
    # should avoid timing out its requests before the server times out - for this reason
    # we increase the CQL driver's client-side timeout in conftest.py.
    request_timeout_in_ms = 180000 if mode in {'debug', 'sanitize'} else 30000

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
        'experimental_features': ['udf',
                                  'alternator-streams',
                                  'broadcast-tables',
                                  'keyspace-storage-options'],

        'skip_wait_for_gossip_to_settle': 0,
        'ring_delay_ms': 0,
        'num_tokens': 16,
        'flush_schema_tables_after_modification': False,
        'auto_snapshot': False,

        'range_request_timeout_in_ms': request_timeout_in_ms,
        'read_request_timeout_in_ms': request_timeout_in_ms,
        'counter_write_request_timeout_in_ms': request_timeout_in_ms,
        'cas_contention_timeout_in_ms': request_timeout_in_ms,
        'truncate_request_timeout_in_ms': request_timeout_in_ms,
        'write_request_timeout_in_ms': request_timeout_in_ms,
        'request_timeout_in_ms': request_timeout_in_ms,
        'user_defined_function_time_limit_ms': 1000,

        'strict_allow_filtering': True,
        'strict_is_not_null_in_views': True,

        'permissions_update_interval_in_ms': 100,
        'permissions_validity_in_ms': 100,

        'reader_concurrency_semaphore_serialize_limit_multiplier': 0,
        'reader_concurrency_semaphore_kill_limit_multiplier': 0,

        'maintenance_socket': socket_path,

        'service_levels_interval_ms': 500,

        'server_encryption_options': {
            'internode_encryption': server_encryption,
            'certificate': 'conf/scylla.crt',
            'keyfile': 'conf/scylla.key',
            'truststore': 'conf/scyllacadb.pem',
        },

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
    '--max-networking-io-control-blocks', '1000',
    '--unsafe-bypass-fsync', '1',
    '--kernel-page-cache', '1',
    '--commitlog-use-o-dsync', '0',
    '--abort-on-lsa-bad-alloc', '1',
    '--abort-on-seastar-bad-alloc',
    '--abort-on-internal-error', '1',
    '--abort-on-ebadf', '1',
    '--logger-log-level', 'raft_topology=debug',
    '--logger-log-level', 'query_processor=debug',
    '--logger-log-level', 'group0_raft_sm=trace',
]

# [--smp, 1], [--smp, 2] -> [--smp, 2]
# [--smp, 1], [--smp] -> [--smp]
# [--smp, 1], [--smp, __missing__] -> [--smp]
# [--smp, 1], [--smp, __remove__] -> []
# [--smp=1], [--smp=2] -> [--smp, 2]
# [--smp=1], [--smp=__remove__] -> []
# [--overprovisioned, --smp=1, --abort-on-ebadf], [--smp=2] -> [--overprovisioned, --smp=2, --abort-on-ebadf]
# [], [--experimental-features, raft, --experimental-features, broadcast-tables] ->
# [--experimental-features, raft, --experimental-features, broadcast-tables]
def merge_cmdline_options(
        base: List[str], override: List[str], appending_options: List[str] = ["--logger-log-level"]) -> List[str]:
    if len(override) == 0:
        return base

    def to_dict(args: List[str]) -> Dict[str, List[Optional[str]]]:
        result: Dict[str, List[Optional[str]]] = {}
        i = 0
        while i < len(args):
            name = args[i]
            if not name.startswith('-'):
                raise ValueError(f'invalid argument name {name}, all args {args}')
            if '=' in name:
                name, _, value = name.partition('=')
                i += 1
            elif i < len(args) - 1 and not args[i + 1].startswith('-'):
                value = args[i + 1]
                i += 2
            else:
                value = None
                i += 1
            result.setdefault(name, []).append(value)
        return result

    def run() -> List[str]:
        merged: Dict[str, List[Optional[str]]] = to_dict(base)
        for name, values in to_dict(override).items():
            merged_values = None
            for v in values:
                if v != '__remove__':
                    if merged_values is None:
                        merged_values = merged.setdefault(name, [])
                        if name not in appending_options:
                            merged_values.clear()
                    merged_values.append(v if v != '__missing__' else None)
                elif name in merged:
                    del merged[name]
                    merged_values = None

        result: List[str] = []
        for name, values in merged.items():
            for v in values:
                result.append(name)
                if v is not None:
                    result.append(v)
        return result

    return run()

class CqlUpState(Enum):
    NOT_CONNECTED = 1,
    CONNECTED = 2,
    QUERIED = 3


def start_stop_lock(func):
    """
    The methods stop, stop_gracefully, and start in ScyllaServer
    are not designed for parallel execution.
    This lock ensures that these methods are executed sequentially
    """
    async def wrap(self: 'ScyllaServer', *args, **kwargs):
        async with self.start_stop_lock:
            result = await func(self, *args, **kwargs)
        return result
    return wrap


def stop_event(func):
    """
    interrupt start node on state "wait for node started" if someone wants to stop it
    """
    async def wrap(self: 'ScyllaServer', *args, **kwargs):
        try:
            self.stop_event.set()
            result = await func(self, *args, **kwargs)
        finally:
            self.stop_event.clear()
        return result
    return wrap


class ScyllaServer:
    """Starts and handles a single Scylla server, managing logs, checking if responsive,
       and cleanup when finished."""
    # pylint: disable=too-many-instance-attributes

    # in seconds, used for topology operations such as bootstrap or decommission
    TOPOLOGY_TIMEOUT = 1000
    start_time: float
    sleep_interval: float
    log_file: BufferedWriter
    host_id: HostID                             # Host id (UUID)
    newid = itertools.count(start=1).__next__   # Sequential unique id

    def __init__(self, mode: str, exe: str, vardir: str,
                 logger: Union[logging.Logger, logging.LoggerAdapter],
                 cluster_name: str, ip_addr: str, seeds: List[str],
                 cmdline_options: List[str],
                 config_options: Dict[str, Any],
                 property_file: Dict[str, Any],
                 append_env: Dict[str,Any],
                 server_encryption: str) -> None:
        # pylint: disable=too-many-arguments
        self.server_id = ServerNum(ScyllaServer.newid())
        # this variable needed to make a cleanup after server is not needed anymore
        self.maintenance_socket_dir = tempfile.TemporaryDirectory(prefix=f"scylladb-{self.server_id}-test.py-")
        self.maintenance_socket_path = f"{self.maintenance_socket_dir.name}/cql.m"
        self.exe = pathlib.Path(exe).resolve()
        self.vardir = pathlib.Path(vardir)
        self.logger = logger
        self.cmdline_options = merge_cmdline_options(SCYLLA_CMDLINE_OPTIONS, cmdline_options)
        self.cluster_name = cluster_name
        self.ip_addr = IPAddress(ip_addr)
        self.seeds = seeds
        self.cmd: Optional[Process] = None
        self.start_stop_lock = asyncio.Lock()
        self.stop_event = asyncio.Event()
        self.log_savepoint = 0
        self.control_cluster: Optional[Cluster] = None
        self.control_connection: Optional[Session] = None
        shortname = f"scylla-{self.server_id}"
        self.workdir = self.vardir / shortname
        self.log_filename = (self.vardir / shortname).with_suffix(".log")
        self.config_filename = self.workdir / "conf/scylla.yaml"
        self.property_filename = self.workdir / "conf/cassandra-rackdc.properties"
        self.certificate_filename = self.workdir / "conf/scylla.crt"
        self.keyfile_filename = self.workdir / "conf/scylla.key"
        self.truststore_filename = self.workdir / "conf/scyllacadb.pem"
        self.resourcesdir = pathlib.Path.cwd() / "test/pylib/resources"
        self.resources_certificate_file = self.resourcesdir / "scylla.crt"
        self.resources_keyfile_file = self.resourcesdir / "scylla.key"

        # Sum of basic server configuration and the user-provided config options.
        self.config = make_scylla_conf(
                mode = mode,
                workdir = self.workdir,
                host_addr = self.ip_addr,
                seed_addrs = self.seeds,
                cluster_name = self.cluster_name,
                server_encryption = server_encryption,
                socket_path=self.maintenance_socket_path) \
            | config_options
        self.property_file = property_file
        self.append_env = append_env

    def change_ip(self, ip_addr: IPAddress) -> None:
        """Change IP address of the current server. Pre: the server is
        stopped"""
        if self.is_running:
            raise RuntimeError(f"Can't change IP of a running server {self.ip_addr}.")
        self.ip_addr = ip_addr
        self.config["listen_address"] = ip_addr
        self.config["rpc_address"] = ip_addr
        self.config["api_address"] = ip_addr
        self.config["prometheus_address"] = ip_addr
        self.config["alternator_address"] = ip_addr
        self._write_config_file()

    def change_seeds(self, seeds: List[str]):
        """Change seeds of the current server. Pre: the server is stopped"""
        if self.is_running:
            raise RuntimeError(f"Can't change seeds of a running server {self.ip_addr}.")
        self.seeds = seeds
        self.config['seed_provider'][0]['parameters'][0]['seeds'] = '{}'.format(','.join(seeds))
        self._write_config_file()

    @property
    def rpc_address(self) -> IPAddress:
        return self.config["rpc_address"]

    def change_rpc_address(self, rpc_address: IPAddress) -> None:
        """Change RPC IP address of the current server. Pre: the server is
        stopped"""
        if self.is_running:
            raise RuntimeError(f"Can't change RPC IP of a running server {self.config['rpc_address']}.")
        self.config["rpc_address"] = rpc_address
        self._write_config_file()

    def wipe_sstables(self, keyspace: str, table: str):
        root_dir = self.workdir/"data"
        for f in glob.iglob(f"./{keyspace}/{table}-????????????????????????????????/**/*", root_dir=root_dir, recursive=True):
            if ((root_dir/f).is_file()):
                (root_dir/f).unlink()

    def get_sstables_disk_usage(self, keyspace: str, table: str) -> int:
        size = 0

        if self.cmd is not None:
            deleted_sstable_re = rf"^.*/{keyspace}/{table}-[0-9a-f]{{32}}/.* \(deleted\)$"
            deleted_sstable_re = re.compile(deleted_sstable_re)
            for f in pathlib.Path(f"/proc/{self.cmd.pid}/fd/").iterdir():
                try:
                    link = f.readlink()
                    if deleted_sstable_re.match(str(link)) is not None:
                        size += f.stat().st_size
                except OSError as e:
                    if e.errno != errno.ENOENT:
                        raise

        table_dir = self.workdir/"data"
        for f in table_dir.glob(f"{keyspace}/{table}-????????????????????????????????/**/*"):
            try:
                size += f.stat().st_size
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise

        return size

    async def install_and_start(self, api: ScyllaRESTAPIClient, expected_error: Optional[str] = None) -> None:
        """Setup and start this server"""
        await self.install()

        self.logger.info("starting server at host %s in %s...", self.ip_addr, self.workdir.name)

        try:
            await self.start(api, expected_error)
        except:
            await self.stop()
            raise

        if self.cmd:
            self.logger.info("started server at host %s in %s, pid %d", self.ip_addr,
                         self.workdir.name, self.cmd.pid)
        elif expected_error:
            self.logger.info("starting server at host %s in %s failed with an expected error",
                             self.ip_addr, self.workdir.name)

    @property
    def is_running(self) -> bool:
        """Check the server subprocess is up"""
        return self.cmd is not None

    def check_scylla_executable(self) -> None:
        """Check if executable exists and can be run"""
        if not os.access(self.exe, os.X_OK):
            raise RuntimeError(f"{self.exe} is not executable")

    async def install(self) -> None:
        """Create a working directory with all subdirectories, initialize
        a configuration file."""

        self.check_scylla_executable()

        self.logger.info("installing Scylla server in %s...", self.workdir)

        # Cleanup any remains of the previously running server in this path
        await async_rmtree(self.workdir, ignore_errors=True)

        try:
            self.workdir.mkdir(parents=True, exist_ok=True)
            self.config_filename.parent.mkdir(parents=True, exist_ok=True)
            self._write_config_file()

            self.log_file = self.log_filename.open("wb")
        except:
            try:
                await async_rmtree(self.workdir)
            except FileNotFoundError:
                pass
            self.log_filename.unlink(missing_ok=True)
            raise

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

    def update_cmdline(self, cmdline_options: List[str]) -> None:
        """Update the command-line options by merging the new options into the existing ones.
           Takes effect only after the node is restarted."""
        self.cmdline_options = merge_cmdline_options(self.cmdline_options, cmdline_options)

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

    def in_maintenance_mode(self) -> bool:
        """Return True if the server is in maintenance mode"""
        return self.config.get("maintenance_mode", False)

    def maintenance_socket(self) -> Optional[str]:
        """Return the maintenance socket path"""
        maintenance_socket_option = self.config["maintenance_socket"]
        if maintenance_socket_option == "workdir":
            return (self.workdir / "cql.m").absolute().as_posix()
        elif maintenance_socket_option == "ignore":
            return None
        return maintenance_socket_option

    async def cql_is_up(self) -> CqlUpState:
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
        in_maintenance_mode = self.in_maintenance_mode()

        if in_maintenance_mode:
            maintenance_socket = self.maintenance_socket()
            if maintenance_socket is None:
                raise RuntimeError("Can't check CQL in maintenance mode without a maintenance socket")
            profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([UnixSocketEndPoint(maintenance_socket)]),
                                       request_timeout=self.TOPOLOGY_TIMEOUT)
            contact_points = [UnixSocketEndPoint(maintenance_socket)]
        else:
            profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([self.rpc_address]),
                                       request_timeout=self.TOPOLOGY_TIMEOUT)
            contact_points=[self.rpc_address]
        connected = False
        try:
            # In a cluster setup, it's possible that the CQL
            # here is directed to a node different from the initial contact
            # point, so make sure we execute the checks strictly via
            # this connection
            with Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                         contact_points=contact_points,
                         # This is the latest version Scylla supports
                         protocol_version=4,
                         control_connection_timeout=self.TOPOLOGY_TIMEOUT,
                         auth_provider=auth) as cluster:
                with cluster.connect() as session:
                    connected = True
                    # See the comment above about `auth::standard_role_manager`. We execute
                    # a 'real' query to ensure that the auth service has finished initializing.
                    session.execute("SELECT key FROM system.local where key = 'local'")
                    self.control_cluster = Cluster(execution_profiles=
                                                        {EXEC_PROFILE_DEFAULT: profile},
                                                   contact_points=contact_points,
                                                   control_connection_timeout=self.TOPOLOGY_TIMEOUT,
                                                   auth_provider=auth)
                    self.control_connection = self.control_cluster.connect()
                    return CqlUpState.QUERIED
        except (NoHostAvailable, InvalidRequest, OperationTimedOut) as exc:
            self.logger.debug("Exception when checking if CQL is up: %s", exc)
            return CqlUpState.CONNECTED if connected else CqlUpState.NOT_CONNECTED
        finally:
            caslog.setLevel(oldlevel)
        # Any other exception may indicate a problem, and is passed to the caller.

    async def get_host_id(self, api: ScyllaRESTAPIClient) -> bool:
        """Try to get the host id (also tests Scylla REST API is serving)"""
        try:
            self.host_id = await api.get_host_id(self.ip_addr)
            return True
        except (aiohttp.ClientConnectionError, HTTPError) as exc:
            if isinstance(exc, HTTPError) and exc.code >= 500:
                raise exc
            return False
        # Any other exception may indicate a problem, and is passed to the caller.

    @start_stop_lock
    async def start(self, api: ScyllaRESTAPIClient, expected_error: Optional[str] = None) -> None:
        """Start an installed server. May be used for restarts."""

        env = os.environ.copy()
        # remove from env to make sure user's SCYLLA_HOME has no impact
        env.pop('SCYLLA_HOME', None)
        env.update(self.append_env)
        env['UBSAN_OPTIONS'] = f'halt_on_error=1:abort_on_error=1:suppressions={os.getcwd()}/ubsan-suppressions.supp'
        env['ASAN_OPTIONS'] = f'disable_coredump=0:abort_on_error=1:detect_stack_use_after_return=1'

        self.cmd = await asyncio.create_subprocess_exec(
            self.exe,
            *self.cmdline_options,
            cwd=self.workdir,
            stderr=self.log_file,
            stdout=self.log_file,
            env=env,
            preexec_fn=os.setsid,
        )

        self.start_time = time.time()
        sleep_interval = 0.1
        cql_up_state = CqlUpState.NOT_CONNECTED

        def report_error(message: str):
            message += f", server_id {self.server_id}, IP {self.ip_addr}, workdir {self.workdir.name}"
            message += f", host_id {self.host_id if hasattr(self, 'host_id') else '<missing>'}"
            message += f", cql [{'connected' if cql_up_state == CqlUpState.CONNECTED else 'not connected'}]"
            if expected_error is not None:
                message += f", the node log was expected to contain the string [{expected_error}]"
            self.logger.error(message)
            self.logger.error("last line of %s:\n%s", self.log_filename, read_last_line(self.log_filename))
            log_handler = logging.getLogger().handlers[0]
            if hasattr(log_handler, 'baseFilename'):
                logpath = log_handler.baseFilename   # type: ignore
            else:
                logpath = "?"
            raise RuntimeError(message + "\nCheck the log files:\n"
                                         f"{logpath}\n"
                                         f"{self.log_filename}")

        while time.time() < self.start_time + self.TOPOLOGY_TIMEOUT and not self.stop_event.is_set():
            assert self.cmd is not None
            if self.cmd.returncode:
                self.cmd = None
                if expected_error is not None:
                    with self.log_filename.open('r') as log_file:
                        for line in log_file:
                            if expected_error in line:
                                return
                        report_error("the node startup failed, but the log file doesn't contain the expected error")
                report_error("failed to start the node")

            if hasattr(self, "host_id") or await self.get_host_id(api):
                cql_up_state = await self.cql_is_up()
                if cql_up_state == CqlUpState.QUERIED:
                    if expected_error is not None:
                        report_error("the node started, but was expected to fail with the expected error")
                    return

            # Sleep and retry
            await asyncio.sleep(sleep_interval)

        if self.stop_event.is_set():
            report_error('failed to start the node as it was requested to be stopped in the meantime')
        else:
            report_error('failed to start the node, timeout reached')

    async def force_schema_migration(self) -> None:
        """This is a hack to change schema hash on an existing cluster node
        which triggers a gossip round and propagation of entire application
        state. Helps quickly propagate tokens and speed up node boot if the
        previous state propagation was missed."""
        auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
        profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy(self.seeds),
                                   request_timeout=self.TOPOLOGY_TIMEOUT)
        with Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                     contact_points=self.seeds,
                     auth_provider=auth,
                     # This is the latest version Scylla supports
                     protocol_version=4,
                     ) as cluster:
            with cluster.connect() as session:
                session.execute("CREATE KEYSPACE IF NOT EXISTS k WITH REPLICATION = {" +
                                "'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }")
                session.execute("DROP KEYSPACE k")

    async def shutdown_control_connection(self) -> None:
        """Shut down driver connection"""
        if self.control_connection is not None:
            self.control_connection.shutdown()
            self.control_connection = None
        if self.control_cluster is not None:
            self.control_cluster.shutdown()
            self.control_cluster = None

    @stop_event
    @start_stop_lock
    async def stop(self) -> None:
        """Stop a running server. No-op if not running. Uses SIGKILL to
        stop, so is not graceful. Waits for the process to exit before return."""
        self.logger.info("stopping %s in %s", self, self.workdir.name)
        if not self.cmd:
            return

        # Dump the profile if exists and supported by the API.
        try:
            api = ScyllaRESTAPIClient()
            await api.dump_llvm_profile(self.ip_addr)
        except:
            # since it is not part of the test functionality, allow
            # this step to fail unconditionally.
            pass
        await self.shutdown_control_connection()
        try:
            self.cmd.kill()
        except ProcessLookupError:
            pass
        else:
            await self.cmd.wait()
        finally:
            if self.cmd:
                self.logger.info("stopped %s in %s", self, self.workdir.name)
            self.cmd = None

    @stop_event
    @start_stop_lock
    async def stop_gracefully(self) -> None:
        """Stop a running server. No-op if not running. Uses SIGTERM to
        stop, so it is graceful. Waits for the process to exit before return."""
        self.logger.info("gracefully stopping %s", self)
        if not self.cmd:
            return

        await self.shutdown_control_connection()
        try:
            self.cmd.terminate()
        except ProcessLookupError:
            pass
        else:
            STOP_TIMEOUT_SECONDS = 120
            wait_task = self.cmd.wait()
            try:
                await asyncio.wait_for(wait_task, timeout=STOP_TIMEOUT_SECONDS)
                if self.cmd.returncode != 0:
                    raise RuntimeError(f"Server {self} exited with non-zero exit code: {self.cmd.returncode}")
            except asyncio.TimeoutError:
                self.cmd.kill()
                await self.cmd.wait()
                raise RuntimeError(
                    f"Stopping server {self} gracefully took longer than {STOP_TIMEOUT_SECONDS}s")
        finally:
            if self.cmd:
                self.logger.info("gracefully stopped %s", self)
            self.cmd = None

    def pause(self) -> None:
        """Pause a running server."""
        if self.cmd:
            self.cmd.send_signal(signal.SIGSTOP)

    def unpause(self) -> None:
        """Unpause a paused server."""
        if self.cmd:
            self.cmd.send_signal(signal.SIGCONT)

    async def uninstall(self) -> None:
        """Clear all files left from a stopped server, including the
        data files and log files."""

        self.logger.info("Uninstalling server at %s", self.workdir)

        try:
            await async_rmtree(self.workdir)
        except FileNotFoundError:
            pass
        self.log_filename.unlink(missing_ok=True)

    def write_log_marker(self, msg) -> None:
        """Write a message to the server's log file (e.g. separator/marker)"""
        self.log_file.seek(0, 2)  # seek to file end
        self.log_file.write(msg.encode())
        self.log_file.flush()

    def setLogger(self, logger: logging.LoggerAdapter):
        """Change the logger used by the server.
           Called when a cluster is reused between tests so that logs during the new test
           are prefixed appropriately with the corresponding test's name.
        """
        self.logger = logger

    def __str__(self):
        host_id = getattr(self, 'host_id', 'undefined id')
        return f"ScyllaServer({self.server_id}, {self.ip_addr}, {host_id})"

    def _write_config_file(self) -> None:
        with self.config_filename.open('w') as config_file:
            yaml.dump(self.config, config_file)
        if self.property_file:
            with self.property_filename.open('w') as property_file:
                for key, value in self.property_file.items():
                    property_file.write(f'{key}={value}\n')
        shutil.copyfile(self.resources_certificate_file, self.certificate_filename)
        shutil.copyfile(self.resources_keyfile_file, self.keyfile_filename)
        shutil.copyfile(self.resources_certificate_file, self.truststore_filename)

class ScyllaCluster:
    """A cluster of Scylla servers providing an API for changes"""
    # pylint: disable=too-many-instance-attributes

    class CreateServerParams(NamedTuple):
        logger: Union[logging.Logger, logging.LoggerAdapter]
        cluster_name: str
        ip_addr: IPAddress
        seeds: List[str]
        property_file: dict[str, Any]
        config_from_test: dict[str, Any]
        cmdline_from_test: List[str]
        server_encryption: str

    def __init__(self, logger: Union[logging.Logger, logging.LoggerAdapter],
                 host_registry: HostRegistry, replicas: int,
                 create_server: Callable[[CreateServerParams], ScyllaServer]) -> None:
        self.logger = logger
        self.host_registry = host_registry
        self.leased_ips = set[IPAddress]()
        self.name = str(uuid.uuid1())
        self.replicas = replicas
        self.create_server = create_server
        # Every ScyllaServer is in one of self.running, self.stopped.
        # These dicts are disjoint.
        # A server ID present in self.removed may be either in self.running or in self.stopped.
        self.running: Dict[ServerNum, ScyllaServer] = {}        # started servers
        self.stopped: Dict[ServerNum, ScyllaServer] = {}        # servers no longer running but present
        self.servers = ChainMap(self.running, self.stopped)
        self.removed: Set[ServerNum] = set()                    # removed servers (might be running)
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
        await asyncio.gather(*(srv.uninstall() for srv in self.stopped.values()))
        await asyncio.gather(*(self.host_registry.release_host(Host(ip))
                               for ip in self.leased_ips))

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
                await asyncio.gather(*(server.stop() for server in self.running.values()))
                self.stopped.update(self.running)
                self.running.clear()

    async def stop_gracefully(self) -> None:
        """Stop all running servers in a clean way"""
        if self.is_running:
            self.is_running = False
            self.logger.info("Cluster %s stopping gracefully", self)
            self.is_dirty = True
            # If self.running is empty, no-op
            await asyncio.gather(*(server.stop_gracefully() for server in self.running.values()))
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
                         property_file: Optional[dict[str, Any]] = None,
                         start: bool = True,
                         seeds: Optional[List[IPAddress]] = None,
                         server_encryption: str = "none",
                         expected_error: Optional[str] = None) -> ServerInfo:
        """Add a new server to the cluster"""
        self.is_dirty = True

        assert start or not expected_error, \
            f"add_server: cannot add a stopped server and expect an error"

        extra_config: dict[str, Any] = config.copy() if config else {}
        if replace_cfg:
            replaced_id = replace_cfg.replaced_id
            assert expected_error or replaced_id in self.servers, \
                f"add_server: replaced id {replaced_id} not found in existing servers"

            replaced_srv = self.servers[replaced_id]
            if replace_cfg.use_host_id:
                extra_config['replace_node_first_boot'] = replaced_srv.host_id
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

        params = ScyllaCluster.CreateServerParams(
            logger = self.logger,
            cluster_name = self.name,
            ip_addr = ip_addr,
            seeds = seeds,
            property_file = property_file,
            config_from_test = extra_config,
            server_encryption = server_encryption,
            cmdline_from_test = cmdline or []
        )

        server = None

        async def handle_join_failure():
            if not replace_cfg or not replace_cfg.reuse_ip_addr:
                self.leased_ips.remove(ip_addr)
                await self.host_registry.release_host(Host(ip_addr))
            self.stopped[server.server_id] = server

        try:
            server = self.create_server(params)
            self.logger.info("Cluster %s adding server...", self)
            if start:
                await server.install_and_start(self.api, expected_error)
            else:
                await server.install()
        except Exception as exc:
            workdir = '<unknown>' if server is None else server.workdir.name
            self.logger.error("Failed to start Scylla server at host %s in %s: %s",
                          ip_addr, workdir, str(exc))
            await handle_join_failure()
            raise

        if expected_error:
            await handle_join_failure()
        else:
            if start:
                self.running[server.server_id] = server
            else:
                self.stopped[server.server_id] = server
            self.logger.info("Cluster %s added %s", self, server)

        return ServerInfo(server.server_id, server.ip_addr, server.rpc_address)

    async def add_servers(self, servers_num: int = 1,
                          cmdline: Optional[List[str]] = None,
                          config: Optional[dict[str, Any]] = None,
                          property_file: Optional[dict[str, Any]] = None,
                          start: bool = True,
                          seeds: Optional[List[IPAddress]] = None,
                          server_encryption: str = "none",
                          expected_error: Optional[str] = None) -> List[ServerInfo]:
        """Add multiple servers to the cluster concurrently"""
        assert servers_num > 0, f"add_servers: cannot add {servers_num} servers"

        return await asyncio.gather(*(self.add_server(None, cmdline, config, property_file, start, seeds, server_encryption, expected_error)
                                      for _ in range(servers_num)))

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

    def running_servers(self) -> list[tuple[ServerNum, IPAddress, IPAddress]]:
        """Get a list of tuples of server id and IP address of running servers (and not removed)"""
        return [(server.server_id, server.ip_addr, server.rpc_address) for server in self.running.values()
                if server.server_id not in self.removed]

    def all_servers(self) -> list[tuple[ServerNum, IPAddress, IPAddress]]:
        """Get a list of tuples of server id and IP address of all servers"""
        return [(server.server_id, server.ip_addr, server.rpc_address) for server in self.servers.values()]

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
        there was a start error, throw it here - the server is
        running when it's added to the pool, which can't be attributed
        to any specific test, throwing it here would stop a specific
        test."""
        if self.start_exception:
            # Mark as dirty so further test cases don't try to reuse this cluster.
            self.is_dirty = True
            raise Exception(f'Exception when starting cluster {self}:\n{self.start_exception}')

        for server in self.running.values():
            server.write_log_marker(f"------ Starting test {name} ------\n")

    def after_test(self, name: str, success: bool) -> None:
        """Mark the cluster as dirty after a failed test.
        If the cluster is not dirty, check that it's still alive and the test
        hasn't left any garbage."""
        assert self.start_exception is None
        if not success:
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

    async def server_stop(self, server_id: ServerNum, gracefully: bool) -> None:
        """Stop a server. No-op if already stopped."""
        self.logger.info("Cluster %s stopping server %s", self, server_id)
        if server_id in self.stopped:
            return
        assert server_id in self.running, f"Server {server_id} unknown"
        self.is_dirty = True
        server = self.running[server_id]
        # Remove the server from `running` only after we successfully stop it.
        # Stopping may fail and if we removed it from `running` now it might leak.
        if gracefully:
            await server.stop_gracefully()
        else:
            await server.stop()
        self.running.pop(server_id)
        self.stopped[server_id] = server

    def server_mark_removed(self, server_id: ServerNum) -> None:
        """Mark server as removed."""
        self.logger.debug("Cluster %s marking server %s as removed", self, server_id)
        self.removed.add(server_id)

    async def server_start(self, server_id: ServerNum, expected_error: Optional[str] = None,
                           seeds: Optional[List[IPAddress]] = None) -> None:
        """Start a server. No-op if already running."""
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
        await server.start(self.api, expected_error)
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

    def get_config(self, server_id: ServerNum) -> dict[str, object]:
        """Get conf/scylla.yaml of the given server as a dictionary.
           Fails if the server cannot be found."""
        assert server_id in self.servers, f"Server {server_id} unknown"
        return self.servers[server_id].get_config()

    def update_config(self, server_id: ServerNum, key: str, value: object) -> None:
        """Update conf/scylla.yaml of the given server by setting `value` under `key`.
           If the server is running, reload the config with a SIGHUP.
           Marks the cluster as dirty.
           Fails if the server cannot be found."""
        assert server_id in self.servers, f"Server {server_id} unknown"
        self.is_dirty = True
        self.servers[server_id].update_config(key, value)

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
        self.base_dir: str = base_dir
        logger = logging.getLogger(self.test_uname)
        self.logger = LogPrefixAdapter(logger, {'prefix': self.test_uname})
        # The currently running test case with self.test_uname prepended, e.g.
        # test_topology.1::test_add_server_add_column
        self.current_test_case_full_name: str = ''
        self.clusters: Pool[ScyllaCluster] = clusters
        self.is_running: bool = False
        self.is_before_test_ok: bool = False
        self.is_after_test_ok: bool = False
        # API
        # NOTE: need to make a safe temp dir as tempfile can't make a safe temp sock name
        # Put the socket in /tmp, not base_dir, to avoid going over the length
        # limit of UNIX-domain socket addresses (issue #12622).
        self.manager_dir: str = tempfile.mkdtemp(prefix="manager-", dir="/tmp")
        self.sock_path: str = f"{self.manager_dir}/api"
        app = aiohttp.web.Application()
        self._setup_routes(app)
        self.runner = aiohttp.web.AppRunner(app)
        self.tasks_history = dict()
        self.server_broken_event = asyncio.Event()

    def repr_tasks_history(self):
        out = "Cluster_history"
        for key, val in self.tasks_history.items():
            out += f"\n{val}:\t\t{repr(key)}"
        return out

    async def start(self) -> None:
        """Get first cluster, setup API"""
        if self.is_running:
            self.logger.warning("ScyllaClusterManager already running")
            return
        self.cluster = await self.clusters.get(self.logger)
        self.logger.info("First Scylla cluster: %s", self.cluster)
        self.cluster.setLogger(self.logger)
        await self.runner.setup()
        self.site = aiohttp.web.UnixSite(self.runner, path=self.sock_path)
        await self.site.start()
        self.is_running = True

    async def _before_test(self, test_case_name: str) -> str:
        self.current_test_case_full_name = f'{self.test_uname}::{test_case_name}'
        root_logger = logging.getLogger()
        # file handler file name should be consistent with topology/conftest.py:manager test_py_log_test variable
        self.test_case_log_fh = logging.FileHandler(f"{self.base_dir}/{test_case_name}.log")
        self.test_case_log_fh.setLevel(root_logger.getEffectiveLevel())
        # to have the custom formatter with a timestamp that used in a test.py but for each testcase's log, we need to
        # extract it from the root logger and apply to the handler
        self.test_case_log_fh.setFormatter(root_logger.handlers[0].formatter)
        root_logger.addHandler(self.test_case_log_fh)
        self.logger.info("Setting up %s", self.current_test_case_full_name)
        if self.cluster.is_dirty:
            self.logger.info(f"Current cluster %s is dirty after test %s, replacing with a new one...",
                             self.cluster.name, self.current_test_case_full_name)
            self.cluster = await self.clusters.replace_dirty(self.cluster, self.logger)
            self.logger.info("Got new Scylla cluster: %s", self.cluster.name)
        self.cluster.setLogger(self.logger)
        self.logger.info("Leasing Scylla cluster %s for test %s", self.cluster, self.current_test_case_full_name)
        self.cluster.before_test(self.current_test_case_full_name)
        self.is_before_test_ok = True
        self.cluster.take_log_savepoint()
        return str(self.cluster)

    async def stop(self) -> None:
        """Stop, cycle last cluster if not dirty and present"""
        self.logger.info("ScyllaManager stopping for test %s", self.test_uname)
        if hasattr(self, "site"):
            await self.site.stop()
            del self.site
        if not self.cluster.is_dirty:
            self.logger.info("Returning Scylla cluster %s for test %s", self.cluster, self.test_uname)
            await self.clusters.put(self.cluster, is_dirty=False)
        else:
            self.logger.info("ScyllaManager: Scylla cluster %s is dirty after %s, stopping it",
                            self.cluster, self.test_uname)
            await self.clusters.put(self.cluster, is_dirty=True)
        del self.cluster
        if os.path.exists(self.manager_dir):
            await async_rmtree(self.manager_dir)
        self.is_running = False

    def _setup_routes(self, app: aiohttp.web.Application) -> None:
        def make_catching_handler(handler: Callable) -> Callable:
            async def catching_handler(request) -> aiohttp.web.Response:
                """Catch all exceptions and return them to the client.
                   Without this, the client would get an 'Internal server error' message
                   without any details. Thanks to this the test log shows the actual error.
                """
                try:
                    ret = await handler(request)
                    if ret is not None:
                        return aiohttp.web.json_response(ret)
                    return aiohttp.web.Response()
                except Exception as e:
                    tb = traceback.format_exc()
                    self.logger.error(f'Exception when executing {handler.__name__}: {e}\n{tb}')
                    return aiohttp.web.Response(status=500, text=str(e))
            return catching_handler

        def route_history_wrapper(blockable = False)-> Callable:
            def outer_wrapper(handler: Callable)-> Callable:
                @wraps(handler)
                async def inner_wrapper(request):
                    if blockable and self.server_broken_event.is_set():
                        raise Exception("ScyllaClusterManager BROKEN")
                    self.logger.info("[ScyllaClusterManager][%s] %s", asyncio.current_task().get_name(), request.url)
                    self.tasks_history[asyncio.current_task()] = request
                    return await handler(request)
                return inner_wrapper
            return outer_wrapper

        def add_get(route: str, handler: Callable):
            app.router.add_get(route, make_catching_handler(route_history_wrapper()(handler)))

        def add_put(route: str, handler: Callable):
            app.router.add_put(route, make_catching_handler(route_history_wrapper(True)(handler)))

        add_get('/up', self._manager_up)
        add_get('/cluster/up', self._cluster_up)
        add_get('/cluster/is-dirty', self._is_dirty)
        add_get('/cluster/replicas', self._cluster_replicas)
        add_get('/cluster/running-servers', self._cluster_running_servers)
        add_get('/cluster/all-servers', self._cluster_all_servers)
        add_get('/cluster/host-ip/{server_id}', self._cluster_server_ip_addr)
        add_get('/cluster/host-id/{server_id}', self._cluster_host_id)
        add_put('/cluster/before-test/{test_case_name}', self._before_test_req)
        add_put('/cluster/after-test/{success}', self._after_test)
        add_put('/cluster/mark-dirty', self._mark_dirty)
        add_put('/cluster/server/{server_id}/stop', self._cluster_server_stop)
        add_put('/cluster/server/{server_id}/stop_gracefully', self._cluster_server_stop_gracefully)
        add_put('/cluster/server/{server_id}/start', self._cluster_server_start)
        add_put('/cluster/server/{server_id}/pause', self._cluster_server_pause)
        add_put('/cluster/server/{server_id}/unpause', self._cluster_server_unpause)
        add_put('/cluster/addserver', self._cluster_server_add)
        add_put('/cluster/addservers', self._cluster_servers_add)
        add_put('/cluster/remove-node/{initiator}', self._cluster_remove_node)
        add_put('/cluster/decommission-node/{server_id}', self._cluster_decommission_node)
        add_put('/cluster/rebuild-node/{server_id}', self._cluster_rebuild_node)
        add_get('/cluster/server/{server_id}/get_config', self._server_get_config)
        add_put('/cluster/server/{server_id}/update_config', self._server_update_config)
        add_put('/cluster/server/{server_id}/update_cmdline', self._server_update_cmdline)
        add_put('/cluster/server/{server_id}/change_ip', self._server_change_ip)
        add_put('/cluster/server/{server_id}/change_rpc_address', self._server_change_rpc_address)
        add_get('/cluster/server/{server_id}/get_log_filename', self._server_get_log_filename)
        add_get('/cluster/server/{server_id}/workdir', self._server_get_workdir)
        add_get('/cluster/server/{server_id}/maintenance_socket_path', self._server_get_maintenance_socket_path)
        add_get('/cluster/server/{server_id}/exe', self._server_get_exe)
        add_put('/cluster/server/{server_id}/wipe_sstables', self._cluster_server_wipe_sstables)
        add_get('/cluster/server/{server_id}/sstables_disk_usage', self._server_get_sstables_disk_usage)

    async def _manager_up(self, _request) -> bool:
        return self.is_running

    async def _cluster_up(self, _request) -> bool:
        """Is cluster running"""
        return self.cluster is not None and self.cluster.is_running

    async def _is_dirty(self, _request) -> bool:
        """Report if current cluster is dirty"""
        assert self.cluster
        return self.cluster.is_dirty

    async def _cluster_replicas(self, _request) -> int:
        """Return cluster's configured number of replicas (replication factor)"""
        assert self.cluster
        return self.cluster.replicas

    async def _cluster_running_servers(self, _request) -> list[tuple[ServerNum, IPAddress, IPAddress]]:
        """Return a dict of running server ids to IPs"""
        return self.cluster.running_servers()

    async def _cluster_all_servers(self, _request) -> list[tuple[ServerNum, IPAddress, IPAddress]]:
        """Return a dict of all server ids to IPs"""
        return self.cluster.all_servers()

    async def _cluster_server_ip_addr(self, request) -> IPAddress:
        """IP address of a server"""
        server_id = ServerNum(int(request.match_info["server_id"]))
        return self.cluster.servers[server_id].ip_addr

    async def _cluster_host_id(self, request) -> HostID:
        """IP address of a server"""
        server_id = ServerNum(int(request.match_info["server_id"]))
        return self.cluster.servers[server_id].host_id

    async def _before_test_req(self, request) -> str:
        cluster_str = await self._before_test(request.match_info['test_case_name'])
        return cluster_str

    async def _after_test(self, _request) -> str:
        assert self.cluster is not None
        assert self.current_test_case_full_name
        self.logger.info(self.repr_tasks_history())
        self.tasks_history.pop(asyncio.current_task())
        # copy current tasks
        tasks = [key for key in self.tasks_history.keys()]
        # wait for all other tasks in ScyllaClusterManager
        try:
            for task in tasks:
                request = self.tasks_history.pop(task)
                if not task.done():
                    self.logger.info("wait for task:%s, request:%s", task, request.path_qs)
                    await asyncio.wait_for(task, timeout=120)
        except asyncio.TimeoutError:
            self.break_manager(f"error on waiting coro {task.get_name()}")

        # check on tasks leakage
        await asyncio.sleep(0.1)
        if self.tasks_history:
            self.break_manager(f"tasks leakage found  {self.tasks_history}")

        success = _request.match_info["success"] == "True"
        self.logger.info("Test %s %s, cluster: %s", self.current_test_case_full_name,
                         "SUCCEEDED" if success else "FAILED", self.cluster)
        try:
            self.cluster.after_test(self.current_test_case_full_name, success)
        finally:
            logging.getLogger().removeHandler(self.test_case_log_fh)
            pathlib.Path(self.test_case_log_fh.baseFilename).unlink()
            self.current_test_case_full_name = ''
        self.is_after_test_ok = True
        cluster_str = str(self.cluster)
        return cluster_str

    def break_manager(self, reason):
        # make ScyllaClusterManager not operatable from client side
        self.logger.error(" %s, BREAK ScyllaClusterManager", reason)
        self.server_broken_event.set()
        self._mark_dirty(None)

    async def _mark_dirty(self, _request) -> None:
        """Mark current cluster dirty"""
        assert self.cluster
        self.cluster.is_dirty = True

    async def _server_stop(self, request: aiohttp.web.Request, gracefully: bool) -> None:
        """Stop a server. No-op if already stopped."""
        assert self.cluster
        server_id = ServerNum(int(request.match_info["server_id"]))
        await self.cluster.server_stop(server_id, gracefully)

    async def _cluster_server_stop(self, request) -> None:
        """Stop a specified server"""
        assert self.cluster
        await self._server_stop(request, gracefully = False)

    async def _cluster_server_stop_gracefully(self, request) -> None:
        """Stop a specified server gracefully"""
        assert self.cluster
        await self._server_stop(request, gracefully = True)

    async def _cluster_server_start(self, request) -> None:
        """Start a specified server (must be stopped)"""
        assert self.cluster
        server_id = ServerNum(int(request.match_info["server_id"]))
        data = await request.json()
        expected_error = data["expected_error"]
        seeds = data["seeds"]
        await self.cluster.server_start(server_id, expected_error, seeds)

    async def _cluster_server_pause(self, request) -> None:
        """Pause the specified server."""
        assert self.cluster
        server_id = ServerNum(int(request.match_info["server_id"]))
        self.cluster.server_pause(server_id)

    async def _cluster_server_unpause(self, request) -> None:
        """Pause the specified server."""
        assert self.cluster
        server_id = ServerNum(int(request.match_info["server_id"]))
        self.cluster.server_unpause(server_id)

    async def _cluster_server_add(self, request) -> dict[str, object]:
        """Add a new server"""
        assert self.cluster
        data = await request.json()
        replace_cfg = ReplaceConfig(**data["replace_cfg"]) if "replace_cfg" in data else None
        s_info = await self.cluster.add_server(replace_cfg, data.get('cmdline'), data.get('config'),
                                               data.get('property_file'), data.get('start', True),
                                               data.get('seeds', None), data.get('server_encryption'), data.get('expected_error', None))
        return {"server_id": s_info.server_id, "ip_addr": s_info.ip_addr, "rpc_address": s_info.rpc_address}

    async def _cluster_servers_add(self, request) -> list[dict[str, object]]:
        """Add new servers concurrently"""
        assert self.cluster
        data = await request.json()
        s_infos = await self.cluster.add_servers(data.get('servers_num'), data.get('cmdline'), data.get('config'),
                                                 data.get('property_file'), data.get('start', True),
                                                 data.get('seeds', None), data.get('server_encryption'), data.get('expected_error', None))
        return [
            {"server_id": s_info.server_id, "ip_addr": s_info.ip_addr, "rpc_address": s_info.rpc_address}
            for s_info in s_infos
        ]

    async def _cluster_remove_node(self, request: aiohttp.web.Request) -> None:
        """Run remove node on Scylla REST API for a specified server"""
        assert self.cluster
        data = await request.json()
        initiator_id = ServerNum(int(request.match_info["initiator"]))
        server_id = ServerNum(int(data["server_id"]))
        assert isinstance(data["ignore_dead"], list), "Invalid list of dead IP addresses"
        ignore_dead = [IPAddress(ip_addr) for ip_addr in data["ignore_dead"]]
        assert initiator_id in self.cluster.running, f"Initiator {initiator_id} is not running"
        if server_id in self.cluster.running:
            self.logger.warning("_cluster_remove_node %s is a running node", server_id)
        else:
            assert server_id in self.cluster.stopped, f"_cluster_remove_node: {server_id} unknown"
        to_remove = self.cluster.servers[server_id]
        initiator = self.cluster.servers[initiator_id]
        expected_error = data["expected_error"]
        self.logger.info("_cluster_remove_node %s with initiator %s", to_remove, initiator)

        # initiate remove
        try:
            await self.cluster.api.remove_node(initiator.ip_addr, to_remove.host_id, ignore_dead,
                                               timeout=ScyllaServer.TOPOLOGY_TIMEOUT)
        except (RuntimeError, HTTPError) as exc:
            if expected_error:
                if expected_error not in str(exc):
                    raise RuntimeError(
                        f"removenode failed (initiator: {initiator}, to_remove: {to_remove},"
                        f" ignore_dead: {ignore_dead}) but did not contain expected error (\"{expected_error}\"),"
                        f"check log file at {initiator.log_filename}, error: \"{exc}\"")
            else:
                raise RuntimeError(
                    f"removenode failed (initiator: {initiator}, to_remove: {to_remove},"
                    f" ignore_dead: {ignore_dead}), check log file at {initiator.log_filename},"
                    f" error: \"{exc}\"")
        else:
            if expected_error:
                self.cluster.server_mark_removed(server_id)
                raise RuntimeError(
                    f"removenode succeeded when it should have failed (initiator: {initiator},"
                    f"to_remove: {to_remove}, ignore_dead: {ignore_dead}, expected error: \"{expected_error}\"),"
                    f" check log file at {initiator.log_filename}")

        self.cluster.server_mark_removed(server_id)

    async def _cluster_decommission_node(self, request) -> None:
        """Run decommission node on Scylla REST API for a specified server"""
        assert self.cluster
        data = await request.json()
        server_id = ServerNum(int(request.match_info["server_id"]))
        self.logger.info("_cluster_decommission_node %s", server_id)
        assert server_id in self.cluster.running, "Can't decommission not running node"
        if len(self.cluster.running) == 1:
            self.logger.warning("_cluster_decommission_node %s is only running node left", server_id)
        server = self.cluster.running[server_id]
        expected_error = data["expected_error"]
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

    async def _cluster_rebuild_node(self, request) -> None:
        """Run rebuild node on Scylla REST API for a specified server"""
        assert self.cluster
        data = await request.json()
        server_id = ServerNum(int(request.match_info["server_id"]))
        self.logger.info("_cluster_rebuild_node %s", server_id)
        assert server_id in self.cluster.running, "Can't rebuild not running node"
        server = self.cluster.running[server_id]
        expected_error = data["expected_error"]
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

    async def _server_get_config(self, request: aiohttp.web.Request) -> dict[str, object]:
        """Get conf/scylla.yaml of the given server as a dictionary."""
        assert self.cluster
        return self.cluster.get_config(ServerNum(int(request.match_info["server_id"])))

    async def _server_update_config(self, request: aiohttp.web.Request) -> None:
        """Update conf/scylla.yaml of the given server by setting `value` under `key`.
           If the server is running, reload the config with a SIGHUP.
           Marks the cluster as dirty."""
        assert self.cluster
        data = await request.json()
        self.cluster.update_config(ServerNum(int(request.match_info["server_id"])),
                                   data['key'], data['value'])

    async def _server_update_cmdline(self, request: aiohttp.web.Request) -> None:
        """Update the command-line options of the given server by merging the new options into the existing ones.
           The update only takes effect after restart.
           Marks the cluster as dirty."""
        assert self.cluster
        data = await request.json()
        self.cluster.update_cmdline(ServerNum(int(request.match_info["server_id"])),
                                    data['cmdline_options'])

    async def _server_change_ip(self, request: aiohttp.web.Request) -> dict[str, object]:
        """Pass change_ip command for the given server to the cluster"""
        assert self.cluster
        server_id = ServerNum(int(request.match_info["server_id"]))
        ip_addr = await self.cluster.change_ip(server_id)
        return {"ip_addr": ip_addr}

    async def _server_change_rpc_address(self, request: aiohttp.web.Request) -> dict[str, object]:
        """Pass change_ip command for the given server to the cluster"""
        assert self.cluster
        server_id = ServerNum(int(request.match_info["server_id"]))
        rpc_address = await self.cluster.change_rpc_address(server_id)
        return {"rpc_address": rpc_address}

    async def _server_get_attribute(self, request: aiohttp.web.Request, attribute: str):
        """Generic request handler which gets a particular attribute of a ScyllaServer instance

        To be used to implement concrete handlers, not for direct use.
        """
        assert self.cluster
        server_id = ServerNum(int(request.match_info["server_id"]))
        assert server_id in self.cluster.servers, f"Server {server_id} unknown"
        server = self.cluster.servers[server_id]
        return getattr(server, attribute)

    async def _server_get_log_filename(self, request: aiohttp.web.Request) -> str:
        return str(await self._server_get_attribute(request, "log_filename"))

    async def _server_get_workdir(self, request: aiohttp.web.Request) -> str:
        return str(await self._server_get_attribute(request, "workdir"))

    async def _server_get_maintenance_socket_path(self, request: aiohttp.web.Request) -> str:
        return str(await self._server_get_attribute(request, "maintenance_socket_path"))

    async def _server_get_exe(self, request: aiohttp.web.Request) -> str:
        return str(await self._server_get_attribute(request, "exe"))

    async def _cluster_server_wipe_sstables(self, request: aiohttp.web.Request):
        data = await request.json()
        server_id = ServerNum(int(request.match_info["server_id"]))
        return self.cluster.wipe_sstables(server_id, data["keyspace"], data["table"])

    async def _server_get_sstables_disk_usage(self, request: aiohttp.web.Request) -> int:
        data = request.query
        server_id = ServerNum(int(request.match_info["server_id"]))
        return self.cluster.get_sstables_disk_usage(server_id, data["keyspace"], data["table"])

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
