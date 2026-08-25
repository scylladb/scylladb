#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Scylla server for testing.
   Provides helpers to setup and manage a single Scylla server for testing.
"""
import asyncio
import copy
import errno
import glob
import itertools
import json
import logging
import os
import pathlib
import platform
import re
import shutil
import signal
import socket
import tempfile
import threading
import time
import uuid
from asyncio.subprocess import Process
from io import BufferedWriter
from typing import Any, Dict, List, NamedTuple, NoReturn, Optional, Union

import aiohttp
import yaml
from cassandra import InvalidRequest                    # type: ignore
from cassandra import OperationTimedOut                 # type: ignore
from cassandra.auth import PlainTextAuthProvider, AuthProvider # type: ignore
from cassandra.cluster import Cluster           # type: ignore # pylint: disable=no-name-in-module
from cassandra.cluster import NoHostAvailable   # type: ignore # pylint: disable=no-name-in-module
from cassandra.cluster import Session           # pylint: disable=no-name-in-module
from cassandra.cluster import ExecutionProfile  # pylint: disable=no-name-in-module
from cassandra.cluster import EXEC_PROFILE_DEFAULT  # pylint: disable=no-name-in-module
from cassandra.connection import UnixSocketEndPoint
from cassandra.policies import ExponentialReconnectionPolicy  # type: ignore
from cassandra.policies import WhiteListRoundRobinPolicy  # type: ignore

from test import TOP_SRC_DIR, TEST_DIR
from test.pylib.driver_utils import safe_driver_shutdown, safe_shutting_down
from test.pylib.internal_types import ServerNum, IPAddress, HostID, ServerInfo, ServerUpState
from test.pylib.rest_client import ScyllaRESTAPIClient, HTTPError
from test.pylib.util import async_rmtree, read_last_line, get_xdist_worker_id, scale_timeout_by_mode
from test.pylib.version_fetch_utils import fetch_and_install_scylla_version


def make_scylla_conf(mode: str, host_addr: str, seed_addrs: List[str], cluster_name: str,
                     server_encryption: str) -> dict[str, object]:
    # We significantly increase default timeouts to allow running tests on a very slow
    # setup (but without network losses). These timeouts can impact the running time of
    # topology tests. For example, the barrier_and_drain topology command waits until
    # background writes' handlers time out. We don't want to slow down tests for no
    # reason, so we increase the timeouts according to each mode's needs. The client
    # should avoid timing out its requests before the server times out - for this reason
    # we increase the CQL driver's client-side timeout in conftest.py.
    request_timeout_in_ms = scale_timeout_by_mode(mode, 30000)

    return {
        'cluster_name': cluster_name,
        'listen_address': host_addr,
        'rpc_address': host_addr,
        'api_address': host_addr,
        'prometheus_address': host_addr,
        'alternator_address': host_addr,
        'seed_provider': [{
            'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
            'parameters': [{
                'seeds': ','.join(seed_addrs)
                }]
            }],

        'developer_mode': True,

        # Allow testing experimental features. Following issue #9467, we need
        # to add here specific experimental features as they are introduced.
        'enable_user_defined_functions': True,
        'experimental_features': ['udf',
                                  'views-with-tablets'],

        'skip_wait_for_gossip_to_settle': 0,
        'shutdown_announce_in_ms': 0,
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
        'request_timeout_on_shutdown_in_seconds': int(request_timeout_in_ms/1000),
        'group0_raft_op_timeout_in_ms': 300000,
        'user_defined_function_time_limit_ms': 1000,

        'strict_allow_filtering': True,
        'strict_is_not_null_in_views': True,

        'permissions_update_interval_in_ms': 100,
        'permissions_validity_in_ms': 100,

        'reader_concurrency_semaphore_serialize_limit_multiplier': 0,
        'reader_concurrency_semaphore_kill_limit_multiplier': 0,
        'view_update_reader_concurrency_semaphore_serialize_limit_multiplier': 0,
        'view_update_reader_concurrency_semaphore_kill_limit_multiplier': 0,

        'authenticator': 'PasswordAuthenticator',
        'authorizer': 'CassandraAuthorizer',
        'tablets_initial_scale_factor': 4 if mode == 'release' else 2,

        'auth_superuser_name': 'cassandra',
        # password is 'cassandra'
        'auth_superuser_salted_password': '$6$x7IFjiX5VCpvNiFk$2IfjTvSyGL7zerpV.wbY7mJjaRCrJ/68dtT3UpT.sSmNYz1bPjtn3mH.kJKFvaZ2T4SbVeBijjmwGjcb83LlV/',

        'service_levels_interval_ms': 500,

        'server_encryption_options': {
            'internode_encryption': server_encryption,
            'certificate': 'conf/scylla.crt',
            'keyfile': 'conf/scylla.key',
            'truststore': 'conf/scyllacadb.pem',
        },

        'rf_rack_valid_keyspaces': True,

        'alternator_allow_system_table_write': True,
        'alternator_ttl_period_in_seconds': 0.5,
        'sstable_format': 'mt',
    }

# Seastar options can not be passed through scylla.yaml, use command line
# for them. Keep everything else in the configuration file to make
# it easier to restart. Sic: if you make a typo on the command line,
# Scylla refuses to boot.
#
# This variable is for options which are supported by *all*
# Scylla versions which participate in the tests.
# Other options should be instead put in ScyllaVersionDescription.
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

# A path to a Scylla executable, with version-specific
# (i.e. not supported by *all* relevant versions)
# Scylla config attached.
class ScyllaVersionDescription(NamedTuple):
    path: str # path to the Scylla executable
    config: dict # a dictionary of added scylla.yaml options
    argv: list[str] # a list of added CLI args

# Returns the description of the current version.
# (I.e. the one we just built and are now testing).
#
# (The path has to be passed by an argument because the current executable
# has no fixed location -- the build directory depends on the build mode,
# and in the case of cmake can be moved to something different than `build`.)
def get_current_version_description(path: str) -> ScyllaVersionDescription:
    return ScyllaVersionDescription(
        path=path,
        config={},
        argv=[
            '--logger-log-level', 'group0_voter_handler=debug',
        ]
    )


async def get_scylla_2025_1_executable(build_mode: str) -> str:
    is_debug = build_mode == 'debug' or build_mode == 'sanitize'
    package = "debug" if is_debug else ""
    arch = platform.machine()
    return fetch_and_install_scylla_version(2025, 1, arch=arch, pack=package)


async def get_scylla_2025_1_description(build_mode: str) -> ScyllaVersionDescription:
    # Note: 2025.1 is the oldest version which participates in upgrade tests in test.py,
    # so the added version-specific config is naturally empty.
    #
    # SCYLLA_CMDLINE_OPTIONS is the 2025.1 baseline, and newer versions add their
    # own version-specific config.
    return ScyllaVersionDescription(
        path=str(await get_scylla_2025_1_executable(build_mode)),
        config={},
        argv=[],
    )

# [--smp, 1], [--smp, 2] -> [--smp, 2]
# [--smp, 1], [--smp] -> [--smp]
# [--smp, 1], [--smp, __missing__] -> [--smp]
# [--smp, 1], [--smp, __remove__] -> []
# [--smp=1], [--smp=2] -> [--smp, 2]
# [--smp=1], [--smp=__remove__] -> []
# [--overprovisioned, --smp=1, --abort-on-ebadf], [--smp=2] -> [--overprovisioned, --smp=2, --abort-on-ebadf]
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


# The driver's default reconnection policy has an unbounded max_delay (600s) and can take
# up to ~64 attempts to give up, i.e. hours, to notice a control connection is unreachable.
# Any Cluster created by this module must bound both individual connection attempts and
# the overall retry budget.
_DRIVER_RECONNECTION_POLICY = ExponentialReconnectionPolicy(base_delay=1.0, max_delay=10.0, max_attempts=10)


class ScyllaServer:
    """Starts and handles a single Scylla server, managing logs, checking if responsive,
       and cleanup when finished."""
    # pylint: disable=too-many-instance-attributes

    # in seconds, used for topology operations such as bootstrap or decommission
    TOPOLOGY_TIMEOUT = 1000
    start_time: float
    sleep_interval: float
    log_file: BufferedWriter | None
    _host_id: HostID                             # Host id (UUID)
    newid = itertools.count(start=1).__next__   # Sequential unique id

    def __init__(self,
                 logger: Union[logging.Logger, logging.LoggerAdapter],
                 vardir: pathlib.Path,
                 version: ScyllaVersionDescription,
                 cmdline_options: List[str],
                 config_options: Dict[str, Any],
                 property_file: Dict[str, Any],
                 append_env: Dict[str, str]) -> None:
        self.server_id = ServerNum(ScyllaServer.newid())
        xdist_worker_id = get_xdist_worker_id()
        # this variable needed to make a cleanup after server is not needed anymore
        self.maintenance_socket_dir = tempfile.TemporaryDirectory(
            prefix=f"scylladb-{f'{xdist_worker_id}-' if xdist_worker_id else ''}{self.server_id}-test.py-",
            ignore_cleanup_errors=True,
        )
        self.maintenance_socket_path = f"{self.maintenance_socket_dir.name}/cql.m"
        # Unix socket for receiving sd_notify messages from Scylla
        self.notify_socket_path = pathlib.Path(self.maintenance_socket_dir.name) / "notify.sock"
        self.notify_socket: Optional[socket.socket] = None
        self._received_serving = False
        self.exe = pathlib.Path(version.path).resolve()
        self.logger = logger
        self.log_file = None
        self.cmdline_options = cmdline_options
        self.auth_provider: Optional[AuthProvider] = None
        self.cmd: Optional[Process] = None
        self.start_stop_lock = asyncio.Lock()
        self.stop_event = asyncio.Event()
        self.log_savepoint = 0
        self.control_cluster: Optional[Cluster] = None
        self.control_connection: Optional[Session] = None
        self.serving_signal = None
        shortname = f"scylla-{f'{xdist_worker_id}-' if xdist_worker_id else ''}{self.server_id}"

        workdir = pathlib.Path(vardir) / shortname
        for opt in ("--workdir", "-W"):
            try:
                id = self.cmdline_options.index(opt)
                workdir = pathlib.Path(self.cmdline_options[id+1])
                break
            except ValueError:
                pass

        self.workdir = workdir
        self.log_filename = self.workdir.with_suffix(".log")
        self.config_filename = self.workdir / "conf/scylla.yaml"
        self.property_filename = self.workdir / "conf/cassandra-rackdc.properties"
        self.certificate_filename = self.workdir / "conf/scylla.crt"
        self.keyfile_filename = self.workdir / "conf/scylla.key"
        self.truststore_filename = self.workdir / "conf/scyllacadb.pem"
        self.resourcesdir = TEST_DIR / "pylib/resources"
        self.resources_certificate_file = self.resourcesdir / "scylla.crt"
        self.resources_keyfile_file = self.resourcesdir / "scylla.key"

        # The basic server configuration (the workdir and the maintenance
        # socket are the server's own) topped by the caller-assembled options.
        # api_doc_dir defaults to a path relative to the current directory,
        # which is the server's workdir here, so point it at the source tree -
        # the same way install.sh points it at the installed copy. Without it
        # every /api-doc record the server advertises is unusable.
        self.config = {
            'workdir': str(self.workdir.resolve()),
            'maintenance_socket': self.maintenance_socket_path,
            'api_doc_dir': f"{TOP_SRC_DIR / 'api/api-doc'}/",
        } | config_options
        self.property_file = property_file
        self.append_env = append_env

    @property
    def ip_addr(self) -> IPAddress:
        return IPAddress(self.config["listen_address"])

    def change_ip(self, ip_addr: IPAddress) -> None:
        """Change IP address of the current server. Pre: the server is
        stopped"""
        if self.is_running:
            raise RuntimeError(f"Can't change IP of a running server {self.ip_addr}.")
        self.config["listen_address"] = ip_addr
        self.config["rpc_address"] = ip_addr
        self.config["api_address"] = ip_addr
        self.config["prometheus_address"] = ip_addr
        self.config["alternator_address"] = ip_addr
        self._write_config_file()

    @property
    def seeds(self) -> List[str]:
        return [s.strip() for s in self.config["seed_provider"][0]["parameters"][0]["seeds"].split(",")]

    def change_seeds(self, seeds: List[str]):
        """Change seeds of the current server. Pre: the server is stopped"""
        if self.is_running:
            raise RuntimeError(f"Can't change seeds of a running server {self.ip_addr}.")
        self.config['seed_provider'][0]['parameters'][0]['seeds'] = ','.join(seeds)
        self._write_config_file()

    @property
    def rpc_address(self) -> IPAddress:
        return self.config["rpc_address"]

    @property
    def datacenter(self) -> str:
        if self.property_file and "dc" in self.property_file:
            return self.property_file["dc"]
        return "DEFAULT_DC"

    @property
    def rack(self) -> str:
        if self.property_file and "rack" in self.property_file:
            return self.property_file["rack"]
        return "DEFAULT_RACK"
    
    def server_info(self) -> ServerInfo:
        pid = self.cmd.pid if self.cmd else None
        return ServerInfo(self.server_id, self.ip_addr, self.rpc_address, self.datacenter, self.rack, pid)

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

    def get_sstables(self, keyspace: str, table: str):
        root_dir = self.workdir/"data"
        sstables = []
        for f in glob.iglob(f"./{keyspace}/{table}-????????????????????????????????/**/*", root_dir=root_dir, recursive=True):
            if ((root_dir/f).is_file()):
                file_path = root_dir / f
                sstables.append(file_path.name)
        return sstables

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

    async def install_and_start(self,
                                api: ScyllaRESTAPIClient,
                                expected_error: Optional[str] = None,
                                expected_server_up_state: ServerUpState = ServerUpState.SERVING) -> None:
        """Setup and start this server."""

        await self.install()

        self.logger.info("starting server at host %s in %s...", self.ip_addr, self.workdir.name)

        try:
            await self.start(api, expected_error, expected_server_up_state)
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
        """Return the contents of conf/scylla.yaml as a dict.

        Returns a copy: mutating it doesn't affect the server; use
        update_config() for that.
        """
        return copy.deepcopy(self.config)

    def update_config(self, config_options: dict[str, Any]) -> None:
        """Update conf/scylla.yaml with `config_options` dict.

        If we're running, reload the config with a SIGHUP.
        """
        self.config.update(config_options)
        self._write_config_file()
        if self.cmd:
            self.cmd.send_signal(signal.SIGHUP)

    def remove_config_option(self, key: str) -> None:
        """Remove an option from conf/scylla.yaml.

        If we're running, reload the config with a SIGHUP.
        """
        self.config.pop(key, None)  # don't fail if there is no such option in the config
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

    def _alternator_ports(self) -> list[tuple[str, int]]:
        """Return (scheme, port) for every configured Alternator port."""
        ports = []
        if "alternator_port" in self.config:
            ports.append(("http", self.config["alternator_port"]))
        if "alternator_https_port" in self.config:
            ports.append(("https", self.config["alternator_https_port"]))
        return ports

    async def check_alternator_connected(self, ports: list[tuple[str, int]]) -> bool:
        """TCP connect to every configured Alternator port.

        Returns True if all ports accept connections.
        """
        for _, port in ports:
            try:
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(self.ip_addr, port), timeout=2)
                writer.close()
                await writer.wait_closed()
            except (OSError, asyncio.TimeoutError):
                return False
        return True

    async def check_alternator_queried(self, ports: list[tuple[str, int]]) -> bool:
        """Sends a GetItem for a randomly-named nonexistent table and validates
        that the response is a DynamoDB-shaped JSON error (contains __type),
        confirming Alternator is processing DynamoDB API requests.

        Returns True if all ports respond correctly.
        """
        table_name = f"nonexistent_table_{uuid.uuid4().hex}"
        headers = {
            "Content-Type": "application/x-amz-json-1.0",
            "X-Amz-Target": "DynamoDB_20120810.GetItem",
        }
        body = json.dumps({"TableName": table_name, "Key": {"k": {"S": "k"}}})
        timeout = aiohttp.ClientTimeout(total=2)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for scheme, port in ports:
                url = f"{scheme}://{self.ip_addr}:{port}/"
                try:
                    # ssl=False skips certificate verification
                    async with session.post(url, headers=headers, data=body, ssl=False) as resp:
                        response_body = await resp.json(content_type=None)
                        if "__type" not in response_body:
                            return False
                except Exception as exc:
                    self.logger.debug("Alternator query check failed for %s: %s", url, exc)
                    return False
        return True

    async def get_cql_up_state(self) -> tuple[bool, bool]:
        """Check CQL connectivity.

        Returns (connected, queried) indicating whether a CQL connection
        was established and whether a query executed successfully.
        """
        caslog = logging.getLogger('cassandra')
        oldlevel = caslog.getEffectiveLevel()
        # Be quiet about connection failures.
        caslog.setLevel('CRITICAL')
        if self.auth_provider is None:
            self.auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
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
        cql_queried = False
        cluster_kwargs = dict(
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            contact_points=contact_points,
            protocol_version=4,  # This is the latest version Scylla supports
            control_connection_timeout=self.TOPOLOGY_TIMEOUT,
            auth_provider=self.auth_provider,
            reconnection_policy=_DRIVER_RECONNECTION_POLICY,
        )
        try:
            # In a cluster setup, it's possible that the CQL
            # here is directed to a node different from the initial contact
            # point, so make sure we execute the checks strictly via
            # this connection
            with safe_shutting_down(Cluster(**cluster_kwargs)) as cluster:
                with cluster.connect() as session:
                    connected = True
                    # See the comment above about `auth::standard_role_manager`. We execute
                    # a 'real' query to ensure that the auth service has finished initializing.
                    session.execute("SELECT key FROM system.local where key = 'local'")
                    # Only create the persistent control connection once; re-creating it on
                    # every successful CQL check would leak driver connections.
                    if self.control_connection is None:
                        # Connect before publishing the cluster, so a failed connect()
                        # doesn't leave a leaked Cluster behind for the next check to
                        # silently overwrite.
                        control_cluster = Cluster(**cluster_kwargs)
                        try:
                            control_connection = control_cluster.connect()
                        except BaseException:
                            safe_driver_shutdown(control_cluster)
                            raise
                        self.control_cluster = control_cluster
                        self.control_connection = control_connection
                    cql_queried = True
        except (NoHostAvailable, InvalidRequest, OperationTimedOut) as exc:
            self.logger.debug("Exception when checking if CQL is up: %s", exc)
        finally:
            caslog.setLevel(oldlevel)
        # Any other exception may indicate a problem, and is passed to the caller.
        return connected, cql_queried

    async def get_alternator_up_state(self, ports: list[tuple[str, int]]) -> tuple[bool, bool]:
        connected = await self.check_alternator_connected(ports)
        queried = connected and await self.check_alternator_queried(ports)
        return connected, queried

    async def get_cql_alternator_up_state(self) -> ServerUpState | None:
        """Get the combined CQL + Alternator up state."""
        cql_connected, cql_queried = await self.get_cql_up_state()
        alt_connected, alt_queried = False, False
        alt_ports = self._alternator_ports()  # `alt_ports` empty = no Alternator
        if alt_ports:
            alt_connected, alt_queried = await self.get_alternator_up_state(alt_ports)
        if cql_queried and (alt_queried or not alt_ports):
            return ServerUpState.CQL_ALTERNATOR_QUERIED
        if not cql_connected or (alt_ports and not alt_connected):
            return None
        # Here both CQL and Alternator (if exists) are at least connected
        return ServerUpState.CQL_ALTERNATOR_CONNECTED

    def _setup_notify_socket(self) -> None:
        """Create a Unix datagram socket for receiving sd_notify messages from Scylla."""
        if self.notify_socket is not None:
            return
        # Remove existing socket file if present
        self.notify_socket_path.unlink(missing_ok=True)
        self.notify_socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM | socket.SOCK_CLOEXEC)
        self.notify_socket.bind(str(self.notify_socket_path))
        self._received_serving = False
        loop = asyncio.get_running_loop()

        def poll_status(s: socket.socket, f: asyncio.Future, logger: Union[logging.Logger, logging.LoggerAdapter]):
            # Try to read all available messages from the socket
            while True:
                try:
                    data = s.recv(4096)
                    # sd_notify message format: "STATUS=serving\n" or "READY=1\nSTATUS=serving\n"
                    message = data.decode('utf-8', errors='replace')
                    if 'STATUS=serving' in message:
                        logger.debug("Received sd_notify 'serving' message")
                        loop.call_soon_threadsafe(lambda: f.done() or f.set_result(True))
                        return
                    if 'STATUS=entering maintenance mode' in message:
                        logger.debug("Received sd_notify 'entering maintenance mode' message")
                        loop.call_soon_threadsafe(lambda: f.done() or f.set_result(True))
                        return
                except socket.timeout:
                    pass
                except Exception as e:
                    logger.debug("Error reading from notify socket: %s", e)
                    break
            loop.call_soon_threadsafe(lambda: f.done() or f.set_result(False))

        self.serving_signal = loop.create_future()
        t = threading.Thread(target=poll_status, args=[self.notify_socket, self.serving_signal, self.logger], daemon=True)
        t.start()

    def _cleanup_notify_socket(self) -> None:
        """Clean up the sd_notify socket."""
        if self.notify_socket is not None:
            self.notify_socket.close()
            self.notify_socket = None
        if self.serving_signal is not None:
            self.serving_signal.cancel()
            self.serving_signal = None
        self.notify_socket_path.unlink(missing_ok=True)

    def check_serving_notification(self) -> bool:
        """Check if Scylla has sent the 'serving' sd_notify message.

        Returns True if the SERVING state has been reached.
        """
        if self._received_serving:
            return True
        if self.notify_socket is None:
            return False
        if self.serving_signal is None:
            return False
        if self.serving_signal.done():
            self._received_serving = self.serving_signal.result()
            self.serving_signal = None
        return self._received_serving

    async def try_get_host_id(self, api: ScyllaRESTAPIClient) -> Optional[HostID]:
        """Try to get the host id (also tests Scylla REST API is serving)"""

        if hasattr(self, "_host_id"):
            return self._host_id
        try:
            self._host_id = await api.get_host_id(self.ip_addr)
            return self._host_id
        except (aiohttp.ClientConnectionError, HTTPError) as exc:
            if isinstance(exc, HTTPError) and exc.code >= 500:
                raise exc
            # Any other exception may indicate a problem, and is passed to the caller.
            return None

    async def get_host_id(self, api: ScyllaRESTAPIClient) -> HostID:
        result = await self.try_get_host_id(api)
        if result is None:
            raise RuntimeError(f"Failed to get host_id for {self}")
        return result

    @start_stop_lock
    async def start(self,
                    api: ScyllaRESTAPIClient,
                    expected_error: Optional[str] = None,
                    expected_server_up_state: ServerUpState = ServerUpState.SERVING,
                    cmdline_options_override: list[str] | None = None,
                    append_env_override: dict[str, str] | None = None) -> None:
        """Start an installed server.

        Use `cmdline_options_override` and `append_env_override` instead of `self.cmdline_options` and
        `self.append_env` correspondingly if provided.

        May be used for restarts.
        """

        env = os.environ.copy()
        # remove from env to make sure user's SCYLLA_HOME has no impact
        env.pop('SCYLLA_HOME', None)
        env.update(self.append_env if append_env_override is None else append_env_override)
        env['UBSAN_OPTIONS'] = f'halt_on_error=1:abort_on_error=1:suppressions={TOP_SRC_DIR / "ubsan-suppressions.supp"}'
        env['ASAN_OPTIONS'] = f'disable_coredump=0:abort_on_error=1:detect_stack_use_after_return=1'

        # Set up socket for receiving sd_notify messages from Scylla
        self._setup_notify_socket()
        env['NOTIFY_SOCKET'] = self.notify_socket_path

        # Reopen log file if it was closed (e.g., after a previous stop)
        if self.log_file is None or self.log_file.closed:
            self.log_file = self.log_filename.open("ab")  # append mode to preserve previous logs

        self.cmd = await asyncio.create_subprocess_exec(
            self.exe,
            *(self.cmdline_options if cmdline_options_override is None else cmdline_options_override),
            cwd=self.workdir,
            stderr=self.log_file,
            stdout=self.log_file,
            env=env,
        )

        if expected_server_up_state == ServerUpState.PROCESS_STARTED:
            return

        server_up_state = ServerUpState.PROCESS_STARTED

        self.start_time = time.time()
        sleep_interval = 0.1

        async def report_error(message: str) -> NoReturn:
            message += f", server_id {self.server_id}, IP {self.ip_addr}, workdir {self.workdir.name}"
            message += f", host_id {await self.try_get_host_id(api) or '<missing>'}"
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

        async def is_expected_state_reached() -> bool:
            if server_up_state >= expected_server_up_state:
                if expected_error is not None:
                    await report_error(
                        f"the node has reached {server_up_state} state,"
                        f" but was expected to fail with the expected error"
                    )
                return True
            return False

        while time.time() < self.start_time + self.TOPOLOGY_TIMEOUT and not self.stop_event.is_set():
            assert self.cmd is not None
            if self.cmd.returncode is not None:
                self.cmd = None
                if expected_error is not None:
                    with self.log_filename.open("r", encoding="utf-8") as log_file:
                        for line in log_file:
                            if re.search(expected_error, line):
                                return
                        await report_error("the node startup failed, but the log file doesn't contain the expected error")
                await report_error("failed to start the node")
            if await self.try_get_host_id(api):
                if server_up_state == ServerUpState.PROCESS_STARTED:
                    server_up_state = ServerUpState.HOST_ID_QUERIED
                if await is_expected_state_reached():
                    return
                # Only poll CQL/Alternator until they are known to be up.
                # Once CQL_ALTERNATOR_QUERIED is reached, skip the poll to avoid
                # repeatedly recreating driver connections while waiting for sd_notify.
                if server_up_state < ServerUpState.CQL_ALTERNATOR_QUERIED:
                    server_up_state = await self.get_cql_alternator_up_state() or server_up_state
                if await is_expected_state_reached():
                    return
                # Check for SERVING state via sd_notify. This is authoritative: Scylla sends
                # STATUS=serving once all configured listeners are ready, and
                # STATUS=entering maintenance mode once the maintenance socket is ready.
                # Both mean the server is fully started and we don't need to wait further.
                if server_up_state >= ServerUpState.CQL_ALTERNATOR_QUERIED and self.check_serving_notification():
                    server_up_state = ServerUpState.SERVING
                if await is_expected_state_reached():
                    return

            # Sleep and retry
            await asyncio.sleep(sleep_interval)

        if self.stop_event.is_set():
            await report_error('failed to start the node as it was requested to be stopped in the meantime')
        else:
            await report_error(
                f"the node failed to reach the expected state ({expected_server_up_state}) within the timeout,"
                f" last seen state {server_up_state}"
            )

    async def force_schema_migration(self) -> None:
        """This is a hack to change schema hash on an existing cluster node
        which triggers a gossip round and propagation of entire application
        state. Helps quickly propagate tokens and speed up node boot if the
        previous state propagation was missed."""
        auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
        profile = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy(self.seeds),
                                   request_timeout=self.TOPOLOGY_TIMEOUT)
        with safe_shutting_down(Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                                        contact_points=self.seeds,
                                        auth_provider=auth,
                                        # This is the latest version Scylla supports
                                        protocol_version=4,
                                        control_connection_timeout=self.TOPOLOGY_TIMEOUT,
                                        reconnection_policy=_DRIVER_RECONNECTION_POLICY,
                                        )) as cluster:
            with cluster.connect() as session:
                session.execute("CREATE KEYSPACE IF NOT EXISTS k WITH REPLICATION = {" +
                                "'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }")
                session.execute("DROP KEYSPACE k")

    def shutdown_control_connection(self) -> None:
        """Shut down driver connection and notify socket"""
        if self.control_connection is not None:
            self.control_connection.shutdown()
            self.control_connection = None
        if self.control_cluster is not None:
            safe_driver_shutdown(self.control_cluster)
            self.control_cluster = None
        self._cleanup_notify_socket()

    async def stop(self) -> None:
        """Stop a running server. No-op if not running. Uses SIGKILL to
        stop, so is not graceful. Waits for the process to exit before return.

        This method intentionally does not acquire start_stop_lock so that it
        can kill a server even while stop_gracefully() is blocked waiting for
        the process to exit (e.g. the node is deadlocked). The concurrent
        stop_gracefully() will unblock once the process dies from SIGKILL.
        A local copy of self.cmd is used because there are await points after
        which another coroutine (stop_gracefully) may set self.cmd to None."""
        self.logger.info("stopping %s in %s", self, self.workdir.name)
        cmd = self.cmd
        if not cmd:
            self.shutdown_control_connection()
            return

        # Dump the profile if exists and supported by the API.
        try:
            api = ScyllaRESTAPIClient()
            await api.dump_llvm_profile(self.ip_addr)
        except:
            # since it is not part of the test functionality, allow
            # this step to fail unconditionally.
            pass
        self.shutdown_control_connection()

        if cmd.returncode is not None:
            # process has already exited
            if cmd.returncode != 0:
                self.logger.error("%s exited with non-zero status code: %d", self, cmd.returncode)
            self.logger.info("stopped %s in %s", self, self.workdir.name)
            self.cmd = None
            return

        try:
            cmd.kill()
        except ProcessLookupError:
            # the process *might* exit after checking for cmd.returncode
            # and before cmd.kill() call. this is unlikely, but should not
            # be considered as a failure.
            pass
        else:
            await cmd.wait()
        finally:
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

        self.shutdown_control_connection()
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
            await async_rmtree(self.workdir, ignore_errors=True)
        except FileNotFoundError:
            pass
        self.log_filename.unlink(missing_ok=True)
        self.log_file = None

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
        host_id = getattr(self, '_host_id', 'undefined id')
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
