#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import logging
import os
import pathlib
import re
import shutil
import time
import aiohttp
from typing import Optional, Any
from cassandra.auth import PlainTextAuthProvider        # type: ignore
from cassandra.cluster import Cluster, NoHostAvailable  # type: ignore

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

authenticator: PasswordAuthenticator
strict_allow_filtering: true
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
    # Regular expression to scan the log
    STARTUP_MSG_RE = re.compile(".*Scylla.*initialization completed")

    def __init__(self, exe=None, vardir=None, host_registry=None,
                 cluster_name=None, seed=None, cmdline_options=None) -> None:
        self.exe = pathlib.Path(exe).resolve()
        self.vardir = pathlib.Path(vardir)
        self.host_registry = host_registry
        self.cmdline_options = cmdline_options
        self.cfg = {
            "cluster_name": cluster_name,
            "host": None,
            "seeds": seed,
            "workdir": None,
        }
        self.cmd: Optional[Any] = None

        async def stop_server():
            if self.is_running:
                await self.stop()

        async def uninstall_server():
            await self.uninstall()

        self.stop_artifact = stop_server
        self.uninstall_artifact = uninstall_server

    async def install_and_start(self) -> None:
        await self.install()

        logging.info("starting server at host %s...", self.cfg["host"])

        await self.start()

        if self.cmd:
            logging.info("started server at host %s, pid %d", self.cfg["host"], self.cmd.pid)

    @property
    def is_running(self) -> bool:
        return self.cmd is not None

    @property
    def host(self) -> str:
        return self.cfg["host"]

    def find_scylla_executable(self) -> None:
        if not os.access(self.exe, os.X_OK):
            raise RuntimeError("{} is not executable", self.exe)

    async def install(self) -> None:
        """Create a working directory with all subdirectories, initialize
        a configuration file."""

        self.find_scylla_executable()

        # Scylla assumes all instances of a cluster use the same port,
        # so each instance needs an own IP address.
        self.cfg["host"] = await self.host_registry.lease_host()
        self.cfg["seeds"] = self.cfg.get("seeds") or self.cfg["host"]
        # Use the last part in host IP 127.151.3.27 -> 27
        self.shortname = pathlib.Path("scylla-" + self.cfg["host"].split(".")[-1])
        self.cfg["workdir"] = self.vardir / self.shortname

        logging.info("installing Scylla server in %s...", self.cfg["workdir"])

        self.log_file_name = self.vardir / self.shortname.with_suffix(".log")

        self.config_file_name = self.cfg["workdir"] / "conf/scylla.yaml"

        # Use tmpdir (likely tmpfs) to speed up scylla start up, but create a
        # link to it to keep everything in one place under testlog/
        self.tmpdir = pathlib.Path(os.getenv('TMPDIR', '/tmp'))
        self.tmpdir = self.tmpdir / ('scylla-'+self.cfg["host"])

        # Cleanup any remains of the previously running server in this path
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        pathlib.Path(self.cfg["workdir"]).unlink(missing_ok=True)

        pathlib.Path(self.tmpdir).mkdir(parents=True, exist_ok=True)
        pathlib.Path(self.cfg["workdir"]).symlink_to(self.tmpdir)
        pathlib.Path(self.config_file_name).parent.mkdir(parents=True, exist_ok=True)
        # Create a configuration file.
        with open(self.config_file_name, 'w') as config_file:
            config_file.write(SCYLLA_CONF_TEMPLATE.format(**self.cfg))

        self.log_file = open(self.log_file_name, "wb")

    def find_log_file_pattern(self, fil, pattern_re) -> bool:
        for line in fil.readlines():
            if pattern_re.match(line):
                return True
        return False

    async def cql_is_up(self) -> bool:
        """Test that CQL is serving, for wait_for_services() below."""
        caslog = logging.getLogger('cassandra')
        oldlevel = caslog.getEffectiveLevel()
        # Be quiet about connection failures.
        caslog.setLevel('CRITICAL')
        auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
        try:
            with Cluster(contact_points=[self.cfg["host"]], auth_provider=auth) as cluster:
                with cluster.connect():
                    return True
        except NoHostAvailable:
            return False
        finally:
            caslog.setLevel(oldlevel)
        # Any other exception may indicate a problem, and is passed to the caller.

    async def rest_api_is_up(self) -> bool:
        """Test that the Scylla REST API is serving. Can be used as a
        checker function with wait_for_services() below."""
        try:
            async with aiohttp.ClientSession() as s:
                url = "http://{}:10000/".format(self.cfg["host"])
                async with s.get(url):
                    return True
        except aiohttp.ClientConnectionError:
            return False
        # Any other exception may indicate a problem, and is passed to the caller.

    async def start(self) -> None:
        """Start an installed server. May be used for restarts."""
        START_TIMEOUT = 300     # seconds

        # Add suite-specific command line options
        scylla_args = SCYLLA_CMDLINE_OPTIONS + self.cmdline_options
        env = os.environ.copy()
        env.clear()     # pass empty env to make user user's SCYLLA_HOME has no impact
        self.cmd = await asyncio.create_subprocess_exec(
            self.exe,
            *scylla_args,
            cwd=self.cfg["workdir"],
            stderr=self.log_file,
            stdout=self.log_file,
            env=env,
            preexec_fn=os.setsid,
        )

        self.start_time = time.time()

        while time.time() < self.start_time + START_TIMEOUT:
            if self.cmd.returncode:
                with open(self.log_file_name, 'r') as log_file:
                    logging.error("failed to start server at host %s", self.cfg["host"])
                    logging.error("last line of {}:".format(self.log_file_name))
                    log_file.seek(0, 0)
                    logging.error(log_file.readlines()[-1].rstrip())
                    h = logging.getLogger().handlers[0]
                    logpath = h.baseFilename if hasattr(h, 'baseFilename') else "?"  # type: ignore
                    raise RuntimeError("""Failed to start server at host {}.
Check the log files:
{}
{}""".format(self.cfg["host"], logpath, self.log_file_name))

            if await self.rest_api_is_up():
                if await self.cql_is_up():
                    return

            # Sleep 10 milliseconds and retry
            await asyncio.sleep(0.1)
            if self.cfg["seeds"] != self.cfg["host"]:
                await self.force_schema_migration()

        raise RuntimeError("failed to start server {}, check server log at {}".format(
            self.host, self.log_file_name))

    async def force_schema_migration(self) -> None:
        auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
        with Cluster(contact_points=[self.cfg["seeds"]], auth_provider=auth) as cluster:
            with cluster.connect() as session:
                session.execute("CREATE KEYSPACE k WITH REPLICATION = {" +
                                "'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
                session.execute("DROP KEYSPACE k")

    async def stop(self) -> None:
        """Stop a running server. No-op if not running. Uses SIGKILL to
        stop, so is not graceful. Waits for the process to exit before return."""
        # Preserve for logging
        host = self.cfg["host"]
        logging.info("stopping server at host %s", host)
        if not self.cmd:
            return

        try:
            self.cmd.kill()
        except ProcessLookupError:
            pass
        else:
            await self.cmd.wait()
        finally:
            if self.cmd:
                logging.info("stopped server at host %s", host)
            self.cmd = None

    async def uninstall(self) -> None:
        """Clear all files left from a stopped server, including the
        data files and log files."""

        if not self.cfg["host"]:
            return
        logging.info("Uninstalling server at %s", self.cfg["workdir"])

        shutil.rmtree(self.tmpdir)
        pathlib.Path(self.cfg["workdir"]).unlink(missing_ok=True)
        pathlib.Path(self.log_file_name).unlink(missing_ok=True)

        await self.host_registry.release_host(self.cfg["host"])
        self.cfg["host"] = None
