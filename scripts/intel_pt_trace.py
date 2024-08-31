# Copyright 2024-present ScyllaDB
# 
# SPDX-License-Identifier: AGPL-3.0-or-later

# Usage:
# python scripts/intel_pt_trace.py

# Runs a Scylla cluster, runs some operation (defined in `run()`) on it under Intel PT recording,
# and outputs out.ftf (Fuchsia trace format) trace to the current working directory.

# Note: needs root rights. Otherwise perf isn't able to record some things,
# and isn't able to decode some things.
# Will call `sudo -v` at the start. Since the script is short, and in most setups sudo has some period
# where you don't have to type the password again, this should be enough to authorize all remaining sudo calls.
# (If it doesn't, you have to tweak the script.)

# Config:
#
# The "time" axis in the trace can show:
# i: instructions retired
# t: wall time
# c: cycles
TIME_AXIS = "i"
# Not recording the kernel can remove some noise, but can also make the trace more disjointed.
RECORD_KERNEL = True

import sys
import os
REPO_DIR = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, REPO_DIR)

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Session, Cluster, ConsistencyLevel
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import ExponentialReconnectionPolicy, RoundRobinPolicy

from test.pylib.host_registry import HostRegistry
from test.pylib.internal_types import ServerNum, IPAddress, HostID, ServerInfo
from test.pylib.manager_client import ManagerClient, IPAddress, ServerInfo
from test.pylib.pool import Pool
from test.pylib.random_tables import Column, TextType
from test.pylib.random_tables import RandomTables
from test.pylib.scylla_cluster import ScyllaServer, ScyllaCluster, get_cluster_manager, merge_cmdline_options
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.conftest import skip_mode
from test.topology.util import reconnect_driver

from typing import Dict, List, Callable, Any, Iterable, Optional, Awaitable, Union, AsyncIterator

import asyncio
import contextlib
import logging
import pathlib
import shlex
import shutil
import ssl
import subprocess
import tempfile
import time
import typing

# Mostly copied from test.py scripts.
# cluster_con helper: set up client object for communicating with the CQL API.
def cluster_con(hosts: List[IPAddress], port: int, use_ssl: bool, auth) -> Cluster:
    """Create a CQL Cluster connection object according to configuration.
       It does not .connect() yet."""
    assert len(hosts) > 0, "python driver connection needs at least one host to connect to"
    profile = ExecutionProfile(
        load_balancing_policy=RoundRobinPolicy(),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
        # The default timeouts should have been more than enough, but in some
        # extreme cases with a very slow debug build running on a slow or very busy
        # machine, they may not be. Observed tests reach 160 seconds. So it's
        # incremented to 200 seconds.
        # See issue #11289.
        # NOTE: request_timeout is the main cause of timeouts, even if logs say heartbeat
        request_timeout=200)
    if use_ssl:
        # Scylla does not support any earlier TLS protocol. If you try,
        # you will get mysterious EOF errors (see issue #6971) :-(
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    else:
        ssl_context = None

    #auth = PlainTextAuthProvider(username='cassandra', password='cassandra')

    return Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                   contact_points=hosts,
                   port=port,
                   protocol_version=4,
                   ssl_context=ssl_context,
                   # The default timeouts should have been more than enough, but in some
                   # extreme cases with a very slow debug build running on a slow or very busy
                   # machine, they may not be. Observed tests reach 160 seconds. So it's
                   # incremented to 200 seconds.
                   # See issue #11289.
                   connect_timeout = 200,
                   control_connection_timeout = 200,
                   # NOTE: max_schema_agreement_wait must be 2x or 3x smaller than request_timeout
                   # else the driver can't handle a server being down
                   max_schema_agreement_wait=20,
                   idle_heartbeat_timeout=200,
                   # The default reconnection policy has a large maximum interval
                   # between retries (600 seconds). In tests that restart/replace nodes,
                   # where a node can be unavailable for an extended period of time,
                   # this can cause the reconnection retry interval to get very large,
                   # longer than a test timeout.
                   reconnection_policy = ExponentialReconnectionPolicy(1.0, 4.0),
                   auth_provider=auth,
                   )

# Mostly copied from test.py scripts.
@contextlib.asynccontextmanager
async def with_manager() -> AsyncIterator[ManagerClient]:
    hosts = HostRegistry()
    def get_cluster_factory(cluster_size: int, options) -> Callable[..., Awaitable]:
        def create_server(create_cfg: ScyllaCluster.CreateServerParams):
            cmdline_options = create_cfg.cmdline_from_test

            default_config_options = \
                    {"authenticator": "PasswordAuthenticator",
                     "authorizer": "CassandraAuthorizer"}
            config_options = default_config_options | \
                             create_cfg.config_from_test

            server = ScyllaServer(
                mode='release',
                exe=f"{shlex.quote(REPO_DIR)}/build/release/scylla",
                vardir="muh_tmp",
                logger=create_cfg.logger,
                cluster_name=create_cfg.cluster_name,
                ip_addr=create_cfg.ip_addr,
                seeds=create_cfg.seeds,
                cmdline_options=cmdline_options,
                config_options=config_options,
                property_file=create_cfg.property_file,
                append_env={}
                )

            return server

        async def create_cluster(logger: Union[logging.Logger, logging.LoggerAdapter]) -> ScyllaCluster:
            cluster = ScyllaCluster(logger, hosts, cluster_size, create_server)
            await cluster.install_and_start()
            return cluster

        return create_cluster

    async def recycle_cluster(cluster: ScyllaCluster) -> None:
        await cluster.stop()
        await cluster.release_ips()

    create_cluster = get_cluster_factory(0, {})
    clusters = Pool(1, create_cluster, recycle_cluster)
    async with get_cluster_manager("muh_tmp", clusters, "muh_tmp/rel") as manager:
        await manager.start()
        url = manager.sock_path
        auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
        manager_int = ManagerClient(url, 9042, False, auth, cluster_con)
        try:
            yield manager_int
        finally:
            await manager_int.stop()

# Sets up perf record with the given arguments, but doesn't start recording yet.
# Recording can be started and stopped by writing/reading magic words to/from the returned file objects.
@contextlib.asynccontextmanager
async def with_perf_record(record_dir: str, args: list[str]) -> typing.AsyncIterator[tuple[typing.IO, typing.IO]]:
    control_fname = os.path.abspath(f"{record_dir}/control.fifo")
    ack_fname = os.path.abspath(f"{record_dir}/ack.fifo")
    os.mkfifo(control_fname)
    os.mkfifo(ack_fname)
    try:
        proc = await asyncio.subprocess.create_subprocess_exec("sudo", "perf", "record", "--snapshot", "--kcore", "--delay=-1", f"--control=fifo:{control_fname},{ack_fname}", "--mmap-pages=256M,256M", *args, cwd=record_dir)
        with open(control_fname, "wb", buffering=0) as w:
            with open(ack_fname, "rb", buffering=0) as r:
                try:
                    yield w, r
                finally:
                    w.write(b"stop\n")
                    r.read(5)
        await proc.communicate()
        assert proc.returncode == 0
    finally:
        os.unlink(control_fname)
        os.unlink(ack_fname)

# Enables recording just for the duration of the `with` statement.
# Takes the return value of with_perf_record as the argument.
@contextlib.asynccontextmanager
async def with_perf_enabled(control_fds: tuple[typing.IO, typing.IO]) -> typing.AsyncIterator[None]:
    w, r = control_fds
    w.write(b"enable\n")
    r.read(5)
    yield
    w.write(b"snapshot\n")
    r.read(5)

async def run(manager: ManagerClient) -> None:
    print("Setting up the cluster...")
    # Setup for the traced operation.
    servers = [await manager.server_add(cmdline=["--idle-poll-time-us=0", "--smp=1"]) for i in range(2)]
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    pids = await asyncio.gather(*[manager.server_get_pid(s.server_id) for s in servers])
    string = ",".join(str(p) for p in pids)
    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}")
    await cql.run_async("CREATE TABLE ks.t (pk int primary key, v int)")
    insert = cql.prepare(f"INSERT INTO ks.t (pk, v) VALUES (?, ?)")
    select = cql.prepare(f"SELECT * FROM ks.t WHERE pk = ? BYPASS CACHE")
    await cql.run_async(insert, [0, 0])
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, "ks", "t") for s in servers])
    flag = '' if RECORD_KERNEL else 'u'

    # The meat.
    print("Starting `perf record`...")
    async with with_perf_record(".", [f"--pid={string}", f"--event=intel_pt/cyc=1/{flag}"]) as control:
        # It's a very good idea to run the operation once after setting up the trace but before starting it.
        # It should raise the probability that all relevant lazy initialization
        # (authenticating clients by Scylla, paging the relevant parts of the executable into memory,
        # etc. are performed ahead of time, so that they don't pollute the trace.
        await cql.run_async(select, [0], host=hosts[0])
        print("Recording...")
        async with with_perf_enabled(control):
            # Here the actual trace happens.
            await cql.run_async(select, [0], host=hosts[0])

async def main() -> None:
    print("Setting up the manager...")
    async with with_manager() as manager:
        await run(manager)

if __name__ == "__main__":
    subprocess.run(["sudo", "-v"])
    dlfilter = f"{REPO_DIR}/scripts/perf2perfetto/target/release/libperf2perfetto.so"
    if not os.path.exists(dlfilter):
        raise RuntimeError(f"Make sure to build {dlfilter} first. (Go into perf2perfetto/ and run `cargo build --release`).")
    with tempfile.TemporaryDirectory() as tmp_dir:
        cwd = os.getcwd()
        os.chdir(tmp_dir)
        asyncio.run(main())
        print("Decoding. It might take several seconds.")
        print("Sometimes `perf scripts` falls into an infinite loop (I think the decoder can't deal with some mutable code in the kernel), so watch out.")
        print("If that happens, you have to kill it manually.")
        subprocess.run(f"sudo perf --no-pager script --itrace=bei0ns --dlfilter={shlex.quote(dlfilter)} --dlarg=out.ftf --dlarg={TIME_AXIS}", shell=True)
        subprocess.run("sudo chown $(whoami) -R perf.data", shell=True)
        subprocess.run(f"cp *.ftf {shlex.quote(cwd)}", shell=True)
    print("If everything went right, the script should have produced out.ftf in the current directory.")
    print("You should be able to view it e.g. with https://ui.perfetto.dev/")
