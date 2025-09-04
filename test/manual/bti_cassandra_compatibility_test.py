#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2025-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

"""
This is a test for BTI index compatibility between Scylla and Cassandra.
It works in conjunction with the C++ part in bti_cassandra_compatibility_test.cc,
and expects ../../build/{build_mode}/test/manual/bti_cassandra_compatibility_test
to be built.

There is a fair amount of overlap between this script and `pgo.py`.
The common parts should probably be extracted.
"""

from collections.abc import Coroutine, AsyncIterator
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional
import argparse
import asyncio
import cassandra.cluster
import contextlib
import glob
import json
import logging
import os
import pathlib
import random
import shlex
import signal
import tempfile
import typing
import uuid
import yaml

################################################################################
# Common aliases.

PathLike = str | pathlib.Path
T = typing.TypeVar('T')

@dataclass
class Process:
    p: asyncio.subprocess.Process
    argv: list[str]
    logfile: Optional[PathLike]
    def __str__(self) -> str:
        status = self.p.returncode if self.p.returncode is not None else 'running'
        return f"PID={self.p.pid} argv={self.argv}, status={status}"

@dataclass
class ProcessError(Exception):
    proc: Process

# process outcome = (process metadata, stdout contents if captured, stderr contents if captured)
ProcessOutcome = tuple[Process, Optional[bytes], Optional[bytes]]

def q(s: PathLike):
    return shlex.quote(str(s))

################################################################################
# Loggers

module_logger = logging.getLogger("main")
config_logger = module_logger.getChild("config")
process_logger = module_logger.getChild("processes")

# If set, stdout and stderr of child processes will be output on the stdout
# and stderr of the script, and less urgent log levels will be enabled.
SUBPROCESS_OUTPUT: ContextVar[bool] = ContextVar('SUBPROCESS_OUTPUT', default=False)

# If set, every child process $pid will have its stdout and stderr logged to
# {LOGDIR.get()}/$pid.log
LOGDIR: ContextVar[Optional[str]] = ContextVar('LOGDIR')

# If set, all output of the script will be written to this path. Both all logs
# and all subprocess outputs.
GLOBAL_LOGFILE: ContextVar[Optional[str]] = ContextVar('GLOBAL_LOGFILE', default=None)

################################################################################
# Child process utilities

@contextlib.asynccontextmanager
async def with_context(ctx: ContextVar[T], val: T) -> AsyncIterator[None]:
    x = ctx.set(val)
    try:
        yield None
    finally:
        ctx.reset(x)

async def clean_gather(*coros: Coroutine) -> list:
    """Differs from asyncio.gather() in that it cancels all other tasks when one
    fails, and waits for the cancellations to complete.
    """
    async with asyncio.TaskGroup() as tg:
        return await asyncio.gather(*[tg.create_task(c) for c in coros])

def shielded(coro: Coroutine) -> Coroutine:
    """Launches a task wrapping `coro` and returns a coroutine which reliably
    awaits the task.

    The returned coroutine (and the task it awaits) cannot be cancelled from
    the outside. `await` called on the returned coroutine, when cancelled, will
    still wait until the `coro` task exits. The task won't see the cancellation
    request.

    This is mainly useful for preventing cleanup tasks ("destructors") from
    being interrupted.
    """
    return _shielded(asyncio.create_task(coro))

async def _shielded(task: asyncio.Task):
    try:
        await asyncio.shield(task)
    finally:
        await task

def cancel_process(proc: Process, timeout: Optional[float] = None, sig: int = signal.SIGINT) -> Coroutine:
    """Cancels a child process spawned with run().

    If the child process has already exited, does nothing. Otherwise sends a
    SIGINT to its process group and returns a coroutine which waits (with an
    optional timeout) for the child to exit. This coroutine is shielded from
    cancellation -- if cancelled, it will defer the cancel until the child
    exits or the wait times out.

    Note that the proper way to cancel the child is to send a SIGINT to its
    PGID, not just its PID. This is how most programs (e.g. shells) expect to
    be cancelled, because sending SIGINT to the entire process group is what
    terminals do when they receive Ctrl+C. So e.g. if our child process is a
    shell which has spawned its own child, sending SIGINT just to the shell
    would kill only the shell, but its own child would not see the SIGINT,
    staying alive even after its parent shell and its grandparent script exit.

    Assumes the PGID of `proc` is equal to its PID, as is set up by default by
    run().
    """
    # This purpose of this optimistic early return is just to avoid log spam.
    # (If the process has exited, "Cancelling" in logs would be misleading).
    if proc.p.returncode is not None:
        # This proc.p.wait() should be a no-op.
        return proc.p.wait()
    return shielded(_cancel_process(proc=proc, timeout=timeout, sig=sig))

async def _cancel_process(proc: Process, timeout: Optional[float], sig: int) -> None:
    process_logger.info(f"Cancelling (using signal {sig}) PIDG of {proc}")
    try:
        os.killpg(proc.p.pid, sig)
    except ProcessLookupError:
        # This either means that the child's entire process group already quit,
        # or that the child's PGID is different from its PID.
        # The latter means that the programmer is breaking the assumptions of
        # cancel_process(), so let's check against that prophylactically.
        try:
            assert os.getpgid(proc.p.pid) == proc.p.pid
        except ProcessLookupError:
            # Seems that the process really quit. That's OK.
            pass
    try:
        async with asyncio.timeout(timeout):
            await proc.p.wait()
    except asyncio.TimeoutError as e:
        process_logger.error(f"Error waiting for {proc}: wait timeout ({timeout}s) exceeded. Moving on without further wait. The process might be still alive after this script exits.")
        raise
    except BaseException as e:
        process_logger.error(f"Error waiting for {proc}: {e}. The process might be still alive after this script exits.")
        raise

async def run(command: list[str], cpuset: Optional[str] = None, **kwargs) -> Process:
    """Spawns a child process.

    kwargs are passed through to Popen().

    The child is ran in its own process group (with PGID equal to its PID),
    (unless process_group is explicitly set in kwargs). This is so that a
    Ctrl+C sent to the script doesn't automatically send SIGINT to children.
    This is important because an uninvited interruption of a child process
    could easily mess up some cleanup procedures, and we don't want cleanups
    to be interrupted, lest something is leaked when the script exits.
    """
    cmd = list(command)
    if cpuset:
        cmd = ["taskset", "-c", cpuset] + cmd

    orig_cmd = cmd[:] # Copy the command here for the purpose of logging, before it's uglified up by the wrappers below.

    kwargs.setdefault("process_group", 0)

    # Optionally log standard streams to the per-process log and to the global log.
    logdir = LOGDIR.get()
    global_logfile = GLOBAL_LOGFILE.get()
    n_handlers = bool(logdir) + bool(global_logfile)
    if n_handlers > 0:
        cmd = ["bash", "-c", 'exec 1> >(tee -a /dev/fd/3 >&1); exec 2> >(tee -a /dev/fd/3 >&2); exec "$@"', "run()"] + cmd
    if global_logfile:
        if n_handlers > 1:
            cmd = ["bash", "-c", 'exec 3> >(tee >(ts "%Y-%m-%d %H:%M:%.S" >>"$0") >&3); exec "$@"', global_logfile] + cmd
        else:
            cmd = ["bash", "-c", 'exec 3> >(ts "%Y-%m-%d %H:%M:%.S" >>"$0"); exec "$@"', global_logfile] + cmd
        n_handlers -= 1
    if logdir:
        if n_handlers > 1:
            cmd = ["bash", "-c", 'exec 3> >(tee -a "$0"/$$.log >&3); exec "$@"', logdir] + cmd
        else:
            cmd = ["bash", "-c", 'exec 3>>"$0"/$$.log; exec "$@"', logdir] + cmd
        n_handlers -= 1

    if not SUBPROCESS_OUTPUT.get():
        kwargs.setdefault("stdout", asyncio.subprocess.DEVNULL)
        kwargs.setdefault("stderr", asyncio.subprocess.DEVNULL)

    process_logger.info(f"Running a process: {orig_cmd}")
    p = await asyncio.create_subprocess_exec(*cmd, **kwargs)

    logfile = f"{logdir}/{p.pid}.log" if logdir else None
    proc = Process(p, orig_cmd, logfile)
    process_logger.debug(f"Started {proc}")
    return proc

async def wait(proc: Process) -> ProcessOutcome:
    """Waits for a process spawned with run() to exit.

    If the wait is cancelled, the child process is cancelled,
    and the wait doesn't return until the child exits.
    """
    process_logger.debug(f"Waiting for {proc}")
    try:
        o, e = await proc.p.communicate()
        process_logger.debug(f"Reaped {proc}")
        return (proc, o, e)
    finally:
        await cancel_process(proc)

async def run_checked(command: list[str], **kwargs) -> ProcessOutcome:
    """Runs a process to completion.

    Checks that it exited with code 0, otherwise raises an exception.
    Convenience wrapper over run() and wait().
    """
    proc, stdout, stderr = await wait(await run(command, **kwargs))
    assert proc.p.returncode is not None
    if proc.p.returncode != 0:
        raise ProcessError(proc)
    return proc, stdout, stderr

async def query(command: list[str], **kwargs) -> bytes:
    """Runs a process and returns its stdout's contents. Convenience wrapper over run_checked."""
    proc, stdout, stderr = await run_checked(command, stdout=asyncio.subprocess.PIPE, **kwargs)
    assert type(stdout) == bytes
    return stdout

async def bash(command: str, **kwargs) -> ProcessOutcome:
    """Runs a bash command. Convenience wrapper over run_checked."""
    return await run_checked(["bash", "-c", command], **kwargs)

################################################################################
# Cluster utilities

async def wait_for_node(proc: Process, addr: str, timeout: Optional[float] = None):
    """Waits for the Scylla/Cassandra node to start serving traffic (by opening its CQL port).
    Raises a timeout exception if the optional `timeout` elapses or if the node process
    exits before opening the port.
    """
    cql_port = 9042
    async with asyncio.TaskGroup() as tg:
        tasks: list[asyncio.Task] = [
            died := tg.create_task(proc.p.wait()),
            started := tg.create_task(wait_for_port(addr, cql_port)),
        ]
        done, not_done = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED, timeout=timeout)
        for t in tasks:
            t.cancel()
    if started in done:
        return
    elif died in done:
        process_logger.error(f"Node {addr} died before opening the CQL port ({cql_port}).")
        raise asyncio.TimeoutError
    else:
        process_logger.error(f"Node {addr} did not open the CQL port ({cql_port}) within the arbitrary timeout ({timeout} seconds).")
        raise asyncio.TimeoutError

# We attempt to randomize cluster names in order to prevent accidental spooky
# inter-cluster talk when the script is run concurrently with
# other instances (possibly from other invocation of the script)
# on the same machine.
def cluster_metadata(workdir: PathLike) -> dict:
    """Reads the cluster metadata file for a given cluster workdir.
    If there is no metadata file, creates it and initializes its parameters with arbitrarily chosen arguments.
    """
    fname = f"{workdir}/cluster_metadata"
    if os.path.exists(fname):
        with open(fname, "r") as f:
            res = json.load(f)
        assert res["name"] and res["subnet"]
        return res
    else:
        subnet = f"127.{random.randrange(0,256)}.{random.randrange(0,256)}"
        name = fr"cassandra-{uuid.uuid1()}"
        res = {"subnet": subnet, "name": name}
        with open(fname, "w") as f:
            json.dump(res, f)
        return res

class AddressAlreadyInUseException(Exception):
    def __init__(self, addresses, diagnostics):
        super().__init__(f"Attempted to start a cluster with addresses {','.join(addresses)}, but some of them appear to be already used:\n{diagnostics}")

def concat_lists_with_separator(lists: list[list[T]], sep: T) -> list:
    """concat_lists_with_separator([["src", "x"], ["src", "y"], ["src", "z"]], "|") == ["src", "x", "|", "src", "y", "|", "src", "z"]
    """
    res: list = []
    for x in lists:
        res.extend(x)
        res.append(sep)
    return res[:-1]

async def validate_addrs_unused(addresses: list[str]) -> None:
    """Checks that there are no TCP sockets listening on any of the addresses.
    If such sockets exist, raises an exception.
    """
    # ss_filter looks like this: src 127.0.0.1 | src 127.0.0.2 | src 127.0.0.3
    ss_filter = concat_lists_with_separator([["src", x] for x in addresses], "|")

    ss_command = ["ss", "--tcp", "--numeric", "--listening", "--no-header", "--processes"] + ss_filter
    ss_output = (await query(ss_command)).strip()
    if ss_output:
        diagnostics = f"Command: {shlex.join(ss_command)}\nOutput (expected empty):\n{ss_output.decode()}"
        raise AddressAlreadyInUseException(addresses, diagnostics)

async def start_node(image: str, cluster_workdir: PathLike, addr: str, seed: str, cluster_name: str, extra_opts: list[str]) -> Process:
    """Starts a Cassandra node from the given container image.
    Its --workdir will be $cluster_workdir/$addr/, its log file will be $cluster_workdir/$addr.log,
    and its PWD will be $cluster_workdir.

    """
    # These paths are relative to cluster_workdir.
    # The directory change to it happens via the cwd=cluster_workdir in run()
    cassandra_workdir = f"{addr}"
    os.makedirs(f"{cluster_workdir}/{cassandra_workdir}", exist_ok=True)
    logfile = f"{addr}.log"

    # The entrypoint script chowns `/var/lib/cassandra`` to `cassandra:cassandra``,
    # and runs `cassandra` as user `cassandra`.
    # But we don't want any ownership changes, we just want to run cassandra
    # as the current user (which will be root inside the container).
    #
    # So we hack the entrypoint script to turn the chown and user switch into no-ops.
    entrypoint_path = "/usr/local/bin/docker-entrypoint.sh"
    modified_entrypoint_path = f"{cluster_workdir}/docker-entrypoint.sh"
    base_entrypoint = (await query(["podman", "run", "--rm", "--entrypoint", "/usr/bin/env", image, "cat", entrypoint_path])).decode()
    with open(modified_entrypoint_path, "w") as f:
        f.write(base_entrypoint)
    os.chmod(modified_entrypoint_path, 0o755)
    await query(["sed", "-E", "-i", r's/chown cassandra/true cassandra/', modified_entrypoint_path])
    await query(["sed", "-E", "-i", r's/exec gosu cassandra/true cassandra/', modified_entrypoint_path])

    with open("cassandra_latest.yaml", "r") as f:
        cassandra_yaml = yaml.safe_load(f)
    cassandra_yaml["cluster_name"] = cluster_name
    cassandra_yaml["broadcast_rpc_address"] = addr
    cassandra_yaml["broadcast_address"] = addr
    # If you want a multi-node cluster,
    # handle `seed` properly here and remove the assert.
    assert addr == seed
    yaml_path = f"{os.path.realpath(cluster_workdir)}/{cassandra_workdir}/cassandra.yaml"
    with open(yaml_path, "w") as f:
        yaml.dump(cassandra_yaml, f)
    command = [
        "podman", "run",
        "--rm",
        "--name", f"{cluster_name}_{addr}",
        "--log-driver", "passthrough-tty",
        "-v", f"{yaml_path}:/etc/cassandra/cassandra.yaml",
        "-v", f"{os.path.realpath(cluster_workdir)}/{cassandra_workdir}:/var/lib/cassandra",
        "-v", f"{modified_entrypoint_path}:{entrypoint_path}",
        "-v", f"{os.path.realpath(cluster_workdir)}:/cluster",
        "-p", f"{addr}:9042:9042",
        "-e", f"CASSANDRA_LISTEN_ADDRESS={addr}",
        "-e", f"JVM_OPTS=-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.ring_delay_ms=0",
        image,
        # "-f" means "run in foreground"
        # "-R" means "allow running cassandra as root"
        "cassandra", "-f", "-R",
    ] + list(extra_opts)

    return await run(
        ['bash', '-c', fr"""{shlex.join(command)} >{q(logfile)} 2>&1"""],
        cwd=cluster_workdir)

async def start_cluster(image: str, addrs: list[str], workdir: PathLike, cluster_name: str, extra_opts: list[str]) -> list[Process]:
    """Starts a Cassandra cluster, with a directory structure like this:
    my_workdir/                                        # {workdir}
    ├── 127.73.130.1.log
    ├── 127.73.130.1                                   # {addrs[0]}
    │   └── commitlog, data, hints,
    ├── 127.73.130.2.log
    ├── 127.73.130.2                                   # {addrs[1]}
    │   └── commitlog, data, hints,
    ├── 127.73.130.3.log
    ├── 127.73.130.3                                   # {addrs[2]}
    │   └── commitlog, data, hints,
    └── cluster_metadata

    The (Linux-wise) working directory of all nodes is the top level directory ({workdir}).

    If start_cluster() is cancelled, it will cancel already spawned nodes
    and wait for them to exit.
    """
    assert addrs

    # Cannot prevent address clashes (because TOCTOU), but better than no validation.
    await validate_addrs_unused(addrs)

    timeout = 300
    procs = []
    seed = addrs[0]
    try:
        for i in range(0, len(addrs)):
            proc = await start_node(image, addr=addrs[i], seed=seed, cluster_workdir=workdir, cluster_name=cluster_name, extra_opts=extra_opts)
            procs.append(proc)
            await wait_for_node(proc, addrs[i], timeout)
    except:
        await stop_cluster(image, procs, addrs, workdir)
        raise
    return procs

async def stop_cluster(image: str, procs: list[Process], addrs: list[str], workdir: PathLike) -> None:
    """Stops a cluster started with start_cluster().
    Doesn't return until all nodes exit, even if stop_cluster() is cancelled.

    """
    await clean_gather(*[cancel_process(p, timeout=60) for p in procs])

async def wait_for_port(addr: str, port: int) -> None:
    await bash(fr'until printf "" >>/dev/tcp/{addr}/{port}; do sleep 0.1; done 2>/dev/null')

@contextlib.asynccontextmanager
async def with_cluster(image: str, workdir: PathLike) -> AsyncIterator[str, tuple[list[str], list[Process]]]:
    """Starts a Cassandra cluster.
    Doesn't monitor the state of the cluster in any way, just starts the cluster as
    the context manager enters, waits for each CQL port to open, yields the cluster's
    control info and stops the cluster as the context manager exits.
    """
    meta = cluster_metadata(workdir)
    cluster_name = meta["name"]
    subnet = meta["subnet"]
    addrs = [f"{subnet}.{i}" for i in range(1,255)][:1]
    process_logger.info(f"Starting a cluster of {image} in {workdir}")
    procs = await start_cluster(addrs=addrs, image=image, workdir=workdir, cluster_name=cluster_name, extra_opts=[])
    process_logger.info(f"Started the cluster in {workdir}")
    try:
        yield cluster_name, addrs, procs
    finally:
        process_logger.info(f"Stopping the cluster in {workdir}")
        await shielded(stop_cluster(image, procs, addrs, workdir))
        process_logger.info(f"Stopped the cluster in {workdir}")

async def main(seed: int, partition_count: Optional[int], row_count: Optional[int], workdir: PathLike, build_mode: str) -> None:
    """
    The logic of the test.

    It has the following parts:
    1. Run the C++ part of the test with "--phase=prepare" mode, which generates a Scylla
       table in BIG format, and corresponding BTI index files.
    2. Import the generated BIG sstable into Cassandra (using `nodetool import`),
       and let Cassandra upgrade them to BTI.
    3. Run the C++ part of the test with "--phase=check" mode, which checks that
       Scylla can read both the Scylla-BTI and the Cassandra-BTI index files,
       and that they return expected results.
    4. Clone the imported table in Cassandra. In the clone,
       replace Cassandra-BTI index files with Scylla-BTI files.
       Check that every row from the original table can be read via a point query on the clone.
    """
    scylla_dir = f"{workdir}/scylla"
    cassandra_dir = f"{workdir}/cassandra"
    cassandra_image = "docker.io/cassandra:5.0.5"
    test_executable = f"../../build/{build_mode}/test/manual/bti_cassandra_compatibility_test"
    os.makedirs(scylla_dir, exist_ok=True)
    os.makedirs(cassandra_dir, exist_ok=True)

    # Can be used to skip the slow prepare phase when debugging.
    write = True

    if write:
        module_logger.info(f"Phase 1: use Scylla to generate a BIG sstable and matching BTI index files.")
        extra_args = (
            ([f"--partition-count={partition_count}"] if partition_count is not None else [])
            + ([f"--row-count={row_count}"] if row_count is not None else [])
        )
        await bash(f"rm -rf {workdir}/scylla/*")
        await run_checked([
            test_executable,
            "--smp=1", "--memory=2G",
            f"--random-seed={seed}",
            f"--scylla-directory={scylla_dir}",
            # "--logger-log-level=trie=trace",
            # "--logger-log-level=testlog=trace",
            "--phase=prepare",
        ] + extra_args)

    # The schema is randomized. Learn it from here.
    with open(f"{workdir}/scylla/tablename.txt", "r") as f:
        orig_tablename = f.read().strip()
        clone_tablename = f"{orig_tablename}_clone"
    with open(f"{workdir}/scylla/schema.cql", "r") as f:
        schema = f.read().strip()
        # Doing this replace via naive string operations is hacky, but good enough.
        clone_schema = schema.replace(orig_tablename, clone_tablename)

    if write:
        module_logger.info(f"Phase 2: import the Scylla-BIG files into Cassandra.")
        # Copy Scylla-generated files into the upload directory (visible to the Cassandra container).
        await bash(f"rm -rf {cassandra_dir}/upload && cp -r {workdir}/scylla {cassandra_dir}/upload")
        with open(f"{cassandra_dir}/upload/schema_clone.cql", "w") as f:
            f.write(clone_schema)
        async with with_cluster(cassandra_image, cassandra_dir) as (cluster_name, addrs, _):
            process_logger.info(f"Cluster name: {cluster_name}, addresses: {addrs}")
            module_logger.info(f"Recreate keyspace.")
            await bash(f"""podman exec {cluster_name}_{addrs[0]} cqlsh -e "DROP KEYSPACE IF EXISTS ks; CREATE KEYSPACE ks WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}};" """)
            module_logger.info(f"Delete potential leftovers from earlier tests.")
            await bash(f"podman exec {cluster_name}_{addrs[0]} bash -c 'rm -rf /var/lib/cassandra/data/ks/*'")
            module_logger.info(f"CREATE the imported table.")
            await bash(f"podman exec {cluster_name}_{addrs[0]} cqlsh -f /cluster/upload/schema.cql")
            # Note: this line must be before the next one, because orig_table_dirname is a prefix of clone_tablename.
            orig_table_dirname = glob.glob(f"{cassandra_dir}/{addrs[0]}/data/ks/{orig_tablename}*")[0]
            module_logger.info(f"CREATE a clone of the imported table. (The sstable will be copied to it later.)")
            await bash(f"podman exec {cluster_name}_{addrs[0]} cqlsh -f /cluster/upload/schema_clone.cql")
            clone_table_dirname = glob.glob(f"{cassandra_dir}/{addrs[0]}/data/ks/{clone_tablename}*")[0]
            module_logger.info(f"Import the sstable from the upload directory.")
            await bash(f"podman exec {cluster_name}_{addrs[0]} bash -c 'rm -rf /var/lib/cassandra/upload && cp -r /cluster/upload /var/lib/cassandra/upload'")
            await bash(f"podman exec {cluster_name}_{addrs[0]} nodetool import ks {orig_tablename} /var/lib/cassandra/upload")
            module_logger.info(f"Upgrade the sstable from BIG to BTI.")
            await bash(f"podman exec {cluster_name}_{addrs[0]} nodetool upgradesstables")

        module_logger.info(f"Validate that Cassandra imported the sstable.")
        # We check that Cassandra generated exactly one sstable,
        # that there are BTI index files, and that the BTI Data.db file
        # is the same size as the Scylla-generated BIG Data.db.
        # (Note: we deliberately don't check that the files are bytewise identical,
        # because they aren't — BTI Data.db files have a different layout for partition tombstones).
        scylla_data = glob.glob(f"{scylla_dir}/*Data.db")
        scylla_partitions = glob.glob(f"{scylla_dir}/*Partitions.db")
        scylla_rows = glob.glob(f"{scylla_dir}/*Rows.db")
        cassandra_data = glob.glob(f"{orig_table_dirname}/*Data.db")
        cassandra_partitions = glob.glob(f"{orig_table_dirname}/*Partitions.db")
        cassandra_rows = glob.glob(f"{orig_table_dirname}/*Rows.db")
        process_logger.info(f"Scylla data files: {scylla_data}")
        process_logger.info(f"Cassandra data files: {cassandra_data}")
        assert len(scylla_data) == 1
        assert len(scylla_partitions) == 1
        assert len(scylla_rows) == 1
        assert len(cassandra_data) == 1
        assert len(cassandra_partitions) == 1
        assert len(cassandra_rows) == 1
        assert os.path.getsize(scylla_data[0]) == os.path.getsize(cassandra_data[0])

        target_rows_filename = os.path.basename(cassandra_rows[0])
        target_partitions_filename = os.path.basename(cassandra_partitions[0])

        await bash(f"cp {orig_table_dirname}/* {clone_table_dirname}")
        await bash(f"cp {scylla_partitions[0]} {clone_table_dirname}/*Partitions.db")
        await bash(f"cp {scylla_rows[0]} {clone_table_dirname}/*Rows.db")

    module_logger.info(f"Phase 3: check that Scylla can read the BTI files (both Cassandra's and its own).")
    await run_checked([
        test_executable,
        f"--random-seed={seed}",
        f"--cassandra-directory={orig_table_dirname}",
        f"--scylla-directory={scylla_dir}",
        "--phase=check",
        # "--logger-log-level=trie=trace",
        # "--logger-log-level=testlog=trace",
    ] + extra_args)

    module_logger.info(f"Phase 4: check that Scylla's BTI files don't break Cassandra queries.")
    async with with_cluster(cassandra_image, cassandra_dir) as (cluster_name, addrs, procs):
        process_logger.info("Connecting to Cassandra and fetching primary key columns...")

        cluster = cassandra.cluster.Cluster([addrs[0]], port=9042, protocol_version=4)
        try:
            session = cluster.connect("ks")
            table_meta = cluster.metadata.keyspaces["ks"].tables[orig_tablename]
            key_columns = [col.name for col in table_meta.primary_key]
            partition_key_columns = [col.name for col in table_meta.partition_key]

            module_logger.info(f"Primary key columns for ks.{orig_tablename}: {key_columns}")
            scan_stmt = session.prepare(f"SELECT * FROM ks.{orig_tablename}")
            point_read_stmt = session.prepare(f"SELECT * FROM ks.{clone_tablename} WHERE " + " AND ".join([f"{col}=?" for col in key_columns]))
            point_read_stmt_partition_key_only = session.prepare(f"SELECT * FROM ks.{clone_tablename} WHERE " + " AND ".join([f"{col}=?" for col in partition_key_columns]))
            module_logger.info(f"Scan statement for ks.{orig_tablename}: {scan_stmt}")
            module_logger.info(f"Point read statement for ks.{orig_tablename}: {point_read_stmt}")
            rows = session.execute(scan_stmt)
            for (i, row) in enumerate(rows):
                key_values = [getattr(row, col) for col in key_columns]
                if None in key_values:
                    module_logger.debug("Checking static row {}: {}".format(i, row))
                    partition_key_values = [getattr(row, col) for col in partition_key_columns]
                    result = session.execute(point_read_stmt_partition_key_only, partition_key_values)
                    result_rows = list(result)
                    if list(result_rows) != [row]:
                        raise RuntimeError("Expected: {}, got: {}".format([row], result_rows))
                else:
                    module_logger.debug("Checking row {}: {}".format(i, row))
                    result = session.execute(point_read_stmt, key_values)
                    result_rows = list(result)
                    if list(result_rows) != [row]:
                        raise RuntimeError("Expected: {}, got: {}".format([row], result_rows))
        finally:
            cluster.shutdown()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--random-seed", type=int, default=random.randint(0, 2**32 - 1), help="Random seed (default: random)")
    parser.add_argument("--partition-count", type=int, help="Max number of partitions")
    parser.add_argument("--row-count", type=int, help="Max number of rows per partition")
    parser.add_argument("--build-mode", type=str, default="release", help="Scylla build mode")
    parser.add_argument("--tmpdir", type=str, help="Temporary directory")
    args = parser.parse_args()

    # logging.getLogger().setLevel(logging.NOTSET)
    logging.getLogger().setLevel(logging.INFO)
    console = logging.StreamHandler()
    formatter = logging.Formatter(fmt='%(levelname)-8s %(name)-14s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)

    module_logger.info(f"Arguments: {args}")

    os.chdir(os.path.dirname(os.path.realpath(__file__)))
    with tempfile.TemporaryDirectory(delete=False, dir=args.tmpdir) as workdir:
        LOGDIR.set(f"{workdir}/logs")
        os.makedirs(LOGDIR.get(), exist_ok=True)
        GLOBAL_LOGFILE.set(f"{workdir}/global.log")
        module_logger.info(f"Working directory: {workdir}")
        asyncio.run(main(
            seed=args.random_seed,
            partition_count=args.partition_count,
            row_count=args.row_count,
            workdir=workdir,
            build_mode=args.build_mode
        ))