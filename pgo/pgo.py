#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from collections.abc import Coroutine, AsyncIterator, Callable
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Optional
import asyncio
import contextlib
import glob
import json
import logging
import os
import pathlib
import random
import re
import shlex
import shutil
import signal
import subprocess
import sys
import tempfile
import typing
import uuid

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

module_logger = logging.getLogger("pgo")
config_logger = module_logger.getChild("config")
process_logger = module_logger.getChild("processes")
training_logger = module_logger.getChild("training")

# If set, stdout and stderr of child processes will be output on the stdout
# and stderr of the script, and less urgent log levels will be enabled.
SUBPROCESS_OUTPUT: ContextVar[bool] = ContextVar('SUBPROCESS_OUTPUT', default=False)

# If set, less urgent log levels will be printed to stdout.
VERBOSE: ContextVar[bool] = ContextVar('VERBOSE', default=False)

# If set, every child process $pid will have its stdout and stderr logged to
# {LOGDIR.get()}/$pid.log
LOGDIR: ContextVar[Optional[str]] = ContextVar('LOGDIR')

# If set, all output of the script will be written to this path. Both all logs
# and all subprocess outputs.
GLOBAL_LOGFILE: ContextVar[Optional[str]] = ContextVar('GLOBAL_LOGFILE', default=None)

################################################################################
# Environment

NODE_CPUSETS: ContextVar[Optional[list[str]]] = ContextVar('NODE_CPUSETS')
CS_CPUSET: ContextVar[Optional[str]] = ContextVar('CS_CPUSET')

def configure_cpusets():
    """
    Let's try to schedule Scylla nodes on separate cpusets, and the load generators on yet
    different cpuset, to speed up the training and/or allow for reasonable tests.
    """
    num_cpus = os.cpu_count()
    if num_cpus >= 12:
        config_logger.info(f"{num_cpus} (>=12) CPUs available")
        NODE_CPUSETS.set([f"0,{num_cpus//2}", f"1,{1+num_cpus//2}", f"2,{2+num_cpus//2}"])
        CS_CPUSET.set(f"3-{num_cpus//2-1},{3+num_cpus//2}-{num_cpus-1}")
    else:
        config_logger.info(f"{num_cpus} (<12) CPUs available")
        config_logger.warning(f"Due to a small number of available CPUs, the training will be run in overprovisioned mode: load generators and Scylla nodes will share cores. This will make the training much slower. This slowness could also result in an inaccurate (unrealistic) profile.")
        NODE_CPUSETS.set(None)
        CS_CPUSET.set(None)
    config_logger.info(f"Choosing cpusets for nodes: {NODE_CPUSETS.get()}")
    config_logger.info(f"Choosing cpuset for load generators: {CS_CPUSET.get()}")

JAVA_HOME: ContextVar[Optional[str]] = ContextVar('JAVA_HOME')

async def configure_java() -> None:
    """
    cassandra-stress can only deal with Java 11
    """
    version_output = (await bash("java -version", stderr=asyncio.subprocess.PIPE))[2]
    assert isinstance(version_output, bytes)
    version_first_line = version_output.decode().split(sep='\n')[0]
    config_logger.info(f"First line of java -version: {version_first_line}")
    version = 11
    if re.search(rf'version.*{version}\.[0-9]+\.[0-9]+', version_first_line):
        config_logger.info(f"Default Java version recognized as Java {version}. Proceeding with the default.")
        JAVA_HOME.set(None)
        return

    config_logger.info(f"Default Java version is not recognized as Java {version}.")
    if os.path.exists(java_path := f'/usr/lib/jvm/java-{version}'):
        config_logger.warning(f"{java_path} found. Choosing it as JAVA_HOME.")
        JAVA_HOME.set(java_path)
        return

    error = f"Failed to find a suitable Java version. Java {version} is required."
    config_logger.error(error)
    raise RuntimeError(error)

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
# Scylla cluster utilities

async def wait_for_node(proc: Process, addr: str, timeout: Optional[float] = None):
    """Waits for the Scylla node to start serving traffic (by opening its CQL port).
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
        training_logger.error(f"Node {addr} died before opening the CQL port ({cql_port}).")
        raise asyncio.TimeoutError
    else:
        training_logger.error(f"Node {addr} did not open the CQL port ({cql_port}) within the arbitrary timeout ({timeout} seconds).")
        raise asyncio.TimeoutError

# We attempt to randomize cluster names in order to prevent accidental spooky
# inter-cluster talk when the script is run concurrently with
# other instances of Scylla (possibly from other invocation of the script)
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
        name = fr"PGO:{workdir}:{uuid.uuid1()}"
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

async def start_node(executable: PathLike, cluster_workdir: PathLike, addr: str, seed: str, cluster_name: str, extra_opts: list[str]) -> Process:
    """Starts a Scylla node.
    Its --workdir will be $cluster_workdir/$addr/, its log file will be $cluster_workdir/$addr.log,
    and its PWD will be $cluster_workdir.

    """
    # These paths are relative to cluster_workdir.
    # The directory change to it happens via the cwd=cluster_workdir in run()
    llvm_profile_file = f"{addr}-%m.profraw"
    scylla_workdir = f"{addr}"
    logfile = f"{addr}.log"
    command = [
        "env",
        f"LLVM_PROFILE_FILE={llvm_profile_file}",
        f"SCYLLA_HOME={os.path.realpath(os.getcwd())}", # We assume that the script has Scylla's `conf/` as its filesystem neighbour.
        os.path.realpath(executable),
        f"--workdir={scylla_workdir}",
        "--ring-delay-ms=0",
        "--developer-mode=yes",
        "--memory=1G",
        "--unsafe-bypass-fsync=1",
        "--kernel-page-cache=1",
        f"--seed-provider-parameters=seeds={seed}",
        f"--listen-address={addr}",
        f"--api-address={addr}",
        f"--rpc-address={addr}",
        f"--prometheus-address={addr}",
        f"--alternator-address={addr}",
        f"--cluster-name={cluster_name}",
        f"--endpoint-snitch=GossipingPropertyFileSnitch",
        f"--read-request-timeout-in-ms=60000",
        f"--write-request-timeout-in-ms=60000",
        f"--cas-contention-timeout-in-ms=60000",
        f"--alternator-port=8000",
        f"--alternator-write-isolation=only_rmw_uses_lwt",
    ] + list(extra_opts)
    return await run(['bash', '-c', fr"""exec {shlex.join(command)} >{q(logfile)} 2>&1"""], cwd=cluster_workdir)

async def start_cluster(executable: PathLike, addrs: list[str], cpusets: Optional[list[str]], workdir: PathLike, cluster_name: str, extra_opts: list[str]) -> list[Process]:
    """Starts a Scylla cluster, with a directory structure like this:
    my_workdir/                                        # {workdir}
    ├── 127.73.130.1.log
    ├── 127.73.130.1                                   # {addrs[0]}
    │   └── commitlog, data, hints, view_hints
    ├── 127.73.130.2.log
    ├── 127.73.130.2                                   # {addrs[1]}
    │   └── commitlog, data, hints, view_hints
    ├── 127.73.130.3.log
    ├── 127.73.130.3                                   # {addrs[2]}
    │   └── commitlog, data, hints, view_hints
    └── cluster_metadata

    The (Linux-wise) working directory of all nodes is the top level directory ({workdir}).
    That's where their profile files will be output.

    If start_cluster() is cancelled, it will cancel already spawned nodes
    and wait for them to exit.
    """
    assert addrs

    # Cannot prevent address clashes (because TOCTOU), but better than no validation.
    await validate_addrs_unused(addrs)

    if cpusets:
        assert len(cpusets) >= len(addrs)
        cpuset_args = [[f"--cpuset={cpusets[i]}"] for i in range(len(addrs))]
    else:
        cpuset_args = [["--smp=2", "--overprovisioned"] for i in range(len(addrs))]

    timeout = 300
    procs = []
    seed = addrs[0]
    try:
        for i in range(0, len(addrs)):
            proc = await start_node(executable, addr=addrs[i], seed=seed, cluster_workdir=workdir, cluster_name=cluster_name, extra_opts=extra_opts+cpuset_args[i])
            procs.append(proc)
            await wait_for_node(proc, addrs[i], timeout)
    except:
        await stop_cluster(procs, addrs)
        raise
    return procs

async def stop_cluster(procs: list[Process], addrs: list[str]) -> None:
    """Stops a Scylla cluster started with start_cluster().
    Doesn't return until all nodes exit, even if stop_cluster() is cancelled.

    """
    await clean_gather(*[cancel_process(p, timeout=60) for p in procs])

async def wait_for_port(addr: str, port: int) -> None:
    await bash(fr'until printf "" >>/dev/tcp/{addr}/{port}; do sleep 0.1; done 2>/dev/null')

async def merge_profraw(directory: PathLike) -> None:
    "Merges LLVM *.profraw files in the directory into a prof.profdata file therein."
    if glob.glob(f"{directory}/*.profraw"):
        await bash(fr"llvm-profdata merge {q(directory)}/*.profraw -output {q(directory)}/prof.profdata")

async def get_bolt_opts(executable: PathLike) -> list[str]:
    """Returns the extra opts which have to be passed to a BOLT-instrumented Scylla
    to trigger a generation of a BOLT profile file.

    The relevant info (address of BOLT's dump function) is extracted from $executable.boltlog --
    it is assumed that BOLT's logs printed during the instrumentation of this executable are written there.
    If there is no such file, returns an empty list.
    """
    file = f"{executable}.boltlog"
    if os.path.exists(file):
        opt = await query(["sed", "-n", r'/entry point is/s/^.*\(0x[[:xdigit:]]*\).*$/\1/p', file])
        addr = opt.decode("ascii").strip()

        return [f"--bolt-instrumentation-dump-address={addr}"]
    else:
        return []

async def quiesce_cluster(addrs: list[str]) -> None:
    """Waits until all given nodes are done with compactions."""
    training_logger.info("Waiting for compactions to end.")
    grep = shlex.quote("compaction_manager_compactions{")
    awk = shlex.quote("{sum += $2} END {print sum}")
    while True:
        _, out, _ = await bash(fr"""for x in {" ".join(addrs)}; do curl --silent $x:9180/metrics; done | grep {grep} | awk {awk}""",
                               stdout=asyncio.subprocess.PIPE)
        assert type(out) == bytes
        if float(out.decode()) == 0:
            break
        await asyncio.sleep(10)

@contextlib.asynccontextmanager
async def with_cluster(executable: PathLike, workdir: PathLike, cpusets: Optional[list[str]] = None) -> AsyncIterator[tuple[list[str], list[Process]]]:
    """Provides a Scylla cluster.
    Doesn't monitor the state of the cluster in any way, just starts the cluster as
    the context manager enters, waits for each CQL port to open, yields the cluster's
    control info and stops the cluster as the context manager exits.
    """
    meta = cluster_metadata(workdir)
    cluster_name = meta["name"]
    subnet = meta["subnet"]
    addrs = [f"{subnet}.{i}" for i in range(1,255)][:3]
    cpusets = cpusets or NODE_CPUSETS.get()
    extra_opts = await get_bolt_opts(executable)
    training_logger.debug(f"BOLT opts for {executable} are {extra_opts}")
    training_logger.info(f"Starting a cluster of {executable} in {workdir}")
    procs = await start_cluster(addrs=addrs, executable=executable, workdir=workdir, cpusets=cpusets, cluster_name=cluster_name, extra_opts=extra_opts)
    training_logger.info(f"Started the cluster in {workdir}")
    try:
        yield addrs, procs
    finally:
        training_logger.info(f"Stopping the cluster in {workdir}")
        await stop_cluster(procs, addrs)
        training_logger.info(f"Stopped the cluster in {workdir}")

################################################################################
# cassandra-stress utilities

def cs_command(cmd: list[str], n: int, node: str, cl: str, pop: Optional[str] = None, warmup: bool = False, rate: str = "threads=200", schema: Optional[str] = None) -> list[str]:
    """Strings together a cassandra-stress command from given options."""
    return (["env", f"JAVA_HOME={JAVA_HOME.get()}"] if JAVA_HOME.get() else []) + [
        "../tools/java/tools/bin/cassandra-stress",
        *cmd,
        f"n={n}",
        f"cl={cl}",
    ] + (["no-warmup"] if not warmup else []) + [
    ] + (["-pop", pop] if pop else []) + [
        "-mode", "native", "cql3", "protocolVersion=4",
        "-node", node,
        "-rate", rate,
    ] + (["-schema", schema] if schema else []) + [
    ]

async def cs(run_kwargs: dict[str, Any] = {}, **params: Any) -> Process:
    """Runs a cassandra-stress process.
    Raises an exception if it reports a workload failure.
    """
    run_kwargs.setdefault('cpuset', CS_CPUSET.get())
    cmd = cs_command(**params)
    training_logger.info(f"Running cassandra-stress: {cmd}")
    proc, *_ = await run_checked(cs_command(**params), **run_kwargs)
    training_logger.info(f"cassandra-stress finished successfully")
    return proc

RF3_SCHEMA = "replication(strategy=NetworkTopologyStrategy,dc1=3)"
RF1_SCHEMA = "replication(strategy=NetworkTopologyStrategy,dc1=1)"

def kw(**kwargs):
    """Python syntax hack. Personal preference.
    kw(a=0, b=1, c=2) == {"a": 0, "b": 1, "c": 2}
    """
    return kwargs

################################################################################
# Training workload definitions

# The reason why we separate training and population phases is that the same training happens
# multiple times (for PGO, CSPGO and BOLT separately), so it saves some time to only populate once.
#
# The reason why there is a level of indirection between trainers and populators (trainers refer to their
# dataset by dataset name, not by function name) is to facilitate sharing of datasets between trainers.
# But this facility isn't currently used.

@contextlib.asynccontextmanager
async def with_cs_populate(executable: PathLike, workdir: PathLike) -> AsyncIterator[str]:
    """Provides a Scylla cluster and waits for compactions to end before stopping it."""
    async with with_cluster(executable=executable, workdir=workdir) as (addrs, procs):
        yield addrs[0]
        async with asyncio.timeout(3600):
            # Should it also flush memtables?
            await quiesce_cluster(addrs=addrs)

@contextlib.asynccontextmanager
async def with_cs_train(executable: PathLike, workdir: PathLike) -> AsyncIterator[str]:
    """Provides a Scylla cluster and merges generated profile files after it's stopped."""
    async with with_cluster(executable=executable, workdir=workdir) as (addrs, procs):
        yield addrs[0]
    await merge_profraw(workdir)

class Trainer(typing.Protocol):
    async def __call__(self, executable: PathLike, workdir: PathLike) -> None: ...
class Populator(typing.Protocol):
    async def __call__(self, executable: PathLike, workdir: PathLike) -> None: ...

# Maps training workload names to their dataset names and functions responsible for running the workload.
trainers: dict[str, tuple[str, Trainer]] = {}
# Maps dataset names to the functions responsible for populating them.
populators: dict[str, Populator] = {}

# BASIC ==================================================

async def populate_basic(executable: PathLike, workdir: PathLike) -> None:
    async with with_cs_populate(executable=executable, workdir=workdir) as server:
        await cs(cmd=["write"], n=2000000, cl="local_quorum", schema=RF3_SCHEMA, node=server)

async def train_basic(executable: PathLike, workdir: PathLike) -> None:
    N = 2500000 # Preferably keep big enough to cause compactions.
    async with with_cs_train(executable=executable, workdir=workdir) as server:
        await cs(cmd=["mixed", "ratio(read=1,write=1)"], n=N, pop=f"dist=UNIFORM(1..{2000000})", cl="local_quorum", node=server)

# disable as it's very similar to CLUSTERING workload
# and exactly the same as we use for our performance tests
#trainers["basic"] = ("basic_dataset", train_basic)
populators["basic_dataset"] = populate_basic

# ALTERNATOR ==================================================

async def train_alternator(executable: PathLike, workdir: PathLike) -> None:
    os.makedirs(workdir, exist_ok=True)
    async with with_cluster(executable=executable, workdir=workdir) as (addrs, procs):
        await asyncio.sleep(5) # FIXME: artificial gossip sleep, get rid of it.

        workloads = [
            ["write", 250_000],
            ["read", 250_000],
            ["scan", 1_000],
            ["write_gsi", 250_000],
            ["write_rmw", 250_000],
        ]
        for workload in workloads:
            # the tool doesn't yet support load balancing so we
            # run one process per node
            await clean_gather(*[run_checked([executable,
                "perf-alternator",
                "--smp", "1",
                "--workload", f"{workload[0]}",
                "--partitions", "100000",
                # we reuse cluster data so don't need to pre-populate
                "--prepopulate-partitions", "0",
                "--operations-per-shard", f"{workload[1]}",
                "--cpuset", f'{CS_CPUSET.get()}',
                "--remote-host", addr,
            ]) for addr in addrs])

    await merge_profraw(workdir)

async def populate_alternator(executable: PathLike, workdir: PathLike) -> None:
    async with with_cs_populate(executable=executable, workdir=workdir) as server:
        pass # this avoids profiling cluster bootstrap code

trainers["alternator"] = ("alternator_dataset", train_alternator)
populators["alternator_dataset"] = populate_alternator

# CLUSTERING  ==================================================

async def populate_clustering(executable: PathLike, workdir: PathLike) -> None:
    async with with_cs_populate(executable=executable, workdir=workdir) as server:
        await cs(cmd=["user", "profile=./conf/clustering.yaml", "ops(insert=1)"], n=5000000, cl="local_quorum", node=server)

async def train_clustering(executable: PathLike, workdir: PathLike) -> None:
    N = 2500000 # Preferably keep big enough to cause compactions.
    async with with_cs_train(executable=executable, workdir=workdir) as server:
        await cs(cmd=["user", "profile=./conf/clustering.yaml", "ops(insert=5,simple1=1,range1=1)"], n=N, cl="local_quorum", node=server)

trainers["clustering"] = ("clustering_dataset", train_clustering)
populators["clustering_dataset"] = populate_clustering

# DECOMMISSION ==================================================

async def populate_decommission(executable: PathLike, workdir: PathLike) -> None:
    async with with_cs_populate(executable=executable, workdir=workdir) as server:
        await cs(cmd=["write"], n=10000000, pop=f"dist=UNIFORM(1..8000000)", schema=RF1_SCHEMA, node=server, cl="one")

async def train_decommission(executable: PathLike, workdir: PathLike) -> None:
    async with with_cluster(executable=executable, workdir=workdir) as (addrs, procs):
        await asyncio.sleep(5) # FIXME: artificial gossip sleep, get rid of it.
        await bash(fr"curl --silent -X POST http://{addrs[2]}:10000/storage_service/decommission")
    await merge_profraw(workdir)

trainers["decommission"] = ("decommission_dataset", train_decommission)
populators["decommission_dataset"] = populate_decommission

# LWT ==================================================

async def populate_lwt(executable: PathLike, workdir: PathLike) -> None:
    async with with_cs_populate(executable=executable, workdir=workdir) as server:
        await cs(cmd=["user", "profile=./conf/lwt.yaml", "ops(insert=1)"], n=1000000, cl="local_quorum", node=server)

async def train_lwt(executable: PathLike, workdir: PathLike) -> None:
    async with with_cs_train(executable=executable, workdir=workdir) as server:
        ops = "ops(stmt-insert=1,stmt-select=1,stmt-update=1,stmt-delete=1,stmt-insert-if-not-exists=1,stmt-update-if-cond=1,stmt-update-if-exists=1,stmt-delete-if-cond=1,stmt-delete-if-exists=1)"
        await cs(cmd=["user", "profile=./conf/lwt.yaml", ops], n=100000, cl="local_quorum", node=server)

trainers["lwt"] = ("lwt_dataset", train_lwt)
populators["lwt_dataset"] = populate_lwt

# SI ==================================================

async def populate_si(executable: PathLike, workdir: PathLike) -> None:
    async with with_cs_populate(executable=executable, workdir=workdir) as server:
        await cs(cmd=["user", "profile=./conf/si.yaml", "ops(insert=1)"], n=100000, cl="local_quorum", node=server)

async def train_si(executable: PathLike, workdir: PathLike) -> None:
    async with with_cs_train(executable=executable, workdir=workdir) as server:
        await cs(cmd=["user", "profile=./conf/si.yaml", "ops(insert=25,si_read1=1,si_read2=1,si_read3=1,si_read4=1,si_read5=10)"], n=100000, cl="local_quorum", node=server)

trainers["si"] = ("si_dataset", train_si)
populators["si_dataset"] = populate_si

# COUNTERS ==================================================

async def populate_counters(executable: PathLike, workdir: PathLike) -> None:
    async with with_cs_populate(executable=executable, workdir=workdir) as server:
        await bash(fr"../tools/java/bin/cqlsh -f conf/counters.yaml {server}")
        # Sleeps added in reaction to schema disagreement errors.
        # FIXME: get rid of this sleep and find a sane way to wait for schema
        # agreement.
        await asyncio.sleep(3)
        await cs(cmd=["counter_write"], n=1000000, cl="local_quorum", node=server, schema="keyspace=counters")

async def train_counters(executable: PathLike, workdir: PathLike) -> None:
    async with with_cs_train(executable=executable, workdir=workdir) as server:
        await cs(cmd=["counter_write"], n=50000, pop=f"dist=UNIFORM(1..1000000)", cl="local_quorum", node=server, schema="keyspace=counters")
        await cs(cmd=["counter_read"], n=50000, pop=f"dist=UNIFORM(1..1000000)", cl="local_quorum", node=server, schema="keyspace=counters")

# This workload depends on cqlsh, so it's commented out until we merge
# python3 support in cqlsh (which, at the moment of writing, is supposed
# to be imminent).
#trainers["counters"] = ("counters_dataset", train_counters)
populators["counters_dataset"] = populate_counters

# REPAIR ==================================================

async def populate_repair(executable: PathLike, workdir: PathLike) -> None:
    async with with_cs_populate(executable=executable, workdir=workdir) as server:
        await cs(cmd=["user", "profile=./conf/repair.yaml", "ops(insert=1)"], n=5000000, cl="local_quorum", node=server)
        await cs(cmd=["write"], n=1000000, cl="local_quorum", schema=RF3_SCHEMA, node=server)

async def train_repair(executable: PathLike, workdir: PathLike) -> None:
    # The idea is to remove some user data sstables from the node (in an offline cluster),
    # start the cluster, and run repair on the affected node.
    # I don't know if it's a good PGO workload.
    # Does this cover repair codepaths reasonably?
    addr = cluster_metadata(workdir)["subnet"] + ".2"
    await bash(fr"rm -rf {workdir}/{addr}/data/ks/*")
    async with with_cluster(executable=executable, workdir=workdir) as (addrs, procs):
        await asyncio.sleep(5) # FIXME: artificial gossip sleep, get rid of it.
        repair_id = (await query(["curl", "--silent", "-X", "POST", fr"http://{addr}:10000/storage_service/repair_async/ks"])).decode()
        await query(["curl", "--silent", fr"http://{addr}:10000/storage_service/repair_status/?id={repair_id}"])
    await merge_profraw(workdir)

trainers["repair"] = ("repair_dataset", train_repair)
populators["repair_dataset"] = populate_repair

################################################################################
# Training procedures

def executable_exists(executable: PathLike) -> bool:
    """Checks if the file exists and is executable"""
    return bool(shutil.which(executable))

async def populate_datasets(executable: PathLike, dataset_names: list[str], dataset_dir: PathLike) -> None:
    """Populates the given datasets if they don't already exist.

    A "dataset" is simply a copy of the entire cluster workdir -- it consists
    of multiple Scylla workdirs and a bit of metadata.
    After this function, there will be a $dataset_dir/$x cluster workdir for each dataset x.
    These cluster workdirs can be copied somewhere for training and restored with start_cluster().
    """
    for t in dataset_names:
        t_dir = fr"{dataset_dir}/{t}"
        if not os.path.exists(t_dir):
            training_logger.info(f"Dataset {t_dir} doesn't exist. Populating.")
            tmpdir = fr"{dataset_dir}/tmp-{t}"
            await bash(fr"rm -rf {q(tmpdir)} && mkdir -p {q(tmpdir)}")
            await populators[t](executable, tmpdir)
            # If we have been using a profile-instrumented Scylla binary for populating,
            # remove its leftover profile files in the cluster directory.
            await bash(fr"find {q(tmpdir)} '(' -name '*.profraw' -o -name '*.fdata' ')' -delete")
            await bash(fr"mv {q(tmpdir)}/ {q(dataset_dir)}/{t}")
            training_logger.info(f"Dataset {t_dir} populated.")
        else:
            training_logger.info(f"Dataset {t_dir} already exists. Not populating.")

async def train_full(executable: PathLike, output_profile_file: PathLike, dataset_dir: PathLike) -> None:
    """Runs all known training workloads on the given executable, using
    prepopulated datasets in `dataset_dir`, or populating them first if they
    don't exist. Subsequent trainings will be able to reuse the datasets.

    The output of the training is a single profile file (prof.profdata) merged
    from all profile files generated by all nodes in all workloads.

    The training clusters will be set up in the directory "workdirs", located in
    the same directory as the output profile file.
    """
    # We create ancestor directories of the output file, if they don't exist.
    topdir = os.path.dirname(output_profile_file)
    os.makedirs(topdir)

    # Set up logging to {output_profile_file}.log/.
    os.makedirs(f"{output_profile_file}.log")
    LOGDIR.set(os.path.realpath(f"{output_profile_file}.log"))
    GLOBAL_LOGFILE.set(os.path.realpath(f"{output_profile_file}.log/global.log"))
    for file in [f"{LOGDIR.get()}/script.log", GLOBAL_LOGFILE.get()]:
        logfile = logging.FileHandler(str(file))
        logfile.setLevel(logging.DEBUG)
        formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(name)-14s %(message)s')
        logfile.setFormatter(formatter)
        logging.getLogger().addHandler(logfile)

    training_logger.info(f"Starting training of executable {executable}. Exhaustive logs can be found in {LOGDIR.get()}/")

    configure_cpusets()
    await configure_java()

    assert executable_exists(executable)

    workdirs = fr"{topdir}/workdirs"
    os.makedirs(workdirs)

    workloads = list(trainers)
    await populate_datasets(executable=executable, dataset_names=[trainers[w][0] for w in workloads], dataset_dir=dataset_dir)
    n = len(workloads)
    for i, w in enumerate(workloads):
        training_logger.info(f"Preparing training workload: {w} [{i+1} out of {n}]")
        workdir = fr"{workdirs}/{w}"
        w_dataset = fr"{dataset_dir}/{trainers[w][0]}"
        training_logger.info(f"Copying dataset {w_dataset} to {workdir}")
        await bash(fr"cp -r {q(w_dataset)} {q(workdir)}")
        training_logger.info(f"Running training workload: {w} [{i+1} out of {n}]")
        await trainers[w][1](executable=executable, workdir=workdir)
        training_logger.info(f"Finished training workload: {w} [{i+1} out of {n}]")

    training_logger.info(f"Merging profile files")

    if profdatas := glob.glob(fr"{workdirs}/**/*.profdata", recursive=True):
        await bash(fr"llvm-profdata merge {shlex.join(profdatas)} -output {q(output_profile_file)}")

    if fdatas := glob.glob(fr"{workdirs}/**/*.fdata", recursive=True):
        await bash(fr"merge-fdata {shlex.join(fdatas)} > {q(output_profile_file)}")

    training_logger.info(f"Finished training of executable {executable}. Output file is {output_profile_file}")

async def dump_log_tail(e: ProcessError):
    msg = f"{e.proc} failed."
    if e.proc.logfile:
        msg += f" Dumping last 50 lines of its log.\nDump of {e.proc.logfile}:\n"
        msg += (await query(["tail", "-n", "50", str(e.proc.logfile)])).decode()
    process_logger.critical(msg)

async def main() -> None:
    try:
        match sys.argv[1]:
            case "train_full":
                await train_full(executable=sys.argv[2], output_profile_file=sys.argv[3], dataset_dir=sys.argv[4])
            case _:
                print(f"Unknown command {sys.argv[1]}")
    except ProcessError as e:
        await dump_log_tail(e)
        raise
    except ExceptionGroup as eg:
        for ex in eg.exceptions:
            if isinstance(ex, ProcessError):
                await dump_log_tail(ex)
        raise

if __name__ == "__main__":
    # We keep the working directory of the script in the directory where the script is placed,
    # that is: ${scylla_repository}/pgo.
    os.chdir(os.path.dirname(os.path.realpath(__file__)))

    logging.getLogger().setLevel(logging.NOTSET)

    # Set up logging to stdout.
    console = logging.StreamHandler()
    formatter = logging.Formatter(fmt='%(levelname)-8s %(name)-14s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger().addHandler(console)

    if not VERBOSE.get():
        console.setLevel(logging.INFO)
        console.addFilter(lambda r: r.levelno >= logging.WARNING if r.name == "pgo.processes" else True)

    asyncio.run(main())
