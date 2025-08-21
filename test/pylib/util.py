#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from __future__ import annotations

import re
import subprocess
import threading
import time
import asyncio
import logging
import pathlib
import os
from collections.abc import Awaitable, Callable, Coroutine
from functools import cache

import random
import string

from typing import Optional, TypeVar, Any

from cassandra.cluster import NoHostAvailable, Session, Cluster # type: ignore # pylint: disable=no-name-in-module
from cassandra.protocol import InvalidRequest # type: ignore # pylint: disable=no-name-in-module
from cassandra.pool import Host # type: ignore # pylint: disable=no-name-in-module
from cassandra.query import Statement # type: ignore # pylint: disable=no-name-in-module
from cassandra import DriverException, ConsistencyLevel  # type: ignore # pylint: disable=no-name-in-module

from test import BUILD_DIR, TOP_SRC_DIR
from test.pylib.internal_types import ServerInfo

logger = logging.getLogger(__name__)


class LogPrefixAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return '[%s] %s' % (self.extra['prefix'], msg), kwargs


T = TypeVar('T')


def unique_name(unique_name_prefix = 'test_'):
    if not hasattr(unique_name, "last_ms"):
        unique_name.last_ms = 0
    current_ms = int(round(time.time() * 1000))
    # If unique_name() is called twice in the same millisecond...
    if unique_name.last_ms >= current_ms:
        current_ms = unique_name.last_ms + 1
    unique_name.last_ms = current_ms
    return unique_name_prefix + str(current_ms) + '_' + ''.join(random.choice(string.ascii_lowercase) for _ in range(5))


async def wait_for(
        pred: Callable[[], Awaitable[Optional[T]]],
        deadline: float,
        period: float = 1,
        before_retry: Optional[Callable[[], Any]] = None) -> T:
    while True:
        assert(time.time() < deadline), "Deadline exceeded, failing test."
        res = await pred()
        if res is not None:
            return res
        await asyncio.sleep(period)
        if before_retry:
            before_retry()


async def wait_for_cql(cql: Session, host: Host, deadline: float) -> None:
    async def cql_ready():
        try:
            await cql.run_async("select * from system.local", host=host)
        except NoHostAvailable:
            logging.info(f"Driver not connected to {host} yet")
            return None
        return True
    await wait_for(cql_ready, deadline)


async def wait_for_cql_and_get_hosts(cql: Session, servers: list[ServerInfo], deadline: float) \
        -> list[Host]:
    """Wait until every server in `servers` is available through `cql`
       and translate `servers` to a list of Cassandra `Host`s.
    """
    ip_set = set(str(srv.rpc_address) for srv in servers)
    async def get_hosts() -> Optional[list[Host]]:
        hosts = cql.cluster.metadata.all_hosts()
        remaining = ip_set - {h.address for h in hosts}
        if not remaining:
            return hosts

        logging.info(f"Driver hasn't yet learned about hosts: {remaining}")
        return None
    def try_refresh_nodes():
        try:
            cql.cluster.refresh_nodes(force_token_rebuild=True)
        except DriverException:
            # Silence the exception, which might get thrown if we call this in the middle of
            # driver reconnect (scylladb/scylladb#17616). `wait_for` will retry anyway and it's enough
            # if we succeed only one `get_hosts()` attempt before timing out.
            pass
    hosts = await wait_for(
        pred=get_hosts,
        deadline=deadline,
        before_retry=try_refresh_nodes,
    )

    # Take only hosts from `ip_set` (there may be more)
    hosts = [h for h in hosts if h.address in ip_set]

    # Make sure `hosts` has same order as `servers`, that is: a given index will
    # refer to the same underlying Scylla instance in both `servers` and `hosts`.
    servers_by_ip = {srv.rpc_address: i for i, srv in enumerate(servers)}
    hosts.sort(key=lambda x: servers_by_ip[x.address])

    await asyncio.gather(*(wait_for_cql(cql, h, deadline) for h in hosts))

    return hosts

def read_last_line(file_path: pathlib.Path, max_line_bytes = 512):
    file_size = os.stat(file_path).st_size
    with file_path.open('rb') as f:
        f.seek(max(0, file_size - max_line_bytes), os.SEEK_SET)
        line_bytes = f.read()
    line_str = line_bytes.decode('utf-8', errors='ignore')
    linesep = os.linesep
    if line_str.endswith(linesep):
        line_str = line_str[:-len(linesep)]
    linesep_index = line_str.rfind(linesep)
    if linesep_index != -1:
        line_str = line_str[linesep_index + len(linesep):]
    elif file_size > max_line_bytes:
        line_str = '...' + line_str
    return line_str


async def get_available_host(cql: Session, deadline: float) -> Host:
    hosts = cql.cluster.metadata.all_hosts()
    async def find_host():
        for h in hosts:
            try:
                await cql.run_async(
                    "select key from system.local where key = 'local'", host=h)
            except NoHostAvailable:
                logging.debug(f"get_available_host: {h} not available")
                continue
            return h
        return None
    return await wait_for(find_host, deadline)


# Wait for the given feature to be enabled.
async def wait_for_feature(feature: str, cql: Session, host: Host, deadline: float) -> None:
    async def feature_is_enabled():
        enabled_features = await get_enabled_features(cql, host)
        return feature in enabled_features or None
    await wait_for(feature_is_enabled, deadline)


async def get_supported_features(cql: Session, host: Host) -> set[str]:
    """Returns a set of cluster features that a node advertises support for."""
    rs = await cql.run_async(f"SELECT supported_features FROM system.local WHERE key = 'local'", host=host)
    return set(rs[0].supported_features.split(","))


async def get_enabled_features(cql: Session, host: Host) -> set[str]:
    """Returns a set of cluster features that a node considers to be enabled."""
    rs = await cql.run_async(f"SELECT value FROM system.scylla_local WHERE key = 'enabled_features'", host=host)
    return set(rs[0].value.split(","))


class KeyGenerator:
    def __init__(self):
        self.pk = None
        self.pk_lock = threading.Lock()

    def next_pk(self):
        with self.pk_lock:
            if self.pk is not None:
                self.pk += 1
            else:
                self.pk = 0
            return self.pk

    def last_pk(self):
        with self.pk_lock:
            return self.pk


async def start_writes(cql: Session, keyspace: str, table: str, concurrency: int = 3, ignore_errors=False):
    logger.info(f"Starting to asynchronously write, concurrency = {concurrency}")

    stop_event = asyncio.Event()

    warmup_writes = 128 // concurrency
    warmup_event = asyncio.Event()

    stmt = cql.prepare(f"INSERT INTO {keyspace}.{table} (pk, c) VALUES (?, ?)")
    stmt.consistency_level = ConsistencyLevel.QUORUM
    rd_stmt = cql.prepare(f"SELECT * FROM {keyspace}.{table} WHERE pk = ?")
    rd_stmt.consistency_level = ConsistencyLevel.QUORUM

    key_gen = KeyGenerator()

    async def do_writes(worker_id: int):
        write_count = 0
        while not stop_event.is_set():
            pk = key_gen.next_pk()

            # Once next_pk() is produced, key_gen.last_key() is assumed to be in the database
            # hence we can't give up on it.
            while True:
                try:
                    await cql.run_async(stmt, [pk, pk])
                    # Check read-your-writes
                    rows = await cql.run_async(rd_stmt, [pk])
                    assert(len(rows) == 1)
                    assert(rows[0].c == pk)
                    write_count += 1
                    break
                except Exception as e:
                    if ignore_errors:
                        pass # Expected when node is brought down temporarily
                    else:
                        raise e

            if pk == warmup_writes:
                warmup_event.set()

        logger.info(f"Worker #{worker_id} did {write_count} successful writes")

    tasks = [asyncio.create_task(do_writes(worker_id)) for worker_id in range(concurrency)]

    await asyncio.wait_for(warmup_event.wait(), timeout=60)

    async def finish():
        logger.info("Stopping workers")
        stop_event.set()
        await asyncio.gather(*tasks)

        last = key_gen.last_pk()
        if last is not None:
            return last + 1
        return 0

    return finish

async def wait_for_view_v1(cql: Session, name: str, node_count: int, timeout: int = 120):
    async def view_is_built():
        done = await cql.run_async(f"SELECT COUNT(*) FROM system_distributed.view_build_status WHERE status = 'SUCCESS' AND view_name = '{name}' ALLOW FILTERING")
        return done[0][0] == node_count or None
    deadline = time.time() + timeout
    await wait_for(view_is_built, deadline)

async def wait_for_view(cql: Session, name: str, node_count: int, timeout: int = 120):
    async def view_is_built():
        done = await cql.run_async(f"SELECT COUNT(*) FROM system.view_build_status_v2 WHERE status = 'SUCCESS' AND view_name = '{name}' ALLOW FILTERING")
        return done[0][0] == node_count or None
    deadline = time.time() + timeout
    await wait_for(view_is_built, deadline)


async def wait_for_first_completed(coros: list[Coroutine]):
    done, pending = await asyncio.wait([asyncio.create_task(c) for c in coros], return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()
    for t in done:
        await t


def ninja(target: str) -> str:
    """Build specified target using ninja."""

    return subprocess.Popen(
        # cmake places build.ninja in build/, traditional is in ./.
        # We choose to test for traditional, not cmake, because IDEs may
        # invoke cmake to learn the configuration and generate false positives
        args=["ninja", *(["-C", str(BUILD_DIR)] if not TOP_SRC_DIR.joinpath("build.ninja").exists() else []), target],
        stdout=subprocess.PIPE,
        cwd=TOP_SRC_DIR,
    ).communicate()[0].decode()


@cache
def get_configured_modes() -> list[str]:
    out = ninja('mode_list')
    # [1/1] List configured modes
    # debug release dev
    return re.sub(r'.* List configured modes\n(.*)\n', r'\1',
                            out, count=1, flags=re.DOTALL).split('\n')[-1].split(' ')


def get_modes_to_run(config) -> list[str]:
    modes = config.getoption('modes')
    if not modes:
        modes = get_configured_modes()
    if not modes:
        raise RuntimeError('No modes configured. Please run ./configure.py first')
    return modes


async def gather_safely(*awaitables: Awaitable):
    """
    Developers using asyncio.gather() often assume that it waits for all futures (awaitables) givens.
    But this isn't true when the return_exceptions parameter is False, which is the default.
    In that case, as soon as one future completes with an exception, the gather() call will return this exception
    immediately, and some of the finished tasks may continue to run in the background.
    This is bad for applications that use gather() to ensure that a list of background tasks has all completed.
    So such applications must use asyncio.gather() with return_exceptions=True, to wait for all given futures to
    complete either successfully or unsuccessfully.
    """
    results = await asyncio.gather(*awaitables, return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException):
            raise result from None
    return results


def get_xdist_worker_id() -> str | None:
    return os.environ.get("PYTEST_XDIST_WORKER")


def execute_with_tracing(cql : Session, statement : str | Statement, log : bool = False, *cql_execute_extra_args, **cql_execute_extra_kwargs):
    """ Execute statement via cql session and log the tracing output. """

    cql_execute_extra_kwargs['trace'] = True
    query_result = cql.execute(statement, *cql_execute_extra_args, **cql_execute_extra_kwargs)

    tracing = query_result.get_all_query_traces(max_wait_sec_per=900)

    ret = []
    page_traces = []
    for trace in tracing:
        ret.append(trace.events)
        if not log:
            continue

        trace_events = []
        for event in trace.events:
            trace_events.append(f"  {event.source} {event.source_elapsed} {event.description}")
        page_traces.append("\n".join(trace_events))

    if log:
        logger.debug("Tracing {}:\n{}\n".format(statement, "\n".join(page_traces)))

    return ret


class DictWithMaxSize[KT, VT](dict[KT, VT]):
    """A dictionary that tracks statistics about its maximum number of items."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Initialize max size to current length after initialization
        self._max_size = len(self)

        # By default, just collect statistics, but if max_size property is set than check the boundary.
        self._check_max_size = False

    @property
    def max_size(self) -> int:
        return self._max_size

    @max_size.setter
    def max_size(self, value: int) -> None:
        """Set the upper boundary for number of items in the dict and enable the check."""

        self._max_size = value
        self._check_max_size = True
        self._update_max_size()

    @max_size.deleter
    def max_size(self) -> None:
        """Reset the statistics to the current count."""

        self._max_size = len(self)
        self._check_max_size = False

    def _update_max_size(self) -> None:
        """Update the maximum item count if current count is larger."""

        actual_size = len(self)
        if self._max_size < actual_size:
            if self._check_max_size:
                raise ValueError(
                    f"Actual number of elements in the dict ({actual_size}) is greater than expected: {self._max_size}"
                )
            self._max_size = actual_size

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        self._update_max_size()

    def update(self, other=(), /, **kwds):
        super().update(other, **kwds)
        self._update_max_size()

    def setdefault(self, key, default=None):
        result = super().setdefault(key, default)
        self._update_max_size()
        return result
