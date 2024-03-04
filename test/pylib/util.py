#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import time
import asyncio
import logging
import pathlib
import os
import pytest

from typing import Callable, Awaitable, Optional, TypeVar, Any

from cassandra.cluster import NoHostAvailable, Session, Cluster # type: ignore # pylint: disable=no-name-in-module
from cassandra.protocol import InvalidRequest # type: ignore # pylint: disable=no-name-in-module
from cassandra.pool import Host # type: ignore # pylint: disable=no-name-in-module
from cassandra import DriverException # type: ignore # pylint: disable=no-name-in-module

from test.pylib.internal_types import ServerInfo

class LogPrefixAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return '[%s] %s' % (self.extra['prefix'], msg), kwargs


unique_name_prefix = 'test_'
T = TypeVar('T')


def unique_name():
    if not hasattr(unique_name, "last_ms"):
        unique_name.last_ms = 0
    current_ms = int(round(time.time() * 1000))
    # If unique_name() is called twice in the same millisecond...
    if unique_name.last_ms >= current_ms:
        current_ms = unique_name.last_ms + 1
    unique_name.last_ms = current_ms
    return unique_name_prefix + str(current_ms)


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


async def read_barrier(cql: Session, host: Host):
    """To issue a read barrier it is sufficient to attempt dropping a
    non-existing table. We need to use `if exists`, otherwise the statement
    would fail on prepare/validate step which happens before a read barrier is
    performed.
    """
    await cql.run_async("drop table if exists nosuchkeyspace.nosuchtable", host = host)


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
