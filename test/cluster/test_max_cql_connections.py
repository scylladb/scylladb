#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import resource
import struct
import time

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.skip_types import skip_env
from test.pylib.util import wait_for
import pytest

CONNECTIONS = 10500


async def open_cql_connection(ip: str) -> asyncio.StreamWriter:
    """Open a raw CQL connection and do the STARTUP handshake (the server
    answers READY or AUTHENTICATE), which is much cheaper than a driver
    session."""
    reader, writer = await asyncio.open_connection(ip, 9042)
    body = struct.pack(">H", 1) + b"".join(struct.pack(">H", len(s)) + s for s in [b"CQL_VERSION", b"3.0.0"])
    writer.write(struct.pack(">BBhBi", 4, 0, 0, 1, len(body)) + body)
    await writer.drain()
    header = await reader.readexactly(9)
    await reader.readexactly(struct.unpack(">i", header[5:9])[0])
    assert header[4] in (0x02, 0x03), f"unexpected response opcode {header[4]}"  # READY or AUTHENTICATE
    return writer


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_max_cql_connections(manager: ScyllaClusterManager):
    """A single shard can serve more than 10000 CQL connections, and frees
    them when they are closed, so that as many can be opened again.
    Reproduces the crash fixed by https://github.com/scylladb/scylla/pull/9052."""
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    needed = CONNECTIONS + 1000
    if hard != resource.RLIM_INFINITY and hard < needed:
        skip_env(f"needs RLIMIT_NOFILE >= {needed}, the hard limit is {hard}")
    if soft != resource.RLIM_INFINITY and soft < needed:
        resource.setrlimit(resource.RLIMIT_NOFILE, (needed, hard))
    try:
        server = await manager.server_add(cmdline=["--smp", "1", "--max-networking-io-control-blocks", str(CONNECTIONS + 1000)], config={"tablets_mode_for_new_keyspaces": "disabled"})

        async def current_connections() -> int:
            metrics = await manager.metrics.query(server.ip_addr)
            return int(metrics.get("scylla_transport_current_connections"))

        baseline = await current_connections()
        for _ in range(2):
            writers = []
            try:
                for start in range(0, CONNECTIONS, 500):
                    writers += await asyncio.gather(*(open_cql_connection(server.ip_addr) for _ in range(start, min(start + 500, CONNECTIONS))))
                assert await current_connections() == baseline + CONNECTIONS
            finally:
                for writer in writers:
                    writer.close()
                await asyncio.gather(*(writer.wait_closed() for writer in writers), return_exceptions=True)

            async def connections_released():
                return True if await current_connections() == baseline else None
            await wait_for(connections_released, time.time() + 60)
    finally:
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
