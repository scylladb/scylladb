#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Reproducer for SCYLLADB-1618: Shard-aware drivers connecting to :9042
can enter an infinite retry loop when there's a conntrack load imbalance.

The shard-aware connection algorithm over port 9042 works as follows:
1. Driver connects to :9042
2. Server assigns the connection to the least-loaded shard (conntrack load balancer)
3. Driver reads SCYLLA_SHARD from the SUPPORTED response
4. If it's the target shard, done. Otherwise, cache as spare (at most 1 per shard,
   closing the old spare if one exists).
5. Retry from step 1.

When there's a permanent imbalance in conntrack load >= 2 between shards,
the driver wanting the most-loaded shard will never reach it. Each retry
always gets the least-loaded shard, and closing old spares is a no-op
for load balance.
"""

import asyncio
import logging
import struct

import pytest
from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)

CQL_PORT = 9042


def build_options_frame(stream: int = 0) -> bytes:
    """Build a CQL protocol v4 OPTIONS frame."""
    frame = bytearray()
    frame.append(0x04)       # version 4
    frame.append(0x00)       # flags
    frame.extend(struct.pack('!H', stream))  # stream
    frame.append(0x05)       # opcode: OPTIONS
    frame.extend(struct.pack('!I', 0))       # body length: 0
    return bytes(frame)


def parse_string_multimap(data: bytes, offset: int) -> dict:
    """Parse a CQL string multimap from bytes at the given offset.
    Returns dict of key -> list of values.
    """
    result = {}
    num_keys = struct.unpack_from('!H', data, offset)[0]
    offset += 2

    for _ in range(num_keys):
        key_len = struct.unpack_from('!H', data, offset)[0]
        offset += 2
        key = data[offset:offset + key_len].decode('utf-8')
        offset += key_len

        num_values = struct.unpack_from('!H', data, offset)[0]
        offset += 2
        values = []
        for _ in range(num_values):
            val_len = struct.unpack_from('!H', data, offset)[0]
            offset += 2
            val = data[offset:offset + val_len].decode('utf-8')
            offset += val_len
            values.append(val)
        result[key] = values

    return result


async def connect_and_get_shard(host: str, port: int):
    """Open a TCP connection and send CQL OPTIONS to determine the assigned shard.
    Returns (reader, writer, shard_id).
    """
    reader, writer = await asyncio.open_connection(host, port)

    writer.write(build_options_frame(stream=0))
    await writer.drain()

    response = await asyncio.wait_for(reader.read(4096), timeout=10.0)
    assert len(response) >= 9, f"Short response: {len(response)} bytes"
    assert response[4] == 0x06, f"Expected SUPPORTED (0x06), got {hex(response[4])}"

    # Body starts at offset 9 (after 9-byte frame header)
    supported = parse_string_multimap(response, 9)
    shard_id = int(supported['SCYLLA_SHARD'][0])

    return reader, writer, shard_id


async def close_conn(writer):
    writer.close()
    await writer.wait_closed()


@pytest.mark.asyncio
async def test_shard_aware_connection_loop(manager: ManagerClient):
    """Reproducer for SCYLLADB-1618.

    Creates a conntrack load imbalance on port 9042 and then simulates
    the driver's shard-aware connection algorithm, showing that it cannot
    reach the overloaded shard within a reasonable number of attempts.
    """
    num_shards = 2
    server = await manager.server_add(cmdline=['--smp', str(num_shards)])
    host = server.ip_addr

    #
    # Step 1: Create conntrack load imbalance on port 9042.
    #
    # Open several connections and categorize by shard, then close
    # all connections on one shard so the other shard is "overloaded".
    #
    setup_conns = []
    shard_conns = {s: [] for s in range(num_shards)}

    for _ in range(6):
        r, w, shard = await connect_and_get_shard(host, CQL_PORT)
        setup_conns.append((r, w, shard))
        shard_conns[shard].append(w)

    # Pick the shard with fewer connections as the "light" shard.
    # Close all its connections so the other shard becomes overloaded.
    light_shard = min(shard_conns, key=lambda s: len(shard_conns[s]))
    overloaded_shard = 1 - light_shard

    logger.info("Shard connection counts before imbalance: %s",
                {s: len(shard_conns[s]) for s in range(num_shards)})
    logger.info("Closing all connections on shard %d to create imbalance", light_shard)

    for w in shard_conns[light_shard]:
        await close_conn(w)
    shard_conns[light_shard].clear()

    # Give conntrack time to process the closes (handle destructors
    # send cross-shard messages).
    await asyncio.sleep(0.5)

    remaining = len(shard_conns[overloaded_shard])
    logger.info("Conntrack imbalance: shard %d has %d connections, shard %d has 0",
                overloaded_shard, remaining, light_shard)
    assert remaining >= 2, \
        f"Need imbalance >= 2, but overloaded shard only has {remaining} connections"

    #
    # Step 2: Simulate the driver's shard-aware algorithm trying to
    # reach the overloaded shard.
    #
    # The driver keeps at most 1 spare connection per shard. When it
    # gets a connection to a shard it already has a spare for, it
    # closes the old spare (decrementing conntrack load) and stores
    # the new one (already counted at accept time). Net effect on
    # conntrack load: zero. So the imbalance persists and the driver
    # is stuck in a loop.
    #
    spares = {}   # shard_id -> writer
    max_attempts = 30
    success = False

    for attempt in range(1, max_attempts + 1):
        r, w, shard = await connect_and_get_shard(host, CQL_PORT)

        if shard == overloaded_shard:
            logger.info("Reached overloaded shard %d on attempt %d", shard, attempt)
            await close_conn(w)
            success = True
            break

        logger.info("Attempt %d: wanted shard %d, got shard %d",
                     attempt, overloaded_shard, shard)

        # Driver behavior: keep at most 1 spare per shard.
        if shard in spares:
            await close_conn(spares[shard])
        spares[shard] = w

    for w in spares.values():
        await close_conn(w)
    for w in shard_conns[overloaded_shard]:
        await close_conn(w)

    assert success, (
        f"Could not connect to shard {overloaded_shard} after {max_attempts} "
        f"attempts — shard-aware retry loop is stuck (SCYLLADB-1618)"
    )
