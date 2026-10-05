#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests for the SCYLLA_FAILURE_REASON_MAP CQL protocol extension, which makes
# Read_failure and Write_failure carry the <reason_map> of protocol v5 in place
# of <numfailures> (see docs/dev/protocol-extensions.md).
#
# The Python driver decodes <reason_map> only on protocol v5, so the tests
# speak protocol v4 over a raw socket.

import asyncio
import logging
import socket
import struct

import pytest
from cassandra import ConsistencyLevel, WriteFailure
from cassandra.query import SimpleStatement

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

EXTENSION = "SCYLLA_FAILURE_REASON_MAP"

OPCODE_ERROR = 0x00
OPCODE_STARTUP = 0x01
OPCODE_READY = 0x02
OPCODE_AUTHENTICATE = 0x03
OPCODE_OPTIONS = 0x05
OPCODE_SUPPORTED = 0x06
OPCODE_QUERY = 0x07
OPCODE_AUTH_RESPONSE = 0x0F
OPCODE_AUTH_SUCCESS = 0x10

READ_FAILURE = 0x1300
WRITE_FAILURE = 0x1500

CONSISTENCY_ALL = 0x0005

REASON_UNKNOWN = 0x0000


def _string(s: str) -> bytes:
    b = s.encode()
    return struct.pack("!H", len(b)) + b


def _long_string(s: str) -> bytes:
    b = s.encode()
    return struct.pack("!i", len(b)) + b


def _string_map(m: dict[str, str]) -> bytes:
    return struct.pack("!H", len(m)) + b"".join(_string(k) + _string(v) for k, v in m.items())


class _Reader:
    def __init__(self, body: bytes):
        self._body = body
        self._pos = 0

    def _take(self, n: int) -> bytes:
        assert self._pos + n <= len(self._body), "truncated message body"
        b = self._body[self._pos:self._pos + n]
        self._pos += n
        return b

    def byte(self) -> int:
        return self._take(1)[0]

    def short(self) -> int:
        return struct.unpack("!H", self._take(2))[0]

    def int(self) -> int:
        return struct.unpack("!i", self._take(4))[0]

    def string(self) -> str:
        return self._take(self.short()).decode()

    def string_list(self) -> list[str]:
        return [self.string() for _ in range(self.short())]

    def string_multimap(self) -> dict[str, list[str]]:
        return {self.string(): self.string_list() for _ in range(self.short())}

    def inetaddr(self) -> str:
        size = self.byte()
        assert size in (4, 16), f"unexpected [inetaddr] size {size}"
        return socket.inet_ntop(socket.AF_INET if size == 4 else socket.AF_INET6, self._take(size))

    def at_end(self) -> bool:
        return self._pos == len(self._body)


class _CqlConnection:
    """A minimal protocol v4 client."""

    def __init__(self, host: str):
        self._sock = socket.create_connection((host, 9042), timeout=60)
        self._stream = 0

    def close(self):
        self._sock.close()

    def _recv_exactly(self, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            assert chunk, "connection closed by the server"
            buf += chunk
        return bytes(buf)

    def request(self, opcode: int, body: bytes) -> tuple[int, bytes]:
        self._stream += 1
        self._sock.sendall(struct.pack("!BBhBI", 0x04, 0, self._stream, opcode, len(body)) + body)
        version, _flags, stream, resp_opcode, length = struct.unpack("!BBhBI", self._recv_exactly(9))
        assert version == 0x84, f"unexpected response version {version:#x}"
        assert stream == self._stream
        return resp_opcode, self._recv_exactly(length)

    def options(self) -> dict[str, list[str]]:
        opcode, body = self.request(OPCODE_OPTIONS, b"")
        assert opcode == OPCODE_SUPPORTED
        return _Reader(body).string_multimap()

    def startup(self, with_extension: bool):
        options = {"CQL_VERSION": "3.0.0"}
        if with_extension:
            options[EXTENSION] = ""
        opcode, _ = self.request(OPCODE_STARTUP, _string_map(options))
        if opcode == OPCODE_AUTHENTICATE:
            token = b"\x00cassandra\x00cassandra"
            opcode, _ = self.request(OPCODE_AUTH_RESPONSE, struct.pack("!i", len(token)) + token)
            assert opcode == OPCODE_AUTH_SUCCESS
        else:
            assert opcode == OPCODE_READY

    def query(self, cql: str, consistency: int) -> tuple[int, bytes]:
        return self.request(OPCODE_QUERY, _long_string(cql) + struct.pack("!HB", consistency, 0))


def _parse_failure(body: bytes, with_extension: bool) -> dict:
    r = _Reader(body)
    error = {"code": r.int(), "message": r.string(), "cl": r.short(), "received": r.int(), "blockfor": r.int()}
    if with_extension:
        error["reason_map"] = {}
        for _ in range(r.int()):
            addr = r.inetaddr()
            error["reason_map"][addr] = r.short()
    else:
        error["numfailures"] = r.int()
    if error["code"] == WRITE_FAILURE:
        error["write_type"] = r.string()
    elif error["code"] == READ_FAILURE:
        error["data_present"] = r.byte()
    assert r.at_end(), f"unexpected trailing bytes in error body: {error}"
    return error


def _run_failing_query(host: str, cql: str, with_extension: bool) -> dict:
    conn = _CqlConnection(host)
    try:
        conn.startup(with_extension)
        opcode, body = conn.query(cql, CONSISTENCY_ALL)
        assert opcode == OPCODE_ERROR, f"expected an ERROR response, got opcode {opcode:#x}"
        error = _parse_failure(body, with_extension)
        logger.info(f"{cql} on {host} (extension {'enabled' if with_extension else 'disabled'}): {error}")
        return error
    finally:
        conn.close()


async def _failing_query(host: str, cql: str, with_extension: bool) -> dict:
    return await asyncio.to_thread(_run_failing_query, host, cql, with_extension)


def _get_supported(host: str) -> dict[str, list[str]]:
    conn = _CqlConnection(host)
    try:
        return conn.options()
    finally:
        conn.close()


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_failure_reason_map(manager: ScyllaClusterManager):
    servers = await manager.servers_add(2, auto_rack_dc="dc1")
    ip0, ip1 = str(servers[0].ip_addr), str(servers[1].ip_addr)
    cql = manager.get_cql()

    supported = await asyncio.to_thread(_get_supported, ip0)
    assert EXTENSION in supported
    reasons = dict(v.split("=") for v in supported[EXTENSION])
    assert set(reasons) == {"REASON_RATE_LIMITED", "REASON_LARGE_DATA_REJECTED",
                            "REASON_CRITICAL_DISK_UTILIZATION", "REASON_ABORTED", "REASON_DISCONNECTED"}

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}") as ks:
        table = "tbl"
        await cql.run_async(f"CREATE TABLE {ks}.{table} (pk int PRIMARY KEY, v int)")
        await cql.run_async(SimpleStatement(f"INSERT INTO {ks}.{table} (pk, v) VALUES (1, 1)", consistency_level=ConsistencyLevel.ALL))

        insert = f"INSERT INTO {ks}.{table} (pk, v) VALUES (2, 2)"
        await manager.api.enable_injection(ip1, "database_apply", one_shot=False,
                                           parameters={"ks_name": ks, "cf_name": table, "what": "throw"})

        # The write fails on servers[1], which is a remote replica of the coordinator servers[0].
        error = await _failing_query(ip0, insert, with_extension=True)
        assert error["code"] == WRITE_FAILURE
        assert error["blockfor"] == 2
        assert error["reason_map"] == {ip1: REASON_UNKNOWN}
        assert error["write_type"] == "SIMPLE"

        # Without the extension the error carries <numfailures>.
        error = await _failing_query(ip0, insert, with_extension=False)
        assert error["code"] == WRITE_FAILURE
        assert error["numfailures"] == 1
        assert error["write_type"] == "SIMPLE"

        # The write fails on the coordinator servers[1] itself.
        error = await _failing_query(ip1, insert, with_extension=True)
        assert error["code"] == WRITE_FAILURE
        assert error["reason_map"] == {ip1: REASON_UNKNOWN}

        # A driver which does not enable the extension still decodes the error.
        with pytest.raises(WriteFailure):
            await cql.run_async(SimpleStatement(insert, consistency_level=ConsistencyLevel.ALL))

        await manager.api.disable_injection(ip1, "database_apply")

        select = f"SELECT v FROM {ks}.{table} WHERE pk = 1"
        await manager.api.enable_injection(ip1, "storage_proxy::handle_read", one_shot=False,
                                           parameters={"cf_name": table, "what": "throw"})

        # The read fails on servers[1], which is a remote replica of the coordinator servers[0].
        error = await _failing_query(ip0, select, with_extension=True)
        assert error["code"] == READ_FAILURE
        assert error["blockfor"] == 2
        assert error["reason_map"] == {ip1: REASON_UNKNOWN}

        error = await _failing_query(ip0, select, with_extension=False)
        assert error["code"] == READ_FAILURE
        assert error["numfailures"] == 1

        await manager.api.disable_injection(ip1, "storage_proxy::handle_read")
