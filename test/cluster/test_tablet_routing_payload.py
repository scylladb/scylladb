#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import socket
import struct
import time
import uuid
from dataclasses import dataclass

import pytest
from cassandra import ConsistencyLevel

from test.cluster.lwt.lwt_common import get_token_for_pk
from test.cluster.util import new_test_keyspace
from test.pylib.internal_types import HostID, ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.tablets import get_tablet_replicas
from test.pylib.util import wait_for

CQL_VERSION = 0x04
CQL_RESPONSE_VERSION = 0x84

OP_ERROR = 0x00
OP_STARTUP = 0x01
OP_READY = 0x02
OP_OPTIONS = 0x05
OP_SUPPORTED = 0x06
OP_QUERY = 0x07
OP_RESULT = 0x08
OP_PREPARE = 0x09
OP_EXECUTE = 0x0A

FLAG_CUSTOM_PAYLOAD = 0x04
FLAG_WARNING = 0x08

RESULT_PREPARED = 0x0004
TABLETS_ROUTING_V1_PAYLOAD = "tablets-routing-v1"
SHARD_AWARE_PORT = 19042


@dataclass
class CqlFrame:
    flags: int
    stream: int
    opcode: int
    body: bytes


def _write_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return struct.pack("!H", len(encoded)) + encoded


def _write_long_string(value: str) -> bytes:
    encoded = value.encode("utf-8")
    return struct.pack("!I", len(encoded)) + encoded


def _write_string_map(values: dict[str, str]) -> bytes:
    body = bytearray(struct.pack("!H", len(values)))
    for key, value in values.items():
        body.extend(_write_string(key))
        body.extend(_write_string(value))
    return bytes(body)


def _write_int_value(value: int) -> bytes:
    encoded = struct.pack("!i", value)
    return struct.pack("!i", len(encoded)) + encoded


def _write_query_parameters(consistency: int, values: list[int] | None = None) -> bytes:
    body = bytearray(struct.pack("!H", consistency))
    if values is None:
        body.append(0x00)
        return bytes(body)

    body.append(0x01)  # VALUES
    body.extend(struct.pack("!H", len(values)))
    for value in values:
        body.extend(_write_int_value(value))
    return bytes(body)


def _read_string(body: bytes, pos: int) -> tuple[str, int]:
    size = struct.unpack_from("!H", body, pos)[0]
    pos += 2
    value = body[pos:pos + size].decode("utf-8")
    pos += size
    return value, pos


def _read_string_list(body: bytes, pos: int) -> tuple[list[str], int]:
    size = struct.unpack_from("!H", body, pos)[0]
    pos += 2
    values = []
    for _ in range(size):
        value, pos = _read_string(body, pos)
        values.append(value)
    return values, pos


def _read_string_multimap(body: bytes) -> dict[str, list[str]]:
    pos = 0
    size = struct.unpack_from("!H", body, pos)[0]
    pos += 2
    values = {}
    for _ in range(size):
        key, pos = _read_string(body, pos)
        value_count = struct.unpack_from("!H", body, pos)[0]
        pos += 2
        key_values = []
        for _ in range(value_count):
            value, pos = _read_string(body, pos)
            key_values.append(value)
        values[key] = key_values
    return values


def _read_short_bytes(body: bytes, pos: int) -> tuple[bytes, int]:
    size = struct.unpack_from("!H", body, pos)[0]
    pos += 2
    value = body[pos:pos + size]
    pos += size
    return value, pos


def _read_string_bytes_map(body: bytes, pos: int) -> tuple[dict[str, bytes], int]:
    size = struct.unpack_from("!H", body, pos)[0]
    pos += 2
    values = {}
    for _ in range(size):
        key, pos = _read_string(body, pos)
        value_size = struct.unpack_from("!i", body, pos)[0]
        pos += 4
        assert value_size >= 0
        values[key] = body[pos:pos + value_size]
        pos += value_size
    return values, pos


def _read_cql_value(body: bytes, pos: int) -> tuple[bytes, int]:
    size = struct.unpack_from("!i", body, pos)[0]
    pos += 4
    assert size >= 0
    value = body[pos:pos + size]
    pos += size
    return value, pos


def _decode_tablet_routing_replicas(payload: bytes) -> list[tuple[uuid.UUID, int]]:
    _first_token, pos = _read_cql_value(payload, 0)
    _last_token, pos = _read_cql_value(payload, pos)
    replicas_payload, pos = _read_cql_value(payload, pos)
    assert pos == len(payload)

    replica_count = struct.unpack_from("!i", replicas_payload, 0)[0]
    pos = 4
    replicas = []
    for _ in range(replica_count):
        replica_payload, pos = _read_cql_value(replicas_payload, pos)
        host_id_payload, replica_pos = _read_cql_value(replica_payload, 0)
        shard_payload, replica_pos = _read_cql_value(replica_payload, replica_pos)
        assert replica_pos == len(replica_payload)
        replicas.append((uuid.UUID(bytes=host_id_payload), struct.unpack("!i", shard_payload)[0]))
    assert pos == len(replicas_payload)
    return replicas


async def _read_frame(reader: asyncio.StreamReader) -> CqlFrame:
    header = await reader.readexactly(9)
    version, flags, stream, opcode, body_len = struct.unpack("!BBhBI", header)
    assert version == CQL_RESPONSE_VERSION
    body = await reader.readexactly(body_len)
    return CqlFrame(flags=flags, stream=stream, opcode=opcode, body=body)


def _frame_body_start(frame: CqlFrame) -> int:
    pos = 0
    if frame.flags & FLAG_WARNING:
        _warnings, pos = _read_string_list(frame.body, pos)
    if frame.flags & FLAG_CUSTOM_PAYLOAD:
        _payload, pos = _read_string_bytes_map(frame.body, pos)
    return pos


def _custom_payload(frame: CqlFrame) -> dict[str, bytes]:
    if not frame.flags & FLAG_CUSTOM_PAYLOAD:
        return {}
    pos = 0
    if frame.flags & FLAG_WARNING:
        _warnings, pos = _read_string_list(frame.body, pos)
    payload, _pos = _read_string_bytes_map(frame.body, pos)
    return payload


def _cql_error(frame: CqlFrame) -> str:
    code = struct.unpack_from("!i", frame.body, 0)[0]
    message, _pos = _read_string(frame.body, 4)
    return f"CQL error {code}: {message}"


class RawCqlClient:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        self._stream = 0

    async def close(self) -> None:
        self._writer.close()
        await self._writer.wait_closed()

    async def send(self, opcode: int, body: bytes = b"") -> CqlFrame:
        self._stream += 1
        frame = struct.pack("!BBhBI", CQL_VERSION, 0, self._stream, opcode, len(body)) + body
        self._writer.write(frame)
        await self._writer.drain()
        response = await asyncio.wait_for(_read_frame(self._reader), timeout=100.0)
        if response.opcode == OP_ERROR:
            raise AssertionError(_cql_error(response))
        return response

    async def options(self) -> dict[str, list[str]]:
        response = await self.send(OP_OPTIONS)
        assert response.opcode == OP_SUPPORTED
        return _read_string_multimap(response.body)

    async def startup(self) -> None:
        response = await self.send(OP_STARTUP, _write_string_map({
            "CQL_VERSION": "3.0.0",
            "TABLETS_ROUTING_V1": "",
        }))
        assert response.opcode == OP_READY

    async def prepare(self, query: str) -> bytes:
        response = await self.send(OP_PREPARE, _write_long_string(query))
        assert response.opcode == OP_RESULT
        pos = _frame_body_start(response)
        result_kind = struct.unpack_from("!i", response.body, pos)[0]
        pos += 4
        assert result_kind == RESULT_PREPARED
        prepared_id, _pos = _read_short_bytes(response.body, pos)
        return prepared_id

    async def execute(self, prepared_id: bytes, values: list[int], consistency: int = ConsistencyLevel.ONE) -> CqlFrame:
        body = bytearray()
        body.extend(struct.pack("!H", len(prepared_id)))
        body.extend(prepared_id)
        body.extend(_write_query_parameters(consistency, values))
        response = await self.send(OP_EXECUTE, bytes(body))
        assert response.opcode == OP_RESULT
        return response

    async def query(self, query: str, consistency: int = ConsistencyLevel.ONE) -> CqlFrame:
        response = await self.send(OP_QUERY, _write_long_string(query) + _write_query_parameters(consistency))
        assert response.opcode == OP_RESULT
        return response


async def _open_connection(host: str, port: int) -> RawCqlClient:
    reader, writer = await asyncio.open_connection(host, port)
    return RawCqlClient(reader, writer)


async def _open_shard_aware_connection(host: str, port: int, shard: int, nr_shards: int) -> RawCqlClient:
    loop = asyncio.get_running_loop()
    last_error: Exception | None = None
    first_port = 20000 + shard
    for local_port in range(first_port, 60000, nr_shards):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(False)
        try:
            sock.bind(("127.0.0.1", local_port))
            await loop.sock_connect(sock, (host, port))
            reader, writer = await asyncio.open_connection(sock=sock)
            return RawCqlClient(reader, writer)
        except OSError as exc:
            last_error = exc
            sock.close()
    raise RuntimeError(f"Could not open shard-aware connection to shard {shard}") from last_error


async def _supported_options(host: str, port: int) -> dict[str, list[str]]:
    client = await _open_connection(host, port)
    try:
        return await client.options()
    finally:
        await client.close()


async def _connect_to_shard(host: str, port: int, shard: int, nr_shards: int) -> RawCqlClient:
    client = await _open_shard_aware_connection(host, port, shard, nr_shards)
    try:
        options = await client.options()
        assert int(options["SCYLLA_SHARD"][0]) == shard
        await client.startup()
        return client
    except Exception:
        await client.close()
        raise


async def _prepared_response_has_tablet_info(
        manager: ManagerClient,
        server: ServerInfo,
        shard_aware_port: int,
        nr_shards: int,
        keyspace: str,
        query: str,
        values: list[int],
        pk: int,
        consistency: int = ConsistencyLevel.ONE) -> bool:
    token = await get_token_for_pk(manager.get_cql(), keyspace, "test_tablet", pk)
    replicas = await get_tablet_replicas(manager, server, keyspace, "test_tablet", token)
    assert replicas
    owning_shard = replicas[0][1]
    wrong_shard = (owning_shard + 1) % nr_shards

    client = await _connect_to_shard(str(server.rpc_address), shard_aware_port, wrong_shard, nr_shards)
    try:
        prepared_id = await client.prepare(query)
        response = await client.execute(prepared_id, values, consistency)
        return TABLETS_ROUTING_V1_PAYLOAD in _custom_payload(response)
    finally:
        await client.close()


async def _get_table_raft_group_id(manager: ManagerClient, keyspace: str, table: str) -> str:
    table_id = await manager.get_table_id(keyspace, table)
    rows = await manager.get_cql().run_async(f"SELECT raft_group_id FROM system.tablets where table_id = {table_id}")
    assert rows
    return str(rows[0].raft_group_id)


async def _wait_for_tablet_raft_leader(
        manager: ManagerClient,
        servers_by_host_id: dict[str, ServerInfo],
        replicas: list[tuple[HostID, int]],
        group_id: str) -> uuid.UUID:
    replica_servers = [servers_by_host_id[str(host_id)] for host_id, _shard in replicas]

    async def get_leader_host_id() -> uuid.UUID | None:
        for server in replica_servers:
            result = await manager.api.get_raft_leader(server.ip_addr, group_id)
            leader = uuid.UUID(result)
            if leader.int != 0:
                return leader
        return None

    return await wait_for(get_leader_host_id, time.time() + 60)


@pytest.mark.asyncio
async def test_tablet_routing_payload_for_lwt_and_serial_from_wrong_shard(manager: ManagerClient) -> None:
    """
    Reproduces #29874 at the protocol boundary: LWT/SERIAL responses should carry
    TABLETS_ROUTING_V1 feedback when the request entered on a wrong tablet shard.
    """
    servers = await manager.servers_add(1, config={
        "tablets_mode_for_new_keyspaces": "enabled",
        "native_shard_aware_transport_port": SHARD_AWARE_PORT,
    }, cmdline=["--smp", "2"])
    server = servers[0]
    cql = manager.get_cql()

    options = await _supported_options(str(server.rpc_address), manager.port)
    assert "TABLETS_ROUTING_V1" in options
    nr_shards = int(options["SCYLLA_NR_SHARDS"][0])
    assert nr_shards > 1
    shard_aware_port = int(options.get("SCYLLA_SHARD_AWARE_PORT", [SHARD_AWARE_PORT])[0])

    missing_payload = []
    async with new_test_keyspace(
            manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 8}") as keyspace:
        await cql.run_async(f"CREATE TABLE {keyspace}.test_tablet (pk int, ck int, v int, PRIMARY KEY (pk, ck));")
        for pk in range(1, 21):
            await cql.run_async(f"INSERT INTO {keyspace}.test_tablet (pk, ck, v) VALUES ({pk}, 1, 2);")

        select_query = f"SELECT pk, ck, v FROM {keyspace}.test_tablet WHERE pk = ? AND ck = ?"
        regular_has_payload = await _prepared_response_has_tablet_info(
            manager, server, shard_aware_port, nr_shards, keyspace, select_query, [1, 1], pk=1)
        assert regular_has_payload

        cases: list[tuple[str, str, list[int], int, int]] = [
            ("SELECT_SERIAL_PREPARED", select_query, [2, 1], 2, ConsistencyLevel.SERIAL),
            ("UPDATE_LWT_IF_VAL",
             f"UPDATE {keyspace}.test_tablet SET v = ? WHERE pk = ? AND ck = ? IF v = ?",
             [4, 3, 1, 2], 3, ConsistencyLevel.ONE),
        ]

        for name, query, values, pk, consistency in cases:
            has_payload = await _prepared_response_has_tablet_info(
                manager, server, shard_aware_port, nr_shards, keyspace, query, values, pk, consistency)
            if not has_payload:
                missing_payload.append(name)

    assert not missing_payload, f"missing tablet routing payload for {missing_payload}"


@pytest.mark.asyncio
async def test_strong_consistency_write_hint_prefers_leader(manager: ManagerClient) -> None:
    servers = await manager.servers_add(3, config={
        "experimental_features": ["strongly-consistent-tables"],
        "tablets_mode_for_new_keyspaces": "enabled",
        "native_shard_aware_transport_port": SHARD_AWARE_PORT,
    }, cmdline=["--smp", "2"])
    cql = manager.get_cql()

    host_ids = await asyncio.gather(*[manager.get_host_id(server.server_id) for server in servers])
    servers_by_host_id = {str(host_id): server for host_id, server in zip(host_ids, servers)}

    async with new_test_keyspace(
            manager,
            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1} AND consistency = 'global'") as keyspace:
        await cql.run_async(f"CREATE TABLE {keyspace}.test_tablet (pk int PRIMARY KEY, v int);")

        token = await get_token_for_pk(cql, keyspace, "test_tablet", 1)
        replicas = await get_tablet_replicas(manager, servers[0], keyspace, "test_tablet", token)
        assert len(replicas) == 3

        group_id = await _get_table_raft_group_id(manager, keyspace, "test_tablet")
        leader_host_id = await _wait_for_tablet_raft_leader(manager, servers_by_host_id, replicas, group_id)
        leader_replica = next(replica for replica in replicas if str(replica[0]) == str(leader_host_id))
        non_leader_replica = next(replica for replica in replicas if str(replica[0]) != str(leader_host_id))

        non_leader_server = servers_by_host_id[str(non_leader_replica[0])]
        options = await _supported_options(str(non_leader_server.rpc_address), manager.port)
        assert "TABLETS_ROUTING_V1" in options
        nr_shards = int(options["SCYLLA_NR_SHARDS"][0])
        shard_aware_port = int(options.get("SCYLLA_SHARD_AWARE_PORT", [SHARD_AWARE_PORT])[0])

        client = await _connect_to_shard(str(non_leader_server.rpc_address), shard_aware_port, non_leader_replica[1], nr_shards)
        try:
            response = await client.query(
                f"INSERT INTO {keyspace}.test_tablet (pk, v) VALUES (1, 13)",
                ConsistencyLevel.QUORUM)
            payload = _custom_payload(response)
            assert TABLETS_ROUTING_V1_PAYLOAD in payload
            replicas_from_hint = _decode_tablet_routing_replicas(payload[TABLETS_ROUTING_V1_PAYLOAD])
            assert replicas_from_hint[0] == (uuid.UUID(str(leader_replica[0])), leader_replica[1])
        finally:
            await client.close()
