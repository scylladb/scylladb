# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the tablets-routing-v2 protocol extension (see
# docs/dev/tablets-routing-v2.md). An EXECUTE request carries a tablet version
# block, and if it doesn't match the version of the tablet, the response
# carries routing information for the tablet.
#
# The Python driver supports the extension, but it chooses the tablet version
# blocks by itself, and it only sends well-formed requests. The tests need to
# choose the blocks, and to send malformed requests, so they send raw CQL
# frames. The server only supports the extension with the
# strongly-consistent-tables experimental feature enabled, which is why the
# tests are in this directory, but the extension applies to all tablet tables.
#############################################################################

import socket
import uuid

import pytest

from test.pylib.skip_types import skip_env

from .. import cql_frames
from ..util import new_test_table


EXTENSION = "TABLETS_ROUTING_V2_EXPERIMENTAL"
PAYLOAD_KEY = "tablets-routing-v2"

CL_QUORUM = 0x0004
VALUES_FLAG = 0x01

PROTOCOL_ERROR = 0x000A


# Opens a connection to the server. It negotiates the protocol extension unless
# told otherwise.
@pytest.fixture(scope="module")
def connect(request, host, scylla_only):
    if request.config.getoption("--ssl"):
        skip_env("The tests talk to the server over a plain socket")
    port = int(request.config.getoption("--port"))
    with cql_frames.connection(host, port) as sock:
        response = cql_frames.request(sock, cql_frames.OPTIONS, b'')
        assert response.opcode == cql_frames.SUPPORTED
        supported = response.body.read_string_multimap()
    if EXTENSION not in supported:
        skip_env(f"The server doesn't support {EXTENSION}")

    def open_connection(negotiate: bool = True):
        return cql_frames.connection(host, port, {EXTENSION: ""} if negotiate else {})
    return open_connection


# Sends an EXECUTE request of a prepared statement with the given int values.
# `block` is the tablet version block, or None to leave it out.
def execute(sock: socket.socket, statement_id: bytes, values: list[int], block: int | None) -> cql_frames.Response:
    body = cql_frames.encode_short_bytes(statement_id)
    body += cql_frames.encode_short(CL_QUORUM)
    body += cql_frames.encode_byte(VALUES_FLAG)
    body += cql_frames.encode_short(len(values))
    body += b''.join(cql_frames.encode_bytes(cql_frames.encode_int(value)) for value in values)
    if block is not None:
        body += cql_frames.encode_byte(block)
    return cql_frames.request(sock, cql_frames.EXECUTE, body)


# The tablets-routing-v2 payload: a tuple (first token: bigint, last token:
# bigint, replicas: list<tuple<uuid, int>>, tablet version: bigint). The tablet
# covers the tokens in (first token, last token].
class RoutingInformation:
    # Reads a replica: a tuple (host ID: uuid, shard: int).
    @staticmethod
    def _read_replica(reader: cql_frames.Reader) -> tuple[uuid.UUID, int]:
        host_id = reader.read_value(cql_frames.Reader.read_uuid)
        shard = reader.read_value(cql_frames.Reader.read_int)
        return host_id, shard

    # Reads a list of replicas: an [int] n, followed by n replicas.
    @staticmethod
    def _read_replicas(reader: cql_frames.Reader) -> list[tuple[uuid.UUID, int]]:
        n = reader.read_int()
        replicas: list[tuple[uuid.UUID, int]] = []
        for _ in range(n):
            replicas.append(reader.read_value(RoutingInformation._read_replica))
        return replicas

    def __init__(self, payload: bytes):
        fields = cql_frames.Reader(payload)
        self.first_token: int = fields.read_value(cql_frames.Reader.read_long)
        self.last_token: int = fields.read_value(cql_frames.Reader.read_long)
        self.replicas: list[tuple[uuid.UUID, int]] = fields.read_value(RoutingInformation._read_replicas)
        self.version: int = fields.read_value(cql_frames.Reader.read_long)
        fields.assert_consumed()


# The routing information of a successful response, or None if it has none.
def routing_information_of(response: cql_frames.Response) -> RoutingInformation | None:
    assert response.opcode == cql_frames.RESULT
    payload = response.custom_payload.get(PAYLOAD_KEY)
    return None if payload is None else RoutingInformation(payload)


# The two blocks differ only in their value nibble, so they can't both match the
# tablet's version: if the server checks the version, at least one of the
# responses carries routing information. Returns the routing information of
# each response, None for none.
def execute_with_both_blocks(sock: socket.socket, statement_id: bytes, values: list[int]) -> list[RoutingInformation | None]:
    infos: list[RoutingInformation | None] = []
    for block in (0x00, 0x01):
        infos.append(routing_information_of(execute(sock, statement_id, values, block)))
    return infos


# The routing information the server returns for the statement.
def routing_information(sock: socket.socket, statement_id: bytes, values: list[int]) -> RoutingInformation:
    infos = execute_with_both_blocks(sock, statement_id, values)
    assert any(info is not None for info in infos)
    return next(info for info in infos if info is not None)


# The last tokens of the tablets of the table.
def tablet_last_tokens(cql, table: str) -> list[int]:
    keyspace, name = table.split(".")
    table_id = cql.execute(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{keyspace}' "
                           f"AND table_name = '{name}'").one().id
    return [row.last_token for row in cql.execute(f"SELECT last_token FROM system.tablets "
                                                  f"WHERE table_id = {table_id}")]


# Without the extension, an EXECUTE request carries no tablet version block, and
# its response carries no routing information.
def test_execute_without_extension(cql, connect, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table, connect(negotiate=False) as sock:
        statement_id = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)").query_id
        assert routing_information_of(execute(sock, statement_id, [1, 1], None)) is None


# With the extension, the tablet version block is mandatory. The server reads
# the block past the end of the request, so it rejects it as truncated.
def test_execute_without_tablet_version_block(cql, connect, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table, connect() as sock:
        statement_id = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)").query_id

        response = execute(sock, statement_id, [1, 1], None)
        assert response.opcode == cql_frames.ERROR
        assert response.body.read_int() == PROTOCOL_ERROR
        assert response.body.read_string() == "truncated frame: expected 1 bytes, length is 0"

        # The same request with a block succeeds, so the rest of it is valid.
        assert execute(sock, statement_id, [1, 1], 0x00).opcode == cql_frames.RESULT


# The routing information is about the tablet of the partition the statement
# accesses. A single node holds all the replicas.
def test_routing_information_payload(cql, connect, test_keyspace, sc_keyspace):
    host_id = cql.execute("SELECT host_id FROM system.local").one().host_id
    for consistency, keyspace in [("eventual", test_keyspace), ("strong", sc_keyspace)]:
        with new_test_table(cql, keyspace, "pk int PRIMARY KEY, v int") as table:
            statement_id = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)").query_id
            with connect() as sock:
                info = routing_information(sock, statement_id, [1, 1])

            # token() needs the partition to exist, and the statement wrote it.
            token = cql.execute(f"SELECT token(pk) FROM {table} WHERE pk = 1").one()[0]
            assert info.first_token < token <= info.last_token, f"{consistency} consistency"
            assert info.last_token in tablet_last_tokens(cql, table), f"{consistency} consistency"
            assert [replica_host_id for replica_host_id, _ in info.replicas] == [host_id], f"{consistency} consistency"


# Checks that the server ignores the tablet version block of a statement: it
# returns no routing information, whatever the block. The statement is on a
# table (pk int, ck int, v int, PRIMARY KEY (pk, ck)) in the keyspace, and {} in
# the query stands for the table.
def assert_no_routing_information(cql, connect, keyspace: str, query: str, values: list[int]):
    with new_test_table(cql, keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table, connect() as sock:
        statement_id = cql.prepare(query.format(table)).query_id
        assert execute_with_both_blocks(sock, statement_id, values) == [None, None]


# A vnode-based table has no tablets, so the server ignores the tablet version
# block.
def test_no_routing_information_for_vnodes(cql, connect, test_keyspace_vnodes):
    assert_no_routing_information(cql, connect, test_keyspace_vnodes,
                                  "INSERT INTO {} (pk, ck, v) VALUES (?, ?, ?)", [1, 1, 1])


# A range scan doesn't access a single tablet, so the server ignores the tablet
# version block.
def test_no_routing_information_for_range_scan(cql, connect, test_keyspace):
    assert_no_routing_information(cql, connect, test_keyspace, "SELECT * FROM {}", [])


# The partition key is a literal, so the driver can't compute the token, and the
# server ignores the tablet version block.
def test_no_routing_information_for_literal_partition_key(cql, connect, test_keyspace):
    assert_no_routing_information(cql, connect, test_keyspace,
                                  "INSERT INTO {} (pk, ck, v) VALUES (1, ?, ?)", [1, 1])
