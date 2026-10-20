# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the tablets-routing-v2 protocol extension (see
# docs/dev/tablets-routing-v2.md). An EXECUTE request carries a tablet version
# block, and if it doesn't match the version of the tablet, the response
# carries routing information for the tablet. A BATCH request carries a routing
# hint (see docs/dev/tablets-routing-v2-batches.md).
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
from ..rest_api import message_injection, scylla_inject_error, wait_for_injection_enter
from ..util import ScyllaMetrics, new_test_keyspace, new_test_table, unique_name

EXTENSION = "TABLETS_ROUTING_V2_EXPERIMENTAL_V2"
PAYLOAD_KEY = "tablets-routing-v2"
NO_ROUTING_STATEMENT = 0xFFFF

CL_ONE = 0x0001
CL_QUORUM = 0x0004
VALUES_FLAG = 0x01
PAGE_SIZE_FLAG = 0x04
PAGING_STATE_FLAG = 0x08

ROWS = 0x0002
HAS_MORE_PAGES_FLAG = 0x0002

PROTOCOL_ERROR = 0x000A

# The two blocks differ only in their value nibble, so they can't both match the
# tablet's version: if the server checks the version, at least one of the
# responses carries routing information.
BLOCKS = (0x00, 0x01)

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
# `block` is the tablet version block, or None to leave it out. The result is
# paged if `page_size` is given, and continues from `paging_state` if given.
def execute(sock: socket.socket, statement_id: bytes, values: list[int], block: int | None,
            page_size: int | None = None, paging_state: bytes | None = None) -> cql_frames.Response:
    flags = VALUES_FLAG
    if page_size is not None:
        flags |= PAGE_SIZE_FLAG
    if paging_state is not None:
        flags |= PAGING_STATE_FLAG
    body = cql_frames.encode_short_bytes(statement_id)
    body += cql_frames.encode_short(CL_QUORUM)
    body += cql_frames.encode_byte(flags)
    body += cql_frames.encode_short(len(values))
    body += b''.join(cql_frames.encode_bytes(cql_frames.encode_int(value)) for value in values)
    if page_size is not None:
        body += cql_frames.encode_int(page_size)
    if paging_state is not None:
        body += cql_frames.encode_bytes(paging_state)
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


# The routing information of a successful response, or None if it has none.
def routing_information_of(response: cql_frames.Response) -> RoutingInformation | None:
    assert response.opcode == cql_frames.RESULT
    payload = response.custom_payload.get(PAYLOAD_KEY)
    return None if payload is None else RoutingInformation(payload)


# The paging state of a Rows result, or None if it's the last page.
def paging_state_of(response: cql_frames.Response) -> bytes | None:
    assert response.opcode == cql_frames.RESULT
    assert response.body.read_int() == ROWS
    flags = response.body.read_int()
    response.body.read_int()  # The column count.
    return response.body.read_bytes() if flags & HAS_MORE_PAGES_FLAG else None


# Executes the statement with each of BLOCKS. Returns the routing information of
# each response, None for none. The paging arguments are as in execute().
def execute_with_both_blocks(sock: socket.socket, statement_id: bytes, values: list[int],
                             page_size: int | None = None,
                             paging_state: bytes | None = None) -> list[RoutingInformation | None]:
    infos: list[RoutingInformation | None] = []
    for block in BLOCKS:
        response = execute(sock, statement_id, values, block, page_size, paging_state)
        infos.append(routing_information_of(response))
    return infos


# The routing information the server returns for the statement.
def routing_information(sock: socket.socket, statement_id: bytes, values: list[int]) -> RoutingInformation:
    infos = execute_with_both_blocks(sock, statement_id, values)
    assert any(info is not None for info in infos)
    return next(info for info in infos if info is not None)


# The block with the given index of a tablet version.
def tablet_version_block(version: int, index: int) -> int:
    return (index << 4) | ((version >> (4 * index)) & 0xF)


# The last tokens of the tablets of the table.
def tablet_last_tokens(cql, table: str) -> list[int]:
    keyspace, name = table.split(".")
    table_id = cql.execute(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{keyspace}' "
                           f"AND table_name = '{name}'").one().id
    return [row.last_token for row in cql.execute(f"SELECT last_token FROM system.tablets "
                                                  f"WHERE table_id = {table_id}")]


# The number of pages so far whose statement was prepared anew with the query
# plan their paging state pins.
def paging_plan_rederivations(cql) -> int:
    return int(ScyllaMetrics.query(cql).get("scylla_cql_select_paging_plan_rederivations") or 0)


# A statement of a BATCH request: query text, or the ID of a prepared
# statement, with its int values.
def batch_statement(query_or_id: str | bytes, values: list[int]) -> bytes:
    if isinstance(query_or_id, str):
        statement = cql_frames.encode_byte(0)
        statement += cql_frames.encode_long_string(query_or_id)
    else:
        statement = cql_frames.encode_byte(1)
        statement += cql_frames.encode_short_bytes(query_or_id)
    statement += cql_frames.encode_short(len(values))
    return statement + b''.join(cql_frames.encode_bytes(cql_frames.encode_int(value)) for value in values)


# Sends a LOGGED BATCH request without waiting for the response. `hint` is a
# pair of the routing statement index and the tablet version block, or None to
# leave out the routing hint.
def send_batch(sock: socket.socket, statements: list[bytes], cl: int, hint: tuple[int, int] | None) -> None:
    body = cql_frames.encode_byte(0) # LOGGED
    body += cql_frames.encode_short(len(statements))
    body += b''.join(statements)
    body += cql_frames.encode_short(cl)
    body += cql_frames.encode_byte(0) # No flags.
    if hint is not None:
        index, block = hint
        body += cql_frames.encode_short(index)
        body += cql_frames.encode_byte(block)
    cql_frames.send_frame(sock, opcode=cql_frames.BATCH, stream=3, body=body)


# Sends a LOGGED BATCH request, like send_batch(), and returns the response.
def batch(sock: socket.socket, statements: list[bytes], cl: int, hint: tuple[int, int] | None) -> cql_frames.Response:
    send_batch(sock, statements, cl, hint)
    return cql_frames.read_frame(sock)


# Sends the batch with each of BLOCKS. Returns the routing information of each
# response, None for none.
def batch_with_both_blocks(sock: socket.socket, statements: list[bytes], cl: int,
                           routing_statement_index: int) -> list[RoutingInformation | None]:
    infos: list[RoutingInformation | None] = []
    for block in BLOCKS:
        response = batch(sock, statements, cl, (routing_statement_index, block))
        infos.append(routing_information_of(response))
    return infos


def assert_protocol_error(response: cql_frames.Response) -> None:
    assert response.opcode == cql_frames.ERROR
    assert response.body.read_int() == PROTOCOL_ERROR


# Statements of the batches of test_routing_information, with their values. {}
# stands for the table. The first statement of every batch binds the partition
# key, the second one gives it as a literal.
INSERT_BOUND_PK = ("INSERT INTO {} (pk, ck, v) VALUES (?, 1, ?)", [1, 1])
INSERT_LITERAL_PK = ("INSERT INTO {} (pk, ck, v) VALUES (1, 2, ?)", [1])
INSERT_OTHER_PARTITION = ("INSERT INTO {} (pk, ck, v) VALUES (2, 2, ?)", [1])
INSERT_BOUND_PK_IF = ("INSERT INTO {} (pk, ck, v) VALUES (?, 1, ?) IF NOT EXISTS", [1, 1])
UPDATE_LITERAL_PK_IF = ("UPDATE {} SET v = ? WHERE pk = 1 AND ck = 2 IF EXISTS", [1])


# A case of test_routing_information. `consistency` is "ec" or "sc", and
# `prepared` tells, for each statement, whether it's sent as the ID of a
# prepared statement rather than as query text.
def routing_case(case_id: str, consistency: str, statements: list[tuple[str, list[int]]], routing_statement_index: int,
                 routing_information: bool, prepared: tuple[bool, bool] = (True, True)):
    return pytest.param(consistency, statements, routing_statement_index, routing_information, prepared, id=case_id)


ROUTING_CASES = [
    routing_case("sc", "sc", [INSERT_BOUND_PK, INSERT_LITERAL_PK], 0, True),
    routing_case("sc-unrouted", "sc", [INSERT_BOUND_PK, INSERT_LITERAL_PK], NO_ROUTING_STATEMENT, False),
    routing_case("ec", "ec", [INSERT_BOUND_PK, INSERT_LITERAL_PK], 0, True),
    routing_case("ec-unrouted", "ec", [INSERT_BOUND_PK, INSERT_LITERAL_PK], NO_ROUTING_STATEMENT, False),
    # A batch may mix prepared statements and query text.
    routing_case("sc-mixed", "sc", [INSERT_BOUND_PK, INSERT_LITERAL_PK], 0, True, (True, False)),
    routing_case("ec-mixed", "ec", [INSERT_BOUND_PK, INSERT_LITERAL_PK], 0, True, (True, False)),
    # An eventually consistent batch may span multiple partitions.
    routing_case("ec-multi-partition", "ec", [INSERT_BOUND_PK, INSERT_OTHER_PARTITION], 0, True),
    routing_case("ec-multi-partition-unrouted", "ec", [INSERT_BOUND_PK, INSERT_OTHER_PARTITION], NO_ROUTING_STATEMENT, False),
    # An eventually consistent batch may be conditional.
    routing_case("ec-conditional", "ec", [INSERT_BOUND_PK_IF, UPDATE_LITERAL_PK_IF], 0, True),
    routing_case("ec-conditional-unrouted", "ec", [INSERT_BOUND_PK_IF, UPDATE_LITERAL_PK_IF], NO_ROUTING_STATEMENT, False),
    routing_case("ec-conditional-mixed", "ec", [INSERT_BOUND_PK_IF, UPDATE_LITERAL_PK_IF], 0, True, (True, False)),
    routing_case("ec-conditional-text", "ec", [INSERT_BOUND_PK_IF, UPDATE_LITERAL_PK_IF], NO_ROUTING_STATEMENT, False, (False, False)),
]


# Without the extension, an EXECUTE request carries no tablet version block, and
# its response carries no routing information.
def test_execute_without_extension(cql, connect, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table, connect(negotiate=False) as sock:
        statement_id = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)").query_id
        assert routing_information_of(execute(sock, statement_id, [1, 1], None)) is None


# With the extension, the tablet version block is mandatory.
def test_execute_without_tablet_version_block(cql, connect, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table, connect() as sock:
        statement_id = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)").query_id
        assert_protocol_error(execute(sock, statement_id, [1, 1], None))


# Whichever block of the tablet version the driver sends, the server returns
# routing information if and only if the block doesn't match it.
@pytest.mark.parametrize("query,values", [
    pytest.param("INSERT INTO {} (pk, ck, v) VALUES (?, ?, ?)", [1, 1, 1], id="insert"),
    pytest.param("SELECT * FROM {} WHERE pk = ?", [1], id="select"),
])
def test_tablet_version_block(cql, connect, test_keyspace, sc_keyspace, query, values):
    for consistency, keyspace in [("eventual", test_keyspace), ("strong", sc_keyspace)]:
        with new_test_table(cql, keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table, connect() as sock:
            statement_id = cql.prepare(query.format(table)).query_id
            version = routing_information(sock, statement_id, values).version
            for index in range(16):
                block = tablet_version_block(version, index)
                info = routing_information_of(execute(sock, statement_id, values, block))
                assert info is None, f"{consistency} consistency, block {block:#04x}"
                # Same index, another value.
                info = routing_information_of(execute(sock, statement_id, values, block ^ 0x01))
                assert info is not None, f"{consistency} consistency, block {block ^ 0x01:#04x}"
                assert info.version == version, f"{consistency} consistency"


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


# A page continues with the query plan of the first page. If the plan of the
# prepared statement has changed since, the server prepares the statement anew
# with the plan of the first page. It's still the statement the driver
# prepared, so the server still checks the tablet version block.
def test_routing_information_after_query_plan_change(cql, connect, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table, connect() as sock:
        for ck in range(2):
            cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES (1, {ck}, 1)")
        query = f"SELECT ck FROM {table} WHERE pk = ? AND v = ? ALLOW FILTERING"
        statement_id = cql.prepare(query).query_id
        version = routing_information(sock, statement_id, [1, 1]).version
        first_page = execute(sock, statement_id, [1, 1], tablet_version_block(version, 0), page_size=1)
        paging_state = paging_state_of(first_page)
        assert paging_state is not None

        # The paging state pins the base table, while the statement, prepared
        # anew, reads the index. The server only returns routing information
        # for reads of the base table.
        cql.execute(f"CREATE INDEX ON {table} ((pk), v)")
        assert cql.prepare(query).query_id == statement_id
        rederivations = paging_plan_rederivations(cql)
        infos = execute_with_both_blocks(sock, statement_id, [1, 1], page_size=1, paging_state=paging_state)
        assert paging_plan_rederivations(cql) > rederivations
        assert any(info is not None for info in infos)


# The routing statement must be one the driver could compute the token from:
# a statement sent as the ID of a prepared statement with partition key bind
# indices. Any other routing statement index, or a missing hint, is a protocol
# error.
def test_invalid_routing_hint(cql, connect, test_keyspace):
    with new_test_table(cql, test_keyspace, "pk int PRIMARY KEY, v int") as table, connect() as sock:
        statements = [
            batch_statement(cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)").query_id, [1, 1]),
            # The partition key is a literal, so the driver can't compute the token.
            batch_statement(cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (1, ?)").query_id, [1]),
            # The driver has no partition key bind indices for query text.
            batch_statement(f"INSERT INTO {table} (pk, v) VALUES (?, ?)", [1, 1]),
        ]

        # 3 is past the end of the batch.
        for hint in [(1, 0), (2, 0), (3, 0), None]:
            assert_protocol_error(batch(sock, statements, CL_ONE, hint))
        # An empty batch has no statement to route by.
        assert_protocol_error(batch(sock, [], CL_ONE, (0, 0)))

        for hint in [(0, 0), (NO_ROUTING_STATEMENT, 0)]:
            assert batch(sock, statements, CL_ONE, hint).opcode == cql_frames.RESULT
        assert batch(sock, [], CL_ONE, (NO_ROUTING_STATEMENT, 0)).opcode == cql_frames.RESULT


# A BATCH request gets routing information if and only if it names a routing
# statement, whatever the consistency of the table.
@pytest.mark.parametrize("consistency,statements,routing_statement_index,routing_information,prepared", ROUTING_CASES)
def test_routing_information(cql, connect, test_keyspace, sc_keyspace, consistency, statements, routing_statement_index,
                             routing_information, prepared):
    keyspace = sc_keyspace if consistency == "sc" else test_keyspace
    with new_test_table(cql, keyspace, "pk int, ck int, v int, PRIMARY KEY (pk, ck)") as table, connect() as sock:
        batch_statements = []
        for (query, values), send_prepared in zip(statements, prepared):
            query = query.format(table)
            batch_statements.append(batch_statement(cql.prepare(query).query_id if send_prepared else query, values))
        infos = batch_with_both_blocks(sock, batch_statements, CL_QUORUM, routing_statement_index)
    assert any(info is not None for info in infos) == routing_information


# The routing statement of a BATCH request may be any statement of the batch,
# and the routing information is about the tablet of the partition it writes.
def test_routing_information_of_routing_statement(cql, connect, has_tablets):
    if not has_tablets:
        skip_env("Routing information is about tablets")
    keyspace_options = ("WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} "
                        "AND tablets = {'initial': 4}")
    # The tablets of an empty table could be merged while the test runs.
    table_options = " WITH tablets = {'min_tablet_count': '4'}"
    with new_test_keyspace(cql, keyspace_options) as keyspace, \
            new_test_table(cql, keyspace, "pk int PRIMARY KEY, v int", table_options) as table, \
            connect() as sock:
        last_tokens = tablet_last_tokens(cql, table)

        # A tablet is identified by its last token.
        def tablet_of(pk):
            # token() needs the partition to exist.
            cql.execute(f"INSERT INTO {table} (pk, v) VALUES ({pk}, 0)")
            token = cql.execute(f"SELECT token(pk) FROM {table} WHERE pk = {pk}").one()[0]
            return min(last_token for last_token in last_tokens if last_token >= token)

        # The last statement is the routing statement. The others write a partition
        # of another tablet, so the routing information tells which statement the
        # batch was routed by.
        routing_pk = 0
        other_pk = 1
        while tablet_of(other_pk) == tablet_of(routing_pk):
            other_pk += 1

        insert = cql.prepare(f"INSERT INTO {table} (pk, v) VALUES (?, ?)").query_id
        statements = [batch_statement(insert, [other_pk, 1]) for _ in range(3)]
        statements.append(batch_statement(insert, [routing_pk, 1]))
        infos = [info for info in batch_with_both_blocks(sock, statements, CL_ONE, 3) if info is not None]
        assert infos
        for info in infos:
            assert info.last_token == tablet_of(routing_pk)


# A BATCH request gets routing information once the batch has succeeded. If the
# table has been dropped by then, it gets none.
@pytest.mark.parametrize("conditional", [False, True], ids=["regular", "conditional"])
def test_routing_information_of_dropped_table(cql, connect, test_keyspace, conditional):
    injection = "batch_statement_before_routing_info"
    table = f"{test_keyspace}.{unique_name()}"
    cql.execute(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")
    try:
        insert = f"INSERT INTO {table} (pk, v) VALUES (?, ?)" + (" IF NOT EXISTS" if conditional else "")
        statements = [batch_statement(cql.prepare(insert).query_id, [1, 1])]
        with connect() as first, connect() as second, scylla_inject_error(cql, injection):
            # Each batch gets one of BLOCKS, so if the table still existed, at
            # least one of them would get routing information.
            for sock, block in zip((first, second), BLOCKS):
                send_batch(sock, statements, CL_QUORUM, (0, block))

            # Both batches have been written and wait to compute their routing information.
            wait_for_injection_enter(cql, injection, count=2)
            cql.execute(f"DROP TABLE {table}")
            message_injection(cql, injection)

            for sock in (first, second):
                assert routing_information_of(cql_frames.read_frame(sock)) is None
    finally:
        cql.execute(f"DROP TABLE IF EXISTS {table}")
