# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for the tablets-routing-v1 protocol extension. If a request that the
# driver could route by token reaches a shard other than the one of the
# partition, the response carries routing information for the tablet.
#
# The Python driver would route the requests to the right shard by itself. The
# tests need to send them to the wrong one, so they send raw CQL frames.
#############################################################################

import socket

import pytest

from test.pylib.skip_types import skip_env

from . import cql_frames
from .util import ScyllaMetrics, new_test_table

EXTENSION = "TABLETS_ROUTING_V1"
PAYLOAD_KEY = "tablets-routing-v1"

CL_QUORUM = 0x0004
VALUES_FLAG = 0x01
PAGE_SIZE_FLAG = 0x04
PAGING_STATE_FLAG = 0x08

ROWS = 0x0002
HAS_MORE_PAGES_FLAG = 0x0002


# Opens a connection to the server, which negotiates the protocol extension.
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
    if int(supported["SCYLLA_NR_SHARDS"][0]) < 2:
        skip_env("The tests need a shard other than the one of the partition")

    def open_connection():
        return cql_frames.connection(host, port, {EXTENSION: ""})
    return open_connection


# The shard the connection is on.
def shard_of(sock: socket.socket) -> int:
    response = cql_frames.request(sock, cql_frames.OPTIONS, b'')
    assert response.opcode == cql_frames.SUPPORTED
    return int(response.body.read_string_multimap()["SCYLLA_SHARD"][0])


# The shard of each partition of the table with an int partition key pk, by the
# partition key. A single node holds all the replicas.
def partition_shards(cql, table: str) -> dict[int, int]:
    keyspace, name = table.split(".")
    table_id = cql.execute(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{keyspace}' "
                           f"AND table_name = '{name}'").one().id
    # A tablet covers the tokens up to its last token, from the last token of
    # the previous one.
    rows = cql.execute(f"SELECT last_token, replicas FROM system.tablets WHERE table_id = {table_id}")
    tablets = sorted((row.last_token, row.replicas[0][1]) for row in rows)
    shards: dict[int, int] = {}
    for pk, token in cql.execute(f"SELECT pk, token(pk) FROM {table}"):
        shards[pk] = next(shard for last_token, shard in tablets if token <= last_token)
    return shards


# Sends an EXECUTE request of a prepared statement with the given int values.
# The result is paged if `page_size` is given, and continues from
# `paging_state` if given.
def execute(sock: socket.socket, statement_id: bytes, values: list[int],
            page_size: int | None = None, paging_state: bytes | None = None) -> cql_frames.Response:
    flags = VALUES_FLAG
    if page_size is not None:
        flags |= PAGE_SIZE_FLAG
    if paging_state is not None:
        flags |= PAGING_STATE_FLAG
    body = cql_frames.encode_short_bytes(statement_id)
    body += cql_frames.encode_short(CL_QUORUM) + cql_frames.encode_byte(flags)
    body += cql_frames.encode_short(len(values))
    body += b''.join(cql_frames.encode_bytes(cql_frames.encode_int(value)) for value in values)
    if page_size is not None:
        body += cql_frames.encode_int(page_size)
    if paging_state is not None:
        body += cql_frames.encode_bytes(paging_state)
    return cql_frames.request(sock, cql_frames.EXECUTE, body)


# Whether a successful response carries routing information.
def has_routing_information(response: cql_frames.Response) -> bool:
    assert response.opcode == cql_frames.RESULT
    return PAYLOAD_KEY in response.custom_payload


# The paging state of a Rows result, or None if it's the last page.
def paging_state_of(response: cql_frames.Response) -> bytes | None:
    assert response.opcode == cql_frames.RESULT
    assert response.body.read_int() == ROWS
    flags = response.body.read_int()
    response.body.read_int()  # The column count.
    return response.body.read_bytes() if flags & HAS_MORE_PAGES_FLAG else None


# The number of pages so far whose statement was prepared anew with the query
# plan their paging state pins.
def paging_plan_rederivations(cql) -> int:
    return int(ScyllaMetrics.query(cql).get("scylla_cql_select_paging_plan_rederivations") or 0)


# A page continues with the query plan of the first page. If the plan of the
# prepared statement has changed since, the server prepares the statement anew
# with the plan of the first page. It's still the statement the driver
# prepared, so the server still returns routing information for it on the
# wrong shard.
def test_routing_information_after_query_plan_change(cql, connect, test_keyspace_tablets):
    if not test_keyspace_tablets:
        skip_env("The test needs a keyspace with tablets")
    # Enough tablets to have some on every shard.
    with new_test_table(cql, test_keyspace_tablets, "pk int, ck int, v int, PRIMARY KEY (pk, ck)",
                        "WITH tablets = {'min_tablet_count': 8}") as table, connect() as sock:
        for pk in range(16):
            for ck in range(2):
                cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES ({pk}, {ck}, 1)")
        shard = shard_of(sock)
        query = f"SELECT ck FROM {table} WHERE pk = ? AND v = ? ALLOW FILTERING"
        statement_id = cql.prepare(query).query_id

        # The paging state of the first page of each partition on another shard.
        paging_states: dict[int, bytes] = {}
        for pk, pk_shard in partition_shards(cql, table).items():
            if pk_shard != shard:
                paging_state = paging_state_of(execute(sock, statement_id, [pk, 1], page_size=1))
                assert paging_state is not None
                paging_states[pk] = paging_state
        assert paging_states, f"No partition on a shard other than {shard}"

        # The paging state pins the base table, while the statement, prepared
        # anew, reads the index. The server only returns routing information
        # for reads of the base table.
        cql.execute(f"CREATE INDEX ON {table} ((pk), v)")
        assert cql.prepare(query).query_id == statement_id

        # Load balancing may move a partition to the shard of the connection,
        # so the page is requested for the first partition that's on another
        # shard both before and after the request.
        for pk, paging_state in paging_states.items():
            if partition_shards(cql, table)[pk] == shard:
                continue
            rederivations = paging_plan_rederivations(cql)
            response = execute(sock, statement_id, [pk, 1], page_size=1, paging_state=paging_state)
            if partition_shards(cql, table)[pk] == shard:
                continue
            assert paging_plan_rederivations(cql) > rederivations
            assert has_routing_information(response)
            return
        pytest.fail(f"All partitions moved to shard {shard}")
