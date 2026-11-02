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
from .rest_api import tablet_balancing_disabled
from .util import new_test_table, paging_plan_rederivations

EXTENSION = "TABLETS_ROUTING_V1"
PAYLOAD_KEY = "tablets-routing-v1"


# Opens a connection to the server, which negotiates the protocol extension.
@pytest.fixture(scope="module")
def connect(request, host, scylla_only):
    if request.config.getoption("--ssl"):
        skip_env("The tests talk to the server over a plain socket")
    port = int(request.config.getoption("--port"))
    with cql_frames.connection(host, port) as sock:
        supported = cql_frames.supported(sock)
    if EXTENSION not in supported:
        skip_env(f"The server doesn't support {EXTENSION}")
    if int(supported["SCYLLA_NR_SHARDS"][0]) < 2:
        skip_env("The tests need a shard other than the one of the partition")

    def open_connection():
        return cql_frames.connection(host, port, {EXTENSION: ""})
    return open_connection


# The shard the connection is on.
def shard_of(sock: socket.socket) -> int:
    return int(cql_frames.supported(sock)["SCYLLA_SHARD"][0])


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


# Whether a successful response carries routing information.
def has_routing_information(response: cql_frames.Response) -> bool:
    assert response.opcode == cql_frames.RESULT
    return PAYLOAD_KEY in response.custom_payload


# A page continues with the query plan of the first page. If the plan of the
# prepared statement has changed since, the server prepares the statement anew
# with the plan of the first page. It's still the statement the driver
# prepared, so the server still returns routing information for it on the
# wrong shard.
def test_routing_information_after_query_plan_change(cql, connect, test_keyspace_tablets):
    if not test_keyspace_tablets:
        skip_env("The test needs a keyspace with tablets")
    # Enough tablets to have some on every shard.
    schema = "pk int, ck int, v int, PRIMARY KEY (pk, ck)"
    extras = "WITH tablets = {'min_tablet_count': 8}"
    with new_test_table(cql, test_keyspace_tablets, schema, extras) as table, connect() as sock:
        with tablet_balancing_disabled(cql):
            # Balancing is off, so the partition stays on the shard it's on now.
            for pk in range(16):
                for ck in range(2):
                    cql.execute(f"INSERT INTO {table} (pk, ck, v) VALUES ({pk}, {ck}, 1)")
            shard = shard_of(sock)
            remote_pks = [pk for pk, pk_shard in partition_shards(cql, table).items() if pk_shard != shard]
            assert remote_pks, f"No partition on a shard other than {shard}"
            pk = remote_pks[0]

            query = f"SELECT ck FROM {table} WHERE pk = ? AND v = ? ALLOW FILTERING"
            statement_id = cql.prepare(query).query_id
            first_page = cql_frames.execute(sock, statement_id, [pk, 1], page_size=1)
            paging_state = cql_frames.paging_state_of(first_page)
            assert paging_state is not None

            # The paging state pins the base table, while the statement, prepared
            # anew, reads the index. The server only returns routing information
            # for reads of the base table.
            cql.execute(f"CREATE INDEX ON {table} ((pk), v)")
            assert cql.prepare(query).query_id == statement_id
            rederivations = paging_plan_rederivations(cql)
            response = cql_frames.execute(sock, statement_id, [pk, 1], page_size=1, paging_state=paging_state)
            assert paging_plan_rederivations(cql) > rederivations
            assert has_routing_information(response)
