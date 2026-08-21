# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import pytest
import time
from . import nodetool
from .util import new_test_table
from cassandra.cluster import NoHostAvailable
from cassandra.query import SimpleStatement, BatchStatement, BatchType

def test_enable_disable_binary(cql, test_keyspace):
    schema = 'k text, v text, primary key (k)'
    with new_test_table(cql, test_keyspace, schema) as table:
        cql.execute(f"INSERT INTO {table} (k,v) VALUES ('foo', 'bar')")
        cql.execute(f"SELECT * FROM {table}")

        nodetool.disablebinary(cql)
        with pytest.raises(NoHostAvailable):
            cql.execute(f"SELECT * FROM {table}")

        nodetool.enablebinary(cql)
        pause = 0.1
        while pause < 100:
            try:
                cql.execute(f"SELECT * FROM {table}")
                break
            except NoHostAvailable:
                time.sleep(pause)
                pause += pause
        else:
            assert False


def test_query_with_custom_payload(cql):
    cql.execute(
        SimpleStatement("SELECT * FROM system.local"),
        custom_payload={"request-id": b"test-request-id"}
    )


def test_execute_prepared_with_custom_payload(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v text") as table:
        prepared = cql.prepare(f"INSERT INTO {table} (p, v) VALUES (?, ?)")
        cql.execute(
            prepared.bind([1, "hello"]),
            custom_payload={"request-id": b"test-request-id"}
        )


def test_batch_with_custom_payload(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v text") as table:
        batch = BatchStatement(batch_type=BatchType.UNLOGGED)
        batch.add(SimpleStatement(f"INSERT INTO {table} (p, v) VALUES (1, 'a')"))
        batch.add(SimpleStatement(f"INSERT INTO {table} (p, v) VALUES (2, 'b')"))
        cql.execute(batch, custom_payload={"request-id": b"test-request-id"})


def test_request_with_multiple_payload_entries(cql):
    cql.execute(
        SimpleStatement("SELECT * FROM system.local"),
        custom_payload={
            "key-one": b"value-one",
            "key-two": b"\xde\xad\xbe\xef",
            "key-three": b"",
        }
    )


def test_request_with_large_payload_value(cql):
    cql.execute(
        SimpleStatement("SELECT * FROM system.local"),
        custom_payload={"large-key": b"x" * 8192}
    )


def test_custom_payload_does_not_affect_query_result(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v text") as table:
        cql.execute(f"INSERT INTO {table} (p, v) VALUES (1, 'hello')")
        cql.execute(f"INSERT INTO {table} (p, v) VALUES (2, 'world')")

        without = sorted(cql.execute(f"SELECT p, v FROM {table}"))
        with_payload = sorted(cql.execute(
            SimpleStatement(f"SELECT p, v FROM {table}"),
            custom_payload={
                "key-a": b"A" * 100,
                "key-b": b"B" * 200,
            }
        ))
        assert without == with_payload
