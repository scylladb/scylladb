# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

# Tests for cases that overflow the grammar or cause overly complex
# expressions that later consume too much time during the analysis phase.

import pytest
from cassandra.protocol import SyntaxException, InvalidRequest
from .util import new_test_table


@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY, v bigint") as table:
        cql.execute(f"INSERT INTO {table} (p, v) VALUES (1, 1)")
        yield table


def nested_function_selector(depth):
    """Build blobasbigint(bigintasblob(blobasbigint(bigintasblob(... v ...))))

    Uses alternating bigintasblob/blobasbigint so that every level is
    type-correct:  bigint -> blob -> bigint -> blob -> ...
    """
    depth //= 2
    return "blobasbigint(bigintasblob(" * depth + "v" + "))" * depth

def nested_cast_selector(depth):
    """Build CAST(CAST(CAST(... CAST(v AS bigint) ... AS bigint) AS bigint) AS bigint)"""
    return f"CAST(" * depth + "v" + f" AS bigint)" * depth

def nested_function_term(depth):
    """Build blobasbigint(bigintasblob(... blobasbigint(bigintasblob((bigint)1)) ...))

    The innermost expression is (bigint)1 (a bigint value).
    Even depths end with blob type, odd depths end with bigint type.
    We always make the outermost return bigint so it matches the column type.
    """
    depth //= 2
    return "blobasbigint(bigintasblob(" * depth + "(bigint)1" + "))" * depth

def nested_c_cast_term(depth):
    """Build (bigint)(bigint)...(bigint)1"""
    return "(bigint)" * depth + "1"

def nested_relation(depth):
    """Build (((... p=1 ...)))"""
    return "(" * depth + "p=1" + ")" * depth

# The default max_function_call_nesting is 12.  Use a depth large enough
# to overflow the evaluator stack before the fix is in place.
DEPTH = 100000

SHALLOW_DEPTH = 10

def test_deeply_nested_function_in_selector(cql, table1, scylla_only):
    """Deeply nested function calls in a SELECT selector must be rejected."""
    selector = nested_function_selector(DEPTH)
    with pytest.raises(SyntaxException):
        cql.execute(f"SELECT {selector} FROM {table1}")
    # see that shallow nesting is accepted
    selector = nested_function_selector(SHALLOW_DEPTH)
    cql.execute(f"SELECT {selector} FROM {table1}")


def test_deeply_nested_cast_in_selector(cql, table1, scylla_only):
    """Deeply nested CAST() in a SELECT selector must be rejected."""
    selector = nested_cast_selector(DEPTH)
    with pytest.raises(SyntaxException):
        cql.execute(f"SELECT {selector} FROM {table1}")
    # see that shallow nesting is accepted
    selector = nested_cast_selector(SHALLOW_DEPTH)
    cql.execute(f"SELECT {selector} FROM {table1}")


def test_deeply_nested_function_in_term(cql, table1, scylla_only):
    """Deeply nested function calls in a WHERE term must be rejected."""
    term = nested_function_term(DEPTH)
    with pytest.raises(SyntaxException):
        cql.execute(f"SELECT * FROM {table1} WHERE v = {term} ALLOW FILTERING")
    # see that shallow nesting is accepted
    term = nested_function_term(SHALLOW_DEPTH-1) # -1 because the cast adds one more level of nesting, see nested_function_term()
    cql.execute(f"SELECT * FROM {table1} WHERE v = {term} ALLOW FILTERING")


def test_deeply_nested_c_cast_in_term(cql, table1, scylla_only):
    """Deeply nested C-style casts in a WHERE term must be rejected."""
    term = nested_c_cast_term(DEPTH)
    with pytest.raises(SyntaxException):
        cql.execute(f"SELECT * FROM {table1} WHERE v = {term} ALLOW FILTERING")
    # see that shallow nesting is accepted
    term = nested_c_cast_term(SHALLOW_DEPTH)
    cql.execute(f"SELECT * FROM {table1} WHERE v = {term} ALLOW FILTERING")

def test_deeply_nested_relation(cql, table1, scylla_only):
    """Deeply nested parentheses in a WHERE relation must be rejected."""
    relation = nested_relation(DEPTH)
    with pytest.raises(SyntaxException):
        cql.execute(f"SELECT * FROM {table1} WHERE {relation} ALLOW FILTERING")
    # see that shallow nesting is accepted
    relation = nested_relation(SHALLOW_DEPTH)
    cql.execute(f"SELECT * FROM {table1} WHERE {relation} ALLOW FILTERING")

def test_lots_of_opening_paren_not_closed(cql, table1, scylla_only):
    """An opening parenthesis with no closing parenthesis must be rejected."""
    with pytest.raises(SyntaxException):
        cql.execute(f"SELECT * FROM {table1} WHERE " + "(" * DEPTH)

# The default max_relations_in_where_clause is 100.
OVER_LIMIT = 200

def make_where_clause(n):
    """Build a WHERE clause with n relations: p = 1 AND v = 1 AND v = 1 ..."""
    return "p = 1" + " AND v = 1" * (n - 1)


@pytest.mark.skip_bug("https://scylladb.atlassian.net/browse/SCYLLADB-1002")
def test_too_many_relations_in_where_clause(cql, table1, scylla_only):
    """A WHERE clause with too many relations must be rejected."""
    where = make_where_clause(OVER_LIMIT)
    with pytest.raises(SyntaxException):
        cql.execute(f"SELECT * FROM {table1} WHERE {where} ALLOW FILTERING")


def test_reasonable_number_of_relations_allowed(cql, table1, scylla_only):
    """A WHERE clause within the limit should be accepted."""
    where = make_where_clause(50)
    # Should not raise - we just need it to parse successfully.
    # The query itself may return no rows, that's fine.
    cql.execute(f"SELECT * FROM {table1} WHERE {where} ALLOW FILTERING")
