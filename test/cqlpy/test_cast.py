# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

###############################################################################
# Tests for CQL casting, e.g `blob_column = (blob)(int)123`
###############################################################################

# CQL supports type casting using C-style casts, although it's pretty limited.
# We only allow such casts between types that have a compatible binary representation.
# Compatible means that the bytes will stay unchanged after the conversion.
# This means that it's legal to cast an int to blob (int is just a 4 byte blob),
# but it's illegal to cast a bigint to int (change 4 bytes -> 8 bytes).
# This simplifies things, to cast we can just reinterpret the value as the other type.

# Another useful use of C-style casts is type hints.
# Sometimes it's impossible to infer the exact type of an expression from the context.
# In such cases the type can be specified by casting the expression to this type.
# For example: `overloadedFunction((int)?)`
# Without the cast it's impossible to guess what should be the bind marker's type.
# The function is overloaded, so there are many possible argument types.
# The type hint specifies that the bind marker has type int.

# An interesting thing is that such casts don't have to be explicit.
# CQL allows to put an int value in a place where a blob value is expected
# and it will be automatically converted without any explicit casting.

# Scylla's support for these casts is richer than Cassandra's, that's
# why some of the tests are marked scylla_only.
# For example Scylla allows expressions like `blob_col = (blob)(int)123`,
# but Cassandra rejects them.

import pytest
from cassandra.protocol import InvalidRequest
from util import unique_name, unique_key_int
import uuid

@pytest.fixture(scope="module")
def table1(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (pk int PRIMARY KEY, blob_col blob, int_col int, bigint_col bigint)")
    yield table
    cql.execute("DROP TABLE " + table)

@pytest.fixture(scope="module")
def table2(cql, test_keyspace):
    table = test_keyspace + "." + unique_name()
    cql.execute(f"CREATE TABLE {table} (pk int PRIMARY KEY, d date)")
    yield table
    cql.execute("DROP TABLE " + table)

# Implicitly casting an integer constant to blob fails, it's unknown what the exact type is - is it tinyint, bigint?
# It's important that it stays this way - in the future we might implement guessing the type of untyped constants
# by assigning the smallest type that can fit the constant, but I think that we shouldn't allow converting them
# to blob without explicitly specifying size of the integer.
def test_cast_int_literal_to_blob(cql, table1):
    pk = unique_key_int()
    with pytest.raises(InvalidRequest, match='blob'):
        cql.execute(f"INSERT INTO {table1} (pk, blob_col) VALUES ({pk}, 12)")

# Putting (blob) before the integer also fails, it's still unknown what the exact type is.
def test_cast_int_literal_to_blob_with_blob_cast(cql, table1):
    pk = unique_key_int()
    with pytest.raises(InvalidRequest, match='blob'):
        cql.execute(f"INSERT INTO {table1} (pk, blob_col) VALUES ({pk}, (blob)123)")

# Putting (int) before the integer specifies the exact type and it's possible to cast the value.
def test_cast_int_literal_with_type_hint_to_blob(cql, table1, scylla_only):
    pk = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (pk, blob_col) VALUES ({pk}, (int)1234)")
    assert list(cql.execute(f"SELECT pk, blob_col FROM {table1} WHERE pk = {pk}")) == [(pk, int(1234).to_bytes(4, 'big'))]

# Converting an int to blob is allowed, but converting a blob to int isn't.
# An int can always be converted to a valid blob, but blobs might have wrong amount of bytes
# and can't be converted to a valid int.
def test_cast_blob_literal_to_int(cql, table1):
    pk = unique_key_int()
    with pytest.raises(InvalidRequest, match='HEX'):
        cql.execute(f"INSERT INTO {table1} (pk) VALUES (0xBAAAAAAD)")
    with pytest.raises(InvalidRequest, match='blob'):
        cql.execute(f"INSERT INTO {table1} (pk) VALUES ((blob)0xBAAAAAAD)")
    with pytest.raises(InvalidRequest, match='blob'):
        cql.execute(f"INSERT INTO {table1} (pk) VALUES ((int)(blob)0xBAAAAAAD)")

# The function blobasint() takes a blob and returns an int. Then this int can be converted back to a blob.
def test_cast_int_func_result_to_blob_implicit(cql, table1):
    pk = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (pk, blob_col) VALUES ({pk}, blobasint(0xdeadbeef))")
    assert list(cql.execute(f"SELECT pk, blob_col FROM {table1} WHERE pk = {pk}")) == [(pk, 0xdeadbeef.to_bytes(4, 'big'))]

# An int can't be cast to bigint because the binary representation is different.
def test_cast_int_to_bigint(cql, table1):
    pk = unique_key_int()
    with pytest.raises(InvalidRequest, match='bigint'):
        cql.execute(f"INSERT INTO {table1} (pk, bigint_col) VALUES ({pk}, blobasint(0xbeefdead))")
    with pytest.raises(InvalidRequest, match='bigint'):
        cql.execute(f"INSERT INTO {table1} (pk, bigint_col) VALUES ({pk}, (bigint)blobasint(0xbeefdead))")

# The function token() returns a bigint, which can be converted to blob.
def test_cast_bigint_token_to_blobl(cql, table1):
    pk = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (pk, blob_col) VALUEs ({pk}, token(1234))")
    assert list(cql.execute(f"SELECT pk, blob_col FROM {table1} WHERE pk = {pk}")) == [(pk, int(8821045555241575141).to_bytes(8, 'big'))]

# Doing (blob)(int)4321 should be allowed - the (int) specifies an exact type for the constant, and an int can be cast to blob.
def test_cast_int_with_type_hint_to_blob_explicit(cql, table1, scylla_only):
    pk = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (pk, blob_col) VALUES ({pk}, (blob)(int)4321)")
    assert list(cql.execute(f"SELECT pk, blob_col FROM {table1} WHERE pk = {pk}")) == [(pk, int(4321).to_bytes(4, 'big'))]

# Passing (int)123432 as an argument of type blob should be allowed - it's a value of int type, so it can be implicitly cast to blob.
def test_cast_int_literal_with_type_hint_to_blob_func_arg(cql, table1, scylla_only):
    pk = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (pk, blob_col) VALUES ({pk}, blobasint((int)123432))")
    assert list(cql.execute(f"SELECT pk, blob_col FROM {table1} WHERE pk = {pk}")) == [(pk, int(123432).to_bytes(4, 'big'))]

# Passing (blob)(int)567 as an argument of type blob should be allowed - same as (int)123432
def test_cast_int_literal_with_type_hint_to_blob_func_arg_explicit(cql, table1, scylla_only):
    pk = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (pk, blob_col) VALUES ({pk}, blobasint((blob)(int)567))")
    assert list(cql.execute(f"SELECT pk, blob_col FROM {table1} WHERE pk = {pk}")) == [(pk, int(567).to_bytes(4, 'big'))]

# Executing the function blobasint(bigint) should fail. The bigint has 8 bytes, but blobasint expects 4 bytes.
# The cast itself is valid - a bigint can be cast to blob, but then executing the function should fail.
def test_blobasint_with_bigint_arg(cql, table1, scylla_only):
    pk = unique_key_int()
    with pytest.raises(InvalidRequest, match='blob'):
        cql.execute(f"INSERT INTO {table1} (pk, blob_col) VALUES ({pk}, blobasint((bigint)1234))")

# Function arguments allow implicit conversions between compatible types. blobasint takes a blob as an argument
# and returns an int, chaining them should be possible.
def test_blobasint_with_blobasint_arg(cql, table1):
    pk = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (pk, blob_col) VALUES ({pk}, blobasint(blobasint(0xFEEDF00D)))")
    assert list(cql.execute(f"SELECT pk, blob_col FROM {table1} WHERE pk = {pk}")) == [(pk, 0xFEEDF00D.to_bytes(4, 'big'))]

# Long cast that should work just like the other examples.
def test_cast_long_chain(cql, table1, scylla_only):
    pk = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (pk, blob_col) VALUES ({pk}, (blob)(blob)(blob)(blob)(blob)(blob)(blob)(blob)(tinyint)42)")
    assert list(cql.execute(f"SELECT pk, blob_col FROM {table1} WHERE pk = {pk}")) == [(pk, int(42).to_bytes(1, 'big'))]

# Casting should also work in a WHERE comparison.
def test_cast_int_to_blob_in_where_clause(cql, table1, scylla_only):
    pk = unique_key_int()
    cql.execute(f"INSERT INTO {table1} (pk, blob_col) VALUES ({pk}, 0x00BA0BAB)") 
    oobaobab_rows = cql.execute(f"SELECT pk FROM {table1} WHERE pk = {pk} AND blob_col = (blob)(int)12192683 ALLOW FILTERING")
    assert list(oobaobab_rows) == [(pk,)]

# Test type hints using C-style casts.
# toDate() has overloads, so preparing toDate(?) can't infer the type for ?.
# Specifying the type toDate((timestamp)?) or toDate((timeuuid)?) fixes the problem.
def test_function_arg_type_hint(cql, table2):
    pk = unique_key_int()

    # toDate has overloads toDate(timestamp) and toDate(timeuuid).
    # Can't infer the type for ?, so preparing the query fails.
    with pytest.raises(InvalidRequest, match='Ambiguous'):
        cql.prepare(f"INSERT INTO {table2} (pk, d) VALUES ({pk}, toDate(?))")

    timestamp_value = 2*86400000 # 2 days after 1970-01-01
    timeuuid_value = uuid.UUID('{53e99c40-b81b-11ed-be60-134dd121e491}')

    # Explicitly specifying the type using a type hint fixes the issue - the type for ? is now known.
    prepared_timestamp = cql.prepare(f"INSERT INTO {table2} (pk, d) VALUES ({pk}, toDate((timestamp)?))")
    prepared_timeuuid = cql.prepare(f"INSERT INTO {table2} (pk, d) VALUES ({pk}, toDate((timeuuid)?))")

    cql.execute(prepared_timestamp, [timestamp_value])
    assert list(cql.execute(f"SELECT d FROM {table2} WHERE pk = {pk}")) == [(2,)]

    # In prepared_timestamp the bind variable has type timestamp, so passing a timeuuid value should fail.
    with pytest.raises(TypeError):
        cql.execute(prepared_timestamp, [timeuuid_value])

    cql.execute(prepared_timeuuid, [timeuuid_value])
    assert list(cql.execute(f"SELECT d FROM {table2} WHERE pk = {pk}")) == [(19417,)]

    # In prepared_timeuuid the bind variable has type timeuuid, so passing a timestamp value should fail
    with pytest.raises(TypeError):
        cql.execute(prepared_timeuuid, [timestamp_value])
