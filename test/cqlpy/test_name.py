# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#############################################################################
# Tests for limitations on the *names* of keyspaces, tables, indexes, views,
# function and columns.
#############################################################################

import pytest
from contextlib import contextmanager
from cassandra.protocol import InvalidRequest, SyntaxException, ConfigurationException, Unauthorized
from .util import unique_name, new_test_table, new_secondary_index, new_function, is_scylla

# This context manager is similar to new_test_table() - creating a table and
# keeping it alive while the context manager is in scope - but allows the
# caller to pick the name of the table. This is useful to attempt to create
# a table whose name has different lengths or characters.
# Note that if used in a shared keyspace, it is recommended to base the
# given table name on the output of unique_name(), to avoid name clashes.
# See the padded_name() function below as an example.
@contextmanager
def new_named_table(cql, keyspace, table, schema, extra=""):
    tbl  = keyspace+'.'+table
    cql.execute(f'CREATE TABLE {tbl} ({schema}) {extra}')
    try:
        yield tbl
    finally:
        cql.execute(f'DROP TABLE {tbl}')

# padded_name() creates a unique name of given length by taking the
# output of unique_name() and padding it with extra 'x' characters:
def padded_name(length):
    u = unique_name()
    assert length >= len(u)
    return u + 'x'*(length-len(u))

# Utility function to create a new keyspace with the given name.
# Created to avoid passing the same replication option in every tests.
@contextmanager
def new_keyspace(cql, ks_name=unique_name()):
    cql.execute(f"CREATE KEYSPACE {ks_name} WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
    try:
        yield ks_name
    finally:
        cql.execute(f"DROP KEYSPACE {ks_name}")

# Utility function to create a new table with the given name.
# Created to avoid passing the same schema option in every tests.
@contextmanager
def new_table(cql, ks_name, tbl_name=unique_name(), extra=""):
    qualified_table_name = ks_name + '.' + tbl_name
    cql.execute(f'CREATE TABLE {qualified_table_name} (p int, x int, PRIMARY KEY (p)) {extra}')
    try:
        yield qualified_table_name
    finally:
        cql.execute(f'DROP TABLE {qualified_table_name}')

# Utility function to create a materialized view with the given name.
# Created to avoid passing the same parameter values in every tests.
@contextmanager
def new_mv(cql, qualified_table_name, mv_name):
    keyspace = qualified_table_name.split('.')[0]
    qualified_mv_name = keyspace + "." + mv_name
    cql.execute(
        f"CREATE MATERIALIZED VIEW {qualified_mv_name} AS SELECT * FROM {qualified_table_name} WHERE p is not null and x is not null PRIMARY KEY (p, x)"
    )
    try:
        yield qualified_mv_name
    finally:
        cql.execute(f"DROP MATERIALIZED VIEW {qualified_mv_name}")

# Cassandra's documentation states that "Both keyspace and table name ... are
# limited in size to 48 characters". This was actually only true in Cassandra
# 3, and by Cassandra 4 and 5 this limitation was dropped (see discussion
# in CASSANDRA-20425).
# Test verifies that a 48-character name is allowed, and passes on all versions
# of Scylla and Cassandra.
def test_table_name_length_48(cql, test_keyspace):
    with new_table(cql, test_keyspace, padded_name(48)):
        pass

# After Cassandra removed the 48-character limit for table names, some applications began creating tables with longer names.
# ScyllaDB relaxed its own limit to maintain compatibility with these applications.
# Due to CDC-enabled tables, ScyllaDB enforces a 192-character limit (see schema::NAME_LENGTH for details),
# while Cassandra allows names up to 222 characters.
# The 192-character limit in ScyllaDB is considered sufficient for most use cases.
# Test verifies that a 192-character name is allowed, and passes on Scylla and latest Cassandra versions (4 and 5).
def test_table_name_length_192(cql, test_keyspace):
    with new_table(cql, test_keyspace, padded_name(192)):
        pass

# If we try an even longer table name length, e.g., 500 characters, we run
# into the problem that an attempt to create a file or directory name based
# on the table name will fail (due to filesystem capabilities). Even if we lift the 48-character limitation
# introduced in Cassandra 3, creating a 500-character name should fail gracefully.
# Test verifies that a 500-character name is rejected by ScyllaDB and Cassandra.
# We mark this test cassandra_bug because Cassandra 5 hangs on this test (CASSANDRA-20425 and CASSANDRA-20389).
def test_table_name_length_500(cql, test_keyspace, cassandra_bug):
    name = padded_name(500)
    with pytest.raises(InvalidRequest, match=name):
        with new_table(cql, test_keyspace, name):
            pass

# Test verifies that a 192-character name for CDC enabled table is accepted for Scylla and Cassandra 4/5.
def test_table_cdc_name_length_192(cql):
    # Incompatible Cassandra <-> Scylla API to enable CDC. See #9859.
    cdc = "{'enabled': true}" if is_scylla(cql) else 'true'
    with new_keyspace(cql) as keyspace:
        with new_table(cql, keyspace, padded_name(192), extra=f"with CDC = {cdc}"):
            pass

# When ScyllaDB lifted the table name length restriction, it also lifted the limit on keyspace name length.
# However, Cassandra still enforces a 48-character limit for keyspace names.
# Test verifies that a keyspace name of exactly 48 characters is accepted by all versions of ScyllaDB and Cassandra.
def test_keyspace_name_length_48(cql):
    with new_keyspace(cql, padded_name(48)):
        pass

# Test verifies that a keyspace name of exactly 192 is accepted by ScyllaDB.
# Marked scylla_only because Cassandra keeps the 48-character limit for keyspace names.
def test_keyspace_name_length_192(cql, scylla_only):
    with new_keyspace(cql, padded_name(192)):
        pass

# Test verifies that too long keyspace (exceeding filesystem capabilities) is gracefully rejected.
def test_keyspace_name_length_500(cql):
    name = padded_name(500)
    with pytest.raises(InvalidRequest, match=name):
        with new_keyspace(cql, name):
            pass

# Test verifies that materialized view names follow the same length rules as table names.
def test_mv_name_length_192(cql, test_keyspace):
    with new_table(cql, test_keyspace) as table:
        with new_mv(cql, table, padded_name(192)):
            pass

# Test verifies that materialized view names follow the same length rules as table names.
# Marked cassandra_bug because Cassandra 5 hangs on this test (CASSANDRA-20425 and CASSANDRA-20389).
def test_mv_name_length_500(cql, test_keyspace, cassandra_bug):
    name = padded_name(500)
    with new_table(cql, test_keyspace) as table:
        with pytest.raises(InvalidRequest, match=name):
            with new_mv(cql, table, name):
                pass

# Test verifies that secondary index names follow the same length rules as table names.
def test_index_name_length_192(cql, test_keyspace):
    with new_table(cql, test_keyspace) as table:
        with new_secondary_index(cql, table, "x", padded_name(192)):
            pass

# Test verifies that secondary index names follow the same length rules as table names.
# Marked cassandra_bug because Cassandra 5 hangs on this test (CASSANDRA-20425 and CASSANDRA-20389).
def test_index_name_length_500(cql, test_keyspace, cassandra_bug):
    name = padded_name(500)
    with new_table(cql, test_keyspace) as table:
        with pytest.raises(InvalidRequest, match=name):
            with new_secondary_index(cql, table, "x", name):
                pass

# Test verifies that column names are not restricted by schema::NAME_LENGTH or filesystem constraints.
def test_function_name_500(cql, test_keyspace):
    # ScyllaDB by default supports Lua functions while Cassandra Java only.
    # In this test we only want to verify function name, hence function body is not important.
    lang = "lua" if is_scylla(cql) else "java"
    with new_function(cql, test_keyspace, f"() CALLED ON NULL INPUT RETURNS int LANGUAGE {lang} AS 'return 0;'",
                      name=padded_name(500)):
        pass

# Test verifies that column names are not restricted by schema::NAME_LENGTH or filesystem constraints.
def test_column_name_500(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int primary key, " + padded_name(500) + " int"):
        pass

# Our documentation claims that table names must match the regular expression
# [a-zA-Z_0-9]{1, 192}. Above, we already tested the length limits, and here
# we want to test the allowed characters in table names given to CREATE TABLE.
#
# In particular, the documentation implies that while quoting a table name
# allows uppercase letters to be preserved, quoting does NOT allow you to use
# forbidden characters, like spaces or punctuation. The same rules apply also
# to quoted names. We want to confirm that in a test.
# We also demonstrate one exception - a difference between quoted and unquoted
# name rules: A table name may begin with an underscore, but in that case must
# be quoted.
def test_create_table_forbidden_chars(cql, test_keyspace):
    # Spaces and various non-alphanumeric characters are not allowed in table
    # names given to CREATE TABLE. Among other things, the "." and "-" which
    # are allowed by Alternator are not allowed by CQL's CREATE TABLE. Neither
    # are spaces, or the "$" character which we use in internal tables like
    # "tablename$paxos".
    base = unique_name()
    for name in ['hello world', 'hello$world', 'hello-world', 'hello.world']:
        # These names are not allowed, neither quoted nor unquoted. But the
        # specific error message is different - invalid characters in an
        # unquoted name result in a SyntaxException, while invalid characters
        # inside quotes are detected later in the requests's execution, and
        # generate an InvalidRequest in ScyllaDB or ConfigurationException
        # in Cassandra (we'll allow both).
        with pytest.raises(SyntaxException):
            with new_table(cql, test_keyspace, f'{base}{name}'):
                pass
        with pytest.raises((InvalidRequest, ConfigurationException)):
            with new_table(cql, test_keyspace, f'"{base}{name}"'):
                pass
    # An underscore in the name the table *is* allowed, in either quoted
    # or unquoted table name:
    with new_table(cql, test_keyspace, f'{base}hello_world'):
        pass
    with new_table(cql, test_keyspace, f'"{base}hello_world"'):
        pass
    # Despite what some older documentation says, an underscore as the first
    # character of the table name *is* allowed. However, it turns out that it
    # is just not allowed in an unquoted name - where it is considered a
    # syntax error. It is allowed just fine in a quoted name.
    with pytest.raises(SyntaxException):
        with new_table(cql, test_keyspace, f'_{base}'):
            pass
    with new_table(cql, test_keyspace, f'"_{base}"'):
        pass

# See also test_utf8.py::test_unicode_in_table_names that verifies that only
# ASCII letters are allowed in table names, not Unicode letters in general.

# Although as tested above CREATE TABLE has rules on which table names it can
# create, if somehow a table with a name that doesn't follow these rules got
# created, it can still be accessed by other operations like ALTER TABLE or
# SELECT. In other words, only CREATE TABLE checks the validity of the table's
# name - other operations just check if a table with the given name exists.
#
# A forbidden-name table can be created by Alternator (which allows the
# characters '.' and '-' in table names) or by LWT (which creates
# an internal table tablename$paxos). In this test we'll check the LWT table -
# that it can be accessed despite having a name which CREATE TABLE would have
# refused to create. This test is Scylla specific (we can't create tables with
# invalid names in Cassandra), and if the LWT implementation changes we'll
# need to update this test. See below for another test for the same thing
# without needing to create the invalid-named table.
def test_access_table_forbidden_chars_lwt(cql, test_keyspace, scylla_only):
    with new_table(cql, test_keyspace) as table:
        _, table_name = table.split('.')
        # The extra "...$paxos" table only appears after a real LWT write is
        # performed. So let's do an LWT operation that will cause it to be
        # created.
        cql.execute(f'INSERT INTO {table}(p,x) values (1,2) IF NOT EXISTS')
        # Check that we can SELECT from the "$paxos" table even though this
        # name is not a valid CQL table's name (we couldn't have created it
        # with CREATE TABLE)
        cql.execute(f'SELECT * FROM {test_keyspace}."{table_name}$paxos"')
        # In the current implementation, the user is not "authorized" to
        # ALTER the internal paxos table. But the important thing is that
        # we don't get an error that the table's name is invalid or something.
        try:
            cql.execute(f'ALTER TABLE {test_keyspace}."{table_name}$paxos" WITH COMMENT=\'hi\'')
        except Unauthorized:
            pass # fine

# This is a more general test for accessing (SELECT, ALTER) a table name with
# invalid characters in its name. Here we don't need the invalid-named table
# to actually exist, but just check when the operation fails, the error
# reported is that the table doesn't exist - we don't get an error saying
# that the table's name is considered invalid.
def test_access_table_forbidden_chars(cql, test_keyspace):
    bad_name = f'{unique_name()}$bad!'
    # We expect to see an error that the table doesn't exist - not that it
    # has a forbidden name.
    # Cassandra says the table "does not exist", Scylla calls it an
    # "unconfigured table".
    with pytest.raises(InvalidRequest, match='does not exist|unconfigured table'):
        cql.execute(f'SELECT * FROM {test_keyspace}."{bad_name}"')
    with pytest.raises(InvalidRequest, match='does not exist|unconfigured table'):
        cql.execute(f'ALTER TABLE {test_keyspace}."{bad_name}" WITH COMMENT=\'hi\'')
