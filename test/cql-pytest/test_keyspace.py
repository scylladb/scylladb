# Copyright 2020-present ScyllaDB
#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

# Tests for basic keyspace operations: CREATE KEYSPACE, DROP KEYSPACE,
# ALTER KEYSPACE

from util import new_test_keyspace, unique_name
import pytest
from cassandra.protocol import SyntaxException, AlreadyExists, InvalidRequest, ConfigurationException

# A basic tests for successful CREATE KEYSPACE and DROP KEYSPACE
def test_create_and_drop_keyspace(cql):
    cql.execute("CREATE KEYSPACE test_create_and_drop_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    cql.execute("DROP KEYSPACE test_create_and_drop_keyspace")

# Trying to create the same keyspace - even if with identical parameters -
# should result in an AlreadyExists error.
def test_create_keyspace_twice(cql):
    cql.execute("CREATE KEYSPACE test_create_keyspace_twice WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    with pytest.raises(AlreadyExists):
        cql.execute("CREATE KEYSPACE test_create_keyspace_twice WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    cql.execute("DROP KEYSPACE test_create_keyspace_twice")

# "IF NOT EXISTS" on CREATE KEYSPACE:
def test_create_keyspace_if_not_exists(cql):
    cql.execute("CREATE KEYSPACE IF NOT EXISTS test_create_keyspace_if_not_exists WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    # A second invocation with IF NOT EXISTS is fine:
    cql.execute("CREATE KEYSPACE IF NOT EXISTS test_create_keyspace_if_not_exists WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    # It doesn't matter if the second invocation has different parameters,
    # they are ignored.
    cql.execute("CREATE KEYSPACE IF NOT EXISTS test_create_keyspace_if_not_exists WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 }")
    cql.execute("DROP KEYSPACE test_create_keyspace_if_not_exists")

# The "WITH REPLICATION" part of CREATE KEYSPACE may not be ommitted - trying
# to do so should result in a syntax error:
def test_create_keyspace_missing_with(cql):
    with pytest.raises(SyntaxException):
        cql.execute("CREATE KEYSPACE test_create_and_drop_keyspace")

# The documentation states that "Keyspace names can have up to 48 alpha-
# numeric characters and contain underscores; only letters and numbers are
# supported as the first character.". This is not accurate. Test what is actually
# enforced:
def test_create_keyspace_invalid_name(cql):
    rep = " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"
    with pytest.raises(InvalidRequest, match='48'):
        cql.execute('CREATE KEYSPACE ' + 'x'*49 + rep)
    # The name xyz!123, unquoted, is a syntax error. With quotes it's valid
    # syntax, but an illegal name.
    with pytest.raises(SyntaxException):
        cql.execute('CREATE KEYSPACE xyz!123' + rep)
    with pytest.raises(InvalidRequest, match='name'):
        cql.execute('CREATE KEYSPACE "xyz!123"' + rep)
    # The documentation claims that only letters and numbers - i.e., not
    # underscores - are allowed as the first character of a table name.
    # This is not, in fact, true... Although an unquoted name beginning
    # with an underscore results in a syntax error in the parser, it quotes
    # such names *are* allowed:
    with pytest.raises(SyntaxException):
        cql.execute('CREATE KEYSPACE _xyz' + rep)
    cql.execute('CREATE KEYSPACE "_xyz"' + rep)
    cql.execute('DROP KEYSPACE "_xyz"')
    # As the documentation states, a keyspace name may begin with a number.
    # But such a name is not allowed by the parser, so it needs to be quoted:
    with pytest.raises(SyntaxException):
        cql.execute('CREATE KEYSPACE 123' + rep)
    cql.execute('CREATE KEYSPACE "123"' + rep)
    cql.execute('DROP KEYSPACE "123"')

# Test trying to ALTER a keyspace with invalid options.
# Reproduces #7595.
def test_create_keyspace_nonexistent_dc(cql):
    with pytest.raises(ConfigurationException):
        ks = unique_name()
        cql.execute(f"CREATE KEYSPACE {ks} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', 'nonexistentdc' : 1 }}")

# Test that attempts to reproduce an issue with double WITH keyword in CREATE
# KEYSPACE statement -- CASSANDRA-9565.
def test_create_keyspace_double_with(cql):
    with pytest.raises(SyntaxException):
        cql.execute('CREATE KEYSPACE WITH WITH DURABLE_WRITES = true')
    with pytest.raises(SyntaxException):
        cql.execute('CREATE KEYSPACE ks WITH WITH DURABLE_WRITES = true')

# Test trying a non-existent keyspace - with or without the IF EXISTS flag.
# This test demonstrates a change of the exception produced between Cassandra 4.0
# and earlier versions (with Scylla behaving like the earlier versions).
def test_drop_keyspace_nonexistent(cql):
    cql.execute('DROP KEYSPACE IF EXISTS nonexistent_keyspace')
    # Cassandra changed the exception it throws on dropping a nonexistent keyspace.
    # Prior to Cassandra 4.0 (commit 207c80c1fd63dfbd8ca7e615ec8002ee8983c5d6, Nov. 2016)
    # it was a ConfigurationException, but in 4.0, it changed to and InvalidRequest.
    # In Sylla, it remains a ConfigurationException is it was in earlier Cassandra.
    with pytest.raises( (InvalidRequest, ConfigurationException) ):
        cql.execute('DROP KEYSPACE nonexistent_keyspace')

# Test trying to ALTER a keyspace.
def test_alter_keyspace(cql):
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }") as keyspace:
        cql.execute(f"ALTER KEYSPACE {keyspace} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }} AND DURABLE_WRITES = false")

# Test trying to ALTER a keyspace with invalid options.
def test_alter_keyspace_invalid(cql):
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }") as keyspace:
        with pytest.raises(ConfigurationException):
            cql.execute(f"ALTER KEYSPACE {keyspace} WITH REPLICATION = {{ 'class' : 'NoSuchStrategy' }}")
        with pytest.raises(ConfigurationException):
            cql.execute(f"ALTER KEYSPACE {keyspace} WITH REPLICATION = {{ 'class' : 'SimpleStrategy' }}")
        with pytest.raises(ConfigurationException):
            cql.execute(f"ALTER KEYSPACE {keyspace} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 'foo' }}")

# Test trying to ALTER a keyspace with invalid options.
# Reproduces #7595.
def test_alter_keyspace_nonexistent_dc(cql):
    with new_test_keyspace(cql, "WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }") as keyspace:
        with pytest.raises(ConfigurationException):
            cql.execute(f"ALTER KEYSPACE {keyspace} WITH replication = {{ 'class' : 'NetworkTopologyStrategy', 'nonexistentdc' : 1 }}")

# Test trying to ALTER a non-existing keyspace
def test_alter_keyspace_nonexisting(cql):
    cql.execute('DROP KEYSPACE IF EXISTS nonexistent_keyspace')
    with pytest.raises(InvalidRequest):
        cql.execute("ALTER KEYSPACE nonexistent_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")

# Test that attempts to reproduce an issue with double WITH keyword in ALTER
# KEYSPACE statement -- CASSANDRA-9565.
def test_alter_keyspace_double_with(cql):
    with pytest.raises(SyntaxException):
        cql.execute('ALTER KEYSPACE WITH WITH DURABLE_WRITES = true')
    with pytest.raises(SyntaxException):
        cql.execute('ALTER KEYSPACE ks WITH WITH DURABLE_WRITES = true')

# TODO: more tests for "WITH REPLICATION" syntax in CREATE TABLE.
# TODO: check the "AND DURABLE_WRITES" option of CREATE TABLE.
# TODO: confirm case insensitivity without quotes, and case sensitivity with them.
