# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#############################################################################
# Tests for the consistency option of a keyspace: which values CREATE
# KEYSPACE accepts, and what ALTER KEYSPACE may and may not change. Whether a
# keyspace ended up strongly consistent is judged by what its tables do, not
# by how DESCRIBE prints the option.
#
# The tests create their own keyspaces. They take the sc_keyspace fixture only
# for its guard, which skips them unless the server can create strongly
# consistent keyspaces at all.
#############################################################################

import re

import pytest
from cassandra.cluster import ConsistencyLevel
from cassandra.protocol import ConfigurationException, InvalidRequest
from cassandra.query import SimpleStatement

from ..util import new_test_keyspace, new_test_table, unique_name


REPLICATION = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"


def is_strongly_consistent(cql, keyspace):
    """
    Whether a table of `keyspace` takes strongly consistent writes, which
    refuse every consistency level but QUORUM and LOCAL_QUORUM.
    """
    with new_test_table(cql, keyspace, "pk int PRIMARY KEY, v int") as table:
        try:
            cql.execute(SimpleStatement(f"INSERT INTO {table} (pk, v) VALUES (1, 1)", consistency_level=ConsistencyLevel.ONE))
        except InvalidRequest as e:
            if "Strongly consistent writes must use QUORUM/LOCAL_QUORUM" not in str(e):
                raise
            return True
        return False


@pytest.mark.parametrize("options, strongly_consistent", [
    ("AND tablets = {'initial': 1} AND consistency = 'global'", True),
    ("AND tablets = {'initial': 1} AND consistency = 'eventual'", False),
    ("AND tablets = {'enabled': false} AND consistency = 'eventual'", False),
])
def test_create_keyspace_with_consistency(cql, sc_keyspace, options, strongly_consistent):
    """
    A keyspace takes `global` with tablets, and `eventual` with or without
    them, and its tables then behave accordingly. These are the accepted
    twins of the refusals in test_create_keyspace_with_invalid_consistency.
    """
    with new_test_keyspace(cql, f"{REPLICATION} {options}") as keyspace:
        assert is_strongly_consistent(cql, keyspace) == strongly_consistent


@pytest.mark.parametrize("options, message", [
    ("AND tablets = {'enabled': false} AND consistency = 'global'",
     "Only eventual consistency is supported for non-tablet keyspaces"),
    ("AND tablets = {'initial': 1} AND consistency = 'local'",
     "Local consistency is not supported yet"),
    ("AND tablets = {'initial': 1} AND consistency = 'bogus'",
     "Consistency option must be one of 'eventual', 'local', or 'global'; found: bogus"),
])
def test_create_keyspace_with_invalid_consistency(cql, sc_keyspace, options, message):
    """
    A consistency option the keyspace cannot have is refused with a reason
    the user can act on.
    """
    with pytest.raises(ConfigurationException, match=re.escape(message)):
        cql.execute(f"CREATE KEYSPACE {unique_name()} {REPLICATION} {options}")


@pytest.mark.parametrize("created, altered", [
    ("AND consistency = 'global'", "eventual"),
    ("AND consistency = 'eventual'", "global"),
    ("", "global"),
])
def test_alter_keyspace_cannot_change_consistency(cql, sc_keyspace, created, altered):
    """
    The consistency option is fixed when the keyspace is created: ALTER
    KEYSPACE refuses to change it in either direction, including making an
    ordinary keyspace strongly consistent, and the keyspace keeps behaving
    as created.
    """
    with new_test_keyspace(cql, f"{REPLICATION} AND tablets = {{'initial': 1}} {created}") as keyspace:
        strongly_consistent = is_strongly_consistent(cql, keyspace)
        with pytest.raises(InvalidRequest, match="Cannot alter consistency option"):
            cql.execute(f"ALTER KEYSPACE {keyspace} WITH consistency = '{altered}'")

        assert is_strongly_consistent(cql, keyspace) == strongly_consistent


def test_alter_keyspace_keeps_consistency(cql, sc_keyspace):
    """
    ALTER KEYSPACE that does not name the consistency option, or restates the
    value the keyspace already has, leaves the keyspace strongly consistent.
    """
    with new_test_keyspace(cql, f"{REPLICATION} AND tablets = {{'initial': 1}} AND consistency = 'global'") as keyspace:
        cql.execute(f"ALTER KEYSPACE {keyspace} WITH durable_writes = false")
        assert is_strongly_consistent(cql, keyspace)

        cql.execute(f"ALTER KEYSPACE {keyspace} WITH consistency = 'global'")
        assert is_strongly_consistent(cql, keyspace)
