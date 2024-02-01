# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Some tests for the new "tablets"-based replication, replicating the old
# "vnodes". Eventually, ScyllaDB will use tablets by default and all tests
# will run using tablets, but these tests are for specific issues discovered
# while developing tablets that didn't exist for vnodes. Note that most tests
# for tablets require multiple nodes, and are in the test/topology*
# directory, so here we'll probably only ever have a handful of single-node
# tests.
#############################################################################

import pytest
from util import new_test_keyspace, new_test_table, unique_name
from cassandra.protocol import ConfigurationException, InvalidRequest

# A fixture similar to "test_keyspace", just creates a keyspace that enables
# tablets with initial_tablets=128
# The "initial_tablets" feature doesn't work if the "tablets" experimental
# feature is not turned on; In such a case, the tests using this fixture
# will be skipped.
@pytest.fixture(scope="module")
def test_keyspace_128_tablets(cql, this_dc):
    name = unique_name()
    try:
        cql.execute("CREATE KEYSPACE " + name + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "': 1 } AND TABLETS = { 'enabled': true, 'initial': 128 }")
    except ConfigurationException:
        pytest.skip('Scylla does not support initial_tablets, or the tablets feature is not enabled')
    yield name
    cql.execute("DROP KEYSPACE " + name)

# In the past (issue #16493), repeatedly creating and deleting a table
# would leak memory. Creating a table with 128 tablets would make this
# leak 128 times more serious and cause a failure faster. This is a
# reproducer for this problem. We basically expect this test not to
# OOM Scylla - the test doesn't "check" anything, the way it failed was
# for Scylla to run out of memory and then fail one of the CREATE TABLE
# or DROP TABLE operations in the loop.
# Note that this test doesn't even involve any data inside the table.
# Reproduces #16493.
def test_create_loop_with_tablets(cql, test_keyspace_128_tablets):
    table = test_keyspace_128_tablets + "." + unique_name()
    for i in range(100):
        cql.execute(f"CREATE TABLE {table} (p int PRIMARY KEY, v int)")
        cql.execute("DROP TABLE " + table)


# Converting vnodes-based keyspace to tablets-based in not implemented yet
def test_alter_cannot_change_vnodes_to_tablets(cql, skip_without_tablets):
    ksdef = "WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : '1' } AND TABLETS = { 'enabled' : false }"
    with new_test_keyspace(cql, ksdef) as keyspace:
        with pytest.raises(InvalidRequest, match="Cannot alter replication strategy vnode/tablets flavor"):
            cql.execute(f"ALTER KEYSPACE {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'initial': 1}};")


# Converting vnodes-based keyspace to tablets-based in not implemented yet
def test_alter_doesnt_enable_tablets(cql, skip_without_tablets):
    ksdef = "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
    with new_test_keyspace(cql, ksdef) as keyspace:
        cql.execute(f"ALTER KEYSPACE {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy'}};")

        res = cql.execute(f"DESCRIBE KEYSPACE {keyspace}").one()
        assert "NetworkTopologyStrategy" in res.create_statement

        res = cql.execute(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{keyspace}'")
        assert len(list(res)) == 0, "tablets replication strategy turned on"


def test_tablet_default_initialization(cql, skip_without_tablets):
    ksdef = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};"
    with new_test_keyspace(cql, ksdef) as keyspace:
        res = cql.execute(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{keyspace}'").one()
        assert res.initial_tablets == 0, "initial_tablets not configured"

        with new_test_table(cql, keyspace, "pk int PRIMARY KEY, c int") as table:
            table = table.split('.')[1]
            res = cql.execute("SELECT * FROM system.tablets")
            for row in res:
                if row.keyspace_name == keyspace and row.table_name == table:
                    assert row.tablet_count > 0, "zero tablets allocated"
                    break
            else:
                assert False, "tablets not allocated"


def test_tablets_can_be_explicitly_disabled(cql, skip_without_tablets):
    ksdef = "WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND TABLETS = {'enabled': false};"
    with new_test_keyspace(cql, ksdef) as keyspace:
        res = cql.execute(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{keyspace}'")
        assert len(list(res)) == 0, "tablets replication strategy turned on"


def test_alter_changes_initial_tablets(cql, skip_without_tablets):
    ksdef = "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};"
    with new_test_keyspace(cql, ksdef) as keyspace:
        cql.execute(f"ALTER KEYSPACE {keyspace} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}} AND tablets = {{'initial': 2}};")
        res = cql.execute(f"SELECT * FROM system_schema.scylla_keyspaces WHERE keyspace_name = '{keyspace}'").one()
        assert res.initial_tablets == 2
