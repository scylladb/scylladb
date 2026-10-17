# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of BatchAuthTest.java from Cassandra's
# test/unit/org/apache/cassandra/auth directory.
#
# The Java test builds a BatchStatement through Cassandra's internal APIs,
# and calls its authorize() method for a logged-in user, without executing
# it. We instead execute the same batch as a CQL BATCH statement, in a
# session logged in as the user. This checks the same permissions, and when
# they are all granted, also performs the writes.
# Cassandra and Scylla cache permissions (the Java test is not affected,
# because it calls authorize() directly), so after each GRANT we retry the
# batch until the GRANT takes effect.

import re
from contextlib import contextmanager
from ..porting import *
from ...util import unique_name
from .create_and_alter_role_test import use_user, dropped_roles
from .grant_and_revoke_test import spin_assert
from cassandra.protocol import Unauthorized

@contextmanager
def tables(cql, keyspace, n):
    names = [keyspace + "." + unique_name() for _ in range(n)]
    for name in names:
        cql.execute("CREATE TABLE %s (k int PRIMARY KEY, v int)" % name)
    try:
        yield names
    finally:
        for name in reversed(names):
            cql.execute("DROP TABLE " + name)

# A role, logged in: the Java test's createUserAndLogin()
@contextmanager
def user_session(cql):
    username = "user_" + unique_name()
    with dropped_roles(cql, username):
        cql.execute("CREATE ROLE %s WITH password = 'password' AND LOGIN = true" % username)
        with use_user(cql, username, "password") as session:
            yield username, session

def batch(*queries):
    return "BEGIN BATCH\n" + ";\n".join(queries) + ";\nAPPLY BATCH"

def insert(table, k):
    return "INSERT INTO %s (k, v) VALUES (%d, 0)" % (table, k)

def updateIf(table, k):
    return "UPDATE %s SET v = 1 WHERE k = %d IF v = 0" % (table, k)

def grant(cql, username, permission, table):
    cql.execute("GRANT %s ON TABLE %s TO %s" % (permission, table, username))

def assertUnauthorized(session, batch, username, permission, table):
    def check():
        with pytest.raises(Unauthorized, match=re.escape("User %s has no %s permission on <table %s> or any of its parents" % (username, permission, table))):
            session.execute(batch)
    spin_assert(check)

def authorized(session, batch):
    spin_assert(lambda: session.execute(batch))

# All statements target the same table
def testsingleTableBatchRequiresModify(cql, test_keyspace):
    with tables(cql, test_keyspace, 1) as [table1], user_session(cql) as (username, session):
        b = batch(insert(table1, 0),
                  insert(table1, 1),
                  insert(table1, 2))

        assertUnauthorized(session, b, username, "MODIFY", table1)

        grant(cql, username, "MODIFY", table1)
        authorized(session, b)

# Every table in the batch needs its own MODIFY check: state retained for one table must not satisfy the
# next one.
def testmultiTableBatchRequiresModifyOnEveryTable(cql, test_keyspace):
    with tables(cql, test_keyspace, 2) as [table1, table2], user_session(cql) as (username, session):
        b = batch(insert(table1, 0),
                  insert(table2, 0),
                  insert(table1, 1))

        assertUnauthorized(session, b, username, "MODIFY", table1)

        # MODIFY on table1 alone must not let the table2 statement through
        grant(cql, username, "MODIFY", table1)
        assertUnauthorized(session, b, username, "MODIFY", table2)

        grant(cql, username, "MODIFY", table2)
        authorized(session, b)

# The unconditional statement comes first and satisfies MODIFY for the table, but the conditional statement
# that follows must still require SELECT, since a CAS update can be used to simulate a read.
def testconditionalStatementAfterUnconditionalRequiresSelect(cql, test_keyspace):
    with tables(cql, test_keyspace, 1) as [table1], user_session(cql) as (username, session):
        b = batch(insert(table1, 0),
                  updateIf(table1, 0))

        assertUnauthorized(session, b, username, "MODIFY", table1)

        grant(cql, username, "MODIFY", table1)
        assertUnauthorized(session, b, username, "SELECT", table1)

        grant(cql, username, "SELECT", table1)
        authorized(session, b)

# The tests batchOnViewedTableRequiresModifyOnView and
# batchOnSeveralViewedTablesRequiresSelectOnEachBase were not translated.
# They check that writing to a table which has materialized views requires
# SELECT permission on the table and MODIFY permission on each of its views.
# Scylla deliberately requires neither: it used to, but this was removed in
# commit 8393ee2e54 (fixing #5205), because the views must be updated
# whenever the base table is, and users shouldn't need (or be given) MODIFY
# permission on views. Scylla's test/boost/cql_auth_query_test.cc tests this
# (modify_table_with_view and modify_table_with_index).
