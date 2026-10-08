# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from .porting import *
from ..util import cql_session, new_cql
from cassandra.cluster import ResponseFuture
from cassandra.protocol import PrepareMessage

REPLICATION = "replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"

# Equivalent of the Java test's sessionNet(version): a new session using the
# given protocol version. It is created like the "cql" fixture's session
# (with util.py's cql_session()), with the same endpoint, credentials and SSL
# setting, and only changes the protocol version. We check that the driver
# indeed uses the requested version, and didn't negotiate a different one.
# Scylla doesn't support protocol version 5 (SCYLLADB-442), so tests with
# version 5 are marked xfail.
@contextmanager
def sessionNet(cql, version):
    endpoint = cql.hosts[0].endpoint
    auth = cql.cluster.auth_provider
    with cql_session(host=endpoint.address, port=endpoint.port, is_ssl=(cql.cluster.ssl_context is not None),
                     username=auth.username, password=auth.password, protocol_version=version) as session:
        assert session.cluster.protocol_version == version
        yield session

V4 = 4
V5 = pytest.param(5, marks=pytest.mark.xfail(reason="SCYLLADB-442"))

# Reproduces SCYLLADB-5186 (warnings when preparing statements)
@pytest.mark.xfail(reason="SCYLLADB-5186")
def testUnqualifiedPreparedSelectOrModificationStatementsEmitWarning(cql):
    for query in ["SELECT id, v1, v2 FROM %s WHERE id = 1",
                  "INSERT INTO %s (id, v1, v2) VALUES (1, 2, 3)",
                  "UPDATE %s SET v1 = 2, v2 = 3 where id = 1"]:
        assertWarningsOnPreparedStatements(cql, query, True, True, True)

def testQualifiedPreparedSelectOrModificationStatementsDoNotEmitWarning(cql):
    for query in ["SELECT id, v1, v2 FROM %keyspace%.%s WHERE id = 1",
                  "INSERT INTO %keyspace%.%s (id, v1, v2) VALUES (1, 2, 3)",
                  "UPDATE %keyspace%.%s SET v1 = 2, v2 = 3 where id = 1"]:
        assertWarningsOnPreparedStatements(cql, query, False, True, True)
        assertWarningsOnPreparedStatements(cql, query, False, True, False)

# Reproduces SCYLLADB-5186 (warnings when preparing statements)
@pytest.mark.xfail(reason="SCYLLADB-5186")
def testSchemaTransformationPreparedStatementEmitsWaring(cql, new_to_cassandra_6):
    assertWarningsOnPreparedStatements(cql, "ALTER TABLE %s ADD c3 int", True, False, True)
    assertWarningsOnPreparedStatements(cql, "ALTER TABLE %keyspace%.%s ADD c3 int", True, False, False)

# Reproduces SCYLLADB-5186 (warnings when preparing statements)
@pytest.mark.xfail(reason="SCYLLADB-5186")
def testBatchPreparedStatementsEmitWarnings(cql):
    assertWarningsOnPreparedStatements(cql, "BEGIN BATCH INSERT INTO %s (id, v1, v2) VALUES (1,2,3) APPLY BATCH", True, True, True)

    # this will evaluate a statement as unqualified because not all are qualified
    assertWarningsOnPreparedStatements(cql, "BEGIN BATCH" +
                                       "  INSERT INTO %keyspace%.%s (id, v1, v2) VALUES (1,2,3); " +
                                       "  INSERT INTO %s (id, v1, v2) VALUES (3, 4, 5) " +
                                       "APPLY BATCH;", True, True, True)

    assertWarningsOnPreparedStatements(cql, "BEGIN BATCH INSERT INTO %keyspace%.%s (id, v1, v2) VALUES (1,2,3) APPLY BATCH;", False, True, True)
    assertWarningsOnPreparedStatements(cql, "BEGIN BATCH INSERT INTO %keyspace%.%s (id, v1, v2) VALUES (1,2,3) APPLY BATCH;", False, True, False)

# The Java test prepares the statement through Cassandra's internal API, with
# a client state which may have a current keyspace, and captures the warnings
# through an internal API. We prepare the statement in a new session (with
# "USE" if a current keyspace is needed). The Python driver's
# Session.prepare() doesn't return the warnings of the PREPARE response, so
# prepareWithWarnings() sends the PREPARE request like Session.prepare() does,
# and returns the warnings from the request's ResponseFuture.
def prepareWithWarnings(session, query):
    future = ResponseFuture(session, PrepareMessage(query=query), query=None, timeout=session.default_timeout)
    future.send_request()
    future.result()
    return future.warnings or []

def assertWarningsOnPreparedStatements(cql, query, expectWarn, forModificationOrSelectStatement, useUse):
    with create_keyspace(cql, REPLICATION) as keyspace, create_table(cql, keyspace, "(id int, v1 int, v2 int, primary key (id))") as table, new_cql(cql) as session:
        if useUse:
            session.execute("USE " + keyspace)

        maybeQueryWithKeyspace = query.replace("%keyspace%", keyspace)
        queryWithTable = maybeQueryWithKeyspace.replace("%s", table.split(".")[1])

        # two times is not a mistake, a warning is emitted just once
        warnings = prepareWithWarnings(session, queryWithTable)
        warnings += prepareWithWarnings(session, queryWithTable)

        if expectWarn and forModificationOrSelectStatement:
            assert len(warnings) == 1 and warnings[0].startswith("`USE <keyspace>` with prepared statements is considered to be an anti-pattern"), warnings
        elif expectWarn:
            assert len(warnings) == 1 and warnings[0].startswith("Prepared statements for other than modification and selection statements should be avoided,"), warnings
        else:
            assert warnings == []

# The Java test uses protocol version 5, but nothing in it is specific to this
# version. Scylla doesn't support version 5 (SCYLLADB-442), so to also check
# Scylla we run the test with both versions 4 and 5. The Java test (in
# Cassandra 6) also checks a prepared Accord transaction, which we don't.
@pytest.mark.parametrize("version", [V4, V5])
def testInvalidatePreparedStatementsOnDrop(cql, version):
    KEYSPACE = unique_name()
    createKsStatement = "CREATE KEYSPACE " + KEYSPACE + " WITH " + REPLICATION
    dropKsStatement = "DROP KEYSPACE IF EXISTS " + KEYSPACE
    with sessionNet(cql, version) as session:
        session.execute(dropKsStatement)
        session.execute(createKsStatement)
        try:
            createTableStatement = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".qp_cleanup (id int PRIMARY KEY, cid int, val text);"
            dropTableStatement = "DROP TABLE IF EXISTS " + KEYSPACE + ".qp_cleanup;"

            session.execute(createTableStatement)

            insert = "INSERT INTO " + KEYSPACE + ".qp_cleanup (id, cid, val) VALUES (?, ?, ?)"
            prepared = session.prepare(insert)
            preparedBatch = session.prepare("BEGIN BATCH\n  " + insert + ";\nAPPLY BATCH")

            session.execute(dropTableStatement)
            session.execute(createTableStatement)

            session.execute(prepared.bind((1, 1, "value")))
            session.execute(preparedBatch.bind((2, 2, "value2")))

            session.execute(dropKsStatement)
            session.execute(createKsStatement)
            session.execute(createTableStatement)

            # The driver will get a response about the prepared statement being invalid, causing it to transparently
            # re-prepare the statement.  We'll rely on the fact that we get no errors while executing this to show that
            # the statements have been invalidated.
            session.execute(prepared.bind((1, 1, "value")))
            session.execute(preparedBatch.bind((2, 2, "value2")))
        finally:
            session.execute(dropKsStatement)

# The Java tests (in Cassandra 6) also check a prepared Accord transaction,
# which we don't.
def invalidatePreparedStatementOnAlter(cql, version, supportsMetadataChange):
    KEYSPACE = unique_name()
    createKsStatement = "CREATE KEYSPACE " + KEYSPACE + " WITH " + REPLICATION
    dropKsStatement = "DROP KEYSPACE IF EXISTS " + KEYSPACE
    with sessionNet(cql, version) as session:
        createTableStatement = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".qp_cleanup (a int PRIMARY KEY, b int, c int);"
        alterTableStatement = "ALTER TABLE " + KEYSPACE + ".qp_cleanup ADD d int;"

        session.execute(dropKsStatement)
        session.execute(createKsStatement)
        try:
            session.execute(createTableStatement)

            select = "SELECT * FROM " + KEYSPACE + ".qp_cleanup"
            preparedSelect = session.prepare(select)
            session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c) VALUES (%s, %s, %s);", (1, 2, 3))
            session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c) VALUES (%s, %s, %s);", (2, 3, 4))

            assert_rows_ignoring_order(session.execute(preparedSelect.bind(())),
                                       row(1, 2, 3),
                                       row(2, 3, 4))

            session.execute(alterTableStatement)

            session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c, d) VALUES (%s, %s, %s, %s);", (3, 4, 5, 6))

            if supportsMetadataChange:
                rs = session.execute(preparedSelect.bind(()))
                assert len(rs.column_names) == 4
                assert_rows_ignoring_order(rs,
                                           row(1, 2, 3, None),
                                           row(2, 3, 4, None),
                                           row(3, 4, 5, 6))
            else:
                rs = session.execute(preparedSelect.bind(()))
                assert len(rs.column_names) == 3
                assert_rows_ignoring_order(rs,
                                           row(1, 2, 3),
                                           row(2, 3, 4),
                                           row(3, 4, 5))
        finally:
            session.execute(dropKsStatement)

@pytest.mark.parametrize("version", [V5])
def testInvalidatePreparedStatementOnAlterV5(cql, version):
    invalidatePreparedStatementOnAlter(cql, version, True)

# With protocol version 4, the Java driver keeps using the result metadata it
# got when it first prepared the statement, so the Java test expects to see
# the table's old 3 columns after the ALTER TABLE. But the Python driver
# updates the result metadata when it re-prepares the statement (which the
# server invalidated on ALTER TABLE), so with protocol version 4 it sees the
# new 4 columns - like with protocol version 5.
def testInvalidatePreparedStatementOnAlterV4(cql):
    invalidatePreparedStatementOnAlter(cql, V4, True)

# The Java tests (in Cassandra 6) also check a prepared Accord transaction,
# which we don't.
def invalidatePreparedStatementOnAlterUnchangedMetadata(cql, version):
    KEYSPACE = unique_name()
    createKsStatement = "CREATE KEYSPACE " + KEYSPACE + " WITH " + REPLICATION
    dropKsStatement = "DROP KEYSPACE IF EXISTS " + KEYSPACE
    with sessionNet(cql, version) as session:
        createTableStatement = "CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".qp_cleanup (a int PRIMARY KEY, b int, c int);"
        alterTableStatement = "ALTER TABLE " + KEYSPACE + ".qp_cleanup ADD d int;"

        session.execute(dropKsStatement)
        session.execute(createKsStatement)
        try:
            session.execute(createTableStatement)

            select = "SELECT a, b, c FROM " + KEYSPACE + ".qp_cleanup"
            preparedSelect = session.prepare(select)
            session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c) VALUES (%s, %s, %s);", (1, 2, 3))
            session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c) VALUES (%s, %s, %s);", (2, 3, 4))

            rs = session.execute(preparedSelect.bind(()))
            assert len(rs.column_names) == 3
            assert_rows_ignoring_order(rs,
                                       row(1, 2, 3),
                                       row(2, 3, 4))

            session.execute(alterTableStatement)

            session.execute("INSERT INTO " + KEYSPACE + ".qp_cleanup (a, b, c, d) VALUES (%s, %s, %s, %s);", (3, 4, 5, 6))

            rs = session.execute(preparedSelect.bind(()))
            assert len(rs.column_names) == 3
            assert_rows_ignoring_order(rs,
                                       row(1, 2, 3),
                                       row(2, 3, 4),
                                       row(3, 4, 5))
        finally:
            session.execute(dropKsStatement)

def testInvalidatePreparedStatementOnAlterUnchangedMetadataV4(cql):
    invalidatePreparedStatementOnAlterUnchangedMetadata(cql, V4)

@pytest.mark.parametrize("version", [V5])
def testInvalidatePreparedStatementOnAlterUnchangedMetadataV5(cql, version):
    invalidatePreparedStatementOnAlterUnchangedMetadata(cql, version)

# The Java test uses protocol version 5, but nothing in it is specific to
# this version, so like testInvalidatePreparedStatementsOnDrop we run it with
# both versions 4 and 5. The Java test (in Cassandra 6) also checks a
# prepared Accord transaction, which we don't.
@pytest.mark.parametrize("version", [V4, V5])
def testStatementRePreparationOnReconnect(cql, test_keyspace, version):
    with sessionNet(cql, version) as session:
        session.execute("USE " + test_keyspace)

        with create_table(cql, test_keyspace, "(id int PRIMARY KEY, cid int, val text)") as table:
            insertCQL = "INSERT INTO " + table + " (id, cid, val) VALUES (?, ?, ?)"
            selectCQL = "Select * from " + table + " where id = ?"

            preparedInsert = session.prepare(insertCQL)
            preparedSelect = session.prepare(selectCQL)

            session.execute(preparedInsert.bind((1, 1, "value")))
            assert len(list(session.execute(preparedSelect.bind((1,))))) == 1

            with sessionNet(cql, version) as newSession:
                newSession.execute("USE " + test_keyspace)
                preparedInsert = newSession.prepare(insertCQL)
                preparedSelect = newSession.prepare(selectCQL)
                newSession.execute(preparedInsert.bind((1, 1, "value")))

                assert len(list(newSession.execute(preparedSelect.bind((1,))))) == 1

# The test prepareAndExecuteWithCustomExpressions was not translated, because
# it uses a custom index implemented by a Java class in Cassandra's test code.

# The test testMetadataFlagsWithLWTs was not translated, because it checks
# the flags in the result metadata of protocol messages, using Cassandra's
# internal protocol client.

# As explained in kb/lwt-differences.rst, Scylla is different from Cassandra
# in that it always returns the old values of the columns in the condition
# (or the whole old row, for IF NOT EXISTS), even if the condition was
# successful - where Cassandra returns just the success boolean. We decided
# to keep this difference, so the expected results depend on the server.
def prepareWithLWT(cql, test_keyspace, version):
    scylla = is_scylla(cql)
    with sessionNet(cql, version) as session:
        session.execute("USE " + test_keyspace)
        with create_table(cql, test_keyspace, "(pk int, v1 int, v2 int, PRIMARY KEY (pk))") as table:
            prepared1 = session.prepare(f"UPDATE {table} SET v1 = ?, v2 = ?  WHERE pk = 1 IF v1 = ?")
            prepared2 = session.prepare(f"INSERT INTO {table} (pk, v1, v2) VALUES (?, 200, 300) IF NOT EXISTS")
            execute(cql, table, "INSERT INTO %s (pk, v1, v2) VALUES (1,1,1)")
            execute(cql, table, "INSERT INTO %s (pk, v1, v2) VALUES (2,2,2)")

            rs = session.execute(prepared1.bind((10, 20, 1)))
            assert_rows(rs, row(True, 1) if scylla else row(True))
            assert len(rs.column_names) == (2 if scylla else 1)

            rs = session.execute(prepared1.bind((100, 200, 1)))
            assert_rows(rs, row(False, 10))
            assert len(rs.column_names) == 2

            rs = session.execute(prepared1.bind((30, 40, 10)))
            assert_rows(rs, row(True, 10) if scylla else row(True))
            assert len(rs.column_names) == (2 if scylla else 1)

            # Try executing the same message once again
            rs = session.execute(prepared1.bind((100, 200, 1)))
            assert_rows(rs, row(False, 30))
            assert len(rs.column_names) == 2

            rs = session.execute(prepared2.bind((1,)))
            assert_rows(rs, row(False, 1, 30, 40))
            assert len(rs.column_names) == 4

            execute(cql, table, "ALTER TABLE %s ADD v3 int;")

            rs = session.execute(prepared2.bind((1,)))
            assert_rows(rs, row(False, 1, 30, 40, None))
            assert len(rs.column_names) == 5

            rs = session.execute(prepared2.bind((20,)))
            assert_rows(rs, row(True, None, None, None, None) if scylla else row(True))
            assert len(rs.column_names) == (5 if scylla else 1)

            rs = session.execute(prepared2.bind((20,)))
            assert_rows(rs, row(False, 20, 200, 300, None))
            assert len(rs.column_names) == 5

# The Java test runs prepareWithLWT() with protocol versions 4 and 5
@pytest.mark.parametrize("version", [V4, V5])
def testPrepareWithLWT(cql, test_keyspace, version):
    prepareWithLWT(cql, test_keyspace, version)

# As in prepareWithLWT(), Scylla returns the old values of the row even if
# the conditions were successful. Moreover, for a batch Scylla returns one
# result row for each conditional statement, while Cassandra returns just one
# row (this is also explained in kb/lwt-differences.rst).
def prepareWithBatchLWT(cql, test_keyspace, version):
    scylla = is_scylla(cql)
    with sessionNet(cql, version) as session:
        session.execute("USE " + test_keyspace)
        with create_table(cql, test_keyspace, "(pk int, v1 int, v2 int, PRIMARY KEY (pk))") as table:
            prepared1 = session.prepare("BEGIN BATCH " +
                                        "UPDATE " + table + " SET v1 = ? WHERE pk = 1 IF v1 = ?;" +
                                        "UPDATE " + table + " SET v2 = ? WHERE pk = 1 IF v2 = ?;" +
                                        "APPLY BATCH;")
            prepared2 = session.prepare("BEGIN BATCH " +
                                        "INSERT INTO " + table + " (pk, v1, v2) VALUES (1, 200, 300) IF NOT EXISTS;" +
                                        "APPLY BATCH")
            execute(cql, table, "INSERT INTO %s (pk, v1, v2) VALUES (1,1,1)")
            execute(cql, table, "INSERT INTO %s (pk, v1, v2) VALUES (2,2,2)")

            rs = session.execute(prepared1.bind((10, 1, 20, 1)))
            if scylla:
                assert_rows(rs, row(True, 1, 1, 1), row(True, 1, 1, 1))
            else:
                assert_rows(rs, row(True))
            assert len(rs.column_names) == (4 if scylla else 1)

            rs = session.execute(prepared1.bind((100, 1, 200, 1)))
            if scylla:
                assert_rows(rs, row(False, 1, 10, 20), row(False, 1, 10, 20))
            else:
                assert_rows(rs, row(False, 1, 10, 20))
            assert len(rs.column_names) == 4

            # Try executing the same message once again
            rs = session.execute(prepared1.bind((100, 1, 200, 1)))
            if scylla:
                assert_rows(rs, row(False, 1, 10, 20), row(False, 1, 10, 20))
            else:
                assert_rows(rs, row(False, 1, 10, 20))
            assert len(rs.column_names) == 4

            rs = session.execute(prepared2.bind(()))
            assert_rows(rs, row(False, 1, 10, 20))
            assert len(rs.column_names) == 4

            execute(cql, table, "ALTER TABLE %s ADD v3 int;")

            rs = session.execute(prepared2.bind(()))
            assert_rows(rs, row(False, 1, 10, 20, None))
            assert len(rs.column_names) == 5

# The Java test runs prepareWithBatchLWT() with protocol versions 4 and 5
@pytest.mark.parametrize("version", [V4, V5])
def testPrepareWithBatchLWT(cql, test_keyspace, version):
    prepareWithBatchLWT(cql, test_keyspace, version)

# The tests testPrepareWithAccordV4, testPrepareWithAccordV5 and
# testPrepareWithAccordCurrent were not translated, because they test Accord
# transactions, which Scylla doesn't support.
