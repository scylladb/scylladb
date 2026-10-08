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
