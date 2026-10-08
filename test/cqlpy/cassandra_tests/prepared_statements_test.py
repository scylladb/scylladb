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
from ..util import cql_session

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
