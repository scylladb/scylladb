# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This file contains the translation of Cassandra's ReservedKeywordsTest.java
# and of KeywordTestBase.java, whose test KeywordSplit1Test.java and
# KeywordSplitTest.java run on two halves of the keywords (in parallel).
# They are translated together because they share the following lists of
# keywords, which the Java tests get from Cassandra's generated parser.

from .porting import *
from ..util import new_test_keyspace, new_materialized_view

# Cassandra's CQL keywords: the names of the K_ tokens in Cassandra's
# src/antlr/Lexer.g. The keywords BETWEEN, CHECK, COLUMN, COMMENT, COMMENTS,
# COMMIT, END, FIELD, FOR, GENERATED, INDEXES, LABEL, LABELS, LET, SECURITY,
# SUPERUSERS, THEN and TRANSACTION are new in Cassandra 6, but they aren't
# reserved, so for Cassandra 5 they are just ordinary identifiers.
KEYWORDS = [
    "ACCESS", "ADD", "AGGREGATE", "AGGREGATES", "ALL", "ALLOW", "ALTER",
    "AND", "ANN", "APPLY", "AS", "ASC", "ASCII", "AUTHORIZE", "BATCH",
    "BEGIN", "BETWEEN", "BIGINT", "BLOB", "BOOLEAN", "BY", "CALLED", "CAST",
    "CHECK", "CIDRS", "CLUSTER", "CLUSTERING", "COLUMN", "COLUMNFAMILY",
    "COMMENT", "COMMENTS", "COMMIT", "COMPACT", "CONTAINS", "COUNT",
    "COUNTER", "CREATE", "CUSTOM", "DATACENTERS", "DATE", "DECIMAL",
    "DEFAULT", "DELETE", "DESC", "DESCRIBE", "DISTINCT", "DOUBLE", "DROP",
    "DURATION", "END", "ENTRIES", "EXECUTE", "EXISTS", "FIELD", "FILTERING",
    "FINALFUNC", "FLOAT", "FOR", "FROM", "FROZEN", "FULL", "FUNCTION",
    "FUNCTIONS", "GENERATED", "GRANT", "GROUP", "HASHED", "IDENTITY", "IF",
    "IN", "INDEX", "INDEXES", "INET", "INITCOND", "INPUT", "INSERT", "INT",
    "INTERNALS", "INTO", "IS", "JSON", "KEY", "KEYS", "KEYSPACE",
    "KEYSPACES", "LABEL", "LABELS", "LANGUAGE", "LET", "LIKE", "LIMIT",
    "LIST", "LOGIN", "MAP", "MASKED", "MATERIALIZED", "MAXWRITETIME",
    "MBEAN", "MBEANS", "MODIFY", "NEGATIVE_INFINITY", "NEGATIVE_NAN",
    "NOLOGIN", "NORECURSIVE", "NOSUPERUSER", "NOT", "NULL", "OF", "ON",
    "ONLY", "OPTIONS", "OR", "ORDER", "PARTITION", "PASSWORD", "PER",
    "PERMISSION", "PERMISSIONS", "POSITIVE_INFINITY", "POSITIVE_NAN",
    "PRIMARY", "RENAME", "REPLACE", "RETURNS", "REVOKE", "ROLE", "ROLES",
    "SCHEMA", "SECURITY", "SELECT", "SELECT_MASKED", "SET", "SFUNC",
    "SMALLINT", "STATIC", "STORAGE", "STYPE", "SUPERUSER", "SUPERUSERS",
    "TABLES", "TEXT", "THEN", "TIME", "TIMESTAMP", "TIMEUUID", "TINYINT",
    "TO", "TOKEN", "TRANSACTION", "TRIGGER", "TRUNCATE", "TTL", "TUPLE",
    "TYPE", "TYPES", "UNLOGGED", "UNMASK", "UNSET", "UPDATE", "USE", "USER",
    "USERS", "USING", "UUID", "VALUES", "VARCHAR", "VARINT", "VECTOR",
    "VIEW", "WHERE", "WITH", "WRITETIME"
]

# Cassandra's reserved keywords, from Cassandra's
# src/resources/org/apache/cassandra/cql3/reserved_keywords.txt (the same in
# Cassandra 5 and 6). INFINITY, NAN and TABLE aren't names of tokens in
# KEYWORDS (they are the text of the tokens POSITIVE_INFINITY, POSITIVE_NAN
# and COLUMNFAMILY).
RESERVED_KEYWORDS = {
    "ADD", "ALLOW", "ALTER", "AND", "APPLY", "ASC", "AUTHORIZE", "BATCH",
    "BEGIN", "BY", "COLUMNFAMILY", "CREATE", "DELETE", "DESC", "DESCRIBE",
    "DROP", "ENTRIES", "EXECUTE", "FROM", "FULL", "GRANT", "IF", "IN",
    "INDEX", "INFINITY", "INSERT", "INTO", "IS", "KEYSPACE", "LIMIT",
    "MATERIALIZED", "MODIFY", "NAN", "NORECURSIVE", "NOT", "NULL", "OF",
    "ON", "OR", "ORDER", "PRIMARY", "RENAME", "REVOKE", "SCHEMA", "SELECT",
    "SET", "TABLE", "TO", "TOKEN", "TRUNCATE", "UNLOGGED", "UPDATE", "USE",
    "USING", "VIEW", "WHERE", "WITH"
}

# Cassandra reserves DESC, DESCRIBE and EXECUTE, but Scylla's grammar lists
# them as unreserved keywords, so Scylla allows them as identifiers. Being
# more permissive than Cassandra is harmless, so on Scylla the tests below
# expect these keywords to be allowed.
SCYLLA_UNRESERVED_KEYWORDS = {"DESC", "DESCRIBE", "EXECUTE"}

# Cassandra allows the keywords ANN, CAST, DEFAULT, REPLACE and UNSET as
# identifiers (CAST since before 4.0, DEFAULT, REPLACE and UNSET since 4.0 -
# see CASSANDRA-16439 - and ANN since it was added in 5.0), but Scylla
# reserves them.
SCYLLA_RESERVED_KEYWORDS = {"ANN", "CAST", "DEFAULT", "REPLACE", "UNSET"}

def isReserved(cql, keyword):
    if is_scylla(cql) and keyword in SCYLLA_UNRESERVED_KEYWORDS:
        return False
    return keyword in RESERVED_KEYWORDS

# The Java tests only parse the statements, without executing them. We
# execute them, against a keyspace or user which doesn't exist: a statement
# which parses fails with a different error than SyntaxException.
def parses(cql, statement):
    try:
        cql.execute(statement)
    except SyntaxException:
        return False
    except Exception:
        pass
    return True

def isAllowed(cql, keyword):
    return parses(cql, f"ALTER TABLE ks.t ADD {keyword} TEXT")

#############################################################################
# Translation of ReservedKeywordsTest.java

def testReservedWordsForColumns(cql):
    for reservedWord in RESERVED_KEYWORDS:
        if is_scylla(cql) and reservedWord in SCYLLA_UNRESERVED_KEYWORDS:
            continue
        assert not isAllowed(cql, reservedWord), f"Reserved keyword {reservedWord} should not have parsed"

# Reproduces SCYLLADB-5189 (Scylla reserves ANN, CAST, DEFAULT, REPLACE and
# UNSET, which Cassandra allows as identifiers).
@pytest.mark.xfail(reason="SCYLLADB-5189")
def testparserAndTextFileMatch(cql):
    # If this test starts to fail that means that the lexer added a new keyword, and this keyword was not updated
    # to be unreserved.
    #
    # To mark a keyword as unreserved, open "Parser.g" and search for
    #    basic_unreserved_keyword returns [String str]
    # or
    #    unreserved_keyword returns [String str]
    # Add your keyword there and rebuild the jar (to generate the parser).
    #
    # If it is desired to make this keyword reserved, then you must first go to the mailing list and request a vote
    # on this change, if that vote passes then you can update "reserved_keywords.txt" (and pylib/cqlshlib/cqlhandling.py::cql_keywords_reserved).
    # Never update "reserved_keywords.txt" without a vote on the mailing list!
    mismatches = [keyword for keyword in KEYWORDS if isReserved(cql, keyword) != (not isAllowed(cql, keyword))]
    assert mismatches == []

# Legacy USER and IDENTITY statements must accept unreserved keywords as names, just like
# role statements do (roleName accepts unreserved_keyword). Otherwise adding
# a new keyword to the grammar breaks existing deployments with a user or identity of that name.
# Scylla doesn't support Cassandra's IDENTITY statements (for mTLS
# authentication), so the "DROP IDENTITY %s" checks were not translated.
# Cassandra allows this only since Cassandra 6 (CASSANDRA-21510).
# Reproduces SCYLLADB-5189 (Scylla reserves ANN, CAST, DEFAULT, REPLACE and
# UNSET, which Cassandra allows as identifiers).
@pytest.mark.xfail(reason="SCYLLADB-5189")
def testUnreservedKeywordsAsUserNameAndIdentity(cql, new_to_cassandra_6):
    failures = []
    for keyword in KEYWORDS:
        if isReserved(cql, keyword):
            continue
        for statement in ["DROP USER %s"]:
            if not parses(cql, statement % keyword):
                failures.append(statement % keyword)
    assert failures == []

#############################################################################
# Translation of KeywordTestBase.java

# The Java test, for each keyword, tries to create a table whose name is the
# keyword, with the keyword also as the name of a clustering column. For a
# reserved keyword this must fail. For an unreserved keyword, the test checks
# that the table can be re-created from its DESCRIBE output, and that the
# keyword can be used to write, read (with the keyword in the WHERE clause)
# and create a materialized view. Doing all of this for each of the ~120
# unreserved keywords takes many seconds, so instead we use many keywords at
# once as the clustering columns of a few tables, and check the keywords
# as table names by creating them in a keyspace which doesn't exist (the
# statement must fail, but not with a SyntaxException).
NONEXISTENT_KEYSPACE = "nonexistent_keyspace"

def keywordTest(cql, test_keyspace, keywords):
    failures = []
    unreserved = []
    for keyword in keywords:
        createStatement = f"CREATE TABLE {NONEXISTENT_KEYSPACE}.{keyword} (c text, {keyword} text, PRIMARY KEY (c, {keyword}))"
        if isReserved(cql, keyword):
            if parses(cql, createStatement):
                failures.append(f"Reserved keyword {keyword} should not have parsed")
        else:
            if not parses(cql, createStatement):
                failures.append(f"Unreserved keyword {keyword} should have parsed")
            unreserved.append(keyword)
    assert failures == []

    # Scylla limits the number of relations in a WHERE clause (by default,
    # to 100), so we use a separate table for each group of 50 keywords.
    for i in range(0, len(unreserved), 50):
        keywordsTableTest(cql, test_keyspace, unreserved[i:i+50])

def keywordsTableTest(cql, test_keyspace, unreserved):
    names = ", ".join(unreserved)
    with create_table(cql, test_keyspace, f"(c text, {', '.join(k + ' text' for k in unreserved)}, PRIMARY KEY (c, {names}))") as table:
        # Call describe and re-create the table, in another keyspace, using
        # the create_statement result.
        describedCreateStatement = cql.execute(f"DESCRIBE TABLE {table}").one().create_statement
        with new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as keyspace2:
            recreateStatement = describedCreateStatement.replace(test_keyspace + ".", keyspace2 + ".")
            assert recreateStatement != describedCreateStatement
            cql.execute(recreateStatement)

        # Check it is possible to insert and select from the table/column, with the keyword used in the
        # where clause
        execute(cql, table, f"INSERT INTO %s(c, {names}) VALUES ('x', {', '.join(repr(k) for k in unreserved)})")
        rows = execute(cql, table, f"SELECT c, {names} FROM %s WHERE c = 'x' AND ({names}) >= ({', '.join(chr(39)*2 for k in unreserved)})")
        # (We don't use assert_rows() here, because one of the columns is
        # called "values", which hides the row's values() method.)
        assert [tuple(row) for row in rows] == [tuple(["x"] + unreserved)]

        # Make a materialized view using the fields.
        # Added as CASSANDRA-11803 motivated adding the reserved/unreserved distinction
        # (Unlike the Java test, the view's WHERE clause can't copy the
        # SELECT's restrictions, because a view doesn't allow a multi-column
        # restriction, so we just require the columns to be not null.)
        where = " AND ".join(f"{k} IS NOT NULL" for k in ["c"] + unreserved)
        with new_materialized_view(cql, table, f"c, {names}", f"c, {names}", where):
            pass

def test(cql, test_keyspace):
    keywordTest(cql, test_keyspace, [k for k in KEYWORDS if k not in SCYLLA_RESERVED_KEYWORDS])

# The same test, for the keywords which Scylla reserves but Cassandra doesn't.
# Reproduces SCYLLADB-5189 (Scylla reserves ANN, CAST, DEFAULT, REPLACE and
# UNSET, which Cassandra allows as identifiers).
@pytest.mark.xfail(reason="SCYLLADB-5189")
def testKeywordsReservedInScylla(cql, test_keyspace):
    keywordTest(cql, test_keyspace, sorted(SCYLLA_RESERVED_KEYWORDS))
