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

# The Java test reads the raw bytes of the value of column v, to check that
# it is empty (and not null). The Python driver returns an empty value of
# most types as None, like a null, so we read the value converted to a blob
# with the native function <type>AsBlob(), which returns an empty blob for an
# empty value, and null for a null.
def asBlob(typ):
    return "v" if typ == "blob" else typ + "AsBlob(v)"

def verify(cql, table, typ, emptyValue):
    result = list(execute(cql, table, "SELECT * FROM %s"))
    assert len(result) == 1
    assert "v" in result[0]._fields
    assert_rows(execute(cql, table, f"SELECT {asBlob(typ)} FROM %s"), row(b""))

    jsonNet = list(execute(cql, table, "SELECT JSON * FROM %s"))
    jsonRowNet = jsonNet[0][0]
    assert re.match(".*\"v\"\\s*:\\s*\"" + re.escape(emptyValue) + "\".*", jsonRowNet), jsonRowNet

    # The Java test also runs Cassandra's sstabledump tool on the table's
    # sstables, and checks its output for the empty value. We can't do this
    # through CQL.

def verifyPlainInsert(cql, table, typ, emptyValue):
    execute(cql, table, "TRUNCATE %s")

    # In most cases we cannot insert empty value when we do not bind variables
    # This is due to the current implementation of org.apache.cassandra.cql3.terms.Constants.Literal.testAssignment
    # execute("INSERT INTO %s (id, v) VALUES (1, '" + emptyValue + "')");
    # The Java test binds an empty ByteBuffer, but the Python driver can't
    # bind raw bytes to a column of a non-blob type, so we convert an empty
    # blob to the column's type with the native function blobAs<type>().
    if typ == "blob":
        execute(cql, table, "INSERT INTO %s (id, v) VALUES (1, ?)", b"")
    else:
        execute(cql, table, f"INSERT INTO %s (id, v) VALUES (1, blobAs{typ}(0x))")
    flush(cql, table)

    verify(cql, table, typ, emptyValue)

def verifyJsonInsert(cql, table, typ, emptyValue):
    execute(cql, table, "TRUNCATE %s")
    execute(cql, table, "INSERT INTO %s JSON '{\"id\":\"1\",\"v\":\"" + emptyValue + "\"}'")
    flush(cql, table)

    verify(cql, table, typ, emptyValue)

# The Java test also has tests for empty values of the types text, blob,
# date, smallint and time (testEmptyText, testEmptyBytes, testEmptyDate,
# testEmptySmallInt and testEmptyTime), which we did not translate: Each of
# them starts with an assumption that an empty value of the type is
# "meaningless" (Cassandra's AbstractType.isEmptyValueMeaningless()), which
# is false for these types, so Cassandra always skips them. It is false for
# different reasons: For text and blob, an empty value is a normal value (an
# empty string or blob), not something like a null. Date, smallint and time,
# on the other hand, don't have an empty value at all in Cassandra, which
# rejects an empty value of these types as invalid. Scylla does accept an
# empty value of these types, as it does for the other types tested here.

# Scylla rejects an empty string in INSERT JSON for a non-string type
# (see #7944 and #10625), while Cassandra accepts it as the type's empty
# value, which we consider a Cassandra bug (CASSANDRA-21745).
def verifyJsonInsertRejected(cql, table):
    with pytest.raises(InvalidRequest):
        execute(cql, table, "INSERT INTO %s JSON '{\"id\":\"1\",\"v\":\"\"}'")

# Each of the original Java tests checks an empty value written both with
# INSERT JSON with an empty string, and with a plain INSERT. Cassandra accepts
# an empty string in INSERT JSON as an empty value of any type, but issue
# #7944 considers this a Cassandra bug for non-string types, which Scylla
# shouldn't copy. So we split each test into two: test<Type> with the plain
# INSERT, and test<Type>Json with the INSERT JSON. For the non-string types
# for which Scylla rejects the empty string (#7944, #10625), the
# test<Type>Json tests expect an error, and are marked cassandra_bug.

# Reproduces SCYLLADB-5185 (SELECT JSON of an empty value fails)
def testEmptyInt(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v INT)") as table:
        verifyPlainInsert(cql, table, "int", "")

def testEmptyIntJson(cql, test_keyspace, cassandra_bug):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v INT)") as table:
        verifyJsonInsertRejected(cql, table)

# Reproduces SCYLLADB-5185 (SELECT JSON of an empty value fails)
def testEmptyTimestamp(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v TIMESTAMP)") as table:
        verifyPlainInsert(cql, table, "timestamp", "")

def testEmptyTimestampJson(cql, test_keyspace, cassandra_bug):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v TIMESTAMP)") as table:
        verifyJsonInsertRejected(cql, table)

def testEmptyUUID(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v UUID)") as table:
        verifyPlainInsert(cql, table, "uuid", "")

def testEmptyUUIDJson(cql, test_keyspace, cassandra_bug):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v UUID)") as table:
        verifyJsonInsertRejected(cql, table)

def testEmptyInetAddress(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v INET)") as table:
        verifyPlainInsert(cql, table, "inet", "")

def testEmptyInetAddressJson(cql, test_keyspace, cassandra_bug):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v INET)") as table:
        verifyJsonInsertRejected(cql, table)

# Reproduces SCYLLADB-5185 (SELECT JSON of an empty value fails)
def testEmptyLong(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v BIGINT)") as table:
        verifyPlainInsert(cql, table, "bigint", "")

def testEmptyLongJson(cql, test_keyspace, cassandra_bug):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v BIGINT)") as table:
        verifyJsonInsertRejected(cql, table)

# Reproduces SCYLLADB-5185 (SELECT JSON of an empty value fails)
def testEmptyDecimal(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v DECIMAL)") as table:
        verifyPlainInsert(cql, table, "decimal", "")

def testEmptyDecimalJson(cql, test_keyspace, cassandra_bug):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v DECIMAL)") as table:
        verifyJsonInsertRejected(cql, table)

# Reproduces SCYLLADB-5185 (SELECT JSON of an empty value fails)
def testEmptyDouble(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v DOUBLE)") as table:
        verifyPlainInsert(cql, table, "double", "")

def testEmptyDoubleJson(cql, test_keyspace, cassandra_bug):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v DOUBLE)") as table:
        verifyJsonInsertRejected(cql, table)

# Reproduces SCYLLADB-5185 (SELECT JSON of an empty value fails)
def testEmptyFloat(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v FLOAT)") as table:
        verifyPlainInsert(cql, table, "float", "")

def testEmptyFloatJson(cql, test_keyspace, cassandra_bug):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v FLOAT)") as table:
        verifyJsonInsertRejected(cql, table)

def testEmptyTimeUUID(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v TIMEUUID)") as table:
        verifyPlainInsert(cql, table, "timeuuid", "")

def testEmptyTimeUUIDJson(cql, test_keyspace, cassandra_bug):
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v TIMEUUID)") as table:
        verifyJsonInsertRejected(cql, table)
