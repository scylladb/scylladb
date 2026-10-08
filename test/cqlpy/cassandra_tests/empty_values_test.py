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

# The Java test skips the tests of types for which an empty value isn't
# meaningless (Cassandra's AbstractType.isEmptyValueMeaningless()).
NOT_MEANINGLESS = {"text", "blob", "date", "smallint", "time"}

def assumeEmptyValueMeaningless(typ):
    if typ in NOT_MEANINGLESS:
        skip_env(f"An empty value of type {typ} isn't meaningless, so the original Java test is skipped")

# Each of the original Java tests checks an empty value written both with
# INSERT JSON with an empty string, and with a plain INSERT. Cassandra accepts
# an empty string in INSERT JSON as an empty value of any type, but issue
# #7944 considers this a Cassandra bug for non-string types, which Scylla
# shouldn't copy. So we split each test into two: test<Type> with the plain
# INSERT, and test<Type>Json with the INSERT JSON. If #7944 is fixed as
# proposed (rejecting the empty string), the test<Type>Json tests of
# non-string types should be changed to expect an error, and be marked
# cassandra_bug.

# Reproduces SCYLLADB-5185 (SELECT JSON of an empty value fails)
@pytest.mark.xfail(reason="SCYLLADB-5185")
def testEmptyInt(cql, test_keyspace):
    assumeEmptyValueMeaningless("int")
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v INT)") as table:
        verifyPlainInsert(cql, table, "int", "")

# Reproduces #7944 (Scylla rejects the empty string in INSERT JSON for this
# type, but with a server error) and SCYLLADB-5185 (SELECT JSON of an empty
# value fails)
@pytest.mark.xfail(reason="#7944, SCYLLADB-5185")
def testEmptyIntJson(cql, test_keyspace):
    assumeEmptyValueMeaningless("int")
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v INT)") as table:
        verifyJsonInsert(cql, table, "int", "")

def testEmptyText(cql, test_keyspace):
    assumeEmptyValueMeaningless("text")
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v TEXT)") as table:
        verifyPlainInsert(cql, table, "text", "")

def testEmptyTextJson(cql, test_keyspace):
    assumeEmptyValueMeaningless("text")
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v TEXT)") as table:
        verifyJsonInsert(cql, table, "text", "")

def testEmptyBytes(cql, test_keyspace):
    assumeEmptyValueMeaningless("blob")
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v BLOB)") as table:
        verifyPlainInsert(cql, table, "blob", "")

def testEmptyBytesJson(cql, test_keyspace):
    assumeEmptyValueMeaningless("blob")
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v BLOB)") as table:
        verifyJsonInsert(cql, table, "blob", "0x")

def testEmptyDate(cql, test_keyspace):
    assumeEmptyValueMeaningless("date")
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v DATE)") as table:
        verifyPlainInsert(cql, table, "date", "")

def testEmptyDateJson(cql, test_keyspace):
    assumeEmptyValueMeaningless("date")
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v DATE)") as table:
        verifyJsonInsert(cql, table, "date", "")

def testEmptySmallInt(cql, test_keyspace):
    assumeEmptyValueMeaningless("smallint")
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v SMALLINT)") as table:
        verifyPlainInsert(cql, table, "smallint", "")

def testEmptySmallIntJson(cql, test_keyspace):
    assumeEmptyValueMeaningless("smallint")
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v SMALLINT)") as table:
        verifyJsonInsert(cql, table, "smallint", "")

def testEmptyTime(cql, test_keyspace):
    assumeEmptyValueMeaningless("time")
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v TIME)") as table:
        verifyPlainInsert(cql, table, "time", "")

def testEmptyTimeJson(cql, test_keyspace):
    assumeEmptyValueMeaningless("time")
    with create_table(cql, test_keyspace, "(id INT PRIMARY KEY, v TIME)") as table:
        verifyJsonInsert(cql, table, "time", "")
