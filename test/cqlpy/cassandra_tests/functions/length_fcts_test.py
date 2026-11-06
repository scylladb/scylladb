# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

from ..porting import *
from decimal import Decimal
import random

# The functions length() and octet_length() were added in Cassandra 6
# (CASSANDRA-20102), so these tests are marked new_to_cassandra_6.

# Reproduces SCYLLADB-5218 (length() and octet_length() functions)
@pytest.mark.xfail(reason="SCYLLADB-5218")
def testOctetLengthNonStrings(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(a tinyint primary key,"
                    + " b smallint,"
                    + " c int,"
                    + " d bigint,"
                    + " e float,"
                    + " f double,"
                    + " g decimal,"
                    + " h varint,"
                    + " i int)") as table:

        execute(cql, table, "INSERT INTO %s (a, b, c, d, e, f, g, h) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                1, 2, 3, 4, 5.2, 6.3, Decimal("6.3"), 4)

        assertRows(execute(cql, table, "SELECT OCTET_LENGTH(a), " +
                           "OCTET_LENGTH(b), " +
                           "OCTET_LENGTH(c), " +
                           "OCTET_LENGTH(d), " +
                           "OCTET_LENGTH(e), " +
                           "OCTET_LENGTH(f), " +
                           "OCTET_LENGTH(g), " +
                           "OCTET_LENGTH(h), " +
                           "OCTET_LENGTH(i) FROM %s"),
                   row(1, 2, 4, 8, 4, 8, 5, 1, None))

# Reproduces SCYLLADB-5218 (length() and octet_length() functions)
@pytest.mark.xfail(reason="SCYLLADB-5218")
def testStringLengthUTF8(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(key text primary key, value blob)") as table:
        # UTF-8 7 codepoint, 21 byte encoded string
        key = "こんにちは世界"
        execute(cql, table, "INSERT INTO %s (key) VALUES (?)", key)

        assertRows(execute(cql, table, "SELECT LENGTH(key), OCTET_LENGTH(key), OCTET_LENGTH(value) FROM %s where key = ?", key),
                   row(7, 21, None))

        # Quickly check that multiple arguments leads to an exception as expected
        assertInvalidMessage(cql, table, "Invalid number of arguments in call to function system.length",
                             "SELECT LENGTH(key, value) FROM %s where key = 'こんにちは世界'")
        assertInvalidMessage(cql, table, "Invalid call to function octet_length, none of its type signatures match",
                             "SELECT OCTET_LENGTH(key, value) FROM %s where key = 'こんにちは世界'")

# The Java test uses QuickTheories to check 1024 random strings of 32 to 100
# characters, with any code points. We check fewer strings, to keep the test
# fast. Note that Cassandra's length() returns Java's String.length(), the
# number of UTF-16 code units, so a code point outside the Basic
# Multilingual Plane counts as 2. Surrogate code points can't be encoded in
# UTF-8, so we don't generate them.
# Reproduces SCYLLADB-5218 (length() and octet_length() functions)
@pytest.mark.xfail(reason="SCYLLADB-5218")
def testOctetLengthStringFuzz(cql, test_keyspace, new_to_cassandra_6):
    with create_table(cql, test_keyspace, "(key text primary key, value blob)") as table:
        rand = random.Random(0)
        def random_code_point():
            while True:
                c = rand.randint(0, 0x10FFFF)
                if not 0xD800 <= c <= 0xDFFF:
                    return chr(c)
        for _ in range(100):
            randString = ''.join(random_code_point() for _ in range(rand.randint(32, 100)))
            sLen = len(randString.encode('utf-16-le')) // 2
            randBytes = randString.encode('utf-8')

            # UTF-8 length (code unit count) and byte length are often
            # different. Spot checked a few of these, and they are different
            # most of the time in this test - but testing that reproducibly
            # requires seeding that would decrease the test power...
            execute(cql, table, "INSERT INTO %s (key, value) VALUES (?, ?)", randString, randBytes)
            assertRows(execute(cql, table, "SELECT LENGTH(key), OCTET_LENGTH(key), OCTET_LENGTH(value) FROM %s where key = ?", randString),
                       row(sLen, len(randBytes), len(randBytes)))
