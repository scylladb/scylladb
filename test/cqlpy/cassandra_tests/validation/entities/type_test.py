# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 6ca34f81386dc8f6020cdf2ea4246bca2a0896c5
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0

from cassandra_tests.porting import *

def testNonExistingOnes(cql, test_keyspace):
    # The Scylla and Cassandra error messages are slightly different, but both
    # include the type's full name and the word "exist":
    assert_invalid_message_re(cql, test_keyspace, f"{test_keyspace}.type_does_not_exist.* exist", "DROP TYPE " + test_keyspace + ".type_does_not_exist")
    assert_invalid_message(cql, test_keyspace, "keyspace_does_not_exist", "DROP TYPE keyspace_does_not_exist.type_does_not_exist")
    execute(cql, test_keyspace, "DROP TYPE IF EXISTS " + test_keyspace + ".type_does_not_exist")
    # Reproduces issue #9082 - if the keyspace doesn't exist, an error was
    # reported instead of just doing nothing:
    execute(cql, test_keyspace, "DROP TYPE IF EXISTS keyspace_does_not_exist.type_does_not_exist")

@pytest.mark.skip(reason="Issue #9300")
def testNowToUUIDCompatibility(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b uuid, PRIMARY KEY (a, b))") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (0, now())")
        assert len(list(execute(cql, table, "SELECT * FROM %s WHERE a=0 AND b < now()"))) == 1

def testDateCompatibility(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b timestamp, c bigint, d varint, PRIMARY KEY (a, b, c, d))") as table:
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (0, toUnixTimestamp(now()), toTimestamp(now()), toTimestamp(now()))")
        assert len(list(execute(cql, table, "SELECT * FROM %s WHERE a=0 AND b <= toUnixTimestamp(now())"))) == 1
        execute(cql, table, "INSERT INTO %s (a, b, c, d) VALUES (1, unixTimestampOf(now()), dateOf(now()), dateOf(now()))")
        assert len(list(execute(cql, table, "SELECT * FROM %s WHERE a=1 AND b <= toUnixTimestamp(now())"))) == 1

@pytest.mark.skip(reason="Issue #9300")
def testReversedTypeCompatibility(cql, test_keyspace):
    with create_table(cql, test_keyspace, "(a int, b timeuuid, PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b DESC)") as table:
        execute(cql, table, "INSERT INTO %s (a, b) VALUES (0, now())")
        assert len(list(execute(cql, table, "SELECT * FROM %s WHERE a=0 AND b < now()"))) == 1
