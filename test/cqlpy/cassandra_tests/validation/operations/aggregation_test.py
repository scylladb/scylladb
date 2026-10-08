# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1


from ...porting import *

# Scylla's error messages for dropping a non-existent aggregate are
# different from Cassandra's: "No function named ks.f found" when no
# argument types are given, and "User function ks.f(int, text) doesn't exist"
# when they are. So we accept either.
def doesntExistMessage(name):
    return re.escape(f"Aggregate '{name}' doesn't exist") + "|" + re.escape(f"No function named {name} found") + "|" + re.escape(f"User function {name} doesn't exist")

def testNonExistingOnes(cql, test_keyspace):
    assert_invalid_throw_message_re(cql, test_keyspace, doesntExistMessage(f"{test_keyspace}.aggr_does_not_exist"),
                              InvalidRequest,
                              "DROP AGGREGATE " + test_keyspace + ".aggr_does_not_exist")

    assert_invalid_throw_message_re(cql, test_keyspace, doesntExistMessage(f"{test_keyspace}.aggr_does_not_exist(int, text)"),
                              InvalidRequest,
                              "DROP AGGREGATE " + test_keyspace + ".aggr_does_not_exist(int,text)")

    assert_invalid_throw_message_re(cql, test_keyspace, doesntExistMessage("keyspace_does_not_exist.aggr_does_not_exist"),
                              InvalidRequest,
                              "DROP AGGREGATE keyspace_does_not_exist.aggr_does_not_exist")

    assert_invalid_throw_message_re(cql, test_keyspace, doesntExistMessage("keyspace_does_not_exist.aggr_does_not_exist(int, text)"),
                              InvalidRequest,
                              "DROP AGGREGATE keyspace_does_not_exist.aggr_does_not_exist(int,text)")

    execute(cql, test_keyspace, "DROP AGGREGATE IF EXISTS " + test_keyspace + ".aggr_does_not_exist")
    execute(cql, test_keyspace, "DROP AGGREGATE IF EXISTS " + test_keyspace + ".aggr_does_not_exist(int,text)")
    execute(cql, test_keyspace, "DROP AGGREGATE IF EXISTS keyspace_does_not_exist.aggr_does_not_exist")
    execute(cql, test_keyspace, "DROP AGGREGATE IF EXISTS keyspace_does_not_exist.aggr_does_not_exist(int,text)")
