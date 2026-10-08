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
from cassandra.protocol import Unauthorized

# Functions are created in a new keyspace (KEYSPACE_PER_TEST in the original
# Java test), which is dropped at the end of the test.
REPLICATION = "replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"

# Equivalent of the Java test's createFunction(): replaces the first "%s" in
# the query by a new unique function name in the given keyspace, creates the
# function and returns its name.
def createFunction(cql, keyspace, query):
    name = keyspace + "." + unique_name()
    cql.execute(query.replace("%s", name, 1))
    return name

def createFunctionOverload(cql, name, query):
    cql.execute(query.replace("%s", name, 1))

def shortFunctionName(name):
    return name.split(".")[1]

# Scylla's error messages for dropping a non-existent function are different
# from Cassandra's: "No function named ks.f found" when no argument types are
# given, and "User function ks.f(int, text) doesn't exist" when they are. So
# we accept either.
def doesntExistMessage(name):
    return (re.escape(f"Function '{name}' doesn't exist") + "|" +
            re.escape(f"No function named {name} found") + "|" +
            re.escape(f"User function {name} doesn't exist"))

# Cassandra rejects modifying functions in the system keyspace with an
# InvalidRequest error "System keyspace 'system' is not user-modifiable",
# while Scylla returns an Unauthorized error "system keyspace is not
# user-modifiable.", so we accept either.
def assertSystemKeyspaceNotModifiable(session, cmd):
    with pytest.raises((InvalidRequest, Unauthorized), match="(?i)system keyspace.* is not user-modifiable"):
        session.execute(cmd)

def testNonExistingOnes(cql, test_keyspace):
    KEYSPACE = test_keyspace
    assert_invalid_throw_message_re(cql, KEYSPACE, doesntExistMessage(f"{KEYSPACE}.func_does_not_exist"),
                                    InvalidRequest,
                                    "DROP FUNCTION " + KEYSPACE + ".func_does_not_exist")

    assert_invalid_throw_message_re(cql, KEYSPACE, doesntExistMessage(f"{KEYSPACE}.func_does_not_exist(int, text)"),
                                    InvalidRequest,
                                    "DROP FUNCTION " + KEYSPACE + ".func_does_not_exist(int, text)")

    assert_invalid_throw_message_re(cql, KEYSPACE, doesntExistMessage("keyspace_does_not_exist.func_does_not_exist"),
                                    InvalidRequest,
                                    "DROP FUNCTION keyspace_does_not_exist.func_does_not_exist")

    assert_invalid_throw_message_re(cql, KEYSPACE, doesntExistMessage("keyspace_does_not_exist.func_does_not_exist(int, text)"),
                                    InvalidRequest,
                                    "DROP FUNCTION keyspace_does_not_exist.func_does_not_exist(int, text)")

    execute(cql, KEYSPACE, "DROP FUNCTION IF EXISTS " + KEYSPACE + ".func_does_not_exist")
    execute(cql, KEYSPACE, "DROP FUNCTION IF EXISTS " + KEYSPACE + ".func_does_not_exist(int,text)")
    execute(cql, KEYSPACE, "DROP FUNCTION IF EXISTS keyspace_does_not_exist.func_does_not_exist")
    execute(cql, KEYSPACE, "DROP FUNCTION IF EXISTS keyspace_does_not_exist.func_does_not_exist(int,text)")
