# This file was translated from the original Java test from the Apache
# Cassandra source repository, as of commit 4ab8bac4a51f8aef0d55b2497699e1291baeda4b
#
# The original Apache Cassandra license:
#
# SPDX-License-Identifier: Apache-2.0
#
# Modifications: Copyright 2026-present ScyllaDB
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

# This is a translation of GrantAndRevokeTest.java from Cassandra's
# test/unit/org/apache/cassandra/auth directory.

import re
import time
from contextlib import contextmanager
from ..porting import *
from ...util import unique_name, new_test_keyspace
from .create_and_alter_role_test import use_user, dropped_roles
from cassandra.protocol import Unauthorized

pass_ = "12345"

# The Java test runs each test in a new keyspace, KEYSPACE_PER_TEST, which
# uses SimpleStrategy. Scylla doesn't allow SimpleStrategy with tablets, so
# we use NetworkTopologyStrategy, and when the test alters the keyspace, it
# sets it to the same replication.
KEYSPACE_REPLICATION = "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"

@contextmanager
def keyspace_per_test(cql):
    with new_test_keyspace(cql, "WITH replication = " + KEYSPACE_REPLICATION) as ks:
        yield ks

def assertUnauthorizedQuery(session, message, query):
    with pytest.raises(Unauthorized, match=re.escape(message)):
        session.execute(query)

# The resource that the permission errors of ALTER TYPE and DROP TYPE
# mention: Cassandra checks these permissions on "<all tables in ks>",
# while Scylla checks them on "<keyspace ks>", so we accept both. This is
# a regular expression, for assertUnauthorizedQueryRE().
def type_resource(keyspace):
    return f"(<all tables in {keyspace}>|<keyspace {keyspace}>)"

def assertUnauthorizedQueryRE(session, message_re, query):
    with pytest.raises(Unauthorized, match=message_re):
        session.execute(query)

# Cassandra's Util.spinAssertEquals(false, () -> { try { check } catch
# (Throwable e) { return true; } return false; }, 10): repeat the check
# until it succeeds, for up to 10 seconds, to wait for permission changes to
# become effective (Cassandra and Scylla cache permissions).
def spin_assert(check, timeout=10):
    deadline = time.time() + timeout
    while True:
        try:
            check()
            return
        # pytest.raises() reports a missing exception with pytest.fail.Exception,
        # which isn't derived from Exception.
        except (Exception, pytest.fail.Exception):
            if time.time() > deadline:
                raise
            time.sleep(0.1)

def create_user(cql, user):
    cql.execute("CREATE ROLE %s WITH LOGIN = TRUE AND password='%s'" % (user, pass_))

# Reproduces SCYLLADB-5145: GRANT and REVOKE of multiple permissions in one
# statement aren't supported.
@pytest.mark.xfail(reason="SCYLLADB-5145")
def testGrantedKeyspace(cql):
    user = "user_" + unique_name()
    with keyspace_per_test(cql) as KEYSPACE_PER_TEST, dropped_roles(cql, user):
        create_user(cql, user)
        cql.execute("GRANT CREATE ON KEYSPACE " + KEYSPACE_PER_TEST + " TO " + user)
        table_name = unique_name()
        table = KEYSPACE_PER_TEST + '.' + table_name
        cql.execute("CREATE TABLE %s (pk int, ck int, val int, val_2 text, PRIMARY KEY (pk, ck))" % table)
        cql.execute("CREATE INDEX ON %s (val_2)" % table)
        index = KEYSPACE_PER_TEST + '.' + table_name + "_val_2_idx"
        type = KEYSPACE_PER_TEST + '.' + unique_name()
        cql.execute("CREATE TYPE %s (a int, b text)" % type)
        mv = KEYSPACE_PER_TEST + ".ks_mv_01"
        cql.execute("CREATE MATERIALIZED VIEW " + mv + " AS SELECT * FROM " + table + " WHERE val IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (val, pk, ck)")

        with use_user(cql, user, pass_) as session:
            # ALTER and DROP tables created by somebody else
            # Spin assert for effective auth changes.
            spin_assert(lambda: assertUnauthorizedQuery(session, "User " + user + " has no MODIFY permission on <table " + table + "> or any of its parents",
                                                        "INSERT INTO %s (pk, ck, val, val_2) VALUES (1, 1, 1, '1')" % table))
            assertUnauthorizedQuery(session, "User " + user + " has no MODIFY permission on <table " + table + "> or any of its parents",
                                    "UPDATE %s SET val = 1 WHERE pk = 1 AND ck = 1" % table)
            assertUnauthorizedQuery(session, "User " + user + " has no MODIFY permission on <table " + table + "> or any of its parents",
                                    "DELETE FROM %s WHERE pk = 1 AND ck = 2" % table)
            assertUnauthorizedQuery(session, "User " + user + " has no SELECT permission on <table " + table + "> or any of its parents",
                                    "SELECT * FROM %s WHERE pk = 1 AND ck = 1" % table)
            assertUnauthorizedQuery(session, "User " + user + " has no SELECT permission on <table " + table + "> or any of its parents",
                                    "SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1 AND ck = 1")
            assertUnauthorizedQuery(session, "User " + user + " has no MODIFY permission on <table " + table + "> or any of its parents",
                                    "TRUNCATE TABLE %s" % table)
            assertUnauthorizedQuery(session, "User " + user + " has no ALTER permission on <table " + table + "> or any of its parents",
                                    "ALTER TABLE %s ADD val_3 int" % table)
            assertUnauthorizedQuery(session, "User " + user + " has no DROP permission on <table " + table + "> or any of its parents",
                                    "DROP TABLE %s" % table)
            assertUnauthorizedQueryRE(session, "User " + user + " has no ALTER permission on " + type_resource(KEYSPACE_PER_TEST) + " or any of its parents",
                                    "ALTER TYPE " + type + " ADD c bigint")
            assertUnauthorizedQueryRE(session, "User " + user + " has no DROP permission on " + type_resource(KEYSPACE_PER_TEST) + " or any of its parents",
                                    "DROP TYPE " + type)
            assertUnauthorizedQuery(session, "User " + user + " has no ALTER permission on <table " + table + "> or any of its parents",
                                    "DROP MATERIALIZED VIEW " + mv)
            assertUnauthorizedQuery(session, "User " + user + " has no ALTER permission on <table " + table + "> or any of its parents",
                                    "DROP INDEX " + index)

        cql.execute("GRANT ALTER, DROP, SELECT, MODIFY ON KEYSPACE " + KEYSPACE_PER_TEST + " TO " + user)

        with use_user(cql, user, pass_) as session:
            # Spin assert for effective auth changes.
            spin_assert(lambda: session.execute("ALTER KEYSPACE " + KEYSPACE_PER_TEST + " WITH replication = " + KEYSPACE_REPLICATION))

            session.execute("INSERT INTO %s (pk, ck, val, val_2) VALUES (1, 1, 1, '1')" % table)
            session.execute("UPDATE %s SET val = 1 WHERE pk = 1 AND ck = 1" % table)
            session.execute("DELETE FROM %s WHERE pk = 1 AND ck = 2" % table)
            assertRows(session.execute("SELECT * FROM %s WHERE pk = 1 AND ck = 1" % table), row(1, 1, 1, "1"))
            assertRows(session.execute("SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1"), row(1, 1, 1, "1"))
            session.execute("TRUNCATE TABLE %s" % table)
            session.execute("ALTER TABLE %s ADD val_3 int" % table)
            session.execute("DROP MATERIALIZED VIEW " + mv)
            session.execute("DROP INDEX " + index)
            session.execute("DROP TABLE %s" % table)
            session.execute("ALTER TYPE " + type + " ADD c bigint")
            session.execute("DROP TYPE " + type)

            # calling creatTableName to create a new table name that will be used by the formatQuery
            table = KEYSPACE_PER_TEST + '.' + unique_name()
            type = KEYSPACE_PER_TEST + "." + unique_name()
            mv = KEYSPACE_PER_TEST + ".ks_mv_02"
            session.execute("CREATE TYPE " + type + " (a int, b text)")
            session.execute("CREATE TABLE %s (pk int, ck int, val int, val_2 text, PRIMARY KEY (pk, ck))" % table)
            session.execute("CREATE MATERIALIZED VIEW " + mv + " AS SELECT * FROM " + table + " WHERE val IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (val, pk, ck)")
            session.execute("INSERT INTO %s (pk, ck, val, val_2) VALUES (1, 1, 1, '1')" % table)
            session.execute("UPDATE %s SET val = 1 WHERE pk = 1 AND ck = 1" % table)
            session.execute("DELETE FROM %s WHERE pk = 1 AND ck = 2" % table)
            assertRows(session.execute("SELECT * FROM %s WHERE pk = 1 AND ck = 1" % table), row(1, 1, 1, "1"))
            assertRows(session.execute("SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1"), row(1, 1, 1, "1"))
            session.execute("TRUNCATE TABLE %s" % table)
            session.execute("ALTER TABLE %s ADD val_3 int" % table)
            session.execute("DROP MATERIALIZED VIEW " + mv)
            session.execute("DROP TABLE %s" % table)
            session.execute("ALTER TYPE " + type + " ADD c bigint")
            session.execute("DROP TYPE " + type)

        cql.execute("REVOKE ALTER, DROP, MODIFY, SELECT ON KEYSPACE " + KEYSPACE_PER_TEST + " FROM " + user)

        table_name = unique_name()
        table = KEYSPACE_PER_TEST + "." + table_name
        cql.execute("CREATE TABLE %s (pk int, ck int, val int, val_2 text, PRIMARY KEY (pk, ck))" % table)
        type = KEYSPACE_PER_TEST + "." + unique_name()
        cql.execute("CREATE TYPE %s (a int, b text)" % type)
        cql.execute("CREATE INDEX ON %s (val_2)" % table)
        index = KEYSPACE_PER_TEST + '.' + table_name + "_val_2_idx"
        mv = KEYSPACE_PER_TEST + ".ks_mv_03"
        cql.execute("CREATE MATERIALIZED VIEW " + mv + " AS SELECT * FROM " + table + " WHERE val IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (val, pk, ck)")

        with use_user(cql, user, pass_) as session:
            # Spin assert for effective auth changes.
            spin_assert(lambda: assertUnauthorizedQuery(session, "User " + user + " has no MODIFY permission on <table " + table + "> or any of its parents",
                                                        "INSERT INTO " + table + " (pk, ck, val, val_2) VALUES (1, 1, 1, '1')"))
            assertUnauthorizedQuery(session, "User " + user + " has no MODIFY permission on <table " + table + "> or any of its parents",
                                    "UPDATE " + table + " SET val = 1 WHERE pk = 1 AND ck = 1")
            assertUnauthorizedQuery(session, "User " + user + " has no MODIFY permission on <table " + table + "> or any of its parents",
                                    "DELETE FROM " + table + " WHERE pk = 1 AND ck = 2")
            assertUnauthorizedQuery(session, "User " + user + " has no SELECT permission on <table " + table + "> or any of its parents",
                                    "SELECT * FROM " + table + " WHERE pk = 1 AND ck = 1")
            assertUnauthorizedQuery(session, "User " + user + " has no SELECT permission on <table " + table + "> or any of its parents",
                                    "SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1 AND ck = 1")
            assertUnauthorizedQuery(session, "User " + user + " has no MODIFY permission on <table " + table + "> or any of its parents",
                                    "TRUNCATE TABLE " + table)
            assertUnauthorizedQuery(session, "User " + user + " has no ALTER permission on <table " + table + "> or any of its parents",
                                    "ALTER TABLE " + table + " ADD val_3 int")
            assertUnauthorizedQuery(session, "User " + user + " has no DROP permission on <table " + table + "> or any of its parents",
                                    "DROP TABLE " + table)
            assertUnauthorizedQueryRE(session, "User " + user + " has no ALTER permission on " + type_resource(KEYSPACE_PER_TEST) + " or any of its parents",
                                    "ALTER TYPE " + type + " ADD c bigint")
            assertUnauthorizedQueryRE(session, "User " + user + " has no DROP permission on " + type_resource(KEYSPACE_PER_TEST) + " or any of its parents",
                                    "DROP TYPE " + type)
            assertUnauthorizedQuery(session, "User " + user + " has no ALTER permission on <table " + table + "> or any of its parents",
                                    "DROP MATERIALIZED VIEW " + mv)
            assertUnauthorizedQuery(session, "User " + user + " has no ALTER permission on <table " + table + "> or any of its parents",
                                    "DROP INDEX " + index)

# Reproduces SCYLLADB-5230: GRANT and REVOKE ON ALL TABLES IN KEYSPACE
# (added in Cassandra 4.1, CASSANDRA-17027) aren't supported.
@pytest.mark.xfail(reason="SCYLLADB-5230")
def testGrantedAllTables(cql):
    user = "user_" + unique_name()
    with keyspace_per_test(cql) as KEYSPACE_PER_TEST, dropped_roles(cql, user):
        create_user(cql, user)
        cql.execute("GRANT CREATE ON ALL TABLES IN KEYSPACE " + KEYSPACE_PER_TEST + " TO " + user)
        table_name = unique_name()
        table = KEYSPACE_PER_TEST + "." + table_name
        cql.execute("CREATE TABLE %s (pk int, ck int, val int, val_2 text, PRIMARY KEY (pk, ck))" % table)
        cql.execute("CREATE INDEX ON %s (val_2)" % table)
        index = KEYSPACE_PER_TEST + '.' + table_name + "_val_2_idx"
        type = KEYSPACE_PER_TEST + "." + unique_name()
        cql.execute("CREATE TYPE %s (a int, b text)" % type)
        mv = KEYSPACE_PER_TEST + ".alltables_mv_01"
        cql.execute("CREATE MATERIALIZED VIEW " + mv + " AS SELECT * FROM " + table + " WHERE val IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (val, pk, ck)")

        with use_user(cql, user, pass_) as session:
            # ALTER and DROP tables created by somebody else
            # Spin assert for effective auth changes.
            spin_assert(lambda: assertUnauthorizedQuery(session, "User " + user + " has no MODIFY permission on <table " + table + "> or any of its parents",
                                                        "INSERT INTO %s (pk, ck, val, val_2) VALUES (1, 1, 1, '1')" % table))
            assertUnauthorizedQuery(session, "User " + user + " has no MODIFY permission on <table " + table + "> or any of its parents",
                                    "UPDATE %s SET val = 1 WHERE pk = 1 AND ck = 1" % table)
            assertUnauthorizedQuery(session, "User " + user + " has no MODIFY permission on <table " + table + "> or any of its parents",
                                    "DELETE FROM %s WHERE pk = 1 AND ck = 2" % table)
            assertUnauthorizedQuery(session, "User " + user + " has no SELECT permission on <table " + table + "> or any of its parents",
                                    "SELECT * FROM %s WHERE pk = 1 AND ck = 1" % table)
            assertUnauthorizedQuery(session, "User " + user + " has no SELECT permission on <table " + table + "> or any of its parents",
                                    "SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1 AND ck = 1")
            assertUnauthorizedQuery(session, "User " + user + " has no MODIFY permission on <table " + table + "> or any of its parents",
                                    "TRUNCATE TABLE %s" % table)
            assertUnauthorizedQuery(session, "User " + user + " has no ALTER permission on <table " + table + "> or any of its parents",
                                    "ALTER TABLE %s ADD val_3 int" % table)
            assertUnauthorizedQuery(session, "User " + user + " has no DROP permission on <table " + table + "> or any of its parents",
                                    "DROP TABLE %s" % table)
            assertUnauthorizedQueryRE(session, "User " + user + " has no ALTER permission on " + type_resource(KEYSPACE_PER_TEST) + " or any of its parents",
                                    "ALTER TYPE " + type + " ADD c bigint")
            assertUnauthorizedQueryRE(session, "User " + user + " has no DROP permission on " + type_resource(KEYSPACE_PER_TEST) + " or any of its parents",
                                    "DROP TYPE " + type)
            assertUnauthorizedQuery(session, "User " + user + " has no ALTER permission on <table " + table + "> or any of its parents",
                                    "DROP MATERIALIZED VIEW " + mv)
            assertUnauthorizedQuery(session, "User " + user + " has no ALTER permission on <table " + table + "> or any of its parents",
                                    "DROP INDEX " + index)

        cql.execute("GRANT ALTER, DROP, SELECT, MODIFY ON ALL TABLES IN KEYSPACE " + KEYSPACE_PER_TEST + " TO " + user)

        with use_user(cql, user, pass_) as session:
            # Spin assert for effective auth changes.
            spin_assert(lambda: assertUnauthorizedQuery(session, "User " + user + " has no ALTER permission on <keyspace " + KEYSPACE_PER_TEST + "> or any of its parents",
                                                        "ALTER KEYSPACE " + KEYSPACE_PER_TEST + " WITH replication = " + KEYSPACE_REPLICATION))
            # The above check succeeds also before the new permissions become
            # effective, so it doesn't wait for them. The Java test doesn't
            # need to, because it disables Cassandra's permissions cache, but
            # we can't, so we wait for the first write to succeed.
            spin_assert(lambda: session.execute("INSERT INTO %s (pk, ck, val, val_2) VALUES (1, 1, 1, '1')" % table))
            session.execute("UPDATE %s SET val = 1 WHERE pk = 1 AND ck = 1" % table)
            session.execute("DELETE FROM %s WHERE pk = 1 AND ck = 2" % table)
            assertRows(session.execute("SELECT * FROM %s WHERE pk = 1 AND ck = 1" % table), row(1, 1, 1, "1"))
            assertRows(session.execute("SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1"), row(1, 1, 1, "1"))
            session.execute("TRUNCATE TABLE %s" % table)
            session.execute("ALTER TABLE %s ADD val_3 int" % table)
            session.execute("DROP MATERIALIZED VIEW " + mv)
            session.execute("DROP INDEX " + index)
            session.execute("DROP TABLE %s" % table)
            session.execute("ALTER TYPE " + type + " ADD c bigint")
            session.execute("DROP TYPE " + type)

            # calling creatTableName to create a new table name that will be used by the formatQuery
            table_name = unique_name()
            table = KEYSPACE_PER_TEST + '.' + table_name
            type = KEYSPACE_PER_TEST + "." + unique_name()
            mv = KEYSPACE_PER_TEST + ".alltables_mv_02"
            session.execute("CREATE TYPE " + type + " (a int, b text)")
            session.execute("CREATE TABLE %s (pk int, ck int, val int, val_2 text, PRIMARY KEY (pk, ck))" % table)
            session.execute("CREATE INDEX ON %s (val_2)" % table)
            index = KEYSPACE_PER_TEST + '.' + table_name + "_val_2_idx"
            session.execute("CREATE MATERIALIZED VIEW " + mv + " AS SELECT * FROM " + table + " WHERE val IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (val, pk, ck)")
            session.execute("INSERT INTO %s (pk, ck, val, val_2) VALUES (1, 1, 1, '1')" % table)
            session.execute("UPDATE %s SET val = 1 WHERE pk = 1 AND ck = 1" % table)
            session.execute("DELETE FROM %s WHERE pk = 1 AND ck = 2" % table)
            assertRows(session.execute("SELECT * FROM %s WHERE pk = 1 AND ck = 1" % table), row(1, 1, 1, "1"))
            assertRows(session.execute("SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1"), row(1, 1, 1, "1"))
            session.execute("TRUNCATE TABLE %s" % table)
            session.execute("ALTER TABLE %s ADD val_3 int" % table)
            session.execute("DROP MATERIALIZED VIEW " + mv)
            session.execute("DROP INDEX " + index)
            session.execute("DROP TABLE %s" % table)
            session.execute("ALTER TYPE " + type + " ADD c bigint")
            session.execute("DROP TYPE " + type)

        cql.execute("REVOKE ALTER, DROP, SELECT, MODIFY ON ALL TABLES IN KEYSPACE " + KEYSPACE_PER_TEST + " FROM " + user)

        table_name = unique_name()
        table = KEYSPACE_PER_TEST + "." + table_name
        cql.execute("CREATE TABLE %s (pk int, ck int, val int, val_2 text, PRIMARY KEY (pk, ck))" % table)
        cql.execute("CREATE INDEX ON %s (val_2)" % table)
        index = KEYSPACE_PER_TEST + '.' + table_name + "_val_2_idx"
        type = KEYSPACE_PER_TEST + "." + unique_name()
        cql.execute("CREATE TYPE %s (a int, b text)" % type)
        mv = KEYSPACE_PER_TEST + ".alltables_mv_03"
        cql.execute("CREATE MATERIALIZED VIEW " + mv + " AS SELECT * FROM " + table + " WHERE val IS NOT NULL AND pk IS NOT NULL AND ck IS NOT NULL PRIMARY KEY (val, pk, ck)")

        with use_user(cql, user, pass_) as session:
            # Spin assert for effective auth changes.
            spin_assert(lambda: assertUnauthorizedQuery(session, "User " + user + " has no MODIFY permission on <table " + table + "> or any of its parents",
                                                        "INSERT INTO " + table + " (pk, ck, val, val_2) VALUES (1, 1, 1, '1')"))
            assertUnauthorizedQuery(session, "User " + user + " has no MODIFY permission on <table " + table + "> or any of its parents",
                                    "UPDATE " + table + " SET val = 1 WHERE pk = 1 AND ck = 1")
            assertUnauthorizedQuery(session, "User " + user + " has no MODIFY permission on <table " + table + "> or any of its parents",
                                    "DELETE FROM " + table + " WHERE pk = 1 AND ck = 2")
            assertUnauthorizedQuery(session, "User " + user + " has no SELECT permission on <table " + table + "> or any of its parents",
                                    "SELECT * FROM " + table + " WHERE pk = 1 AND ck = 1")
            assertUnauthorizedQuery(session, "User " + user + " has no SELECT permission on <table " + table + "> or any of its parents",
                                    "SELECT * FROM " + mv + " WHERE val = 1 AND pk = 1 AND ck = 1")
            assertUnauthorizedQuery(session, "User " + user + " has no MODIFY permission on <table " + table + "> or any of its parents",
                                    "TRUNCATE TABLE " + table)
            assertUnauthorizedQuery(session, "User " + user + " has no ALTER permission on <table " + table + "> or any of its parents",
                                    "ALTER TABLE " + table + " ADD val_3 int")
            assertUnauthorizedQuery(session, "User " + user + " has no DROP permission on <table " + table + "> or any of its parents",
                                    "DROP TABLE " + table)
            assertUnauthorizedQueryRE(session, "User " + user + " has no ALTER permission on " + type_resource(KEYSPACE_PER_TEST) + " or any of its parents",
                                    "ALTER TYPE " + type + " ADD c bigint")
            assertUnauthorizedQueryRE(session, "User " + user + " has no DROP permission on " + type_resource(KEYSPACE_PER_TEST) + " or any of its parents",
                                    "DROP TYPE " + type)
            assertUnauthorizedQuery(session, "User " + user + " has no ALTER permission on <table " + table + "> or any of its parents",
                                    "DROP MATERIALIZED VIEW " + mv)
            assertUnauthorizedQuery(session, "User " + user + " has no ALTER permission on <table " + table + "> or any of its parents",
                                    "DROP INDEX " + index)

def assertWarningsContain(result, message):
    warnings = result.response_future.warnings or []
    assert any(message in w for w in warnings), warnings

# Reproduces SCYLLADB-5231: GRANT and REVOKE which don't change anything
# don't return a warning (added in Cassandra 4.1, CASSANDRA-17333).
@pytest.mark.xfail(reason="SCYLLADB-5231")
def testWarnings(cql):
    user = "user_" + unique_name()
    with new_test_keyspace(cql, "WITH replication = " + KEYSPACE_REPLICATION) as revoke_yeah, dropped_roles(cql, user):
        cql.execute(f"CREATE TABLE {revoke_yeah}.t1 (id int PRIMARY KEY, val text)")
        cql.execute("CREATE USER '" + user + "' WITH PASSWORD '" + pass_ + "'")

        res = cql.execute(f"REVOKE CREATE ON KEYSPACE {revoke_yeah} FROM " + user)
        assertWarningsContain(res, "Role '" + user + f"' was not granted CREATE on <keyspace {revoke_yeah}>")

        res = cql.execute(f"GRANT SELECT ON KEYSPACE {revoke_yeah} TO " + user)
        assert not res.response_future.warnings

        res = cql.execute(f"GRANT SELECT ON KEYSPACE {revoke_yeah} TO " + user)
        assertWarningsContain(res, "Role '" + user + f"' was already granted SELECT on <keyspace {revoke_yeah}>")

        res = cql.execute(f"REVOKE SELECT ON TABLE {revoke_yeah}.t1 FROM " + user)
        assertWarningsContain(res, "Role '" + user + f"' was not granted SELECT on <table {revoke_yeah}.t1>")

        res = cql.execute(f"REVOKE SELECT, MODIFY ON KEYSPACE {revoke_yeah} FROM " + user)
        assertWarningsContain(res, "Role '" + user + f"' was not granted MODIFY on <keyspace {revoke_yeah}>")

# Reproduces SCYLLADB-5147: CREATE TABLE LIKE isn't supported.
@pytest.mark.xfail(reason="SCYLLADB-5147")
def testCreateTableLikeAuthorize(cql, new_to_cassandra_6):
    user = "user_" + unique_name()
    # two keyspaces
    with new_test_keyspace(cql, "WITH replication = " + KEYSPACE_REPLICATION) as ks1, \
         new_test_keyspace(cql, "WITH replication = " + KEYSPACE_REPLICATION) as ks2, \
         dropped_roles(cql, user):
        cql.execute(f"CREATE TABLE {ks1}.sourcetb (id int PRIMARY KEY, val text)")
        cql.execute("CREATE USER '" + user + "' WITH PASSWORD '" + pass_ + "'")

        # same keyspace
        # have no select permission on source table
        cql.execute(f"REVOKE SELECT ON TABLE {ks1}.sourcetb FROM " + user)

        with use_user(cql, user, pass_) as session:
            # Spin assert for effective auth changes.
            spin_assert(lambda: assertUnauthorizedQuery(session, "User " + user + f" has no SELECT permission on <table {ks1}.sourcetb> or any of its parents",
                                                        f"SELECT * FROM {ks1}.sourcetb LIMIT 1"))

            assertUnauthorizedQuery(session, "User " + user + f" has no SELECT permission on <table {ks1}.sourcetb> or any of its parents",
                                    f"CREATE TABLE {ks1}.targetTb LIKE {ks1}.sourcetb")

        # have select permission on source table and do not have create permission on target keyspace
        cql.execute(f"GRANT SELECT ON TABLE {ks1}.sourcetb TO " + user)
        cql.execute(f"REVOKE CREATE ON KEYSPACE {ks1} FROM " + user)

        with use_user(cql, user, pass_) as session:
            spin_assert(lambda: assertUnauthorizedQuery(session, "User " + user + f" has no CREATE permission on <all tables in {ks1}> or any of its parents",
                                                        f"CREATE TABLE {ks1}.targetTb LIKE {ks1}.sourcetb"))

            assertUnauthorizedQuery(session, "User " + user + f" has no CREATE permission on <all tables in {ks1}> or any of its parents",
                                    f"CREATE TABLE {ks1}.targetTb LIKE {ks1}.sourcetb")

        # different keyspaces
        # have select permission on source table and do not have create permission on target keyspace
        cql.execute(f"GRANT SELECT ON TABLE {ks1}.sourcetb TO " + user)
        cql.execute(f"REVOKE CREATE ON KEYSPACE {ks2} FROM " + user)

        with use_user(cql, user, pass_) as session:
            spin_assert(lambda: assertUnauthorizedQuery(session, "User " + user + f" has no CREATE permission on <all tables in {ks2}> or any of its parents",
                                                        f"CREATE TABLE {ks2}.targetTb LIKE {ks1}.sourcetb"))

            assertUnauthorizedQuery(session, "User " + user + f" has no CREATE permission on <all tables in {ks2}> or any of its parents",
                                    f"CREATE TABLE {ks2}.targetTb LIKE {ks1}.sourcetb")

            # source keyspace and table do not exist
            assertUnauthorizedQuery(session, "User " + user + f" has no SELECT permission on <table {ks1}.tbnotexist> or any of its parents",
                                    f"CREATE TABLE {ks2}.targetTb LIKE {ks1}.tbnotexist")
            assertUnauthorizedQuery(session, "User " + user + " has no SELECT permission on <table ksnotexists.sourcetb> or any of its parents",
                                    f"CREATE TABLE {ks2}.targetTb LIKE ksnotexists.sourcetb")
            # target keyspace does not exist
            assertUnauthorizedQuery(session, "User " + user + " has no CREATE permission on <all tables in ksnotexists> or any of its parents",
                                    f"CREATE TABLE ksnotexists.targetTb LIKE {ks1}.sourcetb")

# The tests testSpecificGrantsOnSystemKeyspaces and testGrantOnAllKeyspaces
# were not translated, because they go over the lists of Cassandra's system
# keyspaces and tables, and of the permissions applicable to them, which
# they take from Cassandra's internal classes. Scylla's system keyspaces
# and tables are different.
# The test testGrantOnVirtualKeyspaces was not translated, because it grants
# permissions on Cassandra's virtual keyspaces system_virtual_schema and
# system_views, which Scylla doesn't have.

def testCheckPermissionsAfterAuthorize(cql):
    user = "user_" + unique_name()
    simple_user = "simple_user_" + unique_name()
    with new_test_keyspace(cql, "WITH replication = " + KEYSPACE_REPLICATION) as check_permissions, \
         dropped_roles(cql, user, simple_user):
        cql.execute(f"CREATE TABLE {check_permissions}.t1 (k int PRIMARY KEY)")
        cql.execute(f"INSERT INTO {check_permissions}.t1 (k) VALUES (1)")

        create_user(cql, user)

        cql.execute("CREATE ROLE %s WITH LOGIN = TRUE AND password='%s'" % (simple_user, simple_user))
        cql.execute(f"GRANT AUTHORIZE ON {check_permissions}.t1 TO " + simple_user)

        with use_user(cql, user, pass_) as session:
            assertUnauthorizedQuery(session, "User " + user + f" has no SELECT permission on <table {check_permissions}.t1> or any of its parents",
                                    f"SELECT * FROM {check_permissions}.t1")

        with use_user(cql, simple_user, simple_user) as session:
            assertUnauthorizedQuery(session, "User " + simple_user + f" has no SELECT permission on <table {check_permissions}.t1> or any of its parents",
                                    f"SELECT * FROM {check_permissions}.t1")
            assertUnauthorizedQuery(session, "User " + simple_user + f" has no SELECT permission on <table {check_permissions}.t1> or any of its parents",
                                    f"GRANT SELECT ON {check_permissions}.t1 TO " + user)

        with use_user(cql, user, pass_) as session:
            assertUnauthorizedQuery(session, "User " + user + f" has no SELECT permission on <table {check_permissions}.t1> or any of its parents",
                                    f"SELECT * FROM {check_permissions}.t1")

        cql.execute(f"GRANT SELECT ON {check_permissions}.t1 TO " + simple_user)

        with use_user(cql, simple_user, simple_user) as session:
            spin_assert(lambda: session.execute(f"SELECT * FROM {check_permissions}.t1"))
            session.execute(f"GRANT SELECT ON {check_permissions}.t1 TO " + user)

        with use_user(cql, user, pass_) as session:
            spin_assert(lambda: session.execute(f"SELECT * FROM {check_permissions}.t1"))

# The tests testAddIdentityPermissions,
# testRemoveIdentityPermissionsWithSpecificRolePermission and
# testRemoveIdentityPermissions were not translated, because they test
# Cassandra's ADD IDENTITY and DROP IDENTITY statements, which bind
# certificate identities to roles for Cassandra's mTLS authenticator, which
# Scylla doesn't have.
