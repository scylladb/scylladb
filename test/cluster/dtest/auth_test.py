#
# Copyright (C) 2013-present The Apache Software Foundation
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

"""
All dtest functional test for authentication and authorization tests.

STATE: NOT FULLY IMPLEMENTED
"""

import collections
import logging
import os
import re
import subprocess

import pytest
from cassandra import (
    AlreadyExists,
    AuthenticationFailed,
    InvalidRequest,
    Unauthorized,
    Unavailable,
)
from cassandra.cluster import NoHostAvailable

from dtest_class import Tester
from dtest_setup import DTestSetup
from tools.assertions import assert_invalid
from tools.cluster import new_node
from tools.cluster_topology import generate_cluster_topology
from tools.log_utils import wait_for_any_log


logger = logging.getLogger(__file__)


class TestAuth(Tester):
    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup: DTestSetup):
        fixture_dtest_setup.ignore_log_patterns += [
            r"Can\'t send migration request: node.*is down",
        ]

    @pytest.mark.single_node
    def test_login(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        # also tests default user creation (cassandra/cassandra)
        self.prepare()
        self.get_session(user="cassandra", password="cassandra")
        try:
            self.get_session(user="cassandra", password="badpassword")
        except NoHostAvailable as e:
            assert isinstance(next(iter(e.errors.values())), AuthenticationFailed)
        try:
            self.get_session(user="doesntexist", password="doesntmatter")
        except NoHostAvailable as e:
            assert isinstance(next(iter(e.errors.values())), AuthenticationFailed)
        # Authentication ID must not be null
        try:
            self.get_session(user="", password="")
        except NoHostAvailable as e:
            assert isinstance(next(iter(e.errors.values())), AuthenticationFailed)
            assert "Authentication ID must not be null" in str(next(iter(e.errors.values())))
        # Password must not be null
        try:
            self.get_session(user="cassandra", password="")
        except NoHostAvailable as e:
            assert isinstance(next(iter(e.errors.values())), AuthenticationFailed)
            # Currently the null password can't be identified, comment the assert
            # https://github.com/scylladb/scylla/issues/2274
            # assert 'Password must not be null' in str(list(e.errors.values())[0])

    @pytest.mark.single_node
    def test_anonymous(self):
        """
        Both Scylla and Cassandra allow to create a non-anonymous user which name
        is `anonymous`, Scylla identifies the anonymous user by a flag, not match
        with the name. In authorization, we strictly check anonymous flag before
        query the permission table, which might filter by username. So we are safe.
        """
        self.prepare(nodes=1)
        cassandra = self.get_session(user="cassandra", password="cassandra")

        logger.info("Create a non-anonymous user which name is `anonymous`")
        cassandra.execute("CREATE USER anonymous WITH PASSWORD '12345' NOSUPERUSER")

        logger.info("Login with non-anonymous user `anonymous`")
        session = self.get_session(user="anonymous", password="12345")

        logger.info("The new user should has permission to LIST users")
        logger.info("we don't expect to see error: `You have to be logged in and not anonymous to perform this request`")
        session.execute("LIST USERS")

        logger.info("Give AUTHORIZE permission to non-anonymous user `anonymous`")
        cassandra.execute("GRANT AUTHORIZE ON ALL KEYSPACES to anonymous")

        logger.info("Update config and restart to enable AllowAllAuthenticator/AllowAllAuthorizer")
        self.cluster.stop()
        config = {"authenticator": "org.apache.cassandra.auth.AllowAllAuthenticator", "authorizer": "org.apache.cassandra.auth.AllowAllAuthorizer"}
        self.cluster.set_configuration_options(values=config)
        self.cluster.start(wait_for_binary_proto=True)

        logger.info("Verify permissions of real anonymous user")
        session = self.get_session(user="anonymous", password="12345")
        self.assert_unauthorized("You have to be logged in and not anonymous to perform this request", session, "LIST USERS")
        self.assert_unauthorized("You have to be logged in and not anonymous to perform this request", session, "GRANT SELECT ON ALL KEYSPACES TO anonymous")

    @pytest.mark.single_node
    def test_create_user_permissions(self):
        """
        Description: Try to create new user in two ways, somebody can execute `CREATE USER/CREATE ROLE` is either if
                     they're a superuser or if they have the CREATE permission on <all roles>.

        Expected Result: Fail to create new user for nosuperuser that has no CREATE permission on <all roles>.
        """
        self.prepare()

        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER jackob WITH PASSWORD '12345' NOSUPERUSER")

        jackob = self.get_session(user="jackob", password="12345")
        self.assert_unauthorized("User jackob has no CREATE permission on <all roles> or any of its parents", jackob, "CREATE USER james WITH PASSWORD '54321' NOSUPERUSER")

    @pytest.mark.single_node
    def test_cant_create_existing_user(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        session = self.get_session(user="cassandra", password="cassandra")
        session.execute("CREATE USER 'james@example.com' WITH PASSWORD '12345' NOSUPERUSER")
        assert_invalid(session, "CREATE USER 'james@example.com' WITH PASSWORD '12345' NOSUPERUSER", "james@example.com already exists")

    @pytest.mark.single_node
    def test_list_users(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        session = self.get_session(user="cassandra", password="cassandra")
        session.execute("CREATE USER alex WITH PASSWORD '12345' NOSUPERUSER")
        session.execute("CREATE USER bob WITH PASSWORD '12345' SUPERUSER")
        session.execute("CREATE USER cathy WITH PASSWORD '12345' NOSUPERUSER")
        session.execute("CREATE USER dave WITH PASSWORD '12345' SUPERUSER")

        rows = list(session.execute("LIST USERS"))
        assert 5 == len(rows)
        # {username: isSuperuser} dict.
        users = dict([(r[0], r[1]) for r in rows])

        assert users["cassandra"]
        assert not users["alex"]
        assert users["bob"]
        assert not users["cathy"]
        assert users["dave"]

    @pytest.mark.single_node
    def test_user_cant_drop_themselves(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        session = self.get_session(user="cassandra", password="cassandra")
        # handle different error messages between versions pre and post 2.2.0
        assert_invalid(session, "DROP USER cassandra", "(Users aren't allowed to DROP themselves|Cannot DROP primary role for current login)")

    @pytest.mark.single_node
    def test_dropping_nonexistent_user_throws_exception(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        session = self.get_session(user="cassandra", password="cassandra")
        assert_invalid(session, "DROP USER nonexistent", "nonexistent doesn't exist")

    @pytest.mark.single_node
    def test_drop_user_case_sensitive(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a user, 'Test'
        * Verify that the drop user statement is case sensitive
        """
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER Test WITH PASSWORD '12345'")

        # Should be invalid, as 'test/TEST' does not exist
        assert_invalid(cassandra, "DROP USER test")
        assert_invalid(cassandra, "DROP USER TEST")

        cassandra.execute("DROP USER Test")
        rows = [x[0] for x in list(cassandra.execute("LIST USERS"))]
        assert collections.Counter(rows) == collections.Counter(["cassandra"])

        # Should be invalid, as 'Test' does not exist anymore
        assert_invalid(cassandra, "DROP USER test")
        assert_invalid(cassandra, "DROP USER TEST")
        assert_invalid(cassandra, "DROP USER Test")

        cassandra.execute("CREATE USER test WITH PASSWORD '12345'")

        # Should be invalid, as 'TEST/Test' does not exist
        assert_invalid(cassandra, "DROP USER TEST")
        assert_invalid(cassandra, "DROP USER Test")

        cassandra.execute("DROP USER test")
        rows = [x[0] for x in list(cassandra.execute("LIST USERS"))]
        assert collections.Counter(rows) == collections.Counter(["cassandra"])

        # Should be invalid, as 'test' does not exist anymore
        assert_invalid(cassandra, "DROP USER test")
        assert_invalid(cassandra, "DROP USER TEST")
        assert_invalid(cassandra, "DROP USER Test")

    @pytest.mark.single_node
    def test_drop_user_revoke_all(self):
        """
        Test all user permissions will be revoked when the user is dropped.

        * Create two test user: `test` & `test2`
        * Super gives test SELECT/AUTHORIZE permission
        * `test` gives SELECT permission to `test2`
        * Drop `test` user
        * Recreate a `test` user
        * Verify `test` doesn't has original permissions, they are all revoked
        """
        self.prepare(nodes=1)

        logger.info("Create two test users: `test` and `test2`, and create table ks.cf")
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER test WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER test2 WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key)")

        logger.info("Verify `test` user doesn't have SELECT/AUTHORIZE permissions")
        session = self.get_session(user="test", password="12345")
        self.assert_unauthorized("User test has no SELECT permission on <table ks.cf> or any of its parents", session, "SELECT * FROM ks.cf")
        self.assert_unauthorized("User test has no AUTHORIZE permission on <table ks.cf> or any of its parents", session, "GRANT SELECT ON ks.cf TO test2")

        logger.info("Super gives `test` user SELECT/AUTHORIZE permission on ks.cf")
        cassandra.execute("GRANT SELECT ON ks.cf TO test")
        cassandra.execute("GRANT AUTHORIZE ON ks.cf TO test")
        session.execute("SELECT * from ks.cf")

        logger.info("`test` user gives SELECT permission to `test2`")
        session.execute("GRANT SELECT ON ks.cf TO test2")

        logger.info("Super drops `test` user")
        cassandra.execute("DROP USER test")

        logger.info("Verify test2 still has SELECT permission")
        session = self.get_session(user="test2", password="12345")
        session.execute("SELECT * from ks.cf")

        logger.info("Recreate `test` user, and verify it doesn't have SELECT/AUTHORIZE permissions")
        cassandra.execute("CREATE USER test WITH PASSWORD '12345'")
        session = self.get_session(user="test", password="12345")
        self.assert_unauthorized("User test has no SELECT permission on <table ks.cf> or any of its parents", session, "SELECT * FROM ks.cf")
        self.assert_unauthorized("User test has no AUTHORIZE permission on <table ks.cf> or any of its parents", session, "GRANT SELECT ON ks.cf TO test2")

    @pytest.mark.single_node
    def test_alter_user_case_sensitive(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a user, 'Test'
        * Verify that ALTER statements on the user are case sensitive
        """
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER Test WITH PASSWORD '12345'")
        cassandra.execute("ALTER USER Test WITH PASSWORD '54321'")
        assert_invalid(cassandra, "ALTER USER test WITH PASSWORD '12345'")
        assert_invalid(cassandra, "ALTER USER TEST WITH PASSWORD '12345'")

        cassandra.execute("DROP USER Test")
        cassandra.execute("CREATE USER test WITH PASSWORD '12345'")
        assert_invalid(cassandra, "ALTER USER Test WITH PASSWORD '12345'")
        assert_invalid(cassandra, "ALTER USER TEST WITH PASSWORD '12345'")
        cassandra.execute("ALTER USER test WITH PASSWORD '54321'")

    @pytest.mark.single_node
    def test_regular_users_can_alter_their_passwords_only(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER bob WITH PASSWORD '12345'")

        cathy = self.get_session(user="cathy", password="12345")
        cathy.execute("ALTER USER cathy WITH PASSWORD '54321'")
        cathy = self.get_session(user="cathy", password="54321")
        self.assert_unauthorized("User cathy has no ALTER permission on <role bob> or any of its parents", cathy, "ALTER USER bob WITH PASSWORD 'cantchangeit'")

    @pytest.mark.single_node
    def test_users_cant_alter_their_superuser_status(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        session = self.get_session(user="cassandra", password="cassandra")
        self.assert_unauthorized("You aren't allowed to alter your own superuser status", session, "ALTER USER cassandra NOSUPERUSER")

    @pytest.mark.single_node
    def test_only_superuser_alters_superuser_status(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")

        cathy = self.get_session(user="cathy", password="12345")
        self.assert_unauthorized("Only superusers are allowed to alter superuser status", cathy, "ALTER USER cassandra NOSUPERUSER")

        cassandra.execute("ALTER USER cathy SUPERUSER")

    @pytest.mark.single_node
    def test_altering_nonexistent_user_throws_exception(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        session = self.get_session(user="cassandra", password="cassandra")
        assert_invalid(session, "ALTER USER nonexistent WITH PASSWORD 'doesn''tmatter'", "nonexistent doesn't exist")

    @pytest.mark.single_node
    def test_conditional_create_drop_user(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()
        session = self.get_session(user="cassandra", password="cassandra")

        users = list(session.execute("LIST USERS"))
        assert 1 == len(users)  # cassandra

        session.execute("CREATE USER IF NOT EXISTS aleksey WITH PASSWORD 'sup'")
        session.execute("CREATE USER IF NOT EXISTS aleksey WITH PASSWORD 'ignored'")

        users = list(session.execute("LIST USERS"))
        assert 2 == len(users)  # cassandra + aleksey

        session.execute("DROP USER IF EXISTS aleksey")
        session.execute("DROP USER IF EXISTS aleksey")

        users = list(session.execute("LIST USERS"))
        assert 1 == len(users)  # cassandra

    @pytest.mark.single_node
    def test_create_ks_auth(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")

        cathy = self.get_session(user="cathy", password="12345")
        self.assert_unauthorized("User cathy has no CREATE permission on <all keyspaces> or any of its parents", cathy, "CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")

        cassandra.execute("GRANT CREATE ON ALL KEYSPACES TO cathy")
        cathy.execute("""CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}""")

    @pytest.mark.single_node
    def test_create_cf_auth(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")

        cathy = self.get_session(user="cathy", password="12345")
        self.assert_unauthorized("User cathy has no CREATE permission on <keyspace ks> or any of its parents", cathy, "CREATE TABLE ks.cf (id int primary key)")

        cassandra.execute("GRANT CREATE ON KEYSPACE ks TO cathy")
        cathy.execute("CREATE TABLE ks.cf (id int primary key)")

    @pytest.mark.single_node
    def test_alter_ks_auth(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        cluster_topology = generate_cluster_topology(dc_num=1, rack_num=2, nodes_per_rack=1)
        self.prepare(nodes=cluster_topology)

        cassandra = self.get_session(user="cassandra", password="cassandra")
        if "tablets" in self.scylla_features:
            replication_strategy = "NetworkTopologyStrategy"
            rf_type = self.cluster.nodelist()[0].get_datacenter_name()
        else:
            replication_strategy = "SimpleStrategy"
            rf_type = "replication_factor"

        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute(f"CREATE KEYSPACE ks WITH replication = {{'class': '{replication_strategy}', '{rf_type}': 1}};")

        cathy = self.get_session(user="cathy", password="12345")
        self.assert_unauthorized("User cathy has no ALTER permission on <keyspace ks> or any of its parents", cathy, f"ALTER KEYSPACE ks WITH replication = {{'class':'{replication_strategy}', '{rf_type}':2}}")

        cassandra.execute("GRANT ALTER ON KEYSPACE ks TO cathy")
        cathy.execute(f"ALTER KEYSPACE ks WITH replication = {{'class':'{replication_strategy}', '{rf_type}':2}}")

    @pytest.mark.single_node
    def test_alter_cf_auth(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new user, 'cathy', with no permissions
        * Connect as 'cathy'
        * Assert that trying to alter a ks as 'cathy' throws Unauthorized
        * Grant 'cathy' alter permissions
        * Assert that 'cathy' can alter a ks
        """
        self.prepare()

        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key)")

        cathy = self.get_session(user="cathy", password="12345")
        self.assert_unauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents", cathy, "ALTER TABLE ks.cf ADD val int")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("ALTER TABLE ks.cf ADD val int")

        cassandra.execute("REVOKE ALTER ON ks.cf FROM cathy")
        self.assert_unauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents", cathy, "CREATE INDEX ON ks.cf(val)")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("CREATE INDEX ON ks.cf(val)")

        cassandra.execute("REVOKE ALTER ON ks.cf FROM cathy")

        cathy.execute("USE ks")
        self.assert_unauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents", cathy, "DROP INDEX cf_val_idx")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("DROP INDEX cf_val_idx")

    @pytest.mark.single_node
    def alter_cf_auth_test_without_indexes(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create a new user, 'cathy', with no permissions
        * Connect as 'cathy'
        * Assert that trying to alter a ks as 'cathy' throws Unauthorized
        * Grant 'cathy' alter permissions
        * Assert that 'cathy' can alter a ks
        """
        self.prepare()

        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key)")

        cathy = self.get_session(user="cathy", password="12345")
        self.assert_unauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents", cathy, "ALTER TABLE ks.cf ADD val int")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("ALTER TABLE ks.cf ADD val int")

        cassandra.execute("REVOKE ALTER ON ks.cf FROM cathy")
        self.assert_unauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents", cathy, "CREATE INDEX ON ks.cf(val)")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("ALTER TABLE ks.cf ADD val2 int")

        cassandra.execute("REVOKE ALTER ON ks.cf FROM cathy")

        cathy.execute("USE ks")
        self.assert_unauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents", cathy, "ALTER TABLE ks.cf DROP val2")

        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("ALTER TABLE ks.cf DROP val2")

    @pytest.mark.single_node
    def test_materialized_views_auth(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:** SKIPPED
        """
        self.prepare()

        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, value text)")

        # Try CREATE MV without ALTER permission on base table
        create_mv = "CREATE MATERIALIZED VIEW ks.mv1 AS SELECT * FROM ks.cf WHERE id IS NOT NULL AND value IS NOT NULL PRIMARY KEY (value, id)"
        cathy = self.get_session(user="cathy", password="12345")
        self.assert_unauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents", cathy, create_mv)

        # Grant ALTER permission and CREATE MV
        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute(create_mv)

        # TRY SELECT MV without SELECT permission on base table
        self.assert_unauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents", cathy, "SELECT * FROM ks.mv1")

        # Grant SELECT permission and CREATE MV
        cassandra.execute("GRANT SELECT ON ks.cf TO cathy")
        cathy.execute("SELECT * FROM ks.mv1")

        # Revoke ALTER permission and try DROP MV
        cassandra.execute("REVOKE ALTER ON ks.cf FROM cathy")
        cathy.execute("USE ks")
        self.assert_unauthorized("User cathy has no ALTER permission on <table ks.cf> or any of its parents", cathy, "DROP MATERIALIZED VIEW mv1")

        # GRANT ALTER permission and DROP MV
        cassandra.execute("GRANT ALTER ON ks.cf TO cathy")
        cathy.execute("DROP MATERIALIZED VIEW mv1")

    @pytest.mark.single_node
    def test_drop_ks_auth(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")

        cathy = self.get_session(user="cathy", password="12345")
        self.assert_unauthorized("User cathy has no DROP permission on <keyspace ks> or any of its parents", cathy, "DROP KEYSPACE ks")

        cassandra.execute("GRANT DROP ON KEYSPACE ks TO cathy")
        cathy.execute("DROP KEYSPACE ks")

    @pytest.mark.single_node
    def test_drop_cf_auth(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key)")

        cathy = self.get_session(user="cathy", password="12345")
        self.assert_unauthorized("User cathy has no DROP permission on <table ks.cf> or any of its parents", cathy, "DROP TABLE ks.cf")

        cassandra.execute("GRANT DROP ON ks.cf TO cathy")
        cathy.execute("DROP TABLE ks.cf")

    @pytest.mark.single_node
    def test_modify_and_select_auth(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        cathy = self.get_session(user="cathy", password="12345")
        self.assert_unauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents", cathy, "SELECT * FROM ks.cf")

        cassandra.execute("GRANT SELECT ON ks.cf TO cathy")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        assert 0 == len(rows)

        self.assert_unauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents", cathy, "INSERT INTO ks.cf (id, val) VALUES (0, 0)")

        self.assert_unauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents", cathy, "UPDATE ks.cf SET val = 1 WHERE id = 1")

        self.assert_unauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents", cathy, "DELETE FROM ks.cf WHERE id = 1")

        self.assert_unauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents", cathy, "TRUNCATE ks.cf")

        cassandra.execute("GRANT MODIFY ON ks.cf TO cathy")
        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        cathy.execute("UPDATE ks.cf SET val = 1 WHERE id = 1")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        assert 2 == len(rows)

        cathy.execute("DELETE FROM ks.cf WHERE id = 1")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        assert 1 == len(rows)

        cathy.execute("TRUNCATE ks.cf")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))

        assert 0 == len(rows)

    @pytest.mark.single_node
    def test_grant_revoke_without_ks_specified(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create table ks.cf
        * Create a new users, 'cathy' and 'bob', with no permissions
        * Grant ALL on ks.cf to cathy
        * As cathy, try granting SELECT on cf to bob, without specifying the ks; verify it fails
        * As cathy, USE ks, try again, verify it works this time
        """
        self.prepare()

        cassandra = self.get_session(user="cassandra", password="cassandra")

        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER bob WITH PASSWORD '12345'")

        cassandra.execute("GRANT ALL ON ks.cf TO cathy")

        cathy = self.get_session(user="cathy", password="12345")
        bob = self.get_session(user="bob", password="12345")

        assert_invalid(cathy, "GRANT SELECT ON cf TO bob", "No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename")
        self.assert_unauthorized("User bob has no SELECT permission on <table ks.cf> or any of its parents", bob, "SELECT * FROM ks.cf")

        cathy.execute("USE ks")
        cathy.execute("GRANT SELECT ON cf TO bob")
        bob.execute("SELECT * FROM ks.cf")

    @pytest.mark.single_node
    def test_grant_revoke_auth(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER bob WITH PASSWORD '12345'")

        cathy = self.get_session(user="cathy", password="12345")
        # missing both SELECT and AUTHORIZE
        self.assert_unauthorized("User cathy has no AUTHORIZE permission on <all keyspaces> or any of its parents", cathy, "GRANT SELECT ON ALL KEYSPACES TO bob")

        cassandra.execute("GRANT AUTHORIZE ON ALL KEYSPACES TO cathy")

        # still missing SELECT
        self.assert_unauthorized("User cathy has no SELECT permission on <all keyspaces> or any of its parents", cathy, "GRANT SELECT ON ALL KEYSPACES TO bob")

        cassandra.execute("GRANT SELECT ON ALL KEYSPACES TO cathy")

        # should succeed now with both SELECT and AUTHORIZE
        cathy.execute("GRANT SELECT ON ALL KEYSPACES TO bob")

    @pytest.mark.single_node
    def test_grant_revoke_validation(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")

        assert_invalid(cassandra, "GRANT ALL ON KEYSPACE nonexistent TO cathy", "<keyspace nonexistent> doesn't exist")

        assert_invalid(cassandra, "GRANT ALL ON KEYSPACE ks TO nonexistent", "(User|Role) nonexistent doesn't exist")

        assert_invalid(cassandra, "REVOKE ALL ON KEYSPACE nonexistent FROM cathy", "<keyspace nonexistent> doesn't exist")

        assert_invalid(cassandra, "REVOKE ALL ON KEYSPACE ks FROM nonexistent", "(User|Role) nonexistent doesn't exist")

    @pytest.mark.single_node
    def test_grant_revoke_cleanup(self):
        """
        Originally from dtest.
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("GRANT ALL ON ks.cf TO cathy")

        cathy = self.get_session(user="cathy", password="12345")
        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        assert 1 == len(rows)

        # drop and recreate the user, make sure permissions are gone
        cassandra.execute("DROP USER cathy")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")

        self.assert_unauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents", cathy, "INSERT INTO ks.cf (id, val) VALUES (0, 0)")

        self.assert_unauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents", cathy, "SELECT * FROM ks.cf")

        # grant all the permissions back
        cassandra.execute("GRANT ALL ON ks.cf TO cathy")
        cathy.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")
        rows = list(cathy.execute("SELECT * FROM ks.cf"))
        assert 1 == len(rows)

        # drop and recreate the keyspace, make sure permissions are gone
        cassandra.execute("DROP KEYSPACE ks")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")

        self.assert_unauthorized("User cathy has no MODIFY permission on <table ks.cf> or any of its parents", cathy, "INSERT INTO ks.cf (id, val) VALUES (0, 0)")

        self.assert_unauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents", cathy, "SELECT * FROM ks.cf")

    @pytest.mark.single_node
    def test_type_auth(self):
        """
        Originally from dtest..
        **Description:**

        **Expected Result:**
        """
        self.prepare()

        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")

        cathy = self.get_session(user="cathy", password="12345")
        self.assert_unauthorized("User cathy has no CREATE permission on <keyspace ks> or any of its parents", cathy, "CREATE TYPE ks.address (street text, city text)")
        self.assert_unauthorized("User cathy has no ALTER permission on <keyspace ks> or any of its parents", cathy, "ALTER TYPE ks.address ADD zip_code int")
        self.assert_unauthorized("User cathy has no DROP permission on <keyspace ks> or any of its parents", cathy, "DROP TYPE ks.address")

        cassandra.execute("GRANT CREATE ON KEYSPACE ks TO cathy")
        cathy.execute("CREATE TYPE ks.address (street text, city text)")
        cassandra.execute("GRANT ALTER ON KEYSPACE ks TO cathy")
        cathy.execute("ALTER TYPE ks.address ADD zip_code int")
        cassandra.execute("GRANT DROP ON KEYSPACE ks TO cathy")
        cathy.execute("DROP TYPE ks.address")

    def _check_session_available(self, session, expect_rf_err=False, expect_auth_err=False, expect_invalid_req=False):
        try:
            rows = list(session.execute("LIST USERS"))
            logger.info("Debug users list: %s" % rows)
            assert len(rows) > 0, "Failed to get user list from session"
        except Unavailable as e:
            logger.info("Debug: _check_session_available: Unavailable Exception")
            if expect_rf_err:
                assert e.alive_replicas != e.required_replicas, str(e)
                logger.info("Good: session isn't available (rf error) as expected")
            else:
                logger.info("Fail: session isn't available, but not expected error")
                raise
        except NoHostAvailable as e:
            logger.info(e.errors)
            if expect_auth_err:
                assert isinstance(next(iter(e.errors.values())), AuthenticationFailed)
                logger.info("Good: session isn't available (auth err) as expected")
            else:
                logger.info("Fail: session isn't available, but not expected error")
                raise
        except InvalidRequest as e:
            logger.info(e)
            if expect_invalid_req:
                logger.info("Good: session isn't available (invalid request) as expected")
            else:
                logger.info("Fail: session isn't available, but not expected error")
                raise

        if not (expect_rf_err or expect_auth_err or expect_invalid_req):
            logger.info("Good: session is available as expected")

    @pytest.mark.single_node
    def test_drop_keyspace_system_auth_1_node(self):
        """
        **Description:** try to drop system_auth table
        **Expected Result:** we should not be able to drop system_auth
        """
        self.prepare()
        logger.info("Cluster with 1 nodes started")

        # node = self.cluster.nodelist()[0]
        session = self.get_session(node_idx=0, user="cassandra", password="cassandra")
        logger.info("Successfully get the session from node1")
        # make sure session works
        self._check_session_available(session)

        # expected message like "Cannot DROP <keyspace system_auth>"
        try:
            session.execute("DROP KEYSPACE system_auth")
        except Unauthorized as e:
            assert str(e) == 'Error from server: code=2100 [Unauthorized] message="Cannot DROP <keyspace system_auth>"'

    @pytest.mark.single_node
    def test_change_setting_to_noauth_after_system_auth_was_lost(self):
        """
        **Description:** after the auth info is lost, change the setting of a node
        to no auth (while the node is down), force a client to connect to that node.
        **Expected Result:** Cluster is available but connection failed.
        """
        self.prepare()
        session = self.get_session(user="cassandra", password="cassandra")
        self._check_session_available(session)
        self.cluster.stop()
        config = {"authenticator": "org.apache.cassandra.auth.AllowAllAuthenticator", "authorizer": "org.apache.cassandra.auth.AllowAllAuthorizer"}
        self.cluster.set_configuration_options(values=config)
        self.cluster.start(wait_for_binary_proto=True)

        for session in [self.get_session(), self.get_session(user="cassandra", password="cassandra")]:
            with pytest.raises(Unauthorized, match=r'Error from server: code=2100 \[Unauthorized\] message="You have to be logged in and not ' 'anonymous to perform this request"'):
                session.execute("LIST USERS")

    @pytest.mark.single_node
    def test_restart_node_doesnt_lose_auth_data(self):
        """
        * Launch a one node cluster
        * Connect as the default superuser
        * Create some new users, grant them permissions
        * Stop the cluster, switch to AllowAll auth, restart the cluster
        * Stop the cluster, switch back to auth, restart the cluster
        * Check all user auth data was preserved
        """
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER philip WITH PASSWORD 'strongpass'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int PRIMARY KEY)")
        cassandra.execute("GRANT ALL ON ks.cf to philip")

        self.cluster.stop()
        config = {"authenticator": "org.apache.cassandra.auth.AllowAllAuthenticator", "authorizer": "org.apache.cassandra.auth.AllowAllAuthorizer"}
        self.cluster.set_configuration_options(values=config)
        self.cluster.start(wait_for_binary_proto=True)

        self.cluster.stop()
        config = {"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator", "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer"}
        self.cluster.set_configuration_options(values=config)
        self.cluster.start(wait_for_binary_proto=True)

        philip = self.get_session(user="philip", password="strongpass")
        cathy = self.get_session(user="cathy", password="12345")
        self.assert_unauthorized("User cathy has no SELECT permission on <table ks.cf> or any of its parents", cathy, "SELECT * FROM ks.cf")

        philip.execute("SELECT * FROM ks.cf")

    @pytest.mark.single_node
    def test_system_keyspace_sensitive(self):
        """
        * Launch a one node cluster
        * Try to create KEYSPACEs like: 'SYSTEM_tRaCeS', 'SYSTEM_aUtH'
        * Creation should be failed because system keyspaces is not user-modifiable.
        * Then drop system keyspaces - failed too.
        """
        self.prepare()
        session = self.get_session(user="cassandra", password="cassandra")
        with pytest.raises(InvalidRequest, match=r'Error from server: code=2200 \[Invalid query\] message="system keyspace is not user-modifiable"'):
            session.execute("create KEYSPACE SyStEM WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")

        system_kss = ["SYSTEM_tRaCeS"]
        # system_auth keyspace doesn't exists when consistent topology (and therefore auth-v2) is enabled
        if "consistent-topology-changes" not in self.scylla_features:
            system_kss.append("SYSTEM_aUtH")

        for name in system_kss:
            with pytest.raises(AlreadyExists, match="Keyspace '%s' already exists" % name.lower()):
                session.execute("create KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}" % name)
                pytest.fail("Keyspace %s shouldn't be created")

        with pytest.raises(Unauthorized, match=r'Error from server: code=2100 \[Unauthorized\] message="system keyspace is not user-modifiable."'):
            session.execute("drop KEYSPACE system")
        # https://github.com/scylladb/scylla/issues/2338
        """for name in ['SYSTEM_tRaCeS', 'SYSTEM_aUtH']:
            try:
                session.execute(
                    "drop KEYSPACE %s" % name)
                pytest.fail("Keyspace %s shouldn't be deleted")
            except InvalidRequest as e:
                self.assertEquals(str(e), 'Cannot DROP <keyspace %s>' % name.lower())"""

    @pytest.mark.single_node
    def test_all_authorization_operations(self):
        """
        **Description:** Test all authorization operations, actions and applied objects.
        **Expected Result:** All commands run successfully, no crash is triggered.
        """
        self.prepare()

        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER cathy WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER bob WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER dave WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER anna WITH PASSWORD '12345'")
        cassandra.execute("CREATE USER chuk WITH PASSWORD '12345'")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE TABLE ks.cf2 (id int primary key, val int)")

        cassandra.execute("GRANT CREATE ON ALL KEYSPACES TO cathy")
        cassandra.execute("GRANT ALTER ON KEYSPACE ks TO bob")
        cassandra.execute("GRANT SELECT ON ALL KEYSPACES TO dave")
        cassandra.execute("GRANT ALL ON ks.cf TO dave")
        cassandra.execute("GRANT MODIFY ON KEYSPACE ks TO anna")
        cassandra.execute("GRANT MODIFY ON ks.cf TO cathy")
        cassandra.execute("GRANT DROP ON ks.cf TO bob")
        cassandra.execute("GRANT MODIFY ON ks.cf2 TO bob")
        cassandra.execute("GRANT SELECT ON ks.cf2 TO cathy")
        cassandra.execute("GRANT ALL PERMISSIONS ON ks.cf2 TO chuk")

        all_permissions = [
            ("anna", "<keyspace ks>", "MODIFY"),
            ("bob", "<keyspace ks>", "ALTER"),
            ("bob", "<table ks.cf>", "DROP"),
            ("bob", "<table ks.cf2>", "MODIFY"),
            ("cathy", "<all keyspaces>", "CREATE"),
            ("cathy", "<table ks.cf>", "MODIFY"),
            ("cathy", "<table ks.cf2>", "SELECT"),
            ("chuk", "<table ks.cf2>", "ALTER"),
            ("chuk", "<table ks.cf2>", "AUTHORIZE"),
            ("chuk", "<table ks.cf2>", "DROP"),
            ("chuk", "<table ks.cf2>", "MODIFY"),
            ("chuk", "<table ks.cf2>", "SELECT"),
            ("dave", "<all keyspaces>", "SELECT"),
            ("dave", "<table ks.cf>", "ALTER"),
            ("dave", "<table ks.cf>", "AUTHORIZE"),
            ("dave", "<table ks.cf>", "DROP"),
            ("dave", "<table ks.cf>", "MODIFY"),
            ("dave", "<table ks.cf>", "SELECT"),
        ]

        self.assert_permissions_listed(all_permissions, cassandra, "LIST ALL PERMISSIONS")

        self.assert_permissions_listed([("cathy", "<all keyspaces>", "CREATE"), ("cathy", "<table ks.cf>", "MODIFY"), ("cathy", "<table ks.cf2>", "SELECT")], cassandra, "LIST ALL PERMISSIONS OF cathy")

        expected_permissions = [
            ("bob", "<table ks.cf>", "DROP"),
            ("cathy", "<table ks.cf>", "MODIFY"),
            ("dave", "<table ks.cf>", "ALTER"),
            ("dave", "<table ks.cf>", "AUTHORIZE"),
            ("dave", "<table ks.cf>", "DROP"),
            ("dave", "<table ks.cf>", "MODIFY"),
            ("dave", "<table ks.cf>", "SELECT"),
        ]
        self.assert_permissions_listed(expected_permissions, cassandra, "LIST ALL PERMISSIONS ON ks.cf NORECURSIVE")

        expected_permissions = [("cathy", "<table ks.cf2>", "SELECT"), ("chuk", "<table ks.cf2>", "SELECT"), ("dave", "<all keyspaces>", "SELECT")]
        self.assert_permissions_listed(expected_permissions, cassandra, "LIST SELECT ON ks.cf2")

        self.assert_permissions_listed([("cathy", "<all keyspaces>", "CREATE"), ("cathy", "<table ks.cf>", "MODIFY")], cassandra, "LIST ALL ON ks.cf OF cathy")

        bob = self.get_session(user="bob", password="12345")
        self.assert_permissions_listed([("bob", "<keyspace ks>", "ALTER"), ("bob", "<table ks.cf>", "DROP"), ("bob", "<table ks.cf2>", "MODIFY")], bob, "LIST ALL PERMISSIONS OF bob")

        self.assert_unauthorized("You are not authorized to view everyone's permissions", bob, "LIST ALL PERMISSIONS")

        self.assert_unauthorized("You are not authorized to view cathy's permissions", bob, "LIST ALL PERMISSIONS OF cathy")

    def test_authentication_enabled_only_in_one_node(self):
        """
        **Description:** Authentication is enabled only in one node while disabled in others -
                         try to connect all node one by one.
        **Expected Result:** AuthenticationFailed failed.
        """
        self.prepare()

        node = new_node(self.cluster, bootstrap=False)

        # remove authenticator/authorizer from second node
        data_dir = os.path.join(node.get_path(), "conf/scylla.yaml")
        cmd = "sed -i.bak /authorizer/d %s" % data_dir
        p1 = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
        out, err = p1.communicate()
        assert p1.returncode == 0, err

        cmd = "sed -i.bak /authenticator/d %s" % data_dir
        p2 = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
        out, err = p2.communicate()
        assert p2.returncode == 0, err

        node.start(wait_for_binary_proto=True)
        try:
            session = self.get_session(node_idx=0, user="cassandra", password="cassandra")
            self._check_session_available(session, expect_auth_err=True, expect_invalid_req=True)
        except Unauthorized as e:
            assert str(e) == 'Error from server: code=2100 [Unauthorized] message="You have to be logged in and not anonymous to perform this request"'
        except Exception as e:  # noqa: BLE001
            assert isinstance(next(iter(e.errors.values())), AuthenticationFailed)

        session = self.get_session(node_idx=1, user="cassandra", password="cassandra")
        try:
            self._check_session_available(session, expect_auth_err=True, expect_invalid_req=True)
        except Unauthorized as e:
            assert str(e) == 'Error from server: code=2100 [Unauthorized] message="You have to be logged in and not anonymous to perform this request"'

        with pytest.raises(Exception) as exc:
            self.get_session(node_idx=0)
        assert isinstance(next(iter(exc.value.errors.values())), AuthenticationFailed)

        try:
            session = self.get_session(node_idx=1)
            self._check_session_available(session, expect_auth_err=True, expect_invalid_req=True)
            pytest.fail("Unauthorized expected")
        except NoHostAvailable as e:
            assert isinstance(next(iter(e.errors.values())), AuthenticationFailed)
        except Exception as e:  # noqa: BLE001
            assert str(e) == 'Error from server: code=2100 [Unauthorized] message="You have to be logged in and not anonymous to perform this request"'

    def test_transitional_auth_betweenness_from_default(self):
        """
        Start cluster with default Auth, test user permission during rolling upgrade of enable Transitional Auth.
        """
        logger.info("STEP: start cluster with default AllowAllAuthenticator/AllowAllAuthorizer")
        self.prepare(nodes=2, enable_auth=False)
        nodes = self.cluster.nodelist()

        logger.info("STEP: update config and restart node1 to enable Transitional Auth")
        nodes[0].stop(wait_other_notice=True, gently=True)
        config = {"authenticator": "com.scylladb.auth.TransitionalAuthenticator", "authorizer": "com.scylladb.auth.TransitionalAuthorizer"}
        nodes[0].set_configuration_options(values=config)
        nodes[0].start(wait_for_binary_proto=True)
        timeout = 30 if self.cluster.scylla_mode != "debug" else 180
        wait_for_any_log(self.cluster.nodelist(), "Created default superuser authentication record", timeout)

        session = self.get_session(node_idx=0, user="cassandra", password="cassandra")
        session.execute("CREATE USER normal WITH PASSWORD '123456' NOSUPERUSER")

        logger.info("STEP: (on node1) verify normal user has permission to list users")
        session = self.get_session(node_idx=0, user="normal", password="123456")
        session.execute("LIST USERS")
        logger.info("STEP: (on node1) verify user will login as anonymous if authentication fails")
        session = self.get_session(node_idx=0, user="normal", password="wrongpwd")
        self.assert_unauthorized("You have to be logged in and not anonymous to perform this request", session, "LIST USERS")

        logger.info("STEP: (on node2) verify all users will login as anonymous if authentication fails")
        session = self.get_session(node_idx=1, user="cassandra", password="cassandra")
        self.assert_unauthorized("You have to be logged in and not anonymous to perform this request", session, "LIST USERS")
        session = self.get_session(node_idx=1, user="normal", password="123456")
        self.assert_unauthorized("You have to be logged in and not anonymous to perform this request", session, "LIST USERS")

    def test_transitional_auth_betweenness_from_pwdauth(self):
        """
        Start cluster with strict Auth, test user permission during rolling upgrade of enable Transitional Auth.
        It's a wrong order to transition from strict Auth to AllowAllAuth, but we want to cover it.
        """
        logger.info("STEP: start cluster with PasswordAuthenticator/CassandraAuthorizer")
        self.prepare(nodes=2, enable_auth=True)
        nodes = self.cluster.nodelist()

        session = self.get_session(user="cassandra", password="cassandra")
        logger.info("STEP: create normal user (normal) by super cassandra")
        session.execute("CREATE USER normal WITH PASSWORD '123456' NOSUPERUSER")

        session = self.get_session(user="normal", password="123456")
        rows = list(session.execute("LIST USERS"))
        assert len(rows) == 1, "Expect to see `normal`, actual: %s" % (rows)
        logger.info("Verified normal was created, and available")

        config = {"authenticator": "com.scylladb.auth.TransitionalAuthenticator", "authorizer": "com.scylladb.auth.TransitionalAuthorizer"}

        logger.info("STEP: update config and restart node1 to enable Transitional Auth")
        nodes[0].stop(wait_other_notice=True, gently=True)
        nodes[0].set_configuration_options(values=config)
        nodes[0].start(wait_for_binary_proto=True)

        logger.info("STEP: (on node1) verify all users will login as anonymous if authentication fails")
        session = self.get_session(node_idx=0, user="normal", password="wrong")
        self.assert_unauthorized("You have to be logged in and not anonymous to perform this request", session, "LIST USERS")

        with pytest.raises(NoHostAvailable) as exc:
            session = self.get_session(node_idx=1, user="normal", password="wrong")
            session.execute("LIST USERS")
        assert isinstance(next(iter(exc.value.errors.values())), AuthenticationFailed)
        logger.info("can't get session of node2 with normal user/password")

    def prepare(self, nodes=1, permissions_validity=0, enable_auth=True, wait_for_superuser=False, smp=None):
        config = {"permissions_validity_in_ms": permissions_validity, "permissions_update_interval_in_ms": int(permissions_validity / 2)}
        auth_conf = {"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator", "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer"}
        if enable_auth:
            config.update(auth_conf)
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes)
        if smp:
            for node in self.cluster.nodelist():
                node.set_smp(smp)
        self.cluster.start(wait_other_notice=True, wait_for_binary_proto=True)

        if enable_auth or wait_for_superuser:
            expected_entries = ["Created default superuser role"]

            if enable_auth:
                expected_entries.append("Created default superuser authentication record")

            found = wait_for_any_log(self.cluster.nodelist(), expected_entries, 30, dispersed=True)

            if isinstance(found, list):
                nodes = []
                for n in found:
                    nodes.append(n.name)
            else:
                nodes = found.name
            logger.info(f"Default role created by {nodes}")

    def get_session(self, node_idx=0, user=None, password=None, exclusive=True):
        node = self.cluster.nodelist()[node_idx]
        if exclusive:
            conn = self.patient_exclusive_cql_connection(node, user=user, password=password, timeout=0.1)
        else:
            conn = self.patient_cql_connection(node, user=user, password=password, timeout=0.1)
        return conn

    def assert_permissions_listed(self, expected, session, query, include_superuser=False):
        # from cassandra.query import named_tuple_factory
        # session.row_factory = named_tuple_factory
        rows = session.execute(query)
        perms = [(str(r.username), str(r.resource), str(r.permission)) for r in rows]

        if not include_superuser:
            perms = [(u, r, p) for (u, r, p) in perms if u != "cassandra"]

        assert sorted(expected), sorted(perms)

    def assert_unauthorized(self, message, session, query):
        with pytest.raises(Unauthorized) as cm:
            session.execute(query)
        assert re.search(message, str(cm.value)), f"Expected '{message}', but got '{cm.value!s}'"
