#
# Copyright (C) 2014-present The Apache Software Foundation
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging
import re
import time

import pytest
from cassandra import AuthenticationFailed, InvalidRequest, Unauthorized
from cassandra.cluster import NoHostAvailable
from cassandra.protocol import SyntaxException

from dtest_class import Tester
from tools.assertions import assert_all, assert_invalid, assert_one
from tools.log_utils import wait_for_any_log

logger = logging.getLogger(__name__)


# Second value is superuser status
# Third value is login status, See #7653 for explanation.
mike_role = ["mike", False, True, {}]
role1_role = ["role1", False, False, {}]
role2_role = ["role2", False, False, {}]
cassandra_role = ["cassandra", True, True, {}]


@pytest.fixture(scope="function")
def fixture_set_cluster_settings(fixture_dtest_setup):
    fixture_dtest_setup.cluster.set_configuration_options({"enable_user_defined_functions": "true", "experimental_features": ["udf"]})


@pytest.mark.single_node
@pytest.mark.usefixtures("fixture_set_cluster_settings")
class TestAuthRoles(Tester):
    def test_create_drop_role(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        assert_one(cassandra, "LIST ROLES", cassandra_role)

        cassandra.execute("CREATE ROLE role1")
        assert_all(cassandra, "LIST ROLES", [cassandra_role, role1_role])

        cassandra.execute("DROP ROLE role1")
        assert_one(cassandra, "LIST ROLES", cassandra_role)

    def test_conditional_create_drop_role(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        assert_one(cassandra, "LIST ROLES", cassandra_role)

        cassandra.execute("CREATE ROLE IF NOT EXISTS role1")
        cassandra.execute("CREATE ROLE IF NOT EXISTS role1")
        assert_all(cassandra, "LIST ROLES", [cassandra_role, role1_role])

        cassandra.execute("DROP ROLE IF EXISTS role1")
        cassandra.execute("DROP ROLE IF EXISTS role1")
        assert_one(cassandra, "LIST ROLES", cassandra_role)

    def test_create_drop_role_validation(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        mike = self.get_session(user="mike", password="12345")

        assert_invalid(mike, "CREATE ROLE role2", "User mike has no CREATE permission on <all roles> or any of its parents", Unauthorized)
        cassandra.execute("CREATE ROLE role1")

        assert_invalid(mike, "DROP ROLE role1", "User mike has no DROP permission on <role role1> or any of its parents", Unauthorized)

        assert_invalid(cassandra, "CREATE ROLE role1", "role1 already exists")
        cassandra.execute("DROP ROLE role1")
        assert_invalid(cassandra, "DROP ROLE role1", "role1 doesn't exist")

    def test_role_admin_validation(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE ROLE administrator WITH SUPERUSER = false AND LOGIN = false")
        cassandra.execute("GRANT ALL ON ALL ROLES TO administrator")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("GRANT administrator TO mike")
        cassandra.execute("CREATE ROLE klaus WITH PASSWORD = '54321' AND SUPERUSER = false AND LOGIN = true")
        mike = self.get_session(user="mike", password="12345")
        klaus = self.get_session(user="klaus", password="54321")

        # roles with CREATE on ALL ROLES can create roles
        mike.execute("CREATE ROLE role1 WITH PASSWORD = '11111' AND LOGIN = false")

        # require ALTER on ALL ROLES or a SPECIFIC ROLE to modify
        # disable this check for issue: auth roles: user can still login even login is set to False #4284
        # self.assert_unauthenticated('role1 is not permitted to log in', 'role1', '11111')
        cassandra.execute("GRANT ALTER on ROLE role1 TO klaus")
        klaus.execute("ALTER ROLE role1 WITH LOGIN = true")
        mike.execute("ALTER ROLE role1 WITH PASSWORD = '22222'")
        role1 = self.get_session(user="role1", password="22222")

        # only superusers can set superuser status
        assert_invalid(mike, "ALTER ROLE role1 WITH SUPERUSER = true", "Only superusers are allowed to alter superuser status", Unauthorized)
        assert_invalid(mike, "ALTER ROLE mike WITH SUPERUSER = true", "Only superusers are allowed to alter superuser status.", Unauthorized)

        # roles without necessary permissions cannot create, drop or alter roles except themselves
        assert_invalid(role1, "CREATE ROLE role2 WITH LOGIN = false", "User role1 has no CREATE permission on <all roles> or any of its parents", Unauthorized)
        assert_invalid(role1, "ALTER ROLE mike WITH LOGIN = false", "User role1 has no ALTER permission on <role mike> or any of its parents", Unauthorized)
        assert_invalid(role1, "DROP ROLE mike", "User role1 has no DROP permission on <role mike> or any of its parents", Unauthorized)
        role1.execute("ALTER ROLE role1 WITH PASSWORD = '33333'")

        # roles with roleadmin can drop roles
        mike.execute("DROP ROLE role1")
        assert_all(cassandra, "LIST ROLES", [["administrator", False, False, {}], cassandra_role, ["klaus", False, True, {}], mike_role])

        # revoking role admin removes its privileges
        cassandra.execute("REVOKE administrator FROM mike")
        assert_invalid(mike, "CREATE ROLE role3 WITH LOGIN = false", "User mike has no CREATE permission on <all roles> or any of its parents", Unauthorized)

    def test_create_and_grant_roles_with_superuser_status(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE ROLE another_superuser WITH SUPERUSER = true AND LOGIN = false")
        cassandra.execute("CREATE ROLE non_superuser WITH SUPERUSER = false AND LOGIN = false")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        # mike can create and grant any role, except superusers
        cassandra.execute("GRANT CREATE ON ALL ROLES TO mike")
        cassandra.execute("GRANT AUTHORIZE ON ALL ROLES TO mike")

        # mike can create roles, but not with superuser status
        # and can grant any role, including those with superuser status
        mike = self.get_session(user="mike", password="12345")
        mike.execute("CREATE ROLE role1 WITH SUPERUSER = false")
        mike.execute("GRANT non_superuser TO role1")
        mike.execute("GRANT another_superuser TO role1")
        assert_invalid(mike, "CREATE ROLE role2 WITH SUPERUSER = true", "Only superusers can create a role with superuser status", Unauthorized)
        assert_all(cassandra, "LIST ROLES OF role1", [["another_superuser", True, False, {}], ["non_superuser", False, False, {}], ["role1", False, False, {}]])

    def test_drop_and_revoke_roles_with_superuser_status(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE ROLE another_superuser WITH SUPERUSER = true AND LOGIN = false")
        cassandra.execute("CREATE ROLE non_superuser WITH SUPERUSER = false AND LOGIN = false")
        cassandra.execute("CREATE ROLE role1 WITH SUPERUSER = false")
        cassandra.execute("GRANT another_superuser TO role1")
        cassandra.execute("GRANT non_superuser TO role1")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("GRANT DROP ON ALL ROLES TO mike")
        cassandra.execute("GRANT AUTHORIZE ON ALL ROLES TO mike")

        # mike can drop and revoke any role, including superusers
        mike = self.get_session(user="mike", password="12345")
        mike.execute("REVOKE another_superuser FROM role1")
        mike.execute("REVOKE non_superuser FROM role1")
        mike.execute("DROP ROLE non_superuser")
        mike.execute("DROP ROLE role1")

    def test_drop_role_removes_memberships(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("GRANT role2 TO role1")
        cassandra.execute("GRANT role1 TO mike")
        assert_all(cassandra, "LIST ROLES OF mike", [mike_role, role1_role, role2_role])

        # drop the role indirectly granted
        cassandra.execute("DROP ROLE role2")
        assert_all(cassandra, "LIST ROLES OF mike", [mike_role, role1_role])

        cassandra.execute("CREATE ROLE role2")
        cassandra.execute("GRANT role2 to role1")
        assert_all(cassandra, "LIST ROLES OF mike", [mike_role, role1_role, role2_role])
        # drop the directly granted role
        cassandra.execute("DROP ROLE role1")
        assert_one(cassandra, "LIST ROLES OF mike", mike_role)
        assert_all(cassandra, "LIST ROLES", [cassandra_role, mike_role, role2_role])

    def test_drop_role_revokes_permissions_granted_on_it(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("GRANT ALTER ON ROLE role1 TO mike")
        cassandra.execute("GRANT AUTHORIZE ON ROLE role2 TO mike")

        self.assert_permissions_listed([("mike", "<role role1>", "ALTER"), ("mike", "<role role2>", "AUTHORIZE")], cassandra, "LIST ALL PERMISSIONS OF mike")

        cassandra.execute("DROP ROLE role1")
        cassandra.execute("DROP ROLE role2")
        assert len(list(cassandra.execute("LIST ALL PERMISSIONS OF mike"))) == 0

    def test_grant_revoke_roles(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")
        cassandra.execute("GRANT role1 TO role2")
        cassandra.execute("GRANT role2 TO mike")

        assert_all(cassandra, "LIST ROLES OF role2", [role1_role, role2_role])
        assert_all(cassandra, "LIST ROLES OF mike", [mike_role, role1_role, role2_role])
        assert_all(cassandra, "LIST ROLES OF mike NORECURSIVE", [mike_role, role2_role])

        cassandra.execute("REVOKE role2 FROM mike")
        assert_one(cassandra, "LIST ROLES OF mike", mike_role)

        cassandra.execute("REVOKE role1 FROM role2")
        assert_one(cassandra, "LIST ROLES OF role2", role2_role)

    def test_grant_revoke_role_validation(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        mike = self.get_session(user="mike", password="12345")

        assert_invalid(cassandra, "GRANT role1 TO mike", "role1 doesn't exist")
        cassandra.execute("CREATE ROLE role1")

        assert_invalid(cassandra, "GRANT role1 TO john", "john doesn't exist")
        # In GRANT statement, the target role is verified first
        assert_invalid(cassandra, "GRANT role2 TO john", "john doesn't exist")

        cassandra.execute("CREATE ROLE john WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        assert_invalid(cassandra, "GRANT role2 TO john", "role2 doesn't exist")
        cassandra.execute("CREATE ROLE role2")

        assert_invalid(mike, "GRANT role2 TO john", "User mike has no AUTHORIZE permission on <role role2> or any of its parents", Unauthorized)

        # superusers can always grant roles
        cassandra.execute("GRANT role1 TO john")
        # but regular users need AUTHORIZE permission on the granted role
        cassandra.execute("GRANT AUTHORIZE ON ROLE role2 TO mike")
        mike.execute("GRANT role2 TO john")

        # same applies to REVOKEing roles
        assert_invalid(mike, "REVOKE role1 FROM john", "User mike has no AUTHORIZE permission on <role role1> or any of its parents", Unauthorized)
        cassandra.execute("REVOKE role1 FROM john")
        mike.execute("REVOKE role2 from john")

    def test_list_roles(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")

        assert_all(cassandra, "LIST ROLES", [cassandra_role, mike_role, role1_role, role2_role])

        cassandra.execute("GRANT role1 TO role2")
        cassandra.execute("GRANT role2 TO mike")

        assert_all(cassandra, "LIST ROLES OF role2", [role1_role, role2_role])
        assert_all(cassandra, "LIST ROLES OF mike", [mike_role, role1_role, role2_role])
        assert_all(cassandra, "LIST ROLES OF mike NORECURSIVE", [mike_role, role2_role])

        mike = self.get_session(user="mike", password="12345")
        assert_invalid(mike, "LIST ROLES OF cassandra", "You are not authorized to view the roles granted to role 'cassandra'.", Unauthorized)

        assert_all(mike, "LIST ROLES", [mike_role, role1_role, role2_role])
        assert_all(mike, "LIST ROLES OF mike", [mike_role, role1_role, role2_role])
        assert_all(mike, "LIST ROLES OF mike NORECURSIVE", [mike_role, role2_role])
        assert_all(mike, "LIST ROLES OF role2", [role1_role, role2_role])

        # without SELECT permission on the root level roles resource, LIST ROLES with no OF
        # returns only the roles granted to the user. With it, it includes all roles.
        assert_all(mike, "LIST ROLES", [mike_role, role1_role, role2_role])
        cassandra.execute("GRANT DESCRIBE ON ALL ROLES TO mike")
        assert_all(mike, "LIST ROLES", [cassandra_role, mike_role, role1_role, role2_role])

    def test_grant_revoke_permissions(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("GRANT ALL ON table ks.cf TO role1")
        cassandra.execute("GRANT role1 TO mike")

        mike = self.get_session(user="mike", password="12345")
        mike.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")

        assert_one(mike, "SELECT * FROM ks.cf", [0, 0])

        cassandra.execute("REVOKE role1 FROM mike")
        assert_invalid(mike, "INSERT INTO ks.cf (id, val) VALUES (0, 0)", re.escape("mike has no MODIFY permission on <table ks.cf> or any of its parents"), Unauthorized)

        cassandra.execute("GRANT role1 TO mike")
        cassandra.execute("REVOKE ALL ON ks.cf FROM role1")

        assert_invalid(mike, "INSERT INTO ks.cf (id, val) VALUES (0, 0)", re.escape("mike has no MODIFY permission on <table ks.cf> or any of its parents"), Unauthorized)

    def test_role_caching_authenticated_user(self):
        # This test is to show that the role caching in AuthenticatedUser
        # works correctly and revokes the roles from a logged in user
        self.prepare(roles_expiry=2000)
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        cassandra.execute("CREATE TABLE ks.cf (id int primary key, val int)")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("GRANT ALL ON table ks.cf TO role1")
        cassandra.execute("GRANT role1 TO mike")

        mike = self.get_session(user="mike", password="12345")
        mike.execute("INSERT INTO ks.cf (id, val) VALUES (0, 0)")

        assert_one(mike, "SELECT * FROM ks.cf", [0, 0])

        cassandra.execute("REVOKE role1 FROM mike")
        # mike should retain permissions until the cache expires
        unauthorized = None
        cnt = 0
        while not unauthorized and cnt < 20:
            try:
                mike.execute("SELECT * FROM ks.cf")
                cnt += 1
                time.sleep(0.5)
            except Unauthorized as e:
                unauthorized = e

        assert unauthorized is not None

    def test_prevent_circular_grants(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE ROLE mike")
        cassandra.execute("CREATE ROLE role1")
        cassandra.execute("CREATE ROLE role2")
        cassandra.execute("GRANT role2 to role1")
        cassandra.execute("GRANT role1 TO mike")
        assert_invalid(cassandra, "GRANT mike TO role1", "mike already includes role role1.", InvalidRequest)
        assert_invalid(cassandra, "GRANT mike TO role2", "mike already includes role role2.", InvalidRequest)

    def test_create_user_as_alias_for_create_role(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE USER mike WITH PASSWORD '12345' NOSUPERUSER")
        assert_one(cassandra, "LIST ROLES OF mike", mike_role)

        cassandra.execute("CREATE USER super_user WITH PASSWORD '12345' SUPERUSER")
        assert_one(cassandra, "LIST ROLES OF super_user", ["super_user", True, True, {}])

    def test_role_name(self):
        """Simple test to verify the behavior of quoting when creating roles & users
        @jira_ticket CASSANDRA-10394
        """
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        # unquoted identifiers and unreserved keyword do not preserve case
        # count
        cassandra.execute("CREATE ROLE ROLE1 WITH PASSWORD = '12345' AND LOGIN = true")
        self.assert_unauthenticated("Username and/or password are incorrect", "ROLE1", "12345")
        self.get_session(user="role1", password="12345")

        cassandra.execute("CREATE ROLE COUNT WITH PASSWORD = '12345' AND LOGIN = true")
        self.assert_unauthenticated("Username and/or password are incorrect", "COUNT", "12345")
        self.get_session(user="count", password="12345")

        # string literals and quoted names do preserve case
        cassandra.execute("CREATE ROLE 'ROLE2' WITH PASSWORD = '12345' AND LOGIN = true")
        self.get_session(user="ROLE2", password="12345")
        self.assert_unauthenticated("Username and/or password are incorrect", "Role2", "12345")

        cassandra.execute("""CREATE ROLE "ROLE3" WITH PASSWORD = '12345' AND LOGIN = true""")
        self.get_session(user="ROLE3", password="12345")
        self.assert_unauthenticated("Username and/or password are incorrect", "Role3", "12345")

        # when using legacy USER syntax, both unquoted identifiers and string literals preserve case
        cassandra.execute("CREATE USER USER1 WITH PASSWORD '12345'")
        self.get_session(user="USER1", password="12345")
        self.assert_unauthenticated("Username and/or password are incorrect", "User1", "12345")

        cassandra.execute("CREATE USER 'USER2' WITH PASSWORD '12345'")
        self.get_session(user="USER2", password="12345")
        self.assert_unauthenticated("Username and/or password are incorrect", "User2", "12345")

    def test_role_requires_login_privilege_to_authenticate(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        assert_one(cassandra, "LIST ROLES OF mike", mike_role)
        self.get_session(user="mike", password="12345")

        cassandra.execute("ALTER ROLE mike WITH LOGIN = false")
        assert_one(cassandra, "LIST ROLES OF mike", ["mike", False, False, {}])
        # disable this check for issue: auth roles: user can still login even login is set to False #4284
        # self.assert_unauthenticated('mike is not permitted to log in', 'mike', '12345')

        cassandra.execute("ALTER ROLE mike WITH LOGIN = true")
        assert_one(cassandra, "LIST ROLES OF mike", ["mike", False, True, {}])
        self.get_session(user="mike", password="12345")

    def test_role_requires_password_to_login(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE ROLE mike WITH SUPERUSER = false AND LOGIN = true")
        self.assert_unauthenticated("Username and/or password are incorrect", "mike", None)
        cassandra.execute("ALTER ROLE mike WITH PASSWORD = '12345'")
        self.get_session(user="mike", password="12345")

    def test_superuser_status_is_inherited(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("CREATE ROLE db_admin WITH SUPERUSER = true")

        mike = self.get_session(user="mike", password="12345")
        assert_invalid(mike, "CREATE ROLE another_role WITH SUPERUSER = false AND LOGIN = false", "User mike has no CREATE permission on <all roles> or any of its parents", Unauthorized)

        cassandra.execute("GRANT db_admin TO mike")
        mike.execute("CREATE ROLE another_role WITH SUPERUSER = false AND LOGIN = false")
        assert_all(mike, "LIST ROLES", [["another_role", False, False, {}], cassandra_role, ["db_admin", True, False, {}], mike_role])

    def test_list_users_considers_inherited_superuser_status(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE ROLE db_admin WITH SUPERUSER = true")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND SUPERUSER = false AND LOGIN = true")
        cassandra.execute("GRANT db_admin TO mike")
        assert_all(cassandra, "LIST USERS", [["cassandra", True], ["mike", True]])

    def test_builtin_functions_require_no_special_permissions(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        self.setup_table(cassandra)
        cassandra.execute("INSERT INTO ks.t1 (k,v) VALUES (1,1)")
        mike = self.get_session(user="mike", password="12345")
        cassandra.execute("GRANT ALL PERMISSIONS ON ks.t1 TO mike")
        assert_one(mike, "SELECT * from ks.t1 WHERE k=blobasint(intasblob(1))", [1, 1])

    def test_disallow_grant_revoke_on_builtin_functions(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        self.setup_table(cassandra)
        cassandra.execute("CREATE ROLE mike")
        assert_invalid(cassandra, "GRANT EXECUTE ON FUNCTION system.intasblob(int) TO mike", "Altering permissions on builtin functions is not supported", InvalidRequest)
        assert_invalid(cassandra, "REVOKE ALL PERMISSIONS ON FUNCTION system.intasblob(int) FROM mike", "Altering permissions on builtin functions is not supported", InvalidRequest)
        assert_invalid(cassandra, "GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE system TO mike", "Altering permissions on builtin functions is not supported", InvalidRequest)
        assert_invalid(cassandra, "REVOKE ALL PERMISSIONS ON ALL FUNCTIONS IN KEYSPACE system FROM mike", "Altering permissions on builtin functions is not supported", InvalidRequest)

    def test_disallow_grant_execute_on_non_function_resources(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        self.setup_table(cassandra)
        cassandra.execute("CREATE ROLE mike")
        cassandra.execute("CREATE ROLE role1")

        # can't grant EXECUTE on data or role resources
        # Resource type DataResource does not support any of the requested permissions
        assert_invalid(cassandra, "GRANT EXECUTE ON ALL KEYSPACES TO mike", "Resource <all keyspaces> does not support any of the requested permissions", SyntaxException)
        assert_invalid(cassandra, "GRANT EXECUTE ON KEYSPACE ks TO mike", "Resource <keyspace ks> does not support any of the requested permissions", SyntaxException)
        assert_invalid(cassandra, "GRANT EXECUTE ON TABLE ks.t1 TO mike", re.escape("Resource <table ks.t1> does not support any of the requested permissions"), SyntaxException)
        # Resource type RoleResource does not support any of the requested permissions
        assert_invalid(cassandra, "GRANT EXECUTE ON ALL ROLES TO mike", "Resource <all roles> does not support any of the requested permissions", SyntaxException)
        assert_invalid(cassandra, "GRANT EXECUTE ON ROLE mike TO role1", "Resource <role mike> does not support any of the requested permissions", SyntaxException)

    def test_truncate_with_rbac(self):
        self.prepare()
        cassandra = self.get_session(user="cassandra", password="cassandra")
        self.setup_table(cassandra)
        cassandra.execute("INSERT INTO ks.t1 (k,v) VALUES (1,1)")
        cassandra.execute("CREATE ROLE mike WITH PASSWORD = '12345' AND LOGIN = true")
        cassandra.execute("CREATE ROLE readonly_access")
        cassandra.execute("CREATE ROLE readwrite_access")
        cassandra.execute("GRANT ALL ON ks.t1 TO readwrite_access")
        cassandra.execute("GRANT SELECT ON ks.t1 TO readonly_access")
        # ro-access
        cassandra.execute("GRANT readonly_access TO mike")
        mike = self.get_session(user="mike", password="12345")
        truncate_cql = "TRUNCATE TABLE ks.t1 USING TIMEOUT 5m;"
        assert_invalid(mike, truncate_cql, re.escape("User mike has no MODIFY permission on <table ks.t1> or any of its parents"), Unauthorized)
        # rw-access
        cassandra.execute("GRANT readwrite_access TO mike")
        mike.execute(truncate_cql)

    def setup_table(self, session):
        session.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        session.execute("CREATE TABLE ks.t1 (k int PRIMARY KEY, v int)")

    def assert_unauthenticated(self, message, user, password):
        with pytest.raises(NoHostAvailable) as response:
            node = self.cluster.nodelist()[0]
            self.cql_connection(node, user=user, password=password)
        host = next(iter(response.value.errors.keys()))
        error_message = next(iter(response.value.errors.values())).args[0]
        error_object = next(iter(response.value.errors.values()))
        pattern = rf'Failed to authenticate to {host}:.* code=0100 \[Bad credentials\] message="{message}"'
        assert isinstance(error_object, AuthenticationFailed), "Expected AuthenticationFailed, got %s" % type(error_object)
        assert re.search(pattern, str(error_message)), "Expected: %s" % pattern

    def prepare(self, nodes=1, roles_expiry=0):
        config = {
            "authenticator": "org.apache.cassandra.auth.PasswordAuthenticator",
            "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer",
            "role_manager": "org.apache.cassandra.auth.CassandraRoleManager",
            "permissions_validity_in_ms": 0,
            "roles_validity_in_ms": roles_expiry,
        }
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(nodes).start(wait_other_notice=True, wait_for_binary_proto=True)

        found = wait_for_any_log(self.cluster.nodelist(), ["Created default superuser role", "Created default superuser authentication record"], 30, dispersed=True)

        if isinstance(found, list):
            nodes = []
            for n in found:
                nodes.append(n.name)
        else:
            nodes = found.name
        logger.debug(f"Default role created by {nodes}")

    def get_session(self, node_idx=0, user=None, password=None):
        node = self.cluster.nodelist()[node_idx]
        conn = self.patient_cql_connection(node, user=user, password=password)
        return conn

    @staticmethod
    def assert_permissions_listed(expected, session, query):
        rows = session.execute(query)
        perms = [(str(r.role), str(r.resource), str(r.permission)) for r in rows]
        assert sorted(expected) == sorted(perms), "the current permissions do not meet expectations"
