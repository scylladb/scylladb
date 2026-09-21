import logging
import os
import random
import shutil
import subprocess
import tempfile
import time
import uuid

import pytest
from cassandra import InvalidRequest, Unauthorized
from cassandra.cluster import NoHostAvailable

from dtest_class import Tester, create_cf, create_ks
from tools.assertions import assert_eventually_raises
from tools.ldap_docker import LdapDocker
from tools.marks import unmark

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
@pytest.mark.next_gating
@pytest.mark.single_node
class TestLdap(Tester):
    LDAP_USER = "scylla-qa"
    LDAP_PASSWORD = "cassandra"
    use_saslauth = False

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, fixture_dtest_setup):
        self.create_ldap_container()
        self.saslauthd_dir = os.path.join(tempfile.mkdtemp(), "saslauthd")
        os.mkdir(self.saslauthd_dir)
        self.saslauthd_proc = None

        yield

        if self.saslauthd_proc is not None:
            self.saslauthd_proc.kill()
            self.saslauthd_proc.wait()
            stdout, stderr = self.saslauthd_proc.communicate()
            for line in stdout.splitlines():
                logger.debug(f"{line.strip()}")
            for line in stderr.splitlines():
                logger.debug(f"{line.strip()}")

        # stop the cluster before killing the ldap container
        # to avoid errors like "ldap_role_manager - error in reconnect: std::system_error",
        # on the nodes
        self.cluster.stop(gently=True)
        # Next line requires self.test_path directory to be empty.
        shutil.rmtree(self.saslauthd_dir, ignore_errors=True)
        self.test_ldap_docker.remove_container(force=True)

    @staticmethod
    def wait_for_text(proc, text):
        for i in range(20):
            for stderr_line in iter(proc.stderr.readline, ""):
                logger.debug(f"{stderr_line.strip()}")
                if text in stderr_line:
                    break
            else:
                time.sleep(0.1)
                continue
            break
        else:
            raise TimeoutError(f"failed to get '{text}' in proc stdout/stderr")

    def get_default_scylla_yaml_ldap_config(self):
        return {
            "role_manager": "com.scylladb.auth.LDAPRoleManager",
            "ldap_url_template": f"{self.test_ldap_docker.ldap_server.name}/{self.test_ldap_docker.ldap_base_object}?cn?sub?(uniqueMember=uid={{USER}},ou=Person,{self.test_ldap_docker.ldap_base_object})",
            "ldap_attr_role": "cn",
            "ldap_bind_dn": f"cn=admin,{self.test_ldap_docker.ldap_base_object}",
            "ldap_bind_passwd": "scylla",
        }

    def create_role_in_ldap(self, user, password):
        self.test_ldap_docker.add_ldap_object(f"uid={user},ou=Person,{self.test_ldap_docker.ldap_base_object}", ["uidObject", "organizationalPerson", "top"], {"userPassword": password, "sn": "PersonSn", "cn": "PersonCn"})

    def create_role(self, session, user, password):
        if self.use_saslauth:
            session.execute(f"CREATE ROLE '{user}' WITH login=true")
            self.create_role_in_ldap(user, password)
        else:
            session.execute(f"CREATE ROLE '{user}' WITH login=true AND password='{password}'")

    def prepare(  # noqa: PLR0912, PLR0913
        self,
        nodes=1,
        user="cassandra",
        password="cassandra",
        configure_ldap=True,
        create_role=True,
        create_ks_and_table=True,
        add_cassandra_superuser_to_ldap=True,
        **kwargs,
    ):
        self.nodes = []
        config = dict()
        options = kwargs.get("options", None)
        if options:
            config.update(options)
        if nodes < 3:
            config.update({"commitlog_sync": "batch"})
        cluster = self.cluster
        if configure_ldap:
            ldap_options = kwargs.get("ldap_options", None)
            self.test_ldap_docker.create_ldap_connection()
            if self.use_saslauth:
                saslauthd_conf_path = os.path.join(self.saslauthd_dir, "saslauthd.conf")
                with open(saslauthd_conf_path, "w") as f:
                    f.write(
                        f"ldap_servers: {self.test_ldap_docker.ldap_server.name}\nldap_search_base: ou=Person,{self.test_ldap_docker.ldap_base_object}\nldap_bind_dn: cn=admin,{self.test_ldap_docker.ldap_base_object}\nldap_bind_pw: scylla\n"
                    )
                self.saslauthd_proc = subprocess.Popen(["saslauthd", "-d", "-n", "1", "-l", "-a", "ldap", "-O", saslauthd_conf_path, "-m", self.saslauthd_dir], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                # make sure saslauthd has start and listening
                self.wait_for_text(self.saslauthd_proc, "listening on socket")

                self.test_ldap_docker.add_ldap_object(f"ou=Person,{self.test_ldap_docker.ldap_base_object}", ["organizationalUnit", "top"], {"ou": "Person"})
            if ldap_options:
                config.update(ldap_options)
            else:
                config.update(self.get_default_scylla_yaml_ldap_config())
        if kwargs.get("start_rpc", False):
            config.update(values={"start_rpc": True})
        config.update({"authorizer": "org.apache.cassandra.auth.CassandraAuthorizer", "permissions_validity_in_ms": 0})

        if self.use_saslauth and configure_ldap:
            logger.info("Using com.scylladb.auth.SaslauthdAuthenticator")
            config.update({"authenticator": "com.scylladb.auth.SaslauthdAuthenticator", "saslauthd_socket_path": os.path.join(self.saslauthd_dir, "mux")})
        else:
            logger.info("Using org.apache.cassandra.auth.PasswordAuthenticator")
            config.update({"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator"})
        cluster.set_configuration_options(values=config)

        if not cluster.nodelist():
            # --logger-log-level ldap_role_manager=debug
            (cluster.populate(nodes).start(wait_for_binary_proto=True, jvm_args=["--logger-log-level", "auth-cache=debug"]),)
        self.nodes = cluster.nodelist()[:]

        if self.use_saslauth and configure_ldap and add_cassandra_superuser_to_ldap:
            self.create_role_in_ldap(user, password)
        self.nodes[0].watch_log_for("Created default superuser role 'cassandra'")
        session = self.patient_cql_connection(self.nodes[0], user=user, password=password)
        if create_role:
            self.create_role(session, self.LDAP_USER, self.LDAP_PASSWORD)
        if create_ks_and_table:
            create_ks(session, name="ks", rf=1)
            create_cf(session, name="cf")
            session.execute("INSERT INTO ks.cf (key, c, v) VALUES ('key1', 'c1', 'v1')")

    def create_ldap_container(self):
        docker_name = "-".join(["openldap", f"{uuid.uuid4()}"[:8]])
        self.test_ldap_docker = LdapDocker()
        self.test_ldap_docker.create_ldap_container(name=docker_name)

    def add_role_to_ldap(self, ldap_role="cassandra", ldap_password=LDAP_PASSWORD, unique_members=None):
        unique_members_list = []
        if not unique_members:
            unique_members = [self.LDAP_USER, "qa-user", "cassandra"]
        for member in unique_members:
            if self.use_saslauth:
                self.create_role_in_ldap(member, ldap_password)
            unique_members_list.append(f"uid={member},ou=Person,{self.test_ldap_docker.ldap_base_object}")
        ldap_user_group = [f"cn={ldap_role},{self.test_ldap_docker.ldap_base_object}", ["groupOfUniqueNames", "simpleSecurityObject", "top"], {"uniqueMember": unique_members_list, "userPassword": ldap_password}]

        self.test_ldap_docker.add_ldap_object(*ldap_user_group)

    @staticmethod
    def create_role_grant_permission(session, permission_dict):
        session.execute(f"CREATE ROLE '{permission_dict['role']}'")
        for permission in permission_dict["permissions"]:
            session.execute(f"GRANT {permission} ON {permission_dict['resource']} TO '{permission_dict['role']}'")

    def check_user_permissions(self, permission_dict):
        # Unauthorized-expecting checks are retried because auth-cache updates
        # are eventually consistent. Each retry uses a fresh resource name to
        # avoid collisions with stale-cache successes.
        session = self.patient_cql_connection(self.nodes[0], user=permission_dict["user"], password=permission_dict["password"])
        user = permission_dict["user"]

        def try_create_ks():
            create_ks(session=session, name=f"ks_{uuid.uuid4().__repr__()[6:14]}", rf=1)

        def try_create_cf():
            # `ks` is created during prepare(), so the table is created in an
            # existing keyspace — only Unauthorized is expected here.
            table = f"ks.table_{uuid.uuid4().__repr__()[6:14]}"
            create_cf(session=session, name=table)

        def try_modify(stmt):
            return lambda: session.execute(stmt)

        if "create" not in permission_dict["permissions"]:
            assert_eventually_raises(
                try_create_ks,
                Unauthorized,
                match=rf"User {user} has no CREATE permission on ",
            )
            assert_eventually_raises(
                try_create_cf,
                Unauthorized,
                match=rf"User {user} has no CREATE permission on ",
            )
        else:
            random_name = uuid.uuid4().__repr__()[6:14]
            create_ks(session=session, name=f"ks_{random_name}", rf=1)
            create_cf(session=session, name=f"table_{random_name}")

        if "modify" not in permission_dict["permissions"]:
            modify_match = rf"User {user} has no MODIFY permission on "
            assert_eventually_raises(
                try_modify("INSERT INTO ks.cf (key, c, v) VALUES ('key', 'c', 'v')"),
                Unauthorized,
                match=modify_match,
            )
            assert_eventually_raises(
                try_modify("UPDATE ks.cf SET v = 'vv' WHERE key = 'key' and c = 'c'"),
                Unauthorized,
                match=modify_match,
            )
            assert_eventually_raises(
                try_modify("DELETE from ks.cf WHERE key = 'key' and c = 'c'"),
                Unauthorized,
                match=modify_match,
            )
        else:
            session.execute("INSERT INTO ks.cf (key, c, v) VALUES ('key', 'c', 'v')")
            session.execute("UPDATE ks.cf SET v = 'vv' WHERE key = 'key' and c = 'c'")
            session.execute("DELETE from ks.cf WHERE key = 'key' and c = 'c'")

        if "select" not in permission_dict["permissions"]:
            assert_eventually_raises(
                lambda: session.execute("SELECT * from ks.cf LIMIT 1"),
                Unauthorized,
                match=rf"User {user} has no SELECT permission on ",
            )
        else:
            session.execute("SELECT * from ks.cf LIMIT 1")

    def test_simple_ldap_connection(self):
        self.prepare()
        self.add_role_to_ldap()
        permission = {"user": self.LDAP_USER, "password": self.LDAP_PASSWORD, "role": "cassandra", "permissions": ["create", "modify", "select"], "resource": "all"}
        self.check_user_permissions(permission_dict=permission)

    def test_user_login_only(self):
        self.prepare()
        self.add_role_to_ldap()
        session = self.patient_cql_connection(self.nodes[0], user=self.LDAP_USER, password=self.LDAP_PASSWORD)
        self.create_role(session, "login_user", "test")
        permission = {"user": "login_user", "password": "test", "role": "empty_role", "permissions": [], "resource": None}
        self.check_user_permissions(permission_dict=permission)

    def test_wrong_user(self):
        self.prepare()
        self.add_role_to_ldap()
        failed = False
        try:
            self.cql_connection(self.nodes[0], user="abcd", password=self.LDAP_PASSWORD)
        except Exception:  # noqa: BLE001
            logger.info(f"Failed to get a session for an user that does't exist - Success")
            failed = True
        if not failed:
            raise Exception("User succeeded to create a session, instead of failing")

    def test_partial_permissions(self):
        self.prepare()
        create_permission = {"user": "create_user", "password": "create_user", "role": "create_role", "permissions": ["create"], "resource": "all keyspaces"}
        select_permission = {"user": "select_user", "password": "select_user", "role": "select_role", "permissions": ["select"], "resource": "all keyspaces"}
        modify_permission = {"user": "modify_user", "password": "modify_user", "role": "modify_role", "permissions": ["modify"], "resource": "all keyspaces"}
        create_select_permission = {"user": "create_select_user", "password": "create_select_user", "role": "create_select_role", "permissions": ["create", "select"], "resource": "all keyspaces"}
        modify_select_permission = {"user": "modify_select_user", "password": "modify_select_user", "role": "modify_select_role", "permissions": ["modify", "select"], "resource": "all keyspaces"}
        create_modify_select_permission = {"user": "create_modify_select_user", "password": "create_modify_select_user", "role": "create_modify_select_role", "permissions": ["create", "modify", "select"], "resource": "all keyspaces"}
        all_permissions = {
            "create": create_permission,
            "select": select_permission,
            "modify": modify_permission,
            "create_select": create_select_permission,
            "modify_select": modify_select_permission,
            "create_modify_select": create_modify_select_permission,
        }
        for k, permission_dict in all_permissions.items():
            logger.info(f"Starting with {k}")
            session = self.patient_cql_connection(self.nodes[0], user="cassandra", password="cassandra")
            self.create_role_grant_permission(session=session, permission_dict=permission_dict)
            self.create_role(session, permission_dict["user"], permission_dict["password"])
            self.add_role_to_ldap(ldap_role=permission_dict["role"], unique_members=[permission_dict["user"]])
            self.check_user_permissions(permission_dict=permission_dict)
            logger.info(f"Finished with {k}")

    @unmark.next_gating
    @pytest.mark.no_boot_speedups
    def test_hard_restart_scylla(self):
        self.prepare()
        self.add_role_to_ldap()
        permission = {"user": self.LDAP_USER, "password": self.LDAP_PASSWORD, "role": "empty_role", "permissions": ["create", "modify", "select"], "resource": None}
        self.check_user_permissions(permission_dict=permission)
        self.nodes[0].stop(gently=False)
        self.nodes[0].start(wait_other_notice=True, wait_for_binary_proto=True)
        self.check_user_permissions(permission_dict=permission)

    def test_soft_restart_scylla(self):
        self.prepare()
        self.add_role_to_ldap()
        permission = {"user": self.LDAP_USER, "password": self.LDAP_PASSWORD, "role": "empty_role", "permissions": ["create", "modify", "select"], "resource": None}
        self.check_user_permissions(permission_dict=permission)
        self.nodes[0].stop()
        self.nodes[0].start(wait_other_notice=True, wait_for_binary_proto=True)
        self.check_user_permissions(permission_dict=permission)

    def test_multiple_roles_superuser(self):
        self.prepare()
        list_of_roles = ["r1", "r2", "r3", "r4", "r5", "cassandra"]
        cassandra_session = self.patient_cql_connection(node=self.nodes[0], user="cassandra", password="cassandra")
        for role in list_of_roles:
            permission = {"user": self.LDAP_USER, "password": self.LDAP_PASSWORD, "role": role, "permissions": ["create", "modify", "select"], "resource": "ALL KEYSPACES"}
            if not role == "cassandra":
                self.create_role_grant_permission(session=cassandra_session, permission_dict=permission)
            self.add_role_to_ldap(ldap_role=role)
            self.check_user_permissions(permission_dict=permission)

    def test_multiple_roles_single_permission(self):
        self.prepare(create_role=False)
        actions_list = ["create", "modify", "select"]
        permission = random.choice(actions_list)
        logger.info(f"permission={permission}")
        list_of_roles = ["r1", "r2", "r3", "r4", "r5"]
        cassandra_session = self.patient_cql_connection(node=self.nodes[0], user="cassandra", password="cassandra")
        self.create_role(cassandra_session, self.LDAP_USER, self.LDAP_PASSWORD)
        permission_dict = {"user": f"{self.LDAP_USER}", "password": self.LDAP_PASSWORD, "permissions": [permission], "resource": "ALL KEYSPACES"}
        for role in list_of_roles:
            permission_dict["role"] = role
            self.create_role_grant_permission(session=cassandra_session, permission_dict=permission_dict)
            self.add_role_to_ldap(ldap_role=role)
            self.check_user_permissions(permission_dict=permission_dict)

    def test_multiple_roles_permissions_combination(self):
        self.prepare(create_role=False)
        actions_list = ["create", "modify", "select"]
        list_of_roles = ["r1", "r2", "r3", "r4", "r5", "r6"]
        cassandra_session = self.patient_cql_connection(node=self.nodes[0], user="cassandra", password="cassandra")
        self.create_role(cassandra_session, self.LDAP_USER, self.LDAP_PASSWORD)
        permission = {"user": f"{self.LDAP_USER}", "password": self.LDAP_PASSWORD, "resource": "ALL KEYSPACES"}
        for role, action in zip(list_of_roles, actions_list * 2):
            permission["role"] = role
            permission["permissions"] = [action]
            self.create_role_grant_permission(session=cassandra_session, permission_dict=permission)
            self.add_role_to_ldap(ldap_role=f"{role}")
        permission["permissions"] = actions_list[:]
        self.check_user_permissions(permission_dict=permission)

    @unmark.next_gating
    def test_add_ldap_after_regular_work(self):
        self.prepare(create_role=False, configure_ldap=False)
        cassandra_session = self.patient_cql_connection(node=self.nodes[0], user="cassandra", password="cassandra")
        cassandra_session.execute(f"create role '{self.LDAP_USER}' with login=true and password='{self.LDAP_PASSWORD}'")
        self.create_role_grant_permission(session=cassandra_session, permission_dict={"role": "select_modify", "permissions": ["select", "modify"], "resource": "ALL KEYSPACES"})
        cassandra_session.execute(f"grant 'select_modify' to '{self.LDAP_USER}'")
        for i in range(100):
            cassandra_session.execute(f"INSERT INTO ks.cf (key, c, v) VALUES ('key{i}', 'c{i}', 'v{i}')")
        self.test_ldap_docker.create_ldap_connection()
        self.cluster.set_configuration_options(values=self.get_default_scylla_yaml_ldap_config())
        self.nodes[0].stop()
        self.nodes[0].start(wait_other_notice=True, wait_for_binary_proto=True)
        self.add_role_to_ldap()
        permission = {"user": self.LDAP_USER, "password": self.LDAP_PASSWORD, "role": "cassandra", "permissions": ["create", "modify", "select"], "resource": "ALL KEYSPACES"}
        self.check_user_permissions(permission_dict=permission)

    def test_multiple_users_superuser_role(self):
        self.prepare()
        list_of_unique_members = [f"user_{i}" for i in range(10)]
        self.add_role_to_ldap(unique_members=list_of_unique_members)
        session = self.patient_cql_connection(self.nodes[0], user="cassandra", password="cassandra")
        for user in list_of_unique_members:
            self.create_role(session, user, self.LDAP_PASSWORD)
            permission = {"user": user, "password": self.LDAP_PASSWORD, "role": "cassandra", "permissions": ["create", "modify", "select"], "resource": "ALL KEYSPACES"}
            self.check_user_permissions(permission_dict=permission)

    def test_multiple_users_with_modify_role(self):
        self.prepare()
        session = self.patient_cql_connection(self.nodes[0], user="cassandra", password="cassandra")
        permission = {"password": self.LDAP_PASSWORD, "role": "modify_role", "permissions": ["modify"], "resource": "ALL KEYSPACES"}
        self.create_role_grant_permission(session=session, permission_dict=permission)
        list_of_unique_members = [f"user_{i}" for i in range(10)]
        self.add_role_to_ldap(ldap_role="modify_role", unique_members=list_of_unique_members)
        for user in list_of_unique_members:
            self.create_role(session, user, self.LDAP_PASSWORD)
            permission["user"] = user
            self.check_user_permissions(permission_dict=permission)

    def test_grant_role_permissions(self):
        self.prepare()
        permission = {"user": self.LDAP_USER, "password": self.LDAP_PASSWORD, "role": "test_role", "permissions": ["create"], "resource": "all keyspaces"}

        session = self.patient_cql_connection(self.nodes[0], user="cassandra", password="cassandra")
        self.create_role_grant_permission(session=session, permission_dict=permission)
        self.add_role_to_ldap(ldap_role=permission["role"], unique_members=[permission["user"]])
        self.check_user_permissions(permission_dict=permission)
        session.execute(f"GRANT modify ON {permission['resource']} TO '{permission['role']}'")
        permission["permissions"].append("modify")
        self.check_user_permissions(permission_dict=permission)
        session.execute(f"GRANT select ON {permission['resource']} TO '{permission['role']}'")
        permission["permissions"].append("select")
        self.check_user_permissions(permission_dict=permission)

    def test_revoke_role_permissions(self):
        self.prepare()
        permission = {"user": self.LDAP_USER, "password": self.LDAP_PASSWORD, "role": "test_role", "permissions": ["create", "modify", "select"], "resource": "all keyspaces"}
        session = self.patient_cql_connection(self.nodes[0], user="cassandra", password="cassandra")
        self.create_role_grant_permission(session=session, permission_dict=permission)
        self.add_role_to_ldap(ldap_role=permission["role"], unique_members=[permission["user"]])
        self.check_user_permissions(permission_dict=permission)
        revoke_permission = permission["permissions"].pop(-1)
        session.execute(f"REVOKE {revoke_permission} ON {permission['resource']} FROM '{permission['role']}'")
        self.check_user_permissions(permission_dict=permission)
        revoke_permission = permission["permissions"].pop(-1)
        session.execute(f"REVOKE {revoke_permission} ON {permission['resource']} FROM '{permission['role']}'")
        self.check_user_permissions(permission_dict=permission)

    def test_remove_user_from_ldap(self):
        self.prepare()
        self.add_role_to_ldap()
        permission = {"user": self.LDAP_USER, "password": self.LDAP_PASSWORD, "role": "cassandra", "permissions": ["create", "modify", "select"], "resource": "all"}
        self.check_user_permissions(permission_dict=permission)
        dn = str(self.test_ldap_docker.search_ldap_object(self.test_ldap_docker.ldap_base_object, f"(cn={permission['role']})")).split()[1]
        res = self.test_ldap_docker.modify_ldap_object(dn, {"uniqueMember": [("MODIFY_DELETE", [f"uid={permission['user']},ou=Person,dc=scylladb,dc=com"])]})
        if not res:
            raise Exception("Failed to delete user from LDAP")
        permission["permissions"] = []
        self.check_user_permissions(permission_dict=permission)

    def test_modify_username_on_ldap(self):
        self.prepare()
        self.add_role_to_ldap()
        permission = {"user": self.LDAP_USER, "password": self.LDAP_PASSWORD, "role": "cassandra", "permissions": ["create", "modify", "select"], "resource": "all"}
        self.check_user_permissions(permission_dict=permission)
        new_permission = permission.copy()
        permission["permissions"] = []
        new_user = "qa-user"
        new_permission["user"] = new_user
        dn = str(self.test_ldap_docker.search_ldap_object(self.test_ldap_docker.ldap_base_object, f"(cn={permission['role']})")).split()[1]
        res = self.test_ldap_docker.modify_ldap_object(dn, {"uniqueMember": [("MODIFY_REPLACE", [f"uid={new_user},ou=Person,dc=scylladb,dc=com"])]})
        if not res:
            raise Exception("Failed to modify user on LDAP")
        try:
            self.check_user_permissions(permission_dict=permission)
        except Unauthorized as ex:
            logger.info(f"User {permission['user']} was removed, and it was supposed to fail to connect to scylla")
        session = self.patient_cql_connection(self.nodes[0], user="cassandra", password="cassandra")
        self.create_role(session, new_user, permission["password"])
        self.check_user_permissions(permission_dict=new_permission)

    def test_add_user_to_ldap(self):
        self.prepare()
        self.add_role_to_ldap()
        permission = {"user": self.LDAP_USER, "password": self.LDAP_PASSWORD, "role": "cassandra", "permissions": ["create", "modify", "select"], "resource": "all"}
        self.check_user_permissions(permission_dict=permission)
        new_user = "qa-superuser"

        dn = str(self.test_ldap_docker.search_ldap_object(self.test_ldap_docker.ldap_base_object, f"(cn={permission['role']})")).split()[1]
        res = self.test_ldap_docker.modify_ldap_object(dn, {"uniqueMember": [("MODIFY_ADD", [f"uid={new_user},ou=Person,dc=scylladb,dc=com"])]})
        if not res:
            raise Exception("Failed to modify user on LDAP")
        self.check_user_permissions(permission_dict=permission)
        session = self.patient_cql_connection(self.nodes[0], user="cassandra", password="cassandra")
        self.create_role(session, new_user, permission["password"])
        permission["user"] = new_user
        self.check_user_permissions(permission_dict=permission)


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestLdapSaslAuth(TestLdap):
    use_saslauth = True

    def test_authentication(self):
        with pytest.raises(NoHostAvailable, match=r"Bad credentials"):  # User 'cassandra' absent from LDAP.
            self.prepare(add_cassandra_superuser_to_ldap=False)
        self.test_ldap_docker.add_ldap_object(f"uid=cassandra,ou=Person,{self.test_ldap_docker.ldap_base_object}", ["uidObject", "organizationalPerson", "top"], {"userPassword": "cassandra", "sn": "Cassandra", "cn": "Cassandra"})
        self.patient_cql_connection(self.nodes[0], user="cassandra", password="cassandra")
        with pytest.raises(NoHostAvailable, match=r"Bad credentials"):
            self.patient_cql_connection(self.nodes[0], user="cassandra", password="wrong-password")

    def test_switch_with_password_auth(self):
        """
        Create an user without password in Scylla, the password only exists in LDAP server.
        Switch to PasswordAuthenticator and test login without password. Then swtich back to
        SaslauthdAuthenticator and verify login with ldap account.
        """
        self.prepare(nodes=2)
        node1 = self.nodes[0]
        logger.debug(f"A {self.LDAP_USER} user was created without password in Scylla, the password only exists in LDAP server")
        session = self.patient_cql_connection(node1, user="cassandra", password="cassandra")
        self.patient_cql_connection(node1, user=self.LDAP_USER, password=self.LDAP_PASSWORD)

        # Verify that it's not supported to set password in Scylla by cqlsh when SaslauthdAuthenticator is used
        with pytest.raises(Exception, match=r"Cannot modify passwords with SaslauthdAuthenticator"):
            session.execute(f"ALTER ROLE '{self.LDAP_USER}' WITH PASSWORD = 'new_password'")

        logger.debug("Switch to org.apache.cassandra.auth.PasswordAuthenticator, and restart the cluster ...")
        self.cluster.set_configuration_options(values={"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator"})
        self.cluster.stop()

        nodes = self.cluster.nodes.values()
        for node in nodes:
            node.start(wait_for_binary_proto=False)
        for node in nodes:
            node.wait_for_binary_interface()

        session = self.patient_cql_connection(node1, user="cassandra", password="cassandra")

        with pytest.raises(NoHostAvailable, match=r"Bad credentials"):
            logger.debug(f"Try to login by {self.LDAP_USER} without password")
            self.patient_cql_connection(node1, user=self.LDAP_USER)
        with pytest.raises(NoHostAvailable, match=r"Bad credentials"):
            logger.debug(f"Try to login by {self.LDAP_USER} with password, but there is no password in Scylla for the user")
            self.patient_cql_connection(node1, user=self.LDAP_USER, password=self.LDAP_PASSWORD)

        logger.debug(f"Set password for {self.LDAP_USER} in Scylla, and relogin")
        session.execute(f"ALTER ROLE '{self.LDAP_USER}' WITH PASSWORD = '{self.LDAP_PASSWORD}-new'")
        self.patient_cql_connection(node1, user=self.LDAP_USER, password=f"{self.LDAP_PASSWORD}-new")

        logger.debug("Switch back to com.scylladb.auth.SaslauthdAuthenticator, and restart the cluster ...")
        self.cluster.set_configuration_options(values={"authenticator": "com.scylladb.auth.SaslauthdAuthenticator"})
        self.cluster.stop()
        self.cluster.start(wait_for_binary_proto=True)
        logger.debug("Try to login with old password in ldap")
        self.patient_cql_connection(node1, user=self.LDAP_USER, password=f"{self.LDAP_PASSWORD}")

    def test_invalid_saslauthd_socket(self):
        """
        This test tries to set different invalid socket to scylla, and
        restart the cluster and try to login.
        """
        self.prepare()
        node1 = self.nodes[0]
        self.patient_cql_connection(node1, user=self.LDAP_USER, password=self.LDAP_PASSWORD)

        orig_socket_path = os.path.join(self.saslauthd_dir, "mux")
        subprocess.getoutput(f"sudo chown root:root {orig_socket_path}")

        for sock_path in [orig_socket_path, "/tmp/", "/tmp/unexist_socket_path", "/dev/zero", "/run/systemd/journal/stdout"]:
            logger.debug(f"Set an invalid saslauthd_socket_path {sock_path}, and restart cluster ...")
            self.cluster.set_configuration_options(values={"saslauthd_socket_path": "/tmp/"})
            self.cluster.stop()
            self.cluster.start(wait_for_binary_proto=True)

            logger.debug("Try to login ...")
            with pytest.raises(NoHostAvailable, match=r"Bad credentials"):
                self.patient_cql_connection(node1, user=self.LDAP_USER, password=self.LDAP_PASSWORD)
        subprocess.getoutput(f"sudo chown $USER:$USER {orig_socket_path}")

    def _create_new_superuser(self):
        new_superuser_name = "new_superuser"
        new_superuser_password = "new_superuser_pwd"
        with self.patient_cql_connection(self.nodes[0], user="cassandra", password="cassandra") as session:
            self.create_role(session, new_superuser_name, new_superuser_password)
            session.execute(f"ALTER ROLE {new_superuser_name} WITH SUPERUSER=true")
        return new_superuser_name, new_superuser_password

    def _remove_user_from_ldap(self, username, expected_descritpion="success"):
        ret = self.test_ldap_docker.delete_ldap_object(f"uid={username},ou=Person,dc=scylladb,dc=com")
        assert ret["description"] == expected_descritpion

    @pytest.mark.require("scylladb/scylladb#25571")
    def test_drop_cassandra_role(self):
        self.prepare()
        su_name, su_pasword = self._create_new_superuser()
        with self.patient_cql_connection(self.nodes[0], user=su_name, password=su_pasword) as session:
            # Drop old superuser `cassandra`
            self._remove_user_from_ldap("cassandra")
            with pytest.raises(NoHostAvailable, match=r"Incorrect credentials"):
                session2 = self.patient_cql_connection(self.nodes[0], user="cassandra", password="cassandra")
            session.execute(f"DROP ROLE 'cassandra'")

    @pytest.mark.require("scylladb/scylladb#25571")
    def test_drop_role_from_ldap_twice(self):
        self.prepare()
        with self.patient_cql_connection(self.nodes[0], user="cassandra", password="cassandra") as session:
            test_user = "test_user"
            self.create_role(session, test_user, test_user)
            session.execute(f"DROP ROLE '{test_user}'")
            self._remove_user_from_ldap(test_user)
            self._remove_user_from_ldap(test_user, expected_descritpion="noSuchObject")

    @pytest.mark.require("scylladb/scylladb#25571")
    def test_recreate_role_after_drop(self):
        self.prepare()
        with self.patient_cql_connection(self.nodes[0], user="cassandra", password="cassandra") as session:
            test_user = "test_user"
            self.create_role(session, test_user, test_user)
            session.execute(f"DROP ROLE '{test_user}'")
            self._remove_user_from_ldap(test_user)
            self.create_role(session, test_user, test_user)

            # Create and drop a new user that doesn't exist in LDAP
            session.execute(f"CREATE ROLE 'no_ldap_user'")
            session.execute(f"DROP ROLE 'no_ldap_user'")

    @pytest.mark.require("scylladb/scylladb#25571")
    def test_block_current_role_drop(self):
        self.prepare()
        su_name, su_pasword = self._create_new_superuser()
        with self.patient_cql_connection(self.nodes[0], user=su_name, password=su_pasword) as session:
            with pytest.raises(InvalidRequest, match=r"Cannot DROP primary role for current login"):
                session.execute(f"DROP ROLE '{su_name}'")

    @pytest.mark.require("scylladb/scylladb#25571")
    def test_block_not_existing_drop(self):
        self.prepare()
        with self.patient_cql_connection(self.nodes[0], user="cassandra", password="cassandra") as session:
            with pytest.raises(InvalidRequest, match=r"Role not_existing_role doesn't exist."):
                session.execute(f"DROP ROLE 'not_existing_role'")

    @pytest.mark.require("scylladb/scylladb#25571")
    def test_existing_session_after_drop(self):
        self.prepare()
        with self.patient_cql_connection(self.nodes[0], user="cassandra", password="cassandra") as session:
            test_user = "test_user"
            self.create_role(session, test_user, test_user)
            with self.patient_cql_connection(self.nodes[0], user=test_user, password=test_user) as session2:
                session2.execute("LIST ROLES")
                self._remove_user_from_ldap(test_user)
                # User is removed from LDAP but session works correctly
                session2.execute("LIST ROLES")

                # After the user is dropped, the session stops working
                session.execute(f"DROP ROLE '{test_user}'")
                with pytest.raises(InvalidRequest, match=r"Role test_user doesn't exist."):
                    session2.execute("LIST ROLES")
