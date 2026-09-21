import hashlib
import logging
import os
import ssl
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import NoHostAvailable
from cassandra.query import dict_factory
from ccmlib import common

from dtest_class import Tester, wait_for
from dtest_setup import DTestSetup
from tools.misc import generate_ssl_stores

logger = logging.getLogger(__file__)


def get_system_clients_records(session, protocol_version=None, user=None, ssl_context=None):
    # Read content of system.clients:
    #  address    | port  | client_type | connection_stage | driver_name | driver_version | hostname | protocol_version | shard_id | ssl_cipher_suite | ssl_enabled | ssl_protocol | username
    #  ------------+-------+-------------+------------------+-------------+----------------+----------+------------------+----------+------------------+-------------+--------------+-----------
    #  172.17.0.2 | 37392 |         cql |             null |        null |           null |     null |                0 |        0 |             null |        null |         null | anonymous
    #  172.17.0.2 | 37394 |         cql |             null |        null |           null |     null |                0 |        1 |             null |        null |         null | anonymous
    filters = []
    if ssl_context is None:
        filters.append("ssl_enabled=null")
    elif ssl_context != "*":
        filters.append("ssl_enabled=true")

    if user is None:
        filters.append("username='anonymous'")
    elif user != "*":
        filters.append(f"username='{user}'")

    if protocol_version is None:
        filters.append("protocol_version=null")
    elif protocol_version != "*":
        filters.append(f"protocol_version='{protocol_version}'")

    if filters:
        filters = "WHERE " + " AND ".join(filters) + " ALLOW FILTERING"
    else:
        filters = ""
    query = f"SELECT * FROM system.clients {filters}"
    result_list = list(session.execute(query))
    logger.debug("system.clients: %s", result_list)
    return result_list


class SessionStore:
    def __init__(self):
        self._opened_sessions: dict[int : list[CQLSession]] = {}
        self._closed_sessions: list[CQLSession] = []

    def remember_session(self, session: CQLSession):
        session_hash = hash(session)
        if session_hash not in self._opened_sessions:
            self._opened_sessions[hash(session)] = [session]
            return
        self._opened_sessions[hash(session)].append(session)

    def forget_session(self, session: CQLSession):
        opened_session_bucket = self._opened_sessions.get(hash(session), None)
        if opened_session_bucket:
            if session in opened_session_bucket:
                self._opened_sessions[hash(session)].remove(session)
        self._closed_sessions.append(session)

    def clear_sessions(self):
        for session in self._opened_sessions:
            try:
                session.shutdown()
            except Exception:  # noqa: BLE001
                pass
        self._opened_sessions = []

    def expect_system_clients(self, tester):
        for cql_sessions in self._opened_sessions.values():
            if not cql_sessions:
                continue
            cql_sessions[0].check_if_in_system_clients()
        with tester.patient_cql_connection(tester.cluster.nodelist()[0], user="cassandra", password="cassandra") as session:
            for cql_session in self._closed_sessions:
                if hash(cql_session) in self._opened_sessions:
                    continue
                cql_session.check_if_not_in_system_clients(session)


class CQLSession:
    _session = None

    def __init__(  # noqa: PLR0913
        self,
        user=None,
        password=None,
        port=None,
        ssl_context=None,
        protocol_version=None,
        session_store: SessionStore = None,
        session=None,
    ):
        self.user = user
        self.password = password
        self.port = port
        self.ssl_context = ssl_context
        self.protocol_version = protocol_version
        self.session_store = session_store
        self._session = session
        self.session_store.remember_session(self)

    def __str__(self):
        body = ",".join([n + "=" + str(getattr(self, n)) for n in ["user", "port", "ssl_context", "protocol_version"]])
        return f"CQLSession<{body}>"

    def check_if_in_system_clients(self):
        assert len(get_system_clients_records(self._session, protocol_version=self.protocol_version, user=self.user, ssl_context=self.ssl_context)) > 0, f"Can't find record in system.clients for cql session {self!s}"

    def check_if_not_in_system_clients(self, session):
        result = get_system_clients_records(session, protocol_version=self.protocol_version, user=self.user, ssl_context=self.ssl_context)
        assert len(result) == 0, f"Expected to find 0 record in system.clients for cql session {self!s}, but see {len(result)}:\n{result!s}"

    def __enter__(self):
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        if self._session:
            self._session.shutdown()
        self.session_store.forget_session(self)
        return self

    def __hash__(self):
        return int.from_bytes(hashlib.md5(str(self).encode("utf8")).digest(), "little")


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestSystemClients(Tester):
    _test_users = [
        {"user": "user1", "password": "password1"},
        {"user": "user2", "password": "password2"},
        {"user": "user3", "password": "password3"},
        {"user": "user4", "password": "password4"},
        {"user": "user5", "password": "password5"},
    ]

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup: DTestSetup):
        fixture_dtest_setup.allow_log_errors = True

    def node_session(  # noqa: PLR0913
        self,
        node,
        user=None,
        password=None,
        port=None,
        ssl_context=None,
        session_store=None,
        row_factory=None,
    ):
        logger.debug(f"node_session: node={node} user={user} port={port} ssl_context={ssl_context}")
        session = self.patient_cql_connection(
            self.cluster.nodelist()[node],
            user=user,
            password=password,
            port=port,
            ssl_context=ssl_context,
            row_factory=row_factory,
        )
        return CQLSession(
            user=user,
            password=password,
            session_store=session_store,
            port=port,
            ssl_context=ssl_context,
            session=session,
        )

    @staticmethod
    def get_allowed_client_records(session, allow_anonymous=True, step=0.2, timeout=30):
        end_time = time.time() + timeout
        client_records = get_system_clients_records(session, protocol_version="*", user="*", ssl_context="*")
        if not allow_anonymous:
            while True:
                anonymous_records_count = len([record for record in client_records if record.username == "anonymous"])
                if anonymous_records_count == 0:
                    break
                if time.time() > end_time:
                    raise RuntimeError(f"Timed out waiting for no anonymous records in system_clients")
                time.sleep(step)
                client_records = get_system_clients_records(session, protocol_version="*", user="*", ssl_context="*")
        return client_records

    @staticmethod
    def get_total_records_in_system_clients(session, allow_anonymous=True, step=0.2, timeout=30):
        return len(TestSystemClients.get_allowed_client_records(session, allow_anonymous, step, timeout))

    def wait_total_records_in_system_clients(self, session, expected, timeout=30):
        end_time = time.time() + timeout
        while True:
            last_value = self.get_total_records_in_system_clients(session)
            if last_value == expected:
                return
            if time.time() > end_time:
                raise RuntimeError(f"Timed out waiting for number of records in system_clients to get to {expected}, last value was {last_value}")
            time.sleep(0.2)

    def prepare(  # noqa: PLR0913
        self,
        ssl_optional=False,
        require_ssl_auth=False,
        nodes=1,
        system_auth_rf=1,
        superuser=False,
        ssl_enabled=True,
    ):
        cluster = self.cluster
        if ssl_enabled:
            ip_addresses = [f"{cluster.get_ipprefix()}{i}" for i in range(1, nodes + 1)]
            generate_ssl_stores(self.test_path, ip_addresses=ip_addresses)
        # C* versions before 3.0 (CASSANDRA-10559) do not know about
        # 'client_encryption_options.optional' - so we must not add that parameter
        # Note: does of course not work with scylla, we dont support "optional" (3.x feature)
        ssl_options = {
            "enabled": ssl_enabled,
        }
        if ssl_optional:
            ssl_options["optional"] = ssl_optional

        if common.isScylla(cluster.get_install_dir()):
            ssl_options.update({"certificate": os.path.join(self.test_path, "ccm_node.pem"), "keyfile": os.path.join(self.test_path, "ccm_node.key")})
            if require_ssl_auth:
                ssl_options.update({"truststore": os.path.join(self.test_path, "ccm_node.cer"), "require_client_auth": True})
        else:
            ssl_options.update(
                {
                    "keystore": os.path.join(self.test_path, "keystore.jks"),
                    "keystore_password": "cassandra",
                }
            )
            if require_ssl_auth:
                ssl_options.update({"truststore": os.path.join(self.test_path, "truststore.jks"), "truststore_password": "cassandra", "require_client_auth": True})
        cluster.set_configuration_options(
            {
                "client_encryption_options": ssl_options,
                "authenticator": "org.apache.cassandra.auth.PasswordAuthenticator",
                "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer",
                "role_manager": "org.apache.cassandra.auth.CassandraRoleManager",
                "permissions_validity_in_ms": 0,
                "roles_validity_in_ms": 0,
                "native_transport_port": 9042,
                "native_transport_port_ssl": 9142,
            }
        )
        cluster.populate(nodes).start(wait_for_binary_proto=True, wait_other_notice=True)
        with self.patient_cql_connection(self.cluster.nodelist()[0], user="cassandra", password="cassandra", consistency_level=ConsistencyLevel.ALL) as session:
            # with consistent topology auth-v2 is enabled and it doesn't allow nor require to change RF as it replicates via raft
            if "consistent-topology-changes" not in self.scylla_features:
                if system_auth_rf > 1:
                    session.execute(f"ALTER KEYSPACE system_auth WITH REPLICATION = {{'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor':{system_auth_rf}}};")
                    self.cluster.nodelist()[0].nodetool("repair -- system_auth")
            for user_record in self._test_users:
                user = user_record["user"]
                password = user_record["password"]
                session.execute(f"CREATE ROLE '{user}' WITH PASSWORD = '{password}' AND LOGIN = true AND  SUPERUSER = {superuser}")

    def expect_system_clients(self):
        for cql_sessions in self._opened_sessions.values():
            if not cql_sessions:
                continue
            cql_sessions[0].check_if_in_system_clients()

        error = None
        session = None

        for node in self.cluster.nodelist():
            try:
                session = self.patient_cql_connection(node, user="cassandra", password="cassandra")
                break
            except Exception as exc:  # noqa: BLE001
                error = exc

        if not session:
            raise RuntimeError(f"Can't find working node, last error: {error}")

        for cql_session in self._closed_sessions:
            if hash(cql_session) in self._opened_sessions:
                continue
            cql_session.check_if_not_in_system_clients(session)

    def test_system_clients(self):
        self.prepare(nodes=3, ssl_optional=True, require_ssl_auth=False, system_auth_rf=3)
        session_store = SessionStore()

        # Success SSL connection test
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.load_verify_locations(cafile=os.path.join(self.test_path, "ccm_node.cer"))

        with self.node_session(1, **self._test_users[0], port=9142, session_store=session_store, ssl_context=ssl_context):
            session_store.expect_system_clients(tester=self)
        session_store.expect_system_clients(tester=self)

        node_idx_for_testing_session = 1
        other_node_idx = 0
        for node_idx in [node_idx_for_testing_session, other_node_idx]:
            with self.patient_exclusive_cql_connection(self.cluster.nodelist()[node_idx], user="cassandra", password="cassandra") as inspector_session:
                original_sessions_count = self.get_total_records_in_system_clients(inspector_session, allow_anonymous=False)

                # Failed SSL connection test to non-SSL port
                with pytest.raises(Exception):
                    self.node_session(node_idx_for_testing_session, **self._test_users[0], port=9042, session_store=session_store, ssl_context=ssl_context, timeout=10)
                self.wait_total_records_in_system_clients(inspector_session, original_sessions_count)

                # Failed non-SSL connection test to SSL port
                with pytest.raises(Exception):
                    self.node_session(node_idx_for_testing_session, **self._test_users[0], session_store=session_store, port=9142, timeout=10)
                self.wait_total_records_in_system_clients(inspector_session, original_sessions_count)

                # Failed authentication
                with pytest.raises(Exception):
                    self.node_session(node_idx_for_testing_session, user="user-to-fail", password="password-to-fail", session_store=session_store, port=9042, timeout=10)
                self.wait_total_records_in_system_clients(inspector_session, original_sessions_count)

    def test_system_clients_large(self):
        self.prepare(nodes=len(self._test_users) + 1, ssl_optional=True, require_ssl_auth=False, system_auth_rf=len(self._test_users) + 1)
        session_store = SessionStore()

        # Ledger test
        with self.node_session(1, session_store=session_store, **self._test_users[0]):
            session_store.expect_system_clients(tester=self)
            with self.node_session(2, session_store=session_store, **self._test_users[1]):
                session_store.expect_system_clients(tester=self)
                with self.node_session(3, session_store=session_store, **self._test_users[2]):
                    session_store.expect_system_clients(tester=self)
                    with self.node_session(4, session_store=session_store, **self._test_users[3]):
                        session_store.expect_system_clients(tester=self)
                        with self.node_session(5, session_store=session_store, **self._test_users[4]):
                            logger.info("Number of nodes: %s: %s", len(self.cluster.nodelist()), str(self.cluster.nodelist()))
                            with self.node_session(5, session_store=session_store, **self._test_users[0]):
                                logger.info("Number of nodes: %s: %s", len(self.cluster.nodelist()), str(self.cluster.nodelist()))
                                session_store.expect_system_clients(tester=self)
                            session_store.expect_system_clients(tester=self)
                        session_store.expect_system_clients(tester=self)
                    session_store.expect_system_clients(tester=self)
                session_store.expect_system_clients(tester=self)
            session_store.expect_system_clients(tester=self)
        session_store.expect_system_clients(tester=self)

        # Shutdown while session is alive test
        target_node = self.cluster.nodelist()[5]
        logger.info("Number of nodes: %s: %s", len(self.cluster.nodelist()), str(self.cluster.nodelist()))
        with self.node_session(5, session_store=session_store, **self._test_users[4]) as session:
            session_store.expect_system_clients(tester=self)
            # If gently is True it won't kill node with live session on it
            target_node.stop(gently=False, wait_other_notice=True)
            # Inform expect_system_clients that session should gone
            session_store.forget_session(session)
            session_store.expect_system_clients(tester=self)

        # Decomission while session is alive test
        with self.node_session(4, session_store=session_store, **self._test_users[3]) as session:
            session_store.expect_system_clients(tester=self)
            self.cluster.nodelist()[4].decomission()
            # Inform expect_system_clients that session should gone
            session_store.forget_session(session)
            session_store.expect_system_clients(tester=self)

        session_store.expect_system_clients(tester=self)
        self.cluster.nodelist()[4].stop(wait_other_notice=True)
        session_store.expect_system_clients(tester=self)

        # Drain while session is alive test
        with self.node_session(3, session_store=session_store, **self._test_users[2]):
            session_store.expect_system_clients(tester=self)
            self.cluster.nodelist()[3].node.nodetool("drain")
            # Inform expect_system_clients that session should gone
            session_store.forget_session(session)
            session_store.expect_system_clients(tester=self)
        self.cluster.nodelist()[3].stop(wait_other_notice=True)
        session_store.expect_system_clients(tester=self)

    def test_system_clients_ssl_authentication(self):
        self.prepare(nodes=1, ssl_optional=False, require_ssl_auth=True)
        session_store = SessionStore()

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.load_cert_chain(certfile=os.path.join(self.test_path, "ccm_node.pem"), keyfile=os.path.join(self.test_path, "ccm_node.key"))
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.load_verify_locations(cafile=os.path.join(self.test_path, "ccm_node.cer"))

        # Successful SSL authentication test
        with self.node_session(
            0,
            port=9142,
            session_store=session_store,
            ssl_context=ssl_context,
            **self._test_users[0],
        ):
            session_store.expect_system_clients(tester=self)
        session_store.expect_system_clients(tester=self)

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.load_verify_locations(cafile=os.path.join(self.test_path, "ccm_node.cer"))

        # Failed SSL authentication test
        with self.patient_cql_connection(self.cluster.nodelist()[0], user="cassandra", password="cassandra") as session:
            original_sessions_count = self.get_total_records_in_system_clients(session, allow_anonymous=False)
            with pytest.raises(NoHostAvailable) as exp:
                self.node_session(0, **self._test_users[0], port=9142, session_store=session_store, ssl_context=ssl_context)
            assert "certificate required" in str(exp.value)
            self.wait_total_records_in_system_clients(session, original_sessions_count)

    def _system_client_content(self, fields_with_value, fields_with_none_value=None, ssl_optional=True, ssl_enabled=True):
        if not fields_with_none_value:
            fields_with_none_value = []
        self.prepare(
            nodes=1,
            ssl_optional=ssl_optional,
            require_ssl_auth=False,
            system_auth_rf=1,
            superuser=True,
            ssl_enabled=ssl_enabled,
        )

        if ssl_optional:
            port = 9142
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.load_verify_locations(cafile=os.path.join(self.test_path, "ccm_node.cer"))
        else:
            port = 9042
            ssl_context = None

        empty_value_fields_map = {}
        not_none_fields_map = {}
        session_store = SessionStore()
        with self.node_session(0, **self._test_users[0], row_factory=dict_factory, port=port, session_store=session_store, ssl_context=ssl_context) as session_container:
            session = session_container._session
            current_rows = self.get_allowed_client_records(session, allow_anonymous=False)
            logger.debug(f"system.clients: {current_rows}")
            row = current_rows[0]
            for field in fields_with_none_value:
                field_value = row.get(field, None)
                if field_value is not None:
                    not_none_fields_map[field] = field_value
            for field in fields_with_value:
                field_value = row.get(field, None)
                if field_value is None:
                    empty_value_fields_map[field] = field_value

        assert not empty_value_fields_map, f"expect fields Value with content, got {empty_value_fields_map}"
        assert not not_none_fields_map, f"expect fields value without content, got {not_none_fields_map}"

    @pytest.mark.single_node
    def test_system_client_not_none(self):
        fields = ["address", "port", "client_type", "connection_stage", "protocol_version", "shard_id", "username", "driver_name", "driver_version"]
        self._system_client_content(fields)

    @pytest.mark.require("#9216")
    @pytest.mark.single_node
    def test_system_client_hostname(self):
        fields = ["hostname"]
        self._system_client_content(fields)

    @pytest.mark.require("#9216")
    @pytest.mark.single_node
    def test_system_client_ssl(self):
        fields = ["ssl_cipher_suite", "ssl_enabled", "ssl_protocol"]
        self._system_client_content(fields)

    @pytest.mark.single_node
    def test_system_client_not_none_non_ssl(self):
        fields = ["address", "port", "client_type", "connection_stage", "protocol_version", "shard_id", "username", "driver_name", "driver_version"]
        self._system_client_content(fields, ssl_optional=False, ssl_enabled=False)

    @pytest.mark.require("#9216")
    @pytest.mark.single_node
    def test_system_client_hostname_non_ssl(self):
        fields = ["hostname"]
        self._system_client_content(fields, ssl_optional=False, ssl_enabled=False)
