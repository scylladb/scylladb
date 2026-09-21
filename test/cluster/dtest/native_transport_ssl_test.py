import logging
import os
import shutil
import ssl
import time

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, NoHostAvailable
from ccmlib import common

from dtest_class import Tester, create_cf, create_ks, get_ip_from_node, wait_for
from tools.data import putget
from tools.files import safe_mkdtemp
from tools.marks import issue_open, unmark
from tools.misc import generate_ssl_stores, is_port_used, revoke_certificate
from tools.sslkeygen import wait_for_cert_reload

logger = logging.getLogger(__name__)


class BaseSslTester(Tester):
    def _create_cluster_session(self, node_to_connect, port=9042, use_ssl=False, ca_certs_required=False):
        ssl_context, ssl_options = None, {}
        if use_ssl:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.load_cert_chain(certfile=os.path.join(self.test_path, "ccm_node.pem"), keyfile=os.path.join(self.test_path, "ccm_node.key"))
            ssl_context.check_hostname = ca_certs_required
            ssl_context.verify_mode = ssl.CERT_REQUIRED if ca_certs_required else ssl.CERT_NONE
            ssl_context.load_verify_locations(cafile=os.path.join(self.test_path, "ccm_node.cer"))
        cluster_connection = Cluster([get_ip_from_node(node_to_connect)], port=port, connect_timeout=90, control_connection_timeout=60, protocol_version=4, ssl_context=ssl_context, ssl_options=ssl_options)
        return cluster_connection.connect()

    def _populate_cluster(  # noqa: PLR0913
        self,
        enable_ssl=False,
        native_port=None,
        native_port_ssl=None,
        ssl_optional=False,
        require_auth=False,
        use_revocation=False,
        nodes_num=1,
    ):
        cluster = self.cluster

        if enable_ssl:
            ip_addresses = [f"{cluster.get_ipprefix()}{i}" for i in range(1, nodes_num + 1)]
            generate_ssl_stores(self.test_path, ip_addresses=ip_addresses)
            is_scylla = common.isScylla(cluster.get_install_dir())
            # C* versions before 3.0 (CASSANDRA-10559) do not know about
            # 'client_encryption_options.optional' - so we must not add that parameter
            # Note: does of course not work with scylla, we dont support "optional" (3.x feature)
            options = {"enabled": True}
            if ssl_optional:
                options["optional"] = ssl_optional
            if is_scylla:
                options.update({"certificate": os.path.join(self.test_path, "ccm_node.pem"), "keyfile": os.path.join(self.test_path, "ccm_node.key")})
                if require_auth:
                    options.update({"truststore": os.path.join(self.test_path, "ccm_node.cer"), "require_client_auth": True})
                if use_revocation:
                    options.update(
                        {
                            "certficate_revocation_list": os.path.join(self.test_path, "ccm_node.crl"),
                        }
                    )

            else:
                options.update(
                    {
                        "keystore": os.path.join(self.test_path, "keystore.jks"),
                        "keystore_password": "cassandra",
                    }
                )
                if require_auth:
                    options.update({"truststore": os.path.join(self.test_path, "truststore.jks"), "truststore_password": "cassandra", "require_client_auth": True})

            cluster.set_configuration_options({"client_encryption_options": options})

        if native_port is not None:
            cluster.set_configuration_options({"native_transport_port": native_port})

        if native_port_ssl is not None:
            cluster.set_configuration_options({"native_transport_port_ssl": native_port_ssl})

        cluster.populate(nodes_num)
        return cluster

    @staticmethod
    def _putget(cluster, session, ks="ks", cf="cf"):
        create_ks(session, ks, 1)
        create_cf(session, cf, compression=None)
        putget(cluster, session, cl=ConsistencyLevel.ONE)


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.next_gating
class TestNativeTransportSSL(BaseSslTester):
    """
    Native transport integration tests, specifically for ssl and port configurations.
    """

    @pytest.mark.dtest_debug
    def test_connect_to_ssl(self):
        """
        Connecting to SSL enabled native transport port should only be possible using SSL enabled client
        """
        cluster = self._populate_cluster(enable_ssl=True)
        node1 = cluster.nodelist()[0]

        cluster.start(jvm_args=["--logger-log-level", "cql_server=debug"])

        with pytest.raises(NoHostAvailable):
            # try to connect without ssl options
            logger.info("Should not be able to connect to SSL socket without SSL enabled client")
            self._create_cluster_session(node1, use_ssl=False)

        pattern = "(^io.netty.handler.ssl.NotSslRecordException.*|^.*An unexpected TLS packet was received.*|^.*The specified session has been invalidated for some reason.*)"
        assert len(node1.watch_log_for(pattern, timeout=10)) > 0, "Missing SSL handshake exception while connecting with non-SSL enabled client"

        # enabled ssl on the client and try again (this should work)
        with self._create_cluster_session(node1, use_ssl=True) as session:
            self._putget(cluster, session)

    def test_connect_to_ssl_client_auth(self):
        """
        Connecting to SSL enabled native transport port should only be possible using SSL enabled client.
        Also used certificate cannot be revoked to make connection.
        """

        cluster = self._populate_cluster(enable_ssl=True, require_auth=True, use_revocation=True)
        node1 = cluster.nodelist()[0]

        cluster.start(jvm_args=["--logger-log-level", "cql_server=debug"])

        with pytest.raises(NoHostAvailable):
            # try to connect without ssl options
            logger.info("Should not be able to connect to SSL socket without SSL enabled client")
            self._create_cluster_session(node1, use_ssl=False)

        pattern = "(^io.netty.handler.ssl.NotSslRecordException.*|^.*An unexpected TLS packet was received.*|^.*The specified session has been invalidated for some reason.*)"
        assert len(node1.watch_log_for(pattern, timeout=10)) > 0, "Missing SSL handshake exception while connecting with non-SSL enabled client"

        with pytest.raises(NoHostAvailable):
            # try to connect without auth cert
            logger.info("Should not be able to connect to SSL socket without SSL enabled client")
            self._create_cluster_session(node1, use_ssl=False, ca_certs_required=True)

        with self._create_cluster_session(node1, use_ssl=True, ca_certs_required=True) as session:
            self._putget(cluster, session)

        # verify connection fails after revoking certificate
        mark = node1.mark_log()
        revoke_certificate(self.test_path)
        wait_for_cert_reload(node1, "cql_server", ["ccm_node.crl"], from_mark=mark)

        try:  # hack around assertRaise's lack of msg parameter
            # try to connect with cert in revocation list
            self._create_cluster_session(node1, use_ssl=True, ca_certs_required=True)
            self.fail("Should not be able to connect to SSL socket with revoked certificate")
        except NoHostAvailable:
            pass

    def test_use_custom_port(self):
        """
        Connect to non-default native transport port
        """

        cluster = self._populate_cluster(native_port=9567)
        node1 = cluster.nodelist()[0]

        cluster.start()

        with pytest.raises(NoHostAvailable):
            logger.info("Should not be able to connect to non-default port")
            self._create_cluster_session(node1, use_ssl=False)

        with self._create_cluster_session(node1, port=9567, use_ssl=False) as session:
            self._putget(cluster, session)

    def test_use_custom_ssl_port(self):
        """
        Connect to additional ssl enabled native transport port
        @jira_ticket CASSANDRA-9590
        """

        cluster = self._populate_cluster(enable_ssl=True, native_port_ssl=9666)
        node1 = cluster.nodelist()[0]
        cluster.start()

        # we should be able to connect to default non-ssl port
        with self._create_cluster_session(node1, use_ssl=False) as session:
            self._putget(cluster, session)

        # connect to additional dedicated ssl port
        with self._create_cluster_session(node1, use_ssl=True, port=9666) as session:
            self._putget(cluster, session, ks="ks2")

    @pytest.mark.dtest_debug
    def test_reload_certificates(self, tmp_path):
        """
        Verify certificate reloading on modified file(s)
        """
        cluster = self._populate_cluster(enable_ssl=True)
        node1 = cluster.nodelist()[0]

        cluster.start(jvm_args=["--logger-log-level", "cql_server=debug"])

        # create new certs
        generate_ssl_stores(str(tmp_path), ip_addresses=[n.address() for n in cluster.nodelist()])

        with pytest.raises(NoHostAvailable):
            # try to connect without new, mismatched cert truststore (and required verification). Should fail
            logger.info("Should not be able to connect to SSL socket with mismatched trust store")
            self.patient_cql_connection(node1, ssl_opts={"ca_certs": str(tmp_path / "ccm_node.cer"), "cert_reqs": ssl.CERT_REQUIRED})

        mark = node1.mark_log()

        # copy new certs to old path
        shutil.copytree(str(tmp_path), self.test_path, dirs_exist_ok=True)

        # now we play the waiting game...
        wait_for_cert_reload(node1, "cql_server", ["ccm_node.pem", "ccm_node.key"], from_mark=mark)

        # now we should match
        with self._create_cluster_session(node1, use_ssl=True) as session:
            self._putget(cluster, session)

    def test_disable_regular_port_while_encryption_enabled(self):
        """
        This test activates a cluster with encryption turned on, but instead of using the usual native_transport_port
        (9042) the test configures native_transport_port_ssl instead, and disables native_transport_port by configuring
        it to 0. The test makes sure that the cluster responds to session that came through native_transport_port_ssl
        and not native_transport_port.
        """
        cluster = self._populate_cluster(enable_ssl=True, native_port_ssl=9142, native_port=0)
        cluster.start()
        node1 = cluster.nodelist()[0]
        with self._create_cluster_session(node1, use_ssl=True, port=9142) as session:
            create_ks(session, "ks", 1)
        is_port_listening = common.check_socket_listening(cluster.get_binary_interface(1), timeout=20)
        assert not is_port_listening, "Even after disabling the default cql port, the cluster continues to listen to it"

    def _listen_ports_conf_template(self, disable_value=None):
        """
        Test native transport ports configuration, and verify the listening native transport ports after start.
        try to disable the option by setting the option to None, ccm will remove the options from scylla.yaml
        """
        native_port = 9042
        native_port_ssl = 9142
        native_shard_aware_port = 19042
        native_shard_aware_port_ssl = 19142

        # Native_transport_port can only be disabled by `0'
        # Other 3 options can be disabled by removing the option from scylla.yaml, or set it to ~ ,
        # or null in scylla.yaml, ccm only supports to set the option to None, it will remove the
        # option from scylla.yaml
        disable_values = {"native_transport_port": 0, "native_transport_port_ssl": disable_value, "native_shard_aware_transport_port": disable_value, "native_shard_aware_transport_port_ssl": disable_value}

        default_ports_conf = {
            "native_transport_port": native_port,
            "native_transport_port_ssl": native_port_ssl,
            "native_shard_aware_transport_port": native_shard_aware_port,
            "native_shard_aware_transport_port_ssl": native_shard_aware_port_ssl,
        }

        def restart_and_verify_listen_ports(expected_ports=None):
            """
            Start the node and verify the expected ports are listened, the node will be stop in the end
            """
            if expected_ports is None:
                expected_ports = [native_port, native_shard_aware_port_ssl]
            logger.debug(f"Expected listen ports: {expected_ports}")
            node1 = cluster.nodelist()[0]
            mark = node1.mark_log()
            node1.start(wait_for_binary_proto=True)

            pattern = "|".join([str(port) for port in expected_ports])
            res = node1.grep_log(f"Starting listening for CQL clients on.*:({pattern})", from_mark=mark)
            logger.debug(res)
            assert len(res) == len(expected_ports), f"The listened ports are not same as expected! Expected ports: {expected_ports}\nReal listened ports: {res}"
            for port in expected_ports:
                # Retry to check if the port can be used in 5 seconds
                wait_for(is_port_used, text=f"Waiting port {port} is used", step=0.5, timeout=2, throw_exc=True, port=port, service_name="Native Transport")

            # Wait a while and check if Aborting/Segfault occurred
            time.sleep(2)
            res = node1.grep_log(f"Aborting on shard |Segmentation fault on shard ", from_mark=mark)
            assert not len(res), str(res)
            node1.stop(gently=False)

        logger.debug("Only enabled explicitly native SSL port in init cluster")
        cluster = self._populate_cluster(enable_ssl=True, native_port_ssl=native_port_ssl, native_port=native_port, nodes_num=3)
        restart_and_verify_listen_ports(expected_ports=[native_port, native_port_ssl, native_shard_aware_port])

        logger.debug(sorted(default_ports_conf.keys()))
        for num in range(2 ** len(default_ports_conf)):
            # Try to cover all cases
            ports_conf = default_ports_conf.copy()
            for idx, key in enumerate(sorted(default_ports_conf.keys())):
                if num & (2**idx):  # check if the bit is set
                    ports_conf[key] = disable_values[key]
            logger.debug(f"Test case {num} ({('%4s' % bin(num)[2:]).replace(' ', '0')}):\n {sorted(ports_conf.items(), key=lambda d: d[0])}")
            # cases (9, 10, 11) will fail if disable value is 0
            # cases (13, 15) will fail for if disable_value is None
            cluster.set_configuration_options(ports_conf)
            restart_and_verify_listen_ports(expected_ports=[v for k, v in ports_conf.items() if v not in [0, None]])

    @pytest.mark.skip_if(issue_open("scylladb/scylladb#7500") | issue_open("scylladb/scylladb#7783"))
    @unmark.next_gating
    def test_listen_ports_conf_by_zero(self):
        """
        Test native transport ports configuration, and verify the listening native transport ports after start.
        Disable 3 options by setting it to `0'
        """
        self._listen_ports_conf_template(disable_value=0)

    @pytest.mark.skip_if(issue_open("scylladb/scylladb#7500") | issue_open("scylladb/scylladb#7783"))
    @unmark.next_gating
    def test_listen_ports_conf(self):
        self._listen_ports_conf_template(disable_value=None)


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestServerEncryption(BaseSslTester):
    def test_server_encryption_and_restart_node(self):
        """
        reproducer for https://github.com/scylladb/scylladb/issues/14299

        restart a node configured with server encryption
        """
        ip_addresses = [f"{self.cluster.get_ipprefix()}{i}" for i in range(1, 3)]
        generate_ssl_stores(self.test_path, ip_addresses=ip_addresses)

        options = dict(internode_encryption="all")

        options.update({"certificate": os.path.join(self.test_path, "ccm_node.pem"), "keyfile": os.path.join(self.test_path, "ccm_node.key")})
        options.update({"truststore": os.path.join(self.test_path, "ccm_node.cer"), "require_client_auth": False})

        self.cluster.set_configuration_options({"server_encryption_options": options})

        cluster = self._populate_cluster(nodes_num=2)
        node1, *_ = cluster.nodelist()

        cluster.start()

        with self._create_cluster_session(node1) as session:
            self._putget(cluster, session)

            node1.stop()
            node1.start()

            node1.stop()
            node1.start()

            putget(cluster, session)
