import logging
import os
import time

import pytest
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import Tester, create_cf, create_ks, wait_for
from tools.cluster_topology import generate_cluster_topology
from tools.data import putget
from tools.misc import generate_ssl_stores, is_port_used
from tools.sslkeygen import wait_for_cert_reload

logger = logging.getLogger(__name__)


@pytest.mark.next_gating
@pytest.mark.dtest_debug
@pytest.mark.dtest_full
class TestInternodeSSL(Tester):
    def test_putget_with_internode_ssl(self):
        """
        Simple putget test with internode ssl enabled
        with default 'all' internode compression
        @jira_ticket CASSANDRA-9884
        """
        self.__putget_with_internode_ssl_test("all", internode_encryption="all")

    def test_putget_with_internode_rack_ssl(self):
        """
        Simple putget test with internode ssl enabled
        with default 'all' internode compression and 'rack' internode encryption.
        """
        self.__putget_with_internode_ssl_test("all", internode_encryption="rack")

    def test_putget_with_internode_dc_ssl(self):
        """
        Simple putget test with internode ssl enabled
        with default 'all' internode compression and 'dc' internode encryption.
        """
        self.__putget_with_internode_ssl_test("all", internode_encryption="rack", dcs=2)

    def test_putget_with_internode_ssl_without_compression(self):
        """
        Simple putget test with internode ssl enabled
        without internode compression
        @jira_ticket CASSANDRA-9884
        """
        self.__putget_with_internode_ssl_test("none", internode_encryption="none")

    def test_putget_with_internode_ssl_with_dc_compression(self):
        """
        Simple putget test with internode ssl enabled
        with 'dc' internode compression and 'dc' internode encryption.
        """
        self.__putget_with_internode_ssl_test("dc", internode_encryption="dc", dcs=2)

    def test_putget_with_internode_rack_ssl_with_dc_compression(self):
        """
        Simple putget test with internode ssl enabled
        with 'dc' internode compression and 'rack' internode encryption.
        """
        self.__putget_with_internode_ssl_test("dc", internode_encryption="rack", dcs=2)

    def test_putget_with_reloaded_certificates(self):
        self.__putget_with_internode_ssl_test("all", internode_encryption="all", reload_certs=True)

    def __putget_with_internode_ssl_test(self, internode_compression, internode_encryption="all", dcs=1, reload_certs=False):
        cluster = self.cluster

        self.ignore_log_patterns += [
            "connection dropped: The TLS connection was non-properly terminated",
            "connection dropped: The certificate is NOT trusted",
            "connection dropped: sendmsg: Broken pipe",
            "connection dropped: The specified session has been invalidated for some reason",
            "storage_service -.*fail to update tokens for",
            "storage_service -.*fail to update schema_version for",
        ]

        logger.debug("***using internode ssl***")
        generate_ssl_stores(self.test_path)
        cluster.set_configuration_options({"internode_compression": internode_compression})
        # Use the faster RBNO bootstrap to avodid problems with timeouts
        cluster.set_configuration_options({"enable_repair_based_node_ops": True, "allowed_repair_based_node_ops": "replace,removenode,rebuild,bootstrap,decommission"})
        cluster.enable_internode_ssl(self.test_path, internode_encryption=internode_encryption)

        assert dcs >= 1

        rack_num = 2
        nodes = generate_cluster_topology(dc_num=dcs, rack_num=rack_num, nodes_per_rack=2, dc_name_prefix="dc", rack_name_prefix="rc")
        cluster.set_configuration_options(values={"endpoint_snitch": "org.apache.cassandra.locator.GossipingPropertyFileSnitch"})
        cluster.populate(nodes).start(no_wait=False, wait_for_binary_proto=True, wait_other_notice=True)

        if reload_certs:
            logger.debug("rewriting certs")

            node_marks = {node: node.mark_log() for node in cluster.nodelist()}

            os.remove(os.path.join(self.test_path, "keystore.jks"))
            os.remove(os.path.join(self.test_path, "truststore.jks"))
            mtime = os.path.getmtime(os.path.join(self.test_path, "ccm_node.key"))
            # overwrite old certs
            generate_ssl_stores(self.test_path)

            mtime2 = os.path.getmtime(os.path.join(self.test_path, "ccm_node.key"))
            assert mtime2 > mtime, "Cert regen failed?"

            cluster.enable_internode_ssl(self.test_path, internode_encryption=internode_encryption)

            for node, mark in node_marks.items():
                logger.debug(f"waiting for {node.get_path()} to reload certs")
                wait_for_cert_reload(node, "messaging_service", ["internode-ccm_node.pem", "internode-ccm_node.key"], from_mark=mark)
                logger.debug("done")

        session = self.patient_cql_connection(cluster.nodelist()[0])
        create_ks(session, "ks", rf=rack_num)
        create_cf(session, "cf", compression=None)
        putget(cluster, session)

    def restart_and_verify_listen_ports(self, expected_ports):
        """
        Start the node and verify the expected ports are listened,  the node will be stop in the end
        """
        logger.debug(f"Expected listen ports: {expected_ports}")
        node1 = self.cluster.nodelist()[0]
        mark = node1.mark_log()
        node1.start(wait_for_binary_proto=True)
        pattern = "|".join(str(port) for port in expected_ports)
        res = node1.grep_log(f"Starting.*Messaging Service on.*port ({pattern})", from_mark=mark)
        logger.debug(res)
        assert len(res) == len(expected_ports), f"The listened ports are not same as expected! Expected ports: {expected_ports}\nReal listened ports: {res}"

        for port in expected_ports:
            # Retry to check if the port can be used in 5 seconds
            wait_for(is_port_used, text=f"Waiting port {port} is used", step=0.5, timeout=2, throw_exc=True, port=port, service_name="Storage Service")

        # Wait a while and check if Aborting/Segfault occurred
        time.sleep(2)
        res = node1.grep_log(f"Aborting on shard |Segmentation fault on shard ", from_mark=mark)
        assert not len(res), str(res)
        node1.stop(gently=False)

    @pytest.mark.single_node
    @pytest.mark.parametrize("internode_encryption", ["none", "all", "dc", "rack"])
    def test_listen_ports_conf(self, internode_encryption):
        DEFAULT_PORT = 7000
        DEFAULT_SSL_PORT = 7001

        def to_ports(port, default_values=None):
            if port is None:
                if default_values is None:
                    return []
                else:
                    return default_values
            if port == 0:
                return []
            return [port]

        def to_expected_ports(storage_port, ssl_storage_port):
            if not isinstance(cluster, ScyllaCluster):
                return to_ports(storage_port) + to_ports(ssl_storage_port)

            if internode_encryption == "none":
                return to_ports(storage_port, [DEFAULT_PORT])
            elif internode_encryption == "all":
                return to_ports(ssl_storage_port, [DEFAULT_SSL_PORT])
            else:
                return to_ports(storage_port, [DEFAULT_PORT]) + to_ports(ssl_storage_port, [DEFAULT_SSL_PORT])

        generate_ssl_stores(self.test_path)
        cluster = self.cluster
        cluster.populate(1)
        cluster.enable_internode_ssl(self.test_path, internode_encryption)
        # Test default configuration, ccm only sets storage_port: 7000
        self.restart_and_verify_listen_ports(expected_ports=[DEFAULT_PORT])

        # None removes the specified option from the configuration file
        # change the non-zero port numbers so they are different from the
        # default settings for the sanity sake.
        for storage_port in [0, 7003, None]:
            for ssl_storage_port in [0, 7004, None]:
                self.cluster.set_configuration_options({"storage_port": storage_port, "ssl_storage_port": ssl_storage_port})
                expected_ports = to_expected_ports(storage_port, ssl_storage_port)
                self.restart_and_verify_listen_ports(expected_ports=expected_ports)
