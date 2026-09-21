import logging
import os
import shutil
import socket
import ssl
import tempfile
import threading
import uuid
from enum import Enum
from functools import cached_property
from pathlib import Path

import boto3
import pytest
from cassandra import ConsistencyLevel
from docker.errors import DockerException
from kmip.services import auth
from kmip.services.server.server import KmipServer

from dtest_class import Tester, create_cf, wait_for
from tools.cluster_topology import generate_cluster_topology
from tools.data import insert_c1c2, query_c1c2
from tools.docker_utils import container_reload, container_remove, get_docker_client, get_ip_address_of_container, running_in_docker
from tools.marks import unmark
from tools.misc import generate_ssl_stores
from tools.retrying import retrying

logger = logging.getLogger(__name__)


# remove "default", as this is same as None, and gives us a replicated provider


class KeyProviderEnum(Enum):
    local = "LocalFileSystemKeyProviderFactory"
    replicated = "ReplicatedKeyProviderFactory"
    kmip = "KmipKeyProviderFactory"
    kms = "KmsKeyProviderFactory"
    kms_real = "KmsRealKeyProviderFactory"


# default: 'AES/CBC/PKCS5Padding', length 128
supported_cipher_algorithms = {
    "": [],
    "AES/CBC/PKCS5Padding": [128, 192, 256],  # 192 has problem
    "AES/CBC": [128, 192, 256],  # 192 has problem
    "AES": [128, 192, 256],  # 192 has problem
    "AES/ECB/PKCS5Padding": [128, 192, 256],
    "AES/ECB": [128, 192, 256],
    # legacy algorithms, not supported in openssl 3.x
    # "DES/CBC/PKCS5Padding": [56],
    # "DES/CBC": [56],
    # "DES": [56],
    # 'DESede/CBC/PKCS5Padding': [112, 168],    # not support by Scylla, supported by DSE
    # 'Blowfish/CBC/PKCS5Padding': [32, 448],   # not support by Scylla, supported by DSE
    # "RC2/CBC/PKCS5Padding": [80, 128],  # [40, 80, 128]  # 40 to 128
    # "RC2/CBC": [80, 128],  # [40, 80, 128]  # 40 to 128
    # "RC2": [80, 128],  # [40, 80, 128]  # 40 to 128
}


class BaseKeyProviderFactory:
    def __init__(self, key_provider, tester):
        self.key_provider = key_provider
        self.system_keyfile = None
        self.tester = tester
        self.cluster = tester.cluster

    def __enter__(self):
        self.prepare_conf()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass

    def supported_cipher(self, cipher_algorithm, secret_key_strength):
        return True

    def require_restart(self):
        return False

    def prepare_conf(self):
        pass

    def additional_cf_options(self, ks=None):
        if self.key_provider:
            return {"key_provider": self.key_provider.value}
        return {}

    def verify_secret_key(self, cipher_algorithm=None, secret_key_strength=None):
        pass


class DefaultKeyProviderFactory(BaseKeyProviderFactory):
    def __init__(self, tester):
        BaseKeyProviderFactory.__init__(self, None, tester)


class LocalFileSystemKeyProviderFactory(BaseKeyProviderFactory):
    def __init__(self, tester):
        self.secret_file = os.path.join(tester.test_path, "test/node1/conf/data_encryption_keys")
        BaseKeyProviderFactory.__init__(self, KeyProviderEnum.local, tester)

    def additional_cf_options(self, ks=None):
        return super().additional_cf_options() | {"secret_key_file": os.path.join(self.tester.test_path, "test/node1/conf/secret_key_file_" + ks) if ks else self.secret_file}

    def verify_secret_key(self, cipher_algorithm=None, secret_key_strength=None):
        logger.debug("Verify that local key is generated automatically")
        keyfile = os.path.join(self.tester.test_path, "test/node1/conf/data_encryption_keys")
        assert os.path.exists(keyfile), "Default local key is not generated"

        if cipher_algorithm is None:
            cipher_algorithm = "AES/CBC/PKCS5Padding"
        if secret_key_strength is None:
            secret_key_strength = 128
        found = False
        with open(keyfile) as f:
            for line in f.readlines():
                if line.startswith("%s:%d:" % (cipher_algorithm, secret_key_strength)):
                    logger.debug("Found system key: %s" % line)
                    found = True
        assert found, "Did not find specific local key in %s" % keyfile


class ReplicatedKeyProviderFactory(BaseKeyProviderFactory):
    def __init__(self, tester):
        BaseKeyProviderFactory.__init__(self, KeyProviderEnum.replicated, tester)

    def prepare_conf(self):
        # prepare_secret_key(self)
        pass

    def additional_cf_options(self, ks=None):
        return super().additional_cf_options(ks) | {"system_key": "system_key_" + ks if ks else "system_key"}


class KmipKeyProviderFactory(BaseKeyProviderFactory):
    class TLS13AuthenticationSuite(auth.TLS12AuthenticationSuite):
        """
        An authentication suite used to establish secure network connections.
        Supports TLS 1.3. More importantly, works with gnutls-<recent>
        """

        def __init__(self, cipher_suites=None):
            """
            Create a TLS12AuthenticationSuite object.
            Args:
                cipher_suites (list): A list of strings representing the names of
                    cipher suites to use. Overrides the default set of cipher
                    suites. Optional, defaults to None.
            """
            super().__init__(cipher_suites)
            self._protocol = ssl.PROTOCOL_TLS_SERVER

    @staticmethod
    def fake_wrap_ssl(sock, keyfile=None, certfile=None, server_side=False, cert_reqs=ssl.CERT_NONE, ssl_version=ssl.PROTOCOL_TLS, ca_certs=None, do_handshake_on_connect=True, suppress_ragged_eofs=True, ciphers=None):  # noqa: PLR0913
        ctxt = ssl.SSLContext(protocol=ssl_version)
        ctxt.load_cert_chain(certfile=certfile, keyfile=keyfile)
        ctxt.verify_mode = cert_reqs
        ctxt.load_verify_locations(cafile=ca_certs)
        ctxt.set_ciphers(ciphers)
        return ctxt.wrap_socket(sock, server_side=server_side, do_handshake_on_connect=do_handshake_on_connect, suppress_ragged_eofs=suppress_ragged_eofs)

    def __init__(self, tester):
        self.kmip_host = "kmip_test"
        self.kmip_port = 0
        self.kmip_server = None
        self.kmip_thread = None
        self.tempdir = None
        self.certs = None
        ssl.wrap_socket = self.fake_wrap_ssl
        BaseKeyProviderFactory.__init__(self, KeyProviderEnum.kmip, tester)

    def prepare_conf(self):
        # restart is request to make change effective
        options = {
            "hosts": "127.0.0.1:" + str(self.kmip_port),
            "certificate": self.certs["certfile"],
            "keyfile": self.certs["keyfile"],
            "truststore": self.certs["truststore"],
            "priority_string": "SECURE128:+RSA:-VERS-TLS1.0:-ECDHE-ECDSA",
        }
        self.cluster.set_configuration_options({"kmip_hosts": {self.kmip_host: options}})

    def kmip_serve(self):
        s = self.kmip_server
        assert s is not None

        s._socket.listen(5)
        s._logger.info("Starting connection service...")

        try:
            while s._is_serving:
                try:
                    connection, address = s._socket.accept()
                except TimeoutError:
                    # Setting the default socket timeout to break hung connections
                    # will cause accept to periodically raise socket.timeout. This
                    # is expected behavior, so ignore it and retry accept.
                    pass
                except OSError as e:
                    s._logger.warning("Error detected while establishing new connection.", exc_info=True)
                except KeyboardInterrupt:
                    s._is_serving = False
                    break
                except Exception as e:  # noqa: BLE001
                    s._logger.warning("Error detected while establishing new connection.", exc_info=True)
                else:
                    s._setup_connection_handler(connection, address)
        except KeyboardInterrupt:
            pass

        s._logger.info("Stopping connection service.")

    def __enter__(self):
        self.tempdir = tempfile.TemporaryDirectory(dir="tmp/")

        base_dir = self.tempdir.name
        generate_ssl_stores(base_dir)
        self.certs = {"certfile": os.path.join(base_dir, "ccm_node.pem"), "keyfile": os.path.join(base_dir, "ccm_node.key"), "truststore": os.path.join(base_dir, "trust.pem")}
        assert os.path.exists(self.certs["certfile"])
        assert os.path.exists(self.certs["keyfile"])
        assert os.path.exists(self.certs["truststore"])
        kmiplog = logging.getLogger("kmip.server")
        kmiplog.handlers.clear()  # make pykmip shut up a bit. log will written to log file (setup in init below)
        self.kmip_server = KmipServer(
            hostname="127.0.0.1",
            config_path=None,
            certificate_path=self.certs["certfile"],
            policy_path=self.tempdir.name,
            key_path=self.certs["keyfile"],
            ca_path=self.certs["truststore"],
            auth_suite="TLS1.2",
            database_path=os.path.join(self.tempdir.name, "pykmip.db"),
            log_path=os.path.join(self.tempdir.name, "pykmip.log"),
            enable_tls_client_auth=False,
        )
        assert len(kmiplog.handlers) == 1
        logger.info(kmiplog.handlers)
        self.kmip_server.auth_suite = self.TLS13AuthenticationSuite(self.kmip_server.auth_suite.ciphers)
        # force port to zero -> select dynamically
        self.kmip_server.config.settings["port"] = 0
        self.kmip_server.start()
        self.kmip_port = self.kmip_server._socket.getsockname()[1]
        self.kmip_thread = threading.Thread(name="kmip server", target=self.kmip_serve, daemon=True)
        self.kmip_thread.start()
        self.prepare_conf()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if self.kmip_server is not None:
            self.kmip_server._is_serving = False
            try:
                self.kmip_server._socket.shutdown(socket.SHUT_RDWR)
                self.kmip_server._socket.close()
            except:
                pass
            self.kmip_server = None

        if self.kmip_thread is not None:
            self.kmip_thread.join()
            self.kmip_thread = None

        if self.tempdir is not None:
            self.tempdir.cleanup()
            self.tempdir = None
        self.certs = None

    def additional_cf_options(self, ks=None):
        return super().additional_cf_options(ks) | {"kmip_host": self.kmip_host}

    def require_restart(self):
        return True

    def supported_cipher(self, cipher_algorithm, secret_key_strength):
        # Our KMIP server is not configured to support this configuration.
        # Test fails with error: Invalid key data length 80 for RC2/CBC and kmip.
        # Decided (Roy) don't test it
        return not ("RC2" in cipher_algorithm and secret_key_strength == 80)


class KMSKeyProviderFactory(BaseKeyProviderFactory):
    def __init__(self, tester):
        BaseKeyProviderFactory.__init__(self, KeyProviderEnum.kms, tester)

        self.seed_yaml = Path(__file__).parent / "test_data" / "local-kms" / "seed.yaml"
        self.container = None
        self.master_key = "alias/Scylla-test"
        self.kms_host = "kms_test"
        self.endpoint_url = None
        self.client = get_docker_client()

    @cached_property
    def kms_client(self):
        return boto3.client("kms", endpoint_url=self.endpoint_url, region_name="None")

    def create_new_key_replace_alias(self):
        # create master key
        response = self.kms_client.create_key(Description="dtest", Tags=[{"TagKey": "Name", "TagValue": "dtest"}])
        key_id = response["KeyMetadata"]["KeyId"]
        self.kms_client.delete_alias(AliasName=self.master_key)

        self.kms_client.create_alias(AliasName=self.master_key, TargetKeyId=key_id)

    def prepare_conf(self):
        local_kms_image = "nsmithuk/local-kms:3"
        name = f"local-kms-{str(uuid.uuid4())[:8]}"

        ports = None if running_in_docker() else {"8080/tcp": ("0.0.0.0", None)}

        @retrying(num_attempts=10, sleep_time=1, allowed_exceptions=DockerException)
        def start_local_kms():
            existing_images = self.client.images.list(filters={"reference": local_kms_image})
            if not existing_images:
                self.client.images.pull(local_kms_image)
            try:
                self.container = self.client.containers.run(local_kms_image, name=name, detach=True, ports=ports, volumes=[f"{self.seed_yaml}:/init/seed.yaml"])
            except DockerException as e:
                logger.error(f"Failed to start local kms container: {e}")
                self.client.containers.get(name).remove(force=True)
                raise

        start_local_kms()
        container_reload(self.container)
        if running_in_docker():
            self.endpoint_url = f"http://{get_ip_address_of_container(self.container)}:8080"
        else:
            ports = self.container.attrs["NetworkSettings"]["Ports"]
            port = ports["8080/tcp"][0]["HostPort"]
            self.endpoint_url = "http://localhost:" + port

        def check_connectivity():
            return len(self.kms_client.list_keys().get("Keys", [])) == 2

        wait_for(check_connectivity, timeout=30, text="Waiting until local-kms is ready")

        try:
            options = {"endpoint": self.endpoint_url, "master_key": self.master_key}
            self.cluster.set_configuration_options({"kms_hosts": {self.kms_host: options}})
        except:
            self.container.stop()
            raise

    def __enter__(self):
        self.prepare_conf()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        # Stop all Scylla nodes before killing the KMS container so that nodes
        # do not produce encryption::network_error log entries during teardown
        # when they attempt to reach a KMS endpoint that is no longer available.
        if self.cluster is not None:
            self.cluster.stop(gently=True, wait_other_notice=False)
        self.container.kill()
        for line in self.container.logs().decode().splitlines():
            logger.debug("local-kms output: %s", line)
        container_remove(self.container)

    def additional_cf_options(self, ks=None):
        container_reload(self.container)
        return super().additional_cf_options(ks) | {"kms_host": self.kms_host}

    def supported_cipher(self, cipher_algorithm, secret_key_strength):
        return secret_key_strength >= 128

    def require_restart(self):
        return True


class KMSRealKeyProviderFactory(BaseKeyProviderFactory):
    def __init__(self, tester):
        BaseKeyProviderFactory.__init__(self, KeyProviderEnum.kms, tester)
        self.master_key = "alias/kms_encryption_test"
        self.kms_host = "kms_test"

    def prepare_conf(self):
        options = {"master_key": self.master_key, "aws_region": "us-east-1"}
        self.cluster.set_configuration_options({"kms_hosts": {self.kms_host: options}})

    def __enter__(self):
        self.prepare_conf()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass

    def additional_cf_options(self, ks=None):
        return super().additional_cf_options(ks) | {"kms_host": self.kms_host}

    def supported_cipher(self, cipher_algorithm, secret_key_strength):
        return secret_key_strength >= 128

    def require_restart(self):
        return True


class EncryptionAtRestBase(Tester):
    multiple_num = 3
    default_node_num = 2
    system_key_dir = "./resources/system_keys/"

    def get_session(self, node_idx=0, user=None, password=None):
        node = self.cluster.nodelist()[node_idx]
        conn = self.patient_cql_connection(node, user=user, password=password)
        return conn

    def create_ks(self, kss=None, n=None):
        if kss is None:
            kss = ["ks"]
        n = n if n else self.default_node_num
        session = self.get_session()
        for ks in kss:
            session.execute(f"CREATE KEYSPACE IF NOT EXISTS {ks} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : {n} }}")

    def prepare(self, n=None, kss=None, restart=False):
        if kss is None:
            kss = ["ks"]
        n = n if n else self.default_node_num
        self.cluster.set_configuration_options({"system_key_directory": EncryptionAtRestBase.system_key_dir})
        logger.debug("set system_key_directory to %s", EncryptionAtRestBase.system_key_dir)
        if not self.cluster.nodelist():
            self.cluster.populate(generate_cluster_topology(rack_num=n)).start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=["--logger-log-level", "kms=trace"])
        elif restart:
            self.rolling_restart()
        session = self.get_session()
        self.create_ks(kss=kss, n=n)
        return session

    def drop_keyspace(self, kss=None):
        if kss is None:
            kss = ["ks"]
        session = self.get_session()
        for ks in kss:
            session.execute("DROP KEYSPACE IF EXISTS %s" % ks)

    def drop_cf(self, name="ks.cf"):
        session = self.get_session()
        session.execute("DROP TABLE IF EXISTS %s" % name)

    def cleanup(self, kss=None):
        if kss is None:
            kss = ["ks"]
        self.drop_keyspace(kss=kss)

    def prepare_system_key(self, keyfile="system_key", cipher_algorithm="AES/CBC/PKCS5Padding", secret_key_strength=128):
        dest = os.path.join(EncryptionAtRestBase.system_key_dir, keyfile)
        # use saved key in dtest repo, generate it in future
        # the key can also be created by `dsetool createsystemkey $cipher_algorithm $strength`
        src = os.path.join(EncryptionAtRestBase.system_key_dir, "system_key")  # AES/ECB/PKCS5Padding:128
        if not os.path.exists(dest) or not os.path.samefile(src, dest):
            shutil.copy(src, dest)

    def create_encrypted_cf(  # noqa: PLR0913
        self,
        session,
        name="ks.cf",
        columns=None,
        cipher_algorithm=None,
        secret_key_strength=None,
        compression=None,
        additional_options=None,
    ):
        if additional_options is None:
            additional_options = {}
        if columns is None:
            columns = {"c1": "text", "c2": "text"}
        options = {}
        if additional_options:
            options.update(additional_options)
        if cipher_algorithm:
            options.update({"cipher_algorithm": cipher_algorithm})
        if secret_key_strength:
            options.update({"secret_key_strength": secret_key_strength})
        if "system_key_file" in options:
            self.prepare_system_key(keyfile=options["system_key_file"])
        logger.debug("Create encrypted cf: %s (%s)", name, options)
        create_cf(session, name, columns=columns, scylla_encryption_options=options, compression=compression)
        return options

    def read_verify_workload(self, session, ks="ks", cf="cf"):
        logger.debug("Verify data by read stress: %s.%s", ks, cf)
        for i in range(100):
            query_c1c2(session, i, ConsistencyLevel.QUORUM, ks=ks, cf=cf)

    def prepare_write_workload(self, session, ks="ks", cf="cf", flush=True):
        logger.debug("Insert data to encrypted table: %s.%s", ks, cf)
        insert_c1c2(session, keys=list(range(100)), consistency=ConsistencyLevel.ALL, ks=ks, cf=cf)
        if flush:
            logger.debug("flush cluster")
            self.cluster.flush()

    def rolling_restart(self, user=None, password=None, allow_start_failure=False):
        logger.debug(f"Restart nodes one by one ...{' (start failures allowed)' if allow_start_failure else ''}")
        errors = []
        for node in self.cluster.nodelist():
            node.stop(wait_other_notice=True)
            try:
                node.start(wait_other_notice=True, wait_for_binary_proto=True, jvm_args=["--logger-log-level", "kms=trace"])
            except RuntimeError as e:
                if allow_start_failure:
                    errors.append(e)
        if not errors:
            return self.get_session(user=user, password=password)

    def cluster_restart(self, user=None, password=None):
        logger.debug("Restart cluster ...")
        self.cluster.stop(wait_other_notice=True)
        self.cluster.start(wait_for_binary_proto=True, wait_other_notice=True, jvm_args=["--logger-log-level", "kms=trace"])
        return self.get_session(user=user, password=password)

    def get_key_provider(self, key_provider=None):
        if key_provider == KeyProviderEnum.local:
            ret = LocalFileSystemKeyProviderFactory(self)
        elif key_provider == KeyProviderEnum.replicated:
            ret = ReplicatedKeyProviderFactory(self)
        elif key_provider == KeyProviderEnum.kmip:
            ret = KmipKeyProviderFactory(self)
        elif key_provider == KeyProviderEnum.kms:
            ret = KMSKeyProviderFactory(self)
        elif key_provider == KeyProviderEnum.kms_real:
            ret = KMSRealKeyProviderFactory(self)
        elif key_provider is None:
            ret = DefaultKeyProviderFactory(self)
        else:
            raise Exception("Unknown key_provider: %s" % key_provider)
        return ret


def all_providers():
    return [pytest.param(p, marks=[unmark.next_gating] if p == KeyProviderEnum.kmip else []) for p in KeyProviderEnum]
