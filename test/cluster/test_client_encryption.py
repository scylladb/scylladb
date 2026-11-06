#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Tests for TLS on the CQL ports (client_encryption_options) and for which CQL
ports a node listens on, ported from scylla-dtest native_transport_ssl_test.py
(two of its dtests are ported in test_tls_putget.py, whose helpers these
tests use).

In most of these tests the default CQL port 9042 is encrypted or closed, so
the test framework's own (plain) driver cannot connect to the node: the nodes
are added with connect_driver=False, the tests wait until the node listens
for CQL clients, as ccm does, and connect by themselves.
"""

import asyncio
import datetime
import ipaddress
import logging
import re
import shutil
import socket
import ssl

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import NoHostAvailable
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from test.cluster.test_tls_putget import CONFIG, certificates, connect, create_ks, create_ks_cf, putget
from test.cluster.util import stop_servers_gracefully
from test.pylib.certificates import CertificateAuthority
from test.pylib.driver_utils import safe_driver_shutdown
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

CQL_SERVER_DEBUG = ["--logger-log-level", "cql_server=debug"]
NON_SSL_CLIENT_LOG = ("(^io.netty.handler.ssl.NotSslRecordException.*|^.*An unexpected TLS packet was received.*|"
                      "^.*The specified session has been invalidated for some reason.*)")


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test."""
    yield
    await stop_servers_gracefully(manager)


def client_encryption(certs: dict) -> dict:
    """The dtests' client_encryption_options: the node's certificate and key."""
    return {"enabled": True, "certificate": certs["certificate"], "keyfile": certs["keyfile"]}


async def wait_for_binary_proto(manager: ScyllaClusterManager, server, port: int = 9042):
    """Wait until the node listens for CQL clients on the port, like ccm's
    start(wait_for_binary_proto=True)."""
    log = await manager.server_open_log(server.server_id)
    await log.wait_for(rf"Starting listening for CQL clients on {re.escape(server.ip_addr)}:{port} ", timeout=300)
    return log


async def session_to(*args, **kwargs):
    return await asyncio.to_thread(connect, *args, **kwargs)


async def assert_cannot_connect(*args, **kwargs):
    with pytest.raises(NoHostAvailable):
        cluster, _ = await session_to(*args, **kwargs)
        safe_driver_shutdown(cluster)


async def dtest_putget(manager: ScyllaClusterManager, session, ks: str = "ks") -> None:
    """The dtests' BaseSslTester._putget()."""
    await create_ks_cf(session, ks, 1)
    await putget(manager, session, ks, cl=ConsistencyLevel.ONE)


def certificates_for_ip(tmp_path, ip: str) -> tuple[dict, CertificateAuthority, x509.Certificate]:
    """Like certificates(), but the node's certificate names the node's IP
    address, as the dtests' certificates do (generate_ssl_stores(ip_addresses=...)),
    so that a client checking the server's host name accepts it. Also writes
    an empty certificate revocation list."""
    ca = CertificateAuthority()
    key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "scylla")]))
            .issuer_name(ca.name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=1))
            .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address(ip))]), critical=False)
            .sign(ca.key, hashes.SHA256()))
    (tmp_path / "ccm_node.key").write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                                              serialization.NoEncryption()))
    (tmp_path / "ccm_node.pem").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    certs = {"certificate": str(tmp_path / "ccm_node.pem"), "keyfile": str(tmp_path / "ccm_node.key"),
             "truststore": ca.write_cert(tmp_path / "ccm_node.cer"),
             "crl": ca.write_crl(tmp_path / "ccm_node.crl", revoked=[])}
    return certs, ca, cert


def is_listening(host: str, port: int) -> bool:
    try:
        socket.create_connection((host, port), timeout=1).close()
        return True
    except OSError:
        return False


# Ported from scylla-dtest native_transport_ssl_test.py::TestNativeTransportSSL::test_connect_to_ssl.
# With client encryption enabled and no separate SSL port, the regular CQL
# port is encrypted: a plain client is refused (and the node logs the
# unexpected non-TLS packet), a TLS client can write and read.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_connect_to_ssl(manager: ScyllaClusterManager, tmp_path):
    certs = certificates(tmp_path)
    server = await manager.server_add(connect_driver=False, cmdline=CQL_SERVER_DEBUG,
                                      config={**CONFIG, "client_encryption_options": client_encryption(certs)})
    log = await wait_for_binary_proto(manager, server)

    # Should not be able to connect to SSL socket without SSL enabled client
    await assert_cannot_connect(server.ip_addr, 9042, use_ssl=False)
    await log.wait_for(NON_SSL_CLIENT_LOG, timeout=10)

    # enabled ssl on the client and try again (this should work)
    cluster, session = await session_to(server.ip_addr, 9042, use_ssl=True, certs=certs)
    try:
        await dtest_putget(manager, session)
    finally:
        safe_driver_shutdown(cluster)


# Ported from scylla-dtest native_transport_ssl_test.py::TestNativeTransportSSL::test_connect_to_ssl_client_auth.
# With require_client_auth and a certificate revocation list, only TLS
# clients presenting a trusted certificate can connect and write and read;
# after the certificate is revoked in the list (which the node reloads),
# the client is refused.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_connect_to_ssl_client_auth(manager: ScyllaClusterManager, tmp_path):
    names = {"certificate": str(tmp_path / "ccm_node.pem"), "keyfile": str(tmp_path / "ccm_node.key")}
    server = await manager.server_add(start=False, cmdline=CQL_SERVER_DEBUG, config={**CONFIG, "client_encryption_options": {
        **client_encryption(names),
        "truststore": str(tmp_path / "ccm_node.cer"),
        "require_client_auth": True,
        # Sic, the option's name is misspelled.
        "certficate_revocation_list": str(tmp_path / "ccm_node.crl"),
    }})
    certs, ca, cert = certificates_for_ip(tmp_path, server.ip_addr)
    await manager.server_start(server.server_id, connect_driver=False)
    log = await wait_for_binary_proto(manager, server)

    # Should not be able to connect to SSL socket without SSL enabled client
    await assert_cannot_connect(server.ip_addr, 9042, use_ssl=False)
    await log.wait_for(NON_SSL_CLIENT_LOG, timeout=10)

    # try to connect without auth cert
    await assert_cannot_connect(server.ip_addr, 9042, use_ssl=False, ca_certs_required=True)

    cluster, session = await session_to(server.ip_addr, 9042, use_ssl=True, certs=certs, ca_certs_required=True)
    try:
        await dtest_putget(manager, session)
    finally:
        safe_driver_shutdown(cluster)

    # verify connection fails after revoking certificate
    mark = await log.mark()
    ca.write_crl(certs["crl"], revoked=[cert])
    await log.wait_for(r"^.*cql_server.*Reloaded.*ccm_node\.crl.*", from_mark=mark)

    # try to connect with cert in revocation list
    await assert_cannot_connect(server.ip_addr, 9042, use_ssl=True, certs=certs, ca_certs_required=True)


# Ported from scylla-dtest native_transport_ssl_test.py::TestNativeTransportSSL::test_use_custom_port.
# With a non-default native_transport_port, a client cannot connect on the
# default port 9042, and can write and read over the configured one.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_use_custom_port(manager: ScyllaClusterManager):
    server = await manager.server_add(connect_driver=False, config={**CONFIG, "native_transport_port": 9567})
    await wait_for_binary_proto(manager, server, 9567)

    # Should not be able to connect to non-default port
    await assert_cannot_connect(server.ip_addr, 9042, use_ssl=False)

    cluster, session = await session_to(server.ip_addr, 9567, use_ssl=False)
    try:
        await dtest_putget(manager, session)
    finally:
        safe_driver_shutdown(cluster)


# Ported from scylla-dtest native_transport_ssl_test.py::TestNativeTransportSSL::test_reload_certificates.
# The node reloads its CQL certificate and key when the files are replaced:
# a client trusting only the new certificates is refused before, and a TLS
# client can write and read after.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_reload_certificates(manager: ScyllaClusterManager, tmp_path):
    node_dir = tmp_path / "node"
    new_dir = tmp_path / "new"
    node_dir.mkdir()
    new_dir.mkdir()
    certs = certificates(node_dir)
    server = await manager.server_add(connect_driver=False, cmdline=CQL_SERVER_DEBUG,
                                      config={**CONFIG, "client_encryption_options": client_encryption(certs)})
    log = await wait_for_binary_proto(manager, server)

    # create new certs
    new_certs = certificates(new_dir)

    # try to connect without new, mismatched cert truststore (and required verification). Should fail
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_REQUIRED
    ssl_context.load_verify_locations(cafile=new_certs["truststore"])
    await assert_cannot_connect(server.ip_addr, 9042, use_ssl=False, ssl_context=ssl_context)

    mark = await log.mark()

    # copy new certs to old path
    shutil.copytree(new_dir, node_dir, dirs_exist_ok=True)

    # now we play the waiting game...
    for f in ["ccm_node.pem", "ccm_node.key"]:
        await log.wait_for(rf"^.*cql_server.*Reloaded.*{re.escape(f)}.*", from_mark=mark)

    # now we should match
    cluster, session = await session_to(server.ip_addr, 9042, use_ssl=True, certs=certs)
    try:
        await dtest_putget(manager, session)
    finally:
        safe_driver_shutdown(cluster)


# Ported from scylla-dtest native_transport_ssl_test.py::TestNativeTransportSSL::test_disable_regular_port_while_encryption_enabled.
# With client encryption, native_transport_port=0 and a separate
# native_transport_port_ssl, a TLS client can create a keyspace over the
# SSL port, and the node does not listen on 9042.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_disable_regular_port_while_encryption_enabled(manager: ScyllaClusterManager, tmp_path):
    certs = certificates(tmp_path)
    server = await manager.server_add(connect_driver=False, config={
        **CONFIG,
        "client_encryption_options": client_encryption(certs),
        "native_transport_port": 0,
        "native_transport_port_ssl": 9142,
    })
    await wait_for_binary_proto(manager, server, 9142)
    cluster, session = await session_to(server.ip_addr, 9142, use_ssl=True, certs=certs)
    try:
        await create_ks(session, "ks", 1)
    finally:
        safe_driver_shutdown(cluster)
    assert not is_listening(server.ip_addr, 9042), \
        "Even after disabling the default cql port, the cluster continues to listen to it"
