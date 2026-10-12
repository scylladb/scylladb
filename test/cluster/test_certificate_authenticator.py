#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import ssl
import time

import pytest
from cassandra import Unauthorized  # type: ignore
from cassandra.cluster import Cluster, NoHostAvailable  # type: ignore

from test.pylib.certificates import CertificateAuthority
from test.pylib.driver_utils import safe_driver_shutdown
from test.pylib.internal_types import ServerUpState
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

TLS_PORT = 9142


def gen_certs(path, clients: dict[str, dict]):
    """A CA, and for each client name a certificate signed by it, issued with
    the given CertificateAuthority.issue() arguments."""
    ca = CertificateAuthority()
    ca.write_cert(path / 'ca.crt')
    for name, subject in clients.items():
        ca.issue(subject.get('cn'), path / f'{name}.crt', path / f'{name}.key',
                 organization=subject.get('organization'), email=subject.get('email'))


@pytest.mark.tier3
@pytest.mark.skip_mode(mode="dev", reason="scylla-dtest runs it nightly in release and debug modes only")
async def test_certificate_auth_role_queries(manager: ScyllaClusterManager, tmp_path):
    """Ported from scylla-dtest auth_test.py::TestAuth::test_certificate_auth.

    CertificateAuthenticator maps a client certificate to a role with the
    first auth_certificate_role_queries rule that matches it - here an email
    in the subject alternative name, then the subject's CN - and refuses
    connections without a certificate or whose role does not exist, until
    the role is created. The superuser comes from auth_superuser_name.
    """
    gen_certs(tmp_path, {
        'admin': {'cn': 'admin'},
        'client1': {'cn': 'client1'},
        'client2': {'cn': 'client2'},
        'client3': {'organization': 'TestOrg', 'email': 'client3@scylladb.com'},
    })
    config = {
        'authenticator': 'com.scylladb.auth.CertificateAuthenticator',
        'authorizer': 'org.apache.cassandra.auth.CassandraAuthorizer',
        'auth_superuser_name': 'admin',
        'auth_certificate_role_queries': [
            {'source': 'ALTNAME', 'query': 'EMAIL=(\\w+)@scylladb.com'},
            {'source': 'SUBJECT', 'query': 'CN=([^,]+)'},
        ],
        # Keep the plain ports, on which the test framework's checks run.
        'native_transport_port': 9042,
        'native_shard_aware_transport_port': 19042,
        'native_transport_port_ssl': TLS_PORT,
        'native_shard_aware_transport_port_ssl': 19143,
        'client_encryption_options': {
            'enabled': True,
            'certificate': 'conf/scylla.crt',
            'keyfile': 'conf/scylla.key',
            'truststore': f'{tmp_path}/ca.crt',
            'require_client_auth': True,
        },
    }
    # Nobody can log in on the plain port, so don't wait for that.
    # TestAuth.prepare(nodes=1): populate(1) (SimpleSnitch, datacenter1/rack1),
    # vnodes, and no permission caching.
    config.update({"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256, "endpoint_snitch": "SimpleSnitch",
                   "permissions_validity_in_ms": 0, "permissions_update_interval_in_ms": 0})
    server = await manager.server_add(config=config, property_file={"dc": "datacenter1", "rack": "rack1"},
                                      expected_server_up_state=ServerUpState.HOST_ID_QUERIED, connect_driver=False)

    def connect(name=None):
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        if name:
            ssl_ctx.load_cert_chain(certfile=f'{tmp_path}/{name}.crt', keyfile=f'{tmp_path}/{name}.key')
        cluster = Cluster(contact_points=[server.ip_addr], port=TLS_PORT, ssl_context=ssl_ctx, connect_timeout=10)
        try:
            return cluster.connect(), cluster
        except Exception:
            safe_driver_shutdown(cluster)
            raise

    def refused(name=None):
        with pytest.raises(NoHostAvailable):
            connect(name)

    deadline = time.time() + 60
    while True:
        try:
            admin, admin_cluster = connect('admin')
            break
        except NoHostAvailable:
            if time.time() > deadline:
                raise
            await asyncio.sleep(0.5)
    try:
        for name in [None, 'client1', 'client2', 'client3']:
            refused(name)
        admin.execute("CREATE USER client2 NOSUPERUSER")
        admin.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        admin.execute("CREATE TABLE ks.cf (id int primary key)")
        client2, client2_cluster = connect('client2')
        try:
            refused('client1')
            refused('client3')
            with pytest.raises(Unauthorized, match="User client2 has no SELECT permission on <table ks.cf> or any of its parents"):
                client2.execute("SELECT * FROM ks.cf")
            with pytest.raises(Unauthorized, match="User client2 has no AUTHORIZE permission on <table ks.cf> or any of its parents"):
                client2.execute("REVOKE SELECT ON ks.cf from client2")
        finally:
            safe_driver_shutdown(client2_cluster)
        # client3's certificate has no CN: its role comes from the email.
        admin.execute("CREATE USER client3 NOSUPERUSER")
        client3, client3_cluster = connect('client3')
        try:
            # The dtests' _check_session_available().
            assert len(list(client3.execute("LIST USERS"))) > 0, "Failed to get user list from session"
        finally:
            safe_driver_shutdown(client3_cluster)
    finally:
        safe_driver_shutdown(admin_cluster)
    # A clean shutdown stops the certificate authenticator too.
    await manager.server_stop_gracefully(server.server_id)
