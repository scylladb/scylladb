import os
import ssl
import sys
import socket
import signal
import functools
import threading
from time import sleep

import sqlalchemy
from sqlalchemy.pool import StaticPool

from kmip.services import auth
from kmip.services.server.server import build_argument_parser
from kmip.services.server.server import KmipServer
from kmip import enums
from kmip.pie.client import ProxyKmipClient

# Helper wrapper for running pykmip in scylla testing. Needed for the following
# reasons:
#
# * TLS options (hardcoded) in pykmip are obsolete and will not work with
#   connecting using gnutls of any modern variety.
#
# * We need to use an in-memory SQLite database for testing (file-based SQLite
#   issues many fdatasync's which have been proven to reduce CI stability in
#   some slow machines). An in-memory SQLite database is bound to a single
#   connection. In order to share it among multiple threads, the connection
#   itself must be shared. We achieve that with the StaticPool.
#   https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#using-a-memory-database-in-multiple-threads


def monkey_patch_create_engine():
    original_create_engine = sqlalchemy.create_engine

    @functools.wraps(original_create_engine)
    def patched_create_engine(*args, **kwargs):
        if args and isinstance(args[0], str) and args[0].startswith('sqlite:///:memory:'):
            kwargs['poolclass'] = StaticPool
        return original_create_engine(*args, **kwargs)

    sqlalchemy.create_engine = patched_create_engine


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

def main():
    # Build argument parser and parser command-line arguments.
    parser = build_argument_parser()
    parser.add_option('--create-key', action='store_true', help='Create a KMIP key after starting the server')
    opts, args = parser.parse_args(sys.argv[1:])

    kwargs = {}
    if opts.hostname:
        kwargs['hostname'] = opts.hostname
    if opts.port:
        kwargs['port'] = opts.port
    if opts.certificate_path:
        kwargs['certificate_path'] = opts.certificate_path
    if opts.key_path:
        kwargs['key_path'] = opts.key_path
    if opts.ca_path:
        kwargs['ca_path'] = opts.ca_path
    if opts.auth_suite:
        kwargs['auth_suite'] = opts.auth_suite
    if opts.config_path:
        kwargs['config_path'] = opts.config_path
    if opts.log_path:
        kwargs['log_path'] = opts.log_path
    if opts.policy_path:
        kwargs['policy_path'] = opts.policy_path
    if opts.ignore_tls_client_auth:
        kwargs['enable_tls_client_auth'] = False
    if opts.logging_level:
        kwargs['logging_level'] = opts.logging_level
    if opts.database_path:
        kwargs['database_path'] = opts.database_path

    kwargs['live_policies'] = True

    monkey_patch_create_engine()

    # Create and start the server.
    s = KmipServer(**kwargs)
    # Fix TLS. Try to get this into mainline project, but that will take time...
    s.auth_suite = TLS13AuthenticationSuite(s.auth_suite.ciphers)
    # force port to zero -> select dynamically
    s.config.settings['port'] = 0

    def fake_wrap_ssl(sock, keyfile=None, certfile=None,
                server_side=False, cert_reqs=ssl.CERT_NONE,
                ssl_version=ssl.PROTOCOL_TLS, ca_certs=None,
                do_handshake_on_connect=True,
                suppress_ragged_eofs=True,
                ciphers=None):
        ctxt = ssl.SSLContext(protocol = ssl_version)
        ctxt.load_cert_chain(certfile=certfile, keyfile=keyfile)
        ctxt.verify_mode = cert_reqs
        ctxt.load_verify_locations(cafile=ca_certs)
        if ciphers:
            ctxt.set_ciphers(ciphers)
        return ctxt.wrap_socket(sock, server_side=server_side
                              , do_handshake_on_connect=do_handshake_on_connect
                              , suppress_ragged_eofs=suppress_ragged_eofs)

    ssl.wrap_socket = fake_wrap_ssl

    print("Starting...")

    with s:
        hostname = kwargs.get('hostname', '127.0.0.1')
        port = s._socket.getsockname()[1]

        print("Listening on {}".format(port), flush=True)

        if opts.create_key:
            def run_client():
                try:
                    max_retries = 50  # 5 seconds
                    delay = 0.1
                    for attempt in range(max_retries):
                        try:
                            test_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            test_sock.settimeout(max_retries * delay)
                            test_sock.connect((hostname, port))
                            test_sock.close()
                            break
                        except socket.error as e:
                            if attempt >= max_retries - 1:
                                print(f"Key creation failed: Server not ready after {max_retries * delay} seconds: {e}")
                                os.kill(os.getpid(), signal.SIGTERM)
                                return
                        sleep(delay)
                    with ProxyKmipClient(
                        hostname=hostname,
                        port=port,
                        cert=s.config.settings.get('certificate_path'),
                        key=s.config.settings.get('key_path'),
                        ca=s.config.settings.get('ca_path'),
                        config_file=kwargs.get('config_path'),
                    ) as client:
                        key_id = client.create(
                            algorithm=enums.CryptographicAlgorithm.AES,
                            length=256
                        )
                        print(f"Created key with id: {key_id}", flush=True)

                except Exception as e:
                    print(f"Key creation failed: {e}")
                    os.kill(os.getpid(), signal.SIGTERM)

            client_thread = threading.Thread(target=run_client)
            client_thread.start()

        s.serve()

if __name__ == '__main__':
    main()
