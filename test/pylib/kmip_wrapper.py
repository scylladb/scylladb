#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import ssl
import sys
import functools
import socket
import signal
import sqlalchemy
from sqlalchemy.pool import StaticPool

from kmip.services import auth
from kmip.services.server.server import KmipServer, build_argument_parser, exceptions

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

class KMIPServerWrapper:
    """Wrapper for PyKMIP server"""
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.kmip_server = None
        self.original_create_engine = None
        #self.original_wrap_socket = None

    @property
    def port(self):
        # pylint: disable=protected-access
        """Listening port"""
        return self.kmip_server._socket.getsockname()[1]

    def serve(self):
        """server wrapper"""
        self._serve_in_thread()

    def _serve_in_thread(self):
        # pylint: disable=protected-access,broad-exception-caught
        """Terrible copy of serve function, but without signal handlers"""
        kmip_server = self.kmip_server
        kmip_server._socket.listen(5)
        kmip_server._logger.info("Starting connection service...")

        while kmip_server._is_serving:
            try:
                connection, address = kmip_server._socket.accept()
            except socket.timeout:
                # Setting the default socket timeout to break hung connections
                # will cause accept to periodically raise socket.timeout. This
                # is expected behavior, so ignore it and retry accept.
                pass
            except socket.error as e:
                kmip_server._logger.warning(
                    "Error detected while establishing new connection."
                )
                kmip_server._logger.exception(e)
            except KeyboardInterrupt:
                kmip_server._logger.warning("Interrupting connection service.")
                kmip_server._is_serving = False
                break
            except Exception as e:
                if kmip_server._is_serving:
                    kmip_server._logger.warning(
                        "Error detected while establishing new connection."
                    )
                    kmip_server._logger.exception(e)
            else:
                kmip_server._setup_connection_handler(connection, address)

        kmip_server._logger.info("Stopping connection service.")

    def shutdown(self):
        """Stop serving"""
        # pylint: disable=protected-access
        self.kmip_server._logger.info("Shutting down server socket handler.")
        self.kmip_server._is_serving = False
        self.kmip_server._socket.shutdown(socket.SHUT_RDWR)
        self.kmip_server._socket.close()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        """start"""
        self.original_create_engine = sqlalchemy.create_engine

        @functools.wraps(self.original_create_engine)
        def patched_create_engine(*args, **kwargs):
            if args and isinstance(args[0], str) and args[0].startswith('sqlite:///:memory:'):
                kwargs['poolclass'] = StaticPool
            return self.original_create_engine(*args, **kwargs)

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
            ctxt.set_ciphers(ciphers)
            return ctxt.wrap_socket(sock, server_side=server_side
                                  , do_handshake_on_connect=do_handshake_on_connect
                                  , suppress_ragged_eofs=suppress_ragged_eofs)

        sqlalchemy.create_engine = patched_create_engine
        ssl.wrap_socket = fake_wrap_ssl

        # Create and start the server.
        self.kmip_server = KmipServer(**self.kwargs)
        # Fix TLS. Try to get this into mainline project, but that will take time...
        self.kmip_server.auth_suite = TLS13AuthenticationSuite(self.kmip_server.auth_suite.ciphers)
        # force port to zero -> select dynamically
        self.kmip_server.config.settings['port'] = 0
        self.kmip_server.start()

    def stop(self):
        # pylint: disable=protected-access,broad-exception-caught
        """stop"""
        kmip_server = self.kmip_server
        kmip_server._logger.info("Stopping...")
        self.shutdown()
        # KMIPServer stop is somewhat broken in that it assumes all threads belong to it.
        # We can really just ignore the serving threads. They are daemons and are either
        # done or not important at this point.

        if hasattr(kmip_server, "policy_monitor"):
            try:
                kmip_server.policy_monitor.stop()
                kmip_server.policy_monitor.join()
            except Exception as e:
                kmip_server._logger.exception(e)
                raise exceptions.ShutdownError("Server failed to clean up the policy monitor.")

        sqlalchemy.create_engine = self.original_create_engine


def main():
    """Called from parent process"""
    # Build argument parser and parser command-line arguments.
    parser = build_argument_parser()
    opts, _ = parser.parse_args(sys.argv[1:])

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

    s = KMIPServerWrapper(**kwargs)
    print("Starting...")

    with s:
        print(f'Listening on {s.port}')
        sys.stdout.flush()

        # place signal handling here, and just do a throw
        # to escape the serve loop. We will not wait for daemons
        # or anything, just exit. This makes tests (boost) using
        # this _not_ wait 10s at exit.
        def _signal_handler(signal_number, stack_frame):
            # pylint: disable=protected-access,unused-argument
            s.kmip_server._is_serving = False
            raise KeyboardInterrupt("signal received")

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

        s.serve()

if __name__ == '__main__':
    main()
