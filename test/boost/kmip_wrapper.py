import ssl
import sys

from kmip.services import auth
from kmip.services.server.server import build_argument_parser
from kmip.services.server.server import KmipServer

# Helper wrapper for running pykmip in scylla testing. Needed because TLS options
# (hardcoded) in pykmip are obsolete and will not work with connecting using gnutls
# of any modern variety.

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
        ctxt.set_ciphers(ciphers)
        return ctxt.wrap_socket(sock, server_side=server_side
                              , do_handshake_on_connect=do_handshake_on_connect
                              , suppress_ragged_eofs=suppress_ragged_eofs)

    ssl.wrap_socket = fake_wrap_ssl

    print("Starting...")

    with s:
        print("Listening on {}".format(s._socket.getsockname()[1]))
        sys.stdout.flush()
        s.serve()

if __name__ == '__main__':
    main()
