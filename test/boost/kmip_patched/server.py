# Copyright (c) 2015 The Johns Hopkins University/Applied Physics Laboratory
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import copy
import logging
import logging.handlers as handlers
import multiprocessing
import optparse
import os
import signal
import six
import socket
import ssl
import sys
import threading

from kmip.core import exceptions
from kmip.core import policy as operation_policy
from kmip.services import auth
from kmip.services.server import config
#from kmip.services.server import import engine
from . import engine
from kmip.services.server import monitor
from kmip.services.server import session


class KmipServer(object):
    """
    The public front-end for the entire KmipServer service.

    The KmipServer manages the server configuration and oversees the creation
    of KmipSessions for all successful client connections. It creates the
    KmipEngine used to process all KMIP requests and is in charge of safely
    shutting down all server components upon receiving a termination signal.
    """

    # TODO (peter-hamilton) Move to using **kwargs for all server parameters.
    def __init__(
            self,
            hostname=None,
            port=None,
            certificate_path=None,
            key_path=None,
            ca_path=None,
            auth_suite=None,
            config_path='/etc/pykmip/server.conf',
            log_path='/var/log/pykmip/server.log',
            policy_path=None,
            enable_tls_client_auth=None,
            tls_cipher_suites=None,
            logging_level=None,
            live_policies=False,
            database_path=None
    ):
        """
        Create a KmipServer.

        Settings are loaded initially from the configuration file located at
        config_path, if specified. All other configuration options listed
        below, if specified, will override the settings loaded from the
        configuration file.

        A rotating file logger will be set up with the base log file located
        at log_path. The server itself will handle rotating the log files as
        the logs grow. The server process must have permission to read/write
        to the specified log directory.

        The main KmipEngine request processor is created here, along with all
        information required to manage KMIP client connections and sessions.

        Args:
            hostname (string): The host address the server will be bound to
                (e.g., '127.0.0.1'). Optional, defaults to None.
            port (int): The port number the server will be bound to
                (e.g., 5696). Optional, defaults to None.
            certificate_path (string): The path to the server certificate file
                (e.g., '/etc/pykmip/certs/server.crt'). Optional, defaults to
                None.
            key_path (string): The path to the server certificate key file
                (e.g., '/etc/pykmip/certs/server.key'). Optional, defaults to
                None.
            ca_path (string): The path to the certificate authority (CA)
                certificate file (e.g., '/etc/pykmip/certs/ca.crt'). Optional,
                defaults to None.
            auth_suite (string): A string value indicating the type of
                authentication suite to use for establishing TLS connections.
                Accepted values are: 'Basic', 'TLS1.2'. Optional, defaults to
                None.
            config_path (string): The path to the server configuration file
                (e.g., '/etc/pykmip/server.conf'). Optional, defaults to
                '/etc/pykmip/server.conf'.
            log_path (string): The path to the base server log file
                (e.g., '/var/log/pykmip/server.log'). Optional, defaults to
                '/var/log/pykmip/server.log'.
            policy_path (string): The path to the filesystem directory
                containing PyKMIP server operation policy JSON files.
                Optional, defaults to None.
            enable_tls_client_auth (boolean): A boolean indicating if the TLS
                certificate client auth flag should be required for client
                certificates when establishing a new client session. Optional,
                defaults to None.
            tls_cipher_suites (string): A comma-delimited list of cipher suite
                names (e.g., TLS_RSA_WITH_AES_256_CBC_SHA256,TLS_RSA_WITH_AES_
                128_CBC_SHA256), indicating which specific cipher suites should
                be used by the server when establishing a TLS connection with
                a client. Optional, defaults to None. If None, the default set
                of TLS cipher suites will be used.
            logging_level (string): A logging level enumeration defined by the
                logging package (e.g., DEBUG, INFO). Sets the base logging
                level for the server. All log messages logged at this level or
                higher in criticality will be logged. All log messages lower
                in criticality will not be logged. Optional, defaults to None.
            live_policies (boolean): A boolean indicating if the operation
                policy directory should be actively monitored to autoload any
                policy changes while the server is running. Optional, defaults
                to False.
            database_path (string): The path to the server's SQLite database
                file. Optional, defaults to None.
        """
        self._logger = logging.getLogger('kmip.server')
        self._setup_logging(log_path)

        self.config = config.KmipServerConfig()
        self._setup_configuration(
            config_path,
            hostname,
            port,
            certificate_path,
            key_path,
            ca_path,
            auth_suite,
            policy_path,
            enable_tls_client_auth,
            tls_cipher_suites,
            logging_level,
            database_path
        )
        self.live_policies = live_policies
        self.policies = {}

        self._logger.setLevel(self.config.settings.get('logging_level'))

        cipher_suites = self.config.settings.get('tls_cipher_suites')
        if self.config.settings.get('auth_suite') == 'TLS1.2':
            self.auth_suite = auth.TLS12AuthenticationSuite(cipher_suites)
        else:
            self.auth_suite = auth.BasicAuthenticationSuite(cipher_suites)

        self._session_id = 1
        self._is_serving = False

    def _setup_logging(self, path):
        # Create the logging directory/file if it doesn't exist.
        if not os.path.exists(path):
            if not os.path.isdir(os.path.dirname(path)):
                os.makedirs(os.path.dirname(path))
            open(path, 'w').close()

        handler = handlers.RotatingFileHandler(
            path,
            mode='a',
            maxBytes=1000000,
            backupCount=5
        )
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
        )
        self._logger.addHandler(handler)
        self._logger.setLevel(logging.DEBUG)

    def _setup_configuration(
            self,
            path=None,
            hostname=None,
            port=None,
            certificate_path=None,
            key_path=None,
            ca_path=None,
            auth_suite=None,
            policy_path=None,
            enable_tls_client_auth=None,
            tls_cipher_suites=None,
            logging_level=None,
            database_path=None
    ):
        if path:
            self.config.load_settings(path)

        if hostname:
            self.config.set_setting('hostname', hostname)
        if port:
            self.config.set_setting('port', port)
        if certificate_path:
            self.config.set_setting('certificate_path', certificate_path)
        if key_path:
            self.config.set_setting('key_path', key_path)
        if ca_path:
            self.config.set_setting('ca_path', ca_path)
        if auth_suite:
            self.config.set_setting('auth_suite', auth_suite)
        if policy_path:
            self.config.set_setting('policy_path', policy_path)
        if enable_tls_client_auth is not None:
            self.config.set_setting(
                'enable_tls_client_auth',
                enable_tls_client_auth
            )
        if tls_cipher_suites:
            self.config.set_setting(
                'tls_cipher_suites',
                tls_cipher_suites.split(',')
            )
        if logging_level:
            self.config.set_setting('logging_level', logging_level)
        if database_path:
            self.config.set_setting('database_path', database_path)

    def start(self):
        """
        Prepare the server to start serving connections.

        Configure the server socket handler and establish a TLS wrapping
        socket from which all client connections descend. Bind this TLS
        socket to the specified network address for the server.

        Raises:
            NetworkingError: Raised if the TLS socket cannot be bound to the
                network address.
        """
        self._logger.debug("[KMIP-PATCHED] KmipServer.start(): Creating policy monitor.")
        self.manager = multiprocessing.Manager()
        self.policies = self.manager.dict()
        policies = copy.deepcopy(operation_policy.policies)
        for policy_name, policy_set in six.iteritems(policies):
            self.policies[policy_name] = policy_set

        self.policy_monitor = monitor.PolicyDirectoryMonitor(
            self.config.settings.get('policy_path'),
            self.policies,
            self.live_policies
        )

        def interrupt_handler(trigger, frame):
            self.policy_monitor.stop()
        signal.signal(signal.SIGINT, interrupt_handler)
        signal.signal(signal.SIGTERM, interrupt_handler)

        self.policy_monitor.start()
        self._logger.debug("[KMIP-PATCHED] KmipServer.start(): Created policy monitor.")

        self._logger.debug("[KMIP-PATCHED] KmipServer.start(): Creating KMIP engine.")
        self._engine = engine.KmipEngine(
            policies=self.policies,
            database_path=self.config.settings.get('database_path')
        )
        self._logger.debug("[KMIP-PATCHED] KmipServer.start(): Created KMIP engine.")

        self._logger.info("Starting server socket handler.")

        self._logger.debug("[KMIP-PATCHED] KmipServer.start(): Creating TCP socket.")
        # Create a TCP stream socket and configure it for immediate reuse.
        socket.setdefaulttimeout(10)
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._logger.debug("[KMIP-PATCHED] KmipServer.start(): Created TCP socket.")

        self._logger.debug(
            "Configured cipher suites: {0}".format(
                len(self.config.settings.get('tls_cipher_suites'))
            )
        )
        for cipher in self.config.settings.get('tls_cipher_suites'):
            self._logger.debug(cipher)
        auth_suite_ciphers = self.auth_suite.ciphers.split(':')
        self._logger.debug(
            "Authentication suite ciphers to use: {0}".format(
                len(auth_suite_ciphers)
            )
        )
        for cipher in auth_suite_ciphers:
            self._logger.debug(cipher)

        self._logger.debug("[KMIP-PATCHED] KmipServer.start(): Wrapping TCP socket with TLS.")
        self._socket = ssl.wrap_socket(
            self._socket,
            keyfile=self.config.settings.get('key_path'),
            certfile=self.config.settings.get('certificate_path'),
            server_side=True,
            cert_reqs=ssl.CERT_REQUIRED,
            ssl_version=self.auth_suite.protocol,
            ca_certs=self.config.settings.get('ca_path'),
            do_handshake_on_connect=False,
            suppress_ragged_eofs=True,
            ciphers=self.auth_suite.ciphers
        )
        self._logger.debug("[KMIP-PATCHED] KmipServer.start(): Wrapped TCP socket with TLS.")

        try:
            self._logger.debug("[KMIP-PATCHED] KmipServer.start(): Binding TLS socket to hostname '{0}' and port '{1}'.".format(
                self.config.settings.get('hostname'),
                self.config.settings.get('port')
            ))
            self._socket.bind(
                (
                    self.config.settings.get('hostname'),
                    int(self.config.settings.get('port'))
                )
            )
            self._logger.debug("[KMIP-PATCHED] KmipServer.start(): Bound TLS socket to hostname and port.")
        except Exception as e:
            self._logger.exception(e)
            raise exceptions.NetworkingError(
                "Server failed to bind socket handler to {0}:{1}".format(
                    self.config.settings.get('hostname'),
                    self.config.settings.get('port')
                )
            )
        else:
            self._logger.info(
                "Server successfully bound socket handler to {0}:{1}".format(
                    self.config.settings.get('hostname'),
                    self.config.settings.get('port')
                )
            )
            self._is_serving = True

    def stop(self):
        """
        Stop the server.

        Halt server client connections and clean up any existing connection
        threads.

        Raises:
            NetworkingError: Raised if a failure occurs while sutting down
                or closing the TLS server socket.
        """
        self._logger.info("Cleaning up remaining connection threads.")

        for thread in threading.enumerate():
            if thread is not threading.current_thread():
                try:
                    thread.join(10.0)
                except Exception as e:
                    self._logger.info(
                        "Error occurred while attempting to cleanup thread: "
                        "{0}".format(thread.name)
                    )
                    self._logger.exception(e)
                else:
                    if thread.is_alive():
                        self._logger.warning(
                            "Cleanup failed for thread: {0}. Thread is "
                            "still alive".format(thread.name)
                        )
                    else:
                        self._logger.info(
                            "Cleanup succeeded for thread: {0}".format(
                                thread.name
                            )
                        )

        self._logger.info("Shutting down server socket handler.")
        try:
            self._socket.shutdown(socket.SHUT_RDWR)
            self._socket.close()
        except Exception as e:
            self._logger.exception(e)
            raise exceptions.NetworkingError(
                "Server failed to shutdown socket handler."
            )

        if hasattr(self, "policy_monitor"):
            try:
                self.policy_monitor.stop()
                self.policy_monitor.join()
            except Exception as e:
                self._logger.exception(e)
                raise exceptions.ShutdownError(
                    "Server failed to clean up the policy monitor."
                )

    def serve(self):
        """
        Serve client connections.

        Begin listening for client connections, spinning off new KmipSessions
        as connections are handled. Set up signal handling to shutdown
        connection service as needed.
        """
        self._socket.listen(5)

        def _signal_handler(signal_number, stack_frame):
            self._is_serving = False

            # Python3.5+ silently ignores SIGINT and retries system calls if
            # the signal handler does not raise an exception. Explicitly
            # detect SIGINT and raise a KeyboardInterrupt exception to regain
            # old functionality.
            if signal_number == signal.SIGINT:
                raise KeyboardInterrupt("SIGINT received")

        signal.signal(signal.SIGINT, _signal_handler)
        signal.signal(signal.SIGTERM, _signal_handler)

        self._logger.info("Starting connection service...")

        while self._is_serving:
            try:
                connection, address = self._socket.accept()
            except socket.timeout:
                # Setting the default socket timeout to break hung connections
                # will cause accept to periodically raise socket.timeout. This
                # is expected behavior, so ignore it and retry accept.
                pass
            except socket.error as e:
                self._logger.warning(
                    "Error detected while establishing new connection."
                )
                self._logger.exception(e)
            except KeyboardInterrupt:
                self._logger.warning("Interrupting connection service.")
                self._is_serving = False
                break
            except Exception as e:
                self._logger.warning(
                    "Error detected while establishing new connection."
                )
                self._logger.exception(e)
            else:
                self._setup_connection_handler(connection, address)

        self._logger.info("Stopping connection service.")

    def _setup_connection_handler(self, connection, address):
        self._logger.info(
            "Receiving incoming connection from: {0}:{1}".format(
                address[0],
                address[1]
            )
        )

        session_name = "{0:08}".format(self._session_id)
        self._session_id += 1

        self._logger.info(
            "Dedicating session {0} to {1}:{2}".format(
                session_name,
                address[0],
                address[1]
            )
        )

        try:
            s = session.KmipSession(
                self._engine,
                connection,
                address,
                name=session_name,
                enable_tls_client_auth=self.config.settings.get(
                    'enable_tls_client_auth'
                ),
                auth_settings=self.config.settings.get('auth_plugins')
            )
            s.daemon = True
            s.start()
        except Exception as e:
            self._logger.warning(
                "Failure occurred while starting session: {0}".format(
                    session_name
                )
            )
            self._logger.exception(e)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()


def build_argument_parser():
    parser = optparse.OptionParser(
        usage="%prog [options]",
        description="Run the PyKMIP software server.")

    parser.add_option(
        "-n",
        "--hostname",
        action="store",
        type="str",
        default=None,
        dest="hostname",
        help=(
            "The host address the server will be bound to. A string "
            "representing either a hostname in Internet domain notation or "
            "an IPv4 address. Defaults to None."
        ),
    )
    parser.add_option(
        "-p",
        "--port",
        action="store",
        type="int",
        default=None,
        dest="port",
        help=(
            "The port number the server will be bound to. An integer "
            "representing a port number. Recommended to be 5696 according to "
            "the KMIP specification. Defaults to None."
        ),
    )
    parser.add_option(
        "-c",
        "--certificate_path",
        action="store",
        type="str",
        default=None,
        dest="certificate_path",
        help=(
            "A string representing a path to a PEM-encoded server "
            "certificate file. Defaults to None."
        ),
    )
    parser.add_option(
        "-k",
        "--key_path",
        action="store",
        type="str",
        default=None,
        dest="key_path",
        help=(
            "A string representing a path to a PEM-encoded server "
            "certificate key file. Defaults to None."
        ),
    )
    parser.add_option(
        "-a",
        "--ca_path",
        action="store",
        type="str",
        default=None,
        dest="ca_path",
        help=(
            "A string representing a path to a PEM-encoded certificate "
            "authority certificate file. Defaults to None."
        ),
    )
    parser.add_option(
        "-s",
        "--auth_suite",
        action="store",
        type="str",
        default=None,
        dest="auth_suite",
        help=(
            "A string representing the type of authentication suite to use "
            "when establishing TLS connections. Options include 'Basic' and "
            "'TLS1.2'. Defaults to None."
        ),
    )
    parser.add_option(
        "-f",
        "--config_path",
        action="store",
        type="str",
        default=None,
        dest="config_path",
        help=(
            "A string representing a path to a server configuration file. "
            "Defaults to None."
        ),
    )
    parser.add_option(
        "-l",
        "--log_path",
        action="store",
        type="str",
        default=None,
        dest="log_path",
        help=(
            "A string representing a path to a log file. Defaults to None."
        ),
    )
    parser.add_option(
        "-o",
        "--policy_path",
        action="store",
        type="str",
        default=None,
        dest="policy_path",
        help=(
            "A string representing a path to the operation policy filesystem "
            "directory. Optional, defaults to None."
        ),
    )
    parser.add_option(
        "-i",
        "--ignore_tls_client_auth",
        action="store_true",
        default=False,
        dest="ignore_tls_client_auth",
        help=(
            "A boolean indicating whether or not the TLS certificate client "
            "auth flag should be ignored when establishing client sessions. "
            "Optional, defaults to None."
        )
    )
    parser.add_option(
        "-v",
        "--logging_level",
        action="store",
        type="str",
        default=None,
        dest="logging_level",
        help=(
            "A string representing the logging level for the server (e.g., "
            "DEBUG, INFO). Optional, defaults to None."
        )
    )
    parser.add_option(
        "-d",
        "--database_path",
        action="store",
        type="str",
        default=None,
        dest="database_path",
        help=(
            "A string representing a path to the server's SQLite database "
            "file. Optional, defaults to None."
        ),
    )

    return parser


def main(args=None):
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
    with s:
        s.serve()


if __name__ == '__main__':
    main()
