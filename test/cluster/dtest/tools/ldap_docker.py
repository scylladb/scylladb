#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging

from docker.errors import APIError, DockerException
from ldap3 import ALL, ALL_ATTRIBUTES, Connection, Server
from ldap3.core.exceptions import LDAPSocketOpenError

from tools.docker_utils import ContainerNotRunningError, container_exec_run, container_reload, container_remove, dump_container_logs, get_docker_client, get_ip_address_of_container, running_in_docker
from tools.retrying import retrying

logger = logging.getLogger(__name__)

# Short and independent of wait_for_ldap_server_startup()'s `timeout`, so the probe
# doesn't add another full wait-process timeout to each attempt.
LDAP_TCP_PROBE_TIMEOUT = 5


class ContainerAlreadyStartedError(Exception):
    pass


class ContainerDoesNotExistError(Exception):
    pass


class LdapConnectionAlreadyStartedError(Exception):
    pass


class LdapConnectionDoesNotExistError(Exception):
    pass


class LdapServerNotReadyError(Exception):
    pass


# If the server has terminated the connection lets try to rebuild it
# once.
def dump_ldap_log_on_failure(func):
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            try:
                logger.debug(f"LDAP SERVER LOG DUMP: {args[0].container.logs().decode('utf-8')}")
            except Exception as log_err:  # noqa: BLE001
                logger.debug(f"Failed to dump LDAP logs: {log_err}")
            raise

    return inner


class LdapDocker:
    def __init__(self):
        self.docker = get_docker_client()
        self.name = None
        self.ldap_port = None
        self.ldap_ssl_port = None
        self.container = None
        self.conn = None
        self.ldap_server = None
        self.ldap_base_object = None
        self.ldap_address = None

    def create_ldap_container(  # noqa: PLR0913
        self,
        name,
        ldap_port=None,
        ldap_ssl_port=None,
        image="osixia/openldap:1.4.0",
        organisation="ScyllaDB",
        domain="scylladb.com",
        password="scylla",
    ):
        if self.container:
            raise ContainerAlreadyStartedError("LDAP docker already exists for this instance")
        self.name = name

        ports = None if running_in_docker() else {"389/tcp": ("0.0.0.0", ldap_port), "636/tcp": ("0.0.0.0", ldap_ssl_port)}

        @retrying(num_attempts=10, sleep_time=1, allowed_exceptions=DockerException)
        def start_ldap():
            existing_images = self.docker.images.list(filters={"reference": image})
            if not existing_images:
                self.docker.images.pull(image)

            try:
                self.container = self.docker.containers.run(ports=ports, name=name, environment=[f"LDAP_ORGANISATION={organisation}", f"LDAP_DOMAIN={domain}", f"LDAP_ADMIN_PASSWORD={password}"], image=image, detach=True, labels=["dtest"])
            except DockerException as e:
                logger.error(f"Failed to start LDAP container: {e}")
                self.docker.containers.get(name).remove(force=True)
                raise

        # osixia/openldap's slapd process occasionally crashes during its cold-start
        # bootstrap under CI host contention (DTEST-242), leaving the container "exited".
        # wait_for_ldap_server_startup() exhausting its own retries (LdapServerNotReadyError)
        # means the same thing in practice: this particular container never came up.
        # Neither a dead nor a stuck container can recover on its own, so recreate a
        # fresh one and retry instead of failing the caller on the first attempt.
        # Waiting only 2 attempts per container keeps the worst case (~3 containers x 2 x 30s)
        # close to the old single-container budget (5 x 30s), rather than tripling it.
        @retrying(num_attempts=3, sleep_time=1, allowed_exceptions=(ContainerNotRunningError, LdapServerNotReadyError))
        def start_and_wait_for_ldap():
            start_ldap()
            try:
                self.wait_for_ldap_server_startup(num_attempts=2)
                self._harvest_address_and_ports()
            except (ContainerNotRunningError, LdapServerNotReadyError):
                self._discard_container()
                raise

        start_and_wait_for_ldap()

    def _harvest_address_and_ports(self):
        """Read address/ports only after readiness is confirmed. They are saved once and used to
        build every later LDAP Server, so a bad read must fail here rather than be saved."""
        try:
            container_reload(self.container)
            if running_in_docker():
                address, port, ssl_port = get_ip_address_of_container(self.container), "389", "636"
            else:
                ports = self.container.ports
                address, port, ssl_port = "localhost", ports["389/tcp"][0]["HostPort"], ports["636/tcp"][0]["HostPort"]
        except (APIError, StopIteration, KeyError, IndexError, TypeError) as e:
            dump_container_logs(self.container, tail=500)
            raise ContainerNotRunningError(f"LDAP container {self.name}: could not read address/ports: {e!r}") from e
        if not address:
            dump_container_logs(self.container, tail=500)
            raise ContainerNotRunningError(f"LDAP container {self.name} has no network address assigned")
        self.ldap_address, self.ldap_port, self.ldap_ssl_port = address, port, ssl_port

    def _discard_container(self):
        try:
            container_remove(self.container, force=True)
        finally:
            self.container = None

    def is_container_running(self):
        if not self.container:
            raise ContainerDoesNotExistError("LDAP docker does not exists for this instance")
        return "running" in self.container.status

    def remove_container(self, force=True):
        if not self.container:
            raise ContainerDoesNotExistError("LDAP docker does not exists for this instance")
        if self.conn:
            self.disconnect_ldap()
        container_remove(self.container, force=force)
        self.container = None

    @retrying(num_attempts=5, sleep_time=1, allowed_exceptions=LdapServerNotReadyError)
    def wait_for_ldap_server_startup(self, timeout=30, num_attempts=5):
        container_reload(self.container)
        if self.container.status != "running":
            dump_container_logs(self.container, tail=500)
            raise ContainerNotRunningError(f"LDAP container {self.name} died during startup (status: {self.container.status}). Logs dumped above.")
        # wait-process (a #!/bin/sh -e script) blocks until the image's own
        # container/run/state/startup-done marker file exists, i.e. until the
        # full osixia/openldap bootstrap has finished, not merely until some
        # supervisor process is alive.
        if container_exec_run(self.container, f"timeout {timeout}s container/tool/wait-process")[0] != 0:
            raise LdapServerNotReadyError("LDAP server didn't finish its startup yet...")
        # startup-done is written right as the bootstrap's temporary slapd is killed and
        # runit starts the final one, so port 389 is briefly closed just after wait-process
        # succeeds (~20ms observed locally). Probe it before declaring readiness. This check
        # is only meaningful after wait-process: the temporary bootstrap slapd also answers
        # on 389, so it cannot replace wait-process.
        if container_exec_run(self.container, ["timeout", f"{LDAP_TCP_PROBE_TIMEOUT}", "bash", "-c", "</dev/tcp/127.0.0.1/389"])[0] != 0:
            raise LdapServerNotReadyError("LDAP server finished startup but isn't accepting connections on port 389 yet...")

    # ldap3 marks a Server's address unavailable after a failed socket open and only
    # clears that mark after RESET_AVAILABILITY_TIMEOUT (5s, ~6s in practice because of
    # ldap3's `.seconds > 5` check), which is longer than our 2s retry sleep. A reused
    # Server therefore has no candidate addresses on the next attempt and raises
    # "invalid server address", hiding the real socket error (e.g. connection refused).
    # Build Server and Connection fresh on every attempt.
    @retrying(num_attempts=5, sleep_time=2, allowed_exceptions=LDAPSocketOpenError, message="Binding to LDAP server")
    def _bind_ldap_connection(self, user, password):
        if self.conn:
            try:
                self.conn.unbind()
            except Exception as unbind_err:  # noqa: BLE001
                logger.debug(f"Failed to unbind stale LDAP connection before rebuilding: {unbind_err}")
        self.ldap_server = Server(host=f"ldap://{self.ldap_address}:{self.ldap_port}", get_info=ALL)
        self.conn = Connection(server=self.ldap_server, user=user, password=password)
        self.conn.bind()

    @retrying(num_attempts=5, sleep_time=2, allowed_exceptions=LdapServerNotReadyError, message="Trying to create LDAP connection")
    def create_ldap_connection(self, user="cn=admin,dc=scylladb,dc=com", password="scylla"):
        self.wait_for_ldap_server_startup(5)
        self._bind_ldap_connection(user, password)
        self.ldap_base_object = self.ldap_server.info.naming_contexts[0]

    def is_ldap_connection_bound(self):
        return self.conn.bound

    def disconnect_ldap(self):
        if not self.conn:
            raise LdapConnectionDoesNotExistError("LDAP connection does not exist for this instance")
        self.conn.unbind()
        self.conn = None

    @dump_ldap_log_on_failure
    def add_ldap_object(self, *args, **kwargs):
        self.conn.add(*args, **kwargs)
        return self.conn.result

    @dump_ldap_log_on_failure
    def delete_ldap_object(self, *args, **kwargs):
        self.conn.delete(*args, **kwargs)
        return self.conn.result

    @dump_ldap_log_on_failure
    def search_ldap_object(self, search_base, search_filter):
        self.conn.search(search_base=search_base, search_filter=search_filter, attributes=ALL_ATTRIBUTES)
        return self.conn.entries

    @dump_ldap_log_on_failure
    def modify_ldap_object(self, *args, **kwargs):
        return self.conn.modify(*args, **kwargs)
