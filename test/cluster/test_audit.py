#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import copy
import datetime
import enum
import itertools
import logging
import os.path
import re
import socket
import socketserver
import tempfile
import threading
import uuid
from collections import namedtuple
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Optional, override

import pytest
from cassandra import AlreadyExists, AuthenticationFailed, ConsistencyLevel, InvalidRequest, Unauthorized, Unavailable, WriteFailure
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import NoHostAvailable, Session, EXEC_PROFILE_DEFAULT
from cassandra.query import SimpleStatement, named_tuple_factory

from test.cluster.dtest.dtest_class import create_ks, wait_for
from test.cluster.dtest.tools.assertions import assert_invalid
from test.cluster.dtest.tools.cluster import run_rest_api
from test.cluster.dtest.tools.data import rows_to_list, run_in_parallel

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import read_barrier

logger = logging.getLogger(__name__)

# Stable socket path for syslog backends, shared across tests to enable server reuse.
_syslog_socket_path = tempfile.mktemp(prefix="/tmp/scylla-audit-", suffix=".socket")

# Keys that require server restart (not live-updatable).
_NON_LIVE_AUDIT_KEYS = {"audit", "audit_unix_socket_path"}
# Keys that can be updated via SIGHUP (live-updatable).
_LIVE_AUDIT_KEYS = {"audit_categories", "audit_keyspaces", "audit_tables"}
# Auth config applied when user/password are requested.
_AUTH_CONFIG = {
    "authenticator": "org.apache.cassandra.auth.PasswordAuthenticator",
    "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer",
}


class AuditRowMustNotExistError(Exception):
    pass


class AuditTester:
    audit_default_settings = {"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}

    def _build_server_config(self, needed: dict[str, str],
                             enable_compact_storage: bool,
                             user: str | None) -> dict[str, Any]:
        """Build the full server config dict from audit settings, auth, and flags."""
        cfg: dict[str, Any] = dict(needed)
        cfg["enable_create_table_with_compact_storage"] = enable_compact_storage
        if user:
            cfg.update(_AUTH_CONFIG)
        return cfg

    async def _check_restart_needed(self, current: dict[str, str],
                                    needed: dict[str, str],
                                    absent_keys: set[str],
                                    user: str | None) -> bool:
        """Decide whether a running server must be restarted.

        A restart is needed when non-live audit keys or auth config changed.
        """
        # Non-live audit keys changed or need to be removed.
        restart = any(
            str(current.get(k, "")) != str(needed.get(k, ""))
            for k in _NON_LIVE_AUDIT_KEYS if k in needed
        ) or any(k in current for k in _NON_LIVE_AUDIT_KEYS & absent_keys)

        # Auth config changes also require a restart.
        has_auth = any(k in current for k in _AUTH_CONFIG)
        if user:
            restart = restart or any(
                str(current.get(k, "")) != str(v)
                for k, v in _AUTH_CONFIG.items()
            )
        elif has_auth:
            restart = True

        return restart

    async def _apply_config_to_running_servers(self, servers, needed: dict[str, str],
                                               absent_keys: set[str],
                                               enable_compact_storage: bool,
                                               user: str | None,
                                               auth_provider) -> None:
        """Reconfigure a running server, restarting only if necessary."""
        for srv in servers:
            current = await self.manager.server_get_config(srv.server_id)

            needs_restart = await self._check_restart_needed(current, needed, absent_keys, user)

            # Transitioning from auth to no-auth: remove auth keys before restart.
            has_auth = any(k in current for k in _AUTH_CONFIG)
            if not user and has_auth:
                for k in _AUTH_CONFIG:
                    await self.manager.server_remove_config_option(srv.server_id, k)

            # Remove absent keys so the server reverts to compiled-in defaults.
            for k in absent_keys:
                await self.manager.server_remove_config_option(srv.server_id, k)

            if needs_restart:
                await self.manager.server_stop_gracefully(srv.server_id)
                full_cfg = self._build_server_config(needed, enable_compact_storage, user)
                await self.manager.server_update_config(srv.server_id, config_options=full_cfg)
                await self.manager.server_start(srv.server_id)
                await self.manager.driver_connect(auth_provider=auth_provider)
            else:
                # Server stays up — only push live-updatable keys.
                live_cfg = {k: v for k, v in needed.items() if k in _LIVE_AUDIT_KEYS}
                live_cfg["enable_create_table_with_compact_storage"] = enable_compact_storage
                await self.manager.server_update_config(srv.server_id, config_options=live_cfg)

    async def _start_fresh_servers(self, needed: dict[str, str],
                                   enable_compact_storage: bool,
                                   rf: int,
                                   user: str | None,
                                   auth_provider,
                                   property_file: list[dict[str, str]] | None = None,
                                   cmdline: list[str] | None = None,
                                   has_stopped_servers: bool = False) -> list[str]:
        """Add and start a brand new server with the given config.

        When has_stopped_servers is True, the cluster already contains stopped
        servers whose stale seed IPs would poison newly added servers.  Work
        around this by bootstrapping the first server individually with an
        explicit self-seed before adding the rest.
        """
        cfg = self._build_server_config(needed, enable_compact_storage, user)
        connect_opts: dict[str, Any] = {}
        if auth_provider:
            connect_opts["auth_provider"] = auth_provider
        if property_file is None:
            property_file = [{"dc": "dc1", "rack": f"rack{i + 1}"} for i in range(rf)]

        if has_stopped_servers:
            # Add the first server without starting to learn its IP.
            first = await self.manager.server_add(
                config=cfg, property_file=property_file[0],
                cmdline=cmdline, start=False, connect_driver=False)
            # Start it with an explicit self-seed so it doesn't try the dead IP.
            await self.manager.server_start(first.server_id, seeds=[first.ip_addr])
            server_infos = [first]
            if len(property_file) > 1:
                rest = await self.manager.servers_add(
                    servers_num=len(property_file) - 1, config=cfg,
                    property_file=property_file[1:],
                    cmdline=cmdline,
                    driver_connect_opts=connect_opts)
                server_infos.extend(rest)
        else:
            server_infos = await self.manager.servers_add(
                servers_num=len(property_file), config=cfg,
                property_file=property_file,
                cmdline=cmdline,
                driver_connect_opts=connect_opts,
            )
        return [s.ip_addr for s in server_infos]

    async def prepare_audit_server(self, helper: AuditBackend,
                                   audit_settings: dict[str, str] = audit_default_settings,
                                   enable_compact_storage: bool = False,
                                   rf: int = 1,
                                   user: str | None = None,
                                   password: str | None = None,
                                   property_file: list[dict[str, str]] | None = None,
                                   cmdline: list[str] | None = None) -> list[str]:
        """Ensure a server is running with the correct audit config.

        Starts a new server if none exists, restarts if non-live config
        changed, or live-updates the running server. Clears audit logs.

        Returns:
            List of server IP addresses.
        """
        needed = helper.update_audit_settings(audit_settings)
        absent_keys = (_NON_LIVE_AUDIT_KEYS | _LIVE_AUDIT_KEYS) - needed.keys()
        auth_provider = PlainTextAuthProvider(username=user, password=password or "") if user else None
        expected_servers = len(property_file) if property_file else rf

        servers = await self.manager.running_servers()
        has_stopped_servers = False
        if servers and len(servers) != expected_servers:
            self.manager.driver_close()
            for srv in servers:
                await self.manager.server_stop_gracefully(srv.server_id)
            has_stopped_servers = True
            servers = None

        if servers:
            server_ips = [srv.ip_addr for srv in servers]
            await self._apply_config_to_running_servers(
                servers, needed, absent_keys, enable_compact_storage, user, auth_provider)
        else:
            server_ips = await self._start_fresh_servers(
                needed, enable_compact_storage, rf, user, auth_provider,
                property_file=property_file, cmdline=cmdline,
                has_stopped_servers=has_stopped_servers)

        cql = self.manager.get_cql()
        cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
        audit_mode = needed.get("audit") or ""
        if "table" not in audit_mode:
            cql.execute("DROP KEYSPACE IF EXISTS audit")
        helper.clear_audit_logs(cql)

        return server_ips

    async def prepare(self, helper: AuditBackend | None = None,
                      audit_settings: dict[str, str] = audit_default_settings,
                      create_keyspace: bool = True, rf: int = 1,
                      enable_compact_storage: bool = False,
                      user: str | None = None, password: str | None = None,
                      property_file: list[dict[str, str]] | None = None,
                      cmdline: list[str] | None = None) -> Session:
        """Prepare a server with the given audit config and return a CQL session.

        Ensures the server is running with the correct audit configuration,
        sets self.helper and self.server_addresses, optionally creates the
        test keyspace, and returns the CQL session.
        """
        self.helper = helper or AuditBackendTable()
        self.server_addresses = await self.prepare_audit_server(
            self.helper, audit_settings,
            enable_compact_storage=enable_compact_storage,
            rf=rf,
            user=user, password=password,
            property_file=property_file,
            cmdline=cmdline)
        session = self.manager.get_cql()
        session.execute("DROP KEYSPACE IF EXISTS ks")
        if create_keyspace:
            create_ks(session, "ks", rf)
        return session


@dataclass(eq=True, order=True)
class AuditEntry:
    category: str
    cl: str
    error: bool
    ks: str
    statement: str
    table: str
    user: str


class AuditBackend:
    def __init__(self, socket_path: str | None = None) -> None:
        super().__init__()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def audit_mode(self) -> str:
        raise NotImplementedError

    def clear_audit_logs(self, session: Session | None = None) -> None:
        pass

    def update_audit_settings(self, audit_settings, modifiers=None):
        raise NotImplementedError

    def get_audit_log_dict(self, session, consistency_level):
        raise NotImplementedError


class AuditBackendTable(AuditBackend):
    AUDIT_LOG_QUERY = "SELECT * FROM audit.audit_log"

    audit_default_settings = {"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}

    def __init__(self, socket_path: str | None = None):
        super().__init__(socket_path=socket_path)

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @override
    def audit_mode(self) -> str:
        return "table"

    @override
    def clear_audit_logs(self, session: Session | None = None) -> None:
        if session is None:
            return
        try:
            session.execute("TRUNCATE audit.audit_log")
        except InvalidRequest:
            pass  # audit keyspace may not exist yet

    def update_audit_settings(self, audit_settings, modifiers=None):
        if modifiers is None:
            modifiers = {}
        new_audit_settings = copy.deepcopy(audit_settings or self.audit_default_settings)
        for key in modifiers:
            new_audit_settings[key] = modifiers[key]
        return new_audit_settings

    @override
    def get_audit_log_dict(self, session, consistency_level):
        """_summary_
        returns a dictionary mapping audit mode name to a sorted list of audit log,
        the logs are sorted by the event times (time-uuid) with the node as tie breaker.
        """
        # We would like to have named tuples as results so we can verify the
        # order in which the fields are returned as the tests make assumptions about this.
        assert session.row_factory == named_tuple_factory
        res = session.execute(SimpleStatement(self.AUDIT_LOG_QUERY, consistency_level=consistency_level))
        res_list = list(res)
        res_list.sort(key=lambda row: (row.event_time.time, row.node))
        return { self.audit_mode(): res_list }


class UnixSockerListener:
    class UnixDatagramHandler(socketserver.BaseRequestHandler):

        def handle(self):
            with self.server.parent_instance.condition:
                data = self.request[0].decode("utf-8").strip()
                if data.startswith(self.server.parent_instance.notification_msg):
                    received_id = int(data[len(self.server.parent_instance.notification_msg):])
                    self.server.parent_instance.notification_id = received_id
                    self.server.parent_instance.condition.notify_all()
                elif data != "Initializing syslog audit backend.":
                    self.server.parent_instance.lines.append(data)

    class UnixDatagramServer(socketserver.ThreadingUnixDatagramServer):
        def __init__(self, socket_path, handler, parent_instance, lock):
            self.parent_instance = parent_instance
            self.mutex = lock
            super().__init__(socket_path, handler)

    def __init__(self, socket_path):
        self.socket_path = socket_path
        self.lines = []
        self.server = self.UnixDatagramServer(socket_path, self.UnixDatagramHandler, self, threading.Lock())
        self.server.server_activate()

        self.notification_msg = "Notifying syslog server with id: "
        self.notification_id = 0
        self.condition = threading.Condition(self.server.mutex)

        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()

        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)

    def wait_for_newer_notification_id(self):
        with self.condition:
            expected_id = self.notification_id + 1
            self.sock.sendto(bytes(self.notification_msg + str(expected_id), 'utf-8'), self.socket_path)
            self.condition.wait_for(lambda: self.notification_id == expected_id)

    def get_lines(self):
        # Make sure all in-progress handle() calls are finished
        self.wait_for_newer_notification_id()
        return copy.deepcopy(self.lines)

    def shutdown(self):
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()
        self.sock.close()


class AuditBackendSyslog(AuditBackend):
    audit_default_settings = {"audit": "syslog", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}

    def __init__(self, socket_path: str | None = None):
        super().__init__(socket_path=socket_path)
        self.socket_path = socket_path or tempfile.mktemp(prefix="/tmp/scylla-audit-", suffix=".socket")
        self.unix_socket_listener = UnixSockerListener(self.socket_path)
        self.named_tuple_factory = namedtuple("Row", ["date", "node", "event_time", "category", "consistency", "error", "keyspace_name", "operation", "source", "table_name", "username"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.unix_socket_listener.shutdown()
        if os.path.exists(self.socket_path):
            os.remove(self.socket_path)

    @override
    def audit_mode(self) -> str:
        return "syslog"

    @override
    def clear_audit_logs(self, session: Session | None = None) -> None:
        self.unix_socket_listener.lines.clear()

    def update_audit_settings(self, audit_settings, modifiers=None):
        if modifiers is None:
            modifiers = {}
        new_audit_settings = copy.deepcopy(audit_settings or self.audit_default_settings)
        # This is a hack. The test framework uses "table" as "not none".
        # Appropriate audit mode should be passed from the test itself, and not set here.
        # This converts "table" to its own audit mode, or keeps "none" as is.
        if "audit" in new_audit_settings and new_audit_settings["audit"] == "table":
            new_audit_settings["audit"] = self.audit_mode()
        new_audit_settings["audit_unix_socket_path"] = self.socket_path
        for key in modifiers:
            new_audit_settings[key] = modifiers[key]
        return new_audit_settings

    @override
    def get_audit_log_dict(self, session, consistency_level):
        """_summary_
        returns a dictionary mapping audit mode name to a sorted list of audit log,
        the logs are sorted by the event times (time-uuid) with the node as tie breaker.
        """
        lines = self.unix_socket_listener.get_lines()
        entries = []
        for idx, line in enumerate(lines):
            entries.append(self.line_to_row(line, idx))
        return { self.audit_mode(): entries }

    def line_to_row(self, line, idx):
        metadata, data = line.split(": ", 1)
        data = "".join(data.splitlines()) # Remove newlines
        fields = ["node", "category", "cl", "error", "keyspace", "query", "client_ip", "table", "username"]
        regexp = ", ".join(f"{field}=\"(?P<{field}>.*)\"" for field in fields)
        match = re.match(regexp, data)

        # Arbitrary date because we don't really check the field. We just need to fill it with something
        # and make sure it doesn't change during the test (e.g. when the test is running at 23:59:59)
        date = datetime.datetime(2000, 1, 1, 0, 0)

        node = match.group("node").split(":")[0]
        statement = match.group("query").replace("\\", "") 
        source = match.group("client_ip").split(":")[0]
        event_time = uuid.UUID(int=idx)
        t = self.named_tuple_factory(date, node, event_time, match.group("category"), match.group("cl"), match.group("error") == "true", match.group("keyspace"), statement, source, match.group("table"), match.group("username"))
        return t


class AuditBackendComposite(AuditBackend):
    audit_default_settings = {"audit": "table,syslog", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}

    backends: list[AuditBackend]

    def __init__(self, socket_path: str | None = None):
        super().__init__(socket_path=socket_path)
        self.backends = [AuditBackendTable(), AuditBackendSyslog(socket_path=socket_path)]

    def __exit__(self, exc_type, exc_val, exc_tb):
        for backend in reversed(self.backends):
            try:
                backend.__exit__(exc_type, exc_val, exc_tb)
            except Exception as e:
                logger.error(f"Error while exiting backend: {e}")

    @override
    def audit_mode(self) -> str:
        return ",".join([backend.audit_mode() for backend in self.backends])

    @override
    def clear_audit_logs(self, session: Session | None = None) -> None:
        for backend in self.backends:
            backend.clear_audit_logs(session)

    def update_audit_settings(self, audit_settings, modifiers=None):
        if modifiers is None:
            modifiers = {}
        new_audit_settings = copy.deepcopy(audit_settings or self.audit_default_settings)
        # This is a hack. The test framework uses "table" as "not none".
        # The syslog backend may change "table" to "syslog" before this is called.
        # Appropriate audit mode should be passed from the test itself, and not set here.
        # This converts "table" or "syslog" to its own audit mode, or keeps "none" as is.
        for backend in self.backends:
            new_audit_settings = backend.update_audit_settings(new_audit_settings)
        if "audit" in new_audit_settings and (new_audit_settings["audit"] == "table" or new_audit_settings["audit"] == "syslog"):
            new_audit_settings["audit"] = self.audit_mode()
        for key in modifiers:
            new_audit_settings[key] = modifiers[key]
        return new_audit_settings

    @override
    def get_audit_log_dict(self, session, consistency_level):
        """_summary_
        returns a dictionary mapping audit mode name to a sorted list of audit log,
        the logs are sorted by the event times (time-uuid) with the node as tie breaker.
        """
        rows_dict = dict[str, list[AuditEntry]]()
        for backend in self.backends:
            backend_rows_dict = backend.get_audit_log_dict(session, consistency_level)
            for mode, backend_rows in backend_rows_dict.items():
                assert mode not in rows_dict
                rows_dict[mode] = backend_rows
        return rows_dict


@pytest.mark.single_node
class TestCQLAudit(AuditTester):
    """
    Make sure CQL statements are audited
    """

    AUDIT_LOG_QUERY = "SELECT * FROM audit.audit_log"

    def __init__(self, manager: ManagerClient, helper: AuditBackend | None = None):
        super().__init__()
        self.manager = manager
        self.server_addresses: list[str] = []
        self.helper: AuditBackend | None = helper

    def deduplicate_audit_entries(self, entries_dict):
        """
        Returns a dictionary mapping audit mode name to a list of audit entries with duplicate entries removed.
        """
        deduplicated_entries_dict = dict[str, list[AuditEntry]]()

        for mode, entries in entries_dict.items():
            unique = set()
            deduplicated_entries = list[AuditEntry]()
            for entry in entries:
                fields_subset = (entry.node, entry.category, entry.consistency, entry.error, entry.keyspace_name, entry.operation, entry.source, entry.table_name, entry.username)

                if fields_subset in unique:
                    continue

                unique.add(fields_subset)
                deduplicated_entries.append(entry)
            deduplicated_entries_dict[mode] = deduplicated_entries

        return deduplicated_entries_dict

    def get_audit_log_dict(self, session):
        """Returns a dictionary mapping audit mode name to a sorted list of audit log.

        The logs are sorted by the event times (time-uuid) with the node as tie breaker.
        """
        consistency_level = ConsistencyLevel.QUORUM if len(self.server_addresses) > 1 else ConsistencyLevel.ONE
        log_dict = self.helper.get_audit_log_dict(session, consistency_level)
        logger.debug(f"get_audit_log_dict: {log_dict}")
        return log_dict

    # This assert is added just in order to still fail the test if the order of columns is changed, this is an implied assumption
    def assert_audit_row_fields(self, row):
        expected_fields = ["date", "node", "event_time", "category", "consistency", "error", "keyspace_name", "operation", "source", "table_name", "username"]
        assert list(row._fields) == expected_fields

    def assert_audit_row_eq(  # noqa: PLR0913
        self,
        row,
        category,
        statement,
        table="",
        ks="ks",
        user="anonymous",
        cl="ONE",
        error=False,
    ):
        self.assert_audit_row_fields(row)
        assert row.node in self.server_addresses
        assert row.category == category
        assert row.consistency == cl
        assert row.error == error
        assert row.keyspace_name == ks
        assert row.operation == statement
        assert row.source == "127.0.0.1"
        assert row.table_name == table
        assert row.username == user

    def get_audit_entries_count_dict(self, session) -> dict[str, int]:
        """_summary_
        returns a dictionary mapping audit mode name to the count of audit log entries for that mode.
        """
        reg_dict = self.get_audit_log_dict(session)
        reg_dict = self.filter_out_noise(reg_dict, filter_out_all_auth=True, filter_out_use=True)
        for mode, reg_list in reg_dict.items():
            logger.debug(f"Printing audit {mode} content:")
            for row in reg_list:
                logger.debug("  %s", row)
        return { mode: len(reg_list) for mode, reg_list in reg_dict.items() }

    @staticmethod
    def token_in_range(token, start_token, end_token):
        if end_token < start_token:
            # the case where the end_token circles back to negative values
            # {
            #     "start_token": "9216493101126045011",
            #     "end_token": "-9213846192999778351",
            #         "endpoints": [
            #         "172.19.0.6",
            #         "172.19.0.2",
            #         "172.19.0.3"
            #     ]
            # }
            return token >= start_token or token <= end_token
        else:
            return token >= start_token and token <= end_token

    async def get_partitions_for_token(self, ks, table, token):
        partitions = set()
        for node_ip in self.server_addresses:
            ranges = await self.manager.api.describe_ring(node_ip, ks, table)
            for token_range in ranges:
                start_token = int(token_range["start_token"])
                end_token = int(token_range["end_token"])
                if self.token_in_range(token, start_token, end_token):
                    logger.info(f"Found token {token} in token_range: {token_range}")
                    for e in token_range["endpoints"]:
                        partitions.add(e)
        return partitions

    async def get_audit_partitions_for_operation(self, session, operation):
        """Returns a list of IP addresses for the audit partitions of a given operation."""
        logger.info(f"get_audit_partitions_for_operation: {operation}")
        rows = rows_to_list(session.execute(f"SELECT token(date, node) FROM audit.audit_log WHERE operation = %s ALLOW FILTERING", [operation]))
        if len(rows) == 0:
            return None
        token = rows[0][0]

        partitions = await self.get_partitions_for_token("audit", "audit_log", token)

        assert len(partitions) > 0
        return list(partitions)

    @contextmanager
    def assert_exactly_n_audit_entries_were_added(self, session: Session, expected_entries: int):
        counts_before = self.get_audit_entries_count_dict(session)
        yield
        counts_after = self.get_audit_entries_count_dict(session)
        assert set(counts_before.keys()) == set(counts_after.keys()), f"audit modes changed (before: {list(counts_before.keys())} after: {list(counts_after.keys())})"
        for mode, count_before in counts_before.items():
            count_after = counts_after[mode]
            assert count_after == count_before + expected_entries, f"Expected {expected_entries} new audit entries, but got {count_after - count_before} new entries"

    @contextmanager
    def assert_no_audit_entries_were_added(self, session):
        counts_before = self.get_audit_entries_count_dict(session)
        yield
        counts_after = self.get_audit_entries_count_dict(session)
        assert set(counts_before.keys()) == set(counts_after.keys()), f"audit modes changed (before: {list(counts_before.keys())} after: {list(counts_after.keys())})"
        for mode, count_before in counts_before.items():
            count_after = counts_after[mode]
            assert count_before == count_after, f"audit entries count changed (before: {count_before} after: {count_after})"

    def execute_and_validate_audit_entry(  # noqa: PLR0913
        self,
        session: Session,
        query: Any,
        category: str,
        audit_settings: dict[str, str] = AuditTester.audit_default_settings,
        table: str = "",
        ks: str = "ks",
        cl: str = "ONE",
        user: str = "anonymous",
        expected_error: Any = None,
        bound_values: list[Any] | None = None,
        expect_new_audit_entry: bool = True,
        expected_operation: str | None = None,
        session_for_audit_entry_validation: Session | None = None,
    ):
        """
        Execute a query and validate that an audit entry was added to the audit
        log table. Use the audit_settings parameter in combination with category
        to determine if the audit entry should be added or not. If the audit
        entry is expected, validate that the audit entry's content is as
        expected.
        """

        # In some cases, provided session does not have access to the audit
        # table. In that case, session_for_audit_entry_validation should be
        # provided.
        if session_for_audit_entry_validation is None:
            session_for_audit_entry_validation = session

        if category in audit_settings["audit_categories"].split(",") and expect_new_audit_entry:
            operation = query if expected_operation is None else expected_operation
            error = expected_error is not None

            expected_entries = [AuditEntry(category, cl, error, ks, operation, table, user)]
        else:
            expected_entries = []

        with self.assert_entries_were_added(session_for_audit_entry_validation, expected_entries):
            if expected_error is None:
                res = session.execute(query, bound_values)
            else:
                assert_invalid(session, query, expected=expected_error)
                res = None

        return res

    # Filter out queries that can appear in random moments of the tests,
    # such as LOGINs and USE statements.
    def filter_out_noise(self, rows_dict, filter_out_all_auth=False, filter_out_cassandra_auth=False, filter_out_use=False) -> dict[str, list[AuditEntry]]:
        for mode, rows in rows_dict.items():
            if filter_out_all_auth:
                rows = [row for row in rows if row.category != "AUTH"]
            if filter_out_cassandra_auth:
                rows = [row for row in rows if not (row.category == "AUTH" and row.username == "cassandra")]
            if filter_out_use:
                rows = [row for row in rows if "USE " not in row.operation]
            rows_dict[mode] = rows
        return rows_dict

    @contextmanager
    def assert_entries_were_added(self, session: Session, expected_entries: list[AuditEntry], merge_duplicate_rows: bool = True, filter_out_cassandra_auth: bool = False):
        # Get audit entries before executing the query, to later compare with
        # audit entries after executing the query.
        set_of_rows_before_dict = dict[str, set[AuditEntry]]()
        rows_before_dict = self.get_audit_log_dict(session)
        for mode, rows_before in rows_before_dict.items():
            set_of_rows_before = set(rows_before)
            assert len(set_of_rows_before) == len(rows_before), f"audit {mode} contains duplicate rows: {rows_before}"
            set_of_rows_before_dict[mode] = set_of_rows_before
        yield

        new_rows_dict = dict[str, list[AuditEntry]]()
        def is_number_of_new_rows_correct():
            rows_after_dict = self.get_audit_log_dict(session)
            set_of_rows_after_dict = dict[str, set[AuditEntry]]()
            for mode, rows_after in rows_after_dict.items():
                set_of_rows_after = set(rows_after)
                assert len(set_of_rows_after) == len(rows_after), f"audit {mode} contains duplicate rows: {rows_after}"
                set_of_rows_after_dict[mode] = set_of_rows_after

            nonlocal new_rows_dict
            for mode, rows_after in rows_after_dict.items():
                rows_before = rows_before_dict[mode]
                new_rows_dict[mode] = rows_after[len(rows_before) :]
                assert set(new_rows_dict[mode]) == set_of_rows_after_dict[mode] - set_of_rows_before_dict[mode], f"new rows are not the last rows in the audit table: rows_after={rows_after}, set_of_rows_after_dict[{mode}]={set_of_rows_after_dict[mode]}, set_of_rows_before_dict[{mode}]={set_of_rows_before_dict[mode]}"

            if merge_duplicate_rows:
                new_rows_dict = self.deduplicate_audit_entries(new_rows_dict)

            auth_not_expected = (len([entry for entry in expected_entries if entry.category == "AUTH"]) == 0)
            use_not_expected = (len([entry for entry in expected_entries if "USE " in entry.statement]) == 0)

            new_rows_dict = self.filter_out_noise(
                new_rows_dict,
                filter_out_all_auth=auth_not_expected,
                filter_out_cassandra_auth=filter_out_cassandra_auth,
                filter_out_use=use_not_expected
            )

            for new_rows in new_rows_dict.values():
                assert len(new_rows) <= len(expected_entries)
                if len(new_rows) != len(expected_entries):
                    return False

            return True

        wait_for(is_number_of_new_rows_correct, timeout=60)
        for mode, new_rows in new_rows_dict.items():
            sorted_new_rows = sorted(new_rows, key=lambda row: (row.node, row.category, row.consistency, row.error, row.keyspace_name, row.operation, row.source, row.table_name, row.username))
            assert len(sorted_new_rows) == len(expected_entries)
            for row, entry in zip(sorted_new_rows, sorted(expected_entries)):
                self.assert_audit_row_eq(row, entry.category, entry.statement, entry.table, entry.ks, entry.user, entry.cl, entry.error)

    async def verify_keyspace(self, audit_settings=None, helper=None):
        """
        CREATE KEYSPACE, USE KEYSPACE, ALTER KEYSPACE, DROP KEYSPACE statements
        """
        session = await self.prepare(create_keyspace=False, audit_settings=audit_settings, helper=helper)

        def execute_and_validate_audit_entry(query, category, **kwargs):
            return self.execute_and_validate_audit_entry(session, query, category, audit_settings, **kwargs)

        execute_and_validate_audit_entry(
            "CREATE KEYSPACE ks WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true",
            category="DDL",
        )
        execute_and_validate_audit_entry(
            'USE "ks"',
            category="DML",
        )
        execute_and_validate_audit_entry(
            "ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false",
            category="DDL",
        )
        execute_and_validate_audit_entry(
            "DROP KEYSPACE ks",
            category="DDL",
        )

        # Test that the audit entries are not added if the keyspace is not
        # specified in the audit_keyspaces setting.
        keyspaces = audit_settings["audit_keyspaces"].split(",") if "audit_keyspaces" in audit_settings else []
        assert "ks2" not in keyspaces
        query_sequence = [
            "CREATE KEYSPACE ks2 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true",
            'USE "ks2"',
            "ALTER KEYSPACE ks2 WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false",
            "DROP KEYSPACE ks2",
        ]

        with self.assert_no_audit_entries_were_added(session):
            for query in query_sequence:
                session.execute(query)

    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
    async def test_using_non_existent_keyspace(self, helper_class):
        """
        Test tha using a non-existent keyspace generates an audit entry with an
        error field set to True.
        """

        with helper_class() as helper:
            session = await self.prepare(create_keyspace=False, helper=helper)

            self.execute_and_validate_audit_entry(
                session,
                'USE "ks"',  # ks doesn't exist because create_keyspace=False in prepare
                category="DML",
                expected_error=InvalidRequest,
            )

    async def verify_table(self, audit_settings=AuditTester.audit_default_settings, helper=None, table_prefix="test", overwrite_audit_tables=False):
        """
        CREATE TABLE, ALTER TABLE, TRUNCATE TABLE, DROP TABLE statements
        """
        first_table = f"{table_prefix}_1_{uuid.uuid4().hex[:4]}"
        second_table = f"{table_prefix}_2_{uuid.uuid4().hex[:4]}"

        if helper is None:
            helper = AuditBackendTable()

        if overwrite_audit_tables:
            audit_settings["audit_tables"] = f"ks.{first_table}, ks.{second_table}"

        session = await self.prepare(audit_settings=audit_settings, helper=helper, enable_compact_storage=True)

        def execute_and_validate_audit_entry(query, category, **kwargs):
            return self.execute_and_validate_audit_entry(session, query, category, audit_settings, **kwargs)

        execute_and_validate_audit_entry(
            f"CREATE TABLE {first_table} (k int PRIMARY KEY, v1 int)",
            category="DDL",
            table=first_table,
        )
        execute_and_validate_audit_entry(
            f"CREATE TABLE {second_table} (k int, c1 int, v1 int, PRIMARY KEY (k, c1)) WITH COMPACT STORAGE",
            category="DDL",
            table=second_table,
        )
        execute_and_validate_audit_entry(
            f"ALTER TABLE {first_table} ADD v2 int",
            category="DDL",
            table=first_table,
        )

        for table in [first_table, second_table]:
            for i in range(10):
                if table == first_table:
                    columns = "(k, v1, v2)"
                else:
                    columns = "(k, c1, v1)"

                execute_and_validate_audit_entry(
                    f"INSERT INTO {table} {columns} VALUES ({i}, {i}, {i})",
                    category="DML",
                    table=f"{table}",
                )

            res = execute_and_validate_audit_entry(
                f"SELECT * FROM {table}",
                category="QUERY",
                table=f"{table}",
            )
            assert sorted(rows_to_list(res)) == [[i, i, i] for i in range(10)], res

            execute_and_validate_audit_entry(
                f"TRUNCATE {table}",
                category="DML",
                table=f"{table}",
            )

            res = execute_and_validate_audit_entry(
                f"SELECT * FROM {table}",
                category="QUERY",
                table=f"{table}",
            )
            assert rows_to_list(res) == [], res

            execute_and_validate_audit_entry(
                f"DROP TABLE {table}",
                category="DDL",
                table=f"{table}",
            )

            execute_and_validate_audit_entry(
                f"SELECT * FROM {table}",
                category="QUERY",
                table=f"{table}",
                expected_error=InvalidRequest,
                expect_new_audit_entry=False,
            )

        # Test that the audit entries are not added if the keyspace is not
        # specified in the audit_keyspaces setting.
        keyspaces = audit_settings["audit_keyspaces"].split(",") if "audit_keyspaces" in audit_settings else []
        assert "ks2" not in keyspaces
        query_sequence = [
            "CREATE KEYSPACE ks2 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true",
            f"CREATE TABLE ks2.{first_table} (k int PRIMARY KEY, v1 int)",
            f"ALTER TABLE ks2.{first_table} ADD v2 int",
            f"INSERT INTO ks2.{first_table} (k, v1, v2) VALUES (1, 1, 1)",
            f"SELECT * FROM ks2.{first_table}",
            f"TRUNCATE ks2.{first_table}",
            f"DROP TABLE ks2.{first_table}",
            "DROP KEYSPACE ks2",
        ]

        with self.assert_no_audit_entries_were_added(session):
            for query in query_sequence:
                session.execute(query)

    async def test_audit_keyspace(self, helper_class):
        with helper_class(socket_path=_syslog_socket_path) as helper:
            await self.verify_keyspace(audit_settings=AuditTester.audit_default_settings, helper=helper)

    async def test_audit_keyspace_extra_parameter(self, helper_class):
        with helper_class(socket_path=_syslog_socket_path) as helper:
            await self.verify_keyspace(audit_settings={"audit": "table", "audit_categories": "ADMIN,AUTH,DML,DDL,DCL", "audit_keyspaces": "ks", "extra_parameter": "new"}, helper=helper)

    async def test_audit_keyspace_many_ks(self, helper_class):
        with helper_class() as helper:
            await self.verify_keyspace(audit_settings={"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "a,b,c,ks"}, helper=helper)

    async def test_audit_keyspace_table_not_exists(self, helper_class):
        with helper_class() as helper:
            await self.verify_keyspace(audit_settings={"audit": "table", "audit_categories": "DML,DDL", "audit_keyspaces": "ks", "audit_tables": "ks.fake"}, helper=helper)

    async def test_audit_type_none(self):
        """
        'audit': None
         CREATE KEYSPACE, USE KEYSPACE, ALTER KEYSPACE, DROP KEYSPACE statements
         check audit KS not created
        """

        audit_settings = {"audit": None, "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}

        session = await self.prepare(create_keyspace=False, audit_settings=audit_settings)

        session.execute("CREATE KEYSPACE ks WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true")

        session.execute("USE ks")

        session.execute("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false")

        session.execute("DROP KEYSPACE ks")
        assert_invalid(session, "use audit;", expected=InvalidRequest)

    async def test_audit_type_invalid(self):
        """
        'audit': invalid
         check node not started
        """
        audit_settings = {"audit": "invalid", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}
        expected_error = r"Startup failed: audit::audit_exception \(Bad configuration: invalid 'audit': invalid\)"

        servers = await self.manager.running_servers()
        if not servers:
            await self.manager.server_add(config=audit_settings, expected_error=expected_error)
        else:
            srv = servers[0]
            await self.manager.server_stop_gracefully(srv.server_id)
            await self.manager.server_update_config(srv.server_id, config_options=audit_settings)
            await self.manager.server_start(srv.server_id, expected_error=expected_error)

    async def test_composite_audit_type_invalid(self):
        """
        'audit': table,syslog,invalid
         check node not started
        """
        audit_settings = {"audit": "table,syslog,invalid", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}
        expected_error = r"Startup failed: audit::audit_exception \(Bad configuration: invalid 'audit': invalid\)"

        servers = await self.manager.running_servers()
        if not servers:
            await self.manager.server_add(config=audit_settings, expected_error=expected_error)
        else:
            srv = servers[0]
            await self.manager.server_stop_gracefully(srv.server_id)
            await self.manager.server_update_config(srv.server_id, config_options=audit_settings)
            await self.manager.server_start(srv.server_id, expected_error=expected_error)

    # TODO: verify that the syslog file doesn't exist
    async def test_audit_empty_settings(self):
        """
        'audit': none
         check node started, ks audit not created
        """
        session = await self.prepare(create_keyspace=False, audit_settings={"audit": "none"})
        assert_invalid(session, "use audit;", expected=InvalidRequest)

    async def test_composite_audit_empty_settings(self):
        """
        'audit': table,syslog,none
         check node started, ks audit not created
        """
        session = await self.prepare(create_keyspace=False, audit_settings={"audit": "table,syslog,none"})
        assert_invalid(session, "use audit;", expected=InvalidRequest)

    async def test_audit_audit_ks(self):
        """
        'audit_keyspaces': 'audit'
        check node started, ks audit created
        """
        audit_settings = {"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "audit"}
        session = await self.prepare(create_keyspace=False, audit_settings=audit_settings)

        self.execute_and_validate_audit_entry(session, query=self.AUDIT_LOG_QUERY, category="QUERY", ks="audit", table="audit_log", audit_settings=audit_settings)

    async def test_audit_categories_invalid(self):
        """
        'audit_categories': invalid
        check node not started
        """
        audit_settings = {"audit": "table", "audit_categories": "INVALID", "audit_keyspaces": "ks"}
        expected_error = r"Startup failed: audit::audit_exception \(Bad configuration: invalid 'audit_categories': INVALID\)"

        servers = await self.manager.running_servers()
        if not servers:
            await self.manager.server_add(config=audit_settings, expected_error=expected_error)
        else:
            srv = servers[0]
            await self.manager.server_stop_gracefully(srv.server_id)
            await self.manager.server_update_config(srv.server_id, config_options=audit_settings)
            await self.manager.server_start(srv.server_id, expected_error=expected_error)

    # compact storage is current required for all tests that call verify_table
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    async def test_audit_table(self):
        await self.verify_table(audit_settings=AuditTester.audit_default_settings, table_prefix="test_audit_table")

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    async def test_audit_table_extra_parameter(self):
        await self.verify_table(audit_settings={"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks", "extra_parameter": "new"}, table_prefix="test_audit_table_extra_parameter")

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    async def test_audit_table_audit_keyspaces_empty(self):
        await self.verify_table(audit_settings={"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL"}, table_prefix="test_audit_table_audit_keyspaces_empty", overwrite_audit_tables=True)

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    async def test_audit_table_no_ks(self):
        await self.verify_table(audit_settings={"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL"}, table_prefix="test_audit_table_no_ks", overwrite_audit_tables=True)

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    async def test_audit_categories_part1(self):
        await self.verify_table(audit_settings={"audit": "table", "audit_categories": "AUTH,QUERY,DDL"}, table_prefix="test_audit_categories_part1", overwrite_audit_tables=True)

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    # @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
    async def test_audit_categories_part2(self, helper_class):
        with helper_class() as helper:
            await self.verify_table(audit_settings={"audit": "table", "audit_categories": "DDL, ADMIN,AUTH,DCL", "audit_keyspaces": "ks"}, helper=helper, table_prefix="test_audit_categories_part2")

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    # @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
    async def test_audit_categories_part3(self, helper_class):
        with helper_class() as helper:
            await self.verify_table(audit_settings={"audit": "table", "audit_categories": "DDL, ADMIN,AUTH", "audit_keyspaces": "ks"}, helper=helper, table_prefix="test_audit_categories_part3")

    PasswordMaskingCase = namedtuple("PasswordMaskingCase", ["name", "password", "new_password"])

    # @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
    async def test_user_password_masking(self, helper_class):
        """
        CREATE USER, ALTER USER, DROP USER statements
        """
        with helper_class() as helper:
            session = await self.prepare(user="cassandra", password="cassandra", helper=helper)

            def execute_and_validate_audit_entry(query, category, **kwargs):
                return self.execute_and_validate_audit_entry(session, query, category, self.audit_default_settings, **kwargs, user="cassandra", ks="")

            tests = [self.PasswordMaskingCase("user1", "secret", "Secret^%$#@!"), self.PasswordMaskingCase("user2", "", "")]
            for username, password, new_password in tests:
                execute_and_validate_audit_entry(
                    f"CREATE USER {username} WITH PASSWORD '{password}'",
                    category="DCL",
                    expected_operation=f"CREATE USER {username} WITH PASSWORD '***'",
                )
                execute_and_validate_audit_entry(
                    f"ALTER USER {username} WITH PASSWORD '{new_password}'",
                    category="DCL",
                    expected_operation=f"ALTER USER {username} WITH PASSWORD '***'",
                )
                execute_and_validate_audit_entry(
                    f"DROP USER {username}",
                    category="DCL",
                )

    async def test_negative_audit_records_auth(self):
        """
        Test that failed AUTH attempts are audited.
        """
        session = await self.prepare(user="cassandra", password="cassandra")
        expected_entry = AuditEntry(category="AUTH", statement="LOGIN", table="", ks="", user="wrong_user", cl="", error=True)
        with self.assert_entries_were_added(session, [expected_entry], filter_out_cassandra_auth=True):
            try:
                servers = await self.manager.running_servers()
                bad_auth = PlainTextAuthProvider(username="wrong_user", password="wrong_password")
                bad_session = await self.manager.get_cql_exclusive(servers[0], auth_provider=bad_auth)
                pytest.fail()
            except NoHostAvailable as e:
                errors = e.errors.values()
                assert len(errors) == 1
                error = next(iter(errors))
                assert isinstance(error, AuthenticationFailed)

    async def test_negative_audit_records_admin(self):
        """
        Test that failed ADMIN statements are audited.
        """
        session = await self.prepare(user="cassandra", password="cassandra")
        session.execute("CREATE ROLE test_role")

        stmt = "ATTACH SERVICE_LEVEL test_service_level TO test_role"

        expected_entry = AuditEntry(category="ADMIN", table="", ks="", user="cassandra", cl="ONE", error=True, statement=stmt)

        with self.assert_entries_were_added(session, [expected_entry]):
            assert_invalid(session, stmt, expected=InvalidRequest)

    async def test_negative_audit_records_ddl(self):
        """
        Test that failed DDL statements are audited.
        """
        session = await self.prepare(user="cassandra", password="cassandra", rf=3)

        stmt = "CREATE KEYSPACE ks WITH replication = { 'class':'NetworkTopologyStrategy', 'replication_factor': 1 }"

        expected_entry = AuditEntry(category="DDL", table="", ks="ks", user="cassandra", cl="ONE", error=True, statement=stmt)

        with self.assert_entries_were_added(session, [expected_entry]):
            assert_invalid(session, stmt, expected=AlreadyExists)

    async def test_negative_audit_records_dml(self):
        """
        Test that failed DML statements are audited.
        """
        audit_settings = {"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}
        session = await self.prepare(user="cassandra", password="cassandra", audit_settings=audit_settings)

        session.execute("CREATE TABLE test1 (k int PRIMARY KEY, v1 int, v2 int)")

        stmt = SimpleStatement("INSERT INTO ks.test1 (k, v1, v2) VALUES (1, 1, 1)", consistency_level=ConsistencyLevel.TWO)

        expected_entry = AuditEntry(category="DML", table="test1", ks="ks", user="cassandra", cl="TWO", error=True, statement=stmt.query_string)

        # The driver wraps Unavailable in NoHostAvailable when all hosts fail,
        # so we catch NoHostAvailable and verify the inner error.
        with self.assert_entries_were_added(session, [expected_entry]):
            try:
                session.execute(stmt)
                pytest.fail("Expected Unavailable error")
            except NoHostAvailable as e:
                errors = list(e.errors.values())
                assert len(errors) == 1
                assert isinstance(errors[0], Unavailable)

    async def test_negative_audit_records_dcl(self):
        """
        Test that failed DCL statements are audited.
        """
        session = await self.prepare(user="cassandra", password="cassandra")
        stmt = "GRANT SELECT ON ALL KEYSPACES TO test_role"

        expected_entry = AuditEntry(category="DCL", table="", ks="", user="cassandra", cl="ONE", error=True, statement=stmt)

        with self.assert_entries_were_added(session, [expected_entry]):
            assert_invalid(session, stmt, expected=InvalidRequest)

    async def test_negative_audit_records_query(self):
        """
        Test that failed QUERY statements are audited.
        """
        audit_settings = {"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}
        session = await self.prepare(audit_settings=audit_settings)

        session.execute("CREATE TABLE test1 (k int PRIMARY KEY, v int)")

        stmt = SimpleStatement("SELECT * FROM ks.test1", consistency_level=ConsistencyLevel.TWO)

        expected_entry = AuditEntry(category="QUERY", table="test1", ks="ks", user="anonymous", cl="TWO", error=True, statement=stmt.query_string)

        # The driver wraps Unavailable in NoHostAvailable when all hosts fail,
        # so we catch NoHostAvailable and verify the inner error.
        with self.assert_entries_were_added(session, [expected_entry]):
            try:
                session.execute(stmt)
                pytest.fail("Expected Unavailable error")
            except NoHostAvailable as e:
                errors = list(e.errors.values())
                assert len(errors) == 1
                assert isinstance(errors[0], Unavailable)

    # @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
    async def test_role_password_masking(self, helper_class):
        """
        CREATE ROLE, ALTER ROLE, DROP ROLE statements
        """

        with helper_class() as helper:
            session = await self.prepare(user="cassandra", password="cassandra", helper=helper)

            def execute_and_validate_audit_entry(query, category, **kwargs):
                return self.execute_and_validate_audit_entry(session, query, category, self.audit_default_settings, **kwargs, user="cassandra", ks="")

            tests = [self.PasswordMaskingCase("role1", "Secret!@#$", "Secret^%$#@!"), self.PasswordMaskingCase("role2", "", "")]
            for role_name, password, new_password in tests:
                execute_and_validate_audit_entry(
                    f"CREATE ROLE {role_name} WITH PASSWORD = '{password}'",
                    category="DCL",
                    expected_operation=f"CREATE ROLE {role_name} WITH PASSWORD = '***'",
                )
                execute_and_validate_audit_entry(
                    f"ALTER ROLE {role_name} WITH PASSWORD = '{new_password}'",
                    category="DCL",
                    expected_operation=f"ALTER ROLE {role_name} WITH PASSWORD = '***'",
                )
                execute_and_validate_audit_entry(
                    f"DROP ROLE {role_name}",
                    category="DCL",
                )

    async def test_login(self):
        """
        USER LOGIN
        """
        session = await self.prepare(user="cassandra", password="cassandra", create_keyspace=False)
        session.execute("CREATE USER test WITH PASSWORD 'test'")

        expected_audit_entries = [AuditEntry(category="AUTH", statement="LOGIN", user="test", table="", ks="", cl="", error=False)]

        with self.assert_entries_were_added(session, expected_audit_entries, filter_out_cassandra_auth=True):
            servers = await self.manager.running_servers()
            test_auth = PlainTextAuthProvider(username="test", password="test")
            await self.manager.get_cql_exclusive(servers[0], auth_provider=test_auth)

    async def test_cassandra_login(self):
        """
        Test user login to default (cassandra) user
        """
        session = await self.prepare(user="cassandra", password="cassandra", create_keyspace=False)
        expected_audit_entries = [AuditEntry(category="AUTH", statement="LOGIN", user="cassandra", table="", ks="", cl="", error=False)]

        with self.assert_entries_were_added(session, expected_audit_entries, filter_out_cassandra_auth=False):
            servers = await self.manager.running_servers()
            test_auth = PlainTextAuthProvider(username="cassandra", password="cassandra")
            await self.manager.get_cql_exclusive(servers[0], auth_provider=test_auth)

    async def test_categories(self):
        """
        Test filtering audit categories
        """
        session = await self.prepare(audit_settings={"audit": "table", "audit_categories": "DML", "audit_keyspaces": "ks"})

        def execute_and_validate_audit_entry(query, category, **kwargs):
            return self.execute_and_validate_audit_entry(session, query, category, self.audit_default_settings, **kwargs)

        with self.assert_no_audit_entries_were_added(session):
            session.execute("CREATE TABLE test1 (k int PRIMARY KEY, v1 int)")
            session.execute("ALTER TABLE test1 ADD v2 int")

        for i in range(10):
            execute_and_validate_audit_entry(
                f"INSERT INTO test1 (k, v1, v2) VALUES ({i}, {i}, {i})",
                category="DML",
                table="test1",
            )

        with self.assert_no_audit_entries_were_added(session):
            res = sorted(session.execute("SELECT * FROM test1"))
            assert rows_to_list(res) == [[i, i, i] for i in range(10)], res

        execute_and_validate_audit_entry(
            "TRUNCATE test1",
            category="DML",
            table="test1",
        )

        with self.assert_no_audit_entries_were_added(session):
            res = session.execute("SELECT * FROM test1")
            assert rows_to_list(res) == [], res

            session.execute("DROP TABLE test1")

    def _get_attempt_count(self, session: Session, *, execution_profile=EXEC_PROFILE_DEFAULT, consistency_level: ConsistencyLevel = ConsistencyLevel.ONE) -> int:
        # FlakyRetryPolicy (dtest) has max_retries=5; the default RetryPolicy
        # rethrows immediately on Unavailable, so 0 retries → 1 attempt.
        cl_profile = session.execution_profile_clone_update(execution_profile, consistency_level=consistency_level)
        policy = cl_profile.retry_policy
        retries = getattr(policy, "max_retries", 0)
        return 1 + retries

    async def _test_insert_failure_doesnt_report_success_assign_nodes(self, session: Session):
        servers = await self.manager.running_servers()
        assert len(servers) == 7

        address_to_server = {srv.ip_addr: srv for srv in servers}
        node_to_audit_nodes: dict[int, set[str]] = {}

        for index, srv in enumerate(servers):
            conn = await self.manager.get_cql_exclusive(srv)
            stmt = SimpleStatement("INSERT INTO ks.test1 (k, v1) VALUES (1000, 1000)", consistency_level=ConsistencyLevel.THREE)
            conn.execute(stmt)
            audit_node_ips = await self.get_audit_partitions_for_operation(session, stmt.query_string)
            node_to_audit_nodes[index] = set(audit_node_ips)

        all_addresses = set(srv.ip_addr for srv in servers)
        # the number here is arbitrary
        for i in range(256):
            stmt = SimpleStatement(f"INSERT INTO ks.test1 (k, v1) VALUES ({i}, 1337)", consistency_level=ConsistencyLevel.THREE)
            session.execute(stmt)
            attempt_count = self._get_attempt_count(session, consistency_level=ConsistencyLevel.THREE)

            token = rows_to_list(session.execute(f"SELECT token(k) FROM ks.test1 WHERE k = {i}"))[0][0]

            partitions = await self.get_partitions_for_token("ks", "test1", token)

            for index, audit_nodes in node_to_audit_nodes.items():
                common = audit_nodes & partitions
                if not common:
                    insert_addr = all_addresses - partitions - audit_nodes
                    if len(all_addresses) != 7 or len(partitions) != 3 or len(audit_nodes) != 3 or len(insert_addr) != 1:
                        raise pytest.skip("Failed to assign nodes for insert failure test")
                    audit_partition_servers = [address_to_server[addr] for addr in audit_nodes]
                    insert_server = address_to_server[insert_addr.pop()]
                    kill_server = address_to_server[partitions.pop()]
                    return audit_partition_servers, insert_server, kill_server, stmt.query_string, attempt_count

        return [], None, None, None, None

    async def test_insert_failure_doesnt_report_success(self):
        """
        Test that if an insert fails, the audit log doesn't report the insert
        as successful.

        The test works by creating a table with RF=3, and then stopping one of
        the nodes. Then, an insert is executed with CL=THREE. This insert will
        fail, since the node that is stopped is a replica for the inserted
        partition. The test verifies that the audit log doesn't report the
        insert as successful and reports unsuccessful inserts as expected.
        """
        # 7 nodes: rack1=3, rack2=2, rack3=2
        topology = [{"dc": "dc1", "rack": "rack1"}] * 3 + [{"dc": "dc1", "rack": "rack2"}] * 2 + [{"dc": "dc1", "rack": "rack3"}] * 2
        session: Session = await self.prepare(rf=3, property_file=topology)

        servers = await self.manager.running_servers()
        conn = await self.manager.get_cql_exclusive(servers[0])
        stmt = SimpleStatement("CREATE TABLE ks.test1 (k int PRIMARY KEY, v1 int)")
        with self.assert_exactly_n_audit_entries_were_added(session, 1):
            conn.execute(stmt)

        audit_partition_servers, insert_server, server_to_stop, query_to_fail, query_fail_count = await self._test_insert_failure_doesnt_report_success_assign_nodes(session=session)

        # TODO: remove the loop when scylladb#24473 is fixed
        # We call get_host_id only to cache host_id
        for srv in audit_partition_servers + [insert_server, server_to_stop]:
            if srv is not None:
                await self.manager.get_host_id(srv.server_id)

        if len(audit_partition_servers) != 3 or server_to_stop is None or insert_server is None:
            raise pytest.skip("Failed to assign nodes for insert failure test")

        for srv in audit_partition_servers:
            logger.debug(f"audit_partition_server: {srv.server_id} {srv.ip_addr}")
        logger.debug(f"server_to_stop: {server_to_stop.server_id} {server_to_stop.ip_addr}")
        logger.debug(f"insert_server: {insert_server.server_id} {insert_server.ip_addr}")

        insert_conn = await self.manager.get_cql_exclusive(insert_server)
        stmt = SimpleStatement("INSERT INTO ks.test1 (k, v1) VALUES (1, 1)", consistency_level=ConsistencyLevel.THREE)
        with self.assert_exactly_n_audit_entries_were_added(session, 1):
            insert_conn.execute(stmt)

        await self.manager.server_stop_gracefully(server_to_stop.server_id)

        insert_conn = await self.manager.get_cql_exclusive(insert_server)
        # The driver wraps Unavailable in NoHostAvailable when all hosts fail,
        # so we catch NoHostAvailable and verify the inner error.
        try:
            stmt = SimpleStatement(query_to_fail, consistency_level=ConsistencyLevel.THREE)
            insert_conn.execute(stmt)
            pytest.fail("Expected insert to fail")
        except NoHostAvailable as e:
            errors = list(e.errors.values())
            assert len(errors) == 1
            assert isinstance(errors[0], Unavailable)

        await self.manager.server_start(server_to_stop.server_id)

        rows_dict = dict[str, list[AuditEntry]]()
        timestamp_before = datetime.datetime.now()
        for i in itertools.count(start=1):
            if datetime.datetime.now() - timestamp_before > datetime.timedelta(seconds=60):
                pytest.fail(f"audit log not updated after {i} iterations")

            rows_dict = self.get_audit_log_dict(session)
            # We need to satisfy the end state condition for all audit modes.
            # If any audit mode is not done yet, continue polling.
            all_modes_done = True
            for mode, rows in rows_dict.items():
                rows_with_error = [row for row in rows if row.error and row.operation == query_to_fail]
                if len(rows_with_error) == query_fail_count:
                    logger.info(f"audit mode {mode} log updated after {i} iterations ({i / 10}s)")
                    assert rows_with_error[0].error is True
                    assert rows_with_error[0].consistency == "THREE"

                    # We expect the initial insert to be in the audit log.
                    # it is executed in _test_insert_failure_doesnt_report_success_assign_nodes
                    rows_without_error = [row for row in rows if row.operation == query_to_fail and not row.error]
                    assert len(rows_without_error) == 1
                else:
                    # An audit mode is not done yet, early exit to continue polling.
                    all_modes_done = False
                    break
            if all_modes_done:
                break

    async def test_prepare(self, helper_class):
        """Test prepare statement"""
        with helper_class() as helper:
            session = await self.prepare(helper=helper)

            session.execute(
                """
                CREATE TABLE cf (
                    k varchar PRIMARY KEY,
                    c int,
                )
            """
            )

            with self.assert_no_audit_entries_were_added(session):
                query = "INSERT INTO cf (k, c) VALUES (?, ?);"
                pq = session.prepare(query)

            self.execute_and_validate_audit_entry(
                session,
                pq,
                bound_values=["foo", 4],
                category="DML",
                expected_operation="INSERT INTO cf (k, c) VALUES (?, ?);",
                table="cf",
            )

    async def test_permissions(self, helper_class):
        """Test user permissions"""

        with helper_class() as helper:
            session = await self.prepare(user="cassandra", password="cassandra", helper=helper)
            session.execute("CREATE TABLE test1 (k int PRIMARY KEY, v1 int)")
            session.execute("CREATE USER test WITH PASSWORD 'test'")
            session.execute("GRANT SELECT ON ks.test1 TO test")
            session.execute("INSERT INTO test1 (k, v1) VALUES (1, 1)")

            logging.info("Waiting for appling permissions")
            servers = await self.manager.running_servers()
            for srv in servers:
                await read_barrier(self.manager.api, srv.ip_addr)

            test_session = await self.manager.get_cql_exclusive(servers[0], auth_provider=PlainTextAuthProvider(username="test", password="test"))
            test_session.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
            def execute_and_validate_audit_entry(query, category, **kwargs):
                return self.execute_and_validate_audit_entry(test_session, query, category, session_for_audit_entry_validation=session, user="test", **kwargs)

            execute_and_validate_audit_entry(
                "SELECT * FROM ks.test1",
                category="QUERY",
                table="test1",
            )
            execute_and_validate_audit_entry(
                "INSERT INTO ks.test1 (k, v1) VALUES (2, 2)",
                category="DML",
                table="test1",
                expected_error=Unauthorized,
            )

    async def test_batch(self, helper_class):
        """
        BATCH statement
        """
        with helper_class() as helper:
            session = await self.prepare(helper=helper)

            session.execute(
                """
                CREATE TABLE test8 (
                    userid text PRIMARY KEY,
                    name text,
                    password text
                )
            """
            )

            batch_query = SimpleStatement(
                """
                    BEGIN BATCH
                        INSERT INTO test8 (userid, password, name) VALUES ('user1', 'ch@ngem3b', 'second user');
                        UPDATE test8 SET password = 'ps22dhds' WHERE userid = 'user3';
                        INSERT INTO test8 (userid, password) VALUES ('user4', 'ch@ngem3c');
                        DELETE name FROM test8 WHERE userid = 'user1';
                    APPLY BATCH;
                """,
                consistency_level=ConsistencyLevel.QUORUM,
            )

            expected_audit_operations = [
                "INSERT INTO test8 (userid, password, name) VALUES (user1, ch@ngem3b, second user)",
                "UPDATE test8 SET password = ps22dhds WHERE userid = user3",
                "INSERT INTO test8 (userid, password) VALUES (user4, ch@ngem3c)",
                "DELETE name FROM test8 WHERE userid = user1",
            ]
            expected_entries = list(map(lambda query: AuditEntry(category="DML", statement=query, table="test8", ks="ks", user="anonymous", cl="QUORUM", error=False), expected_audit_operations))

            with self.assert_entries_were_added(session, expected_entries, merge_duplicate_rows=False):
                session.execute(batch_query)

    async def test_service_level_statements(self):
        """
        Test auditing service level statements - ones that use the ADMIN audit category.
        """
        audit_settings = {"audit": "table", "audit_categories": "ADMIN"}
        session = await self.prepare(user="cassandra", password="cassandra", audit_settings=audit_settings, cmdline=["--smp", "1"])

        # Create role to which a service level can be attached.
        session.execute("CREATE ROLE test_role")

        query_sequence = [
            "CREATE SERVICE_LEVEL test_service_level WITH SHARES = 1",
            "ATTACH SERVICE_LEVEL test_service_level TO test_role",
            "DETACH SERVICE_LEVEL FROM test_role",
            "LIST SERVICE_LEVEL test_service_level",
            "LIST ALL SERVICE_LEVELS",
            "LIST ATTACHED SERVICE_LEVEL OF test_role",
            "LIST ALL ATTACHED SERVICE_LEVELS",
            "ALTER SERVICE_LEVEL test_service_level WITH SHARES = 2",
            "DROP SERVICE_LEVEL test_service_level",
        ]

        # Execute previously defined service level statements.
        # Validate that the audit log contains the expected entries.
        for query in query_sequence:
            self.execute_and_validate_audit_entry(session, query, category="ADMIN", audit_settings=audit_settings, ks="", user="cassandra")

        # Create a session with the ADMIN category disabled to validate that
        # the service level statements are not audited in that case.
        session = await self.prepare(user="cassandra", password="cassandra", audit_settings={"audit": "table", "audit_categories": "QUERY,DML,DDL,DCL"})

        # Execute previously defined service level statements.
        # Validate that the audit log does not contain any entries.
        with self.assert_no_audit_entries_were_added(session):
            for query in query_sequence:
                session.execute(query)

    class AuditConfigChanger:
        class ExpectedResult(enum.Enum):
            SUCCESS = 1
            FAILURE_UNPARSABLE_VALUE = 2
            FAILURE_UNUPDATABLE_PARAM = 3

        def __init__(self) -> None:
            super().__init__()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

        async def change_config(self, test, settings, expected_result):
            assert False, "change_config not implemented in AuditConfigChanger"

        async def verify_change(self, test, srv, param, value, from_mark, expected_result):
            current_value = await test.manager.api.get_config(srv.ip_addr, param)
            match expected_result:
                case self.ExpectedResult.SUCCESS:
                    assert current_value == value, f"response: {current_value}, expected: {value}"
                    log_file = await test.manager.server_open_log(srv.server_id)
                    await log_file.wait_for(r"Audit configuration is updated", from_mark=from_mark, timeout=60)
                case self.ExpectedResult.FAILURE_UNPARSABLE_VALUE:
                    assert current_value == value, f"response: {current_value}, expected: {value}"
                    log_file = await test.manager.server_open_log(srv.server_id)
                    await log_file.wait_for(r"Audit configuration update failed ", from_mark=from_mark, timeout=60)
                case self.ExpectedResult.FAILURE_UNUPDATABLE_PARAM:
                    assert current_value != value, f"response: {current_value}, expected different than: {value}"

    class AuditSighupConfigChanger(AuditConfigChanger):
        async def change_config(self, test, settings, expected_result):
            for srv in await test.manager.running_servers():
                logger.info(f"Changing config via manager.server_update_config: Node={srv.ip_addr} settings={settings}")
                log_file = await test.manager.server_open_log(srv.server_id)
                mark = await log_file.mark()
                for param, value in settings.items():
                    mark_per_param = await log_file.mark()
                    logger.info(f"server_update_config: param={param} value={value}")
                    await test.manager.server_update_config(srv.server_id, param, value)
                    await log_file.wait_for(r"completed re-reading configuration file", from_mark=mark_per_param, timeout=60)
                for param, value in settings.items():
                    await self.verify_change(test, srv, param, value, mark, expected_result)

    class AuditCqlConfigChanger(AuditConfigChanger):
        async def change_config(self, test, settings, expected_result):
            for srv in await test.manager.running_servers():
                log_file = await test.manager.server_open_log(srv.server_id)
                mark = await log_file.mark()
                for param in settings:
                    session = await test.manager.get_cql_exclusive(srv)
                    logger.debug(f"Changing config via CQL: Node={srv.ip_addr} param={param} value={settings[param]}")
                    try:
                        session.execute(f"UPDATE system.config SET value='{settings[param]}' where name='{param}';")
                    except WriteFailure:
                        assert expected_result == self.ExpectedResult.FAILURE_UNUPDATABLE_PARAM, f"CQL execution failed but expected_result: {expected_result}"
                for param in settings:
                    await self.verify_change(test, srv, param, settings[param], mark, expected_result)

    async def test_config_liveupdate(self, helper_class, audit_config_changer):
        """
        Test liveupdate config changes in audit.
        Liveupdate categories, tables, and keyspaces and confirm proper audit behavior.
        Execute with both audit sinks (Table, Syslog) and both liveupdate methods (SIGHUP, CQL).
        Use multi-node configuration (nodes=3).
        """
        with helper_class() as helper, audit_config_changer() as config_changer:
            # self.fixture_dtest_setup.allow_log_errors = True

            default_categories = copy.deepcopy(self.audit_default_settings["audit_categories"])
            enabled_but_empty_audit_config = helper.update_audit_settings(self.audit_default_settings, modifiers={"audit_categories": "", "audit_tables": "", "audit_keyspaces": ""})

            session = await self.prepare(helper=helper, rf=3, audit_settings=enabled_but_empty_audit_config)

            session.execute(
                """
                CREATE TABLE test_config_lifeupdate (
                    userid text PRIMARY KEY,
                    name text,
                    password text
                )
            """
            )

            auditted_query = SimpleStatement("INSERT INTO test_config_lifeupdate (userid, password, name) VALUES ('user2', 'password2', 'second user');", consistency_level=ConsistencyLevel.QUORUM)
            expected_new_entries = [AuditEntry(category="DML", statement=auditted_query.query_string, table="test_config_lifeupdate", ks="ks", user="anonymous", cl="QUORUM", error=False)]

            # Started with enabled_but_empty_audit_config config: no auditing
            with self.assert_no_audit_entries_were_added(session):
                session.execute(auditted_query)

            # Config modified with correct categories, multiple tables, and multiple keyspaces: audit works
            await config_changer.change_config(self, {"audit_categories": default_categories, "audit_tables": "test.table1,test.table2", "audit_keyspaces": "ks,ks2,ks3"}, expected_result=self.AuditConfigChanger.ExpectedResult.SUCCESS)
            with self.assert_entries_were_added(session, expected_new_entries, merge_duplicate_rows=False):
                session.execute(auditted_query)

            # Conifg modified with invalid categories: audit works, because previous audit configuration is used
            await config_changer.change_config(self, {"audit_categories": "INVALID_CATEGORIES"}, expected_result=self.AuditConfigChanger.ExpectedResult.FAILURE_UNPARSABLE_VALUE)
            with self.assert_entries_were_added(session, expected_new_entries, merge_duplicate_rows=False):
                session.execute(auditted_query)

            # Conifg modified with valid categories but invalid tables and and non-existing keyspaces: no auditing
            await config_changer.change_config(
                self, {"audit_categories": default_categories, "audit_tables": "invalid.table.twodots", "audit_keyspaces": "non-existing"}, expected_result=self.AuditConfigChanger.ExpectedResult.FAILURE_UNPARSABLE_VALUE
            )
            with self.assert_no_audit_entries_were_added(session):
                session.execute(auditted_query)

            # Config modified with valid tables and empty keyspaces: audit works
            await config_changer.change_config(self, {"audit_tables": "ks.test_config_lifeupdate", "audit_keyspaces": ""}, expected_result=self.AuditConfigChanger.ExpectedResult.SUCCESS)
            with self.assert_entries_were_added(session, expected_new_entries, merge_duplicate_rows=False):
                session.execute(auditted_query)

            # Set empty categories, tables, keyspaces: no auditing
            await config_changer.change_config(self, {"audit_categories": "", "audit_tables": "", "audit_keyspaces": ""}, expected_result=self.AuditConfigChanger.ExpectedResult.SUCCESS)
            with self.assert_no_audit_entries_were_added(session):
                session.execute(auditted_query)

    async def test_config_no_liveupdate(self, helper_class, audit_config_changer):
        """
        Test audit config parameters that don't allow config changes.
        Modification of "audit", "audit_unix_socket_path", and "audit_syslog_write_buffer_size" should be forbidden.
        """
        with helper_class() as helper, audit_config_changer() as config_changer:
            session = await self.prepare(helper=helper)

            session.execute(
                """
                CREATE TABLE test_config_no_lifeupdate (
                    userid text PRIMARY KEY,
                    name text,
                    password text
                )
            """
            )

            auditted_query = SimpleStatement("INSERT INTO test_config_no_lifeupdate (userid, password, name) VALUES ('user2', 'password2', 'second user');", consistency_level=ConsistencyLevel.QUORUM)
            expected_new_entries = [AuditEntry(category="DML", statement=auditted_query.query_string, table="test_config_no_lifeupdate", ks="ks", user="anonymous", cl="QUORUM", error=False)]

            # Modifications of "audit", "audit_unix_socket_path", "audit_syslog_write_buffer_size" are forbidden and will fail
            await config_changer.change_config(self, {"audit": "none"}, expected_result=self.AuditConfigChanger.ExpectedResult.FAILURE_UNUPDATABLE_PARAM)
            await config_changer.change_config(self, {"audit_unix_socket_path": "/path/"}, expected_result=self.AuditConfigChanger.ExpectedResult.FAILURE_UNUPDATABLE_PARAM)
            await config_changer.change_config(self, {"audit_syslog_write_buffer_size": "123123123"}, expected_result=self.AuditConfigChanger.ExpectedResult.FAILURE_UNUPDATABLE_PARAM)

            # Despite unsuccesful attempts to change config, audit works as expected
            with self.assert_entries_were_added(session, expected_new_entries, merge_duplicate_rows=False):
                session.execute(auditted_query)

    async def test_parallel_syslog_audit(self, helper_class):
        """
        Test that cluster doesn't fail if multiple queries are audited in parallel
        """
        with helper_class() as helper:
            audit_settings = helper.update_audit_settings(self.audit_default_settings, modifiers={"audit_keyspaces": "ks,kss"})
            session = await self.prepare(helper=helper, audit_settings=audit_settings, rf=3)

            try:
                session.execute("CREATE KEYSPACE kss WITH replication = { 'class':'NetworkTopologyStrategy', 'replication_factor': 3 }")
                run_in_parallel([{"func": lambda: session.execute("use ks;")}, {"func": lambda: session.execute("use kss;")}] * 1000)
            finally:
                session.execute("DROP KEYSPACE IF EXISTS kss")


@pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
async def test_using_non_existent_keyspace(manager: ManagerClient, helper_class):
    await TestCQLAudit(manager).test_using_non_existent_keyspace(helper_class=helper_class)
    await manager.mark_clean()

@pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
async def test_audit_keyspace(manager: ManagerClient, helper_class):
    await TestCQLAudit(manager).test_audit_keyspace(helper_class=helper_class)
    await manager.mark_clean()

@pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
async def test_audit_keyspace_extra_parameter(manager: ManagerClient, helper_class):
    await TestCQLAudit(manager).test_audit_keyspace_extra_parameter(helper_class)
    await manager.mark_clean()

@pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
async def test_audit_keyspace_many_ks(manager: ManagerClient, helper_class):
    await TestCQLAudit(manager).test_audit_keyspace_many_ks(helper_class)
    await manager.mark_clean()

@pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
async def test_audit_keyspace_table_not_exists(manager: ManagerClient, helper_class):
    await TestCQLAudit(manager).test_audit_keyspace_table_not_exists(helper_class)
    await manager.mark_clean()

async def test_audit_type_none(manager: ManagerClient):
    await TestCQLAudit(manager).test_audit_type_none()
    await manager.mark_clean()

async def test_audit_type_invalid(manager: ManagerClient):
    await TestCQLAudit(manager).test_audit_type_invalid()

async def test_composite_audit_type_invalid(manager: ManagerClient):
    await TestCQLAudit(manager).test_composite_audit_type_invalid()

async def test_audit_empty_settings(manager: ManagerClient):
    await TestCQLAudit(manager).test_audit_empty_settings()
    await manager.mark_clean()

async def test_composite_audit_empty_settings(manager: ManagerClient):
    await TestCQLAudit(manager).test_composite_audit_empty_settings()
    await manager.mark_clean()

async def test_audit_audit_ks(manager: ManagerClient):
    await TestCQLAudit(manager).test_audit_audit_ks()
    await manager.mark_clean()

async def test_audit_categories_invalid(manager: ManagerClient):
    await TestCQLAudit(manager).test_audit_categories_invalid()

async def test_audit_table(manager: ManagerClient):
    await TestCQLAudit(manager).test_audit_table()
    await manager.mark_clean()

async def test_audit_table_extra_parameter(manager: ManagerClient):
    await TestCQLAudit(manager).test_audit_table_extra_parameter()
    await manager.mark_clean()

async def test_audit_table_audit_keyspaces_empty(manager: ManagerClient):
    await TestCQLAudit(manager).test_audit_table_audit_keyspaces_empty()
    await manager.mark_clean()

async def test_audit_table_no_ks(manager: ManagerClient):
    await TestCQLAudit(manager).test_audit_table_no_ks()
    await manager.mark_clean()

async def test_audit_categories_part1(manager: ManagerClient):
    await TestCQLAudit(manager).test_audit_categories_part1()
    await manager.mark_clean()

@pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
async def test_audit_categories_part2(manager: ManagerClient, helper_class):
    await TestCQLAudit(manager).test_audit_categories_part2(helper_class)
    await manager.mark_clean()

@pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
async def test_audit_categories_part3(manager: ManagerClient, helper_class):
    await TestCQLAudit(manager).test_audit_categories_part3(helper_class)
    await manager.mark_clean()

@pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
async def test_user_password_masking(manager: ManagerClient, helper_class):
    await TestCQLAudit(manager).test_user_password_masking(helper_class)
    await manager.mark_dirty()

async def test_negative_audit_records_auth(manager: ManagerClient):
    await TestCQLAudit(manager).test_negative_audit_records_auth()
    await manager.mark_dirty()

async def test_negative_audit_records_admin(manager: ManagerClient):
    await TestCQLAudit(manager).test_negative_audit_records_admin()
    await manager.mark_dirty()

async def test_negative_audit_records_ddl(manager: ManagerClient):
    await TestCQLAudit(manager).test_negative_audit_records_ddl()
    await manager.mark_dirty()

async def test_negative_audit_records_dml(manager: ManagerClient):
    await TestCQLAudit(manager).test_negative_audit_records_dml()
    await manager.mark_dirty()

async def test_negative_audit_records_dcl(manager: ManagerClient):
    await TestCQLAudit(manager).test_negative_audit_records_dcl()
    await manager.mark_dirty()

async def test_negative_audit_records_query(manager: ManagerClient):
    await TestCQLAudit(manager).test_negative_audit_records_query()

@pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
async def test_role_password_masking(manager: ManagerClient, helper_class):
    await TestCQLAudit(manager).test_role_password_masking(helper_class)
    await manager.mark_dirty()

async def test_login(manager: ManagerClient):
    await TestCQLAudit(manager).test_login()
    await manager.mark_dirty()

async def test_cassandra_login(manager: ManagerClient):
    await TestCQLAudit(manager).test_cassandra_login()
    await manager.mark_dirty()

async def test_categories(manager: ManagerClient):
    await TestCQLAudit(manager).test_categories()
    await manager.mark_clean()

async def test_insert_failure_doesnt_report_success(manager: ManagerClient):
    await TestCQLAudit(manager).test_insert_failure_doesnt_report_success()
    await manager.mark_dirty()

@pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
async def test_prepare(manager: ManagerClient, helper_class):
    await TestCQLAudit(manager).test_prepare(helper_class)
    await manager.mark_clean()

@pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
async def test_permissions(manager: ManagerClient, helper_class):
    await TestCQLAudit(manager).test_permissions(helper_class)
    await manager.mark_dirty()

@pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
async def test_batch(manager: ManagerClient, helper_class):
    await TestCQLAudit(manager).test_batch(helper_class)
    await manager.mark_clean()

async def test_service_level_statements(manager: ManagerClient):
    await TestCQLAudit(manager).test_service_level_statements()
    await manager.mark_dirty()

@pytest.mark.parametrize("audit_config_changer", [TestCQLAudit.AuditSighupConfigChanger, TestCQLAudit.AuditCqlConfigChanger])
@pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
async def test_config_liveupdate(manager: ManagerClient, helper_class, audit_config_changer):
    await TestCQLAudit(manager).test_config_liveupdate(helper_class, audit_config_changer)
    await manager.mark_clean()

@pytest.mark.parametrize("audit_config_changer", [TestCQLAudit.AuditSighupConfigChanger, TestCQLAudit.AuditCqlConfigChanger])
@pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
async def test_config_no_liveupdate(manager: ManagerClient, helper_class, audit_config_changer):
    await TestCQLAudit(manager).test_config_no_liveupdate(helper_class, audit_config_changer)
    await manager.mark_clean()

@pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
async def test_parallel_syslog_audit(manager: ManagerClient, helper_class):
    await TestCQLAudit(manager).test_parallel_syslog_audit(helper_class)
    await manager.mark_clean()

