#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import copy
import datetime
import enum
import functools
import itertools
import logging
import os.path
from pathlib import Path
import re
import socket
import socketserver
import tempfile
import threading
import time
import uuid
from collections import namedtuple
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, override

import pytest
from cassandra import AlreadyExists, AuthenticationFailed, ConsistencyLevel, InvalidRequest, Unauthorized, Unavailable, WriteFailure
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import NoHostAvailable, Session, EXEC_PROFILE_DEFAULT
from cassandra.query import BatchStatement, BatchType, SimpleStatement, named_tuple_factory
from botocore.exceptions import ClientError

from test.cluster.dtest.dtest_class import create_ks, wait_for
from test.cluster.dtest.tools.assertions import assert_invalid
from test.cluster.dtest.tools.data import rows_to_list, run_in_parallel

from test.cluster.test_alternator import alternator_config, get_alternator, unique_table_name
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import read_barrier
from test.pylib.skip_types import skip_env
from test.pylib.util import wait_for as wait_for_async
from test.pylib.scylla_cluster import ScyllaVersionDescription

logger = logging.getLogger(__name__)

# Stable socket path for syslog backends, shared across tests to enable server reuse.
syslog_socket_path = tempfile.mktemp(prefix="/tmp/scylla-audit-", suffix=".socket")

# Keys that require server restart (not live-updatable).
NON_LIVE_AUDIT_KEYS = {"audit", "audit_unix_socket_path"}
# Keys that can be updated via SIGHUP (live-updatable).
LIVE_AUDIT_KEYS = {"audit_categories", "audit_keyspaces", "audit_tables"}
# Auth config applied when user/password are requested.
AUTH_CONFIG = {
    "authenticator": "org.apache.cassandra.auth.PasswordAuthenticator",
    "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer",
}


class AuditRowMustNotExistError(Exception):
    pass


class AuditTester:
    audit_default_settings = {"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}

    def __init__(self, manager: ManagerClient):
        self.manager = manager

    def _build_server_config(self, needed: dict[str, str],
                             enable_compact_storage: bool,
                             user: str | None) -> dict[str, Any]:
        """Build the full server config dict from audit settings, auth, and flags."""
        cfg: dict[str, Any] = dict(needed)
        cfg["enable_create_table_with_compact_storage"] = enable_compact_storage
        if user:
            cfg.update(AUTH_CONFIG)
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
            for k in NON_LIVE_AUDIT_KEYS if k in needed
        ) or any(k in current for k in NON_LIVE_AUDIT_KEYS & absent_keys)

        # Auth config changes also require a restart.
        has_auth = any(k in current for k in AUTH_CONFIG)
        if user:
            restart = restart or any(
                str(current.get(k, "")) != str(v)
                for k, v in AUTH_CONFIG.items()
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
            has_auth = any(k in current for k in AUTH_CONFIG)
            if not user and has_auth:
                for k in AUTH_CONFIG:
                    await self.manager.server_remove_config_option(srv.server_id, k)

            if needs_restart:
                # Remove absent keys so the server reverts to compiled-in defaults.
                for k in absent_keys:
                    await self.manager.server_remove_config_option(srv.server_id, k)
                await self.manager.server_stop_gracefully(srv.server_id)
                full_cfg = self._build_server_config(needed, enable_compact_storage, user)
                await self.manager.server_update_config(srv.server_id, config_options=full_cfg)
                await self.manager.server_start(srv.server_id)
                await self.manager.driver_connect(auth_provider=auth_provider)
            else:
                # Server stays up — only push live-updatable keys.
                live_cfg = {k: v for k, v in needed.items() if k in LIVE_AUDIT_KEYS}
                live_cfg["enable_create_table_with_compact_storage"] = enable_compact_storage
                log_file = await self.manager.server_open_log(srv.server_id)
                # Each remove/update sends a SIGHUP.  Wait for each one's
                # "completed re-reading configuration file" before the next
                # so we never match a stale message.
                for k in absent_keys:
                    from_mark = await log_file.mark()
                    await self.manager.server_remove_config_option(srv.server_id, k)
                    await log_file.wait_for(r"completed re-reading configuration file", from_mark=from_mark, timeout=60)
                from_mark = await log_file.mark()
                await self.manager.server_update_config(srv.server_id, config_options=live_cfg)
                await log_file.wait_for(r"completed re-reading configuration file", from_mark=from_mark, timeout=60)

    async def _start_fresh_servers(self, needed: dict[str, str],
                                   enable_compact_storage: bool,
                                   rf: int,
                                   user: str | None,
                                   auth_provider,
                                   property_file: list[dict[str, str]] | None = None,
                                   cmdline: list[str] | None = None) -> list[str]:
        """Add and start a brand new server with the given config.

        When the cluster already contains stopped servers, their stale seed
        IPs would poison newly added servers.  Detect this automatically and
        bootstrap the first server with an explicit self-seed.
        """
        cfg = self._build_server_config(needed, enable_compact_storage, user)
        connect_opts: dict[str, Any] = {}
        if auth_provider:
            connect_opts["auth_provider"] = auth_provider
        if property_file is None:
            property_file = [{"dc": "dc1", "rack": f"rack{i + 1}"} for i in range(rf)]

        has_stopped_servers = len(await self.manager.all_servers()) > len(await self.manager.running_servers())
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
        absent_keys = (NON_LIVE_AUDIT_KEYS | LIVE_AUDIT_KEYS) - needed.keys()
        auth_provider = PlainTextAuthProvider(username=user, password=password or "") if user else None
        expected_servers = len(property_file) if property_file else rf

        servers = await self.manager.running_servers()
        if servers and len(servers) != expected_servers:
            self.manager.driver_close()
            for srv in servers:
                await self.manager.server_stop_gracefully(srv.server_id)
            servers = None

        if servers:
            server_ips = [srv.ip_addr for srv in servers]
            await self._apply_config_to_running_servers(
                servers, needed, absent_keys, enable_compact_storage, user, auth_provider)
        else:
            server_ips = await self._start_fresh_servers(
                needed, enable_compact_storage, rf, user, auth_provider,
                property_file=property_file, cmdline=cmdline)

        cql = self.manager.get_cql()
        cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE
        audit_mode = needed.get("audit") or ""
        if "table" not in audit_mode:
            cql.execute("DROP KEYSPACE IF EXISTS audit")

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
        self.helper.clear_audit_logs(session)
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

    class UnixDatagramServer(socketserver.UnixDatagramServer):
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
        self.unix_socket_listener.get_lines()
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


class CQLAuditTester(AuditTester):
    """
    Make sure CQL statements are audited
    """

    AUDIT_LOG_QUERY = "SELECT * FROM audit.audit_log"

    def __init__(self, manager: ManagerClient, helper: AuditBackend | None = None):
        super().__init__(manager)
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

    def execute_and_validate_new_audit_entry(  # noqa: PLR0913
        self,
        session: Session,
        query: Any,
        category: str,
        table: str = "",
        ks: str = "ks",
        cl: str = "ONE",
        user: str = "anonymous",
        bound_values: list[Any] | None = None,
        expected_operation: str | None = None,
        error: bool = False,
        session_for_audit_entry_validation: Session | None = None,
    ):
        """
        Execute a query and validate that an audit entry was added to the audit
        log table.
        """

        # In some cases, provided session does not have access to the audit
        # table. In that case, session_for_audit_entry_validation should be
        # provided.
        if session_for_audit_entry_validation is None:
            session_for_audit_entry_validation = session

        operation = query if expected_operation is None else expected_operation
        expected_entries = [AuditEntry(category, cl, error, ks, operation, table, user)]

        with self.assert_entries_were_added(session_for_audit_entry_validation, expected_entries):
            res = session.execute(query, bound_values)

        return res

    def execute_and_validate_if_category_enabled(  # noqa: PLR0913
        self,
        session: Session,
        query: Any,
        category: str,
        audit_settings: dict[str, str],
        table: str = "",
        ks: str = "ks",
        cl: str = "ONE",
        user: str = "anonymous",
        bound_values: list[Any] | None = None,
        expected_operation: str | None = None,
        session_for_audit_entry_validation: Session | None = None,
    ):
        """
        Execute a query and validate or skip audit entry validation based on
        whether the given category is enabled in audit_settings.
        """

        if session_for_audit_entry_validation is None:
            session_for_audit_entry_validation = session

        audit_categories = [c.strip() for c in audit_settings.get("audit_categories", "").split(",")]
        if category in audit_categories:
            return self.execute_and_validate_new_audit_entry(
                session, query, category,
                table=table, ks=ks, cl=cl, user=user,
                bound_values=bound_values,
                expected_operation=expected_operation,
                session_for_audit_entry_validation=session_for_audit_entry_validation,
            )
        else:
            with self.assert_no_audit_entries_were_added(session_for_audit_entry_validation):
                res = session.execute(query, bound_values)
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
        rows_before_dict = self.get_audit_log_dict(session)
        for mode, rows_before in rows_before_dict.items():
            set_of_rows_before = set(rows_before)
            assert len(set_of_rows_before) == len(rows_before), f"audit {mode} contains duplicate rows: {rows_before}"
        yield

        new_rows_dict = dict[str, list[AuditEntry]]()
        def is_number_of_new_rows_correct():
            rows_after_dict = self.get_audit_log_dict(session)
            for mode, rows_after in rows_after_dict.items():
                assert len(set(rows_after)) == len(rows_after), f"audit {mode} contains duplicate rows: {rows_after}"

            nonlocal new_rows_dict
            for mode, rows_after in rows_after_dict.items():
                # Different nodes can have different timestamps.
                # If rows from all the nodes are in one list, we can not be
                # sure that the new rows are the last rows in the list.
                # We need to check the new rows for each node separately.
                after_by_node: dict[str, list[AuditEntry]] = {}
                for row in rows_after:
                    after_by_node.setdefault(row.node, []).append(row)
                before_by_node: dict[str, list[AuditEntry]] = {}
                for row in rows_before_dict[mode]:
                    before_by_node.setdefault(row.node, []).append(row)

                new_rows: list[AuditEntry] = []
                for node, node_after in after_by_node.items():
                    node_before = before_by_node.get(node, [])
                    set_node_diff = set(node_after) - set(node_before)
                    new_rows.extend(set_node_diff)

                new_rows.sort(key=lambda row: (row.event_time.time, row.node))
                new_rows_dict[mode] = new_rows

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

        self.execute_and_validate_new_audit_entry(
            session,
            "CREATE KEYSPACE ks WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND tablets = {'enabled': false} AND DURABLE_WRITES = true",
            category="DDL",
        )
        self.execute_and_validate_new_audit_entry(
            session,
            'USE "ks"',
            category="DML",
        )
        self.execute_and_validate_new_audit_entry(
            session,
            "ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false",
            category="DDL",
        )
        self.execute_and_validate_new_audit_entry(
            session,
            "DROP KEYSPACE ks",
            category="DDL",
        )

        # Test that the audit entries are not added if the keyspace is not
        # specified in the audit_keyspaces setting.
        keyspaces = audit_settings["audit_keyspaces"].split(",") if "audit_keyspaces" in audit_settings else []
        assert "ks2" not in keyspaces
        query_sequence = [
            "CREATE KEYSPACE ks2 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND tablets = {'enabled': false} AND DURABLE_WRITES = true",
            'USE "ks2"',
            "ALTER KEYSPACE ks2 WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false",
            "DROP KEYSPACE ks2",
        ]

        with self.assert_no_audit_entries_were_added(session):
            for query in query_sequence:
                session.execute(query)

    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
    async def _test_using_non_existent_keyspace(self, helper_class):
        """
        Test tha using a non-existent keyspace generates an audit entry with an
        error field set to True.
        """

        with helper_class() as helper:
            session = await self.prepare(create_keyspace=False, helper=helper)

            expected_entry = AuditEntry(category="DML", cl="ONE", error=True, ks="ks",
                                        statement='USE "ks"', table="", user="anonymous")
            with self.assert_entries_were_added(session, [expected_entry]):
                assert_invalid(session, 'USE "ks"', expected=InvalidRequest)  # ks doesn't exist because create_keyspace=False in prepare

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

        self.execute_and_validate_new_audit_entry(
            session,
            f"CREATE TABLE {first_table} (k int PRIMARY KEY, v1 int)",
            category="DDL",
            table=first_table,
        )
        self.execute_and_validate_new_audit_entry(
            session,
            f"CREATE TABLE {second_table} (k int, c1 int, v1 int, PRIMARY KEY (k, c1)) WITH COMPACT STORAGE",
            category="DDL",
            table=second_table,
        )
        self.execute_and_validate_new_audit_entry(
            session,
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

                self.execute_and_validate_if_category_enabled(
                    session,
                    f"INSERT INTO {table} {columns} VALUES ({i}, {i}, {i})",
                    category="DML",
                    audit_settings=audit_settings,
                    table=f"{table}",
                )

            res = self.execute_and_validate_if_category_enabled(
                session,
                f"SELECT * FROM {table}",
                category="QUERY",
                audit_settings=audit_settings,
                table=f"{table}",
            )
            assert sorted(rows_to_list(res)) == [[i, i, i] for i in range(10)], res

            self.execute_and_validate_if_category_enabled(
                session,
                f"TRUNCATE {table}",
                category="DML",
                audit_settings=audit_settings,
                table=f"{table}",
            )

            res = self.execute_and_validate_if_category_enabled(
                session,
                f"SELECT * FROM {table}",
                category="QUERY",
                audit_settings=audit_settings,
                table=f"{table}",
            )
            assert rows_to_list(res) == [], res

            self.execute_and_validate_new_audit_entry(
                session,
                f"DROP TABLE {table}",
                category="DDL",
                table=f"{table}",
            )

            with self.assert_no_audit_entries_were_added(session):
                assert_invalid(session, f"SELECT * FROM {table}", expected=InvalidRequest)

        # Test that the audit entries are not added if the keyspace is not
        # specified in the audit_keyspaces setting.
        keyspaces = audit_settings["audit_keyspaces"].split(",") if "audit_keyspaces" in audit_settings else []
        assert "ks2" not in keyspaces
        query_sequence = [
            "CREATE KEYSPACE ks2 WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND tablets = {'enabled': false} AND DURABLE_WRITES = true",
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

    async def _test_audit_keyspace(self, helper_class):
        with helper_class(socket_path=syslog_socket_path) as helper:
            await self.verify_keyspace(audit_settings=AuditTester.audit_default_settings, helper=helper)

    async def _test_audit_keyspace_extra_parameter(self, helper_class):
        with helper_class(socket_path=syslog_socket_path) as helper:
            await self.verify_keyspace(audit_settings={"audit": "table", "audit_categories": "ADMIN,AUTH,DML,DDL,DCL", "audit_keyspaces": "ks", "extra_parameter": "new"}, helper=helper)

    async def _test_audit_keyspace_many_ks(self, helper_class):
        with helper_class() as helper:
            await self.verify_keyspace(audit_settings={"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "a,b,c,ks"}, helper=helper)

    async def _test_audit_keyspace_table_not_exists(self, helper_class):
        with helper_class() as helper:
            await self.verify_keyspace(audit_settings={"audit": "table", "audit_categories": "DML,DDL", "audit_keyspaces": "ks", "audit_tables": "ks.fake"}, helper=helper)

    async def _test_audit_type_none(self):
        """
        'audit': None
         CREATE KEYSPACE, USE KEYSPACE, ALTER KEYSPACE, DROP KEYSPACE statements
         check audit KS not created
        """

        audit_settings = {"audit": None, "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}

        session = await self.prepare(create_keyspace=False, audit_settings=audit_settings)

        session.execute("CREATE KEYSPACE ks WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND tablets = {'enabled': false} AND DURABLE_WRITES = true")

        session.execute("USE ks")

        session.execute("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false")

        session.execute("DROP KEYSPACE ks")
        assert_invalid(session, "use audit;", expected=InvalidRequest)

    async def _test_audit_type_invalid(self):
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

    async def _test_composite_audit_type_invalid(self):
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
    async def _test_audit_empty_settings(self):
        """
        'audit': none
         check node started, ks audit not created
        """
        session = await self.prepare(create_keyspace=False, audit_settings={"audit": "none"})
        assert_invalid(session, "use audit;", expected=InvalidRequest)

    async def _test_composite_audit_empty_settings(self):
        """
        'audit': table,syslog,none
         check node started, ks audit not created
        """
        session = await self.prepare(create_keyspace=False, audit_settings={"audit": "table,syslog,none"})
        assert_invalid(session, "use audit;", expected=InvalidRequest)

    async def _test_audit_audit_ks(self):
        """
        'audit_keyspaces': 'audit'
        check node started, ks audit created
        """
        audit_settings = {"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "audit"}
        session = await self.prepare(create_keyspace=False, audit_settings=audit_settings)

        self.execute_and_validate_new_audit_entry(session, query=self.AUDIT_LOG_QUERY, category="QUERY", ks="audit", table="audit_log")

    async def _test_audit_categories_invalid(self):
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
    async def _test_audit_table(self):
        await self.verify_table(audit_settings=AuditTester.audit_default_settings, table_prefix="test_audit_table")

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    async def _test_audit_table_extra_parameter(self):
        await self.verify_table(audit_settings={"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks", "extra_parameter": "new"}, table_prefix="test_audit_table_extra_parameter")

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    async def _test_audit_table_audit_keyspaces_empty(self):
        await self.verify_table(audit_settings={"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL"}, table_prefix="test_audit_table_audit_keyspaces_empty", overwrite_audit_tables=True)

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    async def _test_audit_table_no_ks(self):
        await self.verify_table(audit_settings={"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL"}, table_prefix="test_audit_table_no_ks", overwrite_audit_tables=True)

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    async def _test_audit_categories_part1(self):
        await self.verify_table(audit_settings={"audit": "table", "audit_categories": "AUTH,QUERY,DDL"}, table_prefix="test_audit_categories_part1", overwrite_audit_tables=True)

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    # @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
    async def _test_audit_categories_part2(self, helper_class):
        with helper_class() as helper:
            await self.verify_table(audit_settings={"audit": "table", "audit_categories": "DDL, ADMIN,AUTH,DCL", "audit_keyspaces": "ks"}, helper=helper, table_prefix="test_audit_categories_part2")

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    # @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
    async def _test_audit_categories_part3(self, helper_class):
        with helper_class() as helper:
            await self.verify_table(audit_settings={"audit": "table", "audit_categories": "DDL, ADMIN,AUTH", "audit_keyspaces": "ks"}, helper=helper, table_prefix="test_audit_categories_part3")

    PasswordMaskingCase = namedtuple("PasswordMaskingCase", ["name", "password", "new_password"])

    # @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog, AuditBackendComposite])
    async def _test_user_password_masking(self, helper_class):
        """
        CREATE USER, ALTER USER, DROP USER statements
        """
        with helper_class() as helper:
            session = await self.prepare(user="cassandra", password="cassandra", helper=helper)

            tests = [self.PasswordMaskingCase("user1", "secret", "Secret^%$#@!"), self.PasswordMaskingCase("user2", "", "")]
            for username, password, new_password in tests:
                self.execute_and_validate_new_audit_entry(
                    session,
                    f"CREATE USER {username} WITH PASSWORD '{password}'",
                    category="DCL",
                    expected_operation=f"CREATE USER {username} WITH PASSWORD '***'",
                    user="cassandra",
                    ks="",
                )
                self.execute_and_validate_new_audit_entry(
                    session,
                    f"ALTER USER {username} WITH PASSWORD '{new_password}'",
                    category="DCL",
                    expected_operation=f"ALTER USER {username} WITH PASSWORD '***'",
                    user="cassandra",
                    ks="",
                )
                self.execute_and_validate_new_audit_entry(
                    session,
                    f"DROP USER {username}",
                    category="DCL",
                    user="cassandra",
                    ks="",
                )

    async def _test_negative_audit_records_auth(self):
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

    async def _test_negative_audit_records_admin(self):
        """
        Test that failed ADMIN statements are audited.
        """
        session = await self.prepare(user="cassandra", password="cassandra")
        session.execute("CREATE ROLE test_role")

        stmt = "ATTACH SERVICE_LEVEL test_service_level TO test_role"

        expected_entry = AuditEntry(category="ADMIN", table="", ks="", user="cassandra", cl="ONE", error=True, statement=stmt)

        with self.assert_entries_were_added(session, [expected_entry]):
            assert_invalid(session, stmt, expected=InvalidRequest)

        session.execute("DROP ROLE IF EXISTS test_role")

    async def _test_negative_audit_records_ddl(self):
        """
        Test that failed DDL statements are audited.
        """
        session = await self.prepare(user="cassandra", password="cassandra", rf=3)

        stmt = "CREATE KEYSPACE ks WITH replication = { 'class':'NetworkTopologyStrategy', 'replication_factor': 1 }"

        expected_entry = AuditEntry(category="DDL", table="", ks="ks", user="cassandra", cl="ONE", error=True, statement=stmt)

        with self.assert_entries_were_added(session, [expected_entry]):
            assert_invalid(session, stmt, expected=AlreadyExists)

    async def _test_negative_audit_records_dml(self):
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

    async def _test_negative_audit_records_dcl(self):
        """
        Test that failed DCL statements are audited.
        """
        session = await self.prepare(user="cassandra", password="cassandra")
        stmt = "GRANT SELECT ON ALL KEYSPACES TO test_role"

        expected_entry = AuditEntry(category="DCL", table="", ks="", user="cassandra", cl="ONE", error=True, statement=stmt)

        with self.assert_entries_were_added(session, [expected_entry]):
            assert_invalid(session, stmt, expected=InvalidRequest)

    async def _test_negative_audit_records_query(self):
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
    async def _test_role_password_masking(self, helper_class):
        """
        CREATE ROLE, ALTER ROLE, DROP ROLE statements
        """

        with helper_class() as helper:
            session = await self.prepare(user="cassandra", password="cassandra", helper=helper)

            tests = [self.PasswordMaskingCase("role1", "Secret!@#$", "Secret^%$#@!"), self.PasswordMaskingCase("role2", "", "")]
            for role_name, password, new_password in tests:
                self.execute_and_validate_new_audit_entry(
                    session,
                    f"CREATE ROLE {role_name} WITH PASSWORD = '{password}'",
                    category="DCL",
                    expected_operation=f"CREATE ROLE {role_name} WITH PASSWORD = '***'",
                    user="cassandra",
                    ks="",
                )
                self.execute_and_validate_new_audit_entry(
                    session,
                    f"ALTER ROLE {role_name} WITH PASSWORD = '{new_password}'",
                    category="DCL",
                    expected_operation=f"ALTER ROLE {role_name} WITH PASSWORD = '***'",
                    user="cassandra",
                    ks="",
                )
                self.execute_and_validate_new_audit_entry(
                    session,
                    f"DROP ROLE {role_name}",
                    category="DCL",
                    user="cassandra",
                    ks="",
                )

    async def _test_login(self):
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

        session.execute("DROP USER IF EXISTS test")

    async def _test_cassandra_login(self):
        """
        Test user login to default (cassandra) user
        """
        session = await self.prepare(user="cassandra", password="cassandra", create_keyspace=False)
        expected_audit_entries = [AuditEntry(category="AUTH", statement="LOGIN", user="cassandra", table="", ks="", cl="", error=False)]

        with self.assert_entries_were_added(session, expected_audit_entries, filter_out_cassandra_auth=False):
            servers = await self.manager.running_servers()
            test_auth = PlainTextAuthProvider(username="cassandra", password="cassandra")
            await self.manager.get_cql_exclusive(servers[0], auth_provider=test_auth)

    async def _test_categories(self):
        """
        Test filtering audit categories
        """
        session = await self.prepare(audit_settings={"audit": "table", "audit_categories": "DML", "audit_keyspaces": "ks"})

        with self.assert_no_audit_entries_were_added(session):
            session.execute("CREATE TABLE test1 (k int PRIMARY KEY, v1 int)")
            session.execute("ALTER TABLE test1 ADD v2 int")

        for i in range(10):
            self.execute_and_validate_new_audit_entry(
                session,
                f"INSERT INTO test1 (k, v1, v2) VALUES ({i}, {i}, {i})",
                category="DML",
                table="test1",
            )

        with self.assert_no_audit_entries_were_added(session):
            res = sorted(session.execute("SELECT * FROM test1"))
            assert rows_to_list(res) == [[i, i, i] for i in range(10)], res

        self.execute_and_validate_new_audit_entry(
            session,
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
            # The audit log entry may not be visible immediately after the
            # insert, so retry with exponential backoff until it appears.
            audit_node_ips = await wait_for_async(
                lambda: self.get_audit_partitions_for_operation(session, stmt.query_string),
                deadline=time.time() + 10,
                period=0.05,
                label=f"audit entry for node {index}")
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
                        skip_env("Failed to assign nodes for insert failure test")
                    audit_partition_servers = [address_to_server[addr] for addr in audit_nodes]
                    insert_server = address_to_server[insert_addr.pop()]
                    kill_server = address_to_server[partitions.pop()]
                    return audit_partition_servers, insert_server, kill_server, stmt.query_string, attempt_count

        return [], None, None, None, None

    async def _test_insert_failure_doesnt_report_success(self):
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
            skip_env("Failed to assign nodes for insert failure test")

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

    async def _test_prepare(self, helper_class):
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

            self.execute_and_validate_new_audit_entry(
                session,
                pq,
                bound_values=["foo", 4],
                category="DML",
                expected_operation="INSERT INTO cf (k, c) VALUES (?, ?);",
                table="cf",
            )

    async def _test_permissions(self, helper_class):
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

            self.execute_and_validate_new_audit_entry(
                test_session,
                "SELECT * FROM ks.test1",
                category="QUERY",
                table="test1",
                user="test",
                session_for_audit_entry_validation=session,
            )

            expected_entry = AuditEntry(category="DML", cl="ONE", error=True, ks="ks",
                                        statement="INSERT INTO ks.test1 (k, v1) VALUES (2, 2)", table="test1", user="test")
            with self.assert_entries_were_added(session, [expected_entry]):
                assert_invalid(test_session, "INSERT INTO ks.test1 (k, v1) VALUES (2, 2)", expected=Unauthorized)

            session.execute("DROP USER IF EXISTS test")

    async def _test_batch(self, helper_class):
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

    async def _test_batch_native_protocol(self, helper_class):
        """
        Native protocol BATCH message (as opposed to CQL text batch).

        Reproducer for a bug where batches sent via the native
        protocol BATCH message were not audited.  The driver's BatchStatement
        sends a native-protocol BATCH (opcode 0x0D) which is handled by
        process_batch_internal in transport/server.cc — a different code path
        from a textual BEGIN BATCH … APPLY BATCH sent as a QUERY message.
        """
        with helper_class() as helper:
            session = await self.prepare(helper=helper)

            session.execute(
                """
                CREATE TABLE test_batch_native (
                    pk int PRIMARY KEY,
                    v text
                )
            """
            )

            # Unprepared native-protocol batch (SimpleStatement inside BatchStatement)
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            batch.add(SimpleStatement("INSERT INTO test_batch_native (pk, v) VALUES (%s, %s)"), (1, "val1"))
            batch.add(SimpleStatement("INSERT INTO test_batch_native (pk, v) VALUES (%s, %s)"), (2, "val2"))

            expected_entries = [
                AuditEntry(category="DML", statement="INSERT INTO test_batch_native (pk, v) VALUES (1, 'val1')", table="test_batch_native", ks="ks", user="anonymous", cl="ONE", error=False),
                AuditEntry(category="DML", statement="INSERT INTO test_batch_native (pk, v) VALUES (2, 'val2')", table="test_batch_native", ks="ks", user="anonymous", cl="ONE", error=False),
            ]

            with self.assert_entries_were_added(session, expected_entries, merge_duplicate_rows=False):
                session.execute(batch)

            # Prepared native-protocol batch
            prepared = session.prepare("INSERT INTO test_batch_native (pk, v) VALUES (?, ?)")
            batch_prepared = BatchStatement(batch_type=BatchType.UNLOGGED)
            batch_prepared.add(prepared, (3, "val3"))
            batch_prepared.add(prepared, (4, "val4"))

            expected_entries_prepared = [
                AuditEntry(category="DML", statement="INSERT INTO test_batch_native (pk, v) VALUES (?, ?)", table="test_batch_native", ks="ks", user="anonymous", cl="ONE", error=False),
                AuditEntry(category="DML", statement="INSERT INTO test_batch_native (pk, v) VALUES (?, ?)", table="test_batch_native", ks="ks", user="anonymous", cl="ONE", error=False),
            ]

            with self.assert_entries_were_added(session, expected_entries_prepared, merge_duplicate_rows=False):
                session.execute(batch_prepared)

    async def _test_service_level_statements(self):
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
            self.execute_and_validate_new_audit_entry(session, query, category="ADMIN", ks="", user="cassandra")

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

    async def _test_config_liveupdate(self, helper_class, audit_config_changer):
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

    async def _test_config_no_liveupdate(self, helper_class, audit_config_changer):
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

    async def _test_parallel_syslog_audit(self, helper_class):
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


# AuditBackendTable, no auth, rf=1

async def test_audit_table_noauth(manager: ManagerClient):
    """Table backend, no auth, single node — groups all tests that share this config."""
    t = CQLAuditTester(manager)
    await t._test_using_non_existent_keyspace(AuditBackendTable)
    await t._test_audit_keyspace(AuditBackendTable)
    await t._test_audit_keyspace_extra_parameter(AuditBackendTable)
    await t._test_audit_keyspace_many_ks(AuditBackendTable)
    await t._test_audit_keyspace_table_not_exists(AuditBackendTable)
    await t._test_audit_audit_ks()
    await t._test_audit_table()
    await t._test_audit_table_extra_parameter()
    await t._test_audit_table_audit_keyspaces_empty()
    await t._test_audit_table_no_ks()
    await t._test_audit_categories_part1()
    await t._test_audit_categories_part2(AuditBackendTable)
    await t._test_audit_categories_part3(AuditBackendTable)
    await t._test_categories()
    await t._test_negative_audit_records_query()
    await t._test_prepare(AuditBackendTable)
    await t._test_batch(AuditBackendTable)
    await t._test_batch_native_protocol(AuditBackendTable)


# AuditBackendTable, auth (cassandra), rf=1

async def test_audit_table_auth(manager: ManagerClient):
    """Table backend, auth enabled, single node."""
    t = CQLAuditTester(manager)
    await t._test_user_password_masking(AuditBackendTable)
    await t._test_negative_audit_records_auth()
    await t._test_negative_audit_records_admin()
    await t._test_negative_audit_records_dml()
    await t._test_negative_audit_records_dcl()
    await t._test_role_password_masking(AuditBackendTable)
    await t._test_login()
    await t._test_cassandra_login()
    await t._test_permissions(AuditBackendTable)


# AuditBackendTable, auth (cassandra), rf=3

async def test_audit_table_auth_multinode(manager: ManagerClient):
    """Table backend, auth enabled, multi-node (rf=3)."""
    t = CQLAuditTester(manager)
    await t._test_negative_audit_records_ddl()


# AuditBackendTable, standalone / special config

async def test_audit_type_none_standalone(manager: ManagerClient):
    """audit=None — verify no auditing occurs."""
    await CQLAuditTester(manager)._test_audit_type_none()


async def test_audit_type_invalid_standalone(manager: ManagerClient):
    """audit=invalid — server should fail to start."""
    await CQLAuditTester(manager)._test_audit_type_invalid()


async def test_composite_audit_type_invalid_standalone(manager: ManagerClient):
    """audit=table,syslog,invalid — server should fail to start."""
    await CQLAuditTester(manager)._test_composite_audit_type_invalid()


async def test_audit_empty_settings_standalone(manager: ManagerClient):
    """audit=none — verify no auditing occurs."""
    await CQLAuditTester(manager)._test_audit_empty_settings()


async def test_composite_audit_empty_settings_standalone(manager: ManagerClient):
    """audit=table,syslog,none — verify no auditing occurs."""
    await CQLAuditTester(manager)._test_composite_audit_empty_settings()


async def test_audit_categories_invalid_standalone(manager: ManagerClient):
    """Invalid audit_categories — server should fail to start."""
    await CQLAuditTester(manager)._test_audit_categories_invalid()


async def test_insert_failure_standalone(manager: ManagerClient):
    """7-node topology, audit=table, no auth — standalone due to unique topology."""
    await CQLAuditTester(manager)._test_insert_failure_doesnt_report_success()


async def test_service_level_statements_standalone(manager: ManagerClient):
    """audit=table, auth, cmdline=--smp 1 — standalone due to special cmdline."""
    await CQLAuditTester(manager)._test_service_level_statements()


# AuditBackendSyslog, no auth, rf=1

async def test_audit_syslog_noauth(manager: ManagerClient):
    """Syslog backend, no auth, single node."""
    t = CQLAuditTester(manager)
    Syslog = functools.partial(AuditBackendSyslog, socket_path=syslog_socket_path)
    await t._test_using_non_existent_keyspace(Syslog)
    await t._test_audit_keyspace(Syslog)
    await t._test_audit_keyspace_extra_parameter(Syslog)
    await t._test_audit_keyspace_many_ks(Syslog)
    await t._test_audit_keyspace_table_not_exists(Syslog)
    await t._test_audit_categories_part2(Syslog)
    await t._test_audit_categories_part3(Syslog)
    await t._test_prepare(Syslog)
    await t._test_batch(Syslog)
    await t._test_batch_native_protocol(Syslog)


# AuditBackendSyslog, auth, rf=1

async def test_audit_syslog_auth(manager: ManagerClient):
    """Syslog backend, auth enabled, single node."""
    t = CQLAuditTester(manager)
    Syslog = functools.partial(AuditBackendSyslog, socket_path=syslog_socket_path)
    await t._test_user_password_masking(Syslog)
    await t._test_role_password_masking(Syslog)
    await t._test_permissions(Syslog)


# AuditBackendComposite, no auth, rf=1

async def test_audit_composite_noauth(manager: ManagerClient):
    """Composite backend (table+syslog), no auth, single node."""
    t = CQLAuditTester(manager)
    Composite = functools.partial(AuditBackendComposite, socket_path=syslog_socket_path)
    await t._test_using_non_existent_keyspace(Composite)
    await t._test_audit_keyspace(Composite)
    await t._test_audit_keyspace_extra_parameter(Composite)
    await t._test_audit_keyspace_many_ks(Composite)
    await t._test_audit_keyspace_table_not_exists(Composite)
    await t._test_audit_categories_part2(Composite)
    await t._test_audit_categories_part3(Composite)
    await t._test_prepare(Composite)
    await t._test_batch(Composite)
    await t._test_batch_native_protocol(Composite)


# AuditBackendComposite, auth, rf=1

async def test_audit_composite_auth(manager: ManagerClient):
    """Composite backend (table+syslog), auth enabled, single node."""
    t = CQLAuditTester(manager)
    Composite = functools.partial(AuditBackendComposite, socket_path=syslog_socket_path)
    await t._test_user_password_masking(Composite)
    await t._test_role_password_masking(Composite)
    await t._test_permissions(Composite)


_syslog = functools.partial(AuditBackendSyslog, socket_path=syslog_socket_path)
_composite = functools.partial(AuditBackendComposite, socket_path=syslog_socket_path)


@pytest.mark.parametrize("helper_class,config_changer", [
    pytest.param(AuditBackendTable, CQLAuditTester.AuditSighupConfigChanger, id="table-sighup"),
    pytest.param(AuditBackendTable, CQLAuditTester.AuditCqlConfigChanger, id="table-cql"),
    pytest.param(_syslog, CQLAuditTester.AuditSighupConfigChanger, id="syslog-sighup"),
    pytest.param(_syslog, CQLAuditTester.AuditCqlConfigChanger, id="syslog-cql"),
    pytest.param(_composite, CQLAuditTester.AuditSighupConfigChanger, id="composite-sighup"),
    pytest.param(_composite, CQLAuditTester.AuditCqlConfigChanger, id="composite-cql"),
])
async def test_config_no_liveupdate(manager: ManagerClient, helper_class, config_changer):
    """Non-live audit config params (audit, audit_unix_socket_path, audit_syslog_write_buffer_size) must be unmodifiable."""
    await CQLAuditTester(manager)._test_config_no_liveupdate(helper_class, config_changer)


@pytest.mark.parametrize("helper_class,config_changer", [
    pytest.param(AuditBackendTable, CQLAuditTester.AuditSighupConfigChanger, id="table-sighup"),
    pytest.param(AuditBackendTable, CQLAuditTester.AuditCqlConfigChanger, id="table-cql"),
    pytest.param(_syslog, CQLAuditTester.AuditSighupConfigChanger, id="syslog-sighup"),
    pytest.param(_syslog, CQLAuditTester.AuditCqlConfigChanger, id="syslog-cql"),
    pytest.param(_composite, CQLAuditTester.AuditSighupConfigChanger, id="composite-sighup"),
    pytest.param(_composite, CQLAuditTester.AuditCqlConfigChanger, id="composite-cql"),
])
async def test_config_liveupdate(manager: ManagerClient, helper_class, config_changer):
    """Live-updatable audit config params (categories, keyspaces, tables) must be modifiable at runtime."""
    await CQLAuditTester(manager)._test_config_liveupdate(helper_class, config_changer)


@pytest.mark.parametrize("helper_class", [
    pytest.param(AuditBackendTable, id="table"),
    pytest.param(_syslog, id="syslog"),
    pytest.param(_composite, id="composite"),
])
async def test_parallel_syslog_audit(manager: ManagerClient, helper_class):
    """Cluster must not fail when multiple queries are audited in parallel."""
    await CQLAuditTester(manager)._test_parallel_syslog_audit(helper_class)

@pytest.mark.asyncio
async def test_upgrade_preserves_ddl_audit_for_tables(
        manager: ManagerClient,
        scylla_2025_1: ScyllaVersionDescription,
        scylla_binary: Path):
    """Verify that upgrading from 2025.1 to master preserves DDL auditing
    for table-scoped audit configurations (SCYLLADB-1155).
    """
    keyspace = "test_audit_upgrade_ks"
    table = "audited_tbl"
    fq_table = f"{keyspace}.{table}"

    audit_settings = {
        "audit": "table",
        "audit_tables": fq_table,
        "audit_keyspaces": keyspace,
    }

    logger.info("Starting server with version 2025.1 and DDL audit config")
    server = await manager.server_add(
        version=scylla_2025_1,
        config=audit_settings,
    )
    cql, _ = await manager.get_ready_cql([server])

    await cql.run_async(
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace}"
        f" WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
    await cql.run_async(f"CREATE TABLE {fq_table} (pk int PRIMARY KEY, v int)")

    t = CQLAuditTester(manager, helper=AuditBackendTable())
    t.server_addresses = [server.ip_addr]
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE

    logger.info("Verifying DDL is audited before upgrade (2025.1)")
    t.helper.clear_audit_logs(cql)
    t.execute_and_validate_new_audit_entry(
        cql,
        f"ALTER TABLE {fq_table} ADD v2 int",
        category="DDL",
        table=table,
        ks=keyspace,
    )

    logger.info("Upgrading server to current binary")
    await manager.server_change_version(server.server_id, scylla_binary)
    cql, _ = await manager.get_ready_cql([server])
    cql.get_execution_profile(EXEC_PROFILE_DEFAULT).consistency_level = ConsistencyLevel.ONE

    logger.info("Verifying DDL is audited after upgrade (master)")
    t.helper.clear_audit_logs(cql)
    t.execute_and_validate_new_audit_entry(
        cql,
        f"ALTER TABLE {fq_table} ADD v3 int",
        category="DDL",
        table=table,
        ks=keyspace,
    )


# Alternator audit regression test

async def test_alternator_basic_ops_audit_disabled(manager: ManagerClient):
    # Basic Alternator operations must not crash when audit is disabled.
    config = alternator_config | {'audit': 'none'}
    server = await manager.server_add(config=config)
    alternator = get_alternator(server.ip_addr)
    client = alternator.meta.client

    table_name = unique_table_name()
    try:
        # DDL: CreateTable
        table = alternator.create_table(
            TableName=table_name,
            BillingMode='PAY_PER_REQUEST',
            KeySchema=[
                {'AttributeName': 'p', 'KeyType': 'HASH'},
                {'AttributeName': 'c', 'KeyType': 'RANGE'},
            ],
            AttributeDefinitions=[
                {'AttributeName': 'p', 'AttributeType': 'S'},
                {'AttributeName': 'c', 'AttributeType': 'S'},
            ],
        )
        # DML: PutItem
        table.put_item(Item={'p': 'pk0', 'c': 'ck0', 'v': 'val0'})
        # DML: UpdateItem
        table.update_item(
            Key={'p': 'pk0', 'c': 'ck0'},
            AttributeUpdates={'v': {'Value': 'updated0', 'Action': 'PUT'}},
        )
        # QUERY: GetItem — verify data correctness
        resp = table.get_item(Key={'p': 'pk0', 'c': 'ck0'}, ConsistentRead=True)
        assert resp['Item']['v'] == 'updated0'
        # QUERY: Query
        qr = table.query(
            KeyConditionExpression='p = :p',
            ExpressionAttributeValues={':p': 'pk0'},
            ConsistentRead=True,
        )
        assert qr['Count'] >= 1
        # QUERY: Scan
        sr = table.scan(ConsistentRead=True)
        assert sr['Count'] >= 1
        # DML: DeleteItem
        table.delete_item(Key={'p': 'pk0', 'c': 'ck0'})
        # Verify deletion
        resp = table.get_item(Key={'p': 'pk0', 'c': 'ck0'}, ConsistentRead=True)
        assert 'Item' not in resp
    finally:
        try:
            client.delete_table(TableName=table_name)
        except ClientError:
            pass
