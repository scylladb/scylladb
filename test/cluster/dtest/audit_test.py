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
import requests
from cassandra import AlreadyExists, AuthenticationFailed, ConsistencyLevel, InvalidRequest, Unauthorized, Unavailable, WriteFailure
from cassandra.cluster import NoHostAvailable, Session
from cassandra.query import SimpleStatement, named_tuple_factory
from ccmlib.scylla_node import ScyllaNode, NodeError

from dtest_class import Tester, create_ks, wait_for
from tools.assertions import assert_invalid
from tools.cluster import run_rest_api
from tools.data import rows_to_list, run_in_parallel

from test.pylib.rest_client import read_barrier

logger = logging.getLogger(__name__)


class AuditRowMustNotExistError(Exception):
    pass


class AuditTester(Tester):
    audit_default_settings = {"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}

    def prepare(  # noqa: PLR0913
        self,
        ordered=False,
        create_keyspace=True,
        use_cache=False,
        nodes=1,
        rf=1,
        protocol_version=None,
        user=None,
        password=None,
        audit_settings=audit_default_settings,
        reload_config=False,
        helper=None,
        **kwargs,
    ):
        logger.debug(f"Preparing cluster with {nodes} node(s): rf={rf} ordered={ordered} use_cache={use_cache} audit_settings={audit_settings}")

        cluster = self.cluster

        cluster.set_configuration_options(values={"logger_log_level": {"audit": "debug"}})

        if helper is None:
            self.helper = AuditBackendTable()
        else:
            self.helper = helper
        self.helper.before_cluster_start()
        audit_settings = self.helper.update_audit_settings(audit_settings)

        if ordered:
            cluster.set_partitioner("org.apache.cassandra.dht.ByteOrderedPartitioner")

        if use_cache:
            cluster.set_configuration_options(values={"row_cache_size_in_mb": 100})

        jvm_args = kwargs.pop("jvm_args", [])

        cluster.set_configuration_options(values=audit_settings)

        if user:
            config = {"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator", "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer", "permissions_validity_in_ms": 0}
            cluster.set_configuration_options(values=config)

        if reload_config:
            # The cluster is restarted to reload the config file.
            cluster.stop()
            cluster.start(wait_for_binary_proto=True)

        if not cluster.nodelist():
            if not isinstance(nodes, dict):
                nodes = [nodes]
            cluster.populate(nodes).start(wait_for_binary_proto=True, jvm_args=jvm_args)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1, protocol_version=protocol_version, user=user, password=password)
        if create_keyspace:
            session.execute("DROP KEYSPACE IF EXISTS ks")
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
    def __init__(self) -> None:
        super().__init__()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def before_cluster_start(self):
        pass

    def get_audit_log_list(self, session, consistency_level):
        raise NotImplementedError


class AuditBackendTable(AuditBackend):
    AUDIT_LOG_QUERY = "SELECT * FROM audit.audit_log"

    audit_default_settings = {"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}

    def __init__(self):
        super().__init__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def update_audit_settings(self, audit_settings, modifiers=None):
        if modifiers is None:
            modifiers = {}
        new_audit_settings = copy.deepcopy(audit_settings or self.audit_default_settings)
        for key in modifiers:
            new_audit_settings[key] = modifiers[key]
        return new_audit_settings

    @override
    def get_audit_log_list(self, session, consistency_level):
        """_summary_
        returns a sorted list of audit log, the logs are sorted by the event times (time-uuid)
        with the node as tie breaker.
        """
        # We would like to have named tuples as results so we can verify the
        # order in which the fields are returned as the tests make assumptions about this.
        assert session.row_factory == named_tuple_factory
        res = session.execute(SimpleStatement(self.AUDIT_LOG_QUERY, consistency_level=consistency_level))
        res_list = list(res)
        res_list.sort(key=lambda row: (row.event_time.time, row.node))
        return res_list


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

    def __init__(self):
        super().__init__()
        self.update_socket_path()
        self.unix_socket_listener = UnixSockerListener(self.socket_path)
        self.named_tuple_factory = namedtuple("Row", ["date", "node", "event_time", "category", "consistency", "error", "keyspace_name", "operation", "source", "table_name", "username"])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.unix_socket_listener.shutdown()
        if os.path.exists(self.socket_path):
            os.remove(self.socket_path)

    def update_audit_settings(self, audit_settings, modifiers=None):
        if modifiers is None:
            modifiers = {}
        new_audit_settings = copy.deepcopy(audit_settings or self.audit_default_settings)
        if "audit" in new_audit_settings and new_audit_settings["audit"] == "table":
            new_audit_settings["audit"] = "syslog"
        new_audit_settings["audit_unix_socket_path"] = self.socket_path
        for key in modifiers:
            new_audit_settings[key] = modifiers[key]
        return new_audit_settings

    @override
    def get_audit_log_list(self, session, consistency_level):
        lines = self.unix_socket_listener.get_lines()
        entries = []
        for idx, line in enumerate(lines):
            entries.append(self.line_to_row(line, idx))
        return entries

    def line_to_row(self, line, idx):
        metadata, data = line.split(": ", 1)
        data = "".join(data.splitlines()) # Remove newlines
        fields = ["node", "category", "cl", "error", "keyspace", "query", "client_ip", "table", "username"]
        regexp = ", ".join(f"{field}=\"(?P<{field}>.*)\"" for field in fields)
        match = re.match(regexp, data)

        date = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        node = match.group("node").split(":")[0]
        statement = match.group("query").replace("\\", "") 
        source = match.group("client_ip").split(":")[0]
        event_time = uuid.UUID(int=idx)
        t = self.named_tuple_factory(date, node, event_time, match.group("category"), match.group("cl"), match.group("error") == "true", match.group("keyspace"), statement, source, match.group("table"), match.group("username"))
        return t

    def update_socket_path(self):
        new_socket_path = tempfile.mktemp(prefix="/tmp/scylla-audit-", suffix=".socket")
        self.socket_path = new_socket_path


@pytest.mark.single_node
class TestCQLAudit(AuditTester):
    """
    Make sure CQL statements are audited
    """

    AUDIT_LOG_QUERY = "SELECT * FROM audit.audit_log"

    def deduplicate_audit_entries(self, entries):
        """
        Returns a list of audit entries with duplicate entries removed.
        """
        unique = set()
        deduplicated_entries = []

        for entry in entries:
            fields_subset = (entry.node, entry.category, entry.consistency, entry.error, entry.keyspace_name, entry.operation, entry.source, entry.table_name, entry.username)

            if fields_subset in unique:
                continue

            unique.add(fields_subset)
            deduplicated_entries.append(entry)

        return deduplicated_entries

    def get_audit_log_list(self, session):
        """_summary_
        returns a sorted list of audit log, the logs are sorted by the event times (time-uuid)
        with the node as tie breaker.
        """
        consistency_level = ConsistencyLevel.QUORUM if len(self.cluster.nodelist()) > 1 else ConsistencyLevel.ONE
        log_list = self.helper.get_audit_log_list(session, consistency_level)
        logger.debug(f"get_audit_log_list: {log_list}")
        return log_list

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
        assert row.node in map(lambda x: x.address(), self.cluster.nodelist())
        assert row.category == category
        assert row.consistency == cl
        assert row.error == error
        assert row.keyspace_name == ks
        assert row.operation == statement
        assert row.source == "127.0.0.1"
        assert row.table_name == table
        assert row.username == user

    def get_audit_entries_count(self, session):
        res_list = self.get_audit_log_list(session)
        res_list = self.filter_out_noise(res_list, filter_out_all_auth=True, filter_out_use=True)
        logger.debug("Printing audit table content:")
        for row in res_list:
            logger.debug("  %s", row)
        return len(res_list)

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

    def get_partitions_for_token(self, ks, table, token):
        partitions = set()
        for node in self.cluster.nodelist():
            result = run_rest_api(node, f"/storage_service/describe_ring/{ks}", api_method="GET", params={"table": table})
            ranges = result.json()
            for token_range in ranges:
                start_token = int(token_range["start_token"])
                end_token = int(token_range["end_token"])
                if self.token_in_range(token, start_token, end_token):
                    logger.info(f"Found token {token} in token_range: {token_range}")
                    for e in token_range["endpoints"]:
                        partitions.add(e)
        return partitions

    def get_audit_partitions_for_operation(self, session, operation):
        logger.info(f"get_audit_partitions_for_operation: {operation}")
        rows = rows_to_list(session.execute(f"SELECT token(date, node) FROM audit.audit_log WHERE operation = %s ALLOW FILTERING", [operation]))
        if len(rows) == 0:
            return None
        token = rows[0][0]

        paritions = self.get_partitions_for_token("audit", "audit_log", token)

        assert len(paritions) > 0
        by_address = {node.address(): node for node in self.cluster.nodelist()}
        return list(map(lambda p: by_address[p], paritions))

    @contextmanager
    def assert_exactly_n_audit_entries_were_added(self, session: Session, expected_entries: int):
        count_before = self.get_audit_entries_count(session)
        yield
        count_after = self.get_audit_entries_count(session)
        assert count_after == count_before + expected_entries, f"Expected {expected_entries} new audit entries, but got {count_after - count_before} new entries"

    @contextmanager
    def assert_no_audit_entries_were_added(self, session):
        count_before = self.get_audit_entries_count(session)
        yield
        count_after = self.get_audit_entries_count(session)
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
    def filter_out_noise(self, rows, filter_out_all_auth=False, filter_out_cassandra_auth=False, filter_out_use=False):
        if filter_out_all_auth:
            rows = [row for row in rows if row.category != "AUTH"]
        if filter_out_cassandra_auth:
            rows = [row for row in rows if not (row.category == "AUTH" and row.username == "cassandra")]
        if filter_out_use:
            rows = [row for row in rows if "USE " not in row.operation]
        return rows

    @contextmanager
    def assert_entries_were_added(self, session: Session, expected_entries: list[AuditEntry], merge_duplicate_rows: bool = True, filter_out_cassandra_auth: bool = False):
        # Get audit entries before executing the query, to later compare with
        # audit entries after executing the query.
        rows_before = self.get_audit_log_list(session)
        set_of_rows_before = set(rows_before)
        assert len(set_of_rows_before) == len(rows_before), f"audit table contains duplicate rows: {rows_before}"

        yield

        new_rows = []
        def is_number_of_new_rows_correct():
            rows_after = self.get_audit_log_list(session)
            set_of_rows_after = set(rows_after)
            assert len(set_of_rows_after) == len(rows_after), f"audit table contains duplicate rows: {rows_after}"

            nonlocal new_rows
            new_rows = rows_after[len(rows_before) :]
            assert set(new_rows) == set_of_rows_after - set_of_rows_before, f"new rows are not the last rows in the audit table: rows_after={rows_after}, set_of_rows_after={set_of_rows_after}, set_of_rows_before={set_of_rows_before}"

            if merge_duplicate_rows:
                new_rows = self.deduplicate_audit_entries(new_rows)

            auth_not_expected = (len([entry for entry in expected_entries if entry.category == "AUTH"]) == 0)
            use_not_expected = (len([entry for entry in expected_entries if "USE " in entry.statement]) == 0)

            new_rows = self.filter_out_noise(
                new_rows,
                filter_out_all_auth=auth_not_expected,
                filter_out_cassandra_auth=filter_out_cassandra_auth,
                filter_out_use=use_not_expected
            )

            assert len(new_rows) <= len(expected_entries)
            return len(new_rows) == len(expected_entries)

        wait_for(is_number_of_new_rows_correct, timeout=60)
        sorted_new_rows = sorted(new_rows, key=lambda row: (row.node, row.category, row.consistency, row.error, row.keyspace_name, row.operation, row.source, row.table_name, row.username))
        assert len(sorted_new_rows) == len(expected_entries)
        for row, entry in zip(sorted_new_rows, sorted(expected_entries)):
            self.assert_audit_row_eq(row, entry.category, entry.statement, entry.table, entry.ks, entry.user, entry.cl, entry.error)

    def verify_keyspace(self, audit_settings=None, helper=None):
        """
        CREATE KEYSPACE, USE KEYSPACE, ALTER KEYSPACE, DROP KEYSPACE statements
        """
        session = self.prepare(create_keyspace=False, audit_settings=audit_settings, helper=helper)

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

    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog])
    def test_using_non_existent_keyspace(self, helper_class):
        """
        Test tha using a non-existent keyspace generates an audit entry with an
        error field set to True.
        """

        with helper_class() as helper:
            session = self.prepare(create_keyspace=False, helper=helper)

            self.execute_and_validate_audit_entry(
                session,
                'USE "ks"',  # ks doesn't exist because create_keyspace=False in prepare
                category="DML",
                expected_error=InvalidRequest,
            )

    def verify_table(self, audit_settings=AuditTester.audit_default_settings, helper=None, table_prefix="test", overwrite_audit_tables=False):
        """
        CREATE TABLE, ALTER TABLE, TRUNCATE TABLE, DROP TABLE statements
        """
        first_table = f"{table_prefix}_1_{uuid.uuid4().hex[:4]}"
        second_table = f"{table_prefix}_2_{uuid.uuid4().hex[:4]}"

        if helper is None:
            helper = AuditBackendTable()

        if overwrite_audit_tables:
            audit_settings["audit_tables"] = f"ks.{first_table}, ks.{second_table}"

        session = self.prepare(audit_settings=audit_settings, helper=helper)

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
        ]

        with self.assert_no_audit_entries_were_added(session):
            for query in query_sequence:
                session.execute(query)

    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog])
    def test_audit_keyspace(self, helper_class):
        with helper_class() as helper:
            self.verify_keyspace(audit_settings=AuditTester.audit_default_settings, helper=helper)

    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog])
    def test_audit_keyspace_extra_parameter(self, helper_class):
        with helper_class() as helper:
            self.verify_keyspace(audit_settings={"audit": "table", "audit_categories": "ADMIN,AUTH,DML,DDL,DCL", "audit_keyspaces": "ks", "extra_parameter": "new"}, helper=helper)

    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog])
    def test_audit_keyspace_many_ks(self, helper_class):
        with helper_class() as helper:
            self.verify_keyspace(audit_settings={"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "a,b,c,ks"}, helper=helper)

    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog])
    def test_audit_keyspace_table_not_exists(self, helper_class):
        with helper_class() as helper:
            self.verify_keyspace(audit_settings={"audit": "table", "audit_categories": "DML,DDL", "audit_keyspaces": "ks", "audit_tables": "ks.fake"}, helper=helper)

    def test_audit_type_none(self):
        """
        'audit': None
         CREATE KEYSPACE, USE KEYSPACE, ALTER KEYSPACE, DROP KEYSPACE statements
         check audit KS not created
        """

        audit_settings = {"audit": None, "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}

        session = self.prepare(create_keyspace=False, audit_settings=audit_settings)

        session.execute("CREATE KEYSPACE ks WITH replication = { 'class':'SimpleStrategy', 'replication_factor':1} AND DURABLE_WRITES = true")

        session.execute("USE ks")

        session.execute("ALTER KEYSPACE ks WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND DURABLE_WRITES = false")

        session.execute("DROP KEYSPACE ks")
        assert_invalid(session, "use audit;", expected=InvalidRequest)

    def test_audit_type_invalid(self):
        """
        'audit': invalid
         check node not started
        """
        self.fixture_dtest_setup.allow_log_errors = True

        audit_settings = {"audit": "invalid", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}

        cluster = self.cluster

        cluster.set_configuration_options(values=audit_settings)

        try:
            cluster.populate(1).start(no_wait=True)
        except (NodeError, RuntimeError):
            pass

        expected_error = r"Startup failed: audit::audit_exception \(Bad configuration: invalid 'audit': invalid\)"
        self.ignore_log_patterns.append(expected_error)
        self.cluster.nodes["node1"].watch_log_for(expected_error)

    # TODO: verify that the syslog file doesn't exist
    def test_audit_empty_settings(self):
        """
        'audit': none
         check node started, ks audit not created
        """
        session = self.prepare(create_keyspace=False, audit_settings={"audit": "none"})
        assert_invalid(session, "use audit;", expected=InvalidRequest)

    def test_audit_audit_ks(self):
        """
        'audit_keyspaces': 'audit'
        check node started, ks audit created
        """
        audit_settings = {"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "audit"}
        session = self.prepare(create_keyspace=False, audit_settings=audit_settings)

        self.execute_and_validate_audit_entry(session, query=self.AUDIT_LOG_QUERY, category="QUERY", ks="audit", table="audit_log", audit_settings=audit_settings)

    @pytest.mark.single_node
    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog])
    def test_audit_categories_invalid(self, helper_class):
        """
        'audit_categories': invalid
        check node not started
        """
        with helper_class() as helper:
            self.fixture_dtest_setup.allow_log_errors = True

            audit_settings = {"audit": "table", "audit_categories": "INVALID", "audit_keyspaces": "ks"}

            cluster = self.cluster

            cluster.set_configuration_options(values=audit_settings)
            cluster.force_wait_for_cluster_start = False

            try:
                cluster.populate(1).start(no_wait=True)
            except NodeError:
                pass
            expected_error = r"Startup failed: audit::audit_exception \(Bad configuration: invalid 'audit_categories': INVALID\)"
            self.ignore_log_patterns.append(expected_error)
            self.cluster.nodes["node1"].watch_log_for(expected_error)

    # compact storage is current required for all tests that call verify_table
    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_audit_table(self):
        self.verify_table(audit_settings=AuditTester.audit_default_settings, table_prefix="test_audit_table")

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_audit_table_extra_parameter(self):
        self.verify_table(audit_settings={"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks", "extra_parameter": "new"}, table_prefix="test_audit_table_extra_parameter")

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_audit_table_audit_keyspaces_empty(self):
        self.verify_table(audit_settings={"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL"}, table_prefix="test_audit_table_audit_keyspaces_empty", overwrite_audit_tables=True)

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_audit_table_no_ks(self):
        self.verify_table(audit_settings={"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL"}, table_prefix="test_audit_table_no_ks", overwrite_audit_tables=True)

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    def test_audit_categories_part1(self):
        self.verify_table(audit_settings={"audit": "table", "audit_categories": "AUTH,QUERY,DDL"}, table_prefix="test_audit_categories_part1", overwrite_audit_tables=True)

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog])
    def test_audit_categories_part2(self, helper_class):
        with helper_class() as helper:
            self.verify_table(audit_settings={"audit": "table", "audit_categories": "DDL, ADMIN,AUTH,DCL", "audit_keyspaces": "ks"}, helper=helper, table_prefix="test_audit_categories_part2")

    @pytest.mark.cluster_options(enable_create_table_with_compact_storage=True)
    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog])
    def test_audit_categories_part3(self, helper_class):
        with helper_class() as helper:
            self.verify_table(audit_settings={"audit": "table", "audit_categories": "DDL, ADMIN,AUTH", "audit_keyspaces": "ks"}, helper=helper, table_prefix="test_audit_categories_part3")

    PasswordMaskingCase = namedtuple("PasswordMaskingCase", ["name", "password", "new_password"])

    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog])
    def test_user_password_masking(self, helper_class):
        """
        CREATE USER, ALTER USER, DROP USER statements
        """
        with helper_class() as helper:
            session = self.prepare(user="cassandra", password="cassandra", helper=helper)

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

    def test_negative_audit_records_auth(self):
        """
        Test that failed AUTH attempts are audited.
        """
        session = self.prepare(user="cassandra", password="cassandra")

        expected_entry = AuditEntry(category="AUTH", statement="LOGIN", table="", ks="", user="wrong_user", cl="", error=True)
        with self.assert_entries_were_added(session, [expected_entry], filter_out_cassandra_auth=True):
            try:
                bad_session = self.exclusive_cql_connection(self.cluster.nodelist()[0], user="wrong_user", password="wrong_password")
                pytest.fail()
            except NoHostAvailable as e:
                errors = e.errors.values()
                assert len(errors) == 1
                error = next(iter(errors))
                assert isinstance(error, AuthenticationFailed)

    def test_negative_audit_records_admin(self):
        """
        Test that failed ADMIN statements are audited.
        """
        session = self.prepare(user="cassandra", password="cassandra")
        session.execute("CREATE ROLE test_role")

        stmt = "ATTACH SERVICE_LEVEL test_service_level TO test_role"

        expected_entry = AuditEntry(category="ADMIN", table="", ks="", user="cassandra", cl="ONE", error=True, statement=stmt)

        with self.assert_entries_were_added(session, [expected_entry]):
            assert_invalid(session, stmt, expected=InvalidRequest)

    def test_negative_audit_records_ddl(self):
        """
        Test that failed DDL statements are audited.
        """
        session = self.prepare(user="cassandra", password="cassandra")

        stmt = "CREATE KEYSPACE ks WITH replication = { 'class':'NetworkTopologyStrategy', 'replication_factor': 3 }"

        expected_entry = AuditEntry(category="DDL", table="", ks="ks", user="cassandra", cl="ONE", error=True, statement=stmt)

        with self.assert_entries_were_added(session, [expected_entry]):
            assert_invalid(session, stmt, expected=AlreadyExists)

    def test_negative_audit_records_dml(self):
        """
        Test that failed DML statements are audited.
        """
        audit_settings = {"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}
        session = self.prepare(user="cassandra", password="cassandra", audit_settings=audit_settings)

        session.execute("CREATE TABLE test1 (k int PRIMARY KEY, v1 int, v2 int)")

        stmt = SimpleStatement("INSERT INTO ks.test1 (k, v1, v2) VALUES (1, 1, 1)", consistency_level=ConsistencyLevel.TWO)

        expected_entry = AuditEntry(category="DML", table="test1", ks="ks", user="cassandra", cl="TWO", error=True, statement=stmt.query_string)

        with self.assert_entries_were_added(session, [expected_entry]):
            assert_invalid(session, stmt, expected=Unavailable)

    def test_negative_audit_records_dcl(self):
        """
        Test that failed DCL statements are audited.
        """
        session = self.prepare(user="cassandra", password="cassandra")

        stmt = "GRANT SELECT ON ALL KEYSPACES TO test_role"

        expected_entry = AuditEntry(category="DCL", table="", ks="", user="cassandra", cl="ONE", error=True, statement=stmt)

        with self.assert_entries_were_added(session, [expected_entry]):
            assert_invalid(session, stmt, expected=InvalidRequest)

    def test_negative_audit_records_query(self):
        """
        Test that failed QUERY statements are audited.
        """
        audit_settings = {"audit": "table", "audit_categories": "ADMIN,AUTH,QUERY,DML,DDL,DCL", "audit_keyspaces": "ks"}
        session = self.prepare(audit_settings=audit_settings)

        session.execute("CREATE TABLE test1 (k int PRIMARY KEY, v int)")

        stmt = SimpleStatement("SELECT * FROM ks.test1", consistency_level=ConsistencyLevel.TWO)

        expected_entry = AuditEntry(category="QUERY", table="test1", ks="ks", user="anonymous", cl="TWO", error=True, statement=stmt.query_string)

        with self.assert_entries_were_added(session, [expected_entry]):
            assert_invalid(session, stmt, expected=Unavailable)

    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog])
    def test_role_password_masking(self, helper_class):
        """
        CREATE ROLE, ALTER ROLE, DROP ROLE statements
        """

        with helper_class() as helper:
            session = self.prepare(user="cassandra", password="cassandra", helper=helper)

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

    def test_login(self):
        """
        USER LOGIN
        """
        session = self.prepare(user="cassandra", password="cassandra", create_keyspace=False)
        session.execute("CREATE USER test WITH PASSWORD 'test'")

        expected_audit_entries = [AuditEntry(category="AUTH", statement="LOGIN", user="test", table="", ks="", cl="", error=False)]

        with self.assert_entries_were_added(session, expected_audit_entries, filter_out_cassandra_auth=True):
            self.prepare(user="test", password="test", create_keyspace=False)

    def test_cassandra_login(self):
        """
        Test user login to default (cassandra) user
        """
        session = self.prepare(user="cassandra", password="cassandra", create_keyspace=False)
        expected_audit_entries = [AuditEntry(category="AUTH", statement="LOGIN", user="cassandra", table="", ks="", cl="", error=False)]

        with self.assert_entries_were_added(session, expected_audit_entries, filter_out_cassandra_auth=False):
            self.prepare(user="cassandra", password="cassandra", create_keyspace=False)

    def test_categories(self):
        """
        Test filtering audit categories
        """
        session = self.prepare(experimental=True, audit_settings={"audit": "table", "audit_categories": "DML", "audit_keyspaces": "ks"})

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

    def _test_insert_failure_doesnt_report_success_assign_nodes(self, session: Session = None):
        all_nodes: set[ScyllaNode] = set(self.cluster.nodelist())
        assert len(all_nodes) == 7

        address_to_node = {node.address(): node for node in all_nodes}
        node_to_audit_nodes = {}

        for index, node in enumerate(all_nodes):
            with self.patient_exclusive_cql_connection(node) as conn:
                stmt = SimpleStatement(f"INSERT INTO ks.test1 (k, v1) VALUES (1000, 1000)", consistency_level=ConsistencyLevel.THREE)
                conn.execute(stmt)
                audit_nodes = self.get_audit_partitions_for_operation(session, stmt.query_string)
                node_to_audit_nodes[index] = set([node.address() for node in audit_nodes])

        all_nodes_addresses = set([node.address() for node in all_nodes])
        # the number here is arbitrary
        for i in range(256):
            stmt = SimpleStatement(f"INSERT INTO ks.test1 (k, v1) VALUES ({i}, 1337)", consistency_level=ConsistencyLevel.THREE)
            session.execute(stmt)

            token = rows_to_list(session.execute(f"SELECT token(k) FROM ks.test1 WHERE k = {i}"))[0][0]

            partitions = self.get_partitions_for_token("ks", "test1", token)

            for index, audit_nodes in node_to_audit_nodes.items():
                common = audit_nodes & partitions
                if not common:
                    insert_node = all_nodes_addresses - partitions - audit_nodes
                    if len(all_nodes_addresses) != 7 or len(partitions) != 3 or len(audit_nodes) != 3 or len(insert_node) != 1:
                        raise pytest.skip(f"Failed to assign nodes for insert failure test")
                    audit_partition_nodes = [address_to_node[address] for address in audit_nodes]
                    insert_node = address_to_node[insert_node.pop()]
                    kill_node = address_to_node[partitions.pop()]
                    return audit_partition_nodes, insert_node, kill_node, stmt.query_string

        return [], [], None, None

    @pytest.mark.exclude_errors("audit - Unexpected exception when writing log with: node_ip")
    def test_insert_failure_doesnt_report_success(self):
        """
        Test that if an insert fails, the audit log doesn't report the insert
        as successful.

        The test works by creating a table with RF=3, and then stopping one of
        the nodes. Then, an insert is executed with CL=THREE. This insert will
        fail, since the node that is stopped is a replica for the inserted
        partition. The test verifies that the audit log doesn't report the
        insert as successful and reports unsuccessful inserts as expected.
        """
        cluster_topology = {"datacenter1": {"rack1": 3, "rack2": 2, "rack3": 2}}
        session: Session = self.prepare(nodes=cluster_topology, rf=3)

        with self.patient_exclusive_cql_connection(self.cluster.nodelist()[0]) as conn:
            stmt = SimpleStatement("CREATE TABLE ks.test1 (k int PRIMARY KEY, v1 int)")
            with self.assert_exactly_n_audit_entries_were_added(session, 1):
                conn.execute(stmt)

        audit_paritition_nodes, insert_node, node_to_stop, query_to_fail = self._test_insert_failure_doesnt_report_success_assign_nodes(session=session)

        # TODO: remove the loop when scylladb#24473 is fixed
        # We call get_host_id only to cache host_id
        for node in audit_paritition_nodes + [insert_node] + [node_to_stop]:
            node.cluster.manager.get_host_id(node.server_id) 

        if len(audit_paritition_nodes) != 3 or node_to_stop is None or insert_node is None:
            raise pytest.skip(f"Failed to assign nodes for insert failure test")

        for audit_node in audit_paritition_nodes:
            logger.debug(f"audit_paritition_nodes: {audit_node.name} {audit_node.address()}")
        logger.debug(f"node_to_stop: {node_to_stop.name} {node_to_stop.address()}")
        logger.debug(f"insert_node: {insert_node.name} {insert_node.address()}")

        with self.patient_exclusive_cql_connection(insert_node) as conn:
            stmt = SimpleStatement("INSERT INTO ks.test1 (k, v1) VALUES (1, 1)", consistency_level=ConsistencyLevel.THREE)
            with self.assert_exactly_n_audit_entries_were_added(session, 1):
                conn.execute(stmt)

        node_to_stop.stop(wait_other_notice=True)
        with self.patient_exclusive_cql_connection(insert_node) as conn:
            with pytest.raises(Unavailable):
                stmt = SimpleStatement(query_to_fail, consistency_level=ConsistencyLevel.THREE)
                conn.execute(stmt)
                pytest.fail("Expected insert to fail")
        node_to_stop.start(wait_for_binary_proto=True, wait_other_notice=True)

        rows = []
        timestamp_before = datetime.datetime.now()
        for i in itertools.count(start=1):
            if datetime.datetime.now() - timestamp_before > datetime.timedelta(seconds=60):
                pytest.fail(f"audit log not updated after {i} iterations")

            rows = self.get_audit_log_list(session)
            rows_with_error = list(filter(lambda r: r.error, rows))
            if len(rows_with_error) == 6:
                logger.info(f"audit log updated after {i} iterations ({i / 10}s)")
                assert rows_with_error[0].error is True
                assert rows_with_error[0].consistency == "THREE"

                # We expect the initial insert to be in the audit log.
                # it is executed in _test_insert_failure_doesnt_report_success_assign_nodes
                rows_without_error = [row for row in rows if row.operation == query_to_fail and not row.error]
                assert len(rows_without_error) == 1
                break

    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog])
    def test_prepare(self, helper_class):
        """Test prepare statement"""
        with helper_class() as helper:
            session = self.prepare(helper=helper)

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

    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog])
    def test_permissions(self, helper_class):
        """Test user permissions"""

        with helper_class() as helper:
            session = self.prepare(user="cassandra", password="cassandra", helper=helper)
            session.execute("CREATE TABLE test1 (k int PRIMARY KEY, v1 int)")
            session.execute("CREATE USER test WITH PASSWORD 'test'")
            session.execute("GRANT SELECT ON ks.test1 TO test")
            session.execute("INSERT INTO test1 (k, v1) VALUES (1, 1)")

            logging.info("Waiting for appling permissions")
            for srv in self.cluster.manager.running_servers():
                asyncio.run(read_barrier(self.cluster.manager.api, srv.ip_addr))

            test_session = self.patient_cql_connection(self.cluster.nodelist()[0], user="test", password="test")

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

    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog])
    def test_batch(self, helper_class):
        """
        BATCH statement
        """
        with helper_class() as helper:
            session = self.prepare(helper=helper)

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

    def test_service_level_statements(self):
        """
        Test auditing service level statements - ones that use the ADMIN audit category.
        """
        audit_settings = {"audit": "table", "audit_categories": "ADMIN"}
        session = self.prepare(user="cassandra", password="cassandra", audit_settings=audit_settings, jvm_args=["--smp", "1"])

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
        session = self.prepare(user="cassandra", password="cassandra", audit_settings={"audit_categories": "QUERY,DML,DDL,DCL"}, reload_config=True)

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

        def change_config(self, test, settings, expected_result):
            assert False, "change_config not implemented in AuditConfigChanger"

        def verify_change(self, node, param, value, from_mark, expected_result):
            current_value = requests.get(f"http://{node.address()}:10000/v2/config/{param}").text.replace('"', "")
            match expected_result:
                case self.ExpectedResult.SUCCESS:
                    assert current_value == value, f"response: {current_value}, expected: {value}"
                    node.watch_log_for(r"Audit configuration is updated", from_mark=from_mark, timeout=60)
                case self.ExpectedResult.FAILURE_UNPARSABLE_VALUE:
                    assert current_value == value, f"response: {current_value}, expected: {value}"
                    node.watch_log_for(r"Audit configuration update failed ", from_mark=from_mark, timeout=60)
                case self.ExpectedResult.FAILURE_UNUPDATABLE_PARAM:
                    assert current_value != value, f"response: {current_value}, expected different than: {value}"

    class AuditSighupConfigChanger(AuditConfigChanger):
        def change_config(self, test, settings, expected_result):
            for node in test.cluster.nodelist():
                logger.info(f"Changing config via manager.server_update_config: Node={node.address()} settings={settings}")
                mark = node.mark_log()
                for param, value in settings.items():
                    mark_per_param = node.mark_log()
                    logger.info(f"server_update_config: param={param} value={value}")
                    node.cluster.manager.server_update_config(node.server_id, param, value)
                    node.watch_log_for(r"completed re-reading configuration file", from_mark=mark_per_param, timeout=60)
                for param, value in settings.items():
                    self.verify_change(node, param, value, mark, expected_result)

    class AuditCqlConfigChanger(AuditConfigChanger):
        def change_config(self, test, settings, expected_result):
            for node in test.cluster.nodelist():
                mark = node.mark_log()
                for param in settings:
                    with test.patient_exclusive_cql_connection(node) as session:
                        logger.debug(f"Changing config via CQL: Node={node.address()} param={param} value={settings[param]}")
                        try:
                            session.execute(f"UPDATE system.config SET value='{settings[param]}' where name='{param}';")
                        except WriteFailure:
                            assert expected_result == self.ExpectedResult.FAILURE_UNUPDATABLE_PARAM, f"CQL execution failed but expected_result: {expected_result}"
                for param in settings:
                    self.verify_change(node, param, settings[param], mark, expected_result)

    @pytest.mark.parametrize("audit_config_changer", [AuditSighupConfigChanger, AuditCqlConfigChanger])
    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog])
    def test_config_liveupdate(self, helper_class, audit_config_changer):
        """
        Test liveupdate config changes in audit.
        Liveupdate categories, tables, and keyspaces and confirm proper audit behavior.
        Execute with both audit sinks (Table, Syslog) and both liveupdate methods (SIGHUP, CQL).
        Use multi-node configuration (nodes=3).
        """
        with helper_class() as helper, audit_config_changer() as config_changer:
            self.fixture_dtest_setup.allow_log_errors = True

            default_categories = copy.deepcopy(self.audit_default_settings["audit_categories"])
            enabled_but_empty_audit_config = helper.update_audit_settings(self.audit_default_settings, modifiers={"audit_categories": "", "audit_tables": "", "audit_keyspaces": ""})

            session = self.prepare(helper=helper, nodes=3, audit_settings=enabled_but_empty_audit_config)

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
            config_changer.change_config(self, {"audit_categories": default_categories, "audit_tables": "test.table1,test.table2", "audit_keyspaces": "ks,ks2,ks3"}, expected_result=self.AuditConfigChanger.ExpectedResult.SUCCESS)
            with self.assert_entries_were_added(session, expected_new_entries, merge_duplicate_rows=False):
                session.execute(auditted_query)

            # Conifg modified with invalid categories: audit works, because previous audit configuration is used
            config_changer.change_config(self, {"audit_categories": "INVALID_CATEGORIES"}, expected_result=self.AuditConfigChanger.ExpectedResult.FAILURE_UNPARSABLE_VALUE)
            with self.assert_entries_were_added(session, expected_new_entries, merge_duplicate_rows=False):
                session.execute(auditted_query)

            # Conifg modified with valid categories but invalid tables and and non-existing keyspaces: no auditing
            config_changer.change_config(
                self, {"audit_categories": default_categories, "audit_tables": "invalid.table.twodots", "audit_keyspaces": "non-existing"}, expected_result=self.AuditConfigChanger.ExpectedResult.FAILURE_UNPARSABLE_VALUE
            )
            with self.assert_no_audit_entries_were_added(session):
                session.execute(auditted_query)

            # Config modified with valid tables and empty keyspaces: audit works
            config_changer.change_config(self, {"audit_tables": "ks.test_config_lifeupdate", "audit_keyspaces": ""}, expected_result=self.AuditConfigChanger.ExpectedResult.SUCCESS)
            with self.assert_entries_were_added(session, expected_new_entries, merge_duplicate_rows=False):
                session.execute(auditted_query)

            # Set empty categories, tables, keyspaces: no auditing
            config_changer.change_config(self, {"audit_categories": "", "audit_tables": "", "audit_keyspaces": ""}, expected_result=self.AuditConfigChanger.ExpectedResult.SUCCESS)
            with self.assert_no_audit_entries_were_added(session):
                session.execute(auditted_query)

    @pytest.mark.parametrize("audit_config_changer", [AuditSighupConfigChanger, AuditCqlConfigChanger])
    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog])
    def test_config_no_liveupdate(self, helper_class, audit_config_changer):
        """
        Test audit config parameters that don't allow config changes.
        Modification of "audit", "audit_unix_socket_path", and "audit_syslog_write_buffer_size" should be forbidden.
        """
        with helper_class() as helper, audit_config_changer() as config_changer:
            self.fixture_dtest_setup.allow_log_errors = True
            session = self.prepare(helper=helper)

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
            config_changer.change_config(self, {"audit": "none"}, expected_result=self.AuditConfigChanger.ExpectedResult.FAILURE_UNUPDATABLE_PARAM)
            config_changer.change_config(self, {"audit_unix_socket_path": "/path/"}, expected_result=self.AuditConfigChanger.ExpectedResult.FAILURE_UNUPDATABLE_PARAM)
            config_changer.change_config(self, {"audit_syslog_write_buffer_size": "123123123"}, expected_result=self.AuditConfigChanger.ExpectedResult.FAILURE_UNUPDATABLE_PARAM)

            # Despite unsuccesful attempts to change config, audit works as expected
            with self.assert_entries_were_added(session, expected_new_entries, merge_duplicate_rows=False):
                session.execute(auditted_query)

    @pytest.mark.parametrize("helper_class", [AuditBackendTable, AuditBackendSyslog])
    def test_parallel_syslog_audit(self, helper_class):
        """
        Test that cluster doesn't fail if multiple queries are audited in parallel
        """
        with helper_class() as helper:
            audit_settings = helper.update_audit_settings(self.audit_default_settings, modifiers={"audit_keyspaces": "ks,kss"})
            session = self.prepare(helper=helper, audit_settings=audit_settings)

            session.execute("CREATE KEYSPACE kss WITH replication = { 'class':'NetworkTopologyStrategy', 'replication_factor': 3 }")
            run_in_parallel([{"func": lambda: session.execute("use ks;")}, {"func": lambda: session.execute("use kss;")}] * 1000)
