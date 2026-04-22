#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import ctypes
import hashlib
from collections.abc import Sequence
from unittest import mock

import pytest
from cassandra import ProtocolVersion
from cassandra.application_info import ApplicationInfoBase
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.protocol import ResultMessage

from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from test.pylib.manager_client import ManagerClient
from test.pylib.util import unique_name


class _UseMetadataId(ApplicationInfoBase):
    def add_startup_options(self, options: dict) -> None:
        options["SCYLLA_USE_METADATA_ID"] = ""


_SIZE_T_BYTES = ctypes.sizeof(ctypes.c_size_t)
_UTF8_TYPE = "org.apache.cassandra.db.marshal.UTF8Type"
_BOOLEAN_TYPE = "org.apache.cassandra.db.marshal.BooleanType"
_DURATION_TYPE = "org.apache.cassandra.db.marshal.DurationType"
_INT32_TYPE = "org.apache.cassandra.db.marshal.Int32Type"
_TEXT_MAP_TYPE = f"org.apache.cassandra.db.marshal.MapType({_UTF8_TYPE},{_UTF8_TYPE})"

# Keep these schemas in sync with the corresponding get_result_metadata()
# implementations in cql3/statements/*.cc.
_LIST_ROLES_SCHEMA = (("role", _UTF8_TYPE),
                      ("super", _BOOLEAN_TYPE),
                      ("login", _BOOLEAN_TYPE),
                      ("options", _TEXT_MAP_TYPE))

# cql3/statements/list_users_statement.cc
_LIST_USERS_SCHEMA = (("name", _UTF8_TYPE),
                      ("super", _BOOLEAN_TYPE))

# cql3/statements/list_permissions_statement.cc
_LIST_PERMISSIONS_SCHEMA = (("role", _UTF8_TYPE),
                            ("username", _UTF8_TYPE),
                            ("resource", _UTF8_TYPE),
                            ("permission", _UTF8_TYPE))

# cql3/statements/list_service_level_statement.cc for LIST SERVICE LEVEL <name>
_LIST_SERVICE_LEVEL_SCHEMA = (("service_level", _UTF8_TYPE),
                              ("timeout", _DURATION_TYPE),
                              ("workload_type", _UTF8_TYPE),
                              ("shares", _INT32_TYPE))

# cql3/statements/list_service_level_statement.cc for LIST ALL SERVICE LEVELS
_LIST_ALL_SERVICE_LEVELS_SCHEMA = (*_LIST_SERVICE_LEVEL_SCHEMA,
                                   ("percentage of all service level shares", _UTF8_TYPE))

# cql3/statements/list_service_level_attachments_statement.cc
_LIST_ATTACHED_SERVICE_LEVEL_SCHEMA = (("role", _UTF8_TYPE),
                                       ("service_level", _UTF8_TYPE))

# cql3/statements/list_effective_service_level_statement.cc
_LIST_EFFECTIVE_SERVICE_LEVEL_SCHEMA = (("service_level_option", _UTF8_TYPE),
                                        ("effective_service_level", _UTF8_TYPE),
                                        ("value", _UTF8_TYPE))


def _feed_string_for_metadata_id(hasher, value: str) -> None:
    encoded = value.encode("utf-8")
    hasher.update(
        len(encoded).to_bytes(_SIZE_T_BYTES, byteorder="little", signed=False)
    )
    hasher.update(encoded)


def _calculate_metadata_id(columns: Sequence[tuple[str, str]]) -> bytes:
    # Match cql3::metadata::calculate_metadata_id() and appending_hash<std::string>.
    hasher = hashlib.sha256()
    for column_name, type_name in columns:
        _feed_string_for_metadata_id(hasher, column_name)
        _feed_string_for_metadata_id(hasher, type_name)
    return hasher.digest()[:16]


_LIST_METADATA_CASES: list[tuple[str, bytes]] = [
    ("LIST ROLES OF {role}",
    _calculate_metadata_id(_LIST_ROLES_SCHEMA)),
    ("LIST USERS",
    _calculate_metadata_id(_LIST_USERS_SCHEMA)),
    ("LIST ALL PERMISSIONS",
    _calculate_metadata_id(_LIST_PERMISSIONS_SCHEMA)),
    ("LIST SERVICE LEVEL {service_level}",
    _calculate_metadata_id(_LIST_SERVICE_LEVEL_SCHEMA)),
    ("LIST ALL SERVICE LEVELS",
    _calculate_metadata_id(_LIST_ALL_SERVICE_LEVELS_SCHEMA)),
    ("LIST ATTACHED SERVICE LEVEL OF {role}",
    _calculate_metadata_id(_LIST_ATTACHED_SERVICE_LEVEL_SCHEMA)),
    ("LIST EFFECTIVE SERVICE LEVEL OF {role}",
    _calculate_metadata_id(_LIST_EFFECTIVE_SERVICE_LEVEL_SCHEMA))
]


def _prepare_and_execute(host: str, query: str) -> tuple[bytes, bool, int]:
    captured = {"metadata_id": None, "metadata_changed": False}
    original_recv = ResultMessage.recv_results_metadata

    def _capturing_recv(self: ResultMessage, f, user_type_map) -> None:
        original_recv(self, f, user_type_map)
        metadata_id = getattr(self, "result_metadata_id", None)
        if metadata_id is not None:
            captured["metadata_id"] = metadata_id
            captured["metadata_changed"] = True

    with mock.patch.object(
        ProtocolVersion, "uses_prepared_metadata", staticmethod(lambda _: True)
    ):
        cluster = Cluster(
            contact_points=[host],
            port=9042,
            protocol_version=4,
            auth_provider=PlainTextAuthProvider("cassandra", "cassandra"),
            application_info=_UseMetadataId(),
            load_balancing_policy=WhiteListRoundRobinPolicy([host]))
        session = cluster.connect()
        try:
            prepared = session.prepare(query)
            prepared_metadata_id = prepared.result_metadata_id
            assert prepared_metadata_id is not None
            with mock.patch.object(ResultMessage, "recv_results_metadata", _capturing_recv):
                rows = list(session.execute(prepared))
            return prepared_metadata_id, captured["metadata_changed"], len(rows)
        finally:
            session.shutdown()
            cluster.shutdown()


@pytest.mark.asyncio
async def test_prepared_list_metadata_ids(manager: ManagerClient) -> None:
    servers = await manager.running_servers()
    if servers:
        server = servers[0]
    else:
        server = await manager.server_add(config=auth_config)
    cql, _ = await manager.get_ready_cql([server])
    role = "r" + unique_name()
    service_level = "sl" + unique_name()

    try:
        await cql.run_async(f"CREATE ROLE {role} WITH PASSWORD = '{role}' AND LOGIN = true")
        await cql.run_async(f"GRANT SELECT ON ALL KEYSPACES TO {role}")
        await cql.run_async(f"CREATE SERVICE LEVEL {service_level} WITH TIMEOUT = 10s AND WORKLOAD_TYPE = 'batch' AND SHARES = 100")
        await cql.run_async(f"ATTACH SERVICE LEVEL {service_level} TO {role}")

        for query_template, expected_metadata_id in _LIST_METADATA_CASES:
            query = query_template.format(role=role, service_level=service_level)

            # _prepare_and_execute() uses the synchronous Python driver, so run it in
            # a worker thread instead of blocking the asyncio-based test harness.
            prepared_metadata_id, metadata_changed, row_count = await asyncio.to_thread(_prepare_and_execute, server.ip_addr, query)
            assert row_count > 0, query
            assert prepared_metadata_id == expected_metadata_id, query
            assert not metadata_changed, query
    finally:
        await cql.run_async(f"DETACH SERVICE LEVEL FROM {role}")
        await cql.run_async(f"DROP SERVICE LEVEL IF EXISTS {service_level}")
        await cql.run_async(f"DROP ROLE IF EXISTS {role}")
