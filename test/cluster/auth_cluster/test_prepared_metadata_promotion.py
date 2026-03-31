#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import dataclasses
from typing import Optional
from unittest import mock

import pytest
from cassandra import ProtocolVersion
from cassandra.application_info import ApplicationInfoBase
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra.protocol import ResultMessage

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error
from test.pylib.util import unique_name
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config


# ---------------------------------------------------------------------------
# Driver helpers for SCYLLA_USE_METADATA_ID / result_metadata_id exchange.
#
# The standard Python driver gates result_metadata_id exchange on protocol v5+
# (ProtocolVersion.uses_prepared_metadata).  ScyllaDB does not implement v5
# but exposes the same semantics on v4 via the SCYLLA_USE_METADATA_ID startup
# extension.  Two lightweight patches make the driver exercise this path:
#
#   1. _UseMetadataId — ApplicationInfoBase subclass that injects
#      SCYLLA_USE_METADATA_ID into the STARTUP options dict.  Passed to
#      Cluster(application_info=...).  The driver merges these options into
#      the STARTUP frame without any filtering.
#
#   2. mock.patch.object(ProtocolVersion, "uses_prepared_metadata", ...) —
#      makes the driver write result_metadata_id in EXECUTE frames and read
#      it back from PREPARE/ROWS responses, which is exactly the v4 extension
#      wire format.
#
# Note: the driver does not send the SKIP_METADATA flag in EXECUTE even with
# these patches (it never ORs it into the options flags byte for prepared
# statements).  The server does not require SKIP_METADATA to trigger
# promotion; without it, it returns full column metadata alongside
# METADATA_CHANGED.
# ---------------------------------------------------------------------------


class _UseMetadataId(ApplicationInfoBase):
    """Inject SCYLLA_USE_METADATA_ID into the CQL STARTUP options."""

    def add_startup_options(self, options: dict) -> None:
        options["SCYLLA_USE_METADATA_ID"] = ""


@dataclasses.dataclass
class _ExecuteResult:
    """Parsed outcome of interest from a prepared-statement EXECUTE."""

    initial_metadata_id: Optional[bytes]
    """result_metadata_id returned by the PREPARE response."""

    result_metadata_id: Optional[bytes]
    """result_metadata_id embedded in the ROWS EXECUTE response, if any."""

    metadata_changed: bool
    """True when the ROWS response carried the METADATA_CHANGED result-metadata flag (i.e., promotion occurred)."""

    row_count: int
    """Number of rows returned by the EXECUTE response."""


def _prepare_and_execute(host: str, query: str) -> _ExecuteResult:
    """
    Connect via the Scylla Python driver with SCYLLA_USE_METADATA_ID negotiated,
    prepare *query*, execute it once, and return relevant metadata_id fields.

    Intended to be called via ``asyncio.to_thread`` to avoid blocking the event loop.

    The function uses two patches scoped to the connection lifetime:

    * ``ProtocolVersion.uses_prepared_metadata`` is forced to return ``True``
      for all protocol versions so that the driver reads/writes result_metadata_id
      in PREPARE and EXECUTE frames on protocol v4.

    * ``ResultMessage.recv_results_metadata`` is wrapped to capture
      result_metadata_id from the ROWS response (the driver parses it there but
      does not propagate it back to the PreparedStatement in the normal rows path).
    """
    captured: dict = {"metadata_id": None, "metadata_changed": False}
    original_recv = ResultMessage.recv_results_metadata

    def _capturing_recv(self: ResultMessage, f, user_type_map) -> None:
        original_recv(self, f, user_type_map)
        rmi = getattr(self, "result_metadata_id", None)
        if rmi is not None:
            captured["metadata_id"] = rmi
            captured["metadata_changed"] = True

    with mock.patch.object(
        ProtocolVersion,
        "uses_prepared_metadata",
        staticmethod(lambda v: True),
    ):
        cluster = Cluster(
            contact_points=[host],
            port=9042,
            protocol_version=4,
            auth_provider=PlainTextAuthProvider("cassandra", "cassandra"),
            application_info=_UseMetadataId(),
            load_balancing_policy=WhiteListRoundRobinPolicy([host]),
        )
        session = cluster.connect()
        try:
            ps = session.prepare(query)
            initial_metadata_id = ps.result_metadata_id
            with mock.patch.object(
                ResultMessage, "recv_results_metadata", _capturing_recv
            ):
                rows = list(session.execute(ps))
            return _ExecuteResult(
                initial_metadata_id=initial_metadata_id,
                result_metadata_id=captured["metadata_id"],
                metadata_changed=captured["metadata_changed"],
                row_count=len(rows),
            )
        finally:
            session.shutdown()
            cluster.shutdown()


@pytest.mark.asyncio
async def test_list_roles_of_prepared_metadata_promotion(
    manager: ManagerClient,
    build_mode: str,
) -> None:
    """Verify that EXECUTE promotes a stale prepared metadata_id, and that
    disabling that promotion suppresses the resulting ``METADATA_CHANGED``.

    ``LIST ROLES OF <role>`` is such a statement: at PREPARE time the server
    does not know the result set schema because the statement implementation
    builds the metadata dynamically at execute time.  The server therefore
    returns the metadata_id of empty metadata in the PREPARE response.

    When the client later sends EXECUTE with the stale empty metadata_id, the
    server should detect the mismatch (the actual rows have real metadata) and
    respond with a ``METADATA_CHANGED`` result that carries the real
    metadata_id so the client can update its cache.  This is the behaviour
    mandated by CQL v5; on CQL v4 it is exercised via the
    SCYLLA_USE_METADATA_ID Scylla protocol extension which enables the same
    wire-level exchange.  The test repeats PREPARE/EXECUTE on the same query
    to show that the promoted metadata_id is cached, and in non-release modes
    it contrasts that with an injected execution where the cache update is
    suppressed.
    """
    server = await manager.server_add(config=auth_config)
    cql, _ = await manager.get_ready_cql([server])

    role = "r" + unique_name()
    await cql.run_async(f"CREATE ROLE {role}")

    promoted = await asyncio.to_thread(
        _prepare_and_execute, server.ip_addr, f"LIST ROLES OF {role}"
    )

    assert promoted.row_count > 0, (
        f"expected EXECUTE for 'LIST ROLES OF {role}' to return at least one row"
    )
    assert promoted.initial_metadata_id is not None, (
        f"expected PREPARE for 'LIST ROLES OF {role}' to return a result_metadata_id"
    )
    assert promoted.metadata_changed, (
        f"expected EXECUTE for 'LIST ROLES OF {role}' to return METADATA_CHANGED "
        f"after PREPARE returned an empty result_metadata_id"
    )
    assert promoted.result_metadata_id is not None, (
        f"expected EXECUTE for 'LIST ROLES OF {role}' to return a result_metadata_id "
        f"alongside METADATA_CHANGED"
    )
    assert promoted.initial_metadata_id != promoted.result_metadata_id, (
        f"expected promoted result_metadata_id to differ from the stale empty one "
        f"returned by PREPARE"
    )

    cached = await asyncio.to_thread(
        _prepare_and_execute, server.ip_addr, f"LIST ROLES OF {role}"
    )

    assert cached.row_count > 0, (
        f"expected second EXECUTE for 'LIST ROLES OF {role}' to return at least one row"
    )
    assert cached.initial_metadata_id == promoted.result_metadata_id, (
        f"expected second PREPARE for 'LIST ROLES OF {role}' to reuse the promoted "
        f"result_metadata_id from the first EXECUTE"
    )
    assert not cached.metadata_changed, (
        f"expected second EXECUTE for 'LIST ROLES OF {role}' not to return "
        f"METADATA_CHANGED after the cache had been promoted"
    )
    assert cached.result_metadata_id is None, (
        f"expected second EXECUTE for 'LIST ROLES OF {role}' not to return a new "
        f"result_metadata_id once the cache had been promoted"
    )

    if build_mode == "release":
        return

    # Use a fresh prepared statement key so the promotion above does not seed
    # the cache for the injected contrast case.
    injected_role = "r" + unique_name()
    await cql.run_async(f"CREATE ROLE {injected_role}")

    async with inject_error(
        manager.api, server.ip_addr, "skip_prepared_result_metadata_promotion"
    ):
        suppressed = await asyncio.to_thread(
            _prepare_and_execute,
            server.ip_addr,
            f"LIST ROLES OF {injected_role}",
        )

    assert suppressed.row_count > 0, (
        f"expected injected EXECUTE for 'LIST ROLES OF {injected_role}' to return at least one row"
    )
    assert suppressed.initial_metadata_id is not None, (
        f"expected injected PREPARE for 'LIST ROLES OF {injected_role}' to return a result_metadata_id"
    )
    assert not suppressed.metadata_changed, (
        f"expected injected EXECUTE for 'LIST ROLES OF {injected_role}' to suppress "
        f"METADATA_CHANGED, but the flag was set"
    )
    assert suppressed.result_metadata_id is None, (
        f"expected injected EXECUTE for 'LIST ROLES OF {injected_role}' to omit "
        f"result_metadata_id when promotion is suppressed"
    )

    promoted_after_suppression = await asyncio.to_thread(
        _prepare_and_execute, server.ip_addr, f"LIST ROLES OF {injected_role}"
    )

    assert promoted_after_suppression.row_count > 0, (
        f"expected post-injection EXECUTE for 'LIST ROLES OF {injected_role}' to return at least one row"
    )
    assert (
        promoted_after_suppression.initial_metadata_id == suppressed.initial_metadata_id
    ), (
        f"expected injected EXECUTE for 'LIST ROLES OF {injected_role}' not to update the cached "
        f"result_metadata_id"
    )
    assert promoted_after_suppression.metadata_changed, (
        f"expected first non-injected EXECUTE for 'LIST ROLES OF {injected_role}' to "
        f"return METADATA_CHANGED because the injected run left the cache stale"
    )
    assert promoted_after_suppression.result_metadata_id is not None, (
        f"expected first non-injected EXECUTE for 'LIST ROLES OF {injected_role}' to "
        f"return a promoted result_metadata_id"
    )
    assert (
        promoted_after_suppression.initial_metadata_id
        != promoted_after_suppression.result_metadata_id
    ), (
        f"expected first non-injected EXECUTE for 'LIST ROLES OF {injected_role}' to "
        f"promote the stale result_metadata_id left by the injected run"
    )
