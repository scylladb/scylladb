# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Async nodetool-compatible helpers for Scylla cluster tests.

Unlike ``test.cqlpy.nodetool``, this module is intentionally Scylla-only and
also uses the async REST API.
"""

from __future__ import annotations

import re

from cassandra.cluster import Session as CassandraSession  # type: ignore # pylint: disable=no-name-in-module

from test.pylib.rest_client import ScyllaRESTAPIClient

_rest_client = ScyllaRESTAPIClient().client


def _rest_host(cql: CassandraSession) -> str:
    # If given an exclusive connection to a single node (get_cql_exclusive()),
    # the request is sent to that specific node. Otherwise, if given a
    # connection to the whole cluster (get_cql()), the request is sent to one
    # of the nodes in the cluster, it is unspecified which one.
    return str(cql.cluster.contact_points[0])


def _parse_keyspace_table(name: str, separator_chars: str = ".") -> tuple[str, str | None]:
    pattern = rf"(?P<keyspace>\w+)(?:[{separator_chars}](?P<table>\w+))?"
    match = re.match(pattern, name)
    if match is None:
        raise ValueError(f"Invalid keyspace/table name: {name}")
    return match.group("keyspace"), match.group("table")


async def flush(cql: CassandraSession, table: str) -> None:
    """Flush the memtables for table of the form ``ks.table``, like ``nodetool flush <ks> <table>``."""
    ks, cf = table.split(".")
    await _rest_client.post(f"/storage_service/keyspace_flush/{ks}", host=_rest_host(cql), params={"cf": cf})


async def flush_keyspace(cql: CassandraSession, ks: str) -> None:
    """Flush all tables in a keyspace, like ``nodetool flush <ks>``."""
    await _rest_client.post(f"/storage_service/keyspace_flush/{ks}", host=_rest_host(cql))


async def excludenode(cql: CassandraSession, excluded_host_id: str) -> None:
    """Exclude a node from token ownership by host ID, like ``nodetool excludenode <host_id>``."""
    await _rest_client.post("/storage_service/exclude_node/", host=_rest_host(cql), params={"hosts": excluded_host_id})


class no_autocompaction_context:
    """Disable autocompaction for keyspace(s) or keyspace.table(s)."""

    def __init__(self, cql: CassandraSession, *names: str):
        self._cql = cql
        self._names = list(names)

    async def __aenter__(self) -> "no_autocompaction_context":
        for name in self._names:
            ks, tbl = _parse_keyspace_table(name, ".:")
            api_path = f"/storage_service/auto_compaction/{ks}" if not tbl else f"/column_family/autocompaction/{ks}:{tbl}"
            await _rest_client.delete(api_path, host=_rest_host(self._cql))
        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback) -> None:
        for name in self._names:
            ks, tbl = _parse_keyspace_table(name, ".:")
            api_path = f"/storage_service/auto_compaction/{ks}" if not tbl else f"/column_family/autocompaction/{ks}:{tbl}"
            await _rest_client.post(api_path, host=_rest_host(self._cql))
