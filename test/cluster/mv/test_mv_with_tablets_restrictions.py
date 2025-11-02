#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import logging
import pytest

from cassandra.cluster import Session as CassandraSession
from cassandra.protocol import InvalidRequest

from test.cluster.conftest import skip_mode
from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_create_mv_and_index_restrictions_in_tablet_keyspaces(manager: ManagerClient):
    """
    Verify that creating a materialized view or a secondary index in a tablet-based keyspace
    is allowed only when RF equals the number of racks, even if `rf_rack_valid_keyspaces` is false.

    The constraint is relevant only for numeric-RF keyspaces. For rack-list keyspaces, it should
    be always allowed.
    """

    async def create_mv_or_index(cql: CassandraSession, schema_kind: str):
        if schema_kind == "view":
            await cql.run_async("CREATE MATERIALIZED VIEW ks.mv "
                                "AS SELECT * FROM ks.t "
                                "WHERE p IS NOT NULL AND v IS NOT NULL "
                                "PRIMARY KEY (v, p)")
        elif schema_kind == "index":
            await cql.run_async("CREATE INDEX myindex ON ks.t(v)")
        else:
            assert False, "Unknown schema kind"

    async def test_create_mv_or_index_with_rf(cql: CassandraSession, schema_kind: str, rf: int, expected_error: str = None):
        try:
            await cql.run_async(f"CREATE KEYSPACE ks WITH replication = "
                                 f"{{'class': 'NetworkTopologyStrategy', 'dc1': {rf}}} "
                                 "AND tablets = {'enabled': true}")
            await cql.run_async("CREATE TABLE ks.t (p int PRIMARY KEY, v int)")
            if expected_error:
                with pytest.raises(InvalidRequest, match=expected_error):
                    await create_mv_or_index(cql, schema_kind)
            else:
                await create_mv_or_index(cql, schema_kind)
        finally:
            await cql.run_async("DROP KEYSPACE IF EXISTS ks")

    config = {'rf_rack_valid_keyspaces': False}

    servers = await manager.servers_add(3, config=config, cmdline=['--logger-log-level', 'tablets=debug'], property_file=[
        {'dc': 'dc1', 'rack': 'rack1'},
        {'dc': 'dc1', 'rack': 'rack2'},
        {'dc': 'dc1', 'rack': 'rack3'},
    ])
    cql, _ = await manager.get_ready_cql(servers)

    for schema_kind in ["view", "index"]:
        # Create MV/index with RF=Racks - should always succeed
        await test_create_mv_or_index_with_rf(cql, schema_kind, 3)

        # Create MV/index with RF!=Racks - should fail for numeric RF
        expected_error = "required to be RF-rack-valid"
        await test_create_mv_or_index_with_rf(cql, schema_kind, 2, expected_error=expected_error)


@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_alter_keyspace_rf_rack_restriction_with_mv_and_index(manager: ManagerClient):
    """
    Verify that ALTER KEYSPACE fails if it changes RF so that RF != number of racks
    for a tablets-based keyspace while it has a materialized view or a secondary index, even if
    `rf_rack_valid_keyspaces` is false.
    It should fail when it has MV/index and succeed if it doesn't.

    The constraint is relevant only for numeric-RF keyspaces. For rack-list keyspaces, it should
    be always allowed.
    """
    config = {'rf_rack_valid_keyspaces': False}

    servers = await manager.servers_add(3, config=config, cmdline=['--logger-log-level', 'tablets=debug'], property_file=[
        {'dc': 'dc1', 'rack': 'rack1'},
        {'dc': 'dc1', 'rack': 'rack2'},
        {'dc': 'dc1', 'rack': 'rack3'},
    ])
    cql, _ = await manager.get_ready_cql(servers)

    for schema_kind in ["view", "index"]:
        # Create a keyspace and MV/index with RF=Racks
        ks = f"ks_{schema_kind}"

        await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication = "
                            f"{{'class': 'NetworkTopologyStrategy', 'dc1': 3}} "
                            "AND tablets = {'enabled': true}")
        await cql.run_async(f"CREATE TABLE {ks}.t (p int PRIMARY KEY, v int)")
        if schema_kind == "view":
            await cql.run_async(f"CREATE MATERIALIZED VIEW {ks}.mv "
                                f"AS SELECT * FROM {ks}.t "
                                "WHERE p IS NOT NULL AND v IS NOT NULL "
                                "PRIMARY KEY (v, p)")
        elif schema_kind == "index":
            await cql.run_async(f"CREATE INDEX myindex ON {ks}.t(v)")
        else:
            assert False, "Unknown schema kind"

        # Try to ALTER KEYSPACE to RF!=Racks - should fail because it has MV/index
        with pytest.raises(InvalidRequest, match="required to be RF-rack-valid"):
            await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = "
                                f"{{'class': 'NetworkTopologyStrategy', 'dc1': 2}}")

        # drop the view/index and verify that ALTER KEYSPACE is now allowed
        if schema_kind == "view":
            await cql.run_async(f"DROP MATERIALIZED VIEW {ks}.mv")
        elif schema_kind == "index":
            await cql.run_async(f"DROP INDEX {ks}.myindex")

        await cql.run_async(f"ALTER KEYSPACE {ks} WITH replication = "
                            f"{{'class': 'NetworkTopologyStrategy', 'dc1': 2}}")

        await cql.run_async(f"DROP KEYSPACE {ks}")
