#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# The pinned plan reaches a later page only through the paging state, so these
# tests page alternately through two coordinators. Refs #18992

import logging
import time

import pytest
from cassandra.protocol import InvalidRequest  # type: ignore
from cassandra.query import SimpleStatement  # type: ignore

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.scylla_server import ScyllaVersionDescription
from test.pylib.util import wait_for, wait_for_cql_and_get_hosts, wait_for_view

logger = logging.getLogger(__name__)

RF2 = "with replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 2}"


async def wait_until_index_visible(cql, hosts, keyspace, index_name, present):
    """Waits until every node agrees on whether the index exists, so that a node
    resolving the pinned plan against a stale index set cannot be raced with.
    Asks system."IndexInfo", which each node filters through its live schema and
    index manager - the state the pinned plan is resolved against - rather than
    system_schema.indexes, whose row is written before that state is updated."""
    async def agreed():
        for host in hosts:
            rows = await cql.run_async(
                    f"SELECT index_name FROM system.\"IndexInfo\" WHERE table_name = '{keyspace}'"
                    f" AND index_name = '{index_name}'", host=host)
            if bool(rows) != present:
                return None
        return True
    await wait_for(agreed, time.time() + 60, label=f"index {index_name} present={present} everywhere")


async def fill(cql, table, rows):
    insert = cql.prepare(f"INSERT INTO {table} (p, v) VALUES (?, ?)")
    for p, v in rows:
        await cql.run_async(insert, [p, v])


async def test_resumed_page_keeps_its_plan_on_another_coordinator(manager: ScyllaClusterManager) -> None:
    servers = await manager.servers_add(2, auto_rack_dc="dc1")
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    async with new_test_keyspace(manager, RF2) as ks:
        table = f"{ks}.tbl"
        await cql.run_async(f"CREATE TABLE {table} (p int PRIMARY KEY, v int)")
        # A sparse match, so a page served by the wrong plan shows up as a wrong
        # row set rather than as the right one by accident.
        await fill(cql, table, [(i, 17 if i % 4 == 0 else 23) for i in range(40)])

        stmt = SimpleStatement(f"SELECT p FROM {table} WHERE v = 17 ALLOW FILTERING", fetch_size=3)
        expected = sorted(row.p for row in cql.execute(stmt))
        assert len(expected) > 3

        page = cql.execute(stmt, host=hosts[0])
        assert page.has_more_pages
        got = list(page.current_rows)
        logger.info("first page: %d rows, base table scanned, coordinator %s", len(got), hosts[0])

        await cql.run_async(f"CREATE INDEX idx ON {table}(v)")
        await wait_for_view(cql, "idx_index", len(servers))
        await wait_until_index_visible(cql, hosts, ks, "idx", present=True)
        logger.info("an index that could serve this query now exists on every node")

        node = 1
        while page.has_more_pages:
            host = hosts[node % len(hosts)]
            page = cql.execute(stmt, host=host, paging_state=page.paging_state)
            logger.info("resumed page: %d rows, coordinator %s", len(page.current_rows), host)
            got.extend(page.current_rows)
            node += 1

        assert expected == sorted(row.p for row in got)


async def test_resuming_a_dropped_index_is_refused_on_another_coordinator(manager: ScyllaClusterManager) -> None:
    servers = await manager.servers_add(2, auto_rack_dc="dc1")
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    async with new_test_keyspace(manager, RF2) as ks:
        table = f"{ks}.tbl"
        await cql.run_async(f"CREATE TABLE {table} (p int PRIMARY KEY, v int)")
        await fill(cql, table, [(i, 17) for i in range(40)])
        await cql.run_async(f"CREATE INDEX idx ON {table}(v)")
        await wait_for_view(cql, "idx_index", len(servers))
        await wait_until_index_visible(cql, hosts, ks, "idx", present=True)

        # ALLOW FILTERING, so a node that re-planned rather than keeping the
        # pinned plan would carry on off a base-table scan instead of failing.
        stmt = SimpleStatement(f"SELECT p FROM {table} WHERE v = 17 ALLOW FILTERING", fetch_size=7)
        page = cql.execute(stmt, host=hosts[0])
        assert page.has_more_pages
        logger.info("first page: %d rows, index scanned, coordinator %s", len(page.current_rows), hosts[0])

        await cql.run_async(f"DROP INDEX {ks}.idx")
        await wait_until_index_visible(cql, hosts, ks, "idx", present=False)
        logger.info("index dropped everywhere; the saved position belongs to a view that is gone")

        with pytest.raises(InvalidRequest, match="no longer available"):
            cql.execute(stmt, host=hosts[1], paging_state=page.paging_state)


async def mixed_version_cluster(manager: ScyllaClusterManager, old_version: ScyllaVersionDescription,
        new_binary: str):
    """Brings up two nodes on a version that records no plan in the paging state
    and upgrades one of them, so that a page can cross from a coordinator which
    records a plan to one that does not know the field, and back."""
    servers = await manager.servers_add(2, config={'tablets_mode_for_new_keyspaces': 'disabled'},
                                       auto_rack_dc="dc1", version=old_version)
    logger.info("cluster up on %s", old_version.path)
    await manager.server_change_version(servers[1].server_id, new_binary)
    await manager.server_sees_others(servers[1].server_id, 1, interval=60.0)
    cql, hosts = await manager.get_ready_cql(servers)
    old_host = next(h for h in hosts if h.address == servers[0].ip_addr)
    new_host = next(h for h in hosts if h.address == servers[1].ip_addr)
    logger.info("upgraded %s: old coordinator %s, new coordinator %s",
                servers[1].server_id, old_host, new_host)
    return cql, old_host, new_host


async def indexed_table(cql, hosts, ks):
    table = f"{ks}.tbl"
    await cql.run_async(f"CREATE TABLE {table} (p int PRIMARY KEY, v int)")
    # A sparse match, so a page served by the wrong plan shows up as a wrong row
    # set rather than as the right one by accident.
    await fill(cql, table, [(i, 17 if i % 4 == 0 else 23) for i in range(40)])
    await cql.run_async(f"CREATE INDEX idx ON {table}(v)")
    await wait_for_view(cql, "idx_index", len(hosts))
    await wait_until_index_visible(cql, hosts, ks, "idx", present=True)
    return table


@pytest.mark.skip_mode(mode='release', reason='dev mode is enough for this test')
@pytest.mark.skip_mode(mode='debug', reason='dev mode is enough for this test')
async def test_paged_read_crosses_versions(manager: ScyllaClusterManager,
        scylla_2025_1: ScyllaVersionDescription, scylla_binary: str) -> None:
    """A paged read has to survive a rolling upgrade in both directions: a state
    naming no plan cannot be refused, and a state naming one has to be read
    without complaint by a version that does not know the field. Refs #18992"""
    cql, old_host, new_host = await mixed_version_cluster(
            manager, scylla_2025_1, str(scylla_binary))

    async with new_test_keyspace(manager, RF2) as ks:
        table = await indexed_table(cql, [old_host, new_host], ks)

        stmt = SimpleStatement(f"SELECT p FROM {table} WHERE v = 17", fetch_size=3)
        expected = sorted(row.p for row in cql.execute(stmt))
        assert len(expected) > 3

        for first, second in [(old_host, new_host), (new_host, old_host)]:
            logger.info("indexed read started on %s, resumed on %s", first, second)
            page = cql.execute(stmt, host=first)
            assert page.has_more_pages
            got = list(page.current_rows)
            while page.has_more_pages:
                page = cql.execute(stmt, host=second, paging_state=page.paging_state)
                got.extend(page.current_rows)
            assert expected == sorted(row.p for row in got)


@pytest.mark.skip_mode(mode='release', reason='dev mode is enough for this test')
@pytest.mark.skip_mode(mode='debug', reason='dev mode is enough for this test')
async def test_resuming_a_pre_plan_paging_state_is_not_refused(manager: ScyllaClusterManager,
        scylla_2025_1: ScyllaVersionDescription, scylla_binary: str) -> None:
    """A state that records no plan has nothing to pin, so dropping the index it
    was reading must not make the new coordinator refuse it - what such a resume
    returns is #18992, which a rolling upgrade cannot fix."""
    cql, old_host, new_host = await mixed_version_cluster(
            manager, scylla_2025_1, str(scylla_binary))

    async with new_test_keyspace(manager, RF2) as ks:
        table = await indexed_table(cql, [old_host, new_host], ks)

        # ALLOW FILTERING, so a base-table scan is still a legal fallback.
        stmt = SimpleStatement(f"SELECT p FROM {table} WHERE v = 17 ALLOW FILTERING", fetch_size=3)
        page = cql.execute(stmt, host=old_host)
        assert page.has_more_pages
        logger.info("first page from %s, which records no plan", old_host)

        await cql.run_async(f"DROP INDEX {ks}.idx")
        await wait_until_index_visible(cql, [old_host, new_host], ks, "idx", present=False)

        try:
            cql.execute(stmt, host=new_host, paging_state=page.paging_state)
            logger.info("the new coordinator served the page")
        except InvalidRequest as e:
            logger.info("the new coordinator refused the page: %s", e)
            assert "no longer available" not in str(e)
