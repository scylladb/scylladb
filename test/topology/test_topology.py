#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test consistency of schema changes with topology changes.
"""
import pytest


@pytest.mark.asyncio
async def test_add_server_add_column(manager, random_tables):
    """Add a node and then add a column to a table and verify"""
    table = await random_tables.add_table(ncolumns=5)
    await manager.server_add()
    await table.add_column()
    await random_tables.verify_schema()


@pytest.mark.asyncio
async def test_stop_server_add_column(manager, random_tables):
    """Add a node, stop an original node, add a column"""
    servers = await manager.running_servers()
    table = await random_tables.add_table(ncolumns=5)
    await manager.server_add()
    await manager.server_stop(servers[1])
    await table.add_column()
    await random_tables.verify_schema()


@pytest.mark.asyncio
async def test_restart_server_add_column(manager, random_tables):
    """Add a node, stop an original node, add a column"""
    servers = await manager.running_servers()
    table = await random_tables.add_table(ncolumns=5)
    ret = await manager.server_restart(servers[1])
    await table.add_column()
    await random_tables.verify_schema()


@pytest.mark.asyncio
async def test_remove_node_add_column(manager, random_tables):
    """Add a node, remove an original node, add a column"""
    servers = await manager.running_servers()
    table = await random_tables.add_table(ncolumns=5)
    await manager.server_add()
    server_uuid = await manager.get_host_id(servers[1])              # get uuid [1]
    await manager.server_stop_gracefully(servers[1])                 # stop     [1]
    await manager.remove_node(servers[0], servers[1], server_uuid)   # Remove   [1]
    await table.add_column()
    await random_tables.verify_schema()


@pytest.mark.asyncio
async def test_decommission_node_add_column(manager, random_tables):
    """Add a node, remove an original node, add a column"""
    servers = await manager.running_servers()
    table = await random_tables.add_table(ncolumns=5)
    await manager.server_add()
    await manager.decommission_node(servers[1])             # Decommission [1]
    await table.add_column()
    await random_tables.verify_schema()
