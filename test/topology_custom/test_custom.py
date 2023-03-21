#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables
from test.pylib.util import unique_name
import pytest


@pytest.mark.asyncio
async def test_custom(request, manager: ManagerClient):
    servers = [await manager.server_add(), await manager.server_add(), await manager.server_add()]
    tables = RandomTables(request.node.name, manager, unique_name())
    table = await tables.add_table(ncolumns=5)
    await table.insert_seq()
    await table.add_index(2)
    await tables.verify_schema(table)
