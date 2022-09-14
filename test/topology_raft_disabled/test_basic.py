#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest


@pytest.mark.asyncio
async def test_basic(manager, random_tables):
    table = await random_tables.add_table(ncolumns=3)
    await table.add_column()
    await random_tables.verify_schema()
