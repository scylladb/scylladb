#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from cassandra.protocol import InvalidRequest   # type: ignore
from pylib.util import unique_name
import pytest


@pytest.mark.asyncio
async def test_delete_empty_string_key(cql, random_tables):
    table = await random_tables.add_table(ncolumns=5)
    s = "foobar"
    # An empty-string clustering *is* allowed:
    await cql.run_async(f"DELETE FROM {table} WHERE pk = '{s}' AND {table.columns[1].name} = ''")
    # But an empty-string partition key is *not* allowed, with a specific
    # error that a "Key may not be empty":
    with pytest.raises(InvalidRequest, match='Key may not be empty'):
        await cql.run_async(f"DELETE FROM {table} WHERE pk = '' AND {table.columns[1].name} = '{s}'")
    await random_tables.verify_schema()
