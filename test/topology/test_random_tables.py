#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import pytest
from cassandra.protocol import InvalidRequest                            # type: ignore


# Simple test of schema helper
@pytest.mark.asyncio
async def test_new_table(cql, random_tables):
    table = await random_tables.add_table(ncolumns=5)
    await cql.run_async(f"INSERT INTO {table} ({','.join(c.name for c in table.columns)})" \
                        f"VALUES ({', '.join(['%s'] * len(table.columns))})",
                        parameters=[c.val(1) for c in table.columns])
    pk_col = table.columns[0]
    ck_col = table.columns[1]
    vals = [pk_col.val(1), ck_col.val(1)]
    res = await cql.run_async(f"SELECT * FROM {table} WHERE {pk_col}=%s AND {ck_col}=%s",
                              parameters=vals)
    assert len(res) == 1
    assert list(res[0])[:2] == vals
    await random_tables.drop_table(table)
    with pytest.raises(InvalidRequest, match='unconfigured table'):
        await cql.run_async(f"SELECT * FROM {table}")
    await random_tables.verify_schema()


# Simple test of schema helper with alter
@pytest.mark.asyncio
async def test_alter_verify_schema(cql, random_tables):
    """Verify table schema"""
    await random_tables.add_tables(ntables=4, ncolumns=5)
    await random_tables.verify_schema()
    # Manually remove a column
    table = random_tables[0]
    await cql.run_async(f"ALTER TABLE {table} DROP {table.columns[-1].name}")
    with pytest.raises(AssertionError, match='Column'):
        await random_tables.verify_schema()
