#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""This is a regression test for #6174"""


import asyncio
import logging
from test.pylib.random_tables import Column, IntType, BigIntType
import pytest


logger = logging.getLogger("test_alter_delete")


LOOPS          =     4    # Attempts to trigger failure
NROWS          =  1000    # Total rows on table
MAX_S          =    10.0  # Max run seconds
ALTER_TABLE_S  =     0.1  # Alter table at 100ms


@pytest.mark.asyncio
async def test_new_table(manager, random_tables):
    """Test LWT regression causing hangs when doing DELETEs while there's an ALTER TABLE"""
    cql = manager.cql
    assert cql is not None

    # Prologue
    table = await random_tables.add_table(columns=[Column('pk', IntType),
                                                   Column('v', IntType),
                                                   Column('d1', IntType),
                                                   Column('d2', IntType)])
    await asyncio.gather(*(table.insert_seq() for _ in range(NROWS)))
    logger.debug("Done prologue")

    async def delete_rows():
        logger.debug("delete_rows: start")
        for i in range(0, NROWS - 1):
            table.delete_pk(i)
            await asyncio.sleep(.001)
        logger.debug("delete_rows: done")

    async def alter_table():
        await asyncio.sleep(ALTER_TABLE_S)
        logger.debug("alter_table: start")
        await table.drop_column("d2")
        await table.add_column(Column("d2", BigIntType))
        logger.debug("alter_table: done")

    # Test woud fail with asyncio.TimeoutError on regression
    # TODO: change to asyncio.timeout for Python 3.11
    try:
        await asyncio.wait_for(asyncio.gather(alter_table(), delete_rows()), timeout = MAX_S)
    except asyncio.TimeoutError as exc:
        raise RuntimeError("LWT regression: deletes hang on alter table") from exc
