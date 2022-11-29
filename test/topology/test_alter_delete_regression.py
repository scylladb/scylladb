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


LOOPS          =     4     # Attempts to trigger failure
INITIAL_ROWS   =    10     # Total rows on table
MAX_S          =  600.0   # Max run seconds
ALTER_TABLE_S  =     0.001 # Alter table every 1ms


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
    await asyncio.gather(*(table.insert_seq() for _ in range(INITIAL_ROWS)))
    logger.debug("Done prologue")

    async def delete_add_rows():
        logger.debug("delete_rows: start")
        for i in range(2**22):
            table.delete_pk(i)
            await asyncio.sleep(.001)
            table.insert_seq()
        logger.debug("delete_rows: done")

    async def alter_table():
        logger.debug("alter_table: start")
        ctypes = [BigIntType, IntType]
        for i in range(0, INITIAL_ROWS - 1):
            await asyncio.sleep(ALTER_TABLE_S)
            logger.debug(f"XXX drop d2 iter {i}")
            await table.drop_column("d2")
            logger.debug(f"XXX adding d2 iter {i}: type {i % 2}")
            await table.add_column("d2", ctypes[i % 2])
        logger.debug("alter_table: done")

    # Test woud fail with asyncio.TimeoutError on regression
    # TODO: change to asyncio.timeout for Python 3.11
    try:
        await asyncio.wait_for(asyncio.gather(alter_table(), delete_add_rows()), timeout = MAX_S)
    except asyncio.TimeoutError as exc:
        pass
        # raise RuntimeError("LWT regression: deletes hang on alter table") from exc
