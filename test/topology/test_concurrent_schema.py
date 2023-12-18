#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import logging
import pytest
from test.pylib.random_tables import Column, UUIDType, IntType


logger = logging.getLogger('schema-test')


# #1207 Schema changes are not linearizable, so concurrent updates may lead to
# inconsistent schemas
# This is the same issue exposed in https://issues.apache.org/jira/browse/CASSANDRA-10250
# and this is a port of that repro.
# How this repro works:
# - Creates 20+20 tables to alter and 20 tables to index
# - In parallel run 20 * create table, and drop/add column and index of previous 2 tables
@pytest.mark.asyncio
async def test_cassandra_issue_10250(random_tables):
    tables = random_tables
    # How many combinations of tables; original Cassandra issue repro had 20
    RANGE = 1
    tables_a = []
    tables_i = []
    # Create sequentially 20+20 tables, 20 to alter later, 20 to index later.
    for n in range(RANGE):
        # alter_me: id uuid, s1 int, ..., s7 int
        tables_a.append(await tables.add_table(columns=[Column(name="id", ctype=UUIDType),
                                               *[Column(name=f"s{i}", ctype=IntType) for i in range(1, 8)]],
                                               pks=1, name=f"alter_me_{n}"))
        # index_me: id uuid, c1 int, ..., c7 int
        tables_i.append(await tables.add_table(columns=[Column(name="id", ctype=UUIDType),
                                               *[Column(name=f"c{i}", ctype=IntType) for i in range(1, 8)]],
                                               pks=1, name=f"index_me_{n}"))
    logger.debug("Created initial tables")

    # Create a bunch of futures to run in parallel (in aws list)
    #   - alter a table in the _a list by adding a column
    #   - alter a table in the _a list by removing a column
    #   - index a table in the _i list
    aws = []
    for n in range(RANGE):
        table = tables.add_table(columns=[Column(name="id", ctype=UUIDType),
                                 *[Column(name=f"c{i}", ctype=IntType) for i in range(1, 4)]], pks=1)
        aws.append(table)
        for a in range(1, 8):
            # Note: removing column after adding to keep at least one non-PK column in the table
            # alter table alter_me_{n} add c{a} int
            aws.append(tables_a[n].add_column(name=f"c{a}", ctype=IntType))
            # alter table alter_me_{n} drop s{a}  value columns
            aws.append(tables_a[n].drop_column(1))
            # create index ix_index_me_{0}_c{1} on index_me_{0} (c{1})
            aws.append(tables_i[n].add_index(f"c{a}", f"ix_index_me_{n}_c{a}"))

    # Run everything in parallel
    await asyncio.gather(*aws)
    logger.debug("Done running concurrent schema changes")
    # Sleep to settle; original Cassandra issue repro sleeps 20 seconds
    await asyncio.sleep(1)
    logger.debug("verifying schema status")
    # When bug happens, deleted columns are still there (often) and/or new columns are missing (rarely)
    await tables.verify_schema()
