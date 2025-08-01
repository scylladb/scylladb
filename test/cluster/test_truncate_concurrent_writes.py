#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot, read_barrier
from test.cluster.conftest import skip_mode
from test.cluster.util import create_new_test_keyspace
from cassandra.query import SimpleStatement, ConsistencyLevel

import pytest
import asyncio
import logging
import time
import random
import glob
import os

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_validate_truncate_with_concurrent_writes(manager: ManagerClient):

    # This test validates that all the data before a truncate started has been deleted,
    # and that none of the data after truncate ended has been deleted

    cmdline = []
    config = {}

    servers = await manager.servers_add(3, config=config, cmdline=cmdline, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ])

    cql = manager.get_cql()
    ks = await create_new_test_keyspace(cql, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}}")
    await cql.run_async(f"CREATE TABLE {ks}.test (pk int, ck int, val int, PRIMARY KEY(pk, ck));")

    trunc_started_event = asyncio.Event()
    trunc_completed_event = asyncio.Event()
    writer_halfpoint = asyncio.Event()

    number_of_ck_to_insert = 10000
    writer_results = {}
    async def writer(pk, writer_halfpoint, trunc_started_event, trunc_completed_event):
        stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, ck, val) VALUES (?, ?, ?);")
        stmt.consistency_level = ConsistencyLevel.ONE
        last_ck_before_trunc = None
        first_ck_after_trunc = None
        logger.info(f'write of pk: {pk} started')
        for ck in range(number_of_ck_to_insert):
            trunc_completed = trunc_completed_event.is_set()
            await cql.run_async(stmt, [pk, ck, ck])
            trunc_started = trunc_started_event.is_set()

            if not trunc_started:
                last_ck_before_trunc = ck
            if first_ck_after_trunc is None and trunc_completed:
                first_ck_after_trunc = ck

            if ck % 50 == 0:
                logger.info(f'written pk: {pk} ck: {ck}')

            if ck > number_of_ck_to_insert / 2:
                writer_halfpoint.set()

        writer_results[pk] = (last_ck_before_trunc, first_ck_after_trunc)
        logger.info(f'write of pk: {pk} ended; last_ck_before_trunc: {last_ck_before_trunc} first_ck_after_trunc: {first_ck_after_trunc}')

    async def do_truncate(writer_halfpoint, trunc_started_event, trunc_completed_event):
        logger.info(f'truncate waiting')
        await writer_halfpoint.wait()
        logger.info(f'truncate starting')
        trunc_started_event.set()
        await cql.run_async(f"TRUNCATE TABLE {ks}.test;")
        trunc_completed_event.set()
        logger.info(f'truncate ended')

    # start the writers and truncate
    tasks = []
    for pk in range(50):
        tasks.append(asyncio.create_task(writer(pk, writer_halfpoint, trunc_started_event, trunc_completed_event)))

    tasks.append(asyncio.create_task(do_truncate(writer_halfpoint, trunc_started_event, trunc_completed_event)))

    await asyncio.gather(*tasks)

    # log the results
    remaining_ck_per_pk = {}
    for pk, cks in writer_results.items():
        last_ck_before_trunc = cks[0]
        first_ck_after_trunc = cks[1]

        stmt = SimpleStatement(f'SELECT ck FROM {ks}.test WHERE pk = {pk} LIMIT 1;')
        stmt.consistency_level = ConsistencyLevel.ALL
        res = await cql.run_async(stmt)
        min_ck = res[0].ck

        stmt = SimpleStatement(f'SELECT count(*) as cnt FROM {ks}.test WHERE pk = {pk} AND ck >= {first_ck_after_trunc};')
        stmt.consistency_level = ConsistencyLevel.ALL
        res = await cql.run_async(stmt)
        count_after_trunc = res[0].cnt

        remaining_ck_per_pk[pk] = (min_ck, count_after_trunc)

        logger.info(f'pk: {pk} min_ck: {min_ck} count: {count_after_trunc} last_ck_before_trunc: {last_ck_before_trunc} first_ck_after_trunc: {first_ck_after_trunc}')

    # assert truncate deleted everything before truncate started
    # and did not delete anything after truncate ended
    for pk, cks in writer_results.items():
        last_ck_before_trunc = cks[0]
        first_ck_after_trunc = cks[1]
        min_ck = remaining_ck_per_pk[pk][0]
        count_after_trunc = remaining_ck_per_pk[pk][1]
        assert min_ck > last_ck_before_trunc, f'ck: {min_ck} for pk: {pk} should have been deleted but was not; {last_ck_before_trunc=}'
        assert min_ck <= first_ck_after_trunc, f'ck: {first_ck_after_trunc} for pk: {pk} should not have been deleted but was; minimal ck remaining: {min_ck}'

        if first_ck_after_trunc + count_after_trunc < number_of_ck_to_insert:
            missing_records = number_of_ck_to_insert - first_ck_after_trunc - count_after_trunc
            assert False, f'{missing_records} records that were written after truncate ended were deleted; {pk=}'
        elif first_ck_after_trunc + count_after_trunc > number_of_ck_to_insert:
            extra_records = first_ck_after_trunc + count_after_trunc - number_of_ck_to_insert
            assert False, f'{extra_records} too many records found after {first_ck_after_trunc=}: expected only {number_of_ck_to_insert - first_ck_after_trunc}; {pk=}'
