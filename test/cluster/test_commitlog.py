#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
import pytest
import logging
import re
import time
from test.pylib.manager_client import ManagerClient
from test.pylib.random_tables import RandomTables
from test.pylib.util import wait_for_cql_and_get_hosts
from test.cluster.util import reconnect_driver, new_test_keyspace
from test.pylib.random_tables import Column, TextType

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_reboot(request, manager: ManagerClient):
    # Check that commitlog provides durability in case of a node reboot.
    #
    # 1. start a node with commitlog_sync: batch
    # 2. create a test table t
    # 3. write some data to the table and truncate it, now a truncation record should be created
    # 4. cleanly restart a node with enabled injection decrease_schema_commitlog_base_segment_id,
    #    this allows to set new segment id to zero, which imitates machine reboot
    # 5. write some data into t
    # 6. save its current content in local variable
    # 7. kill -9 the node and restart it
    # 8. check the table content is preserved

    server_info = await manager.server_add(config={
        'commitlog_sync': 'batch'
    })
    cql = manager.cql
    await wait_for_cql_and_get_hosts(cql, [server_info], time.time() + 60)
    logger.info("Node started")

    random_tables = RandomTables(request.node.name, manager, "ks", 1)
    await random_tables.add_table(name='t', pks=1, columns=[
        Column(name="key", ctype=TextType),
        Column(name="value", ctype=TextType)
    ])
    logger.info("Test table created")

    async def load_table() -> list:
        result = await cql.run_async("select * from ks.t")
        result.sort()
        return result
    def save_table(k: str, v: str):
        return cql.run_async("insert into ks.t(key, value) values (%s, %s)",
                             parameters = [k, v])

    test_rows_count = 10
    futures = []
    for i in range(0, test_rows_count):
        futures.append(save_table(f'key_{i}', f'value_{i}'))
    await asyncio.gather(*futures)
    logger.info("Some data is written into test table")

    await cql.run_async("truncate table ks.t")
    logger.info("Test table is truncated")

    await manager.server_stop_gracefully(server_info.server_id)
    await manager.server_update_config(server_info.server_id,
                                       'error_injections_at_startup',
                                       ['decrease_commitlog_base_segment_id'])
    await manager.server_start(server_info.server_id)
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, [server_info], time.time() + 60)
    logging.info("Node is restarted with decrease_schema_commitlog_base_segment_id injection")

    futures = []
    for i in range(0, test_rows_count):
        futures.append(save_table(f'new_key_{i}', f'new_value_{i}'))
    await asyncio.gather(*futures)
    logger.info("Some new data is written into test table")

    table_content_before_crash = await load_table()
    await manager.server_stop(server_info.server_id)
    await manager.server_start(server_info.server_id)
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, [server_info], time.time() + 60)
    logging.info("Node is killed and restarted")

    table_content_after_crash = await load_table()
    logging.info(f"table content before crash [{table_content_before_crash}], "
                 f"after crash [{table_content_after_crash}]")
    assert table_content_before_crash == table_content_after_crash


@pytest.mark.asyncio
@pytest.mark.xfail(reason="Commitlog segments may be replayed out of order due to unordered_multimap")
async def test_commitlog_segment_replay_order(manager: ManagerClient):
    """Test that commitlog segments are replayed in ascending ID order within a shard.

    This verifies that the fix for the commitlog replay order bug works correctly.
    The bug was that std::unordered_multimap::equal_range() does not guarantee
    iteration order, so segments could be replayed out of order within a shard.

    The test:
    1. Starts a node with small commitlog segments and debug logging
    2. Writes enough data to create multiple commitlog segments
    3. Kills the node abruptly (no clean shutdown)
    4. Restarts the node and waits for replay
    5. Greps the log for "Replaying" messages and verifies they are in ascending
       segment ID order
    """
    # Use small segments (1MB) to ensure we get multiple segments quickly
    # Enable debug logging for commitlog_replayer to see "Replaying" messages
    server_info = await manager.server_add(
        config={
            'commitlog_sync': 'batch',
            'commitlog_segment_size_in_mb': 1,
        },
        cmdline=['--logger-log-level', 'commitlog_replayer=debug']
    )
    cql = manager.cql
    await wait_for_cql_and_get_hosts(cql, [server_info], time.time() + 60)
    logger.info("Node started with small commitlog segments and debug logging")

    async with new_test_keyspace(manager, "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f"""
            CREATE TABLE {ks}.cf (
                pk int,
                ck int,
                v text,
                PRIMARY KEY (pk, ck)
            )
        """)
        logger.info("Test table created")

        # Insert enough data to create multiple commitlog segments
        # Each segment is 1MB, so we need to write enough to fill several
        value = "x" * 10000  # 10KB per row
        rows_written = 0
        # Write enough rows to create at least 3-4 segments (3-4 MB of data)
        target_rows = 400  # ~4MB of data
        batch_size = 50

        for batch in range(target_rows // batch_size):
            futures = []
            for i in range(batch_size):
                row_id = batch * batch_size + i
                futures.append(cql.run_async(
                    f"INSERT INTO {ks}.cf (pk, ck, v) VALUES (%s, %s, %s)",
                    parameters=[row_id // 100, row_id % 100, value]
                ))
            await asyncio.gather(*futures)
            rows_written += batch_size

        logger.info(f"Written {rows_written} rows")

    # Kill the node abruptly (no clean shutdown) to ensure commitlog replay on restart
    logger.info("Stopping node abruptly")
    await manager.server_stop(server_info.server_id)

    # Mark log position AFTER stop and BEFORE restart to only grep replay messages
    log_file = await manager.server_open_log(server_info.server_id)
    mark_before_restart = await log_file.mark()

    # Restart the node - this will trigger commitlog replay
    logger.info("Starting node to trigger commitlog replay")
    await manager.server_start(server_info.server_id)
    cql = await reconnect_driver(manager)
    await wait_for_cql_and_get_hosts(cql, [server_info], time.time() + 60)

    # Wait for replay to complete
    await log_file.wait_for("Log replay complete", from_mark=mark_before_restart, timeout=120)
    logger.info("Commitlog replay complete")

    # Grep for "Replaying" messages which show segment filenames
    # Format: "DEBUG ... [shard X:main] commitlog_replayer - Replaying /path/to/CommitLog-<shard>-<id>.log"
    # We need to filter for DEBUG level (individual segment replay) not INFO (list of all)
    replaying_matches = await log_file.grep(
        r"DEBUG.*\[shard (\d+):.*commitlog_replayer - Replaying .*/CommitLog-\d+-(\d+)\.log",
        from_mark=mark_before_restart
    )

    assert len(replaying_matches) > 0, "Expected to find 'Replaying' log messages"
    logger.info(f"Found {len(replaying_matches)} 'Replaying' messages")

    # Extract processing shard and segment IDs from the log lines
    # The processing shard is [shard X:main] which determines replay order
    # The segment ID is from the filename
    segments_by_processing_shard: dict[int, list[int]] = {}

    for line, match in replaying_matches:
        processing_shard = int(match.group(1))
        segment_id = int(match.group(2))
        if processing_shard not in segments_by_processing_shard:
            segments_by_processing_shard[processing_shard] = []
        segments_by_processing_shard[processing_shard].append(segment_id)

    logger.info(f"Found segments by processing shard: {segments_by_processing_shard}")

    # Verify that segments within each processing shard are in ascending order
    for shard_id, segment_ids in segments_by_processing_shard.items():
        for i in range(1, len(segment_ids)):
            assert segment_ids[i] > segment_ids[i - 1], (
                f"Segments for processing shard {shard_id} are not in ascending order: "
                f"segment {segment_ids[i]} came after {segment_ids[i - 1]}"
            )
        logger.info(f"Processing shard {shard_id}: segments {segment_ids} are in correct ascending order")
