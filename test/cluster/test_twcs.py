#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import glob
import logging
import os
import random
import re
import string
import time

from cassandra import concurrent

from test import DEBUG_MODES
from test.cluster.util import cassandra_stress, run_nodetool
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
import pytest

logger = logging.getLogger(__name__)


def generate_random_text(length=10):
    return "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))


# Ported from scylla-dtest twcs_additional_test.py::TestTimeWindowCompactionStrategyAdditional::test_expired_sstables_are_compacted_separately.
# One node (--smp 1, compaction debug logging): a TWCS table (1-minute
# windows, expired sstables checked at every compaction) gets three minutes
# of simulated writes with a 30 s TTL, flushed every 30 simulated seconds,
# with autocompaction disabled; under a 4-minute cassandra-stress write
# load of 300 threads, once the data has expired (30 s more), autocompaction
# is enabled and the expired sstables are removed (with flushes every 2 s),
# and every compaction that took an expired sstable compacted it to nothing:
# expired sstables are not compacted together with live ones (#9533).
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
async def test_expired_sstables_are_compacted_separately(manager: ScyllaClusterManager):
    ttl = 30
    test_max_duration_minutes = 4
    expiry_grace_time = 30
    node = await manager.server_add(cmdline=["--smp", "1", "--logger-log-level", "compaction=debug"],
                                    config={"tablets_mode_for_new_keyspaces": "disabled", "num_tokens": 256, "endpoint_snitch": "SimpleSnitch"},
                                    property_file={"dc": "datacenter1", "rack": "rack1"})
    session = manager.get_cql()
    session.execute("CREATE KEYSPACE keyspace1 WITH replication={'class':'NetworkTopologyStrategy', 'replication_factor':1}")
    session.cluster.control_connection.wait_for_schema_agreement(wait_time=120)
    session.execute("USE keyspace1")
    session.execute("""CREATE TABLE keyspace1.standard1
            ( key blob PRIMARY KEY, "C0" blob, "C1" blob, "C2" blob, "C3" blob, "C4" blob )
            WITH bloom_filter_fp_chance = 0.01 AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
            AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1',
            'compaction_window_unit': 'MINUTES', 'expired_sstable_check_frequency_seconds': '0'}
            AND crc_check_chance = 1.0
            AND dclocal_read_repair_chance = 0.0
            AND default_time_to_live = 120
            AND tombstone_gc = {'mode': 'immediate'} AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0 AND min_index_interval = 128
            AND read_repair_chance = 0.0
            AND speculative_retry = '99.0PERCENTILE';""")
    await run_nodetool(manager, node, "disableautocompaction")

    # create_sstables_with_short_ttl(): 3 minutes of writes to 10 partitions
    # every (simulated) second, flushed every 30 s.
    insert_statement = session.prepare('INSERT INTO keyspace1.standard1 (key, "C0", "C1", "C2", "C3", "C4") VALUES (?, ?, ?, ?, ?, ?) '
                                       f"USING TIMESTAMP ? AND TTL {ttl}")
    rand_pks = set()
    while len(rand_pks) < 10:
        rand_pks.add(generate_random_text(10).encode())
    for t in range(3 * 60, 0, -1):
        timestamp = int(time.time() - t)
        concurrent.execute_concurrent_with_args(session, insert_statement,
                                                [(pk, b"test", b"test", b"test", b"test", b"tt", timestamp * 1000 * 1000) for pk in rand_pks])
        if t % 30 == 0:
            await run_nodetool(manager, node, "flush")
    await run_nodetool(manager, node, "flush")
    [cf_dir] = glob.glob(os.path.join(await manager.server_get_workdir(node.server_id), "data", "keyspace1", "standard1-*"))
    sstables = [os.path.basename(f) for f in glob.glob(f"{cf_dir}/*-Data.db")]

    # _start_high_load_on_cluster(): cassandra-stress writes in the background.
    stress = asyncio.create_task(cassandra_stress(manager, node, "write", f"duration={test_max_duration_minutes}m", "no-warmup",
                                                  "-rate", "threads=300", "-pop", "seq=1..1000000000", create_keyspace=False))
    try:
        await asyncio.sleep(ttl + expiry_grace_time)
        log = await manager.server_open_log(node.server_id)
        mark = await log.mark()
        await run_nodetool(manager, node, "enableautocompaction")
        # wait_until_sstables_are_evicted()
        deadline = time.time() + (test_max_duration_minutes + 1) * 60
        while time.time() < deadline:
            sstables = [s for s in sstables if os.path.exists(f"{cf_dir}/{s}")]
            if not sstables:
                break
            await run_nodetool(manager, node, "flush")
            await asyncio.sleep(2)
    finally:
        await stress
    assert not sstables, "Expired sstables should be removed soon after expiration time (upon compaction)"

    # expired_sstables_should_not_be_compacted_along_with_unexpired()
    lines = [line for line, _ in await log.grep(r".*", from_mark=mark)]
    ks_compaction_lines = [line for line in lines if "compaction - [Compact keyspace1.standard1" in line]
    start_compaction_lines = [line for line in ks_compaction_lines if "Compacting [" in line]
    end_compaction_lines = [line for line in ks_compaction_lines if "] Compacted" in line]
    expired_sstables = {re.search(r"[\w-]+-Data\.db", line).group(0) for line in lines if "Fully expired sstable" in line}
    assert expired_sstables, "no expired sstables found in logs"
    compactions = {}
    for line in start_compaction_lines:
        if expired_in_compaction := [s for s in expired_sstables if s in line]:
            compaction_id = line.split("compaction - [Compact keyspace1.standard1 ")[1].split("]")[0]
            compactions[compaction_id] = expired_in_compaction
    assert compactions, "no compaction of keyspace1.standard1 found in logs"
    for compaction_id, expired_in_compaction in compactions.items():
        for line in end_compaction_lines:
            if compaction_id in line:
                assert "sstables to []" in line, \
                    f"expired sstables {expired_in_compaction} were compacted along with unexpired ones. id: {compaction_id}. Issue #9533"
