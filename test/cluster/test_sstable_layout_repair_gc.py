#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
import datetime
import glob
import json
import logging
import os
import subprocess

import pytest

from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)


def run_layout(scylla_path: str, workdir: str, table_dir: str, *args: str) -> tuple[str, list[dict]]:
    """The header and the sstables `scylla sstable layout` reports for the table."""
    def layout(*extra: str) -> str:
        res = subprocess.run(
            [scylla_path, "sstable", "layout",
             "--scylla-yaml-file", os.path.join(workdir, "conf", "scylla.yaml"), *extra, *args, table_dir],
            text=True, capture_output=True, check=True)
        return res.stdout
    text = layout()
    header = next(line for line in text.splitlines() if line.startswith("tombstone_gc:"))
    sstables = json.loads(layout("--output-format", "json", "--columns", "tombstones,expired"))
    logger.info("%s: %s", header, sstables)
    return header, [sst for group in sstables["compaction_groups"]
                    for bucket in group["buckets"] for sst in bucket["sstables"]]


def tombstone_drop_time(scylla_path: str, workdir: str, table_dir: str) -> int:
    """The newest local deletion time of the tombstones in the table."""
    res = subprocess.run(
        [scylla_path, "sstable", "dump-statistics",
         "--scylla-yaml-file", os.path.join(workdir, "conf", "scylla.yaml")]
        + glob.glob(os.path.join(table_dir, "*-Data.db")),
        text=True, capture_output=True, check=True)
    drop_times = [int(t) for sst in json.loads(res.stdout)["sstables"].values()
                  for t in sst["stats"]["estimated_tombstone_drop_time"]]
    assert drop_times
    return max(drop_times)


@pytest.mark.parametrize("tablets", [True, False])
async def test_layout_reports_expired_tombstones_from_repair_history(
        manager: ScyllaClusterManager, tablets: bool) -> None:
    """Under the repair tombstone_gc mode, whether a tombstone counts as expired
    follows from when the token range it is in was repaired. The layout operation
    reads that from system.tablets for a tablet-based table and from
    system.repair_history for a vnode-based one.

    Two nodes are needed: a repair with no peer to repair with is skipped and
    records the epoch as its repair time. Each gets its own rack, as a
    tablet-based keyspace wants the replication factor to fit the racks.
    """
    servers = await manager.servers_add(2, auto_rack_dc="dc1")
    cql = manager.get_cql()
    server = servers[0]

    tablets_option = "AND tablets = {'initial': 1}" if tablets else "AND tablets = {'enabled': false}"
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy',"
                                 f" 'replication_factor': 2}} {tablets_option}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int) WITH tombstone_gc = "
                            "{'mode': 'repair', 'propagation_delay_in_seconds': 0}")

        # No compaction may run: it would purge the tombstones the test counts,
        # and it could remove an sstable of the system tables the tool reads
        # while it is being read.
        for keyspace in (ks, "system", "system_schema"):
            await manager.api.disable_autocompaction(server.ip_addr, keyspace)

        deleted_keys = 8
        for pk in range(16):
            await cql.run_async(f"INSERT INTO {ks}.t (pk, v) VALUES ({pk}, 0)")
        for pk in range(deleted_keys):
            await cql.run_async(f"DELETE FROM {ks}.t WHERE pk = {pk}")

        for keyspace in (ks, "system", "system_schema"):
            await manager.api.flush_keyspace(server.ip_addr, keyspace)

        scylla_path = await manager.server_get_exe(server.server_id)
        workdir = await manager.server_get_workdir(server.server_id)
        table_dir = glob.glob(os.path.join(workdir, "data", ks, "t-*"))[0]

        # Nothing was repaired yet, so nothing in the table can be purged.
        header, before = run_layout(scylla_path, workdir, table_dir)
        assert "repaired ranges: 0" in header, header
        assert sum(sst["tombstones"] for sst in before) == deleted_keys, before
        assert all(sst["expired"] == 0 for sst in before), before

        await manager.api.repair(server.ip_addr, ks, "t")
        # the repair time lands in system.tablets or in system.repair_history
        await manager.api.flush_keyspace(server.ip_addr, "system")

        header, after = run_layout(scylla_path, workdir, table_dir)
        assert "repaired ranges: 0" not in header, header
        assert sum(sst["tombstones"] for sst in after) == deleted_keys, after

        # A repair certifies the data written before its flush point, which can
        # precede writes made just before it -- repair_cf_range_row_level() takes
        # the repair time from the flush time. So the tombstones only count as
        # expired if the repair time the node recorded is past them, and the
        # layout has to report exactly that.
        table_id = (await cql.run_async(f"SELECT id FROM system_schema.tables WHERE keyspace_name = '{ks}'"
                                        " AND table_name = 't'"))[0].id
        if tablets:
            rows = await cql.run_async(f"SELECT repair_time FROM system.tablets WHERE table_id = {table_id}")
        else:
            rows = await cql.run_async("SELECT repair_time FROM system.repair_history"
                                       f" WHERE table_uuid = {table_id}")
        # the driver hands the timestamp over as a naive datetime in UTC
        repaired_at = max(int(row.repair_time.replace(tzinfo=datetime.timezone.utc).timestamp()) for row in rows)
        drop_time = tombstone_drop_time(scylla_path, workdir, table_dir)
        expected = deleted_keys if drop_time < repaired_at else 0
        logger.info("repaired_at=%d drop_time=%d expected expired=%d", repaired_at, drop_time, expected)
        assert sum(sst["expired"] for sst in after) == expected, after
