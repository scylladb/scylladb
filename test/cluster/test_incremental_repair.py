#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.pylib.manager_client import ManagerClient
from test.pylib.repair import load_tablet_sstables_repaired_at, load_tablet_repair_time, create_table_insert_data_for_repair
from test.pylib.tablets import get_all_tablet_replicas
from test.cluster.tasks.task_manager_client import TaskManagerClient
from test.cluster.util import reconnect_driver, find_server_by_host_id, get_topology_coordinator, ensure_group0_leader_on, new_test_keyspace, new_test_table, trigger_stepdown
from test.pylib.util import wait_for_cql_and_get_hosts

from cassandra.query import ConsistencyLevel, SimpleStatement

import pytest
import asyncio
import logging
import time
import glob
import os
import subprocess
import json
import socket
import random
import requests
import re

logger = logging.getLogger(__name__)

async def inject_error_on(manager, error_name, servers, params = {}):
    errs = [manager.api.enable_injection(s.ip_addr, error_name, False, params) for s in servers]
    await asyncio.gather(*errs)

async def inject_error_off(manager, error_name, servers):
    errs = [manager.api.disable_injection(s.ip_addr, error_name) for s in servers]
    await asyncio.gather(*errs)

def get_sstables_repaired_at(m, token):
    return m[str(token)]

def get_sstables(workdir, ks, table):
    base_pattern = os.path.join(workdir, "data", f"{ks}*", f"{table}-*","*-Data.db")
    sstables = glob.glob(base_pattern)
    return sstables

async def get_sst_status(run, log, from_mark=None):
    sst_add = await log.grep(rf'.*Added sst.*for incremental repair', from_mark=from_mark)
    sst_skip = await log.grep(rf'.*Skipped adding sst.*for incremental repair', from_mark=from_mark)
    sst_mark = await log.grep(rf'.*Marking.*for incremental repair', from_mark=from_mark)
    logging.info(f'{run=}: {sst_add=} {sst_skip=} {sst_mark=}');
    logging.info(f'{run=}: {len(sst_add)=} {len(sst_skip)=} {len(sst_mark)=}');
    return sst_add, sst_skip, sst_mark

async def insert_keys(cql, ks, start_key, end_key):
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in range(start_key, end_key)])

def get_repaired_at_from_sst(sst_file, scylla_path):
    try:
        cmd = [scylla_path, "sstable", "dump-statistics", sst_file]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        output = json.loads(result.stdout)
        repaired_at = output.get("sstables", {}).get(sst_file, {}).get("stats", {}).get("repaired_at")
        return repaired_at
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed with error: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON output: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

    return None

def get_keys_from_sst(sst_file, scylla_path):
    try:
        cmd = [scylla_path, "sstable", "dump-data", sst_file]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        output = json.loads(result.stdout)
        keys = []
        for sstable_data in output.get("sstables", {}).values():
            for entry in sstable_data:
                key_value = entry.get("key", {}).get("value")
                if key_value:
                    keys.append(int(key_value))
        return keys
    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed with error: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON output: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    return []

def get_metrics(server, metric_name):
    num = 0
    metrics = requests.get(f"http://{server.ip_addr}:9180/metrics").text
    logger.info(f"Got for {metric_name=} {metrics=}");
    pattern = re.compile(rf"^{metric_name}")
    for metric in metrics.split('\n'):
        if pattern.match(metric) is not None:
            num += int(metric.split()[1])
    return num

def get_incremental_repair_sst_skipped_bytes(server):
    return get_metrics(server, "scylla_repair_inc_sst_skipped_bytes")

def get_incremental_repair_sst_read_bytes(server):
    return get_metrics(server, "scylla_repair_inc_sst_read_bytes")

def get_repair_tablet_time_ms(server):
    return get_metrics(server, "scylla_repair_tablet_time_ms")

async def get_sstables_for_server(manager, server, ks):
    node_workdir = await manager.server_get_workdir(server.server_id)
    sstables = get_sstables(node_workdir, ks, 'test')
    return sstables

async def verify_repaired_and_unrepaired_keys(manager, scylla_path, servers, ks, repaired_keys, unrepaired_keys):
    async def process_server_keys(server):
        node_workdir = await manager.server_get_workdir(server.server_id)
        sstables = get_sstables(node_workdir, ks, 'test')
        nr_sstables = len(sstables)
        async def process_sst(sst):
            keys = get_keys_from_sst(sst, scylla_path)
            repaired_at = get_repaired_at_from_sst(sst, scylla_path)
            logger.info(f"Got {node_workdir=} {nr_sstables=} {sst=} {repaired_at=} {keys=} {repaired_keys=} {unrepaired_keys=}")
            if repaired_at == 0 or repaired_at is None:
                for key in keys:
                    assert node_workdir and sst and key in unrepaired_keys
            elif repaired_at > 0:
                for key in keys:
                    assert node_workdir and sst and key in repaired_keys
        await asyncio.gather(*(process_sst(sst) for sst in sstables))
    await asyncio.gather(*[process_server_keys(server) for server in servers])

async def verify_max_repaired_at(manager, scylla_path, servers, ks, max_repaired_at):
    async def process_server_keys(server):
        node_workdir = await manager.server_get_workdir(server.server_id)
        sstables = get_sstables(node_workdir, ks, 'test')
        nr_sstables = len(sstables)
        for sst in sstables:
            keys = get_keys_from_sst(sst, scylla_path)
            repaired_at = get_repaired_at_from_sst(sst, scylla_path)
            logger.info(f"Got {node_workdir=} {nr_sstables=} {sst=} {repaired_at=}")
            assert repaired_at <= max_repaired_at
    await asyncio.gather(*[process_server_keys(server) for server in servers])

async def trigger_tablet_merge(manager, servers, logs):
    s1_log = logs[0]
    s1_mark = await s1_log.mark()
    await inject_error_on(manager, "tablet_force_tablet_count_decrease", servers)
    await s1_log.wait_for('Detected tablet merge for table', from_mark=s1_mark)
    await inject_error_off(manager, "tablet_force_tablet_count_decrease", servers)

async def prepare_cluster_for_incremental_repair(manager, nr_keys = 100 , cmdline = [], tablets = 8):
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, nr_keys=nr_keys, cmdline=cmdline, tablets=tablets)
    repaired_keys = set(range(0, nr_keys))
    unrepaired_keys = set()
    current_key = nr_keys
    token = 'all'
    logs = []
    for s in servers:
        logs.append(await manager.server_open_log(s.server_id))
    return servers, cql, hosts, ks, table_id, logs, repaired_keys,  unrepaired_keys, current_key, token

@pytest.mark.asyncio
async def test_tablet_repair_sstable_skipped_read_metrics(manager: ManagerClient):
    servers, cql, hosts, ks, table_id, logs, _, _, _, token = await prepare_cluster_for_incremental_repair(manager)

    await insert_keys(cql, ks, 0, 100)

    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental')
    skipped_bytes = get_incremental_repair_sst_skipped_bytes(servers[0])
    read_bytes = get_incremental_repair_sst_read_bytes(servers[0])
    # Nothing to skip. Repair all data.
    assert skipped_bytes == 0
    assert read_bytes > 0

    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental')
    skipped_bytes2 = get_incremental_repair_sst_skipped_bytes(servers[0])
    read_bytes2 = get_incremental_repair_sst_read_bytes(servers[0])
    # Skip all. Nothing to repair
    assert skipped_bytes2 > 0
    assert read_bytes2 == read_bytes

    await insert_keys(cql, ks, 200, 300)

    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental')
    skipped_bytes3 = get_incremental_repair_sst_skipped_bytes(servers[0])
    read_bytes3 = get_incremental_repair_sst_read_bytes(servers[0])
    # Both skipped and read bytes should grow
    assert skipped_bytes3 > skipped_bytes2
    assert read_bytes3 > read_bytes2

@pytest.mark.asyncio
async def test_tablet_incremental_repair(manager: ManagerClient):
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, fast_stats_refresh=False, disable_flush_cache_time=True)
    token = -1
    logs = []
    for s in servers:
        logs.append(await manager.server_open_log(s.server_id))

    sstables_repaired_at = 0

    async def get_compaction_status(run, log):
        disable = await log.grep(rf'.*Disabled compaction for.*for incremental repair')
        enable = await log.grep(rf'.*Re-enabled compaction for.*for incremental repair')
        logging.info(f'{run=}: {disable=} {enable=}');
        return disable, enable

    async def get_sst_status(run, log):
        sst_add = await log.grep(rf'.*Added sst.*for incremental repair')
        sst_skip = await log.grep(rf'.*Skipped adding sst.*for incremental repair')
        sst_mark = await log.grep(rf'.*Marking.*for incremental repair')
        logging.info(f'{run=}: {sst_add=} {sst_skip=} {sst_mark=}');
        logging.info(f'{run=}: {len(sst_add)=} {len(sst_skip)=} {len(sst_mark)=}');
        return sst_add, sst_skip, sst_mark

    map0 = await load_tablet_sstables_repaired_at(manager, cql, servers[0], hosts[0], table_id)
    logging.info(f'map1={map0}')
    assert get_sstables_repaired_at(map0, token) == sstables_repaired_at

    # First repair
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental')
    map1 = await load_tablet_sstables_repaired_at(manager, cql, servers[0], hosts[0], table_id)
    logging.info(f'map1={map1}')
    # Check sstables_repaired_at is increased by 1
    assert get_sstables_repaired_at(map1, token) == sstables_repaired_at + 1
    # Check one sstable is added and marked as repaired
    for log in logs:
        sst_add, sst_skip, sst_mark = await get_sst_status("First", log)
        assert len(sst_add) == 1
        assert len(sst_skip) == 0
        assert len(sst_mark) == 1
        disable, enable = await get_compaction_status("First", log)
        assert len(disable) == 1
        assert len(enable) == 1

    # Second repair
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental')
    map2 = await load_tablet_sstables_repaired_at(manager, cql, servers[0], hosts[0], table_id)
    logging.info(f'map2={map2}')
    # Check sstables_repaired_at is increased by 1
    assert get_sstables_repaired_at(map2, token) == sstables_repaired_at + 2
    # Check one sstable is skipped
    for log in logs:
        sst_add, sst_skip, sst_mark = await get_sst_status("Second", log)
        assert len(sst_add) == 1
        assert len(sst_skip) == 1
        assert len(sst_mark) == 1
        disable, enable = await get_compaction_status("Second", log)
        assert len(disable) == 2
        assert len(enable) == 2

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tablet_incremental_repair_error(manager: ManagerClient):
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager)
    token = -1
    map0 = await load_tablet_sstables_repaired_at(manager, cql, servers[0], hosts[0], table_id)

    # Repair should not finish while the injection is enabled. We abort the task
    # before turning the injection off, otherwise it may continue in background
    # and increase sstables_repaired_at.
    await inject_error_on(manager, "repair_tablet_fail_on_rpc_call", servers)
    try:
        repair_response = await manager.api.tablet_repair(
            servers[0].ip_addr,
            ks,
            "test",
            token,
            await_completion=False,
            incremental_mode='incremental',
        )
        task_id = repair_response['tablet_task_id']

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(manager.api.wait_task(servers[0].ip_addr, task_id), timeout=10)

        await manager.api.abort_task(servers[0].ip_addr, task_id)
        await manager.api.wait_task(servers[0].ip_addr, task_id)
    finally:
        await inject_error_off(manager, "repair_tablet_fail_on_rpc_call", servers)

    map1 = await load_tablet_sstables_repaired_at(manager, cql, servers[0], hosts[0], table_id)

    # Check the sstables_repaired_at is not increased
    assert get_sstables_repaired_at(map0, token) == get_sstables_repaired_at(map1, token)

async def do_tablet_incremental_repair_and_ops(manager: ManagerClient, ops: str):
    nr_keys = 100
    servers, cql, hosts, ks, table_id, logs, repaired_keys, unrepaired_keys, current_key, token = await prepare_cluster_for_incremental_repair(manager, nr_keys, cmdline=['--logger-log-level', 'compaction=debug'])
    token = -1

    marks = [await log.mark() for log in logs]
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental')
    # 1 add 0 skip 1 mark
    for log, mark in zip(logs, marks):
        sst_add, sst_skip, sst_mark = await get_sst_status("First", log, from_mark=mark)
        assert len(sst_add) == 1
        assert len(sst_skip) == 0
        assert len(sst_mark) == 1

    await insert_keys(cql, ks, current_key, current_key + nr_keys)
    current_key += nr_keys

    # Test ops does not mix up repaired and unrepaired sstables together
    for server in servers:
        if (ops == 'scrub_abort'):
            await manager.api.keyspace_scrub_sstables(server.ip_addr, ks, 'ABORT')
        elif (ops == 'scrub_validate'):
            await manager.api.keyspace_scrub_sstables(server.ip_addr, ks, 'VALIDATE')
        elif (ops == 'cleanup'):
            await manager.api.cleanup_keyspace(server.ip_addr, ks)
        elif (ops == 'upgradesstables'):
            await manager.api.keyspace_upgrade_sstables(server.ip_addr, ks)
        elif (ops == 'major'):
            await manager.api.keyspace_compaction(server.ip_addr, ks, 'test')
        else:
            assert False # Wrong ops

    marks = [await log.mark() for log in logs]
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental')

    # 1 add 1 skip 1 mark
    for log, mark in zip(logs, marks):
        sst_add, sst_skip, sst_mark = await get_sst_status("Second", log, from_mark=mark)
        assert len(sst_add) == 1
        assert len(sst_mark) == 1
        assert len(sst_skip) == 1

@pytest.mark.asyncio
async def test_tablet_incremental_repair_and_scrubsstables_abort(manager: ManagerClient):
    await do_tablet_incremental_repair_and_ops(manager, 'scrub_abort')

@pytest.mark.asyncio
async def test_tablet_incremental_repair_and_scrubsstables_validate(manager: ManagerClient):
    await do_tablet_incremental_repair_and_ops(manager, 'scrub_validate')

@pytest.mark.asyncio
async def test_tablet_incremental_repair_and_cleanup(manager: ManagerClient):
    await do_tablet_incremental_repair_and_ops(manager, 'cleanup')

@pytest.mark.asyncio
async def test_tablet_incremental_repair_and_upgradesstables(manager: ManagerClient):
    await do_tablet_incremental_repair_and_ops(manager, 'upgradesstables')

@pytest.mark.asyncio
async def test_tablet_incremental_repair_and_major(manager: ManagerClient):
    await do_tablet_incremental_repair_and_ops(manager, 'major')

@pytest.mark.asyncio
async def test_tablet_incremental_repair_and_minor(manager: ManagerClient):
    nr_keys = 100
    servers, cql, hosts, ks, table_id, logs, repaired_keys, unrepaired_keys, current_key, token = await prepare_cluster_for_incremental_repair(manager, nr_keys)

    # Disable autocompaction
    for server in servers:
        await manager.api.disable_autocompaction(server.ip_addr, ks, 'test')

    # First repair
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental')

    # Insert more keys
    await insert_keys(cql, ks, current_key, current_key + nr_keys)
    repaired_keys = repaired_keys | set(range(current_key, current_key + nr_keys))
    current_key += nr_keys

    # Second repair
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental')

    # Insert more keys and flush to get 2 more sstables
    for _ in range(2):
        await insert_keys(cql, ks, current_key, current_key + nr_keys)
        unrepaired_keys = unrepaired_keys | set(range(current_key, current_key + nr_keys))
        current_key += nr_keys
        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, ks)

    # Enable autocompaction
    for server in servers:
        await manager.api.enable_autocompaction(server.ip_addr, ks, 'test')

    tm = TaskManagerClient(manager.api)
    for server in servers:
        module_name = "compaction"
        tasks = await tm.list_tasks(server.ip_addr, module_name)
        logger.info(f"Compaction {tasks=}")
        await tm.drain_module_tasks(server.ip_addr, module_name)

    # Verify repaired and unrepaired keys
    scylla_path = await manager.server_get_exe(servers[0].server_id)

    for server in servers:
        await manager.server_stop_gracefully(server.server_id)

    await verify_repaired_and_unrepaired_keys(manager, scylla_path, servers, ks, repaired_keys, unrepaired_keys)

async def do_test_tablet_incremental_repair_with_split_and_merge(manager, do_split, do_merge):
    nr_keys = 100
    servers, cql, hosts, ks, table_id, logs, repaired_keys, unrepaired_keys, current_key, token = await prepare_cluster_for_incremental_repair(manager, nr_keys)

    # First repair
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental') # sstables_repaired_at 1
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token) # sstables_repaired_at 2

    # Insert more keys
    await insert_keys(cql, ks, current_key, current_key + nr_keys)
    repaired_keys = repaired_keys | set(range(current_key, current_key + nr_keys))
    current_key += nr_keys

    # Second repair
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental') # sstables_repaired_at 3

    # Insert more keys and flush to get 2 more sstables
    for _ in range(2):
        await insert_keys(cql, ks, current_key, current_key + nr_keys)
        unrepaired_keys = unrepaired_keys | set(range(current_key, current_key + nr_keys))
        current_key += nr_keys
        for server in servers:
            await manager.api.flush_keyspace(server.ip_addr, ks)

    if do_split:
        s1_mark = await logs[0].mark()
        await inject_error_on(manager, "tablet_force_tablet_count_increase", servers)
        await logs[0].wait_for('Detected tablet split for table', from_mark=s1_mark)
        await inject_error_off(manager, "tablet_force_tablet_count_increase", servers)

    if do_merge:
        await trigger_tablet_merge(manager, servers, logs)

    scylla_path = await manager.server_get_exe(servers[0].server_id)

    await asyncio.sleep(random.randint(1, 5))

    for server in servers:
        await manager.server_stop_gracefully(server.server_id)

    await verify_repaired_and_unrepaired_keys(manager, scylla_path, servers, ks, repaired_keys, unrepaired_keys)

@pytest.mark.skip_bug(reason="https://github.com/scylladb/scylladb/issues/24153")
@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tablet_incremental_repair_with_split_and_merge(manager: ManagerClient):
    await do_test_tablet_incremental_repair_with_split_and_merge(manager, do_split=True, do_merge=True)

@pytest.mark.skip_bug(reason="https://github.com/scylladb/scylladb/issues/24153")
@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tablet_incremental_repair_with_split(manager: ManagerClient):
    await do_test_tablet_incremental_repair_with_split_and_merge(manager, do_split=True, do_merge=False)

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tablet_incremental_repair_with_merge(manager: ManagerClient):
    await do_test_tablet_incremental_repair_with_split_and_merge(manager, do_split=False, do_merge=True)

@pytest.mark.asyncio
async def test_tablet_incremental_repair_existing_and_repair_produced_sstable(manager: ManagerClient):
    nr_keys = 100
    cmdline = ["--hinted-handoff-enabled", "0"]
    servers, cql, hosts, ks, table_id, logs, repaired_keys, unrepaired_keys, current_key, token = await prepare_cluster_for_incremental_repair(manager, nr_keys, cmdline)

    await manager.server_stop_gracefully(servers[1].server_id)

    # Insert more keys
    await insert_keys(cql, ks, current_key, current_key + nr_keys)
    repaired_keys = repaired_keys | set(range(current_key, current_key + nr_keys))
    current_key += nr_keys

    await manager.server_start(servers[1].server_id)

    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental')

    scylla_path = await manager.server_get_exe(servers[0].server_id)

    for server in servers:
        await manager.server_stop_gracefully(server.server_id)

    await verify_repaired_and_unrepaired_keys(manager, scylla_path, servers, ks, repaired_keys, unrepaired_keys)

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tablet_incremental_repair_merge_higher_repaired_at_number(manager):
    nr_keys = 100
    servers, cql, hosts, ks, table_id, logs, repaired_keys, unrepaired_keys, current_key, token = await prepare_cluster_for_incremental_repair(manager, nr_keys)

    # First repair
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental') # sstables_repaired_at 1
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental') # sstables_repaired_at 2

    # Insert more keys
    await insert_keys(cql, ks, current_key, current_key + nr_keys)
    unrepaired_keys = unrepaired_keys | set(range(current_key, current_key + nr_keys))
    current_key += nr_keys

    # Second repair
    await inject_error_on(manager, "repair_tablet_no_update_sstables_repair_at", servers)
    # some sstable repaired_at = 3, but sstables_repaired_at = 2
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental') # sstables_repaired_at 2
    await inject_error_off(manager, "repair_tablet_no_update_sstables_repair_at", servers)

    scylla_path = await manager.server_get_exe(servers[0].server_id)

    s1_mark = await logs[0].mark()
    await trigger_tablet_merge(manager, servers, logs)
    # The merge process will set the unrepaired sstable with repaired_at=3 to repaired_at=0 during merge
    await logs[0].wait_for('Updating repaired_at for tablet merge .* old=3 new=0 sstables_repaired_at=2', from_mark=s1_mark)
    await logs[0].wait_for('Completed updating repaired_at=.* for tablet merge', from_mark=s1_mark)

    for server in servers:
        await manager.server_stop_gracefully(server.server_id)

    # Verify max repaired_at of sstables should be 2
    max_repaired_at = 2
    await verify_max_repaired_at(manager, scylla_path, servers, ks, 2)

    for server in servers:
        await manager.server_stop_gracefully(server.server_id)

    await verify_repaired_and_unrepaired_keys(manager, scylla_path, servers, ks, repaired_keys, unrepaired_keys)

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tablet_incremental_repair_merge_correct_repaired_at_number_after_merge(manager):
    nr_keys = 100
    servers, cql, hosts, ks, table_id, logs, repaired_keys, unrepaired_keys, current_key, token = await prepare_cluster_for_incremental_repair(manager, nr_keys)

    # First repair
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental') # sstables_repaired_at 1
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental') # sstables_repaired_at 2

    # Insert more keys
    await insert_keys(cql, ks, current_key, current_key + nr_keys)
    unrepaired_keys = unrepaired_keys | set(range(current_key, current_key + nr_keys))
    current_key += nr_keys

    # Second repair
    replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
    last_tokens = [t.last_token for t in replicas]
    for t in last_tokens[0::2]:
        logging.info(f"Start repair for token={t}");
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", t, incremental_mode='incremental') # sstables_repaired_at 3

    scylla_path = await manager.server_get_exe(servers[0].server_id)

    # Trigger merge
    await trigger_tablet_merge(manager, servers, logs)

    # Verify sstables_repaired_at should be 3 after merge
    for server, host in zip(servers, hosts):
        token_repaired_at_map = await load_tablet_sstables_repaired_at(manager, cql, server, host, table_id)
        for last_token, sstables_repaired_at in token_repaired_at_map.items():
            logging.info(f"{last_token=} {sstables_repaired_at=}")
            assert sstables_repaired_at == 3

async def do_test_tablet_incremental_repair_merge_error(manager, error):
    nr_keys = 100
    # Make sure no data commit log replay after force server stop
    cmdline = ['--enable-commitlog', '0']
    servers, cql, hosts, ks, table_id, logs, repaired_keys, unrepaired_keys, current_key, token = await prepare_cluster_for_incremental_repair(manager, nr_keys, cmdline)

    # First repair
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental') # sstables_repaired_at 1

    # Insert more keys
    await insert_keys(cql, ks, current_key, current_key + nr_keys)
    unrepaired_keys = unrepaired_keys | set(range(current_key, current_key + nr_keys))
    current_key += nr_keys

    for server in servers:
        await manager.api.flush_keyspace(server.ip_addr, ks)

    scylla_path = await manager.server_get_exe(server.server_id)

    coord = await get_topology_coordinator(manager)
    coord_serv = await find_server_by_host_id(manager, servers, coord)
    coord_log = await manager.server_open_log(coord_serv.server_id)

    # Trigger merge and error in merge
    mark = await coord_log.mark()
    await inject_error_on(manager, error, [coord_serv])
    await inject_error_on(manager, "tablet_force_tablet_count_decrease", servers)
    await inject_error_on(manager, "tablet_force_tablet_count_decrease_once", servers)
    await coord_log.wait_for(f'Got {error}', from_mark=mark)
    await inject_error_off(manager, "tablet_force_tablet_count_decrease", servers)
    await manager.server_stop(coord_serv.server_id)
    await manager.server_start(coord_serv.server_id)

    for server in servers:
        await manager.server_stop_gracefully(server.server_id)

    await verify_repaired_and_unrepaired_keys(manager, scylla_path, servers, ks, repaired_keys, unrepaired_keys)

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tablet_incremental_repair_merge_error_in_merge_finalization(manager):
    await do_test_tablet_incremental_repair_merge_error(manager, 'handle_tablet_resize_finalization_for_merge_error')

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tablet_incremental_repair_merge_error_in_merge_completion_fiber(manager):
    await do_test_tablet_incremental_repair_merge_error(manager, 'merge_completion_fiber_error')

@pytest.mark.asyncio
async def test_tablet_repair_with_incremental_option(manager: ManagerClient):
    servers, cql, hosts, ks, table_id, logs, _, _, _, token = await prepare_cluster_for_incremental_repair(manager)
    token = -1

    sstables_repaired_at = 0

    async def do_repair_and_check(incremental_mode, expected_increment, expected_log_regex, check=None):
        nonlocal sstables_repaired_at
        mark = await logs[0].mark()
        if check:
            skip1 = get_incremental_repair_sst_skipped_bytes(servers[0])
            read1 = get_incremental_repair_sst_read_bytes(servers[0])
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode=incremental_mode)
        sstables_repaired_at += expected_increment
        m = await load_tablet_sstables_repaired_at(manager, cql, servers[0], hosts[0], table_id)
        assert get_sstables_repaired_at(m, token) == sstables_repaired_at
        await logs[0].wait_for(expected_log_regex, from_mark=mark)
        if check:
            skip2 = get_incremental_repair_sst_skipped_bytes(servers[0])
            read2 = get_incremental_repair_sst_read_bytes(servers[0])
            check(skip1, read1, skip2, read2)

    def check1(skip1, read1, skip2, read2):
        assert skip1 == 0
        assert read1 == 0
        assert skip2 == 0
        assert read2 > 0
    await do_repair_and_check(None, 1, rf'Starting tablet repair by API .* incremental_mode=incremental.*', check1)

    def check2(skip1, read1, skip2, read2):
        assert skip1 == skip2
        assert read1 == read2
    await do_repair_and_check('disabled', 0, rf'Starting tablet repair by API .* incremental_mode=disabled.*', check2)

    def check3(skip1, read1, skip2, read2):
        assert skip1 < skip2
        assert read1 == read2
    await do_repair_and_check('incremental', 1, rf'Starting tablet repair by API .* incremental_mode=incremental.*', check3)

    def check4(skip1, read1, skip2, read2):
        assert skip1 == skip2
        assert read1 < read2
    await do_repair_and_check('full', 1, rf'Starting tablet repair by API .* incremental_mode=full.*', check4)

@pytest.mark.asyncio
async def test_incremental_repair_tablet_time_metrics(manager: ManagerClient):
    servers, _, _, ks, _, _, _, _, _, token = await prepare_cluster_for_incremental_repair(manager)
    time1 = 0
    time2 = 0

    for s in servers:
        time1 += get_repair_tablet_time_ms(s)
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental')
    for s in servers:
        time2 += get_repair_tablet_time_ms(s)

    assert time1 == 0
    assert time2 > 0

# Reproducer for https://github.com/scylladb/scylladb/issues/26346
@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_incremental_repair_finishes_when_tablet_skips_end_repair_stage(manager):
    servers = await manager.servers_add(3, auto_rack_dc="dc1")

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, t text") as cf:
            table = cf.split('.')[-1]

            coord = await get_topology_coordinator(manager)
            coord_serv = await find_server_by_host_id(manager, servers, coord)
            coord_log = await manager.server_open_log(coord_serv.server_id)
            coord_mark = await coord_log.mark()

            await manager.api.enable_injection(coord_serv.ip_addr, "delay_end_repair_update", one_shot=False)
            response = await manager.api.tablet_repair(servers[0].ip_addr, ks, table, "all", await_completion=False, incremental_mode="incremental")
            task_id = response['tablet_task_id']

            await coord_log.wait_for("Finished tablet repair", from_mark=coord_mark)
            await trigger_stepdown(manager, coord_serv)

            # Disable injection in case, the same node is elected as coordinator
            await manager.api.disable_injection(coord_serv.ip_addr, "delay_end_repair_update")
            await manager.api.wait_task(servers[0].ip_addr, task_id)

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_incremental_repair_rejoin_do_tablet_operation(manager):
    cmdline = ['--logger-log-level', 'raft_topology=debug']
    servers = await manager.servers_add(3, auto_rack_dc="dc1", cmdline=cmdline)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, t text") as cf:
            table = cf.split('.')[-1]

            async def get_coord():
                coord = await get_topology_coordinator(manager)
                coord_serv = await find_server_by_host_id(manager, servers, coord)
                coord_log = await manager.server_open_log(coord_serv.server_id)
                return coord, coord_serv, coord_log

            for s in servers:
                await manager.api.enable_injection(s.ip_addr, "repair_finish_wait", one_shot=False)

            coord, coord_serv, coord_log = await get_coord()

            response = await manager.api.tablet_repair(servers[0].ip_addr, ks, table, "all", await_completion=False, incremental_mode="incremental")
            task_id = response['tablet_task_id']

            await coord_log.wait_for("Initiating tablet repair")
            await trigger_stepdown(manager, coord_serv)

            coord, coord_serv, coord_log = await get_coord()
            await coord_log.wait_for("Initiating tablet repair")

            found = False
            for run in range(10):
                for s in servers:
                    log = await manager.server_open_log(s.server_id)
                    res = await log.grep('Repair retry joining with existing session for tablet')
                    if len(res) > 0:
                        found = True
                        break
                if found:
                    break
                await asyncio.sleep(3)
            assert found

            for s in servers:
                await manager.api.message_injection(s.ip_addr, "repair_finish_wait")
                await manager.api.disable_injection(s.ip_addr, "repair_finish_wait")
            await coord_log.wait_for("Finished tablet repair")

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_incremental_retry_end_repair_stage(manager):
    config = {'tablet_load_stats_refresh_interval_in_seconds': 1}
    servers = await manager.servers_add(3, auto_rack_dc="dc1", config=config)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1}") as ks:
        async with new_test_table(manager, ks, "pk int PRIMARY KEY, t text") as cf:
            table = cf.split('.')[-1]

            async def get_coord():
                coord = await get_topology_coordinator(manager)
                coord_serv = await find_server_by_host_id(manager, servers, coord)
                coord_log = await manager.server_open_log(coord_serv.server_id)
                return coord, coord_serv, coord_log

            for s in servers:
                await manager.api.enable_injection(s.ip_addr, "fail_rpc_repair_update_compaction_ctrl", one_shot=False)
                await manager.api.enable_injection(s.ip_addr, "log_tablet_transition_stage_end_repair", one_shot=False)

            coord, coord_serv, coord_log = await get_coord()

            response = await manager.api.tablet_repair(servers[0].ip_addr, ks, table, "all", await_completion=False, incremental_mode="incremental")
            task_id = response['tablet_task_id']

            await coord_log.wait_for("Finished tablet repair")
            await trigger_stepdown(manager, coord_serv)

            coord, coord_serv, coord_log = await get_coord()
            await coord_log.wait_for(r"Failed to perform repair_update_compaction_ctrl for tablet repair .* nr_retried=3")

            for s in servers:
                await manager.api.disable_injection(s.ip_addr, "fail_rpc_repair_update_compaction_ctrl")

            await coord_log.wait_for(r"The end_repair stage finished for tablet repair")

            try:
                await manager.api.wait_task(servers[0].ip_addr, task_id)
            except Exception as e:
                # Currently wait_task throws task not found in case the task_id has finished
                if "not found" not in str(e):
                    raise e

            # Run the second repair after the first is finished
            await manager.api.tablet_repair(servers[0].ip_addr, ks, table, "all", await_completion=True, incremental_mode="incremental")

# Reproducer for https://github.com/scylladb/scylladb/issues/27666.
# This test checks both tablet and vnode table. It tests the code path dealing
# with appending sstables produced by repair to a list work correctly with
# multishard writer when the shard count is different.
@pytest.mark.asyncio
@pytest.mark.parametrize("use_tablet", [False, True])
async def test_repair_sigsegv_with_diff_shard_count(manager: ManagerClient, use_tablet):
    cmdline0 = [ '--smp', '2']
    cmdline1 = [ '--smp', '3']
    servers = await manager.servers_add(1, cmdline=cmdline0, auto_rack_dc="dc1")
    servers.append(await manager.server_add(cmdline=cmdline1, property_file={'dc': 'dc1', 'rack': 'rack2'}))

    cql = manager.get_cql()

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}} AND TABLETS = {{ 'enabled': {str(use_tablet).lower()} }}  ") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        async def write_with_cl_one(range_start, range_end):
            insert_stmt = cql.prepare(f"INSERT INTO {ks}.test (pk, c) VALUES (?, ?)")
            insert_stmt.consistency_level = ConsistencyLevel.ONE
            pks = range(range_start, range_end)
            await asyncio.gather(*[cql.run_async(insert_stmt, (k, k)) for k in pks])

        logger.info("Adding data only on first node")
        await manager.api.flush_keyspace(servers[1].ip_addr, ks)
        await manager.server_stop(servers[1].server_id)
        manager.driver_close()
        cql = await reconnect_driver(manager)
        await write_with_cl_one(0, 10)
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)

        logger.info("Adding data only on second node")
        await manager.server_start(servers[1].server_id)
        await manager.api.flush_keyspace(servers[0].ip_addr, ks)
        await manager.server_stop(servers[0].server_id)
        manager.driver_close()
        cql = await reconnect_driver(manager)
        await write_with_cl_one(10, 20)

        await manager.server_start(servers[0].server_id)
        await manager.servers_see_each_other(servers)

        if use_tablet:
            logger.info("Starting tablet repair")
            await manager.api.tablet_repair(servers[1].ip_addr, ks, "test", token='all', incremental_mode="incremental")
        else:
            logger.info("Starting vnode repair")
            await manager.api.repair(servers[1].ip_addr, ks, "test")

# Reproducer for https://github.com/scylladb/scylladb/issues/27365
# Incremental repair vs table drop
@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tablet_incremental_repair_table_drop_compaction_group_gone(manager: ManagerClient):
    cmdline = ['--logger-log-level', 'repair=debug']
    servers, cql, hosts, ks, table_id, logs, _, _, _, _ = await prepare_cluster_for_incremental_repair(manager, cmdline=cmdline)

    coord = await get_topology_coordinator(manager)
    coord_serv = await find_server_by_host_id(manager, servers, coord)
    coord_log = await manager.server_open_log(coord_serv.server_id)

    # Trigger merge and wait until the merge fiber starts
    s1_mark = await coord_log.mark()

    # Trigger repair and wait for the inc repair prepare preparation to start
    s1_mark = await coord_log.mark()
    await inject_error_on(manager, "wait_after_prepare_sstables_for_incremental_repair", servers)
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token=-1, await_completion=False, incremental_mode='incremental')
    # Wait for preparation to finish.
    await coord_log.wait_for('Re-enabled compaction for range', from_mark=s1_mark)

    s1_mark = await coord_log.mark()
    drop_future = cql.run_async(f"DROP TABLE {ks}.test;")
    await coord_log.wait_for(f'Stopping.*ongoing compactions for table {ks}.test', from_mark=s1_mark)
    await asyncio.sleep(0.2)

    # Continue the repair to trigger use-after-free
    for s in servers:
        await manager.api.message_injection(s.ip_addr, "wait_after_prepare_sstables_for_incremental_repair")

    await drop_future

# Reproducer for the race window bug in incremental repair where minor compaction
# promotes unrepaired data into the repaired sstable set.
#
# Root cause: after mark_sstable_as_repaired() writes new sstables with repaired_at=N+1
# on all replicas, there is a window before the coordinator commits sstables_repaired_at=N+1
# to Raft. During this window is_repaired() still uses the old threshold N, so
# repaired_at=N+1 does not satisfy repaired_at <= N and the sstables are misclassified as
# UNREPAIRED. Minor compaction can then merge them with a genuinely unrepaired sstable
# (repaired_at=0). Because compaction propagates max(repaired_at), the output carries
# repaired_at=N+1. Once sstables_repaired_at advances to N+1 the merged sstable is
# classified REPAIRED even though it contains post-repair data that was never part of the
# repair scan. Replicas that did not compact during this window keep that post-repair data
# in UNREPAIRED sstables. Future incremental repairs skip the REPAIRED sstable on the
# affected replica but process the UNREPAIRED sstable on the others, so the classification
# divergence is never corrected. In tombstone scenarios this enables premature tombstone GC
# on the affected replica leading to data resurrection.

class _LeadershipTransferred(Exception):
    """Raised when leadership transferred to servers[1] during the test, requiring a retry."""
    pass

async def _do_race_window_promotes_unrepaired_data(manager, servers, cql, ks, token, scylla_path, current_key):
    """Core logic for test_incremental_repair_race_window_promotes_unrepaired_data.

    Returns the next current_key value.
    Raises _LeadershipTransferred if servers[1] becomes coordinator after the
    restart, signalling the caller to retry.
    """
    # Ensure servers[1] is not the topology coordinator.  If the coordinator is
    # restarted, the Raft leader dies, a new election occurs, and the new
    # coordinator re-initiates tablet repair -- flushing memtables on all replicas
    # and marking post-repair data as repaired.  That legitimate re-repair masks
    # the compaction-merge bug this test detects.
    coord = await get_topology_coordinator(manager)
    coord_serv = await find_server_by_host_id(manager, servers, coord)
    if coord_serv == servers[1]:
        other = next(s for s in servers if s != servers[1])
        await ensure_group0_leader_on(manager, other)
        coord = await get_topology_coordinator(manager)
        coord_serv = await find_server_by_host_id(manager, servers, coord)
    coord_log = await manager.server_open_log(coord_serv.server_id)
    coord_mark = await coord_log.mark()

    # Hold the race window open: prevent the coordinator from committing
    # end_repair + sstables_repaired_at=2 to Raft.
    await manager.api.enable_injection(coord_serv.ip_addr, "delay_end_repair_update", one_shot=False)

    repair_response = await manager.api.tablet_repair(
        servers[0].ip_addr, ks, "test", token,
        await_completion=False, incremental_mode="incremental"
    )
    task_id = repair_response['tablet_task_id']

    # The topology coordinator logs "Finished tablet repair host=..." once per tablet
    # after mark_sstable_as_repaired() has completed on all replicas for that tablet.
    # With tablets=2, the coordinator logs this message twice (once per tablet).
    # We must wait for BOTH before writing post-repair keys; waiting for only the
    # first leaves the second tablet's repair in progress, which can flush the
    # memtable and mark newly-flushed sstables as repaired, contaminating servers[0]
    # and servers[2] with post-repair data in repaired sstables.
    #
    # IMPORTANT: We match "Finished tablet repair host=" specifically to avoid
    # matching the repair module's "Finished tablet repair for table=..." message
    # (repair/repair.cc), which is also logged on this node when the coordinator
    # happens to be a repair replica.  Both messages appear for the same tablet,
    # so the generic "Finished tablet repair" pattern would consume two messages
    # for one tablet and miss the second tablet entirely.
    #
    # After both tablets complete, S1 is fully rewritten as S1'(repaired_at=2,
    # being_repaired=session_id) on every replica, but sstables_repaired_at in system.tablets is
    # still 1, so is_repaired(1, S1'{repaired_at=2}) == false and S1' lands in the
    # UNREPAIRED compaction view on every replica. The race window is now open.
    pos, _ = await coord_log.wait_for("Finished tablet repair host=", from_mark=coord_mark)
    await coord_log.wait_for("Finished tablet repair host=", from_mark=pos)

    # --- Race window is open ---
    # Write post-repair keys 20-29.  All nodes receive the writes into their memtables
    # (RF=3, hinted handoff disabled).
    post_repair_keys = list(range(current_key, current_key + 10))
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k})") for k in post_repair_keys])
    current_key += 10

    # Flush servers[1] BEFORE the restart so E(repaired_at=0, keys 20-29) lands on disk.
    # At this point servers[1] holds on disk:
    #   S1'  repaired_at=2  being_repaired=session_id  (keys 10-19, from mark_sstable_as_repaired)
    #   E    repaired_at=0  being_repaired=null         (keys 20-29, genuine post-repair data)
    # servers[0] and servers[2] still have keys 20-29 only in their memtables.
    target = servers[1]
    await manager.api.flush_keyspace(target.ip_addr, ks)

    # Restart servers[1].  being_repaired is in-memory and is lost on restart.
    # After restart both S1' and E are loaded from disk with being_repaired=null.
    # Without the classifier fix: is_repaired(sstables_repaired_at=1, S1'{repaired_at=2})
    # is false and being_repaired is null, so S1' lands in the UNREPAIRED view where
    # autocompaction is active.  STCS (min_threshold=2) immediately merges S1' and E into
    # F(repaired_at=max(2,0)=2, keys 10-29), wrongly promoting E into the REPAIRED set.
    # With the classifier fix: S1' has repaired_at==sstables_repaired_at+1 and the tablet
    # is still in the `repair` stage, so it is classified REPAIRING (compaction disabled),
    # and the merge never happens.
    await manager.server_stop_gracefully(target.server_id)
    await manager.server_start(target.server_id)
    await manager.servers_see_each_other(servers)

    # Check if leadership transferred to servers[1] during the restart.
    # If so, the new coordinator will re-initiate repair, masking the bug.
    new_coord = await get_topology_coordinator(manager)
    new_coord_serv = await find_server_by_host_id(manager, servers, new_coord)
    if new_coord_serv == servers[1]:
        await manager.api.disable_injection(coord_serv.ip_addr, "delay_end_repair_update")
        await manager.api.wait_task(servers[0].ip_addr, task_id)
        raise _LeadershipTransferred(
            "servers[1] became topology coordinator after restart")

    # Poll until compaction has produced F(repaired_at=2) containing post-repair keys,
    # confirming that the bug was triggered (S1' and E merged during the race window).
    deadline = time.time() + 60
    compaction_ran = False
    while time.time() < deadline:
        for sst in await get_sstables_for_server(manager, target, ks):
            if get_repaired_at_from_sst(sst, scylla_path) == 2:
                if set(get_keys_from_sst(sst, scylla_path)) & set(post_repair_keys):
                    compaction_ran = True
                    logger.info(f"Post-restart compaction produced F(repaired_at=2) with post-repair keys: {sst}")
                    break
        if compaction_ran:
            break
        await asyncio.sleep(1)

    # --- Release the race window ---
    await manager.api.disable_injection(coord_serv.ip_addr, "delay_end_repair_update")
    await manager.api.wait_task(servers[0].ip_addr, task_id)

    if not compaction_ran:
        logger.warning("Compaction did not merge S1' and E after restart during the race window; "
                       "the bug was not triggered.  Skipping assertion.")
        return current_key

    # Flush servers[0] and servers[2] AFTER the race window closes so their post-repair
    # keys land in G(repaired_at=0): correctly classified as UNREPAIRED.
    for s in [servers[0], servers[2]]:
        await manager.api.flush_keyspace(s.ip_addr, ks)

    # Stop all servers so sstable files on disk are stable.
    for s in servers:
        await manager.server_stop_gracefully(s.server_id)

    post_repair_key_set = set(post_repair_keys)

    async def keys_in_repaired_sstables(server) -> set:
        """Return the set of keys found in any sstable with repaired_at > 0 on this server."""
        result = set()
        for sst in await get_sstables_for_server(manager, server, ks):
            ra = get_repaired_at_from_sst(sst, scylla_path)
            if ra is not None and ra > 0:
                result.update(get_keys_from_sst(sst, scylla_path))
        return result

    repaired_keys_0 = await keys_in_repaired_sstables(servers[0])
    repaired_keys_1 = await keys_in_repaired_sstables(servers[1])
    repaired_keys_2 = await keys_in_repaired_sstables(servers[2])

    logger.info(f"Post-repair keys in repaired sstables: "
                f"servers[0]={len(repaired_keys_0 & post_repair_key_set)}, "
                f"servers[1]={len(repaired_keys_1 & post_repair_key_set)}, "
                f"servers[2]={len(repaired_keys_2 & post_repair_key_set)}")

    # servers[0] and servers[2] were never restarted and the coordinator stayed
    # alive throughout, so no re-repair could have flushed their memtables.
    # Post-repair keys must NOT appear in repaired sstables on these servers.
    assert not (repaired_keys_0 & post_repair_key_set), \
        f"servers[0] should not have post-repair keys in repaired sstables, " \
        f"got: {repaired_keys_0 & post_repair_key_set}"
    assert not (repaired_keys_2 & post_repair_key_set), \
        f"servers[2] should not have post-repair keys in repaired sstables, " \
        f"got: {repaired_keys_2 & post_repair_key_set}"

    # BUG: servers[1] restarted during the race window, losing its in-memory being_repaired
    # markers.  S1'(repaired_at=2) and E(repaired_at=0) both landed in the UNREPAIRED
    # compaction view and were merged into F(repaired_at=2) by autocompaction.  After
    # sstables_repaired_at advances to 2, F is classified REPAIRED even though it contains
    # post-repair data that was never part of the repair scan.  This diverges from servers[0]
    # and servers[2] which keep those keys UNREPAIRED, enabling premature tombstone GC and
    # data resurrection.
    wrongly_promoted = repaired_keys_1 & post_repair_key_set
    assert not wrongly_promoted, \
        f"BUG: {len(wrongly_promoted)} post-repair keys were wrongly promoted to REPAIRED " \
        f"on servers[1] after restart lost the being_repaired markers during the race window. " \
        f"They are UNREPAIRED on servers[0] and servers[2] (classification divergence). " \
        f"Wrongly promoted (first 10): {sorted(wrongly_promoted)[:10]}"
    return current_key

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_incremental_repair_race_window_promotes_unrepaired_data(manager: ManagerClient):
    cmdline = ['--hinted-handoff-enabled', '0']
    servers, cql, hosts, ks, table_id, logs, _, _, current_key, token = \
        await prepare_cluster_for_incremental_repair(manager, nr_keys=10, cmdline=cmdline, tablets=2)

    # Lower min_threshold to 2 so STCS fires as soon as two sstables appear in the
    # UNREPAIRED compaction view, making the race easy to trigger deterministically.
    await cql.run_async(
        f"ALTER TABLE {ks}.test WITH compaction = "
        f"{{'class': 'SizeTieredCompactionStrategy', 'min_threshold': 2, 'max_threshold': 4}}"
    )

    # Disable autocompaction everywhere so we control exactly when compaction runs.
    for s in servers:
        await manager.api.disable_autocompaction(s.ip_addr, ks, 'test')

    scylla_path = await manager.server_get_exe(servers[0].server_id)

    # Repair 1: establishes sstables_repaired_at=1 on all nodes.
    # Keys 0-9 (inserted by preapre_cluster_for_incremental_repair) end up in
    # S0'(repaired_at=1) on all nodes.
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, incremental_mode='incremental')

    # Insert keys 10-19 and flush on all nodes -> S1(repaired_at=0).
    # These will be the subject of repair 2.
    repair2_keys = list(range(current_key, current_key + 10))
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k})") for k in repair2_keys])
    for s in servers:
        await manager.api.flush_keyspace(s.ip_addr, ks)
    current_key += 10

    # If leadership transfers to servers[1] between our coordinator check and the
    # restart, the coordinator change masks the bug.  Detect and retry.
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        try:
            current_key = await _do_race_window_promotes_unrepaired_data(
                manager, servers, cql, ks, token, scylla_path, current_key)
            return
        except _LeadershipTransferred as e:
            logger.warning(f"Attempt {attempt}/{max_attempts}: {e}.  Retrying.")

    pytest.fail(f"Leadership kept transferring to servers[1] after {max_attempts} attempts; "
                "could not run the test without coordinator interference.")

# ----------------------------------------------------------------------------
# Tombstone GC safety tests
#
# These tests verify that incremental repair with tombstone_gc=repair never
# causes data resurrection via premature tombstone GC.  The key invariant is:
#
#   mark_sstable_as_repaired() completes on ALL replicas BEFORE the coordinator
#   commits repair_time to Raft.  Therefore, by the time gc_before advances and
#   a tombstone T becomes GC-eligible, any data D that T shadows has already
#   been promoted from the repairing set to the repaired set.  Tombstone GC
#   running on the repaired set will always see D there and will prevent T from
#   being purged prematurely.
#
# Three scenarios are tested:
#
# 1. Basic ordering guarantee: D arrives via hint flush at the start of repair,
#    is captured in the repairing snapshot, and is promoted to repaired before
#    repair_time advances.  GC then runs on the repaired set; T must not be
#    purged while D is still present.
#
# 2. Hints flush failure: hints flush times out on one replica so D is not
#    delivered during that repair.  repair_time must NOT advance (the guard in
#    repair that skips the repair_history update when hints_batchlog_flushed is
#    false).  Therefore T does not become GC-eligible from this repair, and no
#    resurrection can occur even though D is not yet on that replica.
#
# 3. Propagation delay: D is written with an old USING TIMESTAMP (simulating a
#    write that arrives within the propagation delay window).  T is written
#    later with a higher timestamp.  Repair runs, D is captured in repairing
#    (delivered via hint flush), T is in repaired.  After repair completes D is
#    in repaired too.  GC on the repaired set must not purge T because D has a
#    lower timestamp but is visible.
# ----------------------------------------------------------------------------

async def _setup_tombstone_gc_cluster(manager, *, tablets=2, extra_cmdline=None):
    """Create a 3-node cluster with a tombstone_gc=repair table and return
    (servers, cql, hosts, ks, table_id, logs).

    Uses propagation_delay_in_seconds=0 so that tombstones become GC-eligible
    immediately after repair_time is committed (T.deletion_time < repair_time = gc_before),
    allowing the tests to actually exercise the GC eligibility path without sleeping.
    """
    cmdline = ['--logger-log-level', 'repair=debug']
    if extra_cmdline:
        cmdline += extra_cmdline
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(
        manager, nr_keys=0, cmdline=cmdline, tablets=tablets,
        disable_flush_cache_time=True)
    # Lower propagation_delay to 0 so gc_before = repair_time, making tombstones
    # GC-eligible immediately after a successful repair rather than 1h later.
    await cql.run_async(
        f"ALTER TABLE {ks}.test WITH tombstone_gc = {{'mode': 'repair', 'propagation_delay_in_seconds': '0'}}"
    )
    logs = [await manager.server_open_log(s.server_id) for s in servers]
    return servers, cql, hosts, ks, table_id, logs


async def _trigger_repaired_compaction(manager, server, ks):
    """Force a compaction that operates on the repaired sstable set."""
    await manager.api.keyspace_compaction(server.ip_addr, ks, "test")


async def _assert_key_visible(cql, ks, key, hosts, *, msg=""):
    """Assert that key is readable on every replica (by querying each host directly)."""
    for h in hosts:
        rows = await cql.run_async(
            f"SELECT pk FROM {ks}.test WHERE pk = {key}",
            host=h)
        assert rows, f"Key {key} not found on host {h} after tombstone GC compaction. {msg}"


async def _assert_key_deleted(cql, ks, key, hosts, *, msg=""):
    """Assert that key is not visible (tombstone wins) on every replica."""
    for h in hosts:
        rows = await cql.run_async(
            f"SELECT pk FROM {ks}.test WHERE pk = {key}",
            host=h)
        assert not rows, f"Key {key} unexpectedly visible on host {h} — data resurrection! {msg}"


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tombstone_gc_no_resurrection_basic_ordering(manager: ManagerClient):
    """Verify that the ordering guarantee prevents premature tombstone GC.

    With propagation_delay=0, gc_before = repair_time.  T.deletion_time (wall-clock
    time of DELETE, just before repair starts) < repair_time = gc_before, so T IS
    GC-eligible after repair.  D (CQL timestamp=1) is in the repaired set because it
    was captured in the repairing snapshot and promoted before repair_time was committed.
    Compaction on the repaired set must NOT purge T because D (min_live_timestamp=1)
    makes T non-purgeable (T.timestamp=2 > max_purgeable=1).

    Scenario:
      - D written at ts=1 to all replicas, flushed.
      - T (row deletion, ts=2) written to all replicas, flushed.
      - Repair runs: both D and T move repairing → repaired.  repair_time advances.
      - gc_before = repair_time > T.deletion_time  →  T is GC-eligible.
      - Repaired compaction must NOT purge T because D (in repaired) prevents it.
      - Key must remain deleted.
    """
    servers, cql, hosts, ks, table_id, logs = await _setup_tombstone_gc_cluster(manager)

    # D at ts=1, T at ts=2 (T wins over D, correctly deletes the row).
    key = 42
    await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({key}, 1) USING TIMESTAMP 1")
    for s in servers:
        await manager.api.flush_keyspace(s.ip_addr, ks)

    await cql.run_async(f"DELETE FROM {ks}.test USING TIMESTAMP 2 WHERE pk = {key}")
    for s in servers:
        await manager.api.flush_keyspace(s.ip_addr, ks)

    # Both D and T captured in repairing, then promoted to repaired.  repair_time advances.
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", "all", incremental_mode='incremental')

    # With propagation_delay=0: gc_before = repair_time > T.deletion_time → T is GC-eligible.
    # Compaction on the repaired set: D is also in repaired (promoted before repair_time
    # was committed), so max_purgeable = D.timestamp = 1 < T.timestamp = 2 → T not purgeable.
    for s in servers:
        await _trigger_repaired_compaction(manager, s, ks)

    # T must not have been GC'd; key must remain deleted (not resurrected).
    await _assert_key_deleted(cql, ks, key, hosts,
                              msg="T was prematurely GC'd on repaired compaction (D in repaired should prevent it)")

    logger.info("test_tombstone_gc_no_resurrection_basic_ordering: PASSED")


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tombstone_gc_no_resurrection_hints_flush_failure(manager: ManagerClient):
    """Verify that repair_time stays at epoch when hints flush fails, so tombstones
    are never GC-eligible after such a repair and data resurrection cannot occur.

    With propagation_delay=0, gc_before = repair_time.  When hints flush fails,
    repair_time is set to epoch (gc_clock::time_point{}) by the repair framework
    (flush_time stays at epoch because hints_batchlog_flushed=False).  Therefore
    gc_before = epoch, T.deletion_time ≈ now >> epoch, T is never GC-eligible, and
    compaction cannot purge T regardless of what is in the repaired set.

    Scenario:
      - D and T written to all replicas and flushed.
      - Repair runs with injection that makes hints flush fail on servers[2].
      - repair_time must stay at epoch (guard: hints_batchlog_flushed=False).
      - Compaction on the repaired set: T not GC-eligible → key stays deleted.
    """
    servers, cql, hosts, ks, table_id, logs = await _setup_tombstone_gc_cluster(manager)

    key = 99

    # Write D at ts=1 and T at ts=2 to all nodes so everyone has the tombstone.
    await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({key}, 1) USING TIMESTAMP 1")
    await cql.run_async(f"DELETE FROM {ks}.test USING TIMESTAMP 2 WHERE pk = {key}")
    for s in servers:
        await manager.api.flush_keyspace(s.ip_addr, ks)

    # Read the initial repair_time so we can verify it doesn't meaningfully advance.
    initial_repair_times = await load_tablet_repair_time(cql, hosts, table_id)

    # Inject: make batchlog manager appear uninitialized on servers[2] so hints flush fails.
    await manager.api.enable_injection(servers[2].ip_addr, "repair_flush_hints_batchlog_handler_bm_uninitialized", one_shot=False)

    try:
        # Repair warns about hints flush failure but continues (repair.cc outer catch).
        # flush_time stays at epoch so repair_time will be set to epoch in system.tablets.
        # Use a short timeout: the call may never return because the topology coordinator
        # keeps re-scheduling repairs when repair_time stays at epoch.
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", "all", incremental_mode='incremental', timeout=30)
    except Exception:
        pass  # repair may report failure or time out; we care about side-effects only
    finally:
        await manager.api.disable_injection(servers[2].ip_addr, "repair_flush_hints_batchlog_handler_bm_uninitialized")

    # repair_time must NOT have advanced to a meaningful time (it stays at epoch when
    # hints flush fails, so gc_before = epoch and T is never GC-eligible).
    new_repair_times = await load_tablet_repair_time(cql, hosts, table_id)
    for token, old_time in initial_repair_times.items():
        new_time = new_repair_times.get(token)
        assert new_time == old_time or new_time is None, \
            f"repair_time advanced for token {token} despite hints flush failure: " \
            f"{old_time} → {new_time}"

    # Trigger compaction on the repaired set; T is not GC-eligible (gc_before = epoch).
    for s in servers:
        await _trigger_repaired_compaction(manager, s, ks)

    # Key must remain deleted (T not GC'd).
    await _assert_key_deleted(cql, ks, key, hosts,
                              msg="T was prematurely GC'd despite hints flush failure (repair_time should be epoch)")

    logger.info("test_tombstone_gc_no_resurrection_hints_flush_failure: PASSED")


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tombstone_gc_no_resurrection_propagation_delay(manager: ManagerClient):
    """Verify the ordering guarantee when D arrives via hint flush just before repair.

    D has an old CQL timestamp (ts_d = now - 2h) simulating a write that was delayed
    by the propagation window.  T has a higher CQL timestamp (ts_t = now - 90min) so T
    correctly shadows D.  With propagation_delay=0, T becomes GC-eligible as soon as
    repair_time is committed (T.deletion_time < repair_time = gc_before).

    The invariant being tested: mark_sstable_as_repaired() runs on all replicas BEFORE
    the coordinator commits repair_time to Raft.  So when gc_before advances (repair_time
    committed), D is already in the repaired set on all replicas.  Repaired compaction
    must NOT purge T because D (min_live_timestamp = ts_d < ts_t) is present there.

    Scenario:
      - servers[2] stopped; D and T written (go to hints on coordinator for servers[2]).
      - D and T flushed on servers[0] and servers[1].
      - servers[2] restarted.
      - Repair: hints flush delivers D and T to servers[2] before the repairing snapshot.
        mark_sstable_as_repaired() promotes D and T to repaired on servers[2].
        repair_time then committed.  gc_before = repair_time > T.deletion_time.
      - Repaired compaction: D (ts_d) in repaired prevents T (ts_t > ts_d) from being GC'd.
      - Key must remain deleted.
    """
    servers, cql, hosts, ks, table_id, logs = await _setup_tombstone_gc_cluster(
        manager, extra_cmdline=['--hinted-handoff-enabled', '1'])

    # Stop servers[2] so writes to it are queued as hints on the coordinator.
    await manager.server_stop_gracefully(servers[2].server_id)

    key = 77
    # CQL USING TIMESTAMP is in microseconds since epoch.
    now_us = int(time.time() * 1e6)
    ts_d = now_us - int(2 * 3600 * 1e6)    # 2 hours ago (CQL µs timestamp)
    ts_t = now_us - int(90 * 60 * 1e6)      # 90 minutes ago (CQL µs timestamp); ts_t > ts_d

    # D and T go to servers[0] and servers[1] directly; servers[2] gets hints queued.
    await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({key}, 1) USING TIMESTAMP {ts_d}")
    await cql.run_async(f"DELETE FROM {ks}.test USING TIMESTAMP {ts_t} WHERE pk = {key}")

    for s in [servers[0], servers[1]]:
        await manager.api.flush_keyspace(s.ip_addr, ks)

    # Restart servers[2]; D and T are not on its disk yet.
    await manager.server_start(servers[2].server_id)
    await manager.servers_see_each_other(servers)
    await reconnect_driver(manager)
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    # Repair: hints flush sends D and T to servers[2] before the repairing snapshot.
    # After row sync, mark_sstable_as_repaired() promotes D and T to repaired on servers[2].
    # Only then does the coordinator commit repair_time to Raft.
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", "all", incremental_mode='incremental')

    # With propagation_delay=0: gc_before = repair_time > T.deletion_time → T is GC-eligible.
    # But D (ts_d < ts_t) is already in repaired on servers[2], so max_purgeable = ts_d
    # < ts_t = T.timestamp → T is NOT purgeable.
    for s in servers:
        await _trigger_repaired_compaction(manager, s, ks)

    # Key must remain deleted (T not GC'd, D not resurrected).
    await _assert_key_deleted(cql, ks, key, hosts,
                              msg="Data resurrection: T was GC'd despite D being present in repaired set")

    logger.info("test_tombstone_gc_no_resurrection_propagation_delay: PASSED")


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tombstone_gc_mv_optimization_safe_via_hints(manager: ManagerClient):
    """Verify the repaired-only tombstone GC optimization is safe for non-co-located MVs
    when view hints deliver the shadowing row before the MV repair snapshot.

    The optimization (sstable_set_for_tombstone_gc returns only repaired sstables) is
    extended to materialized view tables.  For this to be safe, any D_view that could
    shadow a GC-eligible T_mv in the repaired set must itself be in the repaired set by
    the time repaired compaction runs.

    The repair invariant that makes this safe: flush_hints() is called before
    take_storage_snapshot() during tablet repair.  flush_hints() creates a sync point
    covering BOTH _hints_manager (base mutations) AND _hints_for_views_manager (view
    mutations).  It waits until ALL pending hints — including D_view entries stored in
    hints_for_views_manager while the target node was down — have been replayed to the
    target node before the repairing snapshot is taken.  D_view therefore lands in the
    MV's repairing sstable and is promoted to repaired.  When a repaired compaction then
    checks for shadows it finds D_view in the repaired set, keeping T_mv non-purgeable.

    Scenario:
      1. 3-node cluster, base table + MV with tombstone_gc=repair, propagation_delay=0,
         SYNCHRONOUS_UPDATES=TRUE, 1 tablet (ensures single tablet for predictable placement).
      2. Stop servers[2]; D_base(ts_d) and T_base(ts_t) written to servers[0] and servers[1].
         View update dispatch: D_view(ts_d) and T_mv(ts_t) hints queued in
         hints_for_views_manager for servers[2].
      3. Flush servers[0] and servers[1] so the data is in sstables.
      4. Start servers[2] (view hints still pending).
      5. Run MV tablet repair: flush_hints() replays D_view + T_mv to servers[2] before
         take_storage_snapshot(); both are captured in the repairing sstable and promoted
         to repaired.  repair_time committed; gc_before = repair_time > T_mv.deletion_time.
      6. Trigger repaired compaction on servers[2]'s MV.  With the optimization applied:
         only repaired sstables are scanned for shadows; D_view (ts_d) is found there →
         max_purgeable < ts_t → T_mv is NOT purged.
      7. Assert: the MV row is NOT visible on servers[2] — T_mv preserved, no resurrection.
    """
    servers, cql, hosts, ks, table_id, logs = await _setup_tombstone_gc_cluster(
        manager, tablets=1, extra_cmdline=['--hinted-handoff-enabled', '1'])

    mv_name = "mv_test"
    await cql.run_async(
        f"CREATE MATERIALIZED VIEW {ks}.{mv_name} AS "
        f"SELECT * FROM {ks}.test WHERE pk IS NOT NULL AND c IS NOT NULL "
        f"PRIMARY KEY (c, pk) "
        f"WITH SYNCHRONOUS_UPDATES = TRUE "
        f"AND tombstone_gc = {{'mode': 'repair', 'propagation_delay_in_seconds': '0'}}"
    )

    # Stop servers[2] so it misses the base writes; view hints will be queued for it.
    await manager.server_stop_gracefully(servers[2].server_id)

    key = 99
    c_val = 7
    now_us = int(time.time() * 1e6)
    ts_d = now_us - int(2 * 3600 * 1e6)   # 2 hours ago
    ts_t = now_us - int(1 * 3600 * 1e6)   # 1 hour ago (ts_t > ts_d so T_mv shadows D_view)

    # D_base and T_base go to servers[0] and servers[1] only.  servers[2] is down:
    # S0/S1 dispatch D_view and T_mv to MV replicas; for servers[2] these are stored as
    # view hints in hints_for_views_manager.
    await cql.run_async(
        f"INSERT INTO {ks}.test (pk, c) VALUES ({key}, {c_val}) USING TIMESTAMP {ts_d}"
    )
    await cql.run_async(
        f"DELETE FROM {ks}.test USING TIMESTAMP {ts_t} WHERE pk = {key}"
    )

    for s in [servers[0], servers[1]]:
        await manager.api.flush_keyspace(s.ip_addr, ks)

    # Restart servers[2]; view hints (D_view + T_mv) are still pending.
    await manager.server_start(servers[2].server_id)
    await manager.servers_see_each_other(servers)
    await reconnect_driver(manager)
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    # MV tablet repair:
    #   flush_hints() → hints_for_views_manager replays D_view + T_mv to servers[2] before snapshot
    #   take_storage_snapshot() flushes MV memtable → D_view + T_mv in repairing sstable
    #   mark_sstable_as_repaired() → D_view + T_mv promoted to repaired
    #   repair_time committed → gc_before = repair_time > T_mv.deletion_time → T_mv GC-eligible
    await manager.api.tablet_repair(servers[0].ip_addr, ks, mv_name, "all", incremental_mode='incremental')

    # Repaired compaction on servers[2] MV with the optimization applied to MV:
    # repaired set scanned → D_view(ts_d) found → max_purgeable = ts_d < ts_t → T_mv non-purgeable.
    await manager.api.keyspace_compaction(servers[2].ip_addr, ks, mv_name)

    # Query servers[2] directly (CL=ONE).  If T_mv was incorrectly GC'd, D_view would
    # resurface and the row would be visible — that would be data resurrection.
    stmt = SimpleStatement(
        f"SELECT pk FROM {ks}.{mv_name} WHERE c = {c_val} AND pk = {key}",
        consistency_level=ConsistencyLevel.ONE,
    )
    rows = await cql.run_async(stmt, host=hosts[2])
    assert not rows, (
        f"Data resurrection on servers[2] MV: (c={c_val}, pk={key}) visible after repaired "
        f"compaction — T_mv was incorrectly GC'd, meaning D_view was NOT in the repaired set "
        f"despite the hints-before-snapshot invariant. Optimization is broken for MV."
    )

    logger.info("test_tombstone_gc_mv_optimization_safe_via_hints: PASSED")


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
async def test_tombstone_gc_mv_safe_staging_processor_delay(manager: ManagerClient):
    """Verify no resurrection when the view-update-generator staging processor is delayed
    past the point where T_mv has been GC'd from the repaired set.

    This test exercises the "staging delay" edge case in the tombstone GC optimization safety
    analysis for materialized views.  The optimization (sstable_set_for_tombstone_gc returns
    only repaired sstables) could in principle allow T_mv to be purged from the repaired set
    BEFORE the staging processor has dispatched D_view to MV-on-servers[0].  The test verifies
    that staging correctly generates a no-op in this situation — no D_view is dispatched — so
    no resurrection occurs.

    The safety mechanism that prevents resurrection here is the read-before-write performed by
    stream_view_replica_updates (via as_mutation_source_excluding_staging()):
      When the staging processor fires, it reads the CURRENT base-table state including T_base
      (which was written to servers[0] AFTER base repair returned).  The view_update_builder
      sees T_base as the existing partition tombstone, marks D_base as dead (ts_d < ts_t), and
      generates NO view update.  D_view is therefore never dispatched to MV-on-servers[0].
    Because D_view never reaches MV-on-servers[0], no resurrection can occur even if T_mv was
    already GC'd from the repaired set when staging fires.

    Scenario:
      1. 3-node cluster, base table + MV, tombstone_gc=repair, propagation_delay=0,
         SYNCHRONOUS_UPDATES=FALSE, hinted-handoff DISABLED (so no view hints are stored
         for the offline node).
      2. Stop servers[0].  D_base(ts_d) written to servers[1] only.
         servers[1] dispatches D_view to MV replicas on servers[1] and servers[2].
         MV-on-servers[0] receives nothing.
      3. Flush servers[1] and servers[2].  Restart servers[0].
      4. Enable view_update_generator_pause_before_processing injection on servers[0].
      5. Run BASE table repair.  Row-sync detects servers[0] missing D_base and writes
         it via the staging path (repair_writer → staging SSTable → view_update_generator).
         The view_update_generator on servers[0] blocks BEFORE dispatching any view update.
         Base repair returns without waiting for the staging processor.
      6. T_base(ts_t > ts_d) written to all three nodes NOW (after base repair).
         servers[0] dispatches T_mv directly to MV-on-servers[0]; all MV replicas get T_mv.
      7. Flush all servers.
      8. Run MV table repair.
         flush_hints(): no view hints (hinted-handoff disabled) → no-op.
         take_storage_snapshot(): MV-on-servers[0] has T_mv only; MV-on-servers[1/2] have
         D_view + T_mv; the maybe_compact_for_streaming reader drops D_view (shadowed by T_mv)
         → hashes equal → no row-sync needed.
         repair_time committed; gc_before = repair_time > T_mv.deletion_time → T_mv GC-eligible.
      9. Trigger repaired compaction on servers[0] MV.
         Optimization: only repaired sstables scanned → no D_view → T_mv is GC-eligible → T_mv PURGED.
     10. Assert: key NOT visible on servers[0] MV (T_mv purged, staging still blocked).
     11. Release staging injection.  process_staging_sstables calls
         stream_view_replica_updates with as_mutation_source_excluding_staging() to read the
         base table.  T_base(ts_t) is present → D_base row marker (ts_d) expired by T_base →
         view update is a no-op → D_view NOT dispatched.
     12. Wait for "Processed ks.test" log line confirming staging completed.
     13. Flush + compact MV on servers[0].  No D_view was written → nothing resurrects.
     14. Assert: key still NOT visible on servers[0] MV.

    The test demonstrates that the tombstone GC optimization is safe even when the staging
    processor fires after T_mv has already been purged.
    """
    servers, cql, hosts, ks, table_id, logs = await _setup_tombstone_gc_cluster(
        manager, tablets=1,
        extra_cmdline=['--hinted-handoff-enabled', '0'])

    mv_name = "mv_test"
    await cql.run_async(
        f"CREATE MATERIALIZED VIEW {ks}.{mv_name} AS "
        f"SELECT * FROM {ks}.test WHERE pk IS NOT NULL AND c IS NOT NULL "
        f"PRIMARY KEY (c, pk) "
        f"WITH SYNCHRONOUS_UPDATES = FALSE "
        f"AND tombstone_gc = {{'mode': 'repair', 'propagation_delay_in_seconds': '0'}}"
    )

    # Stop servers[0]; it will miss D_base and D_view (no hints, hinted-handoff disabled).
    await manager.server_stop_gracefully(servers[0].server_id)

    key = 77
    c_val = 5
    now_us = int(time.time() * 1e6)
    ts_d = now_us - int(2 * 3600 * 1e6)   # 2 hours ago
    ts_t = now_us - int(1 * 3600 * 1e6)   # 1 hour ago (ts_t > ts_d so T_mv shadows D_view)

    # D_base written to servers[1] only (servers[0] is down, servers[2] will also
    # receive a replica copy because tablet RF=3).  servers[1] dispatches D_view to
    # MV replicas.  MV-on-servers[0] gets nothing.
    await cql.run_async(
        f"INSERT INTO {ks}.test (pk, c) VALUES ({key}, {c_val}) USING TIMESTAMP {ts_d}",
        host=hosts[1])

    for s in [servers[1], servers[2]]:
        await manager.api.flush_keyspace(s.ip_addr, ks)

    # Restart servers[0]; still no D_base on base or D_view on MV for servers[0].
    await manager.server_start(servers[0].server_id)
    await manager.servers_see_each_other(servers)
    await reconnect_driver(manager)
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    # Block the staging processor on servers[0] BEFORE running base repair, so that
    # the staged D_base (written by row-sync during base repair) never gets dispatched
    # to MV-on-servers[0] until we explicitly release the injection.
    await manager.api.enable_injection(
        servers[0].ip_addr, "view_update_generator_pause_before_processing", one_shot=True)

    # Base repair: row-sync detects servers[0] missing D_base → writes D_base to servers[0]
    # via the staging path → staging sstable registered → view_update_generator BLOCKED.
    # Base repair returns without waiting for staging to complete.
    # NOTE: T_base has NOT been written yet, so D_base is not shadowed during repair
    # hash computation; the hashes are unequal and row-sync fires as expected.
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", "all",
                                    incremental_mode='incremental')

    # T_base(ts_t) written to all nodes NOW — after base repair so staging is already blocked.
    # servers[0] dispatches T_mv to MV-on-servers[0]; all MV replicas now have T_mv.
    # D_view remains absent from MV-on-servers[0] (staging still blocked).
    await cql.run_async(
        f"DELETE FROM {ks}.test USING TIMESTAMP {ts_t} WHERE pk = {key}")

    for s in servers:
        await manager.api.flush_keyspace(s.ip_addr, ks)

    # MV repair:
    #   flush_hints() → no view hints (hinted-handoff disabled) → no-op.
    #   take_storage_snapshot(): servers[0] MV has T_mv only; servers[1,2] MV have
    #   D_view(ts_d) + T_mv(ts_t); the streaming compacting reader drops D_view (shadowed
    #   by T_mv) → hashes equal → no row-sync → repair_time committed.
    #   T_mv.deletion_time ≈ write time (step above); repair_time > deletion_time → GC-eligible.
    await manager.api.tablet_repair(servers[0].ip_addr, ks, mv_name, "all",
                                    incremental_mode='incremental')

    # Repaired compaction on servers[0] MV with staging still blocked:
    #   sstable_set_for_tombstone_gc = only repaired sstables = {T_mv sstable}.
    #   No D_view in repaired → T_mv is GC-eligible → T_mv PURGED.
    await manager.api.keyspace_compaction(servers[0].ip_addr, ks, mv_name)

    # T_mv has been purged; staging still blocked.  No resurrection yet.
    stmt = SimpleStatement(
        f"SELECT pk FROM {ks}.{mv_name} WHERE c = {c_val} AND pk = {key}",
        consistency_level=ConsistencyLevel.ONE)
    rows = await cql.run_async(stmt, host=hosts[0])
    assert not rows, (
        f"Unexpected resurrection on servers[0] MV after first compaction "
        f"(c={c_val}, pk={key}) visible — T_mv was not GC'd or D_view was incorrectly present.")

    # Release the staging processor.
    # stream_view_replica_updates is called with as_mutation_source_excluding_staging() to
    # read the current base table state.  T_base(ts_t) is present on servers[0] base table
    # (written in step 6 above).  The view_update_builder sets _existing_partition_tombstone
    # = T_base, applies it onto the D_base row (ts_d < ts_t → row marker expired), and
    # generates a no-op view update.  D_view is NOT dispatched to MV-on-servers[0].
    await manager.api.message_injection(
        servers[0].ip_addr, "view_update_generator_pause_before_processing")

    # Wait for staging processing to complete: view_update_generator logs
    # "Processed ks.base_cf: N sstables" after generate_updates_from_staging_sstables returns.
    # The staging sstable is registered for the BASE TABLE (test), not the MV.
    await logs[0].wait_for(f"Processed {ks}.test")

    # Flush and compact again.  No D_view was dispatched by staging → nothing new in MV.
    for s in servers:
        await manager.api.flush_keyspace(s.ip_addr, ks)
    await manager.api.keyspace_compaction(servers[0].ip_addr, ks, mv_name)

    rows = await cql.run_async(stmt, host=hosts[0])
    assert not rows, (
        f"Resurrection after staging on servers[0] MV: (c={c_val}, pk={key}) visible. "
        f"The staging processor dispatched D_view despite T_base being present in the "
        f"base table — the read-before-write safety mechanism failed.")

    logger.info("test_tombstone_gc_mv_safe_staging_processor_delay: PASSED")
