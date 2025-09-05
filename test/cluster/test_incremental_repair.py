#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.manager_client import ManagerClient
from test.cluster.conftest import skip_mode
from test.pylib.repair import load_tablet_sstables_repaired_at, create_table_insert_data_for_repair
from test.pylib.tablets import get_all_tablet_replicas
from test.cluster.tasks.task_manager_client import TaskManagerClient

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

async def get_sst_status(run, log):
    sst_add = await log.grep(rf'.*Added sst.*for incremental repair')
    sst_skip = await log.grep(rf'.*Skipped adding sst.*for incremental repair')
    sst_mark = await log.grep(rf'.*Marking.*for incremental repair')
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

def local_process_id(cql):
    ip = socket.gethostbyname(cql.cluster.contact_points[0])
    port = cql.cluster.port
    ip2hex = lambda ip: ''.join([f'{int(x):02X}' for x in reversed(ip.split('.'))])
    port2hex = lambda port: f'{int(port):04X}'
    addr1 = ip2hex(ip) + ':' + port2hex(port)
    addr2 = ip2hex('0.0.0.0') + ':' + port2hex(port)
    LISTEN = '0A'
    with open('/proc/net/tcp', 'r') as f:
        for line in f:
            cols = line.split()
            if cols[3] == LISTEN and (cols[1] == addr1 or cols[1] == addr2):
                inode = cols[9]
                break
        else:
            # Didn't find a process listening on the given address
            return None
    target = f'socket:[{inode}]'
    for proc in os.listdir('/proc'):
        if not proc.isnumeric():
            continue
        dir = f'/proc/{proc}/fd/'
        try:
            for fd in os.listdir(dir):
                if os.readlink(dir + fd) == target:
                    # Found the process!
                    return proc
        except:
            # Ignore errors. We can't check processes we don't own.
            pass
    return None

def get_scylla_path(cql):
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        pid = local_process_id(cql)
        if not pid:
            logger.warning(f"Attempt {attempt}/{max_retries}: Could not get local process ID for CQL. Retrying...")
            time.sleep(1)
            continue

        path = None
        try:
            path = os.readlink(f'/proc/{pid}/exe')
            subprocess.check_output([path, '--list-tools'], stderr=subprocess.PIPE)
            return path
        except:
            logger.warning(f"Attempt {attempt}/{max_retries}: Failed to determine or verify Scylla path. Retrying...")
            time.sleep(1)
            continue

    assert False, f"Failed to find and verify Scylla executable path after {max_retries} attempts."

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

async def preapre_cluster_for_incremental_repair(manager, nr_keys = 100 , cmdline = []):
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, nr_keys=nr_keys, cmdline=cmdline)
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
    servers, cql, hosts, ks, table_id, logs, _, _, _, token = await preapre_cluster_for_incremental_repair(manager)

    await insert_keys(cql, ks, 0, 100)

    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)
    skipped_bytes = get_incremental_repair_sst_skipped_bytes(servers[0])
    read_bytes = get_incremental_repair_sst_read_bytes(servers[0])
    # Nothing to skip. Repair all data.
    assert skipped_bytes == 0
    assert read_bytes > 0

    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)
    skipped_bytes2 = get_incremental_repair_sst_skipped_bytes(servers[0])
    read_bytes2 = get_incremental_repair_sst_read_bytes(servers[0])
    # Skip all. Nothing to repair
    assert skipped_bytes2 > 0
    assert read_bytes2 == read_bytes

    await insert_keys(cql, ks, 200, 300)

    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)
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
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)
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
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)
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
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_incremental_repair_error(manager: ManagerClient):
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager)
    token = -1
    map0 = await load_tablet_sstables_repaired_at(manager, cql, servers[0], hosts[0], table_id)

    # Repair should not finish with error
    await inject_error_on(manager, "repair_tablet_fail_on_rpc_call", servers)
    try:
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, timeout=10)
        assert False # Check the tablet repair is not supposed to finish
    except TimeoutError:
        logger.info("Repair timeout as expected")
    await inject_error_off(manager, "repair_tablet_fail_on_rpc_call", servers)

    map1 = await load_tablet_sstables_repaired_at(manager, cql, servers[0], hosts[0], table_id)

    # Check the sstables_repaired_at is not increased
    assert get_sstables_repaired_at(map0, token) == get_sstables_repaired_at(map1, token)

async def do_tablet_incremental_repair_and_ops(manager: ManagerClient, ops: str):
    nr_keys = 100
    servers, cql, hosts, ks, table_id, logs, repaired_keys, unrepaired_keys, current_key, token = await preapre_cluster_for_incremental_repair(manager, nr_keys)
    token = -1

    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)
    # 1 add 0 skip 1 mark
    for log in logs:
        sst_add, sst_skip, sst_mark = await get_sst_status("First", log)
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

    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)

    # 1 add 1 skip 1 mark
    for log in logs:
        sst_add, sst_skip, sst_mark = await get_sst_status("Second", log)
        assert len(sst_add) == 2
        assert len(sst_mark) == 2
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
    servers, cql, hosts, ks, table_id, logs, repaired_keys, unrepaired_keys, current_key, token = await preapre_cluster_for_incremental_repair(manager, nr_keys)

    # Disable autocompaction
    for server in servers:
        await manager.api.disable_autocompaction(server.ip_addr, ks, 'test')

    # First repair
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)

    # Insert more keys
    await insert_keys(cql, ks, current_key, current_key + nr_keys)
    repaired_keys = repaired_keys | set(range(current_key, current_key + nr_keys))
    current_key += nr_keys

    # Second repair
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)

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
    scylla_path = get_scylla_path(cql)

    for server in servers:
        await manager.server_stop_gracefully(server.server_id)

    await verify_repaired_and_unrepaired_keys(manager, scylla_path, servers, ks, repaired_keys, unrepaired_keys)

async def do_test_tablet_incremental_repair_with_split_and_merge(manager, do_split, do_merge):
    nr_keys = 100
    servers, cql, hosts, ks, table_id, logs, repaired_keys, unrepaired_keys, current_key, token = await preapre_cluster_for_incremental_repair(manager, nr_keys)

    # First repair
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token) # sstables_repaired_at 1
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token) # sstables_repaired_at 2

    # Insert more keys
    await insert_keys(cql, ks, current_key, current_key + nr_keys)
    repaired_keys = repaired_keys | set(range(current_key, current_key + nr_keys))
    current_key += nr_keys

    # Second repair
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token) # sstables_repaired_at 3

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

    scylla_path = get_scylla_path(cql)

    await asyncio.sleep(random.randint(1, 5))

    for server in servers:
        await manager.server_stop_gracefully(server.server_id)

    await verify_repaired_and_unrepaired_keys(manager, scylla_path, servers, ks, repaired_keys, unrepaired_keys)

@pytest.mark.skip(reason="https://github.com/scylladb/scylladb/issues/24153")
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_incremental_repair_with_split_and_merge(manager: ManagerClient):
    await do_test_tablet_incremental_repair_with_split_and_merge(manager, do_split=True, do_merge=True)

@pytest.mark.skip(reason="https://github.com/scylladb/scylladb/issues/24153")
@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_incremental_repair_with_split(manager: ManagerClient):
    await do_test_tablet_incremental_repair_with_split_and_merge(manager, do_split=True, do_merge=False)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_incremental_repair_with_merge(manager: ManagerClient):
    await do_test_tablet_incremental_repair_with_split_and_merge(manager, do_split=False, do_merge=True)

@pytest.mark.asyncio
async def test_tablet_incremental_repair_existing_and_repair_produced_sstable(manager: ManagerClient):
    nr_keys = 100
    cmdline = ["--hinted-handoff-enabled", "0"]
    servers, cql, hosts, ks, table_id, logs, repaired_keys, unrepaired_keys, current_key, token = await preapre_cluster_for_incremental_repair(manager, nr_keys, cmdline)

    await manager.server_stop_gracefully(servers[1].server_id)

    # Insert more keys
    await insert_keys(cql, ks, current_key, current_key + nr_keys)
    repaired_keys = repaired_keys | set(range(current_key, current_key + nr_keys))
    current_key += nr_keys

    await manager.server_start(servers[1].server_id)

    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)

    scylla_path = get_scylla_path(cql)

    for server in servers:
        await manager.server_stop_gracefully(server.server_id)

    await verify_repaired_and_unrepaired_keys(manager, scylla_path, servers, ks, repaired_keys, unrepaired_keys)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_incremental_repair_merge_higher_repaired_at_number(manager):
    nr_keys = 100
    servers, cql, hosts, ks, table_id, logs, repaired_keys, unrepaired_keys, current_key, token = await preapre_cluster_for_incremental_repair(manager, nr_keys)

    # First repair
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token) # sstables_repaired_at 1
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token) # sstables_repaired_at 2

    # Insert more keys
    await insert_keys(cql, ks, current_key, current_key + nr_keys)
    unrepaired_keys = unrepaired_keys | set(range(current_key, current_key + nr_keys))
    current_key += nr_keys

    # Second repair
    await inject_error_on(manager, "repair_tablet_no_update_sstables_repair_at", servers)
    # some sstable repaired_at = 3, but sstables_repaired_at = 2
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token) # sstables_repaired_at 2
    await inject_error_off(manager, "repair_tablet_no_update_sstables_repair_at", servers)

    scylla_path = get_scylla_path(cql)

    s1_mark = await logs[0].mark()
    await trigger_tablet_merge(manager, servers, logs)
    # The merge process will set the unrepaired sstable with repaired_at=3 to repaired_at=0 during merge
    await logs[0].wait_for('Finished repaired_at update for tablet merge .* old=3 new=0 sstables_repaired_at=2', from_mark=s1_mark)

    for server in servers:
        await manager.server_stop_gracefully(server.server_id)

    # Verify max repaired_at of sstables should be 2
    max_repaired_at = 2
    await verify_max_repaired_at(manager, scylla_path, servers, ks, 2)

    for server in servers:
        await manager.server_stop_gracefully(server.server_id)

    await verify_repaired_and_unrepaired_keys(manager, scylla_path, servers, ks, repaired_keys, unrepaired_keys)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_incremental_repair_merge_correct_repaired_at_number_after_merge(manager):
    nr_keys = 100
    servers, cql, hosts, ks, table_id, logs, repaired_keys, unrepaired_keys, current_key, token = await preapre_cluster_for_incremental_repair(manager, nr_keys)

    # First repair
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token) # sstables_repaired_at 1
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token) # sstables_repaired_at 2

    # Insert more keys
    await insert_keys(cql, ks, current_key, current_key + nr_keys)
    unrepaired_keys = unrepaired_keys | set(range(current_key, current_key + nr_keys))
    current_key += nr_keys

    # Second repair
    replicas = await get_all_tablet_replicas(manager, servers[0], ks, 'test')
    last_tokens = [t.last_token for t in replicas]
    for t in last_tokens[0::2]:
        logging.info(f"Start repair for token={t}");
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", t) # sstables_repaired_at 3

    scylla_path = get_scylla_path(cql)

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
    servers, cql, hosts, ks, table_id, logs, repaired_keys, unrepaired_keys, current_key, token = await preapre_cluster_for_incremental_repair(manager, nr_keys, cmdline)

    # First repair
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token) # sstables_repaired_at 1

    # Insert more keys
    await insert_keys(cql, ks, current_key, current_key + nr_keys)
    unrepaired_keys = unrepaired_keys | set(range(current_key, current_key + nr_keys))
    current_key += nr_keys

    for server in servers:
        await manager.api.flush_keyspace(server.ip_addr, ks)

    scylla_path = get_scylla_path(cql)

    # Trigger merge and error in merge
    s1_mark = await logs[0].mark()
    await inject_error_on(manager, error, servers[:1])
    await inject_error_on(manager, "tablet_force_tablet_count_decrease", servers)
    await logs[0].wait_for(f'Got {error}', from_mark=s1_mark)
    await inject_error_off(manager, "tablet_force_tablet_count_decrease", servers)
    await manager.server_stop(servers[0].server_id)
    await manager.server_start(servers[0].server_id)

    for server in servers:
        await manager.server_stop_gracefully(server.server_id)

    await verify_repaired_and_unrepaired_keys(manager, scylla_path, servers, ks, repaired_keys, unrepaired_keys)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_incremental_repair_merge_error_in_merge_finalization(manager):
    await do_test_tablet_incremental_repair_merge_error(manager, 'handle_tablet_resize_finalization_for_merge_error')

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_incremental_repair_merge_error_in_merge_completion_fiber(manager):
    await do_test_tablet_incremental_repair_merge_error(manager, 'merge_completion_fiber_error')

@pytest.mark.asyncio
async def test_tablet_repair_with_incremental_option(manager: ManagerClient):
    servers, cql, hosts, ks, table_id, logs, _, _, _, token = await preapre_cluster_for_incremental_repair(manager)
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
    await do_repair_and_check(None, 1, rf'Starting tablet repair by API .* incremental_mode=regular.*', check1)

    def check2(skip1, read1, skip2, read2):
        assert skip1 == skip2
        assert read1 == read2
    await do_repair_and_check('disabled', 0, rf'Starting tablet repair by API .* incremental_mode=disabled.*', check2)

    def check3(skip1, read1, skip2, read2):
        assert skip1 < skip2
        assert read1 == read2
    await do_repair_and_check('regular', 1, rf'Starting tablet repair by API .* incremental_mode=regular.*', check3)

    def check4(skip1, read1, skip2, read2):
        assert skip1 == skip2
        assert read1 < read2
    await do_repair_and_check('full', 1, rf'Starting tablet repair by API .* incremental_mode=full.*', check4)
