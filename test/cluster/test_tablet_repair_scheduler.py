#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.cqlpy import nodetool
from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts, Host
from test.cluster.conftest import skip_mode
from test.pylib.repair import load_tablet_sstables_repaired_at, load_tablet_repair_time, create_table_insert_data_for_repair, get_tablet_task_id, load_tablet_repair_task_infos
from test.pylib.rest_client import inject_error_one_shot, read_barrier
from test.cluster.util import create_new_test_keyspace

from cassandra.cluster import Session as CassandraSession

from test.cqlpy.conftest import scylla_data_dir
from test.cluster.tasks.task_manager_client import TaskManagerClient

import pytest
import asyncio
import logging
import re
import requests
import time
import datetime
import glob
import os
import subprocess
import json
import socket

logger = logging.getLogger(__name__)


async def inject_error_one_shot_on(manager, error_name, servers):
    errs = [inject_error_one_shot(manager.api, s.ip_addr, error_name) for s in servers]
    await asyncio.gather(*errs)

async def inject_error_on(manager, error_name, servers, params = {}):
    errs = [manager.api.enable_injection(s.ip_addr, error_name, False, params) for s in servers]
    await asyncio.gather(*errs)

async def inject_error_off(manager, error_name, servers):
    errs = [manager.api.disable_injection(s.ip_addr, error_name) for s in servers]
    await asyncio.gather(*errs)

async def guarantee_repair_time_next_second():
    # The repair time granularity is seconds. This ensures the repair time is
    # different than the previous one.
    await asyncio.sleep(1)

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
    pid = local_process_id(cql)
    if not pid:
        pytest.skip("Can't find local Scylla process")
    # Now that we know the process id, use /proc to find the executable.
    try:
        path = os.readlink(f'/proc/{pid}/exe')
    except:
        pytest.skip("Can't find local Scylla executable")
    # Confirm that this executable is a real tool-providing Scylla by trying
    # to run it with the "--list-tools" option
    try:
        subprocess.check_output([path, '--list-tools'])
    except:
        pytest.skip("Local server isn't Scylla")
    return path

async def verify_repaired_and_unrepaired_keys(manager, scylla_path, servers, ks, repaired_keys, unrepaired_keys):
    # Verify repaired and unrepaired keys
    for server in servers:
        node_workdir = await manager.server_get_workdir(server.server_id)
        sstables = get_sstables(node_workdir, ks, 'test')
        logger.info(f"Got {node_workdir=} {sstables=}")
        for sst in sstables:
            keys = get_keys_from_sst(sst, scylla_path)
            repaired_at = get_repaired_at_from_sst(sst, scylla_path)
            logger.info(f"Got {sst=} {repaired_at=} {keys=} {repaired_keys=} {unrepaired_keys=}")
            if repaired_at == 0 or repaired_at == None:
                for key in keys:
                    assert key in unrepaired_keys
            elif repaired_at > 0:
                for key in keys:
                    assert key in repaired_keys

@pytest.mark.asyncio
async def test_tablet_manual_repair(manager: ManagerClient):
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, fast_stats_refresh=False, disable_flush_cache_time=True)
    token = -1

    start = time.time()
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)
    duration = time.time() - start
    map1 = await load_tablet_repair_time(cql, hosts[0:1], table_id)
    logging.info(f'map1={map1} duration={duration}')

    await guarantee_repair_time_next_second()

    start = time.time()
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)
    duration = time.time() - start
    map2 = await load_tablet_repair_time(cql, hosts[0:1], table_id)
    logging.info(f'map2={map2} duration={duration}')

    t1 = map1[str(token)]
    t2 = map2[str(token)]
    logging.info(f't1={t1} t2={t2}')

    assert t2 > t1

@pytest.mark.asyncio
async def test_tombstone_gc_insert_flush(manager: ManagerClient):
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, fast_stats_refresh=False, disable_flush_cache_time=True)
    token = "all"
    logs = []
    for s in servers:
        await manager.api.set_logger_level(s.ip_addr, "database", "debug")
        await manager.api.set_logger_level(s.ip_addr, "tablets", "debug")
        logs.append(await manager.server_open_log(s.server_id))

    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)

    timeout = 600
    deadline = time.time() + timeout
    while True:
        done = True
        for s in servers:
                await read_barrier(manager.api, s.ip_addr)
        for log in logs:
            inserts = await log.grep(rf'.*Insert pending repair time for tombstone gc: table={table_id}.*')
            flushes = await log.grep(rf'.*Flush pending repair time for tombstone gc: table={table_id}.*')
            logging.info(f'{inserts=} {flushes=}');
            logging.info(f'{len(inserts)=} {len(flushes)=}');
            ok = len(inserts) == len(flushes) and len(inserts) > 0
            if not ok:
                done = False
        if done:
            break
        else:
            assert time.time() < deadline

@pytest.mark.asyncio
async def test_tablet_manual_repair_all_tokens(manager: ManagerClient):
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, fast_stats_refresh=False, disable_flush_cache_time=True)
    token = "all"
    now = datetime.datetime.utcnow()
    map1 = await load_tablet_repair_time(cql, hosts[0:1], table_id)

    await guarantee_repair_time_next_second()

    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)
    map2 = await load_tablet_repair_time(cql, hosts[0:1], table_id)
    logging.info(f'{map1=} {map2=}')
    assert len(map1) == len(map2)
    for k, v in map1.items():
        assert v == None
    for k, v in map2.items():
        assert v != None
        assert v > now

@pytest.mark.asyncio
async def test_tablet_manual_repair_async(manager: ManagerClient):
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, fast_stats_refresh=False)
    token = "-1"
    log = await manager.server_open_log(servers[0].server_id)
    res = await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, await_completion=False)
    tablet_task_id = res['tablet_task_id']
    logging.info(f"{tablet_task_id=}")
    res = await log.grep(rf'.*Issued tablet repair by API request table_id={table_id}.*tablet_task_id={tablet_task_id}.*')
    logging.info(f"{res=}")
    assert len(res) == 1

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_manual_repair_reject_parallel_requests(manager: ManagerClient):
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, fast_stats_refresh=False)
    token = -1

    await inject_error_on(manager, "tablet_repair_add_delay_in_ms", servers, params={'value':'3000'})

    class asyncState:
        error = 0
        ok = 0

    state = asyncState()

    async def run_repair(state):
        try:
            await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)
            state.ok = state.ok + 1
        except Exception as e:
            logging.info(f"Got exception as expected: {e}")
            state.error = state.error + 1

    await asyncio.gather(*[run_repair(state) for i in range(3)])

    # A new tablet repair request can only be issued after the first one is
    # finished.
    assert state.ok == 1
    assert state.error == 2

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_repair_error_and_retry(manager: ManagerClient):
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager)

    # Repair should finish with one time error injection
    token = -1
    await inject_error_one_shot_on(manager, "repair_tablet_fail_on_rpc_call", servers)
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)
    await inject_error_off(manager, "repair_tablet_fail_on_rpc_call", servers)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_repair_error_not_finish(manager: ManagerClient):
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager)

    token = -1
    # Repair should not finish with error
    await inject_error_on(manager, "repair_tablet_fail_on_rpc_call", servers)
    try:
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, timeout=10)
        assert False # Check the tablet repair is not supposed to finish
    except TimeoutError:
        logger.info("Repair timeout as expected")
    await inject_error_off(manager, "repair_tablet_fail_on_rpc_call", servers)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_repair_error_delete(manager: ManagerClient):
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager)

    token = -1
    async def repair_task():
        await inject_error_on(manager, "repair_tablet_fail_on_rpc_call", servers)
        # Check failed repair request can be deleted
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, timeout=900)

    async def del_repair_task():
        tablet_task_id = None
        while tablet_task_id == None:
            tablet_task_id = await get_tablet_task_id(cql, hosts[0], table_id, token)
        status = None
        while status == None:
            try:
                status = await manager.api.get_task_status(servers[0].ip_addr, tablet_task_id)
            except:
                status == None
        await manager.api.abort_task(servers[0].ip_addr, tablet_task_id)

    await asyncio.gather(repair_task(), del_repair_task());
    await inject_error_off(manager, "repair_tablet_fail_on_rpc_call", servers)

def get_repair_row_from_disk(server):
    row_num = 0
    metrics = requests.get(f"http://{server.ip_addr}:9180/metrics").text
    pattern = re.compile("^scylla_repair_row_from_disk_nr")
    for metric in metrics.split('\n'):
        if pattern.match(metric) is not None:
            row_num += int(metric.split()[1])

    return row_num

def check_repairs(row_num_before: list[int], row_num_after: list[int], expected_repairs: list[int]):
    assert len(row_num_before) == len(row_num_after)
    for i, val_before in enumerate(row_num_before):
        if i in expected_repairs:
            assert val_before < row_num_after[i]
        else:
            assert val_before == row_num_after[i]

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.parametrize("included_host_count", [2, 1, 0])
async def test_tablet_repair_hosts_filter(manager: ManagerClient, included_host_count):
    injection = "handle_tablet_migration_repair_fail"
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager)
    hosts_filter = "00000000-0000-0000-0000-000000000000"
    if included_host_count == 1:
        hosts_filter = f"{hosts[0].host_id}"
    elif included_host_count == 2:
        hosts_filter = f"{hosts[0].host_id},{hosts[1].host_id}"

    row_num_before = [get_repair_row_from_disk(server) for server in servers]

    token = -1
    async def repair_task():
        await inject_error_on(manager, injection, servers)
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, hosts_filter=hosts_filter)

    async def check_filter():
        tablet_task_id = None
        while tablet_task_id == None:
            tablet_task_id = await get_tablet_task_id(cql, hosts[0], table_id, token)

        res = await load_tablet_repair_task_infos(cql, hosts[0], table_id)
        assert len(res) == 1
        assert res[str(token)].repair_hosts_filter.split(",").sort() == hosts_filter.split(",").sort()

        await inject_error_off(manager, injection, servers)

    await asyncio.gather(repair_task(), check_filter())

    row_num_after = [get_repair_row_from_disk(server) for server in servers]
    check_repairs(row_num_before, row_num_after, [0, 1] if included_host_count == 2 else [])

async def prepare_multi_dc_repair(manager) -> tuple[list[ServerInfo], CassandraSession, list[Host], str, str]:
    servers = [await manager.server_add(property_file = {'dc': 'DC1', 'rack' : 'R1'}),
               await manager.server_add(property_file = {'dc': 'DC1', 'rack' : 'R2'}),
               await manager.server_add(property_file = {'dc': 'DC2', 'rack' : 'R3'})]
    cql = manager.get_cql()
    ks = await create_new_test_keyspace(cql, "WITH replication = {'class': 'NetworkTopologyStrategy', "
                  "'DC1': 2, 'DC2': 1} AND tablets = {'initial': 8};")
    await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH tombstone_gc = {{'mode':'repair'}};")
    keys = range(256)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])
    table_id = await manager.get_table_id(ks, "test")
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    return (servers, cql, hosts, ks, table_id)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
@pytest.mark.parametrize("dcs_filter_and_res", [("DC1", [0, 1]), ("DC2", []), ("DC3", [])])
async def test_tablet_repair_dcs_filter(manager: ManagerClient, dcs_filter_and_res):
    dcs_filter, expected_repairs = dcs_filter_and_res
    servers, cql, hosts, ks, table_id = await prepare_multi_dc_repair(manager)

    row_num_before = [get_repair_row_from_disk(server) for server in servers]

    token = -1
    async def repair_task():
        await inject_error_on(manager, "repair_tablet_fail_on_rpc_call", servers)
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, dcs_filter=dcs_filter)

    async def check_filter():
        tablet_task_id = None
        while tablet_task_id == None:
            tablet_task_id = await get_tablet_task_id(cql, hosts[0], table_id, token)

        res = await load_tablet_repair_task_infos(cql, hosts[0], table_id)
        assert len(res) == 1
        assert res[str(token)].repair_dcs_filter.split(",").sort() == dcs_filter.split(",").sort()

        await inject_error_off(manager, "repair_tablet_fail_on_rpc_call", servers)

    await asyncio.gather(repair_task(), check_filter())

    row_num_after = [get_repair_row_from_disk(server) for server in servers]
    check_repairs(row_num_before, row_num_after, expected_repairs)

@pytest.mark.asyncio
@skip_mode('release', 'error injections are not supported in release mode')
async def test_tablet_repair_hosts_and_dcs_filter(manager: ManagerClient):
    servers, cql, hosts, ks, table_id = await prepare_multi_dc_repair(manager)
    dcs_filter = "DC1,DC2"
    hosts_filter = f"{hosts[0].host_id},{hosts[2].host_id}"

    row_num_before = [get_repair_row_from_disk(server) for server in servers]

    token = -1
    async def repair_task():
        await inject_error_on(manager, "repair_tablet_fail_on_rpc_call", servers)
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, hosts_filter=hosts_filter, dcs_filter=dcs_filter)

    async def check_filter():
        tablet_task_id = None
        while tablet_task_id == None:
            tablet_task_id = await get_tablet_task_id(cql, hosts[0], table_id, token)

        res = await load_tablet_repair_task_infos(cql, hosts[0], table_id)
        assert len(res) == 1
        assert res[str(token)].repair_dcs_filter.split(",").sort() == dcs_filter.split(",").sort()

        await inject_error_off(manager, "repair_tablet_fail_on_rpc_call", servers)

    await asyncio.gather(repair_task(), check_filter())

    row_num_after = [get_repair_row_from_disk(server) for server in servers]
    check_repairs(row_num_before, row_num_after, [0, 2])

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

    map0 = await load_tablet_sstables_repaired_at(cql, hosts[0:1], table_id)
    logging.info(f'map1={map0}')
    assert get_sstables_repaired_at(map0, token) == sstables_repaired_at

    # First repair
    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)
    map1 = await load_tablet_sstables_repaired_at(cql, hosts[0:1], table_id)
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
    map2 = await load_tablet_sstables_repaired_at(cql, hosts[0:1], table_id)
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
    map0 = await load_tablet_sstables_repaired_at(cql, hosts[0:1], table_id)

    # Repair should not finish with error
    await inject_error_on(manager, "repair_tablet_fail_on_rpc_call", servers)
    try:
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, timeout=10)
        assert False # Check the tablet repair is not supposed to finish
    except TimeoutError:
        logger.info("Repair timeout as expected")
    await inject_error_off(manager, "repair_tablet_fail_on_rpc_call", servers)

    map1 = await load_tablet_sstables_repaired_at(cql, hosts[0:1], table_id)

    # Check the sstables_repaired_at is not increased
    assert get_sstables_repaired_at(map0, token) == get_sstables_repaired_at(map1, token)

@pytest.mark.asyncio
async def do_tablet_incremental_repair_and_ops(manager: ManagerClient, ops: str):
    nr_keys = 100
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, nr_keys=nr_keys)
    current_key = nr_keys
    token = -1
    logs = []
    for s in servers:
        logs.append(await manager.server_open_log(s.server_id))

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
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, nr_keys=nr_keys)
    repaired_keys = set(range(0, nr_keys))
    unrepaired_keys = set()
    current_key = nr_keys
    token = 'all'

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
    await verify_repaired_and_unrepaired_keys(manager, scylla_path, servers, ks, repaired_keys, unrepaired_keys)

@pytest.mark.asyncio
async def do_test_tablet_incremental_repair_with_split_and_merge(manager, do_split, do_merge):
    nr_keys = 100
    cmdline = []
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, nr_keys=nr_keys, cmdline=cmdline)
    repaired_keys = set(range(0, nr_keys))
    unrepaired_keys = set()
    current_key = nr_keys
    token = 'all'
    logs = []
    for s in servers:
        await manager.api.set_logger_level(s.ip_addr, "load_balancer", "debug")
        await manager.api.set_logger_level(s.ip_addr, "table", "debug")
        logs.append(await manager.server_open_log(s.server_id))
    s1_log = logs[0]

    if False:
        # Disable autocompaction
        for server in servers:
            await manager.api.disable_autocompaction(server.ip_addr, ks, 'test')

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

    if False:
        # Enable autocompaction
        for server in servers:
            await manager.api.enable_autocompaction(server.ip_addr, ks, 'test')

    if False:
        tm = TaskManagerClient(manager.api)
        for server in servers:
            module_name = "compaction"
            tasks = await tm.list_tasks(server.ip_addr, module_name)
            logger.info(f"Compaction {tasks=}")
            await tm.drain_module_tasks(server.ip_addr, module_name)

    if do_split:
        s1_mark = await s1_log.mark()
        await inject_error_on(manager, "tablet_force_tablet_count_increase", servers)
        await s1_log.wait_for('Detected tablet split for table', from_mark=s1_mark)
        await inject_error_off(manager, "tablet_force_tablet_count_increase", servers)

    if do_merge:
        s1_mark = await s1_log.mark()
        await inject_error_on(manager, "tablet_force_tablet_count_decrease", servers)
        await s1_log.wait_for('Detected tablet merge for table', from_mark=s1_mark)
        await inject_error_off(manager, "tablet_force_tablet_count_decrease", servers)

    scylla_path = get_scylla_path(cql)

    if False:
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
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager, nr_keys=nr_keys, cmdline=cmdline)
    repaired_keys = set(range(0, nr_keys))
    unrepaired_keys = set()
    current_key = nr_keys
    token = 'all'

    await manager.server_stop_gracefully(servers[1].server_id)

    # Insert more keys
    await insert_keys(cql, ks, current_key, current_key + nr_keys)
    repaired_keys = repaired_keys | set(range(current_key, current_key + nr_keys))
    current_key += nr_keys

    await manager.server_start(servers[1].server_id)

    await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token)

    scylla_path = get_scylla_path(cql)

    await verify_repaired_and_unrepaired_keys(manager, scylla_path, servers, ks, repaired_keys, unrepaired_keys)
