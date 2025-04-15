#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.pylib.util import wait_for_cql_and_get_hosts, Host
from test.cluster.conftest import skip_mode
from test.pylib.repair import load_tablet_repair_time, create_table_insert_data_for_repair, get_tablet_task_id, load_tablet_repair_task_infos
from test.pylib.rest_client import inject_error_one_shot, read_barrier
from test.cluster.util import create_new_test_keyspace

from cassandra.cluster import Session as CassandraSession

import pytest
import asyncio
import logging
import re
import requests
import time
import datetime

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
    servers, cql, hosts, ks, table_id = await create_table_insert_data_for_repair(manager)
    hosts_filter = "00000000-0000-0000-0000-000000000000"
    if included_host_count == 1:
        hosts_filter = f"{hosts[0].host_id}"
    elif included_host_count == 2:
        hosts_filter = f"{hosts[0].host_id},{hosts[1].host_id}"

    row_num_before = [get_repair_row_from_disk(server) for server in servers]

    token = -1
    async def repair_task():
        await inject_error_on(manager, "repair_tablet_fail_on_rpc_call", servers)
        await manager.api.tablet_repair(servers[0].ip_addr, ks, "test", token, hosts_filter=hosts_filter)

    async def check_filter():
        tablet_task_id = None
        while tablet_task_id == None:
            tablet_task_id = await get_tablet_task_id(cql, hosts[0], table_id, token)

        res = await load_tablet_repair_task_infos(cql, hosts[0], table_id)
        assert len(res) == 1
        assert res[str(token)].repair_hosts_filter.split(",").sort() == hosts_filter.split(",").sort()

        await inject_error_off(manager, "repair_tablet_fail_on_rpc_call", servers)

    await asyncio.gather(repair_task(), check_filter())

    row_num_after = [get_repair_row_from_disk(server) for server in servers]
    check_repairs(row_num_before, row_num_after, [0, 1] if included_host_count == 2 else [])

async def prepare_multi_dc_repair(manager) -> tuple[list[ServerInfo], CassandraSession, list[Host], str, str]:
    servers = [await manager.server_add(property_file = {'dc': 'DC1', 'rack' : 'R1'}),
               await manager.server_add(property_file = {'dc': 'DC1', 'rack' : 'R1'}),
               await manager.server_add(property_file = {'dc': 'DC2', 'rack' : 'R2'})]
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
