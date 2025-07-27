#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.internal_types import ServerInfo
from test.pylib.util import wait_for_cql_and_get_hosts, Host
from test.cluster.util import create_new_test_keyspace

from cassandra.cluster import Session as CassandraSession

import asyncio
import time
import logging
import json

async def load_tablet_repair_time(cql, hosts, table_id):
    all_rows = []
    repair_time_map = {}

    for host in hosts:
        logging.debug(f'Query hosts={host}');
        rows = await cql.run_async(f"SELECT last_token, repair_times from system.tablets where table_id = {table_id}", host=host)
        all_rows += rows
    for row in all_rows:
        logging.debug(f"Got system.tablets={row}")

    for row in all_rows:
        key = str(row[0])
        repair_time_map[key] = row[1][table_id] if row[1] is not None and table_id in row[1] else None

    return repair_time_map

async def load_tablet_repair_task_infos(cql, host, table_id):
    repair_task_infos = {}

    rows = await cql.run_async(f"SELECT last_token, repair_task_info_v2 from system.tablets where table_id = {table_id}", host=host)

    for row in rows:
        if row.repair_task_info_v2 is not None:
            key = str(row.last_token)
            repair_task_infos[key] = row.repair_task_info_v2

    return repair_task_infos

async def create_table_insert_data_for_repair(manager, rf = 3 , tablets = 8, fast_stats_refresh = True, nr_keys = 256, disable_flush_cache_time = False, cmdline = None) -> (list[ServerInfo], CassandraSession, list[Host], str, str):
    assert rf <= 3, "A keyspace with RF > 3 will be RF-rack-invalid if there are fewer racks than the RF"

    if fast_stats_refresh:
        config = {'error_injections_at_startup': ['short_tablet_stats_refresh_interval']}
    else:
        config = {}
    if disable_flush_cache_time:
        config.update({'repair_hints_batchlog_flush_cache_time_in_ms': 0})
    servers = await manager.servers_add(3, config=config, cmdline=cmdline,
                                        property_file=[{"dc": "dc1", "rack": f"r{i % rf}"} for i in range(rf)])
    cql = manager.get_cql()
    ks = await create_new_test_keyspace(cql, "WITH replication = {{'class': 'NetworkTopologyStrategy', "
                  "'replication_factor': {}}} AND tablets = {{'initial': {}}};".format(rf, tablets))
    await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int) WITH tombstone_gc = {{'mode':'repair'}};")
    keys = range(nr_keys)
    await asyncio.gather(*[cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES ({k}, {k});") for k in keys])
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    logging.info(f'Got hosts={hosts}');
    table_id = await manager.get_table_id(ks, "test")
    return (servers, cql, hosts, ks, table_id)

async def get_tablet_task_id(cql, host, table_id, token):
    rows = await cql.run_async(f"SELECT last_token, repair_task_info_v2 from system.tablets where table_id = {table_id}", host=host)
    for row in rows:
        if row.last_token == token:
            if row.repair_task_info_v2 == None:
                return None
            else:
                return str(row.repair_task_info_v2.tablet_task_id)
    return None

async def create_table_insert_data_for_repair_multiple_rows(manager, rf = 3 , tablets = 8, cmdline = None):
    assert rf <= 3, "A keyspace with RF > 3 will be RF-rack-invalid if there are fewer racks than the RF"
    config = {}

    servers = await manager.servers_add(3, config=config, cmdline=cmdline,
                                        property_file=[{"dc": "dc1", "rack": f"r{i % rf}"} for i in range(rf)])
    cql = manager.get_cql()
    ks = await create_new_test_keyspace(cql, "WITH replication = {{'class': 'NetworkTopologyStrategy', "
                  "'replication_factor': {}}} AND tablets = {{'initial': {}}};".format(rf, tablets))
    create_table_cql = f"CREATE TABLE IF NOT EXISTS {ks}.test ( pk int, ck int, data int, PRIMARY KEY (pk, ck)) WITH tombstone_gc = {{'mode':'repair'}};"
    await cql.run_async(create_table_cql)
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    table_id = await manager.get_table_id(ks, "test")
    return (servers, cql, hosts, ks, table_id)
