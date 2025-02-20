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
        rows = await cql.run_async(f"SELECT last_token, repair_time from system.tablets where table_id = {table_id}", host=host)
        all_rows += rows
    for row in all_rows:
        logging.debug(f"Got system.tablets={row}")

    for row in all_rows:
        key = str(row[0])
        repair_time_map[key] = row[1]

    return repair_time_map

async def load_tablet_repair_task_infos(cql, host, table_id):
    repair_task_infos = {}

    rows = await cql.run_async(f"SELECT last_token, repair_task_info from system.tablets where table_id = {table_id}", host=host)

    for row in rows:
        if row.repair_task_info is not None:
            key = str(row.last_token)
            repair_task_infos[key] = row.repair_task_info

    return repair_task_infos

async def create_table_insert_data_for_repair(manager, rf = 3 , tablets = 8, fast_stats_refresh = True, nr_keys = 256, disable_flush_cache_time = False) -> (list[ServerInfo], CassandraSession, list[Host], str, str):
    if fast_stats_refresh:
        config = {'error_injections_at_startup': ['short_tablet_stats_refresh_interval']}
    else:
        config = {}
    if disable_flush_cache_time:
        config.update({'repair_hints_batchlog_flush_cache_time_in_ms': 0})
    servers = [await manager.server_add(config=config), await manager.server_add(config=config), await manager.server_add(config=config)]
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
    rows = await cql.run_async(f"SELECT last_token, repair_task_info from system.tablets where table_id = {table_id}", host=host)
    for row in rows:
        if row.last_token == token:
            if row.repair_task_info == None:
                return None
            else:
                return str(row.repair_task_info.tablet_task_id)
    return None
