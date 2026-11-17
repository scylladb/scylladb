#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests for a user-defined function stored in the schema which compiled
# successfully when it was created, but fails to compile when Scylla loads it
# on startup - e.g., because a limit became stricter.

import time

import pytest
from cassandra.protocol import InvalidRequest

from test.cluster.util import new_test_keyspace, reconnect_driver
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts


# A Wasm function adding 1 to its argument, whose module has a memory of
# 20 pages (1.25 MB). This is within the default wasm_udf_memory_limit (2 MB),
# but not within a limit of 1 MB.
WASM_PLUS_ONE = ('(module (func $wasm_plus_one (param i32) (result i32) local.get 0 i32.const 1 i32.add) '
                 '(memory 20) (global i32 (i32.const 0)) (export "memory" (memory 0)) '
                 '(export "wasm_plus_one" (func $wasm_plus_one)) (export "_scylla_abi" (global 0)))')


# Test that when a function fails to compile when Scylla starts, the other
# functions of the same keyspace can still be used, and the failing function
# fails with an error when called, but can be dropped.
# Reproduces SCYLLADB-6379.
async def test_function_fails_to_compile_at_startup(manager: ScyllaClusterManager):
    # The cluster tests disable user-defined functions by default
    server = await manager.server_add(config={'enable_user_defined_functions': True})
    cql = manager.get_cql()
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}") as ks:
        await cql.run_async(f'CREATE TABLE {ks}.t (p int PRIMARY KEY, i int)')
        await cql.run_async(f'INSERT INTO {ks}.t (p, i) VALUES (1, 2)')
        await cql.run_async(f"CREATE FUNCTION {ks}.lua_plus_one(i int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE lua AS 'return i + 1'")
        await cql.run_async(f"CREATE FUNCTION {ks}.wasm_plus_one(i int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE wasm AS '{WASM_PLUS_ONE}'")

        # Restart Scylla with a memory limit too low for the Wasm function
        await manager.server_stop_gracefully(server.server_id)
        await manager.server_update_config(server.server_id, 'wasm_udf_memory_limit', 1024 * 1024)
        await manager.server_start(server.server_id)
        cql = await reconnect_driver(manager)
        await wait_for_cql_and_get_hosts(cql, [server], time.time() + 60)

        # Check that the Lua function still works
        rows = await cql.run_async(f'SELECT {ks}.lua_plus_one(i) AS r FROM {ks}.t WHERE p = 1')
        assert rows[0].r == 3
        # Check that calling the Wasm function fails with an error
        with pytest.raises(InvalidRequest, match='failed to compile'):
            await cql.run_async(f'SELECT {ks}.wasm_plus_one(i) AS r FROM {ks}.t WHERE p = 1')
        # Check that the Wasm function can still be dropped
        await cql.run_async(f'DROP FUNCTION {ks}.wasm_plus_one')
