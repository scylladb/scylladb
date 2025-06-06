# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

import asyncio
import random
import logging
import pytest
import itertools
import time
import contextlib
from test.pylib.manager_client import ManagerClient, ServerInfo
from test.pylib.rest_client import read_barrier, ScyllaMetrics, HTTPError
from cassandra.cluster import ConsistencyLevel, Session as CassandraSession
from cassandra.policies import FallthroughRetryPolicy, ConstantReconnectionPolicy
from cassandra.protocol import ServerError
from cassandra.query import SimpleStatement
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

async def get_metrics(manager: ManagerClient, servers: list[ServerInfo]) -> list[ScyllaMetrics]:
    return await asyncio.gather(*[manager.metrics.query(s.ip_addr) for s in servers])

def dict_memory(metrics: list[ScyllaMetrics]) -> int:
    return sum([m.get("scylla_sstable_compression_dicts_total_live_memory_bytes") for m in metrics])

async def live_update_config(manager: ManagerClient, servers: list[ServerInfo], key: str, value: str):
    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, deadline = time.time() + 60)
    await asyncio.gather(*[cql.run_async("UPDATE system.config SET value=%s WHERE name=%s", [value, key], host=host) for host in hosts])

async def populate_with_repeated_blob(ks: str, cf: str, blob: bytes, pk_range, cql: CassandraSession):
    insert = cql.prepare(f"INSERT INTO {ks}.{cf} (pk, c) VALUES (?, ?);")
    insert.consistency_level = ConsistencyLevel.ALL;
    for pks in itertools.batched(pk_range, n=100):
        await asyncio.gather(*[
            cql.run_async(insert, [k, blob])
            for k in pks
        ])

async def get_data_size_for_server(manager: ManagerClient, server: ServerInfo, ks: str, cf: str) -> int:
    sstable_info = await manager.api.get_sstable_info(server.ip_addr, ks, cf)
    sizes = [x['data_size'] for s in sstable_info for x in s['sstables']]
    return sum(sizes)

async def get_total_data_size(manager: ManagerClient, servers: list[ServerInfo], ks: str, cf: str) -> int:
    return sum(await asyncio.gather(*[get_data_size_for_server(manager, s, ks, cf) for s in servers]))

common_debug_cli_options = [
    '--logger-log-level=storage_service=debug',
    '--logger-log-level=api=trace',
    '--logger-log-level=database=debug',
    '--logger-log-level=sstable_compressor_factory=debug',
    '--abort-on-seastar-bad-alloc',
    '--dump-memory-diagnostics-on-alloc-failure-kind=all',
]

async def test_retrain_dict(manager: ManagerClient):
    """
    Tests basic functionality of SSTable compression with shared dictionaries.
    - Creates a table.
    - Populates it with artificial data which compresses extremly well with dicts but extremely badly without dicts.
    - Calls retrain_dict to retrain the recommended dictionary for that table.
    - For both supported algorithms (lz4 and zstd):
        - Rewrites the existing sstables using the new dictionary.
        - Checks that sstable sizes decreased greatly after the rewrite.
        - Checks that the rewritten files are readable.
    - Checks that the recommended dictionary isn't forgotten after a reboot.
    - Checks that dictionaries are cleared after the corresponding table is dropped.
    """
    # Bootstrap cluster and configure server
    logger.info("Bootstrapping cluster")

    ks_name = "test"
    cf_name = "test"
    rf = 2

    servers = (await manager.servers_add(2, cmdline=[
        *common_debug_cli_options,
    ], auto_rack_dc="dc1"))

    logger.info("Creating table")
    cql = manager.get_cql()
    await cql.run_async(
        f"CREATE KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}"
    )
    await cql.run_async(f"CREATE TABLE {ks_name}.{cf_name} (pk int PRIMARY KEY, c blob);")
    blob = random.randbytes(8*1024);

    logger.info("Disabling autocompaction for the table")
    await asyncio.gather(*[manager.api.disable_autocompaction(s.ip_addr, ks_name, cf_name) for s in servers])

    logger.info("Populating table")
    n_blobs = 1000
    await populate_with_repeated_blob("test", "test", blob, range(n_blobs), cql)
    total_uncompressed_size = len(blob) * n_blobs * 2

    logger.info("Flushing table")
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, ks_name, cf_name) for s in servers])

    logger.info("Checking initial SSTables")
    total_size = await get_total_data_size(manager, servers, ks_name, cf_name)
    assert total_size > 0.9 * total_uncompressed_size
    logger.info(f"Compression ratio is {total_size/total_uncompressed_size}")

    logger.info("Altering table to ZstdWithDictsCompressor")
    await cql.run_async(
        "ALTER TABLE test.test WITH COMPRESSION = {'sstable_compression': 'ZstdWithDictsCompressor', 'chunk_length_in_kb': 4};"
    )

    logger.info("Rewrite SSTables")
    await asyncio.gather(*[manager.api.keyspace_upgrade_sstables(s.ip_addr, ks_name) for s in servers])

    logger.info("Checking SSTables after upgrade to zstd")
    total_size = await get_total_data_size(manager, servers, ks_name, cf_name)
    assert total_size > 0.9 * total_uncompressed_size
    logger.info(f"Compression ratio is {total_size/total_uncompressed_size}")

    logger.info("Retraining dict")
    await manager.api.retrain_dict(servers[0].ip_addr, ks_name, cf_name)
    await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])

    logger.info("Rewriting SSTables")
    await asyncio.gather(*[manager.api.keyspace_upgrade_sstables(s.ip_addr, ks_name) for s in servers])

    logger.info("Checking SSTable sizes after retraining dict")
    total_size = await get_total_data_size(manager, servers, ks_name, cf_name)
    assert total_size < 0.1 * total_uncompressed_size
    logger.info(f"Compression ratio is {total_size/total_uncompressed_size}")

    logger.info("Rebooting, rewriting, and checking size again")
    await asyncio.gather(*[manager.server_stop_gracefully(s.server_id) for s in servers])
    await asyncio.gather(*[manager.server_start(s.server_id) for s in servers])
    await asyncio.gather(*[manager.api.keyspace_upgrade_sstables(s.ip_addr, ks_name) for s in servers])
    total_size = await get_total_data_size(manager, servers, ks_name, cf_name)
    assert total_size < 0.1 * total_uncompressed_size
    logger.info(f"Compression ratio is {total_size/total_uncompressed_size}")

    logger.info("Validating that the zstd-dict-compressed SSTables are readable")
    await manager.driver_connect(server=servers[0])
    cql = manager.get_cql()
    select = cql.prepare(f"SELECT c FROM {ks_name}.{cf_name} WHERE pk = ?;")
    select.consistency_level = ConsistencyLevel.ALL;
    results = await cql.run_async(select, [42])
    assert results[0][0] == blob

    logger.info("Altering table to use LZ4WithDictsCompressor compression")
    await cql.run_async(
        "ALTER TABLE test.test WITH COMPRESSION = {'sstable_compression': 'LZ4WithDictsCompressor'};"
    )

    logger.info("Rewriting SSTables")
    await asyncio.gather(*[manager.api.keyspace_upgrade_sstables(s.ip_addr, "test") for s in servers])

    logger.info("Checking SSTable sizes")
    total_size = await get_total_data_size(manager, servers, ks_name, cf_name)
    assert total_size < 0.1 * total_uncompressed_size
    logger.info(f"Compression ratio is {total_size/total_uncompressed_size}")

    logger.info("Validating that the lz4-dict-compressed SSTables are readable")
    results = await cql.run_async(select, [42])
    assert results[0][0] == blob

    logger.info("Testing the estimator")
    logger.info("Populating with new rows")
    other_blob = random.randbytes(8*1024);
    await populate_with_repeated_blob(ks_name, cf_name, other_blob, range(n_blobs, 2*n_blobs), cql)
    logger.info("Flushing")
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, ks_name, cf_name) for s in servers])
    logger.info("Running the estimator")
    logger.info(await manager.api.estimate_compression_ratios(servers[0].ip_addr, ks_name, cf_name))

    # Check that dropping the table also drops the dict.
    assert (await cql.run_async("SELECT COUNT(name) FROM system.dicts"))[0][0] == 1
    logger.info("Dropping the table")
    await cql.run_async("DROP TABLE test.test")
    await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])
    assert (await cql.run_async("SELECT COUNT(name) FROM system.dicts"))[0][0] == 0

    logger.info("Test completed successfully")

async def test_estimate_compression_ratios(manager: ManagerClient):
    logger.info("Bootstrapping cluster")

    ks_name = "test"
    cf_name = "test"
    rf = 2

    servers = (await manager.servers_add(2, cmdline=[
        *common_debug_cli_options,
    ], auto_rack_dc="dc1"))

    cql = manager.get_cql()

    logger.info("Creating table")
    await cql.run_async(
        f"CREATE KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}"
    )

    chunk_len_kb = 4
    await cql.run_async(f"""
        CREATE TABLE {ks_name}.{cf_name} (pk int PRIMARY KEY, c blob)
        WITH COMPRESSION = {{'sstable_compression': 'ZstdWithDictsCompressor', 'chunk_length_in_kb': {chunk_len_kb}}};
    """)

    logger.info("Disabling autocompaction for the table")
    await asyncio.gather(*[manager.api.disable_autocompaction(s.ip_addr, ks_name, cf_name) for s in servers])

    blob_size = 8 * 1024

    logger.info("Populating table with a repeated random blob")
    await populate_with_repeated_blob("test", "test", random.randbytes(blob_size), range(1000), cql)

    logger.info("Flushing table")
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, ks_name, cf_name) for s in servers])

    logger.info("Retraining dict")
    await manager.api.retrain_dict(servers[0].ip_addr, ks_name, cf_name)
    await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])

    logger.info("Adding a second set of rows with a different random blob")
    await populate_with_repeated_blob("test", "test", random.randbytes(blob_size), range(1000, 2000), cql)

    logger.info("Flushing table")
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, ks_name, cf_name) for s in servers])

    logger.info("Calling the estimator")
    result = await manager.api.estimate_compression_ratios(servers[0].ip_addr, ks_name, cf_name)
    #[
    #  {
    #    'level': 1,
    #    'chunk_length_in_kb': 1,
    #    'dict': 'none',
    #    'sstable_compression': 'LZ4WithDictsCompressor',
    #    'ratio': 1.0087509
    #    },
    #    ...
    #  }
    #]
    logger.debug(f"Estimator results: {result}")

    expected_entries = {}
    for dic in ['none', 'past', 'future']:
        for chunk_length_in_kb in [1, 4, 16]:
            # The first blob_size bytes in the blob are uncompressible,
            # the rest compresses perfectly against the front part.
            ratio = 1.0 if chunk_length_in_kb <= 4 else chunk_len_kb * 1024 / blob_size
            match dic:
                case 'none':
                    pass
                case 'past':
                    # 'past' dict perfectly compresses half of the dataset.
                    # The computed ratio only applies to the other half.
                    ratio = ratio / 2
                case 'future':
                    # 'future' dict perfectly compresses half the entire dataset.
                    ratio = 0.0
            e = {'level': 1, 'chunk_length_in_kb': chunk_length_in_kb, 'dict': dic, 'sstable_compression': 'LZ4WithDictsCompressor'}
            expected_entries[tuple(e.items())] = ratio
            for level in range(1, 6):
                e = {'level': level, 'chunk_length_in_kb': chunk_length_in_kb, 'dict': dic, 'sstable_compression': 'ZstdWithDictsCompressor'}
                expected_entries[tuple(e.items())] = ratio

    logger.debug(f"Checking that estimation results match expectations: {result}")
    for r in result:
        ratio = r.pop('ratio')
        assert isinstance(ratio, float)
        expected_ratio = expected_entries[tuple(r.items())]
        logger.debug(f"r={r}")
        assert expected_ratio - 0.1 < ratio < expected_ratio + 0.1

    assert sorted(expected_entries.keys()) == sorted(tuple(r.items()) for r in result)

async def test_dict_memory_limit(manager: ManagerClient):
    # Bootstrap cluster and configure server
    logger.info("Bootstrapping cluster")

    # Should be enough to fit 3 dicts of size <128 kiB.
    # (The default dict size is ~110 kiB)
    shard_memory = 1*1024*1024*1024 / 2
    intended_dict_memory_bugdet = 4*128*1024
    mem_fraction = intended_dict_memory_bugdet / shard_memory
    target_n_dicts = 3

    servers = (await manager.servers_add(1, cmdline=[
        *common_debug_cli_options,
        #'--memory=1G', # test.py forces --memory=1G, and it can't be used twice
        '--smp=2',
        f'--sstable-compression-dictionaries-memory-budget-fraction={mem_fraction}',
    ]))

    ks_name = "test"
    cf_name = "test"
    rf = 1

    # Create keyspace and table
    cql = manager.get_cql()

    await cql.run_async(
        f"CREATE KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}"
    )

    for algo in ['ZstdWithDictsCompressor', 'LZ4WithDictsCompressor']:
        logger.info(f"Creating table with algo {algo}")

        await cql.run_async("CREATE TABLE test.test (pk int PRIMARY KEY, c blob);")

        logger.info("Disabling autocompaction for the table")
        await asyncio.gather(*[manager.api.disable_autocompaction(s.ip_addr, ks_name, cf_name) for s in servers])

        logger.info("Altering table to use zstd compression")
        await cql.run_async(
            f"ALTER TABLE test.test WITH COMPRESSION = {{'sstable_compression': '{algo}'}};"
        )

        logger.info("Creating several SSTables, retraining a dict after each")
        insert = cql.prepare("INSERT INTO test.test (pk, c) VALUES (?, ?);")
        for i in range(target_n_dicts + 2):
            blob = random.randbytes(96 * 1024)
            await asyncio.gather(*[cql.run_async(insert, [i, blob]) for i in range(10)])
            await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, ks_name, cf_name) for s in servers])
            await manager.api.retrain_dict(servers[0].ip_addr, "test", "test")
            dict_mem = dict_memory(await get_metrics(manager, servers))
            assert dict_mem < intended_dict_memory_bugdet * 2
            total_size = await get_total_data_size(manager, servers, ks_name, cf_name)
            logger.info(f"Round 0, step {i}: total_size={total_size}, dictmem={dict_mem}")

        assert dict_mem > intended_dict_memory_bugdet * 0.5

        logger.info("Rewriting sstables to latest dict")
        await asyncio.gather(*[manager.api.keyspace_upgrade_sstables(s.ip_addr, ks_name) for s in servers])
        # There should only exist 1 live dict.
        assert dict_memory(await get_metrics(manager, servers)) < 256*1024

        logger.info("Validating query results")
        select = cql.prepare("SELECT c FROM test.test WHERE pk = ?;")
        select.consistency_level = ConsistencyLevel.ALL;
        results = await cql.run_async(select, [0])
        assert results[0][0] == blob

        logger.info("Dropping the table and checking that there are no leftovers")
        await cql.run_async("DROP TABLE test.test")
        assert dict_memory(await get_metrics(manager, servers)) == 0

async def test_sstable_compression_dictionaries_enable_writing(manager: ManagerClient):
    """
    Tests basic functionality of the `sstable_compression_dictionaries_enable_writing` config knob.
    When it's disabled, the node should fall back from the new
    LZ4WithDictsCompressor and ZstdWithDictsCompressor
    to the Cassandra-compatible LZ4Compressor and ZstdCompressor.
    """
    servers = await manager.servers_add(1, [
        *common_debug_cli_options,
    ])

    # Create keyspace and table
    logger.info("Creating tables")
    cql = manager.get_cql()

    dict_algorithms = ['LZ4WithDicts', 'ZstdWithDicts']
    nondict_algorithms = ['Snappy', 'LZ4', 'Deflate', 'Zstd']
    algorithms = dict_algorithms + nondict_algorithms
    no_compression = 'NoCompression'
    all_tables = dict_algorithms + nondict_algorithms + [no_compression]

    await cql.run_async("""
        CREATE KEYSPACE test
        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}
    """)
    await asyncio.gather(*[
        cql.run_async(f'''
            CREATE TABLE test."{algo}" (pk int PRIMARY KEY, c blob)
            WITH COMPRESSION = {{'sstable_compression': '{algo}Compressor'}};
        ''')
        for algo in algorithms],
        cql.run_async(f'''
            CREATE TABLE test."{no_compression}" (pk int PRIMARY KEY, c blob)
            WITH COMPRESSION  = {{}}
        ''')
    )


    # Populate data with
    blob = random.randbytes(16*1024);
    logger.info("Populating table")
    n_blobs = 100
    for algo in all_tables:
        insert = cql.prepare(f'''INSERT INTO test."{algo}" (pk, c) VALUES (?, ?);''')
        insert.consistency_level = ConsistencyLevel.ALL;
        for pks in itertools.batched(range(n_blobs), n=100):
            await asyncio.gather(*[
                cql.run_async(insert, [k, blob])
                for k in pks
            ])

    async def validate_select():
        cql = manager.get_cql()
        for algo in all_tables:
            select = cql.prepare(f'''SELECT c FROM test."{algo}" WHERE pk = ? BYPASS CACHE;''')
            results = await cql.run_async(select, [42])
            assert results[0][0] == blob

    # Flush to get initial sstables
    await asyncio.gather(*[manager.api.keyspace_flush(s.ip_addr, "test") for s in servers])

    async def get_compressor_names(cf: str) -> set[str]:
        sstable_info = await manager.api.get_sstable_info(servers[0].ip_addr, "test", cf)
        # Example sstable_info:
        # [
        # {
        #     "keyspace": "test",
        #     "sstables": [
        #     {
        #         "data_size": 4106,
        #         "extended_properties": [
        #         {
        #             "attributes": [
        #             {
        #                 "key": "sstable_compression",
        #                 "value": "LZ4WithDictsCompressor"
        #             }
        #             ],
        #             "group": "compression_parameters"
        #         }
        #         ],
        #         "filter_size": 28,
        #         "generation": "3gon_0xa1_22swh25ky8cykb6muf",
        #         "index_size": 128,
        #         "level": 0,
        #         "size": 73219,
        #         "timestamp": "2025-03-17T11:58:49Z",
        #         "version": "me"
        #     },
        #     ],
        #     "table": "LZ4WithDicts"
        # }
        # ]
        names = set()
        for table_info in sstable_info:
            for sstable in table_info["sstables"]:
                for prop in sstable.get("extended_properties", []):
                    if prop["group"] == "compression_parameters":
                        for attr in prop["attributes"]:
                            if attr["key"] == "sstable_compression":
                                names.add(attr.get("value"))
        return names

    await asyncio.gather(*[
        manager.api.retrain_dict(servers[0].ip_addr, "test", algo)
        for algo in all_tables
    ])
    await asyncio.gather(*[read_barrier(manager.api, s.ip_addr) for s in servers])
    await manager.api.keyspace_upgrade_sstables(servers[0].ip_addr, "test")

    name_prefix = "org.apache.cassandra.io.compress."

    for algo in dict_algorithms:
        assert (await get_compressor_names(algo)) == {f"{algo}Compressor"}
    for algo in nondict_algorithms:
        assert (await get_compressor_names(algo)) == {name_prefix + f"{algo}Compressor"}
    assert (await get_compressor_names(no_compression)) == set()

    await live_update_config(manager, servers, 'sstable_compression_dictionaries_enable_writing', "false")
    await manager.api.keyspace_upgrade_sstables(servers[0].ip_addr, "test")

    assert (await get_compressor_names("LZ4WithDicts")) == {name_prefix + "LZ4Compressor"}
    assert (await get_compressor_names("ZstdWithDicts")) == {name_prefix + "ZstdCompressor"}
    for algo in nondict_algorithms:
        assert (await get_compressor_names(algo)) == {name_prefix + f"{algo}Compressor"}
    assert (await get_compressor_names(no_compression)) == set()

async def test_sstable_compression_dictionaries_allow_in_ddl(manager: ManagerClient):
    """
    Tests the sstable_compression_dictionaries_allow_in_ddl option.
    When it's disabled, ALTER and CREATE statements should not be allowed
    to configure tables to use compression dictionaries for sstables.
    """
    # Bootstrap cluster and configure server
    logger.info("Bootstrapping cluster")

    servers = (await manager.servers_add(1, cmdline=[
        *common_debug_cli_options,
        "--sstable-compression-dictionaries-allow-in-ddl=false",
    ], auto_rack_dc="dc1"))

    @contextlib.asynccontextmanager
    async def with_expect_server_error(msg):
        try:
            yield
        except ServerError as e:
            if e.message != msg:
                raise
        else:
            raise Exception('Expected a ServerError, got no exceptions')

    cql = manager.get_cql()
    hosts = await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    await cql.run_async("""
        CREATE KEYSPACE test
        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}
    """)

    for new_algo in ['LZ4WithDicts', 'ZstdWithDicts']:
        logger.info(f"Tested algorithm: {new_algo}")
        table_name = f"test.{new_algo}"

        logger.info("Check that disabled sstable_compression_dictionaries_allow_in_ddl prevents CREATE with dict compression")
        async with with_expect_server_error(f"sstable_compression {new_algo}Compressor has been disabled by `sstable_compression_dictionaries_allow_in_ddl: false`"):
            await cql.run_async(SimpleStatement(f'''
                CREATE TABLE {table_name} (pk int PRIMARY KEY, c blob)
                WITH COMPRESSION = {{'sstable_compression': '{new_algo}Compressor'}};
            ''', retry_policy=FallthroughRetryPolicy()), host=hosts[0])

        logger.info("Enable the config option")
        await live_update_config(manager, servers, 'sstable_compression_dictionaries_allow_in_ddl', "true")

        logger.info("CREATE the table with dict compression")
        await cql.run_async(SimpleStatement(f'''
            CREATE TABLE {table_name} (pk int PRIMARY KEY, c blob)
            WITH COMPRESSION = {{'sstable_compression': '{new_algo}Compressor'}};
        ''', retry_policy=FallthroughRetryPolicy()), host=hosts[0])

        logger.info("Disable compression on the table")
        await cql.run_async(SimpleStatement(f'''
            ALTER TABLE {table_name}
            WITH COMPRESSION = {{'sstable_compression': ''}};
        ''', retry_policy=FallthroughRetryPolicy()), host=hosts[0])

        logger.info("Disable the config option again")
        await live_update_config(manager, servers, 'sstable_compression_dictionaries_allow_in_ddl', "false")

        logger.info("Check that disabled sstable_compression_dictionaries_allow_in_ddl prevents ALTER with dict compression")
        async with with_expect_server_error(f"sstable_compression {new_algo}Compressor has been disabled by `sstable_compression_dictionaries_allow_in_ddl: false`"):
            await cql.run_async(SimpleStatement(f'''
                ALTER TABLE {table_name}
                WITH COMPRESSION = {{'sstable_compression': '{new_algo}Compressor'}};
            ''', retry_policy=FallthroughRetryPolicy()), host=hosts[0])

        logger.info("Enable the config option again")
        await live_update_config(manager, servers, 'sstable_compression_dictionaries_allow_in_ddl', "true")

        logger.info("ALTER the table with dict compression")
        await cql.run_async(SimpleStatement(f'''
            ALTER TABLE {table_name}
            WITH COMPRESSION = {{'sstable_compression': '{new_algo}Compressor'}};
        ''', retry_policy=FallthroughRetryPolicy()), host=hosts[0])
        logger.info("Enable the config option again")

        logger.info("Disable the config option for the next test")
        await live_update_config(manager, servers, 'sstable_compression_dictionaries_allow_in_ddl', "false")
