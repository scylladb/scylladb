#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import contextlib
import tempfile
import time
import glob
import os
import itertools
import logging
import subprocess
import json
import uuid

from test import DEBUG_MODES
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.internal_types import ServerInfo
from test.pylib.util import wait_for_cql_and_get_hosts
from test.pylib.tablets import get_all_tablet_replicas

from test.pylib import nodetool

from test.pylib.encryption_provider import KeyProviderFactory, KeyProvider, make_key_provider_factory, KMSKeyProviderFactory, LocalFileSystemKeyProviderFactory
from test.cluster.util import new_test_keyspace, new_test_table, FeatureConfig
from test.cluster.dtest.tools.assertions import assert_one
from test.cqlpy.test_tools import _generate_key_for_shard

from typing import Callable, Coroutine

from cassandra import ConsistencyLevel
from cassandra.cluster import Session as CassandraSession, NoHostAvailable
from cassandra.protocol import ConfigurationException
from cassandra.auth import PlainTextAuthProvider

import pytest

logger = logging.getLogger(__name__)

@pytest.fixture(scope="function")
def workdir():
    # pylint: disable=missing-function-docstring
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir

async def test_file_streaming_respects_encryption(manager: ScyllaClusterManager,
                                                  storage_config: FeatureConfig, workdir):
    # pylint: disable=missing-function-docstring
    cfg = {
        'tablets_mode_for_new_keyspaces': 'enabled',
    }
    cfg = storage_config.get_cluster_cfg(cfg)

    cmdline = ['--smp=1']
    servers = []
    servers.append(await manager.server_add(config=cfg, cmdline=cmdline))
    await manager.disable_tablet_balancing()

    cql = manager.cql
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    ks_cmd = storage_config.get_keyspace_opts(
        "CREATE KEYSPACE ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1}")
    cql.execute(ks_cmd)
    cql.execute(f"""CREATE TABLE ks.t(pk text primary key) WITH scylla_encryption_options = {{
        'cipher_algorithm' : 'AES/ECB/PKCS5Padding',
        'secret_key_strength' : 128,
        'key_provider': 'LocalFileSystemKeyProviderFactory',
        'secret_key_file': '{workdir}/data_encryption_key'
    }}""")
    cql.execute("INSERT INTO ks.t(pk) VALUES('alamakota')")

    servers.append(await manager.server_add(config=cfg, cmdline=cmdline))

    tablet_replicas = await get_all_tablet_replicas(manager, servers[0], 'ks', 't')
    host_ids = await asyncio.gather(*[manager.get_host_id(s.server_id) for s in servers])
    await manager.api.move_tablet(servers[0].ip_addr, "ks", "t", host_ids[0], 0, host_ids[1], 0, tablet_replicas[0][0])

    rows = cql.execute("SELECT * from ks.t WHERE pk = 'alamakota'")
    assert len(list(rows)) == 1


def filter_ciphers(kp: KeyProviderFactory, ciphers=dict[str, list[int]]) -> list[tuple[str, len]]:
    """filter out ciphers based on provider caps"""
    if not ciphers:
        return [(None, None)]
    return [(cipher, length) for cipher in ciphers for length in ciphers[cipher] 
            if kp.supported_cipher(cipher, length)]

async def create_ks(manager: ScyllaClusterManager, replication_factor: int=1):
    """create test keyspace"""
    return new_test_keyspace(manager,
                             opts="with replication = {'class': 'NetworkTopologyStrategy', "
                             f"'replication_factor': {replication_factor}}}"
                             )


async def create_encrypted_cf(manager: ScyllaClusterManager, ks: str,
    columns: str=None,
    cipher_algorithm=None,
    secret_key_strength=None,
    compression=None,
    additional_options=None,
):
    """create test cf"""
    if additional_options is None:
        additional_options = {}
    if columns is None:
        columns = "key text PRIMARY KEY, c1 text, c2 text"
    options = {}
    if additional_options:
        options.update(additional_options)
    if cipher_algorithm:
        options.update({"cipher_algorithm": cipher_algorithm})
    if secret_key_strength:
        options.update({"secret_key_strength": secret_key_strength})

    extra = f'WITH scylla_encryption_options={options}'

    if compression is not None:
        extra = f"{extra} AND compression = {{ 'sstable_compression': '{compression}Compressor' }}"

    return new_test_table(manager, ks, columns, extra)

async def do_prepare_write_workload(cql: CassandraSession, table_name, key_ids, flush=True) -> list[str]:
    """write some data, returns list of written partition keys"""
    c1_values = ['value1']
    c2_values = ['value2']

    statement = cql.prepare(f"INSERT INTO {table_name} (key, c1, c2) VALUES (?, ?, ?)")
    statement.consistency_level = ConsistencyLevel.ALL

    await asyncio.gather(*[cql.run_async(statement, params) for params in
                           list(map(lambda x, y, z: [x, y, z], key_ids,
                                    itertools.cycle(c1_values),
                                    itertools.cycle(c2_values)))]
                                    )

    if flush:
        await nodetool.flush(cql, table_name)

    return key_ids


async def prepare_write_workload(cql: CassandraSession, table_name, flush=True, n: int = None) -> list[str]:
    """write some data, returns list of written partition keys"""
    key_ids = [f"k{x}" for x in range(n if n else 100)]
    return await do_prepare_write_workload(cql, table_name, key_ids, flush=flush)

async def read_verify_workload(cql: CassandraSession, table_name: str, keys: list[str]):
    """check written data using single-partition queries"""
    statement = cql.prepare(f"SELECT c1, c2 FROM {table_name} WHERE key = ?")
    rows = await asyncio.gather(*[cql.run_async(statement, [key]) for key in keys])
    for key, result in zip(keys, rows):
        assert len(list(result)) == 1, f"Expected 1 row for key={key}, got {len(list(result))}"

async def _smoke_test(manager: ScyllaClusterManager, key_provider: KeyProviderFactory,
                      ciphers: dict[str, list[int]], compression: str = None,
                      exception_handler: Callable[[Exception,str,str], None] = None,
                      options: dict = {},
                      num_servers: int = 1,
                      restart: Callable[[ScyllaClusterManager, list[ServerInfo], list[str]], Coroutine[None, None, None]] = None):
    """helper to create cluster, cfs, data and verify it after restart"""
    cfg = options | key_provider.configuration_parameters()

    servers: list[ServerInfo] = await manager.servers_add(servers_num = num_servers, config=cfg, auto_rack_dc='dc1')
    cql = manager.cql
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    async with await create_ks(manager, replication_factor = num_servers) as ks:
        # to reduce test time, create one cf for every alg/len combo we test.
        # avoids rebooting cluster for every check.
        async with contextlib.AsyncExitStack() as stack:
            cfs = []
            for cipher_algorithm, secret_key_strength in filter_ciphers(key_provider, ciphers):
                try:
                    additional_options = key_provider.additional_cf_options()
                    table_name = await stack.enter_async_context(
                        await create_encrypted_cf(manager, ks, cipher_algorithm=cipher_algorithm,
                                                  secret_key_strength=secret_key_strength,
                                                  compression=compression,
                                                  additional_options=additional_options
                                                  ))
                    keys = await prepare_write_workload(cql, table_name=table_name)
                    cfs.append((table_name, keys))
                except Exception as e:
                    if exception_handler:
                        exception_handler(e, cipher_algorithm, secret_key_strength)
                        continue
                    raise e
            # restart the cluster
            if restart:
                await restart(manager, servers, [table_name for table_name, _ in cfs])
                cql, _ = await manager.get_ready_cql(servers)
            else:
                await manager.rolling_restart(servers)
            for table_name, keys in cfs:
                await read_verify_workload(cql, table_name=table_name, keys=keys)

# default: 'AES/CBC/PKCS5Padding', length 128
supported_cipher_algorithms = {
    "": [],
    "AES/CBC/PKCS5Padding": [128, 192, 256],  # 192 has problem
    "AES/CBC": [128, 192, 256],  # 192 has problem
    "AES": [128, 192, 256],  # 192 has problem
    "AES/ECB/PKCS5Padding": [128, 192, 256],
    "AES/ECB": [128, 192, 256],
    # legacy algorithms, not supported in openssl 3.x
    # "DES/CBC/PKCS5Padding": [56],
    # "DES/CBC": [56],
    # "DES": [56],
    # 'DESede/CBC/PKCS5Padding': [112, 168],    # not support by Scylla, supported by DSE
    # 'Blowfish/CBC/PKCS5Padding': [32, 448],   # not support by Scylla, supported by DSE
    # "RC2/CBC/PKCS5Padding": [80, 128],  # [40, 80, 128]  # 40 to 128
    # "RC2/CBC": [80, 128],  # [40, 80, 128]  # 40 to 128
    # "RC2": [80, 128],  # [40, 80, 128]  # 40 to 128
}

async def test_supported_cipher_algorithms(manager, key_provider):
    """Checks our providers can operate the algos we claim"""
    errors = []
    def handler(e, cipher, length):
        logger.debug(str(e))
        errors.append(f"Cipher '{cipher}', length {length}, "
                      f"key provider {key_provider}' failed. Error {e}")

    await _smoke_test(manager, key_provider=key_provider, 
                      ciphers=supported_cipher_algorithms, 
                      exception_handler=handler)
    assert not errors, errors

async def test_wrong_cipher_algorithm(manager, key_provider):
    """Checks we reject non-valid cipher parameters/algos"""
    errors = []
    expected_errors = []

    broken_ciphers = {c: l for oc in supported_cipher_algorithms if oc
                      for l in [supported_cipher_algorithms[oc][:1]]
                      for a in ["Abc/", "/Abc", "Abc"] for c in [oc + a, a + oc]}

    def handler(e, cipher, length):
        try:
            raise e
        except (NoHostAvailable, ConfigurationException) as exc_details:
            error_str = str(exc_details)
            logger.debug(error_str)
            assert (
                f"Invalid algorithm string: {cipher}" in error_str
                or ("Invalid algorithm" in error_str and cipher in error_str)
                or "Could not write key file" in error_str
                or ("[Server error] message=" in error_str and "abc" in error_str)
                or "non-supported padding option" in error_str
                or "routines::unsupported" in error_str
            ), error_str
            expected_errors.append(e)
        except Exception as exc:
            errors.append(f"Unexpected exception: {exc}. Encryption options: "
                          f"'key_provider': '{key_provider}', 'cipher_algorithm': "
                          f"'{cipher}', 'secret_key_strength': {length}")
            logger.debug(errors[-1])

    await _smoke_test(manager, key_provider=key_provider, 
                      ciphers=broken_ciphers, 
                      exception_handler=handler)

    # TODO: Uncomment next line when issue https://github.com/scylladb/scylla-enterprise/issues/1973 will be resolve
    # assert not unexpected_success, "Negative tests succeeded unexpectedly: %s" % '\n'.join(unexpected_success)

    assert not errors, errors
    assert len(expected_errors) == len(broken_ciphers), expected_errors

@pytest.mark.parametrize(argnames="compression", argvalues=("LZ4", "Snappy", "Deflate"))
async def test_encryption_table_compression(manager, tmpdir, suite_log_dir, compression, scylla_binary):
    """Test compression + ear"""
    logger.debug("---- Test with compression: %s -----", compression)
    async with make_key_provider_factory(KeyProvider.local, tmpdir, suite_log_dir, scylla_binary) as key_provider:
        await _smoke_test(manager, key_provider,
                          ciphers={"AES/CBC/PKCS5Padding": [128]},
                          compression=compression)


async def test_reboot(manager, key_provider):
    """Tests SIGKILL restart of 3-node cluster"""
    async def restart(manager: ScyllaClusterManager, servers: list[ServerInfo], table_names: list[str]):
        # pylint: disable=unused-argument
        for s in servers:
            await manager.server_stop(s.server_id, convict=False)
            await manager.server_start(s.server_id)

    num_servers = 3
    # Replicated provider cannot handle hard reboot of cluster safely.
    # We can't be sure keys are propagated such that they are reachable
    # for restarted nodes. However, using single node the test can run,
    # though obviously somewhat lamely.
    if key_provider.key_provider == KeyProvider.replicated:
        num_servers = 1

    options = {"commitlog_sync": "batch"}
    await _smoke_test(manager, key_provider=key_provider, 
                      ciphers={"AES/CBC/PKCS5Padding": [128]},
                      options=options,
                      num_servers=num_servers,
                      restart=restart)

def get_sstables(node_workdir, ks:str, table:str, sst_type = None):
    """Glob sstable files (of type) at node_workdir"""
    base_pattern = os.path.join(node_workdir, "data", f"{ks}*", f"{table}-*", f"*{'-' + sst_type if sst_type else ''}.db")
    sstables = glob.glob(base_pattern)
    return sstables

async def get_sstable_metadata(manager: ScyllaClusterManager, server: ServerInfo, keyspace: str, column_family: str):
    """Load scylla metadata component sstables for server and cf"""
    scylla_path = await manager.server_get_exe(server.server_id)
    node_workdir = await manager.server_get_workdir(server.server_id)
    scylla_sstables = get_sstables(node_workdir, keyspace, column_family, 'Scylla')
    res = subprocess.check_output([scylla_path, "sstable", "dump-scylla-metadata",
                                   "--scylla-yaml-file",
                                   os.path.join(node_workdir, "conf", "scylla.yaml"),
                                   "--sstables"] + scylla_sstables,
                                  stderr=subprocess.PIPE)
    scylla_metadata = json.loads(res.decode('utf-8', 'ignore'))
    return scylla_metadata

async def validate_sstables_encryption(manager: ScyllaClusterManager, server: ServerInfo, table_name: str, encrypted:bool, expected_data=None):
    """Verify sstables for table encrypted or not"""
    keyspace, column_family  = table_name.split(".")
    scylla_path = await manager.server_get_exe(server.server_id)
    async with nodetool.no_autocompaction_context(manager.cql, table_name):
        scylla_metadata = await get_sstable_metadata(manager, server, keyspace, column_family)
        logger.debug("validate_sstables_encrypted(): scylla_metadata=%s", scylla_metadata)

        encrypt_opts = ["scylla_encryption_options" in metadata.get("extension_attributes", {}) 
                        for _, metadata in scylla_metadata['sstables'].items()]

        assert encrypt_opts, encrypt_opts # should not be empty

        if encrypted:
            assert all(encrypt_opts), encrypt_opts
        else:
            assert not any(encrypt_opts), encrypt_opts

        if expected_data is not None:
            node_workdir = await manager.server_get_workdir(server.server_id)
            sstables = get_sstables(node_workdir, keyspace, column_family)
            res = subprocess.check_output([scylla_path, "sstable", "query",
                                           "--scylla-yaml-file", 
                                           os.path.join(node_workdir, "conf", "scylla.yaml"),
                                           "--output-format", "json", "--sstables"] + sstables,
                                          stderr=subprocess.PIPE)
            scylla_data = json.loads(res.decode('utf-8', 'ignore'))
            actual_data = [list(r.values()) for r in scylla_data]
            assert actual_data == expected_data


async def test_alter(manager, key_provider):
    """Tests altering encrypted CF:s and verify sstable data"""
    async def restart(manager: ScyllaClusterManager, servers: list[ServerInfo], table_names: list[str]):
        cql = manager.cql
        expected_data = [list(row._asdict().values()) 
                         for row in cql.execute(f"SELECT * FROM {table_names[0]}")]
        logger.info("expected_data=%s", expected_data)
        # we cannot use tools like scylla sstable with replicated provider
        # and read encrypted tables.
        if key_provider.key_provider != KeyProvider.replicated:
            await validate_sstables_encryption(manager, servers[0],
                                               table_names[0], True,
                                               expected_data=expected_data)
        # disable encryption
        cql = manager.cql
        cql.execute(f"ALTER TABLE {table_names[0]} with "
                    "scylla_encryption_options={'key_provider':'none'}")
        table_desc = cql.execute(f"DESC {table_names[0]}").one().create_statement
        assert "key_provider" not in table_desc, f"key_provider isn't disabled, schema:\n {table_desc}"
        await manager.api.keyspace_upgrade_sstables(servers[0].ip_addr, table_names[0].split(".")[0])

        await validate_sstables_encryption(manager, servers[0],
                                           table_names[0], False,
                                           expected_data=expected_data)

        await read_verify_workload(cql, table_name=table_names[0], keys=[row[0] for row in expected_data])
        # enable encryption again
        options = key_provider.additional_cf_options()
        cql.execute(f"ALTER TABLE {table_names[0]} with scylla_encryption_options={options}")
        await manager.api.keyspace_upgrade_sstables(servers[0].ip_addr, table_names[0].split(".")[0])
        table_desc = cql.execute(f"DESC {table_names[0]}").one().create_statement
        assert options["key_provider"] in table_desc, f"key_provider set, schema:\n {table_desc}"
        await manager.rolling_restart(servers)

    await _smoke_test(manager, key_provider=key_provider, 
                      ciphers={"AES/CBC/PKCS5Padding": [128]},
                      restart=restart)

async def test_per_table_master_key(manager: ScyllaClusterManager, tmpdir, suite_log_dir):
    """Test per table KMS master key"""
    class MultiAliasKMSProvider (KMSKeyProviderFactory):
        """Special KMS using different master keys for each table"""
        def __init__(self, tmpdir, log_dir):
            super(MultiAliasKMSProvider, self).__init__(tmpdir, log_dir)
            self.key_count: int = 0
            self.key_ids: list = []
            self.aliases: list = []

        def additional_cf_options(self):
            alias_name = f"alias/Scylla-test-{self.key_count}"
            key_id = self.create_master_key(alias_name=alias_name)
            self.key_ids.append(key_id)
            self.aliases.append(alias_name)
            self.key_count += 1
            return super().additional_cf_options() | {"master_key": alias_name}

    async with MultiAliasKMSProvider(tmpdir, suite_log_dir) as kp:
        async def restart(manager: ScyllaClusterManager, servers: list[ServerInfo],
                          table_names: list[str]):
            await manager.rolling_restart(servers)
            i = 0
            for table_name in table_names:
                keyspace, column_family  = table_name.split(".")
                await validate_sstables_encryption(manager, servers[0],
                                                   table_name, True)
                async with nodetool.no_autocompaction_context(manager.cql, table_name):
                    scylla_metadata = await get_sstable_metadata(manager, servers[0], 
                                                                 keyspace, column_family)
                    table_key_ids = [metadata.get("extension_attributes", {})["scylla_key_id"]
                                          for _, metadata in scylla_metadata['sstables'].items()]
                    key_id = kp.key_ids[i]
                    i = i + 1
                    # AWS KMS key ids are encoded as encrypting key + encrypted data, thus
                    # the ID we got when creating the key earlier should be visible in 
                    # the metadata identifier
                    assert all([key_id in table_key_id for table_key_id in table_key_ids])

        await _smoke_test(manager, kp,
                          ciphers={"AES/CBC/PKCS5Padding": [128, 256]},
                          restart=restart)


async def test_non_existant_table_master_key(manager: ScyllaClusterManager, tmpdir, suite_log_dir):
    """Test we fail properly if using a non-existant master key"""
    class NoSuchKeyKMSProvider (KMSKeyProviderFactory):
        """Special KMS using nonexisting master key"""
        def additional_cf_options(self):
            return super().additional_cf_options() | {"master_key": "alias/NoSuchKey"}

    async with NoSuchKeyKMSProvider(tmpdir, suite_log_dir) as kp:
        with pytest.raises(Exception):
            await _smoke_test(manager, kp, ciphers={"AES/CBC/PKCS5Padding": [128]})

async def test_system_auth_encryption(manager: ScyllaClusterManager, tmpdir):
    cfg = {"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator", 
               "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer",
                "commitlog_sync": "batch" }

    servers: list[ServerInfo] = await manager.servers_add(servers_num = 1, config=cfg, 
                                                          driver_connect_opts={'auth_provider': PlainTextAuthProvider(username='cassandra', password='cassandra')})
    cql = manager.cql
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    async def grep_database_files(pattern: str, path: str, files: str, expect:bool):
        pattern_found_counter = 0
        pbytes = pattern.encode("utf-8")
        for server in servers:
            node_workdir = await manager.server_get_workdir(server.server_id)
            dirname = os.path.join(node_workdir, path)
            file_paths = glob.glob(os.path.join(dirname, files), recursive=True)
            file_paths = [f for f in file_paths if os.path.isfile(f) and not os.path.islink(f)]

            for file_path in file_paths:
                try:
                    with open(file_path, 'rb') as f:
                        data = f.read()
                        if pbytes in data:
                            pattern_found_counter += 1
                            logger.debug("Pattern '%s' found in %s", pattern, file_path)
                except FileNotFoundError:
                    pass # assume just compacted away

        if expect:
            assert pattern_found_counter > 0
        else:
            assert pattern_found_counter == 0

    async def verify_system_info(expect: bool):
        user = f"user_{str(uuid.uuid4())}".replace('-','_')
        pwd = f"pwd_{str(uuid.uuid4())}"
        cql.execute(f"CREATE USER {user} WITH PASSWORD '{pwd}' NOSUPERUSER")
        assert_one(cql, f"LIST ROLES of {user}", [user, False, True, {}])

        logger.debug("Verify PART 1: check commitlogs -------------")

        await grep_database_files(pwd, "commitlog", "**/*.log", False)
        await grep_database_files(user, "commitlog", "**/*.log", expect)

        salted_hash = None
        system_auth = None
        for ks in ['system', 'system_auth_v2', 'system_auth']:
            try:
                # We could have looked for any role/salted_hash pair, but we
                # already know a role "cassandra" exists (we just used it to
                # connect to CQL!), so let's just use that role.
                salted_hash = cql.execute(f"SELECT salted_hash FROM {ks}.roles WHERE role = '{user}'").one().salted_hash
                system_auth = ks
                break
            except:
                pass

        assert salted_hash is not None
        assert system_auth is not None
        await grep_database_files(salted_hash, "commitlog", "**/*.log", expect)

        rand_comment = f"comment_{str(uuid.uuid4())}"

        async with await create_ks(manager) as ks:
            async with new_test_table(manager, ks, "key text PRIMARY KEY, c1 text, c2 text") as table:
                cql.execute(f"ALTER TABLE {table} WITH comment = '{rand_comment}'")
                await grep_database_files(rand_comment, "commitlog/schema", "**/*.log", expect)
                # Note: original test did greping in sstables. This does no longer work
                # since all system tables are compressed, and thus binary greping will 
                # not work. We could do scylla sstable dump-data and grep in the json,
                # but this is somewhat pointless as this would, if it handles it, just
                # decrypt the info from the sstable, thus we can't really verify anything.
                # We could maybe check that the expected system tables are in fact encrypted,
                # though this is more a promise than guarantee... Also, the only tables
                # encrypted are paxos and batchlog -> pointless

    await verify_system_info(True) # not encrypted

    cfg = {"system_info_encryption": {
        "enabled": True, 
        "key_provider": "LocalFileSystemKeyProviderFactory"},
        "system_key_directory": os.path.join(tmpdir, "resources/system_keys")
        }

    for server in servers:
        await manager.server_update_config(server.server_id, config_options=cfg)
        await manager.server_restart(server.server_id)

    await manager.rolling_restart(servers)

    await verify_system_info(False) # should not see stuff now


async def test_system_encryption_reboot(manager: ScyllaClusterManager, tmpdir):
    """Tests SIGKILL restart of encrypted node"""
    async def restart(manager: ScyllaClusterManager, servers: list[ServerInfo], table_names: list[str]):
        # pylint: disable=unused-argument
        for s in servers:
            await manager.server_stop(s.server_id, convict=False)
            await manager.server_start(s.server_id)

    options = {"commitlog_sync": "batch",
               "system_info_encryption": {
                   "enabled": True,
                   "key_provider": "LocalFileSystemKeyProviderFactory"
                   }
                   }

    async with LocalFileSystemKeyProviderFactory(tmpdir) as kp:
        await _smoke_test(manager, key_provider=kp, 
                          ciphers={"AES/CBC/PKCS5Padding": [128]},
                          options=options,
                          restart=restart)

def millis_now():
    return time.perf_counter_ns() / 1_000_000

@pytest.mark.parametrize(
        "provider", [ KeyProvider.kms, KeyProvider.gcp, KeyProvider.azure ]
)
@pytest.mark.skip_mode(mode=DEBUG_MODES, reason='slow test with timing dependencies')
async def test_keycache_does_not_expire_when_active(manager, tmpdir, suite_log_dir, scylla_binary, provider):
    """
    Reproduces (and tests) two nightmare conditions for remote key providers:
    1.) Keys only being used on shards != 0. This can, if cache values are
        bad, lead to values expiring on shard 0 while they are still actually
        being used on other shards. 
    2.) Even if we also refresh shard 0 (at least via other shard refresh calls), 
        if a remote is unaccessible long enough, we will fail all reload attempts 
        and eventually reach expiration anyway. Calls from other shards will not
        help this, as reload counts into expiry
        Now, a key actually being in use will become expired even though we
        have not verified it should be discarded.

    This is a rather slow test, and has some timing dependency, i.e. if we stall
    such that the assumed cache reloads do not happen, it can fail "spuriourly",
    however, upping the timeouts makes the test slower overall.
    """

    # Doing this only with fake GCP kms. Because we control this mock, and can make
    # it do server errors on demand.
    # TODO: add error proxies for AWS and Azure.
    async with make_key_provider_factory(provider, tmpdir, suite_log_dir, scylla_binary) as key_provider:
        async with key_provider.create_error_state_object() as error_state:
            options = error_state.configuration_parameters()
            additional_options = error_state.additional_cf_options()
            expiry_ms = 2000
            refresh_ms = 200 # set real low to reduce risk of flakyness.
            epconf = error_state.get_endpoint_config(options)
            epconf['key_cache_expiry'] = f'{expiry_ms}ms'
            epconf['key_cache_refresh'] = f'{refresh_ms}ms'

            cmd = ['--smp', '2']
            await manager.servers_add(servers_num = 1, config=options, cmdline=cmd)
            cql = manager.get_cql()

            logger.info("Creating shard 1 keys")
            all_keys_for_shard = _generate_key_for_shard(scylla_binary, 2, 1)
            keys = list(itertools.islice(all_keys_for_shard, 20))

            logger.info("Creating shard 0 keys")
            all_keys_for_shard0 = _generate_key_for_shard(scylla_binary, 2, 0)
            keys0 = list(itertools.islice(all_keys_for_shard0, 10))

            async with new_test_keyspace(manager,opts="with replication = {'class': 'SimpleStrategy', 'replication_factor': 1} AND tablets = {'enabled': false}") as ks:
                columns = "key int PRIMARY KEY, c1 text, c2 text"
                async with await create_encrypted_cf(manager, ks, additional_options=additional_options, columns=columns) as table_name:
                    logger.info("Doing shard 0 workload")
                    await do_prepare_write_workload(cql, table_name, keys)
                    cql.execute(f"SELECT * from {table_name}") # make sure reading keys are made available

                    logger.info("Putting key server into error mode")
                    error_state.enter_error_state(True)

                    start = millis_now()
                    last = start
                    now = last

                    try:
                        # Do inserts on shard 1 until we've exceeded the expiry enough.
                        # Depending on cache values, this could have let the keys on 
                        # shard 0 expire since remote is not reachable
                        while (now - start) < (expiry_ms*8):
                            await asyncio.sleep(expiry_ms / float(2000))
                            now = millis_now()
                            try:
                                logger.info("Doing shard 1 workload")
                                await do_prepare_write_workload(cql, table_name, keys)
                            except Exception as e:
                                if (now - last) > expiry_ms:
                                    logger.info("Sleep took too long. Expiry might have happened: %s", e)
                                    break
                                raise e
                            last = millis_now()

                        # If we kept alive cached keys even though we get server
                        # errors, this should be fine to do. If not, this will fail
                        # with a somewhat lengthy timeout for the flush call
                        logger.info("Doing shard 0 workload")
                        await do_prepare_write_workload(cql, table_name, keys0)
                    finally:
                        error_state.enter_error_state(False)

