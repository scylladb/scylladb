#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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

from test.pylib.manager_client import ManagerClient, ServerInfo
from test.pylib.util import wait_for_cql_and_get_hosts
from test.pylib.tablets import get_all_tablet_replicas

from test.cqlpy import nodetool

from test.pylib.encryption_provider import KeyProviderFactory, KeyProvider, make_key_provider_factory, KMSKeyProviderFactory, LocalFileSystemKeyProviderFactory
from test.cluster.util import new_test_keyspace, new_test_table
from test.cluster.dtest.tools.assertions import assert_one

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

async def test_file_streaming_respects_encryption(manager: ManagerClient, workdir):
    # pylint: disable=missing-function-docstring
    cfg = {
        'tablets_mode_for_new_keyspaces': 'enabled',
    }

    cmdline = ['--smp=1']
    servers = []
    servers.append(await manager.server_add(config=cfg, cmdline=cmdline))
    await manager.api.disable_tablet_balancing(servers[0].ip_addr)

    cql = manager.cql
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
    cql.execute("CREATE KEYSPACE ks WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1};")
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

async def create_ks(manager: ManagerClient, replication_factor: int=1):
    """create test keyspace"""
    return new_test_keyspace(manager,
                             opts="with replication = {'class': 'NetworkTopologyStrategy', "
                             f"'replication_factor': {replication_factor}}}"
                             )


async def create_encrypted_cf(manager: ManagerClient, ks: str,
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

async def prepare_write_workload(cql: CassandraSession, table_name, flush=True, n: int = None):
    """write some data"""
    keys = list(range(n if n else 100))
    c1_values = ['value1']
    c2_values = ['value2']

    statement = cql.prepare(f"INSERT INTO {table_name} (key, c1, c2) VALUES (?, ?, ?)")
    statement.consistency_level = ConsistencyLevel.ALL

    await asyncio.gather(*[cql.run_async(statement, params) for params in
                           list(map(lambda x, y, z: [f"k{x}", y, z], keys,
                                    itertools.cycle(c1_values),
                                    itertools.cycle(c2_values)))]
                                    )

    if flush:
        nodetool.flush(cql, table_name)

async def read_verify_workload(cql: CassandraSession, table_name: str, expected_len: int = 100):
    """check written data"""
    rows = list(cql.execute(f"SELECT c1, c2 FROM {table_name}"))
    assert len(rows) == expected_len

async def _smoke_test(manager: ManagerClient, key_provider: KeyProviderFactory,
                      ciphers: dict[str, list[int]], compression: str = None,
                      exception_handler: Callable[[Exception,str,str], None] = None,
                      options: dict = {},
                      num_servers: int = 1,
                      restart: Callable[[ManagerClient, list[ServerInfo], list[str]], Coroutine[None, None, None]] = None):
    """helper to create cluster, cfs, data and verify it after restart"""
    cfg = options | key_provider.configuration_parameters()

    servers: list[ServerInfo] = await manager.servers_add(servers_num = num_servers, config=cfg)
    cql = manager.cql
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    async with await create_ks(manager) as ks:
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
                    await prepare_write_workload(cql, table_name=table_name)
                    cfs.append(table_name)
                except Exception as e:
                    if exception_handler:
                        exception_handler(e, cipher_algorithm, secret_key_strength)
                        continue
                    raise e
            # restart the cluster
            if restart:
                await restart(manager, servers, cfs)
                await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)
            else:
                await manager.rolling_restart(servers)
            for table_name in cfs:
                await read_verify_workload(cql, table_name=table_name)

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
async def test_encryption_table_compression(manager, tmpdir, compression):
    """Test compression + ear"""
    logger.debug("---- Test with compression: %s -----", compression)
    async with make_key_provider_factory(KeyProvider.local, tmpdir) as key_provider:
        await _smoke_test(manager, key_provider,
                          ciphers={"AES/CBC/PKCS5Padding": [128]},
                          compression=compression)


async def test_reboot(manager, key_provider):
    """Tests SIGKILL restart of 3-node cluster"""
    async def restart(manager: ManagerClient, servers: list[ServerInfo], table_names: list[str]):
        # pylint: disable=unused-argument
        for s in servers:
            await manager.server_stop(s.server_id)
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

async def get_sstable_metadata(manager: ManagerClient, server: ServerInfo, keyspace: str, column_family: str):
    """Load scylla metadata component sstables for server and cf"""
    scylla_path = await manager.server_get_exe(server.server_id)
    node_workdir = await manager.server_get_workdir(server.server_id)
    scylla_sstables = get_sstables(node_workdir, keyspace, column_family, 'Scylla')
    res = subprocess.check_output([scylla_path, "sstable", "dump-scylla-metadata",
                                   "--scylla-yaml-file",
                                   os.path.join(node_workdir, "conf", "scylla.yaml"),
                                   "--sstables"] + scylla_sstables)
    scylla_metadata = json.loads(res.decode('utf-8', 'ignore'))
    return scylla_metadata

async def validate_sstables_encryption(manager: ManagerClient, server: ServerInfo, table_name: str, encrypted:bool, expected_data=None):
    """Verify sstables for table encrypted or not"""
    keyspace, column_family  = table_name.split(".")
    scylla_path = await manager.server_get_exe(server.server_id)
    with nodetool.no_autocompaction_context(manager.cql, table_name):
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
                                           "--output-format", "json", "--sstables"] + sstables)
            scylla_data = json.loads(res.decode('utf-8', 'ignore'))
            actual_data = [list(r.values()) for r in scylla_data]
            assert actual_data == expected_data


async def test_alter(manager, key_provider):
    """Tests altering encrypted CF:s and verify sstable data"""
    async def restart(manager: ManagerClient, servers: list[ServerInfo], table_names: list[str]):
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

        await read_verify_workload(cql, table_name=table_names[0])
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

async def test_per_table_master_key(manager: ManagerClient, tmpdir):
    """Test per table KMS master key"""
    class MultiAliasKMSProvider (KMSKeyProviderFactory):
        """Special KMS using different master keys for each table"""
        def __init__(self, tmpdir):
            super(MultiAliasKMSProvider, self).__init__(tmpdir)
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

    async with MultiAliasKMSProvider(tmpdir) as kp:
        async def restart(manager: ManagerClient, servers: list[ServerInfo],
                          table_names: list[str]):
            await manager.rolling_restart(servers)
            i = 0
            for table_name in table_names:
                keyspace, column_family  = table_name.split(".")
                await validate_sstables_encryption(manager, servers[0],
                                                   table_name, True)
                with nodetool.no_autocompaction_context(manager.cql, table_name):
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


async def test_non_existant_table_master_key(manager: ManagerClient, tmpdir):
    """Test we fail properly if using a non-existant master key"""
    class NoSuchKeyKMSProvider (KMSKeyProviderFactory):
        """Special KMS using nonexisting master key"""
        def additional_cf_options(self):
            return super().additional_cf_options() | {"master_key": "alias/NoSuchKey"}

    async with NoSuchKeyKMSProvider(tmpdir) as kp:
        with pytest.raises(Exception):
            await _smoke_test(manager, kp, ciphers={"AES/CBC/PKCS5Padding": [128]})

async def test_system_auth_encryption(manager: ManagerClient, tmpdir):
    cfg = {"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator", 
               "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer"}

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
                with open(file_path, 'rb') as f:
                    data = f.read()
                    if pbytes in data:
                        pattern_found_counter += 1
                        logger.debug("Pattern '%s' found in %s", pattern, file_path)

        if expect:
            assert pattern_found_counter > 0
        else:
            assert pattern_found_counter == 0

    async def verify_system_info(expect: bool):
        user = f"user_{str(uuid.uuid4())}"
        pwd = f"pwd_{str(uuid.uuid4())}"
        cql.execute(f"CREATE USER {user} WITH PASSWORD '{pwd}' NOSUPERUSER")
        assert_one(cql, f"LIST ROLES of {user}", [user, False, True, {}])

        logger.debug("Verify PART 1: check commitlogs -------------")

        grep_database_files(pwd, "commitlog", "**/*.log", expect)
        grep_database_files(user, "commitlog", "**/*.log", True)

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
        grep_database_files(salted_hash, "commitlog", "**/*.log", expect)

        rand_comment = f"comment_{str(uuid.uuid4())}"

        async with await create_ks(manager) as ks:
            async with await new_test_table(cql, ks, "key text PRIMARY KEY, c1 text, c2 text") as table:
                cql.execute(f"ALTER TABLE {table} WITH comment = '{rand_comment}'")
                grep_database_files(rand_comment, "commitlog/schema", "**/*.log", expect)
                nodetool.flush_all(cql)

                logger.debug("Verify PART 2: check sstable files -------------\n`system_info_encryption` won't encrypt sstable files on disk")
                logger.debug("GREP_DB_FILES: Check PM key user in sstable file ....")
                grep_database_files(user, f"data/{system_auth}/", "**/*-Data.db", expect=True)
                logger.debug("GREP_DB_FILES: Check original password in commitlogs .... Original password should never be saved")
                grep_database_files(pwd, f"data/{system_auth}/", "**/*-Data.db", expect=False)
                logger.debug("GREP_DB_FILES: Check salted_hash of password in sstable file ....")
                grep_database_files(salted_hash, f"data/{system_auth}/", "**/*-Data.db", expect=False)
                logger.debug("GREP_DB_FILES: Check table comment in sstable file ....")
                grep_database_files(rand_comment, "data/system_schema/", "**/*-Data.db", expect=True)

    verify_system_info(True) # not encrypted

    cfg = {"system_info_encryption": {
        "enabled": True, 
        "key_provider": "LocalFileSystemKeyProviderFactory"}
        }

    for server in servers:
        manager.server_update_config(server.server_id, config_options=cfg)

    await manager.rolling_restart(servers)

    verify_system_info(False) # should not see stuff now


async def test_system_encryption_reboot(manager: ManagerClient, tmpdir):
    """Tests SIGKILL restart of encrypted node"""
    async def restart(manager: ManagerClient, servers: list[ServerInfo], table_names: list[str]):
        # pylint: disable=unused-argument
        for s in servers:
            await manager.server_stop(s.server_id)
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
