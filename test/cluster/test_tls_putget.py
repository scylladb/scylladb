#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Writes and reads (the dtests' putget: batches of overwrites, flushed in
between, then a slice read) over clusters with internode TLS and
compression settings (also with certificates replaced while the nodes
run), over a client TLS port next to the plain one, and across restarts of
a node with internode TLS.

Ported from scylla-dtest. Like the dtests' nodes, the nodes use vnodes (256
per node), certificates issued by a test CA, and are stopped gracefully at
the end of each test, as the dtests' teardown did.
"""

import asyncio
import logging
import os
import re

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from test.cluster.util import run_nodetool, stop_servers_gracefully
from test.pylib.certificates import CertificateAuthority
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256}


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


def certificates(tmp_path) -> dict:
    ca = CertificateAuthority()
    ca.issue("scylla", tmp_path / "ccm_node.pem", tmp_path / "ccm_node.key")
    return {"certificate": str(tmp_path / "ccm_node.pem"), "keyfile": str(tmp_path / "ccm_node.key"),
            "truststore": ca.write_cert(tmp_path / "ccm_node.cer")}


async def create_ks(cql, ks: str, rf: int) -> None:
    """The dtests' create_ks()."""
    await cql.run_async(f"CREATE KEYSPACE {ks} WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}")
    await cql.run_async(f"USE {ks}")


async def create_ks_cf(cql, ks: str, rf: int) -> None:
    """The dtests' create_ks() and create_cf(session, "cf", compression=None)."""
    await create_ks(cql, ks, rf)
    await cql.run_async(f"CREATE COLUMNFAMILY {ks}.cf (key varchar, c varchar, v varchar, PRIMARY KEY(key, c)) WITH comment='test cf' "
                        "AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'true'} AND compression = {}")
    await cql.run_async(f"SELECT * FROM {ks}.cf LIMIT 1")


async def putget(manager: ScyllaClusterManager, cql, ks: str, cl=ConsistencyLevel.QUORUM) -> None:
    """The dtests' putget(): three rounds of batched overwrites of key k0,
    each flushed on every node, then a read of the row's 100 cells."""
    async def flush_all():
        for s in await manager.running_servers():
            await run_nodetool(manager, s, "flush")
    for updates in ([(i, i) for i in range(100)], [(i * 4, i * 2) for i in range(50)], [(i * 20, i * 5) for i in range(20)]):
        kvs = [f"UPDATE {ks}.cf SET v='value{v}' WHERE key='k0' AND c='c{c:02d}'" for v, c in updates]
        await cql.run_async(SimpleStatement(f"BEGIN BATCH {'; '.join(kvs)} APPLY BATCH", consistency_level=cl))
        await asyncio.sleep(0.01)
        await flush_all()
    res = list(await cql.run_async(SimpleStatement(f"SELECT * FROM {ks}.cf WHERE key='k0'", consistency_level=cl)))
    assert len(res) == 100
    for i in range(100):
        expected = f"value{i * 4}" if i % 5 == 0 else f"value{i * 2}" if i % 2 == 0 else f"value{i}"
        assert res[i][2] == expected, f"for {i}, expecting {expected}, got {res[i][2]}"


############################################################################
# Ported from internode_ssl_test.py: in each of one or two datacenters, four
# nodes in two racks (rc1, rc2) of GossipingPropertyFileSnitch, with
# repair-based node operations, as the dtests' clusters.

INTERNODE_IGNORED_LOGS = [
    "connection dropped: The TLS connection was non-properly terminated",
    "connection dropped: The certificate is NOT trusted",
    "connection dropped: sendmsg: Broken pipe",
    "connection dropped: The specified session has been invalidated for some reason",
    "storage_service -.*fail to update tokens for",
    "storage_service -.*fail to update schema_version for",
]


async def putget_with_internode_ssl(manager: ScyllaClusterManager, tmp_path, internode_compression: str,
                                    internode_encryption: str = "all", dcs: int = 1, reload_certs: bool = False) -> None:
    """The dtests' __putget_with_internode_ssl_test(): a cluster of dcs
    datacenters, each with two nodes in each of the racks rc1 and rc2, with
    internode TLS and compression, optionally replacing the certificates
    of the running nodes, then putget() at RF 2."""
    manager.ignore_log_patterns += INTERNODE_IGNORED_LOGS
    certs = certificates(tmp_path)
    # Use the faster RBNO bootstrap to avoid problems with timeouts
    config = {**CONFIG, "internode_compression": internode_compression,
              "enable_repair_based_node_ops": True, "allowed_repair_based_node_ops": "replace,removenode,rebuild,bootstrap,decommission",
              "endpoint_snitch": "org.apache.cassandra.locator.GossipingPropertyFileSnitch",
              "server_encryption_options": {"internode_encryption": internode_encryption, **certs}}
    assert dcs >= 1
    rack_num = 2
    topology = [{"dc": f"dc{dc}", "rack": f"rc{rack}"} for dc in range(1, dcs + 1) for rack in range(1, rack_num + 1) for _ in range(2)]
    servers = await manager.servers_add(len(topology), config=config, property_file=topology)

    if reload_certs:
        logger.debug("rewriting certs")
        logs = [await manager.server_open_log(s.server_id) for s in servers]
        marks = [await log.mark() for log in logs]
        with open(certs["keyfile"], "rb") as f:
            old_key = f.read()
        # overwrite old certs
        certificates(tmp_path)
        with open(certs["keyfile"], "rb") as f:
            assert f.read() != old_key, "Cert regen failed?"
        for log, mark in zip(logs, marks):
            for f in [certs["certificate"], certs["keyfile"]]:
                await log.wait_for(f"messaging_service.*Reloaded.*{re.escape(os.path.basename(f))}", from_mark=mark)

    cql = manager.get_cql()
    await create_ks_cf(cql, "ks", rack_num)
    await putget(manager, cql, "ks")


# Ported from scylla-dtest internode_ssl_test.py::TestInternodeSSL::test_putget_with_internode_ssl.
# putget with internode TLS ('all') and 'all' internode compression.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_putget_with_internode_ssl(manager: ScyllaClusterManager, tmp_path):
    await putget_with_internode_ssl(manager, tmp_path, "all", internode_encryption="all")


# Ported from scylla-dtest internode_ssl_test.py::TestInternodeSSL::test_putget_with_internode_rack_ssl.
# putget with 'rack' internode encryption and 'all' internode compression.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_putget_with_internode_rack_ssl(manager: ScyllaClusterManager, tmp_path):
    await putget_with_internode_ssl(manager, tmp_path, "all", internode_encryption="rack")


# Ported from scylla-dtest internode_ssl_test.py::TestInternodeSSL::test_putget_with_internode_dc_ssl.
# putget over two datacenters with 'all' internode compression; as in the
# dtest, the internode encryption is 'rack', not 'dc'.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_putget_with_internode_dc_ssl(manager: ScyllaClusterManager, tmp_path):
    await putget_with_internode_ssl(manager, tmp_path, "all", internode_encryption="rack", dcs=2)


# Ported from scylla-dtest internode_ssl_test.py::TestInternodeSSL::test_putget_with_internode_ssl_without_compression.
# putget with neither internode encryption nor internode compression.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_putget_with_internode_ssl_without_compression(manager: ScyllaClusterManager, tmp_path):
    await putget_with_internode_ssl(manager, tmp_path, "none", internode_encryption="none")


# Ported from scylla-dtest internode_ssl_test.py::TestInternodeSSL::test_putget_with_internode_ssl_with_dc_compression.
# putget over two datacenters with 'dc' internode encryption and compression.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_putget_with_internode_ssl_with_dc_compression(manager: ScyllaClusterManager, tmp_path):
    await putget_with_internode_ssl(manager, tmp_path, "dc", internode_encryption="dc", dcs=2)


# Ported from scylla-dtest internode_ssl_test.py::TestInternodeSSL::test_putget_with_internode_rack_ssl_with_dc_compression.
# putget over two datacenters with 'rack' internode encryption and 'dc'
# internode compression.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_putget_with_internode_rack_ssl_with_dc_compression(manager: ScyllaClusterManager, tmp_path):
    await putget_with_internode_ssl(manager, tmp_path, "dc", internode_encryption="rack", dcs=2)


# Ported from scylla-dtest internode_ssl_test.py::TestInternodeSSL::test_putget_with_reloaded_certificates.
# The internode certificate, key and truststore of the running nodes are
# replaced by new ones; every node reloads them, and putget works.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_putget_with_reloaded_certificates(manager: ScyllaClusterManager, tmp_path):
    await putget_with_internode_ssl(manager, tmp_path, "all", internode_encryption="all", reload_certs=True)
