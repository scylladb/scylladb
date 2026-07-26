#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import asyncio
import json
import logging
import os
import re
import subprocess
import tempfile
import time
import uuid

from cassandra.query import SimpleStatement, ConsistencyLevel

from test.cluster.util import wait_for_cql_and_get_hosts, wait_for_no_pending_topology_transition, wait_for_no_running_compactions
from test.pylib.manager_client import ManagerClient

logger = logging.getLogger(__name__)


def verify_uuid_sstable_generation(generation: str) -> None:
    match = re.fullmatch(r"([0-9a-z]{4})_([0-9a-z]{4})_([0-9a-z]{5})([0-9a-z]{13})", generation)
    assert match is not None
    assert int(match.group(2), 36) < 24 * 60 * 60
    assert int(match.group(3), 36) < 10_000_000
    assert int(match.group(4), 36) < 1 << 64


def encode_base36(value: int) -> str:
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    if value == 0:
        return "0"
    result = ""
    while value:
        value, digit = divmod(value, 36)
        result = digits[digit] + result
    return result


def encode_uuid_sstable_generation(generation_uuid: uuid.UUID) -> str:
    seconds, decimicroseconds = divmod(generation_uuid.time, 10_000_000)
    days, seconds = divmod(seconds, 24 * 60 * 60)
    lsb = generation_uuid.int & ((1 << 64) - 1)
    generation = f"{encode_base36(days):0>4}_{encode_base36(seconds):0>4}_{encode_base36(decimicroseconds):0>5}{encode_base36(lsb):0>13}"
    verify_uuid_sstable_generation(generation)
    return generation


def parse_node_reference(reference: str) -> tuple[str, str]:
    parts = reference.split("/")
    assert len(parts) == 3
    assert parts[0] == "nodes"
    host_id = str(uuid.UUID(parts[1]))
    generation = parts[2]
    verify_uuid_sstable_generation(generation)
    return host_id, generation


async def get_object_storage_refs(manager: ManagerClient, object_storage, server):
    params = {
        "endpoint": object_storage.address,
        "bucket": object_storage.bucket_name,
    }
    sstables = await manager.api.client.get_json("/storage_service/object_storage/sstables", host=server.ip_addr, params=params)
    assert sstables
    refs = {}
    for sstable in sstables:
        sstable_id = str(uuid.UUID(sstable["sstable_id"]))
        references = sstable.get("references", [])
        assert sstable["num_references"] == len(references)
        if not references:
            logger.info("Ignoring object-storage SSTable %s with no references", sstable_id)
            continue
        for reference in references:
            host_id, generation = parse_node_reference(reference)
            key = (sstable_id, host_id, generation)
            assert key not in refs
            refs[key] = reference
    return sstables, refs


async def get_node_registry_refs(cql, host, table_id: str):
    rows = await cql.run_async("SELECT table_id, node_owner, generation, sstable_id, status FROM system.sstables", host=host)
    refs = {}
    for row in rows:
        if str(row.table_id) != table_id:
            continue
        if row.status != "sealed":
            continue
        sstable_id = str(row.sstable_id if row.sstable_id is not None else row.generation)
        generation = encode_uuid_sstable_generation(row.generation)
        key = (sstable_id, str(row.node_owner), generation)
        assert key not in refs
        refs[key] = row.status
    return refs


async def verify_object_storage_namespace(manager: ManagerClient, object_storage, cql, server, live_servers, ks: str, table: str = "test") -> None:
    """Verify object-storage namespace layout and reference counts through REST API and system.sstables."""
    await manager.disable_tablet_balancing()
    await wait_for_no_pending_topology_transition(manager, time.time() + 120)
    try:
        for node in live_servers:
            await manager.api.disable_autocompaction(node.ip_addr, ks, table)

        await wait_for_no_running_compactions(manager, live_servers, time.time() + 120)

        for node in live_servers:
            await manager.api.flush_keyspace(node.ip_addr, ks)

        table_id = str(await manager.get_table_id(ks, table))
        live_hosts = await wait_for_cql_and_get_hosts(cql, live_servers, time.time() + 30)

        async def get_registry_refs():
            registry_refs = {}
            for refs in await asyncio.gather(*(get_node_registry_refs(cql, host, table_id) for host in live_hosts)):
                for key, status in refs.items():
                    assert key not in registry_refs
                    registry_refs[key] = status
            return registry_refs

        sstables, object_storage_refs = await get_object_storage_refs(manager, object_storage, server)
        registry_refs = await get_registry_refs()
        assert object_storage_refs.keys() == registry_refs.keys(), f"Only in object_storage: {object_storage_refs.keys() - registry_refs.keys()}\nOnly in registry: {registry_refs.keys() - object_storage_refs.keys()}"
        logger.info("Verified %d object-storage SSTables and %d references", len(sstables), len(object_storage_refs))
    finally:
        for node in live_servers:
            await manager.api.enable_autocompaction(node.ip_addr, ks, table)
        await manager.enable_tablet_balancing()


async def get_sealed_sstable_registry_rows(cql, host, table_id):
    rows = await cql.run_async(
        SimpleStatement(
            "SELECT generation, sstable_id, version, format, status FROM system.sstables WHERE table_id = %s ALLOW FILTERING",
            consistency_level=ConsistencyLevel.ONE),
        parameters=[table_id], host=host)
    return [row for row in rows if row.status == "sealed"]


async def dump_object_storage_sstable_metadata(manager: ManagerClient, object_storage, server, ks: str, table: str, rows, schema_cql: str):
    """Dump Scylla metadata for object-storage SSTables through scylla-sstable."""
    scylla_path = await manager.server_get_exe(server.server_id)
    workdir = await manager.server_get_workdir(server.server_id)
    scylla_yaml = os.path.join(workdir, "conf", "scylla.yaml")

    with tempfile.TemporaryDirectory() as tmpdir:
        schema_file = os.path.join(tmpdir, "schema.cql")
        with open(schema_file, "w") as f:
            f.write(schema_cql)

        data_fqns = [
            f"{object_storage.type.lower()}://{object_storage.bucket_name}/sstables/{row.sstable_id}/Data.db"
            for row in rows
        ]
        out = subprocess.check_output([
            scylla_path,
            "sstable",
            "dump-scylla-metadata",
            "--scylla-yaml-file", scylla_yaml,
            "--schema-file", schema_file,
            *data_fqns,
        ])
        metadata = json.loads(out)["sstables"]

    return metadata
