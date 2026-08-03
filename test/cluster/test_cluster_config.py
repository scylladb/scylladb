# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

import asyncio
import logging
import time
from pathlib import Path

import pytest
from cassandra.protocol import InvalidRequest, SyntaxException  # type: ignore # pylint: disable=no-name-in-module

from test.cluster.util import reconnect_driver
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import read_barrier
from test.pylib.scylla_cluster import ScyllaVersionDescription, get_current_version_description
from test.pylib.util import wait_for, wait_for_feature


logger = logging.getLogger(__name__)


def get_old_scylla_version(scylla_binary: Path, fallback: ScyllaVersionDescription) -> tuple[Path, ScyllaVersionDescription]:
    scylla_binary = Path(scylla_binary)
    old_scylla_binary = scylla_binary.with_name("scylla.no.cluster.config")
    if old_scylla_binary.exists():
        return old_scylla_binary, ScyllaVersionDescription(path=str(old_scylla_binary), config={}, argv=[])

    return Path(fallback.path), fallback


CLUSTER_CONFIGS_QUERY = "SELECT configs FROM system_schema.scylla_clusters"
KEYSPACE_CONFIGS_QUERY = "SELECT configs FROM system_schema.scylla_keyspaces WHERE keyspace_name = %s"
TABLE_CONFIGS_QUERY = "SELECT configs FROM system_schema.scylla_tables WHERE keyspace_name = %s AND table_name = %s"


async def wait_for_config_map_value(cql, host, query: str, params: list[str], config_name: str, expected_value: str | None) -> None:
    async def configs_map_value_equal():
        rows = await cql.run_async(query, params, host=host)
        configs = (rows[0].configs if rows else None) or {}
        value = configs.get(config_name)
        if value == expected_value:
            return True
        logger.info("Observed config map value for %s: %s on host=%s expected=%s", config_name, value, host, expected_value)
        return None

    # 120s to match the schema-agreement waits below: debug and sanitize builds
    # propagate schema-backed config much more slowly than dev builds.
    await wait_for(configs_map_value_equal, deadline=time.time() + 120)


async def wait_for_config_map_value_on_hosts(cql, hosts, query: str, params: list[str], config_name: str, expected_value: str | None) -> None:
    await asyncio.gather(*[
        wait_for_config_map_value(cql, host, query, params, config_name, expected_value)
        for host in hosts
    ])


async def get_schema_version(manager: ManagerClient, server) -> str:
    version = await manager.api.client.get_json("/storage_service/schema_version", host=server.ip_addr)
    assert isinstance(version, str) and len(version) > 0
    return version


async def wait_for_schema_agreement(manager: ManagerClient, servers, deadline: float) -> str:
    """Wait until every given server reports the same schema version and return it.

    Used as an end-to-end liveness check for a mixed-version cluster (a cluster-config-aware
    node joined to cluster-config-unaware nodes before the CLUSTER_CONFIG_REGISTRY_V0
    feature is enabled): the cluster must converge on a single schema version. Note that
    with GROUP0_SCHEMA_VERSIONING the reported version is a persisted group0 state-id rather
    than a locally computed digest, so this guards overall schema-propagation liveness, not
    the digest gating in isolation (see the C++ tests for that).
    """
    async def schema_versions_agree():
        for srv in servers:
            await read_barrier(manager.api, srv.ip_addr)
        versions = await asyncio.gather(*(get_schema_version(manager, srv) for srv in servers))
        if len(set(versions)) == 1:
            return versions[0]
        logger.info("Schema versions do not yet agree: %s", dict(zip((s.server_id for s in servers), versions)))
        return None

    return await wait_for(schema_versions_agree, deadline)


@pytest.mark.asyncio
async def test_cluster_config_auto_repair_schema_scope_persistence(manager: ManagerClient) -> None:
    servers = [
        await manager.server_add(cmdline=["--smp", "1"], property_file={"dc": "dc1", "rack": "rack1"}),
        await manager.server_add(cmdline=["--smp", "1"], property_file={"dc": "dc1", "rack": "rack2"}),
        await manager.server_add(cmdline=["--smp", "1"], property_file={"dc": "dc2", "rack": "rack1"}),
    ]
    await manager.driver_connect()
    cql, hosts = await manager.get_ready_cql(servers)

    await cql.run_async("CREATE KEYSPACE ks_cfg WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE ks_cfg.tbl (pk int PRIMARY KEY, v int)")

    await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = true")
    await wait_for_config_map_value_on_hosts(cql, hosts, CLUSTER_CONFIGS_QUERY, [], "auto_repair_enabled", "true")

    await cql.run_async("ALTER KEYSPACE ks_cfg WITH auto_repair_enabled = false")
    await wait_for_config_map_value_on_hosts(cql, hosts, KEYSPACE_CONFIGS_QUERY, ["ks_cfg"], "auto_repair_enabled", "false")

    await cql.run_async("ALTER TABLE ks_cfg.tbl WITH auto_repair_enabled = true")
    await wait_for_config_map_value_on_hosts(cql, hosts, TABLE_CONFIGS_QUERY, ["ks_cfg", "tbl"], "auto_repair_enabled", "true")

    await cql.run_async("ALTER TABLE ks_cfg.tbl WITH auto_repair_enabled = null")
    await wait_for_config_map_value_on_hosts(cql, hosts, TABLE_CONFIGS_QUERY, ["ks_cfg", "tbl"], "auto_repair_enabled", None)

    await cql.run_async("ALTER KEYSPACE ks_cfg WITH auto_repair_enabled = null")
    await wait_for_config_map_value_on_hosts(cql, hosts, KEYSPACE_CONFIGS_QUERY, ["ks_cfg"], "auto_repair_enabled", None)

    await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = null")
    await wait_for_config_map_value_on_hosts(cql, hosts, CLUSTER_CONFIGS_QUERY, [], "auto_repair_enabled", None)


@pytest.mark.asyncio
async def test_rack_name_is_not_globally_unique(manager: ManagerClient) -> None:
    """Regression guard: rack names are not required to be globally unique.

    Two nodes in different datacenters are allowed to use the same rack name.
    If global uniqueness is ever enforced, the second server_add should fail.
    """
    servers = [
        await manager.server_add(cmdline=["--smp", "1"], property_file={"dc": "dc1", "rack": "rack1"}),
        await manager.server_add(cmdline=["--smp", "1"], property_file={"dc": "dc2", "rack": "rack1"}),
    ]
    await manager.driver_connect()
    _, hosts = await manager.get_ready_cql(servers)

    assert len(hosts) == 2
    assert {server.datacenter for server in servers} == {"dc1", "dc2"}
    assert {server.rack for server in servers} == {"rack1"}


@pytest.mark.asyncio
async def test_cluster_config_auto_repair_survives_restart_and_join(manager: ManagerClient) -> None:
    servers = [
        await manager.server_add(cmdline=["--smp", "1"], property_file={"dc": "dc1", "rack": "rack1"}),
        await manager.server_add(cmdline=["--smp", "1"], property_file={"dc": "dc1", "rack": "rack2"}),
    ]
    await manager.driver_connect()
    cql, hosts = await manager.get_ready_cql(servers)

    await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = true")
    await wait_for_config_map_value_on_hosts(cql, hosts, CLUSTER_CONFIGS_QUERY, [], "auto_repair_enabled", "true")

    await manager.server_restart(servers[1].server_id, wait_others=1)
    await manager.driver_connect()
    cql, hosts = await manager.get_ready_cql(servers)
    await wait_for_config_map_value_on_hosts(cql, hosts, CLUSTER_CONFIGS_QUERY, [], "auto_repair_enabled", "true")

    new_server = await manager.server_add(cmdline=["--smp", "1"], property_file={"dc": "dc2", "rack": "rack1"})
    servers.append(new_server)
    await manager.driver_connect()
    cql, hosts = await manager.get_ready_cql(servers)
    await wait_for_config_map_value_on_hosts(cql, hosts, CLUSTER_CONFIGS_QUERY, [], "auto_repair_enabled", "true")

    await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = null")
    await wait_for_config_map_value_on_hosts(cql, hosts, CLUSTER_CONFIGS_QUERY, [], "auto_repair_enabled", None)


# Regression test: finalizing a vnodes-to-tablets migration rebuilds keyspace_metadata
# field-by-field in topology_coordinator::finalize_migration(). If it drops config_options,
# make_create_keyspace_mutations() emits a collection tombstone for the empty map and every
# keyspace-scope override is silently erased.
@pytest.mark.asyncio
async def test_keyspace_config_survives_vnodes_to_tablets_migration(manager: ManagerClient) -> None:
    server = await manager.server_add(cmdline=["--smp", "1"], config={"num_tokens": 16})
    await manager.driver_connect()
    cql, hosts = await manager.get_ready_cql([server])

    await cql.run_async(
        "CREATE KEYSPACE ks_mig WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
        " AND tablets = {'enabled': false}")
    await cql.run_async("CREATE TABLE ks_mig.t (pk int PRIMARY KEY)")

    await cql.run_async("ALTER KEYSPACE ks_mig WITH auto_repair_enabled = false")
    await wait_for_config_map_value_on_hosts(cql, hosts, KEYSPACE_CONFIGS_QUERY, ["ks_mig"], "auto_repair_enabled", "false")

    await manager.api.create_vnode_tablet_migration(server.ip_addr, "ks_mig")
    await manager.api.upgrade_node_to_tablets(server.ip_addr)
    await manager.server_restart(server.server_id)
    await manager.driver_connect()
    cql, hosts = await manager.get_ready_cql([server])
    await manager.api.finalize_vnode_tablet_migration(server.ip_addr, "ks_mig")

    rows = await cql.run_async("SELECT initial_tablets, configs FROM system_schema.scylla_keyspaces WHERE keyspace_name = 'ks_mig'")
    assert len(rows) == 1 and rows[0].initial_tablets is not None, "keyspace still uses vnodes after finalization"
    assert (rows[0].configs or {}).get("auto_repair_enabled") == "false", \
        "keyspace-scope cluster-config override was lost by tablets migration finalization"


@pytest.mark.skip_bug(
    link="https://scylladb.atlassian.net/browse/SCYLLADB-3818",
    reason="An audited statement run just before a node is stopped for the rolling upgrade "
           "leaves a write response handler that never expires, pinning a stale "
           "token_metadata version and deadlocking barrier_and_drain, so the node added at "
           "the end never leaves bootstrap",
)
@pytest.mark.asyncio
async def test_mixed_version_upgrade_with_old_binary(
    manager: ManagerClient, scylla_binary: Path, scylla_2026_1: ScyllaVersionDescription,
) -> None:
    scylla_binary = Path(scylla_binary)
    _, old_scylla_version = get_old_scylla_version(scylla_binary, scylla_2026_1)
    current_version = get_current_version_description(str(scylla_binary))

    servers = await manager.servers_add(2, cmdline=["--smp", "1"], version=old_scylla_version)
    cql, host_list = await manager.get_ready_cql(servers)

    with pytest.raises((InvalidRequest, SyntaxException)):
        await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = true", host=host_list[0])

    await manager.server_change_version(servers[0].server_id, str(scylla_binary), current_version)
    cql = await reconnect_driver(manager)
    cql, ready_hosts = await manager.get_ready_cql(servers)

    with pytest.raises(InvalidRequest):
        await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = true", host=ready_hosts[0])

    await manager.server_change_version(servers[1].server_id, str(scylla_binary), current_version)
    cql = await reconnect_driver(manager)
    cql, ready_hosts = await manager.get_ready_cql(servers)

    await wait_for_feature("CLUSTER_CONFIG_REGISTRY_V0", cql, ready_hosts[0], time.time() + 60)
    await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = true", host=ready_hosts[0])
    await wait_for_config_map_value_on_hosts(cql, ready_hosts, CLUSTER_CONFIGS_QUERY, [], "auto_repair_enabled", "true")

    new_server = await manager.server_add(cmdline=["--smp", "1"])
    servers.append(new_server)
    cql = await reconnect_driver(manager)
    cql, ready_hosts = await manager.get_ready_cql(servers)
    await wait_for_config_map_value_on_hosts(cql, ready_hosts, CLUSTER_CONFIGS_QUERY, [], "auto_repair_enabled", "true")

    await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = null")
    await wait_for_config_map_value_on_hosts(cql, ready_hosts, CLUSTER_CONFIGS_QUERY, [], "auto_repair_enabled", None)


@pytest.mark.asyncio
async def test_mixed_version_downgrade_with_old_binary_is_rejected_after_feature_enablement(
    manager: ManagerClient, scylla_binary: Path, scylla_2026_1: ScyllaVersionDescription,
) -> None:
    scylla_binary = Path(scylla_binary)
    old_scylla_binary, old_scylla_version = get_old_scylla_version(scylla_binary, scylla_2026_1)

    # Use a sstable format that the old binary can still read: node 1 is going to be
    # downgraded below while carrying data (e.g. system_schema) written by the current
    # binary, and the current binary's default sstable_format ('mt', see
    # test/pylib/scylla_cluster.py) is newer than what 2026.1 understands.
    servers = await manager.servers_add(2, cmdline=["--smp", "1"], config={"sstable_format": "me"})
    cql, hosts = await manager.get_ready_cql(servers)

    await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = true", host=hosts[0])
    await wait_for_config_map_value_on_hosts(cql, hosts, CLUSTER_CONFIGS_QUERY, [], "auto_repair_enabled", "true")

    await asyncio.gather(*(wait_for_feature("CLUSTER_CONFIG_REGISTRY_V0", cql, host, time.time() + 60) for host in hosts))

    await manager.server_stop_gracefully(servers[0].server_id)
    await manager.server_switch_executable(servers[0].server_id, str(old_scylla_binary), old_scylla_version)
    # feature_service::check_features() rejects the first unsupported feature it finds while
    # iterating the cluster's persisted (sorted) feature set, which -- when falling back to the
    # real 2026.1 release rather than a purpose-built "no cluster config" binary -- may well be
    # some other feature enabled since 2026.1 (e.g. ALTERNATOR_STREAMS) rather than
    # CLUSTER_CONFIG_REGISTRY_V0 itself. Match the shared "was previously enabled in the cluster"
    # wording used for both the "unknown feature" and "known but disabled feature" cases instead of
    # pinning to CLUSTER_CONFIG_REGISTRY_V0 specifically.
    await manager.server_start(
        servers[0].server_id,
        expected_error="was previously enabled in the cluster|Feature check failed|received notification of being banned from the cluster from",
    )


@pytest.mark.asyncio
async def test_old_binary_cannot_join_after_cluster_config_feature_enablement(
    manager: ManagerClient, scylla_binary: Path, scylla_2026_1: ScyllaVersionDescription,
) -> None:
    scylla_binary = Path(scylla_binary)
    _, old_scylla_version = get_old_scylla_version(scylla_binary, scylla_2026_1)

    servers = await manager.servers_add(2, cmdline=["--smp", "1"])
    cql, hosts = await manager.get_ready_cql(servers)

    await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = true", host=hosts[0])
    await wait_for_config_map_value_on_hosts(cql, hosts, CLUSTER_CONFIGS_QUERY, [], "auto_repair_enabled", "true")

    await asyncio.gather(*(wait_for_feature("CLUSTER_CONFIG_REGISTRY_V0", cql, host, time.time() + 60) for host in hosts))

    new_server = await manager.server_add(cmdline=["--smp", "1"], version=old_scylla_version, start=False)
    await manager.server_start(
        new_server.server_id,
        expected_error="Feature check failed|received notification of being banned from the cluster from|Unknown feature 'CLUSTER_CONFIG_REGISTRY_V0' was previously enabled in the cluster",
    )


@pytest.mark.asyncio
async def test_joining_node_observes_preexisting_schema_backed_config_metadata(manager: ManagerClient) -> None:
    servers = [
        await manager.server_add(cmdline=["--smp", "1"], property_file={"dc": "dc1", "rack": "rack1"}),
        await manager.server_add(cmdline=["--smp", "1"], property_file={"dc": "dc1", "rack": "rack2"}),
    ]
    await manager.driver_connect()
    cql, hosts = await manager.get_ready_cql(servers)

    await cql.run_async("CREATE KEYSPACE ks_cfg_join WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await cql.run_async("CREATE TABLE ks_cfg_join.tbl (pk int PRIMARY KEY, v int)")
    await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = true")
    await cql.run_async("ALTER KEYSPACE ks_cfg_join WITH auto_repair_enabled = false")
    await cql.run_async("ALTER TABLE ks_cfg_join.tbl WITH auto_repair_enabled = true")

    await wait_for_config_map_value_on_hosts(cql, hosts, KEYSPACE_CONFIGS_QUERY, ["ks_cfg_join"], "auto_repair_enabled", "false")

    new_server = await manager.server_add(cmdline=["--smp", "1"], property_file={"dc": "dc2", "rack": "rack1"})
    servers.append(new_server)
    await manager.driver_connect()
    cql, hosts = await manager.get_ready_cql(servers)
    new_host = hosts[2]

    await wait_for_config_map_value(cql, new_host, CLUSTER_CONFIGS_QUERY, [], "auto_repair_enabled", "true")
    await wait_for_config_map_value(cql, new_host, KEYSPACE_CONFIGS_QUERY, ["ks_cfg_join"], "auto_repair_enabled", "false")
    await wait_for_config_map_value(cql, new_host, TABLE_CONFIGS_QUERY, ["ks_cfg_join", "tbl"], "auto_repair_enabled", "true")


@pytest.mark.asyncio
async def test_mixed_version_schema_agreement_before_feature_enablement(
    manager: ManagerClient, scylla_binary: Path, scylla_2026_1: ScyllaVersionDescription,
) -> None:
    """Integration regression: a cluster-config-aware node joins a cluster of
    cluster-config-unaware ("old") binaries before the CLUSTER_CONFIG_REGISTRY_V0
    cluster feature is enabled, and the cluster keeps working — nodes reach schema
    agreement and schema changes flow from both old and new nodes.

    The node-oriented config tables (scylla_clusters/scylla_datacenters/scylla_racks/
    scylla_nodes) are created locally on the new node but are gated out of schema-digest
    participation by the CLUSTER_CONFIG_TABLES schema feature until the cluster feature is
    enabled; an old binary does not have these tables at all. This test exercises that the
    presence of those local tables on the new node does not break joining or schema
    operations in a mixed cluster, and that DROP KEYSPACE (which must not fabricate
    keyspace-keyed mutations for those non-keyspace-keyed tables) works across versions.

    Scope note: this is a broad end-to-end regression. It does NOT isolate the schema-digest
    gating itself, because in a GROUP0_SCHEMA_VERSIONING cluster (both binaries enable it)
    the gossiped/compared schema version is a persisted group0 state-id, not a locally
    computed digest, and empty/tombstone-only config tables are digest-neutral regardless.
    The authoritative, isolating proof of the digest gating and the drop-keyspace fix lives
    in the C++ unit tests (test/boost/schema_change_test.cc:
    test_cluster_config_tables_gated_by_schema_feature,
    test_cluster_config_tables_digest_neutral_when_empty,
    test_drop_keyspace_does_not_mutate_config_tables).
    """
    scylla_binary = Path(scylla_binary)
    _, old_scylla_version = get_old_scylla_version(scylla_binary, scylla_2026_1)

    # Start with old (cluster-config-unaware) binaries only.
    old_servers = await manager.servers_add(2, cmdline=["--smp", "1"], version=old_scylla_version)
    await manager.driver_connect()
    await manager.get_ready_cql(old_servers)

    # The feature must not be enabled in an all-old cluster.
    old_cql = await manager.get_cql_exclusive(old_servers[0])
    with pytest.raises((InvalidRequest, SyntaxException)):
        await old_cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = true")

    # A schema change issued by an old node, before the new node joins.
    await old_cql.run_async("CREATE KEYSPACE ks_pre WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await wait_for_schema_agreement(manager, old_servers, deadline=time.time() + 60)

    # Add a new (cluster-config-aware) node. It joins while the cluster feature stays
    # disabled (the old nodes do not support it). server_add without an explicit version
    # uses the default (under-test) binary, which is the cluster-config-aware one.
    new_server = await manager.server_add(cmdline=["--smp", "1"])
    all_servers = old_servers + [new_server]

    # Core assertion: the whole mixed cluster converges on a single schema version even
    # though only the new node has the node-oriented config tables locally.
    await wait_for_schema_agreement(manager, all_servers, deadline=time.time() + 120)

    # Even with a new node present, the feature is not yet enabled cluster-wide, so
    # cluster config writes are still rejected (we are genuinely in the mixed window).
    new_cql = await manager.get_cql_exclusive(new_server)
    with pytest.raises(InvalidRequest):
        await new_cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = true")

    # Schema changes from the new node must propagate and keep the cluster in agreement.
    await new_cql.run_async("CREATE KEYSPACE ks_new WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await new_cql.run_async("CREATE TABLE ks_new.tbl (pk int PRIMARY KEY, v int)")
    await wait_for_schema_agreement(manager, all_servers, deadline=time.time() + 120)

    # Schema changes from an old node must likewise keep agreement (including DROP
    # KEYSPACE, which previously fabricated keyspace-keyed mutations for the
    # non-keyspace-keyed config tables on the new node).
    old_cql = await manager.get_cql_exclusive(old_servers[1])
    await old_cql.run_async("DROP KEYSPACE ks_pre")
    await old_cql.run_async("CREATE KEYSPACE ks_old WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
    await wait_for_schema_agreement(manager, all_servers, deadline=time.time() + 120)



async def stored_config_state(cql) -> dict:
    """The stored cluster/keyspace/table config maps for user objects, as plain dicts.

    This is the state the round-trip test compares: replaying a dump must reproduce it
    exactly — no invented overrides, no dropped ones. Objects with no stored overrides
    are represented by an absent key.
    """
    state: dict = {"cluster": {}, "keyspaces": {}, "tables": {}}
    for row in await cql.run_async(CLUSTER_CONFIGS_QUERY):
        state["cluster"] = dict(row.configs or {})
    for row in await cql.run_async("SELECT keyspace_name, configs FROM system_schema.scylla_keyspaces"):
        if row.keyspace_name.startswith("ks_rt") and row.configs:
            state["keyspaces"][row.keyspace_name] = dict(row.configs)
    for row in await cql.run_async("SELECT keyspace_name, table_name, configs FROM system_schema.scylla_tables"):
        if row.keyspace_name.startswith("ks_rt") and row.configs:
            state["tables"][(row.keyspace_name, row.table_name)] = dict(row.configs)
    return state


# The round-trip acceptance test for the DESCRIBE format (the executable slot carries only
# stored overrides; DESC SCHEMA WITH INTERNALS leads with the ALTER CLUSTER block): dump,
# wipe, replay, and require the stored configs maps to be reproduced exactly at every
# scope. Then prove inheritance survived the trip: a broader-scope ALTER in the restored
# cluster still propagates to the purely-inheriting table.
@pytest.mark.asyncio
async def test_describe_schema_internals_round_trips_stored_config(manager: ManagerClient) -> None:
    server = await manager.server_add(cmdline=["--smp", "1"])
    await manager.driver_connect()
    cql, hosts = await manager.get_ready_cql([server])

    replication = "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
    # ks_rt1: override at keyspace AND at one table; ks_rt2: override stored equal to the
    # registry default (must round-trip as a stored override, not be dropped); ks_rt3 and
    # its table: purely inheriting — must come back with NO stored entry.
    await cql.run_async(f"CREATE KEYSPACE ks_rt1 WITH replication = {replication}")
    await cql.run_async("CREATE TABLE ks_rt1.t1 (pk int PRIMARY KEY)")
    await cql.run_async(f"CREATE KEYSPACE ks_rt2 WITH replication = {replication}")
    await cql.run_async(f"CREATE KEYSPACE ks_rt3 WITH replication = {replication}")
    await cql.run_async("CREATE TABLE ks_rt3.t3 (pk int PRIMARY KEY)")

    await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = true")
    await cql.run_async("ALTER KEYSPACE ks_rt1 WITH auto_repair_enabled = false")
    await cql.run_async("ALTER TABLE ks_rt1.t1 WITH auto_repair_enabled = true")
    await cql.run_async("ALTER KEYSPACE ks_rt2 WITH auto_repair_enabled = false")  # equals the registry default
    await wait_for_config_map_value_on_hosts(cql, hosts, CLUSTER_CONFIGS_QUERY, [], "auto_repair_enabled", "true")

    source_state = await stored_config_state(cql)
    assert source_state["cluster"] == {"auto_repair_enabled": "true"}
    assert source_state["keyspaces"] == {"ks_rt1": {"auto_repair_enabled": "false"},
                                         "ks_rt2": {"auto_repair_enabled": "false"}}
    assert source_state["tables"] == {("ks_rt1", "t1"): {"auto_repair_enabled": "true"}}

    def dump_schema():
        return [(row.keyspace_name, row.type, row.name, row.create_statement)
                for row in cql.execute("DESC SCHEMA WITH INTERNALS")]

    dump = dump_schema()

    # The cluster block leads the dump, before any keyspace, and INTERNALS output is pure
    # CQL (no comment lines).
    assert dump[0][1] == "cluster_config" and dump[0][2] == "cluster", f"dump starts with {dump[0]}"
    assert dump[0][3] == "ALTER CLUSTER WITH auto_repair_enabled = true;"

    # Determinism: a second dump of the unchanged source is identical.
    assert dump_schema() == dump

    # Wipe: drop the schema and clear the cluster-scope override, then replay the dump the
    # way a restore tool would - skipping the comment-wrapped topology block (none here)
    # and the pre-existing auth/service-level objects.
    await cql.run_async("DROP KEYSPACE ks_rt1")
    await cql.run_async("DROP KEYSPACE ks_rt2")
    await cql.run_async("DROP KEYSPACE ks_rt3")
    await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = null")
    wiped = await stored_config_state(cql)
    assert wiped == {"cluster": {}, "keyspaces": {}, "tables": {}}

    # Replay only what this test wiped: the cluster block and the ks_rt* keyspaces (the
    # dump also carries unrelated pre-existing objects such as the audit keyspace, roles,
    # and service levels, which still exist in the target).
    for row_keyspace, row_type, row_name, create_statement in dump:
        if row_type == "cluster_config" and row_name == "cluster":
            # The cluster block holds one ALTER CLUSTER statement per line.
            for statement in create_statement.splitlines():
                await cql.run_async(statement)
        elif row_type in ("keyspace", "table") and (row_keyspace or "").startswith("ks_rt"):
            await cql.run_async(create_statement)

    restored_state = await stored_config_state(cql)
    assert restored_state == source_state, f"round-trip changed stored config: {restored_state} != {source_state}"

    # Propagation survived: flipping the cluster scope in the restored cluster reaches the
    # purely-inheriting table, which must NOT have acquired a stored override of its own.
    await cql.run_async("ALTER CLUSTER WITH auto_repair_enabled = false")
    await wait_for_config_map_value_on_hosts(cql, hosts, CLUSTER_CONFIGS_QUERY, [], "auto_repair_enabled", "false")
    t3_rows = [row for row in cql.execute("DESCRIBE TABLE ks_rt3.t3") if row.name == "t3"]
    assert len(t3_rows) == 1
    assert "\n    -- AND auto_repair_enabled = false  -- from cluster (table=NULL, keyspace=NULL, cluster=false)" in t3_rows[0].create_statement
    assert "\n    AND auto_repair_enabled" not in t3_rows[0].create_statement
