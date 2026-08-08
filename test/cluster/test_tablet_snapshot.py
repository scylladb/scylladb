#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import asyncio
import logging
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from test import TOP_SRC_DIR
from test.cluster.util import get_topology_coordinator
from test.cluster.util import new_test_keyspace
from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.util import unique_name
from test.pylib.util import wait_for_view
from test.pylib.manager_client import ManagerClient

from tablets.snapshot import take_snapshot
from tablets import topology

logger = logging.getLogger(__name__)


def assert_expected_topology_shape(topo: topology.Topology, ks: str, mv: str, expected_host_count: int, expected_tablet_count: int) -> None:
    table_ids_by_name = {topo.get_table_name(table_id): table_id for table_id in topo.iter_table_ids()}
    expected_tables = {
        f"{ks}.t1",
        f"{ks}.t2",
        f"{ks}.{mv}",
    }

    assert expected_tables <= table_ids_by_name.keys()
    assert topo.host_count() == expected_host_count

    t1_id = table_ids_by_name[f"{ks}.t1"]
    t2_id = table_ids_by_name[f"{ks}.t2"]
    mv_id = table_ids_by_name[f"{ks}.{mv}"]

    assert topo.get_base_table_id(t1_id) == t1_id
    assert topo.get_base_table_id(t2_id) == t2_id
    assert topo.get_base_table_id(mv_id) in table_ids_by_name.values()

    for table_id in (t1_id, t2_id, mv_id):
        assert len(topo.get_tablet_map(table_id).tablets) == expected_tablet_count

    for host in topo.all_hosts():
        assert host.dc == "dc1"
        assert host.rack is not None


def take_manual_snapshot(host: str, workdir: Path, output_dir: str = "manual_snapshot") -> Path:
    snapshot_script = TOP_SRC_DIR / "scripts" / "tablets" / "snapshot.py"
    result = subprocess.run(
        [sys.executable, str(snapshot_script), "--manual", "--cluster", host, "--output-dir", output_dir],
        cwd=workdir,
        check=True,
        capture_output=True,
        text=True,
    )
    for line in result.stdout.strip().splitlines():
        if line.startswith("cqlsh "):
            line = line.replace(
                "cqlsh ",
                f"{TOP_SRC_DIR / 'bin/cqlsh'} ",
                1,
            )
        subprocess.run(line, cwd=workdir, shell=True, check=True, text=True)
    return workdir / output_dir


def get_live_topology(host: str) -> topology.LiveClusterTopologySource:
    return topology.get_live_topology_source_from_args(SimpleNamespace(
        cluster=host,
        port=None,
        user=None,
        password=None,
    ))


@pytest.mark.asyncio
async def test_tablet_snapshot_matches_live_topology(manager: ManagerClient, tmp_path: Path):
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    cql = manager.get_cql()

    async with new_test_keyspace(manager, f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': 3}}") as ks:
        mv = unique_name()
        await cql.run_async(f"CREATE TABLE {ks}.t1 (pk int PRIMARY KEY, v int)")
        await cql.run_async(f"CREATE TABLE {ks}.t2 (pk int PRIMARY KEY, v int)")
        await cql.run_async(
            f"CREATE MATERIALIZED VIEW {ks}.{mv} AS SELECT pk, v FROM {ks}.t1 "
            f"WHERE pk IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, pk)"
        )
        await wait_for_view(cql, mv, len(servers))

        with get_live_topology(servers[0].ip_addr) as live_src:
            live_topo = live_src.get_topology()

            assert_expected_topology_shape(live_topo, ks, mv, expected_host_count=3, expected_tablet_count=4)

            # Verify that the snapshot taken from a live node matches the live topology,
            # and that a manual snapshot also matches.
            snapshot_dir = take_snapshot(servers[0].ip_addr, tmp_path)
            snapshot_topo = topology.TopologyFromSnapshot(str(snapshot_dir)).get_topology()

            manual_snapshot_dir = take_manual_snapshot(servers[0].ip_addr, tmp_path)
            manual_snapshot_topo = topology.TopologyFromSnapshot(str(manual_snapshot_dir)).get_topology()

            assert live_topo == snapshot_topo
            assert live_topo == manual_snapshot_topo


@pytest.mark.asyncio
@pytest.mark.skip_mode(mode='release', reason='error injections are not supported in release mode')
@pytest.mark.nightly
async def test_tablet_snapshot_taken_after_replace_before_rebuild(manager: ManagerClient, tmp_path: Path):
    """
    A replace finishes its topology request before the replaced node's replicas are rebuilt.
    A snapshot taken in that window still lists the replaced node as a replica, while
    system.topology already has it as left and the joining node as normal.
    """
    servers = await manager.servers_add(3, auto_rack_dc="dc1")
    cql = manager.get_cql()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3}"
                                          " AND tablets = {'initial': 8}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t1 (pk int PRIMARY KEY, v int)")

        # Keeps unrelated migrations away. Does not stop the replace driven rebuild.
        await manager.disable_tablet_balancing()

        coordinator = await get_topology_coordinator(manager)
        coordinator_server = await manager.find_server_by_host_id(servers, coordinator)
        replaced_server = next(s for s in servers if s.server_id != coordinator_server.server_id)
        replaced_host_id = UUID(str(await manager.get_host_id(replaced_server.server_id)))
        survivor = next(s for s in servers if s.server_id != replaced_server.server_id)

        logger.info(f"Downing {replaced_server} to replace it")
        await manager.server_stop(replaced_server.server_id, convict=True)
        await manager.others_not_see_server(replaced_server.ip_addr)

        # An empty migration plan, so nothing is rebuilt off the replaced node and it stays in
        # system.tablets as a replica.
        await manager.api.enable_injection(coordinator_server.ip_addr, "tablet_migration_bypass", one_shot=False)

        logger.info("Replacing the node, which blocks in await_tablets_rebuilt()")
        replace_cfg = ReplaceConfig(replaced_id=replaced_server.server_id, reuse_ip_addr=False, use_host_id=True)
        joining_server = await manager.server_add(replace_cfg, start=False,
                                                  property_file=replaced_server.property_file())
        joining_log = await manager.server_open_log(joining_server.server_id)
        replace_task = asyncio.create_task(manager.server_start(joining_server.server_id))

        try:
            # Logged once the replace itself is done, so system.topology already has the joining
            # node as normal and the replaced one as left, with the replicas not rebuilt yet.
            await joining_log.wait_for("Waiting for tablet replicas from the replaced node to be rebuilt",
                                       timeout=300)

            def assert_replace_in_progress(topo: topology.Topology, source: str) -> None:
                replaced = topo.get_host(replaced_host_id)
                assert replaced is not None, f"{source}: replaced node is missing from the topology"
                assert replaced.node_state == "left", f"{source}: node_state is {replaced.node_state}"

                # It is not a peer any imbalance is measured against, but it is still a replica.
                assert not replaced.is_normal_token_owner(), f"{source}: counted as a token owner"
                assert replaced not in list(topo.all_normal_token_owner_hosts()), f"{source}: listed as a peer"
                assert len(list(topo.all_normal_token_owner_hosts())) == len(servers), f"{source}: wrong peer count"

                held = [tablet for _, tablet in topo.all_tablets()
                        if any(host_id == replaced_host_id for host_id, _ in tablet.replicas)]
                assert held, f"{source}: replaced node holds no tablet replicas"

            with get_live_topology(survivor.ip_addr) as live_src:
                assert_replace_in_progress(live_src.get_topology(), "live cluster")

            snapshot_dir = take_snapshot(survivor.ip_addr, tmp_path)
            assert_replace_in_progress(
                topology.TopologyFromSnapshot(str(snapshot_dir)).get_topology(), "snapshot")
        finally:
            # The window is all this test needs. Letting the replace finish would cost most of
            # the runtime, so the half joined cluster is thrown away instead.
            replace_task.cancel()
            await manager.mark_dirty()
