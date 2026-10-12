#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
tools/scyllatop against the metrics of a three-node cluster under a
cassandra-stress write load, ported from scylla-dtest scyllatop_test.py
(TestScyllaTop), one test per dtest: started in its default interactive mode
and interrupted, and in batch mode. Like the dtests' populate(3), the nodes
use the SimpleSnitch, in datacenter1/rack1.
"""

import asyncio
import logging
import os
import signal
import subprocess
import sys

import pytest

from test import DEBUG_MODES
from test.cluster.util import cassandra_stress, stop_servers_gracefully
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

SCYLLATOP = os.path.join(os.path.dirname(__file__), "..", "..", "tools", "scyllatop", "scyllatop.py")
CONFIG = {"tablets_mode_for_new_keyspaces": "disabled", "endpoint_snitch": "SimpleSnitch"}
GATE = pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test."""
    yield
    await stop_servers_gracefully(manager)


def get_cli(server, tmp_path) -> list[str]:
    """The dtests' get_cli(): scyllatop with a log file, the first node's
    metrics and DEBUG verbosity."""
    pytest.importorskip("urwid")
    logfile = tmp_path / "scyllatop.log"
    return [sys.executable, SCYLLATOP, "-L", str(logfile), "-p", f"http://{server.ip_addr}:9180/metrics", "-v", "DEBUG"]


async def interactive_start(server, tmp_path, wait=True, sleep_time=10):
    """The dtests' interactive_start(): scyllatop without options, with its
    output piped, interrupted after sleep_time seconds."""
    p = subprocess.Popen(get_cli(server, tmp_path), stdout=subprocess.PIPE, universal_newlines=True)
    if not wait:
        return p
    await asyncio.sleep(sleep_time)
    p.send_signal(signal.SIGINT)
    out, err = await asyncio.to_thread(p.communicate)
    logger.debug(f"Length of output is {len(out.split())}")
    assert p.returncode == 0, err
    assert len(out) > 0, "Output should not be empty"


async def batch_mode_start(server, tmp_path, wait=True, n=1):
    """The dtests' batch_mode_start(): scyllatop -b -n <n>."""
    p = subprocess.Popen(get_cli(server, tmp_path) + ["-b", "-n", str(n)], stdout=subprocess.PIPE, universal_newlines=True)
    if not wait:
        return p
    out, err = await asyncio.to_thread(p.communicate)
    logger.debug(f"Length of output is {len(out.split())}")
    assert p.returncode == 0, err
    assert len(out) > 0, "Output should not be empty"


async def stress_write(manager: ScyllaClusterManager, server):
    """node.stress(["write", "duration=10s", "no-warmup", "-rate", "threads=2"])."""
    await cassandra_stress(manager, server, "write", "duration=10s", "no-warmup", "-rate", "threads=2", create_keyspace=False)


# Ported from scylla-dtest scyllatop_test.py::TestScyllaTop::test_default_start.
# Interactive scyllatop exits cleanly on SIGINT after showing metrics, also
# while the cluster serves a write load.
@GATE
async def test_default_start(manager: ScyllaClusterManager, tmp_path):
    servers = await manager.servers_add(3, config=CONFIG)
    await interactive_start(servers[0], tmp_path)
    p = await interactive_start(servers[0], tmp_path, wait=False)
    await stress_write(manager, servers[0])
    p.send_signal(signal.SIGINT)
    out, err = await asyncio.to_thread(p.communicate)
    logger.debug(f"Length of output is {len(out.split())}")
    assert p.returncode == 0, err


# Ported from scylla-dtest scyllatop_test.py::TestScyllaTop::test_batch_mode_start.
# Batch-mode scyllatop prints one iteration, and 20 while the cluster serves
# a write load.
@GATE
async def test_batch_mode_start(manager: ScyllaClusterManager, tmp_path):
    servers = await manager.servers_add(3, config=CONFIG)
    await batch_mode_start(servers[0], tmp_path)
    p = await batch_mode_start(servers[0], tmp_path, wait=False, n=20)
    await stress_write(manager, servers[0])
    out, err = await asyncio.to_thread(p.communicate)
    logger.debug(f"Length of output is {len(out.split())}")
    assert p.returncode == 0, err
