#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import json
import os
import psutil
import pytest
import simple_scylla_cluster_manager as sscm
import subprocess
import sys


@pytest.fixture(scope="module")
def sscm_script_path(real_scylla_repo_path):
    return os.path.join(real_scylla_repo_path, "scripts", "simple_scylla_cluster_manager.py")


@pytest.fixture
def invoke_sscm_script(sscm_script_path, cluster_dir):
    def func(args):
        return subprocess.check_output(
                [sys.executable, sscm_script_path] + args,
                cwd=cluster_dir,
                text=True)
    return func


@pytest.fixture
def supervisor_client(cluster_dir):
    return sscm.SupervisorClient(cluster_dir)


def test_create_start_stop_no_nodes(cluster_dir, dummy_scylla_repo_path, scylla_mode, invoke_sscm_script):
    invoke_sscm_script(["create-cluster", "--path", dummy_scylla_repo_path, "--mode", scylla_mode])

    for f in (sscm.LOG_FILE, sscm.PID_FILE, sscm.CONFIG_FILE, sscm.FIFO_IN, sscm.FIFO_OUT):
        assert os.path.exists(os.path.join(cluster_dir, f))

    invoke_sscm_script(["stop-cluster"])

    for f in (sscm.LOG_FILE, sscm.CONFIG_FILE):
        assert os.path.exists(os.path.join(cluster_dir, f))

    for f in (sscm.PID_FILE, sscm.FIFO_IN, sscm.FIFO_OUT):
        assert not os.path.exists(os.path.join(cluster_dir, f))

    invoke_sscm_script(["start-cluster"])

    for f in (sscm.LOG_FILE, sscm.PID_FILE, sscm.CONFIG_FILE, sscm.FIFO_IN, sscm.FIFO_OUT):
        assert os.path.exists(os.path.join(cluster_dir, f))

    invoke_sscm_script(["stop-cluster"])

    for f in (sscm.LOG_FILE, sscm.CONFIG_FILE):
        assert os.path.exists(os.path.join(cluster_dir, f))

    for f in (sscm.PID_FILE, sscm.FIFO_IN, sscm.FIFO_OUT):
        assert not os.path.exists(os.path.join(cluster_dir, f))


def test_create_start_stop_with_nodes(dummy_scylla_repo_path, scylla_mode, invoke_sscm_script, supervisor_client):
    invoke_sscm_script(["create-cluster",
            "--path", dummy_scylla_repo_path,
            "--mode", scylla_mode,
            "--nodes", "1",
            "--start",
            "--",
            "--ring-delay-ms", "0",
            "--skip-wait-for-gossip-to-settle", "0",
            "--shutdown-announce-in-ms", "0"])

    def check_status(node_status):
        cluster_status = json.loads(supervisor_client.send_command(["status"]))

        assert len(cluster_status) == 1
        node1_status = cluster_status[0]

        assert node1_status["index"] == 1
        assert node1_status["status"] == node_status

        if node_status == "UP":
            pid = node1_status["pid"]
            assert psutil.pid_exists(pid)
            return pid

        assert "pid" not in node1_status

    pid = check_status("UP")

    invoke_sscm_script(["stop-cluster"])

    assert not psutil.pid_exists(pid)

    invoke_sscm_script(["start-cluster"])

    check_status("DOWN")

    invoke_sscm_script(["stop-cluster"])
    invoke_sscm_script(["start-cluster", "--start"])

    check_status("UP")

    invoke_sscm_script(["stop-cluster"])


def create_cluster_with_config(invoke_sscm_script, config, extra_args):
    invoke_sscm_script(["create-cluster",
             "--path", config["default_scylla_config"]["path"],
             "--mode", config["default_scylla_config"]["mode"],
             "--smp", str(config["default_scylla_config"]["smp"]),
             "--memory", config["default_scylla_config"]["memory"],
             "--name", config["name"],
             "--network-prefix", str(config["network_prefix"])]
            + extra_args + ["--"] + config["default_scylla_config"]["extra_args"])


def test_config_no_nodes(cluster_dir, dummy_scylla_repo_path, scylla_mode, invoke_sscm_script):
    reference_config = {
        "name": "test-config",
        "network_prefix": 100,
        "default_scylla_config": {
            "extra_args": ["--logger-log-level", "query=trace"],
            "memory": "8G",
            "mode": scylla_mode,
            "path": dummy_scylla_repo_path,
            "smp": 4,
        },
        "nodes": { }
    }

    create_cluster_with_config(invoke_sscm_script, reference_config, [])

    with open(os.path.join(cluster_dir, sscm.CONFIG_FILE)) as f:
        config = json.load(f)

    assert config == reference_config
