#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import contextlib
import json
import os
import pytest
import psutil
import simple_scylla_cluster_manager as sscm
import subprocess


@contextlib.contextmanager
def create_test_cluster(cluster_dir, scylla_repo_path, scylla_mode):
    res = sscm.execute_command([
            "--cluster-directory", cluster_dir,
            "create-cluster",
            "--path", scylla_repo_path,
            "--mode", scylla_mode,
            "--smp", "1",
            "--memory", "1G",
            "--network-prefix", "1",
            "--",
            "--ring-delay-ms", "0",
            "--skip-wait-for-gossip-to-settle", "0",
            "--shutdown-announce-in-ms", "0"])
    try:
        yield res
        sscm.execute_command(["--cluster-directory", cluster_dir, "stop-cluster"])
        with open(os.path.join(cluster_dir, sscm.LOG_FILE)) as f:
            print(f.read())
    finally:
        try:
            res.supervisor_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            res.supervisor_process.kill()


@pytest.fixture
def test_cluster(cluster_dir, dummy_scylla_repo_path, scylla_mode):
    with create_test_cluster(cluster_dir, dummy_scylla_repo_path, scylla_mode) as cluster:
        assert cluster.supervisor_client is not None
        assert cluster.supervisor_process is not None
        yield cluster.supervisor_client
        assert cluster.supervisor_process.poll() is None


def check_node_status(supervisor_client, statuses):
    cluster_status = json.loads(supervisor_client.send_command(["status"]))
    print(str(cluster_status))
    assert len(cluster_status) == len(statuses)
    for status, expected_status in zip(cluster_status, statuses):
        assert status["status"] == expected_status


def test_add_node(test_cluster):
    check_node_status(test_cluster, [])
    test_cluster.send_command(["add-node"])
    check_node_status(test_cluster, ["DOWN"])
    test_cluster.send_command(["add-node", "-n2"])
    check_node_status(test_cluster, ["DOWN", "DOWN", "DOWN"])
    test_cluster.send_command(["add-node", "--start"])
    check_node_status(test_cluster, ["DOWN", "DOWN", "DOWN", "UP"])


def test_add_node_config(cluster_dir, dummy_scylla_repo_path, scylla_mode, test_cluster):
    def get_config():
        with open(os.path.join(cluster_dir, sscm.CONFIG_FILE)) as f:
            cfg = json.load(f)
            print(cfg)
            return cfg

    def add_node_with_config(cfg):
        test_cluster.send_command(["add-node",
            "--smp", str(cfg["smp"]),
            "--memory", str(cfg["memory"]),
            "--mode", str(cfg["mode"]),
            "--path", str(cfg["path"]),
            "--"] + cfg["extra_args"])

    config = get_config()
    assert len(config["nodes"]) == 0

    default_config = config["default_scylla_config"]

    # Add node with explicit default config
    add_node_with_config(default_config)
    config = get_config()
    assert len(config["nodes"]) == 1
    assert config["nodes"]["1"] == {"index": 1, "ip": "127.1.0.1"}

    repo_link = os.path.join(cluster_dir, "repo_link")
    os.symlink(dummy_scylla_repo_path, repo_link)

    non_default_mode = "dev" if scylla_mode != "dev" else "release"
    non_default_config = {"smp": 2, "memory": "200M", "mode": non_default_mode, "path": repo_link, "extra_args": ["--abort-on-internal-error"]}
    assert set(non_default_config.keys()) == set(default_config.keys())
    for k in default_config.keys():
        assert not default_config[k] == non_default_config[k]

    # Add node with explicit non-default config
    add_node_with_config(non_default_config)
    config = get_config()
    non_default_config.update({"index": 2, "ip": "127.1.0.2"})
    assert len(config["nodes"]) == 2
    assert config["nodes"]["2"] == non_default_config

    # Add node with implicit default config
    test_cluster.send_command(["add-node"])
    config = get_config()
    assert len(config["nodes"]) == 3
    assert config["nodes"]["3"] == {"index": 3, "ip": "127.1.0.3"}

    # Add node with --clear-extra-args=yes
    test_cluster.send_command(["add-node", "--clear-extra-args", "yes"])
    config = get_config()
    assert len(config["nodes"]) == 4
    assert config["nodes"]["4"] == {"index": 4, "ip": "127.1.0.4", "extra_args": []}

def test_start_node(test_cluster):
    test_cluster.send_command(["add-node", "-n5"])
    check_node_status(test_cluster, ["DOWN", "DOWN", "DOWN", "DOWN", "DOWN"])

    with pytest.raises(RuntimeError, match="Operation failed, supervisor responed with: no node index was provided"):
        test_cluster.send_command(["start-node"])

    with pytest.raises(RuntimeError, match="Operation failed, supervisor responed with: failed to start node#100: node not found"):
        test_cluster.send_command(["start-node", "-i100"])

    check_node_status(test_cluster, ["DOWN", "DOWN", "DOWN", "DOWN", "DOWN"])

    test_cluster.send_command(["start-node", "-i2"])
    check_node_status(test_cluster, ["DOWN", "UP", "DOWN", "DOWN", "DOWN"])

    # Starting an already started node is not an error
    test_cluster.send_command(["start-node", "-i2"])
    check_node_status(test_cluster, ["DOWN", "UP", "DOWN", "DOWN", "DOWN"])

    test_cluster.send_command(["start-node", "-i1", "-i3"])
    check_node_status(test_cluster, ["UP", "UP", "UP", "DOWN", "DOWN"])

    test_cluster.send_command(["start-node", "-a"])
    check_node_status(test_cluster, ["UP", "UP", "UP", "UP", "UP"])


def test_stop_node(test_cluster):
    test_cluster.send_command(["add-node", "-n5", "-s"])
    check_node_status(test_cluster, ["UP", "UP", "UP", "UP", "UP"])

    with pytest.raises(RuntimeError, match="Operation failed, supervisor responed with: no node index was provided"):
        test_cluster.send_command(["stop-node"])

    with pytest.raises(RuntimeError, match="Operation failed, supervisor responed with: failed to stop node#100: node not found"):
        test_cluster.send_command(["stop-node", "-i100"])

    check_node_status(test_cluster, ["UP", "UP", "UP", "UP", "UP"])

    test_cluster.send_command(["stop-node", "-i2"])
    check_node_status(test_cluster, ["UP", "DOWN", "UP", "UP", "UP"])

    # Stopping an already started node is not an error
    test_cluster.send_command(["stop-node", "-i2"])
    check_node_status(test_cluster, ["UP", "DOWN", "UP", "UP", "UP"])

    test_cluster.send_command(["stop-node", "-i1", "-i3"])
    check_node_status(test_cluster, ["DOWN", "DOWN", "DOWN", "UP", "UP"])

    test_cluster.send_command(["stop-node", "-a"])
    check_node_status(test_cluster, ["DOWN", "DOWN", "DOWN", "DOWN", "DOWN"])

def _get_indexes(indexes, nodes):
        if indexes is None:
            return list(range(1, nodes + 1)), ["-a"]
        else:
            return indexes, [f"-i{i}" for i in indexes]


def test_restart_node(test_cluster):
    test_cluster.send_command(["add-node", "-n2", "-s"])
    check_node_status(test_cluster, ["UP", "UP"])

    with pytest.raises(RuntimeError, match="Operation failed, supervisor responed with: no node index was provided"):
        test_cluster.send_command(["restart-node"])

    with pytest.raises(RuntimeError, match="Operation failed, supervisor responed with: failed to restart node#100: node not found"):
        test_cluster.send_command(["restart-node", "-i100"])

    def restart_nodes(indexes = None):
        indexes, indexes_args = _get_indexes(indexes, 2)
        cluster_status_before = json.loads(test_cluster.send_command(["status"]))
        test_cluster.send_command(["restart-node"] + indexes_args)
        cluster_status_after = json.loads(test_cluster.send_command(["status"]))
        for before, after in zip(cluster_status_before, cluster_status_after):
            assert before["status"] == "UP"
            assert after["status"] == "UP"
            if before["index"] in indexes:
                assert before["pid"] != after["pid"]
            else:
                assert before["pid"] == after["pid"]

    restart_nodes([1])
    restart_nodes([1, 2])
    restart_nodes(None)

    test_cluster.send_command(["stop-node", "-a"])
    check_node_status(test_cluster, ["DOWN", "DOWN"])

    # Restarting a stopped node is not an error
    test_cluster.send_command(["restart-node", "-i1"])
    check_node_status(test_cluster, ["UP", "DOWN"])


def test_upgrade_node(test_cluster, cluster_dir, dummy_scylla_repo_path):
    test_cluster.send_command(["add-node", "-n2", "-s"])
    check_node_status(test_cluster, ["UP", "UP"])

    with pytest.raises(RuntimeError, match="Operation failed, supervisor responed with: no node index was provided"):
        test_cluster.send_command(["upgrade-node"])

    with pytest.raises(RuntimeError, match="Operation failed, supervisor responed with: failed to upgrade node#100: node not found"):
        test_cluster.send_command(["upgrade-node", "-i100"])

    def make_link(i):
        repo_link = os.path.join(cluster_dir, f"repo_link{i}")
        os.symlink(dummy_scylla_repo_path, repo_link)
        return repo_link

    def upgrade_nodes(indexes, repo_link, start="yes"):
        indexes, indexes_args = _get_indexes(indexes, 2)

        cluster_status_before = json.loads(test_cluster.send_command(["status"]))

        with open(os.path.join(cluster_dir, sscm.CONFIG_FILE), "r") as f:
            config = json.load(f)
            for i in indexes:
                config["nodes"][str(i)]["path"] = repo_link
        with open(os.path.join(cluster_dir, sscm.CONFIG_FILE), "w") as f:
            json.dump(config, f)

        for before in cluster_status_before:
            assert before["status"] == "UP"

        test_cluster.send_command(["reload-config"])
        test_cluster.send_command(["upgrade-node", "--start", start] + indexes_args)

        cluster_status_after = json.loads(test_cluster.send_command(["status"]))
        for before, after in zip(cluster_status_before, cluster_status_after):
            if before["index"] not in indexes:
                assert before["status"] == "UP"
                assert after["status"] == "UP"
                assert before["pid"] == after["pid"]
                continue

            if start == "yes":
                assert after["status"] == "UP"
                assert before["pid"] != after["pid"]
            else:
                assert after["status"] == "DOWN"
                assert "pid" not in after


    upgrade_nodes([2], make_link(1))
    upgrade_nodes([1, 2], make_link(2))
    upgrade_nodes(None, make_link(3))
    upgrade_nodes([1], make_link(4), start="no")


def test_remove_node(test_cluster, cluster_dir):
    test_cluster.send_command(["add-node", "-n6"])
    check_node_status(test_cluster, ["DOWN", "DOWN", "DOWN", "DOWN", "DOWN", "DOWN"])

    with pytest.raises(RuntimeError, match="Operation failed, supervisor responed with: no node index was provided"):
        test_cluster.send_command(["remove-node"])

    with pytest.raises(RuntimeError, match="Operation failed, supervisor responed with: failed to remove node#100: node not found"):
        test_cluster.send_command(["remove-node", "-i100"])

    def remove_nodes(indexes):
        indexes, indexes_args = _get_indexes(indexes, 6)

        test_cluster.send_command(["remove-node"] + indexes_args)
        for node in json.loads(test_cluster.send_command(["status"])):
            assert node["index"] not in indexes

        with open(os.path.join(cluster_dir, sscm.CONFIG_FILE)) as f:
            config = json.load(f)

        for i in indexes:
            assert not os.path.exists(os.path.join(cluster_dir, f"node{i}"))
            assert i not in config

    # Check that a running node can be removed too
    test_cluster.send_command(["start-node", "-i4"])

    remove_nodes([4])
    remove_nodes([2, 3])
    remove_nodes(None)

def test_status(test_cluster, cluster_dir):
    test_cluster.send_command(["add-node", "-n3"])

    def find_pid(index):
        for node in json.loads(test_cluster.send_command(["status"])):
            if node["index"] == index:
                return node["pid"]
        assert False

    def check_statuses(nodes):
        cluster_status = json.loads(test_cluster.send_command(["status"]))
        assert len(cluster_status) == len(nodes)
        for node in cluster_status:
            i = node["index"]
            expected_status = nodes[i]
            assert node["ip"].split(".")[-1] == str(i)
            assert node["status"] == expected_status
            if expected_status == "UP":
                assert psutil.pid_exists(node["pid"])
            elif expected_status == "FAILED":
                assert node["return_code"] == -9
                assert "pid" not in node
            else:
                assert "pid" not in node

    check_statuses({1: "DOWN", 2: "DOWN", 3: "DOWN"})

    test_cluster.send_command(["remove-node", "-i2"])
    check_statuses({1: "DOWN", 3: "DOWN"})

    test_cluster.send_command(["start-node", "-i1"])
    check_statuses({1: "UP", 3: "DOWN"})

    proc = psutil.Process(find_pid(1))
    proc.kill()

    check_statuses({1: "FAILED", 3: "DOWN"})


def test_start_stop_real_scylla(cluster_dir, real_scylla_repo_path, scylla_mode):
    with create_test_cluster(cluster_dir, real_scylla_repo_path, scylla_mode) as cluster:
        client = cluster.supervisor_client
        client.send_command(["add-node"])
        client.send_command(["start-node", "-i1"])
        client.send_command(["stop-node", "-i1"])


def test_start_real_scylla_fails(cluster_dir, real_scylla_repo_path, scylla_mode):
    with create_test_cluster(cluster_dir, real_scylla_repo_path, scylla_mode) as cluster:
        client = cluster.supervisor_client
        # add the `--smp` arg to the command-line again (the supervisor already adds it), so scylla fails to start
        client.send_command(["add-node", "--", "--smp", "1"])
        with pytest.raises(RuntimeError, match="Operation failed, supervisor responed with: failed to start node#1: scylla process exited with non-zero exit code: 2"):
            client.send_command(["start-node", "-i1"])
