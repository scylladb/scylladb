#!/usr/bin/env python3
#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

"""
Simple Scylla Cluster Manager

See ./simple_scylla_cluster_manager.py --help

For usage from python, see:
* SupervisorClient
* execute_command()
"""

import argparse
import json
import logging
import os
import psutil
import random
import re
import shutil
import subprocess
import sys
import textwrap
import time
import traceback


LOG_FILE = "supervisor.log"
PID_FILE = "supervisor.pid"
CONFIG_FILE = "config.json"
FIFO_IN = "supervisor_in"
FIFO_OUT = "supervisor_out"

SCYLLA_LOG_FILE = "system.log"


class RawDescriptionDefaultsHelpFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
    pass


def _parse_args(app_name, argv):
    parser = argparse.ArgumentParser(
            prog=f"{app_name}",
            formatter_class=RawDescriptionDefaultsHelpFormatter,
            description=textwrap.dedent(f"""\
                Simple Scylla Cluster Manager

                Create local clusters for testing or experimentation.
                Add/remove, start/stop/restart/upgrade nodes.

                The cluster manager treats ScyllaDB as a simple process.
                Adding a node is simply copying over the chosen binary and setting up all the
                auxiliary files/directories.
                Removing a node is simply stopping it and nuking its data.
                All the Scylla specific cluster/node management is left to the user.

                This script has two components:
                * server (supervisor)
                * client

                The supervisor runs in the background, listening for commands. Commands are
                received via the {FIFO_IN} FIFO and results are sent via the
                {FIFO_OUT} FIFO.
                When invoked, {app_name} will forward the command-line as-is to the server, which
                will respond with a simple JSON response. The protocol is line-based. A command
                is one line and the response is one line too.
                The supervisor's pid can be found int {PID_FILE} and its log is in
                {LOG_FILE}.

                The supervisor is started with either create-cluster or start-cluster. The latter
                should be used when there is already a cluster created but the supervisor was
                stopped. The supervisor is stopped with stop-cluster.

                Example:

                # Create a cluster with two nodes, start the supervisor, then start the nodes
                # By default, Scylla is copied from the same repo, the script is located in.
                # Also creates a local symlink of {app_name}.
                $ /path/to/scylla.git/scripts/{app_name} create-cluster -n2 -s
                $ ./{app_name} status
                [
                    {{
                        "index": 1,
                        "ip": "127.0.150.1",
                        "status": "UP",
                        "pid": 254315
                    }},
                    {{
                        "index": 2,
                        "ip": "127.0.150.2",
                        "status": "UP",
                        "pid": 254323
                    }}
                ]
                # Stop all nodes and stop the supervisor
                $ ./{app_name} stop-cluster
                """))
    parser.add_argument("-d", "--cluster-directory", action="store", type=str, default=".",
                        help=f"the working directory of the cluster (must be writeable by {app_name})")
    subparsers = parser.add_subparsers(dest="operation", help="the cluster/node operation to execute")

    create_start_cluster_add_node_parser = argparse.ArgumentParser(add_help=False)
    create_start_cluster_add_node_parser.add_argument("-s", "--start", action="store_true",
                                                      help="start the nodes added to the cluster")

    parser_create_cluster = subparsers.add_parser(
            "create-cluster",
            parents=[create_start_cluster_add_node_parser],
            formatter_class=RawDescriptionDefaultsHelpFormatter,
            description=textwrap.dedent(f"""\
                Setup a new cluster

                Creates the required infrastructure, then starts the the supervisor.
                Also creates a local symlink to {app_name} for convenience.
                Should be run in an empty directory, but this is not enforced.

                The following files are created:
                * {app_name} - symlink to {app_name} (for convenience)
                * {LOG_FILE} - supervisor logs here
                * {PID_FILE} - contains the pid of the supervisor
                * {FIFO_IN} - input fifo of the supervisor
                * {FIFO_OUT} - output fifo of the supervisor
                * {CONFIG_FILE} - containing the cluster's configuration

                If the cluster is created with more than 0 nodes, there will be a directory
                created for each, see add-node. The supervisor (and all the nodes) can be stopped
                with stop-cluster. If there is already a cluster setup, you can use start-cluster
                to start the supervisor.

                The provided scylla-related parameters will serve as the defaults when adding new
                nodes. They can be overriden on a node-by-node basis in add-node.
                """),
            help="create a new cluster")
    parser_create_cluster.add_argument("--name", action="store", default="sccm-test-cluster",
                                       help="name of the new cluster (will be passed to scylla's --cluster-name)")
    parser_create_cluster.add_argument("-n", "--nodes", type=int, action="store", default=0,
                                       help="amount of nodes to create the cluster with")
    parser_create_cluster.add_argument("--network-prefix", type=int, action="store", default=None,
                                       help="ip addresses for nodes will be constructed as 127.NETWORK_PREFIX.0.X, "
                                       "if not provided, a random number between 1 and 255 will be used")

    subparsers.add_parser(
            "start-cluster",
            parents=[create_start_cluster_add_node_parser],
            formatter_class=RawDescriptionDefaultsHelpFormatter,
            description=textwrap.dedent("""\
                Start an existing cluster

                Starts the supervisor and optionally start all the nodes.
                Use stop-cluster to stop the supervisor (and the nodes).
                """),
            help="start the supervisor for an existing cluster and optionally start the nodes")

    subparsers.add_parser(
            "stop-cluster",
            formatter_class=RawDescriptionDefaultsHelpFormatter,
            description=textwrap.dedent("""\
                Stop the cluster

                Stops all the nodes, then stops the supervisor.
                Use start-cluster to start the supervisor again.
                """),
            help="stop all nodes and stop the supervisor")

    node_ops_parser = argparse.ArgumentParser(add_help=False)
    node_ops_parser.add_argument("-i", "--node-index", action="append", default=[], type=int,
                                 help="the index of the node to execute the operation on")
    node_ops_parser.add_argument("-a", "--all", action="store_true",
                                 help="execute operation on all nodes in the cluster")

    parser_add_node = subparsers.add_parser(
            "add-node",
            parents=[create_start_cluster_add_node_parser],
            formatter_class=RawDescriptionDefaultsHelpFormatter,
            description=textwrap.dedent(f"""\
                Add one or more new nodes to the cluster

                Creates a new directory for the node, called node{{i}}, where i is the next
                available index. The assigned index(es) are printed on success.
                The node's directory is populated with a Scylla binary, a log file for Scylla
                ({SCYLLA_LOG_FILE}), and any other directories and resources required to run a
                Scylla node.

                Optionally, the nodes can also be started, or you can do that later with start-node.
                """),
            epilog=textwrap.dedent("""\
                Scylla related options default to the values provided in create-cluster. Provide
                new values here if you want to override their values for the new node(s).
                """),
            help="add one or more new node(s) to the cluster")
    parser_add_node.add_argument("-n", "--nodes", type=int, action="store", default=1,
                                 help="amount of nodes to add")
    parser_add_node.add_argument("--clear-extra-args", action="store", default="no", choices=["yes", "no"],
                                 help="remove all extra args for this node")

    for args, kwargs in [
            (("-p", "--path"), {"help": "path to the Scylla repository to use, if not provided, "
                                "it will be attempted to be deduced from the script's path, "
                                "assuming it is invoked from the repository directly"}),
            (("--mode",), {"default": "dev", "choices": ["coverage", "dev", "debug", "release", "sanitize"],
                           "help": "Scylla build mode to use"}),
            (("-c", "--smp"), {"type": int, "default": 2, "help": "number of cpus to start Scylla with"}),
            (("-m", "--memory"), {"default": "4G", "help": "amount of memory to start Scylla with"}),
            (("extra_args",), {"nargs": "*", "default": [], "help": "extra arguments to pass to Scylla, "
                               "use -- to separate from the rest of the arguments"})]:
        parser_create_cluster.add_argument(*args, **kwargs)
        if "default" in kwargs:
            kwargs["default"] = None
        parser_add_node.add_argument(*args, **kwargs)

    subparsers.add_parser(
            "start-node",
            parents=[node_ops_parser],
            formatter_class=RawDescriptionDefaultsHelpFormatter,
            description=textwrap.dedent("""\
                Start the node(s) and wait for them to complete initialization

                Use stop-node and restart-node to stop and restart the node respectively.
                Use stop-cluster to stop all nodes (as well as the supervisor).
                """),
            help="start the node(s)")
    subparsers.add_parser(
            "stop-node",
            parents=[node_ops_parser],
            formatter_class=RawDescriptionDefaultsHelpFormatter,
            description=textwrap.dedent("""\
                Stop the node(s) and wait for them to shut down

                If the node doesn't stop in 30 seconds, it is killed with SIGKILL.
                Use start-node to start the node(s) again.
                """),
            help="stop the node(s)")
    subparsers.add_parser(
            "restart-node",
            parents=[node_ops_parser],
            formatter_class=RawDescriptionDefaultsHelpFormatter,
            description=textwrap.dedent("""\
                Stop, then start one or more nodes

                Convenience shortcut to issuing stop-node and start-node.
                """),
            help="restart the node(s)")
    parser_upgrade_node = subparsers.add_parser(
            "upgrade-node",
            parents=[node_ops_parser],
            formatter_class=RawDescriptionDefaultsHelpFormatter,
            description=textwrap.dedent("""\
                Stop the node(s), copy over the executable and auxiliary files, then optionally start the node

                Allows upgrading the node to a new binary.
                Note that the files will be copied over from the path pointed at in the
                configuration. To make this a real upgrade, first edit config.json, issue
                reload-config and then issue upgrade-node.
                """),
            help="upgrades the node to a new binary")
    parser_upgrade_node.add_argument("-s", "--start", action="store", default="yes", choices=["yes", "no"],
                                     help="start the node after upgrade")
    subparsers.add_parser(
            "remove-node",
            parents=[node_ops_parser],
            formatter_class=RawDescriptionDefaultsHelpFormatter,
            description=textwrap.dedent("""\
                Stop the node(s) and remove all its data

                Can also be called if the node is not down. Does not take care of removing the
                node from the cluster, this is left to the user.
                """),
            help="stop the node(s) and remove all its data (doesn\'t remove the node from the cluster)")

    subparsers.add_parser(
            "status",
            formatter_class=RawDescriptionDefaultsHelpFormatter,
            description=textwrap.dedent(f"""\
                Print the status of all the nodes in the cluster

                Example:
                $ ./{app_name} status
                [
                    {{
                        "index": 1,
                        "ip": "127.0.150.1",
                        "status": "UP",
                        "pid": 254315
                    }},
                    {{
                        "index": 2,
                        "ip": "127.0.150.2",
                        "status": "UP",
                        "pid": 254323
                    }}
                ]

                Where:
                * index - the index of the node (assigned during add-node)
                * ip - the ip of the node (assigned during add-node)
                * status - UP (running), DOWN (stopped) or FAILED (was UP but exited with non-0 status since)
                * return_code - the exit code of the Scylla process (only when status is FAILED)
                * pid - the pid of the Scylla process
                """),
            help="status of the cluster")
    subparsers.add_parser(
            "reload-config",
            formatter_class=RawDescriptionDefaultsHelpFormatter,
            description=textwrap.dedent(f"""\
                Reload the configuration from {CONFIG_FILE}

                The configuration changes (if any) are not applied. The configuration changes,
                will be picked up when nodes are added/started/restarted/upgraded (depending on
                the changed config items).
                Be mindful of the fact that {CONFIG_FILE} is also written by {app_name}
                during create-cluster, add-node and remove-node.
                """),
            help=f"reloads the config from {CONFIG_FILE}")

    return parser.parse_args(argv)


class Supervisor:
    class Node:
        def __init__(self, index, ip, handle):
            self.index = index
            self.ip = ip
            self.handle = handle

    def __init__(self, app_name):
        logging.basicConfig(
                filename=LOG_FILE,
                filemode='a',
                format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                datefmt='%H:%M:%S',
                level=logging.DEBUG)

        self._logger = logging.getLogger('supervisor')

        self._logger.info(f"Initializing supervisor (pid: {os.getpid()})")

        self._app_name = app_name
        self._config = {}
        self._nodes = {}
        self._error = None

        with open(PID_FILE, "w") as f:
            f.write(str(os.getpid()))

        self._logger.info("Opening command FIFOs")

        self._in = open(FIFO_IN, "r")
        self._out = open(FIFO_OUT, "w")

        self._load_config()

        self._current_index = max(self._config["nodes"].keys(), default=0)

        self._logger.info("Initialization complete")

    def _stop_nodes(self):
        self._logger.info("Stopping nodes")
        self.stop_node(all=True)

    def __del__(self):
        self._stop_nodes()

        self._logger.info("Cleaning up")
        self._in.close()
        self._out.close()
        os.unlink(PID_FILE)
        os.unlink(FIFO_IN)
        os.unlink(FIFO_OUT)

        self._logger.info("Supervisor shut down")

    def _save_config(self):
        with open(CONFIG_FILE, "w") as f:
            json.dump(self._config, f, indent=4)

    def _get_next_index(self):
        self._current_index += 1
        return self._current_index

    def _make_success(self, content=""):
        return {"success": True, "content": str(content)}

    def _make_fail(self, err):
        return {"success": False, "content": str(err)}

    def _load_config(self):
        self._logger.info(f"Loading config from {CONFIG_FILE}")
        try:
            with open(CONFIG_FILE, 'r') as f:
                self._config = json.load(f)
            # JSON mandates str map keys, convert to int for convenience and consistency
            self._config["nodes"] = {int(k): v for k, v in self._config["nodes"].items()}
        except Exception as e:
            self._error = f"Failed to load configuration from {CONFIG_FILE}: {str(e)}"
            self._logger.error(self._error)
            return self._make_fail(self._error)
        self._logger.debug(f"Loaded config: {json.dumps(self._config, indent=4)}")
        return self._make_success()

    def _get_node_config_item(self, i, key):
        node_config = self._config["nodes"][i]
        default_value = self._config["default_scylla_config"][key]
        return node_config.get(key, default_value)

    def _send_reply(self, res=None):
        if res is None:
            res = self._make_success()
        self._out.write(json.dumps(res) + '\n')
        self._out.flush()

    def _copy_scylla_files(self, i):
        node_dir = f"node{i}"
        scylla_path = self._get_node_config_item(i, "path")
        scylla_mode = self._get_node_config_item(i, "mode")
        shutil.copy(os.path.join(scylla_path, "build", scylla_mode, "scylla"), os.path.join(node_dir, "scylla"))
        shutil.copy(os.path.join(scylla_path, "conf", "scylla.yaml"), os.path.join(node_dir, "scylla.yaml"))
        node_api_dir = os.path.join(node_dir, "api-doc")
        if os.path.exists(node_api_dir):
            shutil.rmtree(node_api_dir)
        shutil.copytree(os.path.join(scylla_path, "api", "api-doc"), node_api_dir)

    def _add_node(self, clear_extra_args, **node_config_override):
        i = self._get_next_index()
        node_dir = f"node{i}"
        os.mkdir(node_dir)

        os.mkdir(os.path.join(node_dir, "data"))
        os.mkdir(os.path.join(node_dir, "commitlog"))
        os.mkdir(os.path.join(node_dir, "hints"))
        os.mkdir(os.path.join(node_dir, "view_hints"))

        node_ip = f"127.{self._config['network_prefix']}.0.{i}"

        default_node_config = self._config["default_scylla_config"]
        node_config = {
            "index": i,
            "ip": node_ip,
        }
        for item_name in default_node_config.keys():
            value = node_config_override.get(item_name)
            if item_name == "extra_args":
                if clear_extra_args == "yes":
                    node_config[item_name] = []
                elif len(value) > 0 and value != default_node_config[item_name]:
                    node_config[item_name] = value
            elif value is not None and value != default_node_config[item_name]:
                node_config[item_name] = value

        self._config["nodes"][i] = node_config

        self._logger.debug(f"node_config is {node_config}")

        self._copy_scylla_files(i)

        self._save_config()

        return i

    def add_node(self, start=False, nodes=1, clear_extra_args="no", **node_config_override):
        self._logger.info(f"add_node({start}, {nodes}, {node_config_override})")

        indexes = []
        for _ in range(nodes):
            indexes.append(self._add_node(clear_extra_args, **node_config_override))

        if start:
            for i in indexes:
                self._start_node(i)

        return self._make_success(f"Added node with index(es): {indexes}")

    def _apply_to_nodes(self, op_name, node_indexes, all, **additional_args):
        self._logger.info(f"{op_name}_node({node_indexes}, {all}, {additional_args})")

        indexes = list(self._config["nodes"].keys()) if all else list(map(int, node_indexes))

        if len(indexes) == 0:
            return self._make_fail("no node index was provided")

        op = getattr(self, f"_{op_name}_node")

        for i in indexes:
            if i not in self._config["nodes"]:
                return self._make_fail(f"failed to {op_name} node#{i}: node not found")
            try:
                op(i, **additional_args)
            except Exception as e:
                self._logger.error(f"failed to {op_name} node#{i}: {traceback.format_exc()}")
                return self._make_fail(f"failed to {op_name} node#{i}: {e}")

    def _start_node(self, i):
        if i in self._nodes:
            return

        node_config = self._config["nodes"][i]

        node_path = os.path.abspath(f"node{i}")
        node_smp = self._get_node_config_item(i, "smp")
        node_memory = self._get_node_config_item(i, "memory")
        node_ip = node_config["ip"]

        if self._nodes:
            min_node_index = min(self._nodes.keys())
            seed_node_ip = self._nodes[min_node_index].ip
        else:
            seed_node_ip = node_ip

        invoke_cmd = [os.path.join(node_path, "scylla"),
                      f"-c{node_smp}",
                      f"-m{node_memory}",
                      "--cluster-name", self._config['name'],
                      "--options-file", os.path.join(node_path, "scylla.yaml"),
                      "--developer-mode=1",
                      "--overprovisioned",
                      "--unsafe-bypass-fsync=1",
                      "--relaxed-dma",
                      "--listen-address", node_ip,
                      "--rpc-address", node_ip,
                      "--api-address", node_ip,
                      "--prometheus-address", node_ip,
                      "--seed-provider-parameters", f"seeds={seed_node_ip}",
                      "--data-file-directories", os.path.join(node_path, "data"),
                      "--commitlog-directory", os.path.join(node_path, "commitlog"),
                      "--hints-directory", os.path.join(node_path, "hints"),
                      "--view-hints-directory", os.path.join(node_path, "view_hints"),
                      "--api-ui-dir", os.path.join(node_path, "api-ui"),
                      "--api-doc-dir", os.path.join(node_path, "api-doc")]
        invoke_cmd += self._get_node_config_item(i, "extra_args")

        system_log_path = os.path.join(node_path, SCYLLA_LOG_FILE)
        with open(system_log_path, "w") as system_log:
            handle = subprocess.Popen(
                    invoke_cmd,
                    stdin=subprocess.DEVNULL,
                    stdout=system_log,
                    stderr=subprocess.STDOUT,
                    cwd=os.path.abspath(node_path))
            self._nodes[i] = Supervisor.Node(i, node_ip, handle)

        pattern = re.compile("init - Scylla version .* initialization completed.")
        match = None
        while match is None:
            with open(system_log_path, "r") as system_log:
                for line in system_log:
                    match = pattern.search(line)
                    if match is not None:
                        break
            rc = handle.poll()
            if rc is not None:
                raise RuntimeError(f"scylla process exited with non-zero exit code: {rc}")
            time.sleep(0.1)

    def start_node(self, node_index=[], all=False):
        return self._apply_to_nodes("start", node_index, all)

    def _stop_node(self, i):
        if i not in self._nodes:
            return

        handle = self._nodes[i].handle
        handle.terminate()
        try:
            handle.wait(timeout=30)
        except subprocess.TimeoutExpired:
            handle.kill()

        del self._nodes[i]

    def stop_node(self, node_index=[], all=False):
        return self._apply_to_nodes("stop", node_index, all)

    def _restart_node(self, i):
        self._stop_node(i)
        self._start_node(i)

    def restart_node(self, node_index=[], all=False):
        return self._apply_to_nodes("restart", node_index, all)

    def _upgrade_node(self, i, start="yes"):
        self._stop_node(i)
        self._copy_scylla_files(i)
        if start == "yes":
            self._start_node(i)

    def upgrade_node(self, node_index=[], all=False, start="yes"):
        return self._apply_to_nodes("upgrade", node_index, all, start=start)

    def _remove_node(self, i):
        self._stop_node(i)
        shutil.rmtree(f"node{i}")
        del self._config["nodes"][i]
        self._save_config()

    def remove_node(self, node_index=[], all=False):
        return self._apply_to_nodes("remove", node_index, all)

    def status(self):
        self._logger.info("status()")
        res = []
        for i, node in self._config["nodes"].items():
            node_data = {"index": i, "ip": node["ip"]}
            if i in self._nodes:
                handle = self._nodes[i].handle
                rc = handle.poll()
                if rc is None:
                    node_data["status"] = "UP"
                    node_data["pid"] = handle.pid
                else:
                    del self._nodes[i]
                    node_data["status"] = "FAILED"
                    node_data["return_code"] = rc
            else:
                node_data["status"] = "DOWN"
            res.append(node_data)

        return self._make_success(json.dumps(res, indent=4))

    def reload_config(self):
        self._logger.info("reload_config()")
        return self._load_config()

    def run(self):
        self._logger.info("Starting command listen loop")
        while True:
            command = self._in.readline().rstrip()

            if not command:
                time.sleep(0.2)
                continue

            if command == "check":
                self._logger.info("Check command received")
                if self._error is None:
                    self._send_reply()
                    continue
                else:
                    self._send_reply(self._make_fail(self._error))
                    break

            if command == "stop":
                self._logger.info("Stop command received")
                self._stop_nodes()
                self._send_reply()
                break

            self._logger.info(f"Executing command {command}")

            try:
                args = _parse_args(self._app_name, command.split(' '))
                op = args.operation.replace('-', '_')
                if not hasattr(self, op):
                    raise RuntimeError(f"Invalid supervisor command: {op}")
                args_dict = vars(args)
                del args_dict["operation"]
                del args_dict["cluster_directory"]
                self._send_reply(getattr(self, op)(**args_dict))
                self._logger.info("Command execution successful")
            except Exception as e:
                self._logger.error(f"Command execution failed: {traceback.format_exc()}")
                self._send_reply(self._make_fail(e))

        self._logger.info("Listen loop terminated")
        time.sleep(0.2)  # give time to the client to read the last response


def _get_fifo_paths(cluster_directory):
    sin = os.path.join(cluster_directory, FIFO_IN)
    sout = os.path.join(cluster_directory, FIFO_OUT)
    return sin, sout


class SupervisorClient:
    """Local client, for the supervisor

    Allows initializing a connection and sending commands to the supervisor.
    """

    def __init__(self, cluster_directory):
        """Initialize the client for a supervisor running in the specified cluster directory"""
        self.supervisor_in_fifo, self.supervisor_out_fifo = _get_fifo_paths(cluster_directory)

    def send_command(self, argv):
        """Send commands to the supervisor

        Only node commands are supported, e.g. commands ending with the *-node
        prefix as opposed to those with a *-cluster prefix.
        Pass the full command-line in the argv parameter, e.g.:

            client = simple_scylla_cluster_manager.SupervisorClient("/path/to/cluster_dir")
            client.send_command(["start-node", "-i1"])

        """
        if not os.path.exists(self.supervisor_in_fifo) or not os.path.exists(self.supervisor_out_fifo):
            raise RuntimeError(f"Failed to find supervisor input/output FIFO at {self.supervisor_in_fifo} and "
                               "{self.supervisor_out_fifo}, is the supervisor running?")

        with (open(self.supervisor_in_fifo, "w") as pout,
                open(self.supervisor_out_fifo, "r") as pin):
            pout.write(' '.join(argv) + '\n')
            pout.flush()

            res = json.loads(pin.readline())
            if res["success"]:
                return res.get("content", "")

            raise RuntimeError(f"Operation failed, supervisor responed with: {res['content']}")


class ClusterAlreadyStarted(RuntimeError):
    pass


def _start_supervisor(cluster_directory):
    if os.path.exists(os.path.join(cluster_directory, PID_FILE)):
        raise ClusterAlreadyStarted(f"{PID_FILE} already exists, is the supervisor running?")

    sin, sout = _get_fifo_paths(cluster_directory)

    os.mkfifo(sin)
    os.mkfifo(sout)

    with open(os.path.join(cluster_directory, LOG_FILE), "w") as supervisor_log:
        supervisor_process = subprocess.Popen(
                [sys.executable, os.path.abspath(__file__), "supervisor"],
                stdin=subprocess.DEVNULL,
                stdout=supervisor_log,
                stderr=subprocess.STDOUT,
                cwd=os.path.abspath(cluster_directory))

    supervisor_client = SupervisorClient(cluster_directory)

    # The supervisor can only successfully open the pipes if they are also
    # opened here with the opposite mode. Send a "check" command to coordinate
    # opening the pipes. This command also serves the purpose of checking if the
    # supervisor was successfully initialized.
    supervisor_client.send_command(["check"])

    return supervisor_process, supervisor_client


def _maybe_start_nodes(nodes, start_all, supervisor_client):
    if not start_all or nodes == 0:
        return

    for i in range(1, nodes + 1):
        supervisor_client.send_command(["start-node", f"-i{i}"])


class ExecuteCommandResult:
    def __init__(self, content="", supervisor_process=None, supervisor_client=None):
        self.content = content
        self.supervisor_process = supervisor_process
        self.supervisor_client = supervisor_client


def _execute_command(argv0, remaining_argv):
    app_name = os.path.basename(argv0)
    args = _parse_args(app_name, remaining_argv)

    operation = args.operation
    args_dict = vars(args)
    del args_dict["operation"]

    if operation == "create-cluster":
        os.symlink(os.path.abspath(argv0), os.path.join(args.cluster_directory, app_name))

        if args.network_prefix is None:
            # Avoid picking 0, to not clash with a scylla started with the default parameters
            network_prefix = random.randint(1, 255)
        else:
            network_prefix = args.network_prefix

        if args.path is None:
            path = os.path.abspath(os.path.join(os.path.dirname(argv0), '..'))
        else:
            path = os.path.abspath(args.path)

        config = {
            "name": args.name,
            "network_prefix": network_prefix,
            "default_scylla_config": {
                "extra_args": args.extra_args,
                "memory": args.memory,
                "mode": args.mode,
                "path": path,
                "smp": args.smp,
            },
            "nodes": {}
        }

        with open(os.path.join(args.cluster_directory, CONFIG_FILE), "w") as f:
            json.dump(config, f, indent=4)

        supervisor_process, supervisor_client = _start_supervisor(args.cluster_directory)

        supervisor_client.send_command(["add-node", "--nodes", str(args.nodes)])

        _maybe_start_nodes(args.nodes, args.start, supervisor_client)
        return ExecuteCommandResult("", supervisor_process, supervisor_client)
    elif operation == "start-cluster":
        supervisor_process, supervisor_client = _start_supervisor(args.cluster_directory)
        if args.start:
            nodes_status = json.loads(supervisor_client.send_command(['status']))
            _maybe_start_nodes(len(nodes_status), args.start, supervisor_client)
        return ExecuteCommandResult("", supervisor_process, supervisor_client)
    elif operation == "stop-cluster":
        try:
            with open(os.path.join(args.cluster_directory, PID_FILE)) as f:
                supervisor = psutil.Process(int(f.read().rstrip()))
        except OSError:
            return ExecuteCommandResult(f"{PID_FILE} is missing, is the supervisor stopped already?")
        supervisor_client = SupervisorClient(args.cluster_directory)
        supervisor_client.send_command(['stop'])
        supervisor.wait()
        return ExecuteCommandResult()
    else:
        supervisor_client = SupervisorClient(args.cluster_directory)
        content = supervisor_client.send_command(remaining_argv)
        return ExecuteCommandResult(content)


def execute_command(args):
    """Execute the command and return the results (if any)

    Handles all supported commands, unlike SupervisorClient, which talks to the
    supervisor directly, which can only handle node-related commands (but not
    cluster related ones).
    Returns ExecuteCommandResult.
    If an error is ecountered, an exception will be thrown.
    """
    return _execute_command(os.path.abspath(__file__), args)


if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == 'supervisor':
        supervisor = Supervisor(os.path.basename(sys.argv[0]))
        supervisor.run()
    else:
        try:
            res = _execute_command(sys.argv[0], sys.argv[1:])
            if res.content:
                print(res.content)
        except Exception as e:
            print(f"Failed to execute command: {e}")
            exit(1)
