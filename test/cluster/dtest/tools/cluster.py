#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, TypedDict

import requests

from test.cluster.dtest.ccmlib.node import NodeError
from test.cluster.dtest.ccmlib.scylla_node import ScyllaNode
from test.cluster.dtest.ccmlib.utils.version import parse_version
from test.cluster.dtest.tools.marks import get_version

if TYPE_CHECKING:
    from test.cluster.dtest.ccmlib.cluster import Cluster
    from test.cluster.dtest.ccmlib.scylla_cluster import ScyllaCluster


logger = logging.getLogger(__name__)


def new_node(cluster: ScyllaCluster, bootstrap: bool = True,
             data_center: str | None = None,
             rack: str | None = None) -> ScyllaNode:
    if cluster.ccm_parity:
        # scylla-dtest's new_node(): an auto_bootstrap node added with cluster.add(),
        # which places it in the first node's datacenter when it names none, and
        # makes it a seed unless it bootstraps.
        node = cluster.new_node(len(cluster.nodes) + 1, auto_bootstrap=True, is_seed=not bootstrap,
                                data_center=data_center, rack=rack)
    elif data_center and rack:
        cluster.populate({data_center: {rack: 1}})
        node = cluster.nodelist()[-1]
    else:
        cluster.populate(1)
        node = cluster.nodelist()[-1]
    node.bootstrap = bootstrap
    return node


def run_rest_api(run_on_node: ScyllaNode, cmd, api_method: str = "post", params: dict | None = None):
    """
    :param api_method: post/get
    :param run_on_node: node to send the REST API command.
    :param cmd: api command to execute.
    :return: api-command-request result
    """
    cmd_prefix = f"http://{run_on_node.address()}:10000"
    full_cmd = cmd_prefix + cmd
    api_method = api_method.lower()
    logger.debug(f"Send restful api: {full_cmd}: api_method={api_method}")
    if api_method == "post":
        result = requests.post(full_cmd, params=params)
    elif api_method == "get":
        result = requests.get(full_cmd, params=params)
    elif api_method == "delete":
        result = requests.delete(full_cmd, params=params)
    else:
        raise Exception(f"Unknown request API method: {api_method}")
    try:
        result.raise_for_status()
    except requests.HTTPError as e:
        logger.info("failed to %s: '%s' (%s)", api_method, e, e.response.text)
        raise

    result_json = result.json() if result.text else "{}"
    logger.debug(f"API result: {result_json}")
    return result


# NOTE: the functions below are restored verbatim (imports aside) from
# scylla-dtest's tools/cluster.py; they were trimmed when this module was
# first ported in-tree, but not-yet-adapted dtest/unported test modules still
# import them.


class Group0Member(TypedDict):
    host_id: str
    is_voter: bool


class TokenRingMember(TypedDict):
    host_ip: str
    host_id: str


def get_token_ring_members(node: ScyllaNode) -> list[TokenRingMember]:
    token_ring_members = []
    result = run_rest_api(run_on_node=node, cmd="/storage_service/host_id", api_method="get")
    if not result.text:
        return []

    for member in result.json():
        token_ring_members.append({"host_ip": member.get("key"), "host_id": member.get("value")})

    return token_ring_members


def get_group0_members(node: ScyllaNode) -> list[Group0Member]:
    def _parse_cqlsh_output(output: tuple[str, str]) -> list[str]:
        result = []
        stdout, stderr = output
        if stderr:
            return []

        for line in stdout.strip().split("\n"):
            if not line.strip():
                break
            result.append(line.strip())

        if not result and len(result) < 2:
            return []
        return result[2:]

    group0_members = []
    output = node.run_cqlsh("select value from system.scylla_local where key = 'raft_group0_id'", return_output=True)
    result = _parse_cqlsh_output(output)
    if not result:
        return []
    raft_group0_id = result[0]

    output = node.run_cqlsh(f"select server_id, can_vote from system.raft_state where group_id = {raft_group0_id} and disposition = 'CURRENT'", return_output=True)
    result = _parse_cqlsh_output(output)
    if not result:
        return []
    for line in result:
        server_id, can_vote = line.split("|")
        can_vote = True if can_vote.strip() == "True" else False
        group0_members.append({"host_id": server_id.strip(), "is_voter": can_vote})

    return group0_members


def minimum_scylla_version(version, oss_version, enterprise_version):
    v = parse_version(version)
    if v >= parse_version("2000"):
        return v >= parse_version(enterprise_version)
    else:
        return v >= parse_version(oss_version)


def has_views_with_tablets_experimental_feature(cassandra_dir: str | None = None, scylla_version: str | None = None) -> bool:
    version = str(get_version(cassandra_dir=cassandra_dir, scylla_version=scylla_version))
    return minimum_scylla_version(version, "6.3-dev", "2025.1-dev")


def enable_views_with_tablets_experimental_feature(config: dict, cassandra_dir: str | None = None, scylla_version: str | None = None):
    do_enable = has_views_with_tablets_experimental_feature(cassandra_dir=cassandra_dir, scylla_version=scylla_version)
    if "experimental_features" in config:
        if do_enable and "views-with-tablets" not in config["experimental_features"]:
            config["experimental_features"].append("views-with-tablets")
        elif not do_enable and "views-with-tablets" in config["experimental_features"]:
            config["experimental_features"].remove("views-with-tablets")
    elif do_enable:
        config["experimental_features"] = ["views-with-tablets"]


def restart_cluster(cluster: Cluster, new_cluster_options: dict[str, Any] | None = None, jvm_args: list[str] | None = None, start_in_parallel: bool = False, already_stopped: bool = False) -> None:
    if not already_stopped:
        logger.debug("restart_cluster: stop the cluster")
        cluster.stop()
    if new_cluster_options:
        logger.debug("restart_cluster: set new configuration options")
        cluster.set_configuration_options(values=new_cluster_options)
    if start_in_parallel:
        marks = {}

        logger.debug("restart_cluster: start all cluster nodes almost simultaneously")
        for node in cluster.nodelist():
            marks[node] = node.mark_log()
            p = node.start(no_wait=True, jvm_args=jvm_args)
            if not node.is_running():
                raise NodeError(f"Error starting {node.name}.", p)

        logger.debug("restart_cluster: check that nodes started successfully and see each other")
        for node, mark in marks.items():
            rest_nodes = [other_node for other_node in marks if other_node != node]
            node.watch_log_for_alive(nodes=rest_nodes, from_mark=mark)
            node.watch_rest_for_alive(nodes=rest_nodes)
            node.watch_log_for("Starting listening for CQL clients", from_mark=mark)
    cluster.start(jvm_args=jvm_args)
