#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import requests

from test.cluster.dtest.ccmlib.scylla_node import ScyllaNode

if TYPE_CHECKING:
    from test.cluster.dtest.ccmlib.scylla_cluster import ScyllaCluster


logger = logging.getLogger(__name__)


def new_node(cluster: ScyllaCluster, bootstrap: bool = True) -> ScyllaNode:
    assert bootstrap is True, "bootstrap=True is supported only"

    return cluster.populate(1).nodelist()[-1]


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
