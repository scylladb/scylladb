#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from test.dtest.ccmlib.scylla_node import ScyllaNode

if TYPE_CHECKING:
    from test.dtest.ccmlib.scylla_cluster import ScyllaCluster
    from test.dtest.ccmlib.scylla_node import ScyllaNode


logger = logging.getLogger(__name__)


def new_node(cluster: ScyllaCluster, bootstrap: bool = True) -> ScyllaNode:
    return cluster.new_node(-1)
