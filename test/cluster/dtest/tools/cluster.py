#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from test.cluster.dtest.ccmlib.scylla_node import ScyllaNode

if TYPE_CHECKING:
    from test.cluster.dtest.ccmlib.scylla_cluster import ScyllaCluster


logger = logging.getLogger(__name__)


def new_node(cluster: ScyllaCluster, bootstrap: bool = True) -> ScyllaNode:
    assert bootstrap is True, "bootstrap=True is supported only"

    return cluster.populate(1).nodelist()[-1]
