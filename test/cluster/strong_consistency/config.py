#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Cluster and keyspace setup for strongly-consistent tests.

Forgetting either of the two things an SC test needs — ``consistency =
'global'`` on the keyspace, tablets rather than vnodes — fails *silently*:
what comes up is a perfectly good eventually-consistent keyspace, and the test
keeps passing while testing something else.  Hence one definition of each
here, and *what* makes a keyspace strongly consistent taken from
:class:`~test.cluster.util.FeatureConfigurations` rather than spelled out
again, so that the two cannot drift.  The experimental feature itself is on
for every server started on the version under test, with all the others (see
``make_scylla_conf()`` in ``test/pylib/scylla_server.py``), unless its test
sets an ``experimental_features`` list of its own, which replaces the default
one.  A server started on an older version and switched to the current one
keeps the older version's config, so it runs without it.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Mapping, Optional, Sequence

from test.cluster.util import FeatureConfigurations
from test.pylib.internal_types import ServerInfo
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.util import wait_for_cql_and_get_hosts

logger = logging.getLogger(__name__)

#: The one description of a strongly-consistent keyspace, shared with every
#: test parametrized over ``FeatureConfigurations``.
SC_FEATURE = FeatureConfigurations.STRONG_CONSISTENCY.value

# Logger levels that make an SC failure diagnosable from the test log.  Not
# part of the feature description: a test that merely runs *under* SC has no
# reason to want them.
SC_CMDLINE = [
    "--logger-log-level", "sc_groups_manager=debug",
    "--logger-log-level", "sc_coordinator=debug",
    "--logger-log-level", "sc_state_machine=debug",
    "--logger-log-level", "raft_groups_storage=debug",
]


def sc_keyspace_opts(replication_factor: int = 3, initial_tablets: int = 10) -> str:
    """CREATE KEYSPACE options for a strongly-consistent keyspace.

    ``consistency = 'global'`` is appended by the feature description rather
    than spelled here, so this function cannot disagree with it.
    """
    return SC_FEATURE.get_keyspace_opts(
        "WITH replication = "
        "{'class': 'NetworkTopologyStrategy', "
        f"'replication_factor': {replication_factor}}} "
        f"AND tablets = {{'initial': {initial_tablets}}}"
    )


async def boot_sc_cluster(
    manager: ScyllaClusterManager,
    num_nodes: int = 6,
    *,
    dc: str = "my_dc",
    config: Optional[Mapping[str, Any]] = None,
    cmdline: Optional[Sequence[str]] = None,
    cql_ready_timeout_s: float = 60,
) -> tuple[list[ServerInfo], Any]:
    """Start an SC-enabled cluster and wait until CQL is usable on every node.

    ``config`` and ``cmdline`` are the test's own additions, on top of what
    :data:`SC_FEATURE` requires and :data:`SC_CMDLINE`.  Returns
    ``(servers, cql)``.
    """
    logger.info("Bootstrapping cluster of %d nodes", num_nodes)
    servers = await manager.servers_add(
        num_nodes,
        config=SC_FEATURE.get_cluster_cfg(dict(config or {})),
        cmdline=SC_CMDLINE + list(cmdline or []),
        auto_rack_dc=dc,
    )

    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(
        cql, servers, time.time() + cql_ready_timeout_s)
    return servers, cql
