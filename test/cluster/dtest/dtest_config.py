#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from cassandra.connection import DRIVER_NAME, DRIVER_VERSION


class DTestConfig:
    def __init__(self):
        self.use_vnodes = True
        self.num_tokens = -1
        self.experimental_features = []
        self.tablets = False
        self.force_gossip_topology_changes = False
        self.scylla_features = set()

    def setup(self, request):
        self.use_vnodes = request.config.getoption("--use-vnodes")
        self.num_tokens = request.config.getoption("--num-tokens")
        self.experimental_features = request.config.getoption("--experimental-features") or set()
        self.tablets = request.config.getoption("--tablets", default=False)
        self.force_gossip_topology_changes = request.config.getoption("--force-gossip-topology-changes", default=False)
        self.scylla_features = request.config.scylla_features

    @property
    def is_scylla(self):
        return True

    @property
    def driver_version(self):
        if "scylla" in DRIVER_NAME.lower():
            return f"scylla-driver=={DRIVER_VERSION}"
        else:
            return f"cassandra-driver=={DRIVER_VERSION}"
