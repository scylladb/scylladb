#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from cassandra.connection import DRIVER_NAME, DRIVER_VERSION

from test.cluster.dtest.ccmlib import scylla_repository


class DTestConfig:
    def __init__(self):
        self.use_vnodes = True
        self.num_tokens = -1
        self.experimental_features = []
        self.tablets = False
        self.scylla_features = set()
        self.cassandra_dir = None
        self.cassandra_version = None
        self._scylla_version = None

    def setup(self, request):
        self.use_vnodes = request.config.getoption("--use-vnodes")
        self.num_tokens = request.config.getoption("--num-tokens")
        self.experimental_features = request.config.getoption("--experimental-features") or set()
        self.tablets = request.config.getoption("--tablets", default=False)
        self.scylla_features = request.config.scylla_features
        if build_modes := getattr(request.config, "build_modes", None):
            # So that a debug run upgrades from a debug relocatable package.
            scylla_repository.set_build_mode(build_modes[0])

    @property
    def scylla_version(self):
        """Which Scylla the cluster runs.

        The build under test, unless an upgrade test overrides the dtest_config
        fixture to start the cluster on an older release (see
        UpgradeTester.dtest_config). Resolved on demand, so that collecting tests
        in a tree that has not been built yet still works.
        """
        if self._scylla_version is None:
            self._scylla_version = scylla_repository.current_version()
        return self._scylla_version

    @scylla_version.setter
    def scylla_version(self, version):
        self._scylla_version = version

    def get_version_from_build(self):
        """The version of the Scylla this config points at, e.g. "2025.1.15".

        ccm reads it out of the install dir; here scylla_repository knows it
        already. Version-gated markers (tools/marks.get_version()) go through
        this, which is why it has to answer for an older release too.
        """
        _, version = scylla_repository.setup(self.scylla_version)
        return version

    @property
    def is_scylla(self):
        return True

    @property
    def driver_version(self):
        if "scylla" in DRIVER_NAME.lower():
            return f"scylla-driver=={DRIVER_VERSION}"
        else:
            return f"cassandra-driver=={DRIVER_VERSION}"
