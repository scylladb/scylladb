#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from test.pylib.scylla_cluster import get_cluster_manager
from test.pylib.suite.base import Test, run_test
from test.pylib.suite.python import  PythonTest, PythonTestSuite

if TYPE_CHECKING:
    import argparse


class TopologyTestSuite(PythonTestSuite):
    """A collection of Python pytests against Scylla instances dealing with topology changes.
       Instead of using a single Scylla cluster directly, there is a cluster manager handling
       the lifecycle of clusters and bringing up new ones as needed. The cluster health checks
       are done per test case.
    """

    async def add_test(self, shortname: str, casename: str) -> None:
        """Add test to suite"""
        test = TopologyTest(self.next_id((shortname, 'topology', self.mode)), shortname, casename, self)
        self.tests.append(test)

    def junit_tests(self):
        """Return an empty list, since topology tests are excluded from an aggregated Junit report to prevent double
        count in the CI report"""
        return []


class TopologyTest(PythonTest):
    """Run a pytest collection of cases against Scylla clusters handling topology changes"""
    status: bool

    def __init__(self, test_no: int, shortname: str, casename: str, suite) -> None:
        super().__init__(test_no, shortname, casename, suite)

    async def run(self, options: argparse.Namespace) -> Test:

        self._prepare_pytest_params(options)

        test_path = os.path.join(self.suite.options.tmpdir, self.mode)
        async with get_cluster_manager(self.uname, self.suite.clusters, test_path) as manager:
            self.args.insert(0, "--tmpdir={}".format(options.tmpdir))
            self.args.insert(0, "--manager-api={}".format(manager.sock_path))
            if options.artifacts_dir_url:
                self.args.insert(0, "--artifacts_dir_url={}".format(options.artifacts_dir_url))

            try:
                # Note: start manager here so cluster (and its logs) is available in case of failure
                await manager.start()
                self.success = await run_test(self, options)
            except Exception as e:
                self.server_log = manager.cluster.read_server_log()
                self.server_log_filename = manager.cluster.server_log_filename()
                if not manager.is_before_test_ok:
                    print("Test {} pre-check failed: {}".format(self.name, str(e)))
                    print("Server log of the first server:\n{}".format(self.server_log))
                    # Don't try to continue if the cluster is broken
                    raise
            manager.logger.info("Test %s %s", self.uname, "succeeded" if self.success else "failed ")
        return self
