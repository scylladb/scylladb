#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from scripts import coverage
from test.pylib.suite.base import Test, TestSuite, path_to, read_log, run_test

if TYPE_CHECKING:
    import argparse


class RunTestSuite(TestSuite):
    """TestSuite for test directory with a 'run' script """

    def __init__(self, path: str, cfg, options: argparse.Namespace, mode: str) -> None:
        super().__init__(path, cfg, options, mode)
        self.scylla_exe = path_to(self.mode, "scylla")
        self.scylla_env = dict(self.base_env)
        if self.mode == "coverage":
            self.scylla_env = coverage.env(self.scylla_exe, distinct_id=self.name)

        self.scylla_env['SCYLLA'] = self.scylla_exe

    async def add_test(self, shortname, casename) -> None:
        test = RunTest(self.next_id((shortname, self.suite_key)), shortname, self)
        self.tests.append(test)

    @property
    def pattern(self) -> str:
        return "run"


class RunTest(Test):
    """Run tests in a directory started by a run script"""

    def __init__(self, test_no: int, shortname: str, suite) -> None:
        super().__init__(test_no, shortname, suite)
        self.path = suite.suite_path / shortname
        self.xmlout = os.path.join(suite.options.tmpdir, self.mode, "xml", self.uname + ".xunit.xml")
        self.args = [
            "--junit-xml={}".format(self.xmlout),
            "-o",
            "junit_suite_name={}".format(self.suite.name)
        ]
        self.args.append(f"--alluredir={self.allure_dir}")
        if not suite.options.save_log_on_success:
            self.args.append("--allure-no-capture")

    def print_summary(self) -> None:
        print("Output of {} {}:".format(self.path, " ".join(self.args)))
        print(read_log(self.log_filename))

    async def run(self, options: argparse.Namespace) -> Test:
        # This test can and should be killed gently, with SIGTERM, not with SIGKILL
        self.success = await run_test(self, options, gentle_kill=True, env=self.suite.scylla_env)
        logging.info("Test %s %s", self.uname, "succeeded" if self.success else "failed ")
        return self
