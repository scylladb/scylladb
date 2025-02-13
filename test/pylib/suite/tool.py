#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import logging
import os
import xml.etree.ElementTree as ET
from typing import TYPE_CHECKING

from test.pylib.suite.base import  Test, TestSuite,run_test
from test.pylib.util import LogPrefixAdapter

if TYPE_CHECKING:
    import argparse


class ToolTestSuite(TestSuite):
    """A collection of Python pytests that test tools

    These tests do not need an cluster setup for them. They invoke scylla
    manually, in tool mode.
    """

    def __init__(self, path, cfg: dict, options: argparse.Namespace, mode: str) -> None:
        super().__init__(path, cfg, options, mode)

    @property
    def pattern(self) -> str:
        return ["*_test.py", "test_*.py"]

    async def add_test(self, shortname, casename) -> None:
        test = ToolTest(self.next_id((shortname, self.suite_key)), shortname, self)
        self.tests.append(test)


class ToolTest(Test):
    """Run a collection of pytest test cases

    That do not need a scylla cluster set-up for them."""

    def __init__(self, test_no: int, shortname: str, suite) -> None:
        super().__init__(test_no, shortname, suite)
        launcher = self.suite.cfg.get("launcher", "pytest")
        self.path = launcher.split(maxsplit=1)[0]
        self.xmlout = os.path.join(self.suite.options.tmpdir, self.mode, "xml", self.uname + ".xunit.xml")

    def _prepare_pytest_params(self, options: argparse.Namespace):
        launcher = self.suite.cfg.get("launcher", "pytest")
        self.args = launcher.split()[1:]
        self.args += [
            "-s",  # don't capture print() output inside pytest
            "--log-level=DEBUG",   # Capture logs
            "-o",
            "junit_family=xunit2",
            "--junit-xml={}".format(self.xmlout),
            "--mode={}".format(self.mode),
            "--run_id={}".format(self.id)
        ]
        self.args.append(f"--alluredir={self.allure_dir}")
        if not options.save_log_on_success:
            self.args.append("--allure-no-capture")
        if options.markers:
            self.args.append(f"-m={options.markers}")

            # https://docs.pytest.org/en/7.1.x/reference/exit-codes.html
            no_tests_selected_exit_code = 5
            self.valid_exit_codes = [0, no_tests_selected_exit_code]
        self.args.append(str(self.suite.suite_path / (self.shortname + ".py")))

    def print_summary(self) -> None:
        print("Output of {} {}:".format(self.path, " ".join(self.args)))

    async def run(self, options: argparse.Namespace) -> Test:
        self._prepare_pytest_params(options)

        loggerPrefix = self.mode + '/' + self.uname
        logger = LogPrefixAdapter(logging.getLogger(loggerPrefix), {'prefix': loggerPrefix})
        self.success = await run_test(self, options)
        logger.info("Test %s %s", self.uname, "succeeded" if self.success else "failed ")
        return self

    def write_junit_failure_report(self, xml_res: ET.Element) -> None:
        super().write_junit_failure_report(xml_res)
