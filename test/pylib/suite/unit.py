#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import logging
import os
import shlex
from typing import TYPE_CHECKING

from scripts import coverage
from test.pylib.scylla_cluster import merge_cmdline_options
from test.pylib.suite.base import Test, TestSuite, palette, path_to, read_log, run_test

if TYPE_CHECKING:
    import argparse


class UnitTestSuite(TestSuite):
    """TestSuite instantiation for non-boost unit tests"""

    def __init__(self, path: str, cfg: dict, options: argparse.Namespace, mode: str) -> None:
        super().__init__(path, cfg, options, mode)
        # Map of custom test command line arguments, if configured
        self.custom_args = cfg.get("custom_args", {})
        self.extra_cmdline_options = cfg.get("extra_scylla_cmdline_options", [])
        # Map of tests that cannot run with compaction groups
        self.all_can_run_compaction_groups_except = cfg.get("all_can_run_compaction_groups_except")

    async def create_test(self, shortname, casename, suite, args):
        exe = path_to(suite.mode, "test", suite.name, shortname)
        if not os.access(exe, os.X_OK):
            print(palette.warn(f"Unit test executable {exe} not found."))
            return
        test = UnitTest(self.next_id((shortname, self.suite_key)), shortname, suite, args)
        self.tests.append(test)

    def prepare_arg(self, arg):
        extra_cmdline_options = ' '.join(self.extra_cmdline_options)
        return f'{arg} {extra_cmdline_options}'

    async def add_test(self, shortname, casename) -> None:
        """Create a UnitTest class with possibly custom command line
        arguments and add it to the list of tests"""
        # Skip tests which are not configured, and hence are not built
        if os.path.join("test", self.name, shortname) not in self.options.tests:
            return

        # Default seastar arguments, if not provided in custom test options,
        # are two cores and 2G of RAM
        args = self.custom_args.get(shortname, ["-c2 -m2G"])
        args = merge_cmdline_options(args, self.options.extra_scylla_cmdline_options)
        for a in args:
            await self.create_test(shortname, casename, self, self.prepare_arg(a))

    @property
    def pattern(self) -> str:
        # This should only match individual tests and not combined_tests.cc
        # file of the combined test.
        # It is because combined_tests.cc itself does not contain any tests.
        # To keep the code simple, we have avoided this by renaming
        # combined test file to “_tests.cc” instead of changing the match
        # pattern.
        return "*_test.cc"


class UnitTest(Test):
    standard_args = shlex.split("--overprovisioned --unsafe-bypass-fsync 1 "
                                "--kernel-page-cache 1 "
                                "--blocked-reactor-notify-ms 2000000 --collectd 0 "
                                "--max-networking-io-control-blocks=100 ")

    def __init__(self, test_no: int, shortname: str, suite, args: str) -> None:
        super().__init__(test_no, shortname, suite)
        self.path = path_to(self.mode, "test", suite.name, shortname.split('.')[0])
        self.args = shlex.split(args) + UnitTest.standard_args
        if self.mode == "coverage":
            self.env.update(coverage.env(self.path))

    def print_summary(self) -> None:
        print("Output of {} {}:".format(self.path, " ".join(self.args)))
        print(read_log(self.log_filename))

    async def run(self, options) -> Test:
        self.success = await run_test(self, options, env=self.env)
        logging.info("Test %s %s", self.uname, "succeeded" if self.success else "failed ")
        return self
