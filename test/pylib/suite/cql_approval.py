#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import difflib
import filecmp
import logging
import os
import pathlib
import shutil
import time
import xml.etree.ElementTree as ET
from io import StringIO
from typing import TYPE_CHECKING

from test.pylib.suite.base import Test, palette, read_log, run_test
from test.pylib.suite.python import PythonTestSuite
from test.pylib.util import LogPrefixAdapter

if TYPE_CHECKING:
    import argparse
    from typing import Dict, Optional


class CQLApprovalTestSuite(PythonTestSuite):
    """Run CQL commands against a single Scylla instance"""

    def __init__(self, path, cfg, options: argparse.Namespace, mode) -> None:
        super().__init__(path, cfg, options, mode)

    async def add_test(self, shortname: str, casename: str) -> None:
        test = CQLApprovalTest(self.next_id((shortname, self.suite_key)), shortname, self)
        self.tests.append(test)

    @property
    def pattern(self) -> str:
        return "*test.cql"


class CQLApprovalTest(Test):
    """Run a sequence of CQL commands against a standalone Scylla"""

    def __init__(self, test_no: int, shortname: str, suite) -> None:
        super().__init__(test_no, shortname, suite)
        # Path to cql_repl driver, in the given build mode
        self.path = "pytest"
        self.cql = suite.suite_path / (self.shortname + ".cql")
        self.result = suite.suite_path / (self.shortname + ".result")
        self.tmpfile = os.path.join(suite.options.tmpdir, self.mode, self.uname + ".reject")
        self.reject = suite.suite_path / (self.shortname + ".reject")
        self.server_log: Optional[str] = None
        self.server_log_filename: Optional[pathlib.Path] = None
        self.is_before_test_ok = False
        self.is_executed_ok = False
        self.is_new = False
        self.is_after_test_ok = False
        self.is_equal_result = False
        self.summary = "not run"
        self.unidiff: Optional[str] = None
        self.server_log = None
        self.server_log_filename = None
        self.env: Dict[str, str] = dict()
        self._prepare_args(suite.options)

    def reset(self) -> None:
        """Reset the test before a retry, if it is retried as flaky"""
        super().reset()
        self.is_before_test_ok = False
        self.is_executed_ok = False
        self.is_new = False
        self.is_after_test_ok = False
        self.is_equal_result = False
        self.summary = "not run"
        self.unidiff = None
        self.server_log = None
        self.server_log_filename = None
        self.env = dict()
        old_tmpfile = pathlib.Path(self.tmpfile)
        if old_tmpfile.exists():
            old_tmpfile.unlink()


    def _prepare_args(self, options: argparse.Namespace):
        self.args = [
            "-s",  # don't capture print() inside pytest
            "test/pylib/cql_repl/cql_repl.py",
            "--input={}".format(self.cql),
            "--output={}".format(self.tmpfile),
            "--run_id={}".format(self.id),
            "--mode={}".format(self.mode),
        ]
        self.args.append(f"--alluredir={self.allure_dir}")
        if not options.save_log_on_success:
            self.args.append("--allure-no-capture")

    async def run(self, options: argparse.Namespace) -> Test:
        self.success = False
        self.summary = "failed"

        loggerPrefix = self.mode + '/' + self.uname
        logger = LogPrefixAdapter(logging.getLogger(loggerPrefix), {'prefix': loggerPrefix})
        def set_summary(summary):
            self.summary = summary
            log_func = logger.info if self.success else logger.error
            log_func("Test %s %s", self.uname, summary)
            if self.server_log is not None:
                logger.info("Server log:\n%s", self.server_log)

        # TODO: consider dirty_on_exception=True
        async with (cm := self.suite.clusters.instance(False, logger)) as cluster:
            try:
                cluster.before_test(self.uname)
                logger.info("Leasing Scylla cluster %s for test %s", cluster, self.uname)
                self.args.insert(1, "--host={}".format(cluster.endpoint()))
                # If pre-check fails, e.g. because Scylla failed to start
                # or crashed between two tests, fail entire test.py
                self.is_before_test_ok = True
                cluster.take_log_savepoint()
                self.is_executed_ok = await run_test(self, options, env=self.env)
                cluster.after_test(self.uname, self.is_executed_ok)
                cm.dirty = cluster.is_dirty
                self.is_after_test_ok = True

                if not self.is_executed_ok:
                    set_summary("""returned non-zero return status.\n
Check test log at {}.""".format(self.log_filename))
                elif not os.path.isfile(self.tmpfile):
                    set_summary("failed: no output file")
                elif not os.path.isfile(self.result):
                    set_summary("failed: no result file")
                    self.is_new = True
                else:
                    self.is_equal_result = filecmp.cmp(self.result, self.tmpfile)
                    if not self.is_equal_result:
                        self.unidiff = format_unidiff(str(self.result), self.tmpfile)
                        set_summary("failed: test output does not match expected result")
                        assert self.unidiff is not None
                        logger.info("\n{}".format(palette.nocolor(self.unidiff)))
                    else:
                        self.success = True
                        set_summary("succeeded")
            except Exception as e:
                # Server log bloats the output if we produce it in all
                # cases. So only grab it when it's relevant:
                # 1) failed pre-check, e.g. start failure
                # 2) failed test execution.
                if not self.is_executed_ok:
                    self.server_log = cluster.read_server_log()
                    self.server_log_filename = cluster.server_log_filename()
                    if not self.is_before_test_ok:
                        set_summary("pre-check failed: {}".format(e))
                        print("Test {} {}".format(self.name, self.summary))
                        print("Server log  of the first server:\n{}".format(self.server_log))
                        # Don't try to continue if the cluster is broken
                        raise
                set_summary("failed: {}".format(e))
            finally:
                if os.path.exists(self.tmpfile):
                    if self.is_executed_ok and (self.is_new or not self.is_equal_result):
                        # Move the .reject file close to the .result file
                        # so that it's easy to analyze the diff or overwrite .result
                        # with .reject.
                        shutil.move(self.tmpfile, self.reject)
                    else:
                        pathlib.Path(self.tmpfile).unlink()

        return self

    def print_summary(self) -> None:
        print("Test {} ({}) {}".format(palette.path(self.name), self.mode,
                                       self.summary))
        if not self.is_executed_ok:
            print(read_log(self.log_filename))
            if self.server_log is not None:
                print("Server log of the first server:")
                print(self.server_log)
        elif not self.is_equal_result and self.unidiff:
            print(self.unidiff)

    def write_junit_failure_report(self, xml_res: ET.Element) -> None:
        assert not self.success
        xml_fail = ET.SubElement(xml_res, 'failure')
        xml_fail.text = self.summary
        if not self.is_executed_ok:
            if self.log_filename.exists():
                system_out = ET.SubElement(xml_res, 'system-out')
                system_out.text = read_log(self.log_filename)
            if self.server_log_filename:
                system_err = ET.SubElement(xml_res, 'system-err')
                system_err.text = read_log(self.server_log_filename)
        elif self.unidiff:
            system_out = ET.SubElement(xml_res, 'system-out')
            system_out.text = palette.nocolor(self.unidiff)


def format_unidiff(fromfile: str, tofile: str) -> str:
    with open(fromfile, "r") as frm, open(tofile, "r") as to:
        buf = StringIO()
        diff = difflib.unified_diff(
            frm.readlines(),
            to.readlines(),
            fromfile=fromfile,
            tofile=tofile,
            fromfiledate=time.ctime(os.stat(fromfile).st_mtime),
            tofiledate=time.ctime(os.stat(tofile).st_mtime),
            n=10)           # Number of context lines

        for i, line in enumerate(diff):
            if i > 60:
                break
            if line.startswith('+'):
                line = palette.diff_in(line)
            elif line.startswith('-'):
                line = palette.diff_out(line)
            elif line.startswith('@'):
                line = palette.diff_mark(line)
            buf.write(line)
        return buf.getvalue()
