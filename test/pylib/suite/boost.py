#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import asyncio
import collections
import logging
import os
import shlex
import subprocess
import xml.etree.ElementTree as ET
from typing import TYPE_CHECKING

from scripts import coverage
from test.pylib.scylla_cluster import merge_cmdline_options
from test.pylib.suite.base import Test, palette, path_to, read_log, run_test
from test.pylib.suite.unit import UnitTest, UnitTestSuite

if TYPE_CHECKING:
    import argparse
    from collections.abc import Iterable
    from typing import Dict, List, Optional


TestPath = collections.namedtuple('TestPath', ['suite_name', 'test_name', 'case_name'])


class BoostTestSuite(UnitTestSuite):
    """TestSuite for boost unit tests"""

    # A cache of individual test cases, for which we have called
    # --list_content. Static to share across all modes.
    _case_cache: Dict[str, List[str]] = dict()

    _exec_name_cache: Dict[str, str] = dict()

    def _generate_cache(self, exec_path, exec_name) -> None:
        res = subprocess.run(
            [exec_path, '--list_content'],
            check=True,
            capture_output=True,
            env=dict(os.environ,
                     **{"ASAN_OPTIONS": "halt_on_error=0"}),
        )
        testname = None
        fqname = None
        for line in res.stderr.decode().splitlines():
            if not line.startswith('    '):
                testname = line.strip().rstrip('*')
                fqname = os.path.join(self.mode, self.name, testname)
                self._exec_name_cache[fqname] = exec_name
                self._case_cache[fqname] = []
            else:
                casename = line.strip().rstrip('*')
                if casename.startswith('_'):
                    continue
                self._case_cache[fqname].append(casename)

    def __init__(self, path, cfg: dict, options: argparse.Namespace, mode) -> None:
        super().__init__(path, cfg, options, mode)
        exec_name = 'combined_tests'
        exec_path = path_to(self.mode, "test", self.name, exec_name)
        # Apply combined test only for test/boost,
        # cache the tests only if the executable exists, so we can
        # run test.py with a partially built tree
        if self.name == 'boost' and os.path.exists(exec_path):
            self._generate_cache(exec_path, exec_name)

    async def create_test(self, shortname: str, casename: str, suite, args) -> None:
        fqname = os.path.join(self.mode, self.name, shortname)
        if fqname in self._exec_name_cache:
            execname = self._exec_name_cache[fqname]
            combined_test = True
        else:
            execname = None
            combined_test = False
        exe = path_to(suite.mode, "test", suite.name, execname if combined_test else shortname)
        if not os.access(exe, os.X_OK):
            print(palette.warn(f"Boost test executable {exe} not found."))
            return
        options = self.options
        allows_compaction_groups = self.all_can_run_compaction_groups_except != None and shortname not in self.all_can_run_compaction_groups_except
        if options.parallel_cases and (shortname not in self.no_parallel_cases) and casename is None:
            fqname = os.path.join(self.mode, self.name, shortname)
            # since combined tests are preloaded to self._case_cache, this will
            # only run in non-combined test mode
            if fqname not in self._case_cache:
                process = await asyncio.create_subprocess_exec(
                    exe, *['--list_content'],
                    stderr=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    env=dict(os.environ,
                             **{"ASAN_OPTIONS": "halt_on_error=0"}),
                    preexec_fn=os.setsid,
                )
                _, stderr = await asyncio.wait_for(process.communicate(), options.timeout)
                # --list_content produces the list of all test cases in the file. When BOOST_DATA_TEST_CASE is used it
                # will additionally produce the lines with numbers for each case preserving the function name like this:
                # test_singular_tree_ptr_sz*
                #     _0*
                #     _1*
                #     _2*
                # however, it's only possible to run test_singular_tree_ptr_sz that will execute all test cases
                # this line catches only test function name ignoring unrelated lines like '_0'
                # Note: this will ignore any test case starting with a '_' symbol
                case_list = [case[:-1] for case in stderr.decode().splitlines() if case.endswith('*') and not case.strip().startswith('_')]
                self._case_cache[fqname] = case_list

            case_list = self._case_cache[fqname]
            if len(case_list) == 1:
                test = BoostTest(self.next_id((shortname, self.suite_key)), shortname, suite, args, None, allows_compaction_groups, execname)
                self.tests.append(test)
            else:
                for case in case_list:
                    test = BoostTest(self.next_id((shortname, self.suite_key, case)), shortname, suite, args, case, allows_compaction_groups, execname)
                    self.tests.append(test)
        else:
            test = BoostTest(self.next_id((shortname, self.suite_key)), shortname, suite, args, casename, allows_compaction_groups, execname)
            self.tests.append(test)

    async def add_test(self, shortname, casename) -> None:
        """Create a UnitTest class with possibly custom command line
        arguments and add it to the list of tests"""
        fqname = os.path.join(self.mode, self.name, shortname)
        if fqname in self._exec_name_cache:
            execname = self._exec_name_cache[fqname]
            combined_test = True
        else:
            combined_test = False
        # Skip tests which are not configured, and hence are not built
        if os.path.join("test", self.name, execname if combined_test else shortname) not in self.options.tests:
            return
        # Default seastar arguments, if not provided in custom test options,
        # are two cores and 2G of RAM
        args = self.custom_args.get(shortname, ["-c2 -m2G"])
        args = merge_cmdline_options(args, self.options.extra_scylla_cmdline_options)
        for a in args:
            await self.create_test(shortname, casename, self, self.prepare_arg(a))

    def junit_tests(self) -> Iterable['Test']:
        """Boost tests produce an own XML output, so are not included in a junit report"""
        return []

    def boost_tests(self) -> Iterable['Tests']:
        return self.tests


class BoostTest(Test):
    """A unit test which can produce its own XML output"""

    standard_args = shlex.split("--overprovisioned --unsafe-bypass-fsync 1 "
                                "--kernel-page-cache 1 "
                                "--blocked-reactor-notify-ms 2000000 --collectd 0 "
                                "--max-networking-io-control-blocks=100 ")

    def __init__(self, test_no: int, shortname: str, suite, args: str,
                 casename: Optional[str], allows_compaction_groups : bool, execname: Optional[str]) -> None:
        boost_args = []
        combined_test = True if execname else False
        _shortname = shortname
        if casename:
            shortname += '.' + casename
            if combined_test:
                boost_args += ['--run_test=' + _shortname + '/' + casename]
            else:
                boost_args += ['--run_test=' + casename]
        else:
            if combined_test:
                boost_args += ['--run_test=' + _shortname]

        super().__init__(test_no, shortname, suite)
        if combined_test:
            self.path = path_to(self.mode, "test", suite.name, execname)
        else:
            self.path = path_to(self.mode, "test", suite.name, shortname.split('.')[0])
        self.args = shlex.split(args) + UnitTest.standard_args
        if self.mode == "coverage":
            self.env.update(coverage.env(self.path))

        self.xmlout = os.path.join(suite.options.tmpdir, self.mode, "xml", self.uname + ".xunit.xml")
        boost_args += ['--report_level=no',
                       '--logger=HRF,test_suite:XML,test_suite,' + self.xmlout]
        boost_args += ['--catch_system_errors=no']  # causes undebuggable cores
        boost_args += ['--color_output=false']
        boost_args += ['--']
        self.args = boost_args + self.args
        self.casename = casename
        self.__test_case_elements: list[ET.Element] = []
        self.allows_compaction_groups = allows_compaction_groups

    def reset(self) -> None:
        """Reset the test before a retry, if it is retried as flaky"""
        super().reset()
        self.__test_case_elements = []

    def get_test_cases(self) -> list[ET.Element]:
        if not self.__test_case_elements:
            self.__parse_logger()
        return self.__test_case_elements

    @staticmethod
    def test_path_of_element(test: ET.Element) -> TestPath:
        path = test.attrib['path']
        prefix, case_name = path.rsplit('::', 1)
        suite_name, test_name = prefix.split('.', 1)
        return TestPath(suite_name, test_name, case_name)

    def __parse_logger(self) -> None:
        def attach_path_and_mode(test):
            # attach the "path" to the test so we can group the tests by this string
            test_name = test.attrib['name']
            prefix = self.name.replace(os.path.sep, '.')
            test.attrib['path'] = f'{prefix}::{test_name}'
            test.attrib['mode'] = self.mode
            return test

        try:
            root = ET.parse(self.xmlout).getroot()
            # only keep the tests which actually ran, the skipped ones do not have
            # TestingTime tag in the corresponding TestCase tag.
            self.__test_case_elements = map(attach_path_and_mode,
                                            root.findall(".//TestCase[TestingTime]"))
            os.unlink(self.xmlout)
        except ET.ParseError as e:
            message = palette.crit(f"failed to parse XML output '{self.xmlout}': {e}")
            if e.msg.__contains__("no element found"):
                message = palette.crit(f"Empty testcase XML output, possibly caused by a crash in the cql_test_env.cc, "
                                       f"details: '{self.xmlout}': {e}")
            print(f"error: {self.name}: {message}")

    def check_log(self, trim: bool) -> None:
        self.__parse_logger()
        super().check_log(trim)

    async def run(self, options):
        if options.random_seed:
            self.args += ['--random-seed', options.random_seed]
        if self.allows_compaction_groups and options.x_log2_compaction_groups:
            self.args += [ "--x-log2-compaction-groups", str(options.x_log2_compaction_groups) ]
        self.success = await run_test(self, options, env=self.env)
        logging.info("Test %s %s", self.uname, "succeeded" if self.success else "failed ")
        return self

    def write_junit_failure_report(self, xml_res: ET.Element) -> None:
        """Does not write junit report for Jenkins legacy reasons"""
        assert False

    def print_summary(self) -> None:
        print("Output of {} {}:".format(self.path, " ".join(self.args)))
        print(read_log(self.log_filename))
