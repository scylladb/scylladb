#
# Copyright (c) 2014 Bruno Oliveira
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# 
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from __future__ import annotations

import asyncio
import collections
import io
import logging
import os
import subprocess
import time
from collections.abc import Sequence
from functools import cache
from pathlib import Path
from xml.etree import ElementTree
from xml.etree.ElementTree import ParseError

import allure

from test import TOP_SRC_DIR, BUILD_DIR
from test.pylib.cpp.facade import CppTestFacade, CppTestFailure
from test.pylib.cpp.util import make_test_object
from test.pylib.resource_gather import get_resource_gather

TIMEOUT_DEBUG = 60 * 5 # seconds
TIMEOUT = 60 * 2 # seconds
COMBINED_TESTS = Path('test/boost/combined_tests')
logger = logging.getLogger(__name__)


class BoostTestFacade(CppTestFacade):
    """
    Facade for BoostTests that's responsible for discovering test functions and executing them correctly.
    """

    def list_tests(
        self,
        executable: Path,
        no_parallel: bool,
    ) -> tuple[bool, list[str]]:
        """
        Return a boolean value indicating whether the tests combined or not and the list of tests
        """
        if no_parallel:
            return False, [os.path.basename(os.path.splitext(executable)[0])]
        else:
            if not os.path.isfile(executable):
                return True, self.combined_suites[executable.stem]
            args = [executable, '--list_content']
            try:
                output = subprocess.check_output(
                    args,
                    stderr=subprocess.STDOUT,
                    universal_newlines=True,
                )
            except subprocess.CalledProcessError as e:
                output = e.output
            # --list_content produces the list of all test cases in the file. When BOOST_DATA_TEST_CASE is used it
            # additionally produce the lines with numbers for each case preserving the function name like this:
            # test_singular_tree_ptr_sz*
            #     _0*
            #     _1*
            #     _2*
            # however, it's only possible to run test_singular_tree_ptr_sz that executes all test cases
            # this line catches only test function name ignoring unrelated lines like '_0'
            # Note: this ignores any test case starting with a '_' symbol
            return False, [case[:-1] for case in output.splitlines() if
                         case.endswith('*') and not case.strip().startswith('_')]

    def run_test(
        self,
        executable: Path,
        original_name: str,
        test_name: str,
        mode: str,
        file_name: Path,
        test_args:Sequence[str] = (),
        env: dict = None,
    ) -> tuple[list[CppTestFailure], str] | tuple[None, str]:
        test = make_test_object(test_name, file_name.parent.name, mode, original_name)
        resource_gather = get_resource_gather(self.gather_metrics, test=test, tmpdir=str(self.temp_dir))
        resource_gather.make_cgroup()
        def read_file(name: Path) -> str:
            try:
                with io.open(name) as f:
                    return f.read()
            except IOError:
                return ''

        timeout = TIMEOUT_DEBUG if mode=='debug' else TIMEOUT
        root_log_dir = self.temp_dir / mode
        log_xml = root_log_dir / f"{test_name}.log"
        stdout_file_path = root_log_dir/ f"{test_name}_stdout.log"
        report_xml = root_log_dir / f"{test_name}.xml"
        args = [ str(executable),
                 '--output_format=XML',
                 '--report_level=no',
                 f"--report_sink={report_xml}",
                 f"--log_sink={log_xml}",
                 '--catch_system_errors=no',
                 '--color_output=false',
                 ]
        if original_name != Path(executable).stem:
            if executable.stem == COMBINED_TESTS.stem:
                args.append(f"--run_test={file_name.stem}/{original_name}")
            else:
                args.append(f"--run_test={original_name}")
        # Tests are written in the way that everything after '--' passes to the test itself rather than to the test framework
        args.append('--')
        args.extend(test_args)
        os.chdir(TOP_SRC_DIR)

        p,out = resource_gather.run_process(args, timeout, env)

        with open(stdout_file_path, 'w') as fd:
            fd.write(out)
        log = read_file(log_xml)
        report = read_file(report_xml)

        metrics = resource_gather.get_test_metrics()
        test_passed = p.returncode == 0
        resource_gather.write_metrics_to_db(metrics, success=test_passed)
        resource_gather.remove_cgroup()

        try:
            results = self._parse_log(log=log)
        except ParseError:
            logger.warning('Error parsing the log_sink output. Can be empty or invalid')
            results = None

        if p.returncode != 0:
            allure.attach(out, name='output', attachment_type=allure.attachment_type.TEXT)
            msg = (
                'working_dir: {working_dir}\n'
                'Internal Error: calling {executable} '
                'for test {test_id} failed (return_code={return_code}):\n'
                'output file:{stdout}\n'
                'log:{log}\n'
                'report:{report}\n'
                'command to repeat:{command}'
            )
            failure = CppTestFailure(
                file_name.name,
                line_num=results[0].line_num,
                contents=msg.format(
                    working_dir=os.getcwd(),
                    executable=executable,
                    test_id=test_name,
                    stdout=stdout_file_path.absolute(),
                    log=log,
                    report=report,
                    command=' '.join(p.args),
                    return_code=p.returncode,
                ),
            )
            return [failure], out

        report_xml.unlink(missing_ok=True)
        log_xml.unlink(missing_ok=True)

        if results:
            return results, out

        return None, out

    def _parse_log(self, log: str) -> list[CppTestFailure]:
        """
        Parse the 'log' section produced by BoostTest.

        This is always an XML file, and from this it's possible to parse most of the
        failures possible when running BoostTest.
        """
        parsed_elements = []

        log_root = ElementTree.fromstring(log)

        if log_root is not None:
            parsed_elements.extend(log_root.findall('Exception'))
            parsed_elements.extend(log_root.findall('Error'))
            parsed_elements.extend(log_root.findall('FatalError'))

        result = []
        for elem in parsed_elements:
            last_checkpoint = elem.find('LastCheckpoint')
            if last_checkpoint is not None:
                elem = last_checkpoint
            file_name = elem.attrib['file']
            line_num = int(elem.attrib['line'])
            result.append(CppTestFailure(file_name, line_num, elem.text or ''))
        return result


@cache
def get_combined_tests(session):
    suites = collections.defaultdict()
    modes = session.config.getoption('modes')
    if 'dev' in modes:
        executable = BUILD_DIR / 'dev' / COMBINED_TESTS
    else:
        executable = BUILD_DIR / modes[0] / COMBINED_TESTS
    args = [executable, '--list_content']

    output = subprocess.check_output(
        args,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )
    current_suite = ''
    for line in output.splitlines():
        if not line.startswith('    '):
            current_suite = line.strip().rstrip('*')
            suites[current_suite] = []
        else:
            case_name = line.strip().rstrip('*')
            suites[current_suite].append(case_name)
    return suites
