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

import collections
import io
import logging
import os
import subprocess
from collections.abc import Sequence
from functools import cache
from pathlib import Path
from xml.etree import ElementTree
from xml.etree.ElementTree import ParseError

import allure
from pytest import Config
from test import BUILD_DIR, COMBINED_TESTS
from test.pylib.cpp.common_cpp_conftest import get_modes_to_run
from test.pylib.cpp.facade import CppTestFacade, CppTestFailure

logger = logging.getLogger(__name__)

class BoostTestFacade(CppTestFacade):
    """
    Facade for BoostTests that's responsible for discovering test functions and executing them correctly.
    """

    def list_tests(
        self,
        executable: Path,
        no_parallel: bool,
        mode: str
    ) -> tuple[bool, list[str]]:
        """
        Return a boolean value indicating whether the tests combined or not and the list of tests
        """
        if no_parallel:
            return False, [os.path.basename(os.path.splitext(executable)[0])]
        else:
            if not os.path.isfile(executable):

                if not self.combined_suites[mode]:
                    raise FileNotFoundError(f"Executable file for test {executable.stem} for {mode} is not found. "
                                            f"It can be part of combined tests, but combined tests binary is absent.")
                if executable.stem not in self.combined_suites[mode]:
                    raise FileNotFoundError(
                        f"Binary for test {executable.stem} does not exist nor is it listed in the combined suites for "
                        f"mode {mode}. Probably a typo or test not annotated with BOOST_AUTO_TEST_CASE")
                return True, self.combined_suites[mode][executable.stem]
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
            # this line catches only test function name ignoring lines like '_0', so it will count test with dataprovider
            # as one test case.
            # Note: this ignores any test case starting with a '_' symbol
            # TODO: add support for test cases with dataprovider
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
        def read_file(name: Path) -> str:
            try:
                with io.open(name) as f:
                    return f.read()
            except IOError:
                return ''
        root_log_dir = self.temp_dir / mode
        log_xml = root_log_dir / f"{test_name}.{self.run_id}.xml"
        args = [ str(executable),
                 '--report_level=no',
                 '--output_format=XML',
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
        if self.random_seed:
            args.append(f'--random-seed={self.random_seed}')
        args.extend(test_args)
        test_passed, stdout_file_path, return_code = self.run_process(test_name, mode, file_name, args, env)

        log = read_file(log_xml)

        try:
            results = self._parse_log(log=log)
        except ParseError:
            logger.warning('Error parsing the log_sink output. Can be empty or invalid')
            results = None

        if not test_passed:
            allure.attach(stdout_file_path.read_bytes(), name='output', attachment_type=allure.attachment_type.TEXT)
            msg = (
                f'working_dir: {os.getcwd()}\n'
                f'Internal Error: calling {executable} '
                f'for test {test_name} failed ({return_code=}):\n'
                f'output file:{stdout_file_path.absolute()}\n'
                f'log:{log}\n'
                f'command to repeat:{" ".join(args)}\n'
            )
            failure = CppTestFailure(
                file_name.name,
                line_num=results[0].line_num if results else -1,
                contents=msg
            )
            return [failure], ''

        if not self.save_log_on_success:
            log_xml.unlink(missing_ok=True)
            stdout_file_path.unlink(missing_ok=True)

        if results:
            return results, ''

        return None, ''

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
def get_combined_tests(config: Config):
    suites = collections.defaultdict()
    modes = get_modes_to_run(config)
    for mode in modes:
        suites[mode] = collections.defaultdict()
        executable = BUILD_DIR / mode / COMBINED_TESTS

        args = [executable, '--list_content']

        if not os.path.isfile(executable):
            logger.warning(f"Combined test executable {executable} does not exist. Skipping boost test discovery of combined_tests.")
            continue
        output = subprocess.check_output(
            args,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
        )
        current_suite = ''
        for line in output.splitlines():
            if not line.startswith('    '):
                current_suite = line.strip().rstrip('*')
                suites[mode][current_suite] = []
            else:
                # --list_content produces the list of all test cases in the file. When BOOST_DATA_TEST_CASE is used it
                # additionally produce the lines with numbers for each case preserving the function name like this:
                # group0_voter_calculator_test *
                #     existing_voters_are_kept_across_racks *
                #     leader_is_retained_as_voter *
                #         _0 *
                #         _1 *
                #         _2 *
                # this line catches only test function name ignoring lines like '_0', so it will count test with dataprovider
                # as one test case.
                # Note: this ignores any test case starting with a '_' symbol
                # TODO: add support for test cases with dataprovider
                case_name = line.strip()
                if not case_name.startswith('_'):
                    suites[mode][current_suite].append(case_name.rstrip('*'))
    return suites
