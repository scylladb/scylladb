#
# Copyright (c) 2014 Bruno Oliveira
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import io
import os
import subprocess
from collections.abc import Sequence
from pathlib import Path
from xml.etree import ElementTree

from test.pylib.cpp.facade import CppTestFacade, CppTestFailure, run_process

TIMEOUT_DEBUG = 60 * 5 # seconds
TIMEOUT = 60 * 2 # seconds

class BoostTestFacade(CppTestFacade):
    """
    Facade for BoostTests that's responsible for discovering test functions and executing them correctly.
    """

    @staticmethod
    def list_tests(
        executable: str,
        no_parallel: bool,
    ) -> list[str]:
        if no_parallel:
            return [os.path.basename(os.path.splitext(executable)[0])]
        else:
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
            return [case[:-1] for case in output.splitlines() if
                         case.endswith('*') and not case.strip().startswith('_')]

    def run_test(
        self,
        executable: str,
        test_id: str,
        mode: str,
        file_name: str,
        test_args:Sequence[str] = (),
    ) -> tuple[list[CppTestFailure], str] | tuple[None, str]:
        def read_file(name: Path) -> str:
            try:
                with io.open(name) as f:
                    return f.read()
            except IOError:
                return ''

        timeout = TIMEOUT_DEBUG if mode=='debug' else TIMEOUT
        log_xml = self.temp_dir / mode / 'pytest' / f"{test_id}.log.xml"
        report_xml = self.temp_dir / mode / 'pytest' / f"{test_id}.xml"
        args = [ executable,
                 '--output_format=XML',
                 f"--report_sink={report_xml}",
                 f"--log_sink={log_xml}",
                 '--catch_system_errors=no',
                 '--color_output=false',
                 ]
        if test_id != Path(executable).stem:
            args.append(f"--run_test={test_id}")
        # Tests are written in the way that everything after '--' passes to the test itself rather than to the test framework
        args.append('--')
        args.extend(test_args)
        os.chdir(self.temp_dir.parent)
        p, stderr, stdout = run_process(args, timeout)

        log = read_file(log_xml)
        report = read_file(report_xml)

        results = self._parse_log(log=log)

        if p.returncode != 0:
            msg = (
                'working_dir: {working_dir}\n'
                'Internal Error: calling {executable} '
                'for test {test_id} failed (return_code={return_code}):\n'
                'output:{stdout}\n'
                'std error:{stderr}\n'
                'log:{log}\n'
                'report:{report}\n'
                'command to repeat:{command}'
            )
            failure = CppTestFailure(
                results[0].filename,
                line_num=results[0].line_num,
                contents=msg.format(
                    working_dir=os.getcwd(),
                    executable=executable,
                    test_id=test_id,
                    stdout=stdout,
                    stderr=stderr,
                    log=log,
                    report=report,
                    command=' '.join(p.args),
                    return_code=p.returncode,
                ),
            )
            return [failure], stdout

        if results:
            return results, stdout

        return None, stdout

    def _parse_log(self, log: str) -> list[CppTestFailure]:
        """
        Parse the 'log' section produced by BoostTest.

        This is always an XML file, and from this it's possible to parse most of the
        failures possible when running BoostTest.
        """
        parsed_elements = []

        log_root = ElementTree.fromstring(log)
        testlog = log_root.find('TestLog')

        parsed_elements.extend(log_root.findall('Exception'))
        parsed_elements.extend(log_root.findall('Error'))
        parsed_elements.extend(log_root.findall('FatalError'))

        if testlog is not None:
            parsed_elements.extend(testlog.findall('Exception'))
            parsed_elements.extend(testlog.findall('Error'))
            parsed_elements.extend(testlog.findall('FatalError'))

        result = []
        for elem in parsed_elements:
            last_checkpoint = elem.find('LastCheckpoint')
            file_name = last_checkpoint.attrib['file']
            line_num = int(last_checkpoint.attrib['line'])
            result.append(CppTestFailure(file_name, line_num, elem.text or ''))
        return result
