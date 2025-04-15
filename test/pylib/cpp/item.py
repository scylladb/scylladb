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

from pathlib import Path
from typing import Sequence, Any, Iterator

import pytest
from _pytest._code.code import TerminalRepr, ReprFileLocation
from _pytest._io import TerminalWriter

from test.pylib.cpp.boost.boost_facade import COMBINED_TESTS
from test.pylib.cpp.facade import CppTestFailure, CppTestFailureList, CppTestFacade


class CppFailureRepr(object):
    failure_sep = "---"

    def __init__(self, failures: Sequence[CppTestFailure]) -> None:
        self.failures = failures

    def __str__(self) -> str:
        reprs = []
        for failure in self.failures:
            pure_lines = "\n".join(x[0] for x in failure.get_lines())
            repr_loc = self._get_repr_file_location(failure)
            reprs.append("%s\n%s" % (pure_lines, repr_loc))
        return self.failure_sep.join(reprs)

    def _get_repr_file_location(self, failure: CppTestFailure) -> ReprFileLocation:
        filename, line_num = failure.get_file_reference()
        return ReprFileLocation(filename, line_num, "C++ failure")

    def toterminal(self, tw: TerminalWriter) -> None:
        for index, failure in enumerate(self.failures):
            for line, markup in failure.get_lines():
                markup_params = {m: True for m in markup}
                tw.line(line, **markup_params)

            location = self._get_repr_file_location(failure)
            location.toterminal(tw)

            if index != len(self.failures) - 1:
                tw.line(self.failure_sep, cyan=True)


class CppTestFunction(pytest.Item):
    """
    Represents a single test function in the file.
    """
    facade = None

    def __init__(self, *, executable: Path, facade: CppTestFacade, mode: str, test_unique_name: str, arguments: Sequence[str],
                 file_name: Path, run_id:int = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.facade = facade
        self.executable = executable
        self.mode = mode
        self.file_name = file_name
        self.originalname = kwargs['name']
        self.test_unique_name = test_unique_name
        self._arguments = arguments
        self.run_id = run_id
        self.fixturenames = []
        self.own_markers = []
        self.add_marker(pytest.mark.cpp)

    @property
    def nodeid(self) -> str:
        return self._nodeid

    @nodeid.setter
    def nodeid(self, nodeid: str) -> None:
        self._nodeid = nodeid

    def runtest(self) -> None:

        failures, output = self.facade.run_test(self.executable, self.originalname, self.test_unique_name, self.mode,
                                                self.file_name, self._arguments)
        # Report the c++ output in its own sections
        self.add_report_section("call", "c++", output)

        if failures:
            raise CppTestFailureList(failures)

    def repr_failure(  # type:ignore[override]
            self, excinfo: pytest.ExceptionInfo[BaseException], **kwargs: Any) -> str | TerminalRepr | CppFailureRepr:
        if isinstance(excinfo.value, CppTestFailureList):
            return CppFailureRepr(excinfo.value.failures)
        return pytest.Item.repr_failure(self, excinfo)

    def reportinfo(self) -> tuple[Any, int, str]:
        return self.path, 0, self.originalname


class CppFile(pytest.File):
    """
    Represents the C++ test file with all necessary information for test execution
    """
    def __init__(self, *, no_parallel_run: bool = False, modes: list[str], disabled_tests: dict[str, set[str]],
                 run_id=None, facade: CppTestFacade, arguments: Sequence[str], parameters: list[str] = None, project_root: Path,
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.facade = facade
        self.modes = modes
        self.run_id = run_id
        self.disabled_tests = disabled_tests
        self.no_parallel_run = no_parallel_run
        self.parameters = parameters
        self.project_root = project_root
        self._arguments = arguments

    def collect(self) -> Iterator[CppTestFunction]:
        for mode in self.modes:
            test_name = self.path.stem
            if test_name in self.disabled_tests[mode]:
                continue
            executable = Path(f'{self.project_root}/build/{mode}/test/{self.path.parent.name}/{test_name}')
            combined, tests = self.facade.list_tests(executable, self.no_parallel_run)
            if combined:
                executable = executable.parent / COMBINED_TESTS.stem
            for test_name in tests:
                if '/' in test_name:
                    test_name = test_name.replace('/', '_')
                if self.parameters:
                    for index, parameter in enumerate(self.parameters):
                        yield CppTestFunction.from_parent(self, name=test_name, executable=executable,
                                                          facade=self.facade, mode=mode, test_unique_name=f'{test_name}.{index + 1}',
                                                          file_name=self.path, run_id=self.run_id,
                                                          arguments=[*self._arguments, parameter])
                else:
                    yield CppTestFunction.from_parent(self, name=test_name, executable=executable, facade=self.facade, mode=mode,
                                                      file_name=self.path, test_unique_name=test_name, run_id=self.run_id, arguments=self._arguments)
