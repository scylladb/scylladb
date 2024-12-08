#
# Copyright (c) 2014 Bruno Oliveira
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from __future__ import annotations

import inspect
from pathlib import Path
from typing import Sequence, Any, Iterator

import pytest
from _pytest._code.code import TerminalRepr, ReprFileLocation
from _pytest._io import TerminalWriter

from test.pylib.cpp.facade import CppTestFailure, CppFailureError, CppTestFacade


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

    def __init__(self, *, executable: Path, facade: CppTestFacade, mode: str, test_name: str, arguments: Sequence[str],
                 file_name, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.facade = facade
        self.executable = executable
        self.mode = mode
        self.file_name = file_name
        self.test_name = test_name
        self._arguments = arguments
        self.add_marker(pytest.mark.cpp)

    @property
    def nodeid(self) -> str:
        return self._nodeid

    @nodeid.setter
    def nodeid(self, nodeid: str) -> None:
        self._nodeid = nodeid

    def runtest(self) -> None:

        failures, output = self.facade.run_test(str(self.executable), self.test_name, self.mode, self.file_name,
                                                self._arguments)
        # Report the c++ output in its own sections
        self.add_report_section("call", "c++", output)

        if failures:
            raise CppFailureError(failures)

    def repr_failure(  # type:ignore[override]
            self, excinfo: pytest.ExceptionInfo[BaseException], **kwargs: Any) -> str | TerminalRepr | CppFailureRepr:
        if isinstance(excinfo.value, CppFailureError):
            return CppFailureRepr(excinfo.value.failures)
        return pytest.Item.repr_failure(self, excinfo)

    def reportinfo(self) -> tuple[Any, int, str]:
        return self.path, 0, self.name


class CppFile(pytest.File):
    """
    Represents the C++ test file with all necessary information for test execution
    """
    def __init__(self, *, no_parallel_run: bool = False, modes: list[str], disabled_tests: dict[str, set[str]],
                 run_id=None, facade: CppTestFacade, arguments: Sequence[str], parameters: list[str] = None,
                 **kwargs: Any, ) -> None:
        super().__init__(**kwargs)
        self.facade = facade
        self.modes = modes
        self.run_id = run_id
        self.disabled_tests = disabled_tests
        self.no_parallel_run = no_parallel_run
        self.parameters = parameters
        self._arguments = arguments

    def collect(self) -> Iterator[CppTestFunction]:
        project_root = find_repo(self.path)
        suite_dir = Path(inspect.getfile(self.facade.__class__)).resolve().parent
        for mode in self.modes:
            test_name = self.path.stem
            if test_name in self.disabled_tests[mode]:
                continue
            executable = Path(f'{project_root}/build/{mode}/test/{suite_dir.name}') / test_name
            tests = self.facade.list_tests(str(executable), self.no_parallel_run)
            for test_name in tests:
                test_id = f"{test_name}.{mode}"
                if self.run_id:
                    test_id = f"{test_id}.{self.run_id}"
                if self.parameters:
                    for index, parameter in enumerate(self.parameters):
                        yield CppTestFunction.from_parent(self, name=f'{test_id}.{index + 1}', executable=executable,
                                                          facade=self.facade, mode=mode, test_name=test_name,
                                                          file_name=self.path,
                                                          arguments=[*self._arguments, parameter])
                else:
                    yield CppTestFunction.from_parent(self, name=test_id, executable=executable, facade=self.facade, mode=mode,
                                                      file_name=self.path, test_name=test_name, arguments=self._arguments)


def find_repo(path):
    """Find repository root from the path's parents"""
    for path in Path(path).parents:
        # Check whether 'path/.git' exists and is a directory
        git_dir = path / '.git'
        if git_dir.is_dir():
            return path
