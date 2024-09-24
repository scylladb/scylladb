from __future__ import annotations

import os
import string
from pathlib import Path
from typing import Sequence, Any, Iterator

import pytest
from _pytest._code.code import TerminalRepr, ReprFileLocation
from _pytest._io import TerminalWriter

from test.pylib.cpp.boost import BoostTestFacade, BoostTestFailure, BoostFailureError


def get_code_context_around_line(filename: str, linenum: int) -> list[str]:
    """
    return code context lines, with the last line being the line at
    linenum.
    """
    if  os.path.isfile(filename):
        index = linenum - 1
        with open(filename) as f:
            index_above = index - 2
            index_above = index_above if index_above >= 0 else 0
            return [x.rstrip() for x in f.readlines()[index_above: index + 1]]
    return []


def get_left_whitespace(line: str) -> str:
    result = ""
    for c in line:
        if c in string.whitespace:
            result += c
        else:
            break
    return result


class CppFailureRepr(object):
    failure_sep = "---"

    def __init__(self, failures: Sequence[BoostTestFailure]) -> None:
        self.failures = failures

    def __str__(self) -> str:
        reprs = []
        for failure in self.failures:
            pure_lines = "\n".join(x[0] for x in failure.get_lines())
            repr_loc = self._get_repr_file_location(failure)
            reprs.append("%s\n%s" % (pure_lines, repr_loc))
        return self.failure_sep.join(reprs)

    #
    def _get_repr_file_location(self, failure: BoostTestFailure) -> ReprFileLocation:
        filename, linenum = failure.get_file_reference()
        return ReprFileLocation(filename, linenum, "C++ failure")

    #
    def toterminal(self, tw: TerminalWriter) -> None:
        for index, failure in enumerate(self.failures):
            filename, linenum = failure.get_file_reference()
            code_lines = get_code_context_around_line(filename, linenum)
            for line in code_lines:
                tw.line(line, white=True, bold=True)  # pragma: no cover

            indent = get_left_whitespace(code_lines[-1]) if code_lines else ""

            for line, markup in failure.get_lines():
                markup_params = {m: True for m in markup}
                tw.line(indent + line, **markup_params)

            location = self._get_repr_file_location(failure)
            location.toterminal(tw)

            if index != len(self.failures) - 1:
                tw.line(self.failure_sep, cyan=True)


class CppItem(pytest.Item):
    facade = None

    def __init__(self, *, executable: Path, facade: BoostTestFacade, arguments: Sequence[str], **kwargs: Any, ) -> None:
        super().__init__(**kwargs)
        self.facade = facade
        self.executable = executable
        self._arguments = arguments
        self.add_marker(pytest.mark.cpp)

    def runtest(self) -> None:

        failures, output = self.facade.run_test(str(self.executable), self.name, self._arguments, )
        # Report the c++ output in its own sections
        self.add_report_section("call", "c++", output)

        if failures:
            raise BoostFailureError(failures)

    def repr_failure(  # type:ignore[override]
            self, excinfo: pytest.ExceptionInfo[BaseException], **kwargs: Any) -> str | TerminalRepr | CppFailureRepr:
        if isinstance(excinfo.value, BoostFailureError):
            return CppFailureRepr(excinfo.value.failures)
        return pytest.Item.repr_failure(self, excinfo)

    def reportinfo(self) -> tuple[Any, int, str]:
        return self.path, 0, self.name


class CppFile(pytest.File):
    def __init__(self, *, executable: Path, mode: str, temp_dir: Path, no_parallel_run: bool = False,
            arguments: Sequence[str], **kwargs: Any, ) -> None:
        super().__init__(**kwargs)
        self.facade = BoostTestFacade(mode=mode, temp_dir=temp_dir)
        self.executable = executable
        self.no_parallel_run = no_parallel_run
        self._arguments = arguments

    def collect(self) -> Iterator[CppItem]:
        tests = self.facade.list_tests(str(self.executable), self.no_parallel_run, )
        if len(tests) == 1:
            yield CppItem.from_parent(self, name=tests[0], executable=self.executable, facade=self.facade,
                arguments=self._arguments, )
        else:
            for test_id in tests:
                args = [f'--run_test={test_id}']
                args.extend(self._arguments)
                yield CppItem.from_parent(self, name=test_id, executable=self.executable, facade=self.facade,
                    arguments=args, )
