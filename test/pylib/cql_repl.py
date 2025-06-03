#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import difflib
import filecmp
import re
import shutil
import time
from collections.abc import Iterable
from functools import cached_property
from itertools import islice
from typing import TYPE_CHECKING

import pytest
from _pytest.python import Module
from _pytest.fixtures import TopRequest
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from tabulate import tabulate

from test.pylib.suite.base import palette

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Any

    from _pytest._code.code import ExceptionInfo, TerminalRepr, TracebackStyle
    from cassandra.cluster import Session


CQL_TEST_SUFFIX = "_test.cql"

COMMENT_RE = re.compile(r"^\s*((--|//).*)?$")  # comments allowed by CQL - -- and //
DELIMITER_RE = re.compile(r"^(?!\s*(--|//)).*;\s*$")  # a comment is not a delimiter even if ends with one
IP_ADDRESS_RE = re.compile(r"\d+\.\d+\.\d+\.\d+")


class CqlTestException(Exception):
    ...


class CqlFile(Module):
    obj = None

    def funcnamefilter(self, name: str) -> bool:
        return True

    def collect(self) -> Iterable[CqlTest]:
        assert self.ihook.pytest_pycollect_makeitem(
            collector=self,
            name=self.name,
            obj=self.obj,
        ) is None, ".cql file will be not collected as a Python module"
        yield CqlTest.from_parent(parent=self, name=self.name.removesuffix(CQL_TEST_SUFFIX))


class CqlTest(pytest.Item):
    def __init__(self, name: str, parent: CqlFile):
        super().__init__(name=name, parent=parent)

        # Stuff needed for fixture support.
        self.obj = None
        self._fixtureinfo = self.session._fixturemanager.getfixtureinfo(node=self, func=None, cls=None)
        self.fixturenames = self._fixtureinfo.names_closure
        self._initrequest()

    def _initrequest(self) -> None:
        self.funcargs: dict[str, Any] = {}  # fixtures need to be used automatically to appear in this dict
        self._request = TopRequest(self, _ispytest=True)  # type: ignore[arg-type]

    def setup(self) -> None:
        self._request._fillfixtures()

    @cached_property
    def cql(self) -> Session:
        """CQL connection to the DB host."""

        return self.funcargs["cql"]

    @cached_property
    def keyspace(self) -> str:
        """Name of a keyspace used by the test."""

        return self.funcargs["keyspace"]

    @cached_property
    def output_path(self) -> Path:
        """Path to a file to collect the test output."""

        return self.funcargs["output_path"]

    @cached_property
    def result_path(self) -> Path:
        """Path to a file with the expected test output."""

        return self.path.with_suffix(".result")

    @cached_property
    def reject_path(self) -> Path:
        """Path to a file to store the test output if it will be different from .result file."""

        return self.path.with_suffix(".reject")

    def runtest(self) -> None:
        with self.path.open(encoding="utf-8") as ifile, self.output_path.open(mode="a", encoding="utf-8") as ofile:
            self.cql.set_keyspace(self.keyspace)

            for line in ifile:
                ofile.write("> ")

                if COMMENT_RE.match(line):
                    ofile.write(line)
                    continue

                query_bits = [line]

                # Read the rest of input until delimiter or EOF.
                while not DELIMITER_RE.match(line):
                    line = ifile.readline()
                    if not line:
                        break
                    query_bits.append(line)

                ofile.write("> ".join(query_bits))

                stmt = SimpleStatement(
                    query_string="".join(query_bits),
                    consistency_level=ConsistencyLevel.ONE,
                    serial_consistency_level=ConsistencyLevel.SERIAL,
                )

                try:
                    result = self.cql.execute(stmt)
                    if not result.column_names:
                        ofile.write("OK\n")
                    else:
                        ofile.write(tabulate(
                            tabular_data=prettify(result.current_rows),
                            headers=result.column_names,
                            tablefmt="psql",
                        ))
                        ofile.write("\n")
                except Exception as exc:
                    # Replace IP addresses with 127.0.0.1 in the message to make the output stable.
                    ofile.write(f"{IP_ADDRESS_RE.sub(repl="127.0.0.1", string=str(exc))}\n")

        if not self.output_path.is_file():
            raise CqlTestException("No output file")

        try:
            if not self.result_path.is_file():
                raise CqlTestException("No result file")

            if not filecmp.cmp(self.result_path, self.output_path):
                raise CqlTestException(
                    f"Test output does not match expected result:\n"
                    f"Expected: {self.result_path}\n"
                    f"Actual (copied to): {self.reject_path}\n"
                    f"Diff:\n{format_unified_diff(fromfile=self.result_path, tofile=self.output_path, head=61)}"
                )
        except CqlTestException:
            # Move the .reject file close to the .result file so that it's easy to analyze the diff or
            # overwrite .result with .reject.
            shutil.move(self.output_path, self.reject_path)
            raise

        self.output_path.unlink()

    def repr_failure(self,
                     excinfo: ExceptionInfo[BaseException],
                     style: TracebackStyle | None = None) -> str | TerminalRepr:
        if excinfo.errisinstance(CqlTestException):
            return excinfo.value.args[0]
        return super().repr_failure(excinfo, style)


def prettify(rows: Any) -> Any:
    """Recursively replace all None's with "null" and convert all iterables to lists."""

    if rows is None:
        return "null"

    if isinstance(rows, str | float | int | bool) or not isinstance(rows, Iterable):
        return rows

    return [prettify(row) for row in rows]


def colorize_unified_diff_line(line: str) -> str:
    match line[0]:
        case "+":
            return palette.diff_in(line)
        case "-":
            return palette.diff_out(line)
        case "@":
            return palette.diff_mark(line)
    return line


def format_unified_diff(fromfile: Path,
                        tofile: Path,
                        n_context_lines: int = 10,  # number of diff context lines
                        head: int | None = None) -> str:  # return first `head` lines of the output
    with fromfile.open(encoding="utf-8") as frm, tofile.open(encoding="utf-8") as to:
        diff = difflib.unified_diff(
            a=frm.readlines(),
            b=to.readlines(),
            fromfile=str(fromfile),
            tofile=str(tofile),
            fromfiledate=time.ctime(fromfile.stat().st_mtime),
            tofiledate=time.ctime(tofile.stat().st_mtime),
            n=n_context_lines,
        )
        return "".join(colorize_unified_diff_line(line) for line in islice(diff, head))
