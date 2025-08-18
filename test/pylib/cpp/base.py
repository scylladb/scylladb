#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import os
import pathlib
import shlex
import subprocess
from abc import ABC, abstractmethod
from functools import cached_property
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest
from _pytest._code.code import ReprFileLocation

from scripts import coverage as coverage_script
from test import DEBUG_MODES, TEST_DIR, TOP_SRC_DIR, path_to
from test.pylib.resource_gather import get_resource_gather
from test.pylib.runner import BUILD_MODE, RUN_ID, TEST_SUITE

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from typing import Any

    from _pytest._code.code import TerminalRepr
    from _pytest._io import TerminalWriter


UBSAN_OPTIONS = [
    "halt_on_error=1",
    "abort_on_error=1",
    f"suppressions={TOP_SRC_DIR / 'ubsan-suppressions.supp'}",
    os.getenv("UBSAN_OPTIONS"),
]
ASAN_OPTIONS = [
    "disable_coredump=0",
    "abort_on_error=1",
    "detect_stack_use_after_return=1",
    os.getenv("ASAN_OPTIONS"),
]
BASE_TEST_ENV = {
    "UBSAN_OPTIONS": ":".join(filter(None, UBSAN_OPTIONS)),
    "ASAN_OPTIONS": ":".join(filter(None, ASAN_OPTIONS)),
    "SCYLLA_TEST_ENV": "yes",
}

DEFAULT_SCYLLA_ARGS = [
    "--overprovisioned",
    "--unsafe-bypass-fsync=1",
    "--kernel-page-cache=1",
    "--blocked-reactor-notify-ms=2000000",
    "--collectd=0",
    "--max-networking-io-control-blocks=100",
]
DEFAULT_CUSTOM_ARGS = ["-c2 -m2G"]

TIMEOUT = 60 * 15 # seconds
TIMEOUT_DEBUG = 60 * 30 # seconds


class CppFile(pytest.File, ABC):
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

        self.test_name = self.path.stem

    # Implement following properties as cached_property because they are read-only, and based on stash items which
    # will be assigned in test/pylib/runner.py::pytest_collect_file() hook after a CppFile instance was created.

    @cached_property
    def build_mode(self) -> str:
        return self.stash[BUILD_MODE]

    @cached_property
    def run_id(self) -> int:
        return self.stash[RUN_ID]

    @cached_property
    def suite_config(self) -> dict[str, Any]:
        return self.stash[TEST_SUITE].cfg

    @cached_property
    def build_basedir(self) -> pathlib.Path:
        return pathlib.Path(path_to(self.build_mode, "test", self.stash[TEST_SUITE].name))

    @cached_property
    def log_dir(self) -> pathlib.Path:
        return pathlib.Path(self.config.getoption("--tmpdir")).joinpath(self.build_mode).absolute()

    @cached_property
    def exe_path(self) -> pathlib.Path:
        return self.build_basedir / self.test_name

    @abstractmethod
    def list_test_cases(self) -> list[str]:
        ...

    @abstractmethod
    def run_test_case(self, test_case: CppTestCase) -> tuple[None | list[CppTestFailure], str]:
        ...

    @cached_property
    def test_env(self) -> dict[str, str]:
        variables = {
            **BASE_TEST_ENV,
            "TMPDIR": str(self.log_dir),
        }
        if self.build_mode == "coverage":
            variables.update(coverage_script.env(self.exe_path))
        return variables

    @cached_property
    def test_args(self) -> list[str]:
        args = [*DEFAULT_SCYLLA_ARGS, *self.suite_config.get("extra_scylla_cmdline_options", [])]
        if x_log2_compaction_groups := self.config.getoption("--x-log2-compaction-groups"):
            if all_can_run_compaction_groups_except := self.suite_config.get("all_can_run_compaction_groups_except"):
                if self.test_name not in all_can_run_compaction_groups_except:
                    args.append(f"--x-log2-compaction-groups={x_log2_compaction_groups}")
        return args

    def collect(self) -> Iterator[CppTestCase]:
        custom_args = self.suite_config.get("custom_args", {}).get(self.test_name, DEFAULT_CUSTOM_ARGS)

        for test_case in self.list_test_cases():
            # Start `index` from 1 if there are more than one custom_args item.  This allows us to create
            # test cases with unique names for each custom_args item and don't add any additional suffixes
            # if there is only one item (in this case `index` is 0.)
            for index, args in enumerate(custom_args, start=1 if len(custom_args) > 1 else 0):
                yield CppTestCase.from_parent(
                    parent=self,
                    name=f"{test_case}.{index}" if index else test_case,
                    test_case_name=test_case,
                    test_custom_args=shlex.split(args),
                )

    @classmethod
    def pytest_collect_file(cls, file_path: pathlib.Path, parent: pytest.Collector) -> pytest.Collector | None:
        if file_path.name.endswith("_test.cc"):
            return cls.from_parent(parent=parent, path=file_path)
        return None


class CppTestCase(pytest.Item):
    parent: CppFile

    def __init__(self, *, test_case_name: str, test_custom_args: list[str], **kwargs: Any):
        super().__init__(**kwargs)

        self.test_case_name = test_case_name
        self.test_custom_args = test_custom_args

        self.fixturenames = []
        self.own_markers = []
        self.add_marker(pytest.mark.cpp)

    def get_artifact_path(self, extra: str = "", suffix: str = "") -> pathlib.Path:
        return self.parent.log_dir / ".".join(
            (self.path.relative_to(TEST_DIR).with_suffix("") / f"{self.name}{extra}.{self.parent.run_id}{suffix}").parts
        )

    def make_testpy_test_object_mock(self) -> SimpleNamespace:
        """Returns object that used in resource gathering.

        It needed to not change the logic of writing metrics to DB that used in test types from test.py.
        """
        return SimpleNamespace(
            time_end=0,
            time_start=0,
            id=self.parent.run_id,
            mode=self.parent.build_mode,
            success=False,
            shortname=self.name,
            suite=SimpleNamespace(
                log_dir=self.parent.log_dir,
                name=self.parent.test_name,
            ),
        )

    def run_exe(self, test_args: list[str], output_file: pathlib.Path) -> subprocess.Popen[str]:
        resource_gather = get_resource_gather(
            is_switched_on=self.config.getoption("--gather-metrics"),
            test=self.make_testpy_test_object_mock(),
        )
        resource_gather.make_cgroup()
        process = resource_gather.run_process(
            args=[self.parent.exe_path, *test_args, *self.test_custom_args],
            timeout=TIMEOUT_DEBUG if self.parent.build_mode in DEBUG_MODES else TIMEOUT,
            output_file=output_file,
            cwd=TOP_SRC_DIR,
            env=self.parent.test_env,
        )
        resource_gather.write_metrics_to_db(
            metrics=resource_gather.get_test_metrics(),
            success=process.returncode == 0,
        )
        resource_gather.remove_cgroup()
        return process

    def runtest(self) -> None:
        failures, output = self.parent.run_test_case(test_case=self)

        # Report the c++ output in its own sections.
        self.add_report_section(when="call", key="c++", content=output)

        if failures:
            raise CppTestFailureList(failures)

    def repr_failure(self,
                     excinfo: pytest.ExceptionInfo[BaseException | CppTestFailureList],
                     **kwargs: Any) -> str | TerminalRepr | CppFailureRepr:
        if isinstance(excinfo.value, CppTestFailureList):
            return CppFailureRepr(excinfo.value.failures)
        return pytest.Item.repr_failure(self, excinfo)

    def reportinfo(self) -> tuple[Any, int, str]:
        return self.path, 0, self.test_case_name


class CppTestFailure(Exception):
    def __init__(self, file_name: str, line_num: int, content: str) -> None:
        self.file_name = file_name
        self.line_num = line_num
        self.lines = content.splitlines()

    def get_lines(self) -> list[tuple[str, tuple[str, ...]]]:
        m = ("red", "bold")
        return [(x, m) for x in self.lines]

    def get_file_reference(self) -> tuple[str, int]:
        return self.file_name, self.line_num


class CppTestFailureList(Exception):
    def __init__(self, failures: Sequence[CppTestFailure]) -> None:
        self.failures = list(failures)


class CppFailureRepr:
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

    @staticmethod
    def _get_repr_file_location(failure: CppTestFailure) -> ReprFileLocation:
        filename, line_num = failure.get_file_reference()
        return ReprFileLocation(path=filename, lineno=line_num, message="C++ failure")

    def toterminal(self, tw: TerminalWriter) -> None:
        for index, failure in enumerate(self.failures):
            for line, markup in failure.get_lines():
                markup_params = {m: True for m in markup}
                tw.line(line, **markup_params)

            location = self._get_repr_file_location(failure)
            location.toterminal(tw)

            if index != len(self.failures) - 1:
                tw.line(self.failure_sep, cyan=True)
