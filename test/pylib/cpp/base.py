#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import os
import pathlib
import shlex
import signal
import struct
import subprocess
from abc import ABC, abstractmethod
from functools import cache, cached_property
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from _pytest._code.code import ReprFileLocation

from scripts import coverage as coverage_script
from test import BUILD_DIR, DEBUG_MODES, TEST_DIR, TOP_SRC_DIR, asan_options, path_to, ubsan_options, \
    use_traditional_build
from test.pylib.coverage_utils import coverage_dir
from test.pylib.runner import BUILD_MODE, CPP_TEST_LOG, CPP_TEST_LOG_KEPT, RUN_ID, TEST_SUITE
from test.pylib.scylla_server import merge_cmdline_options
from test.pylib.util import get_configured_tests, ninja_build, ninja_cwd

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from typing import Any

    from _pytest._code.code import TerminalRepr
    from _pytest._io import TerminalWriter


BASE_TEST_ENV = {
    "UBSAN_OPTIONS": ubsan_options(inherit=True),
    "ASAN_OPTIONS": asan_options(inherit=True),
    "SCYLLA_TEST_ENV": "yes",
}

DEFAULT_SCYLLA_ARGS = [
    "--overprovisioned",
    "--unsafe-bypass-fsync=1",
    "--kernel-page-cache=1",
    "--blocked-reactor-notify-ms=2000000",
    "--collectd=0",
    "--max-networking-io-control-blocks=1000",
]
DEFAULT_CUSTOM_ARGS = ["-c2 -m2G"]

TIMEOUT = 60 * 15 # seconds
TIMEOUT_DEBUG = 60 * 30 # seconds


class CppFile(pytest.File, ABC):
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

        self.test_name = self.path.stem

    # Implement following properties as cached_property because they are read-only, and based on stash items which
    # will be assigned in test/pylib/runner.py::pytest_collect_file() and modify_pytest_item() after instance creation.

    @cached_property
    def build_mode(self) -> str:
        return self.stash[BUILD_MODE]

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

    @cached_property
    def debug_exe_path(self) -> pathlib.Path:
        """The executable to run under gdb: the unstripped re-link of exe_path, if there is one.

        The configure.py build strips the test binaries and emits an unstripped
        variant of each under a `_g` suffix, which --build links when --gdb is
        given.  The cmake build has no such target and has to be configured
        with debug info instead, so there this is just exe_path.
        """

        unstripped = self.exe_path.with_name(f"{self.exe_path.name}_g")
        if self.config.getoption("--build") and unstripped.is_file():
            return unstripped
        return self.exe_path

    @property
    def exe_names(self) -> list[str]:
        """Names of the executables which can run this test file, most specific first.

        Only used to find the one the build system is configured to build.
        """

        return [self.test_name]

    def build(self) -> None:
        """Build the executable of this test file.

        Has to happen before the test cases are collected, since listing them
        runs the executable.
        """

        suite_name = self.stash[TEST_SUITE].name
        configured_tests = get_configured_tests()
        for exe_name in self.exe_names:
            if f"test/{suite_name}/{exe_name}" in configured_tests:
                break
        else:
            raise FileNotFoundError(
                f"None of the executables which can run {self.path.name} ({', '.join(self.exe_names)})"
                " is configured to be built, please re-run ./configure.py (or cmake, for a cmake build)",
            )
        # In both build systems a test executable is a ninja target named
        # after its path, relative to the directory ninja runs in.  Derive it
        # from build_basedir, the directory exe_path looks the executable up
        # in, so that what is built is what the test will run.
        target = os.path.relpath(self.build_basedir / exe_name, ninja_cwd())
        targets = (target,)
        if self.config.getoption("--gdb") and use_traditional_build():
            # The test cases are listed by running the executable itself, so
            # the stripped one is needed too, even though gdb gets the
            # unstripped re-link.  See debug_exe_path.
            targets += (f"{target}_g",)

        # Collection is captured, so the build would run with its output
        # swallowed until the whole session ends without suspending it.
        capture_manager = self.config.pluginmanager.getplugin("capturemanager")
        with capture_manager.global_and_fixture_disabled():
            build_ninja_targets(targets)

    @abstractmethod
    def list_test_cases(self) -> list[str]:
        ...

    @abstractmethod
    def run_test_case(self, test_case: CppTestCase) -> tuple[None | list[CppTestFailure], Path]:
        ...

    @cached_property
    def test_env(self) -> dict[str, str]:
        variables = {
            **BASE_TEST_ENV,
            "TMPDIR": str(self.log_dir),
        }
        if self.build_mode == "coverage":
            # "%m" in LLVM_PROFILE_FILE expands to a value unique to this
            # binary and makes the profile runtime merge counters into the
            # resulting file (under a file lock) rather than overwrite it.
            # The binary is invoked once per test case, so all cases
            # accumulate into a single profile instead of each dumping a
            # full separate one -- for a large multi-object boost binary
            # that would balloon disk usage by the number of cases.
            profile_base = coverage_dir(self.log_dir) / self.stash[TEST_SUITE].name / f"{self.test_name}.%m"
            variables.update(coverage_script.env(profile_base))
        return variables

    @cached_property
    def test_args(self) -> list[str]:
        args = merge_cmdline_options(DEFAULT_SCYLLA_ARGS, self.suite_config.get("extra_scylla_cmdline_options", []))
        if x_log2_compaction_groups := self.config.getoption("--x-log2-compaction-groups"):
            if all_can_run_compaction_groups_except := self.suite_config.get("all_can_run_compaction_groups_except"):
                if self.test_name not in all_can_run_compaction_groups_except:
                    args.append(f"--x-log2-compaction-groups={x_log2_compaction_groups}")
        return args

    def collect(self) -> Iterator[CppTestCase]:
        if self.config.getoption("--build"):
            self.build()

        custom_args = self.suite_config.get("custom_args", {}).get(self.test_name, DEFAULT_CUSTOM_ARGS)

        for test_case in self.list_test_cases():
            if isinstance(test_case, list):
                test_labels = test_case[1]
                test_case = test_case[0]
            else:
                test_labels = []
            # Start `index` from 1 if there are more than one custom_args item.  This allows us to create
            # test cases with unique names for each custom_args item and don't add any additional suffixes
            # if there is only one item (in this case `index` is 0.)
            for index, args in enumerate(custom_args, start=1 if len(custom_args) > 1 else 0):
                yield CppTestCase.from_parent(
                    parent=self,
                    name=f"{test_case}.{index}" if index else test_case,
                    test_case_name=test_case,
                    test_custom_args=shlex.split(args),
                    own_markers=test_labels,
                )

    @classmethod
    def pytest_collect_file(cls, file_path: pathlib.Path, parent: pytest.Collector) -> pytest.Collector | None:
        if file_path.name.endswith("_test.cc"):
            return cls.from_parent(parent=parent, path=file_path)
        return None


class CppTestCase(pytest.Item):
    parent: CppFile

    def __init__(self, *, test_case_name: str, test_custom_args: list[str], own_markers: list[str] | set[str], **kwargs: Any):
        super().__init__(**kwargs)

        self.test_case_name = test_case_name
        self.test_custom_args = test_custom_args

        self.fixturenames = []
        self.own_markers = [getattr(pytest.mark, mark_name) for mark_name in own_markers]
        self.add_marker(pytest.mark.cpp)

    @cached_property
    def run_id(self) -> int:
        return self.stash[RUN_ID]

    def get_artifact_path(self, extra: str = "", suffix: str = "") -> pathlib.Path:
        return self.parent.log_dir / ".".join(
            (self.path.relative_to(TEST_DIR).with_suffix("") / f"{self.name}{extra}.{self.run_id}{suffix}").parts
        )

    def run_exe(self, test_args: list[str], output_file: pathlib.Path) -> subprocess.Popen[str]:
        if self.config.getoption("--gdb"):
            return self.run_exe_under_gdb(test_args=test_args, output_file=output_file)

        args = [str(self.parent.exe_path), *test_args, *self.test_custom_args]
        timeout = TIMEOUT_DEBUG if self.parent.build_mode in DEBUG_MODES else TIMEOUT
        env = {**os.environ, **self.parent.test_env}

        with output_file.open(mode="w", encoding="utf-8") as output_handle:
            p = subprocess.Popen(
                args=args,
                bufsize=1,
                stdout=output_handle,
                stderr=subprocess.STDOUT,
                close_fds=True,
                cwd=TOP_SRC_DIR,
                env=env,
                text=True,
            )
            try:
                p.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                p.kill()
                p.communicate()
            except KeyboardInterrupt:
                p.kill()
                raise
        return p

    def run_exe_under_gdb(self, test_args: list[str], output_file: pathlib.Path) -> subprocess.Popen[str]:
        """Run the test executable under gdb, on the terminal pytest was started from.

        The test's output goes to the terminal, interleaved with the debugging
        session, so the log file is left empty -- it only exists to keep the
        callers which read it working.  There is no timeout either: sitting at
        a prompt for an arbitrarily long time is the entire point.
        """

        exe_path = self.parent.debug_exe_path
        require_debug_info(exe_path=exe_path, build_mode=self.parent.build_mode)

        output_file.write_bytes(b"")
        args = ["gdb", "--args", str(exe_path), *test_args, *self.test_custom_args]
        env = {**os.environ, **self.parent.test_env}
        # Suspending the capture restores stdout and stderr, but not stdin:
        # pytest keeps that pointed at /dev/null, so that a test reading it
        # fails instead of hanging.  gdb would read that as an immediate EOF
        # and quit before the user gets to type anything, so hand it the
        # terminal explicitly.
        try:
            terminal = open("/dev/tty", mode="rb")
        except OSError as e:
            pytest.fail(f"--gdb needs a terminal to run the debugging session on: {e}", pytrace=False)

        capture_manager = self.config.pluginmanager.getplugin("capturemanager")
        with terminal, capture_manager.global_and_fixture_disabled():
            print(f"Running under gdb: {subprocess.list2cmdline(args)}", flush=True)
            # ^C at the gdb prompt is for gdb, but the signal goes to the
            # whole foreground process group, this process included, which
            # would tear the session down mid-debugging.  gdb interrupts a
            # running inferior itself, having handed it the terminal.
            interrupt_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
            try:
                p = subprocess.Popen(args=args, stdin=terminal, close_fds=True, cwd=TOP_SRC_DIR, env=env, text=True)
                p.communicate()
            finally:
                signal.signal(signal.SIGINT, interrupt_handler)
        return p

    def runtest(self) -> None:
        failures, output = self.parent.run_test_case(test_case=self)

        # Write output to stdout so pytest captures it for both terminal and JUnit report.
        # Only show the last 300 lines to avoid excessive output.
        lines = get_lines_from_end(output)
        if lines:
            print("\n" + "=" * 70)
            print("C++ Test Output (last 300 lines):")
            print("=" * 70)
            print('\n'.join(lines))
            print("=" * 70 + "\n")

        keep_log = bool(failures) or self.config.getoption("--save-log-on-success")
        if not keep_log:
            output.unlink(missing_ok=True)
        if not self.config.getoption("--gdb"):
            # Under gdb the output went to the terminal, so there is no log to
            # point at.
            self.user_properties.append((CPP_TEST_LOG, str(output)))
            self.user_properties.append((CPP_TEST_LOG_KEPT, keep_log))

        if self.config.getoption("--gdb"):
            # The exit status is gdb's, not the test's: quitting a session
            # leaves it 0 however the test was doing, so there is no verdict
            # to report.
            pytest.skip("ran under gdb, which reports no test result")

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


@cache
def build_ninja_targets(targets: tuple[str, ...]) -> None:
    """Build ninja targets, at most once per session.

    Several test files can share an executable (all of the combined tests do),
    and a test file is collected once per build mode and per --repeat, so
    without the cache the same targets would be built over and over again.
    """

    ninja_build(*targets)


def elf_section_names(path: pathlib.Path) -> list[str]:
    """Names of the sections of an ELF64 file, or nothing if it isn't one."""

    with path.open(mode="rb") as f:
        header = f.read(64)
        if len(header) < 64 or header[:4] != b"\x7fELF" or header[4] != 2:  # 2: ELFCLASS64
            return []
        endianness = "<" if header[5] == 1 else ">"
        section_headers_offset, = struct.unpack_from(f"{endianness}Q", header, 0x28)
        header_size, header_count, names_index = struct.unpack_from(f"{endianness}3H", header, 0x3a)
        if not section_headers_offset or not header_count:
            return []

        f.seek(section_headers_offset)
        section_headers = f.read(header_size * header_count)
        # The section holding the section names is a section itself.  In a
        # section header sh_offset is at 0x18 and sh_size at 0x20.
        names_offset, names_size = struct.unpack_from(f"{endianness}2Q", section_headers, names_index * header_size + 0x18)
        f.seek(names_offset)
        names = f.read(names_size)

    section_names = []
    for index in range(header_count):
        name_offset, = struct.unpack_from(f"{endianness}I", section_headers, index * header_size)
        section_names.append(names[name_offset:names.index(b"\0", name_offset)].decode(encoding="ascii", errors="replace"))
    return section_names


def require_debug_info(exe_path: pathlib.Path, build_mode: str) -> None:
    """Refuse to debug a test executable which carries no debug info.

    There is nothing worth opening gdb for then: no source lines and no
    variables, and for a stripped binary not even symbol names.  Say what to
    build instead.
    """

    if not exe_path.is_file():
        return
    section_names = elf_section_names(exe_path)
    if not section_names:
        # Not an ELF we can read; leave the judgement to gdb.
        return
    if any(name.startswith(".debug_") or name == ".gnu_debuglink" for name in section_names):
        return

    if ".symtab" in section_names:
        # The symbol table survived, so nothing was stripped: the objects
        # themselves carry no DWARF, having been compiled without -g.  Both
        # build systems do that for the dev mode -- can_have_debug_info in
        # configure.py, no WITH_DEBUG_INFO in cmake/mode.Dev.cmake -- and no
        # re-link can make up for it.
        pytest.fail(
            f"{exe_path} has symbols, but no debug info: the {build_mode} objects were compiled"
            " without -g, so gdb would have no source lines or variables.\n"
            f"The {build_mode} mode is built without debug info, so debug in a mode which has it,"
            " e.g. --mode=debug or --mode=release.  A cmake build can also be configured with"
            " -DScylla_WITH_DEBUG_INFO=ON, which gives every mode debug info.",
            pytrace=False,
        )

    target = os.path.relpath(exe_path, ninja_cwd())
    if use_traditional_build():
        if target.endswith("_g"):
            # _g is the unstripped re-link itself, so there is no _g of it.
            how_to_fix = (
                f"Re-configure and build the {build_mode} tests with debug info:\n"
                f"    ./configure.py --tests-debuginfo 1 && ninja {target}"
            )
        else:
            how_to_fix = (
                f"Pass --build, which links the unstripped variant of the executable ({target}_g) and"
                " debugs that, or build it yourself:\n"
                f"    ninja {target}_g\n"
                f"Alternatively, re-configure and build the {build_mode} tests with debug info:\n"
                f"    ./configure.py --tests-debuginfo 1 && ninja {target}"
            )
    else:
        how_to_fix = (
            f"Re-configure and build the {build_mode} tests with debug info:\n"
            f"    cmake -DScylla_WITH_DEBUG_INFO=ON {BUILD_DIR} && ninja -C {BUILD_DIR} {target}"
        )

    pytest.fail(f"{exe_path} was stripped, so gdb would have no symbols or source lines.\n{how_to_fix}",
                pytrace=False)


def get_lines_from_end(file_path: pathlib.Path, lines_count: int = 300) -> list[str]:
    """
    Seeks to the end of the file and reads backwards to find the last N lines
    without iterating over the whole file.
    """
    chunk_size = 8192  # 8KB chunks
    buffer = ""

    with file_path.open("rb") as f:
        f.seek(0, os.SEEK_END)
        file_size = f.tell()
        pointer = file_size

        while pointer > 0:
            # Read one chunk backwards
            pointer -= min(pointer, chunk_size)
            f.seek(pointer)
            chunk = f.read(min(file_size - pointer, chunk_size)).decode('utf-8', errors='ignore')
            buffer = chunk + buffer

            # Stop once we have enough lines
            if len(buffer.splitlines()) > lines_count:
                break

    # Return only the requested number of lines
    return buffer.splitlines()[-lines_count:]
