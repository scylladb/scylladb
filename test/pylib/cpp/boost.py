#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import os
import fcntl
import hashlib
import logging
import subprocess
import tempfile
import pathlib
import json
from functools import cache, cached_property
from itertools import chain
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING
from xml.etree import ElementTree

import allure

from test.pylib import sched_dir
from test.pylib.cpp.base import CppFile, CppTestFailure

if TYPE_CHECKING:

    from test.pylib.cpp.base import CppTestCase


COMBINED_TESTS = "combined_tests"

logger = logging.getLogger(__name__)


class BoostTestFile(CppFile):
    @cached_property
    def exe_path(self) -> pathlib.Path:
        path = super().exe_path
        if not path.is_file():
            path = self.build_basedir / COMBINED_TESTS
            if not path.is_file() or self.test_name not in get_boost_test_list_content(executable=path, combined=True):
                raise FileNotFoundError(
                    f"There is no separate {self.build_mode} binary built for {self.path.name},"
                    " and it's not built into the combined tests binary",
                )
        return path

    @cached_property
    def combined(self) -> bool:
        return self.exe_path.name == COMBINED_TESTS

    @cached_property
    def no_parallel(self) -> bool:
        """Run all test cases in a single process."""

        return self.test_name in self.suite_config.get("no_parallel_cases", [])

    def list_test_cases(self) -> list[str]:
        if self.no_parallel:
            return [self.test_name]
        return get_boost_test_list_json_content(executable=self.exe_path,combined=self.combined).get(self.test_name, [])

    def run_test_case(self, test_case: CppTestCase) -> tuple[list[CppTestFailure], Path] | tuple[None, Path]:
        run_test = f"{self.test_name}/{test_case.test_case_name}" if self.combined else test_case.test_case_name

        log_sink = tempfile.NamedTemporaryFile(mode="w+t")
        args = [
            "--logger=HRF,warning,stdout",
            f"--logger=XML,warning,{log_sink.name}",
            "--catch_system_errors=no",
            "--color_output=false",
        ]
        if not self.no_parallel:
            args.append(f"--run_test={run_test}")

        args.append("--")

        if random_seed := self.config.getoption("--random-seed"):
            args.append(f"--random-seed={random_seed}")
        args.extend(self.test_args)

        stdout_file_path = test_case.get_artifact_path(extra="_stdout", suffix=".log").absolute()
        process = test_case.run_exe(test_args=args, output_file=stdout_file_path)

        try:
            log_xml = pathlib.Path(log_sink.name).read_text(encoding="utf-8")
        except IOError:
            log_xml = ""
        finally:
            log_sink.close()
        results = parse_boost_test_log_sink(log_xml=log_xml)

        if return_code := process.returncode:
            allure.attach(stdout_file_path.read_bytes(), name="output", attachment_type=allure.attachment_type.TEXT)
            return [CppTestFailure(
                file_name=results[0].file_name if results else self.path.name,
                line_num=results[0].line_num if results else -1,
                content=dedent(f"""\
                    working_dir: {os.getcwd()}
                    Internal Error: calling {self.exe_path} for test {run_test} failed ({return_code=}):
                    output file: {stdout_file_path}
                    command to repeat: {subprocess.list2cmdline(process.args)}
                    error: {results[0].lines if results else 'unknown'}
                """),
            )], stdout_file_path

        return None, stdout_file_path


pytest_collect_file = BoostTestFile.pytest_collect_file


# Listing a test binary costs a process start and ~70 ms, and every xdist worker collects
# the whole suite, so on a run with many workers the same 150-odd binaries get listed once
# per worker.  Measured on the release build: 10.6 CPU-seconds per worker, all spent in the
# first seconds of the run, which saturated the machine before a single test had started.
#
# The workers are separate processes, so an in-process cache -- which is what @cache below
# already is -- cannot help them share anything; the cache has to be somewhere all of them
# can see.  It is not something to keep, so it lives in the run's scratch directory, which
# is emptied at the start of every run and removed at its end, and entries are keyed by
# what each binary *is* so a rebuild can never read a stale listing.
@cache
def _listing_cache_dir() -> pathlib.Path | None:
    path = sched_dir.boost_list_cache()
    if path is None:
        return None
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError:
        return None
    return path


def _listing_cache_file(executable: pathlib.Path, combined: bool, kind: str) -> pathlib.Path | None:
    cache_dir = _listing_cache_dir()
    if cache_dir is None:
        return None
    try:
        st = executable.stat()
    except OSError:
        return None
    key = f"{executable}|{st.st_mtime_ns}|{st.st_size}|{combined}|{kind}"
    return cache_dir / f"{_listing_prefix(executable, combined, kind)}.{hashlib.sha1(key.encode()).hexdigest()[:16]}.json"


def _listing_prefix(executable: pathlib.Path, combined: bool, kind: str) -> str:
    """The part of a cache entry's name shared by every build of one listing of one binary.

    A binary is listed more than one way (combined_tests both plain and as json), and each
    listing is its own entry: sweeping by the binary's name alone had each listing delete
    the other, so every worker listed combined_tests again.
    """
    # The binary's path, not just its name: every build mode has its own foo_test, and the
    # modes of one run share this directory.
    where = hashlib.sha1(str(executable).encode()).hexdigest()[:8]
    return f"{executable.name}.{where}.{kind}{'.combined' if combined else ''}"


def _shared_listing(executable: pathlib.Path, combined: bool, kind: str, compute, encode, decode):
    """compute() the listing once per build, not once per worker."""
    path = _listing_cache_file(executable, combined, kind)
    if path is None:
        return compute()
    try:
        return decode(json.loads(path.read_text()))
    except Exception:
        pass            # missing, unreadable or not the shape we write: list it again
    # Miss.  Take the lock so that the sixty-odd workers racing us here wait for one
    # listing instead of each running their own.
    try:
        lock = open(path.with_suffix(".lock"), "w")
    except OSError:
        return compute()
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
    except OSError:
        lock.close()
        return compute()
    try:
        try:
            return decode(json.loads(path.read_text()))   # filled while we waited
        except Exception:
            pass
        data = compute()
        try:
            tmp = path.with_suffix(f".{os.getpid()}.tmp")
            tmp.write_text(json.dumps(encode(data)))
            os.replace(tmp, path)
            # every rebuild of this binary leaves an entry behind; keep only the live one
            for stale in path.parent.glob(f"{_listing_prefix(executable, combined, kind)}.*.json"):
                if stale != path:
                    stale.unlink(missing_ok=True)
        except OSError:
            pass          # cache is an optimisation; a failure to store it is not an error
        return data
    finally:
        lock.close()


@cache
def get_boost_test_list_json_content(executable: pathlib.Path, combined: bool = False)-> dict[str, list[list[str, set[str]]]]:
    """
    mimic get_boost_test_list_content but using --list_json_content which provides more structured data including test labels

    List the content of test tree in an executable.
    Return a dict where key is the name of test file and value is a list of tests in this file with their labels.
    In case of combined tests the dict will have multiple items, otherwise we assume that name of the executable is the same
    as the source test file (.cc)
    """
    return _shared_listing(
        executable, combined, "json",
        lambda: _list_json_content(executable, combined),
        # labels are sets, which JSON has no notion of
        encode=lambda tree: {k: [[name, sorted(labels)] for name, labels in v] for k, v in tree.items()},
        decode=lambda raw: {k: [[name, set(labels)] for name, labels in v] for k, v in raw.items()},
    )


def _list_json_content(executable: pathlib.Path, combined: bool) -> dict[str, list[list[str, set[str]]]]:
    try:
        output = subprocess.check_output(
            [executable, "--","--list_json_content"],
            stderr=subprocess.STDOUT,
            universal_newlines=True,
        )
    except subprocess.CalledProcessError as e:
        if "Test setup error: test tree is empty" in e.output:
            return {executable.name: []}
        raise e

    data = json.loads(output)
    test_tree = {}

    def parse_suite(key, suite, suite_str=""):
        for _suite in suite["suites"]:
            parse_suite(key, _suite, f'{suite_str}/{_suite["name"]}')
        for test in suite["tests"]:
            if test["name"].startswith("_"):
                test_tree[key].append([suite["name"], []])
                break
            test_name = f'{suite_str}/{test["name"]}' if suite_str else test["name"]
            labels = set(test["labels"].split(",")) if "labels" in test and test["labels"] else set()
            test_tree[key].append([test_name, labels])

    for file in data:
        for s in file["content"]["suites"]:
            k = s["name"] if combined else executable.name
            if k not in test_tree:
                test_tree[k] = []
            parse_suite(k, s, suite_str=s["name"] if not combined else "")
        if file["content"]["tests"]:
            if executable.name not in test_tree:
                test_tree[executable.name] = []
            for test in file["content"]["tests"]:
                labels = set(test["labels"].split(",")) if "labels" in test and test["labels"] else set()
                test_tree[executable.name].append([test["name"], labels])
    return test_tree

@cache
def get_boost_test_list_content(executable: pathlib.Path, combined: bool = False) -> dict[str, list[str]]:
    """List the content of test tree in an executable.

    Return a dict where key is the name of test file and value is a list of tests in this file.  In case of
    combined tests the dict will have multiple items, otherwise we assume that name of the executable is the same
    as the source test file (.cc)

    --list_content produces the list of all test cases in the file.  When BOOST_DATA_TEST_CASE is used it
    additionally produce the lines with numbers for each case preserving the function name like this:

    test_singular_tree_ptr_sz*
        _0*
        _1*
        _2*

    or, in case of combined tests:

    group0_voter_calculator_test*
        existing_voters_are_kept_across_racks*
        leader_is_retained_as_voter*
            _0*
            _1*
            _2*

    Lines like '_0' are ignored because we count a test with a dataprovider as one test case.
    """
    return _shared_listing(
        executable, combined, "plain",
        lambda: _list_content(executable, combined),
        encode=lambda tree: tree,
        decode=lambda raw: raw,
    )


def _list_content(executable: pathlib.Path, combined: bool) -> dict[str, list[str]]:
    output = subprocess.check_output(
        [executable, "--list_content"],
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )

    current_suite = {}
    stack = []
    root = current_suite

    # In either combined tests, or tests with suites, we have
    # indentation signifying the suite group. These can even be
    # nested, so build a tree of dicts.
    indent = 0
    last = ''
    for line in output.splitlines():
        name = line.strip().removesuffix("*")  # remove spaces around and trailing `*`

        if name.startswith("_"):
            # TODO: add support for test cases with dataprovider
            continue
        n = next((i for i, c in enumerate(line) if c != ' '), len(line))
        if n > indent:
            # last was a test suite
            stack.append((current_suite, indent))
            current_suite = current_suite[last]
        elif n < indent:
            # end of a suite
            while True:
                current_suite, level = stack.pop()
                if level == n:
                    break

        indent = n
        current_suite[name] = {}
        last = name

    # reduce a dict to a list of the test paths flattened
    # to <suite>/<suite>/<case>
    def flatten(tests: dict[str, dict]) -> list[str]:
        res = []
        for s, v in tests.items():
            if v:
                sub = flatten(v)
                res = res + ["/".join([s, vv]) for vv in sub]
            else:
                res.append(s)
        return res

    test_tree = {}

    if combined:
        # if combined, we keep each top level suite
        test_tree = { k: flatten(v) for k, v in root.items() }
    else:
        # else flatten suites to designators and store under exec
        test_tree = { executable.name : flatten(root) }

    return test_tree

def parse_boost_test_log_sink(log_xml: str) -> list[CppTestFailure]:
    """Parse the output of 'log' section produced by BoostTest.

    This is always an XML file, and from this it's possible to parse most of the failures
    possible when running BoostTest.
    """
    result = []

    try:
        log_root = ElementTree.fromstring(text=log_xml)
        if log_root is not None:
            for elem in chain.from_iterable(log_root.findall(tag) for tag in ("Exception", "Error", "FatalError")):
                last_checkpoint = elem.find("LastCheckpoint")
                if last_checkpoint is not None:
                    elem = last_checkpoint
                result.append(CppTestFailure(
                    file_name=elem.attrib["file"],
                    line_num=int(elem.attrib["line"]),
                    content=elem.text or "",
                ))
    except ElementTree.ParseError:
        logger.warning("Error parsing the log_sink output. Can be empty or invalid.")

    return result
