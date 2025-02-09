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

import shlex
import subprocess
from abc import ABC
from pathlib import Path
from subprocess import TimeoutExpired
from typing import Sequence

from pytest import Config


class CppTestFailure(Exception):
    def __init__(self, filename: str, line_num: int, contents: str) -> None:
        self.filename = filename
        self.line_num = line_num
        self.lines = contents.splitlines()

    def get_lines(self) -> list[tuple[str, tuple[str, ...]]]:
        m = ("red", "bold")
        return [(x, m) for x in self.lines]

    def get_file_reference(self) -> tuple[str, int]:
        return self.filename, self.line_num

class CppTestFailureList(Exception):
    def __init__(self, failures: Sequence[CppTestFailure]) -> None:
        self.failures = list(failures)

class CppTestFacade(ABC):
    def __init__(self, config: Config, combined_tests: dict[str, list[str]] = None):
        self.temp_dir: Path = Path(config.getoption('tmpdir'))
        self.combined_suites: dict[str, list[str]] = combined_tests

    def list_tests(self, executable: Path , no_parallel: bool) -> tuple[bool,list[str]]:
        raise NotImplementedError

    def run_test(self, executable: Path, original_name: str, test_id: str, mode:str, file_name: Path, test_args: Sequence[str] = ()) -> tuple[Sequence[CppTestFailure] | None, str]:
         raise NotImplementedError


def run_process(args: list[str], timeout):
    args = shlex.split(' '.join(args))
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        stdout, stderr = p.communicate(timeout=timeout)
    except TimeoutExpired:
        print('Timeout reached')
        p.kill()
        stdout = p.stdout.read()
        stderr = p.stderr.read()
    except KeyboardInterrupt:
        p.kill()
        raise
    return p, stderr, stdout
