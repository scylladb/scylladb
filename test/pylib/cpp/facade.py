#
# Copyright (c) 2014 Bruno Oliveira
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
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

class CppFailureError(Exception):
    def __init__(self, failures: Sequence[CppTestFailure]) -> None:
        self.failures = list(failures)

class CppTestFacade(ABC):
    def __init__(self, config: Config):
        self.temp_dir = Path(config.getoption('tmpdir'))

    @staticmethod
    def list_tests(executable: str , no_parallel: bool) -> list[str]:
        raise NotImplementedError

    def run_test(self, executable: str, test_id: str, mode:str, file_name: str, test_args: Sequence[str] = ()) -> tuple[Sequence[CppTestFailure] | None, str]:
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
