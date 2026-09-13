#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""It is pytest all the way down.

This module turns the ``test.py`` command line into pytest arguments and makes
the single ``pytest.main()`` call.  It is the only place that knows about
pytest flags.
"""

import dataclasses
import os
import pathlib
import shlex
from bisect import insort
from typing import TYPE_CHECKING

import pytest

from test import HOST_ID, TOP_SRC_DIR

if TYPE_CHECKING:
    import argparse


# TODO: Remove _CollectionArgument and _deduplicate_test_args once we update
# to pytest 9.x, which fixes argument deduplication:
# https://github.com/pytest-dev/pytest/issues/12083
@dataclasses.dataclass(frozen=True, order=True)
class _CollectionArgument:
    """Resolved collection argument for deduplication.

    A version-independent subset of pytest's CollectionArgument that
    includes the fields needed for normalization (parametrization and
    original_index were added in pytest 9.0).

    ``a in b`` means ``b`` subsumes (contains) ``a``.  Adapted from
    pytest 9.0.3 ``_pytest.main.is_collection_argument_subsumed_by``.
    """
    path: pathlib.Path
    parts: tuple[str, ...]
    parametrization: str
    original_index: int

    def __contains__(self, other: _CollectionArgument) -> bool:
        if self.path != other.path:
            return not self.parts and other.path.is_relative_to(self.path)
        if len(self.parts) > len(other.parts) or other.parts[:len(self.parts)] != self.parts:
            return False
        return not self.parametrization or self.parametrization == other.parametrization


def _deduplicate_test_args(args: list[str]) -> list[str]:
    """Remove duplicate and subsumed test arguments.

    Resolves and normalizes CLI test arguments, then applies the normalization
    algorithm from pytest 9.0.3 to remove exact duplicates and arguments whose
    paths are contained within another argument's path.
    For example, ``["test/cql", "test/cql/lua_test.cql"]`` becomes ``["test/cql"]``.
    """
    if not args:
        return args
    invocation_path = pathlib.Path.cwd()
    resolved_sorted: list[_CollectionArgument] = []
    unresolved_indices: set[int] = set()
    for i, arg in enumerate(args):
        # Adapted from pytest 9.0.3 _pytest.main.resolve_collection_argument.
        base, squacket, rest = arg.partition("[")
        strpath, *parts = base.split("::")
        fspath = pathlib.Path(os.path.abspath(invocation_path / strpath))
        if not fspath.exists():
            # Keep unresolved args: let pytest report the error.
            unresolved_indices.add(i)
            continue
        insort(resolved_sorted, _CollectionArgument(
            path=fspath,
            parts=tuple(parts),
            parametrization=squacket + rest,
            original_index=i,
        ))

    # Normalize: remove duplicates and subsumed arguments using an O(n log n)
    # sort-based algorithm adapted from pytest 9.0.3.
    normalized = resolved_sorted[:1]
    for ca in resolved_sorted[1:]:
        if ca not in normalized[-1]:
            normalized.append(ca)

    kept_indices = {ca.original_index for ca in normalized} | unresolved_indices
    return [arg for i, arg in enumerate(args) if i in kept_indices]


def pytest_args(options: argparse.Namespace) -> list[str]:
    """Render the parsed command line into the pytest one."""
    temp_dir = pathlib.Path(options.tmpdir).absolute()

    report_dir = temp_dir / 'report'
    junit_output_file = report_dir / f'pytest_cpp_{HOST_ID}.xml'
    files_to_run = options.name if options.keep_duplicates else _deduplicate_test_args(options.name)
    files_to_run = files_to_run or [str(TOP_SRC_DIR / 'test/')]
    args = [
        '--color=yes',
        f'--repeat={options.repeat}',
        *[f'--mode={mode}' for mode in options.modes],
    ]
    if options.list_tests:
        args.extend(['--collect-only', '--quiet', '--no-header'])
    else:
        args.extend([
            f'--junit-xml={junit_output_file}',
            "-rf",
            f'-n{options.jobs}',
            f'--tmpdir={temp_dir}',
            f'--maxfail={options.max_failures}',
            f'--alluredir={report_dir / f"allure_{HOST_ID}"}',
            f'--dist=worksteal',
        ])
    if options.verbose:
        args.append('-v')
    if options.keep_duplicates:
        args.append('--keep-duplicates')
    if options.quiet:
        args.append('--quiet')
        args.extend(['-p','no:sugar'])
    if options.pytest_arg:
        # If pytest_arg is provided, it should be a string with arguments to pass to pytest
        args.extend(shlex.split(options.pytest_arg))
    if options.random_seed:
        args.append(f'--random-seed={options.random_seed}')
    if options.gather_metrics:
        args.append('--gather-metrics')
    if options.coverage:
        args.append('--coverage')
        args.extend(f'--coverage-mode={mode}' for mode in options.coverage_modes)
    if options.artifacts_dir_url:
        args.append(f'--artifacts_dir_url={options.artifacts_dir_url}')
    if options.timeout:
        args.append(f'--timeout={options.timeout}')
    if options.session_timeout:
        args.append(f'--session-timeout={options.session_timeout}')
    if options.skip_patterns:
        args.append(f'-k={" and ".join([f"not {pattern}" for pattern in options.skip_patterns])}')
    if options.k:
        args.append(f'-k={options.k}')
    if options.extra_scylla_cmdline_options:
        args.append(f'--extra-scylla-cmdline-options={options.extra_scylla_cmdline_options}')
    if options.exe_path:
        args.append(f'--exe-path={options.exe_path}')
    if options.exe_url:
        args.append(f'--exe-url={options.exe_url}')
    if not options.save_log_on_success:
        args.append('--allure-no-capture')
    else:
        args.append('--save-log-on-success')
    if options.markers:
        args.append(f'-m={options.markers}')
    if options.log_level:
        args.append(f'--log-level={options.log_level}')
    args.extend(files_to_run)
    return args


def run_pytest(options: argparse.Namespace) -> int:
    """Run the tests: a single ``pytest.main()`` call."""
    return pytest.main(args=pytest_args(options))
