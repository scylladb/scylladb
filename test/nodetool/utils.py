#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest
import subprocess
import itertools


def _do_check_nodetool_fails_with(
        nodetool,
        nodetool_args: tuple,
        nodetool_kwargs: dict,
        matcher):

    with pytest.raises(subprocess.CalledProcessError) as e:
        nodetool(*nodetool_args, **nodetool_kwargs)

    err_lines = e.value.stderr.rstrip().split('\n')
    out_lines = e.value.stdout.rstrip().split('\n')

    matcher(out_lines, err_lines)


def check_nodetool_fails_with(
        nodetool,
        nodetool_args: tuple,
        nodetool_kwargs: dict,
        expected_errors: list[str]):

    def matcher(out_lines, err_lines):
        match = False
        for expected_error in expected_errors:
            if expected_error in err_lines or expected_error in out_lines:
                match = True
                break
        assert match, f'expected error not found in {err_lines=}, {out_lines}'

    _do_check_nodetool_fails_with(nodetool, nodetool_args, nodetool_kwargs, matcher)


def check_nodetool_fails_with_all(
        nodetool,
        nodetool_args: tuple,
        nodetool_kwargs: dict,
        expected_errors: list[str]):

    def matcher(out_lines, err_lines):
        match = 0
        for expected_error in expected_errors:
            if expected_error in err_lines or expected_error in out_lines:
                match += 1
        assert match == len(expected_errors), f'not all expected errors are found in {out_lines=} and {err_lines}'

    _do_check_nodetool_fails_with(nodetool, nodetool_args, nodetool_kwargs, matcher)


def check_nodetool_fails_with_error_contains(
        nodetool,
        nodetool_args: tuple,
        nodetool_kwargs: dict,
        expected_errors: list[str]):

    def matcher(out_lines, err_lines):
        match = False
        for expected_error in expected_errors:
            for err_line in itertools.chain(out_lines, err_lines):
                if expected_error in err_line:
                    match = True
                    break

        assert match, f'none of expected errors is found in {out_lines=} and {err_lines}'

    _do_check_nodetool_fails_with(nodetool, nodetool_args, nodetool_kwargs, matcher)
