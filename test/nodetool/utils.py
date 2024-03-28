#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import itertools


def _do_check_nodetool_fails_with(
        nodetool,
        nodetool_args: tuple,
        nodetool_kwargs: dict,
        matcher):

    res = nodetool(*nodetool_args, **nodetool_kwargs, check_return_code=False)

    assert res.returncode != 0

    err_lines = res.stderr.rstrip().split('\n')
    out_lines = res.stdout.rstrip().split('\n')

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


def format_size(v):
    units = {
        1 << 40: 'TB',
        1 << 30: 'GB',
        1 << 20: 'MB',
        1 << 10: 'KB',
    }

    for n, unit in units.items():
        if v > n:
            d = v / n
            return f'{d:.2f} {unit}'
    return f'{v} bytes'
