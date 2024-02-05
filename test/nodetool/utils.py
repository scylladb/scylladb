#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest
import subprocess


def _do_check_nodetool_fails_with(
        nodetool,
        nodetool_args: tuple,
        nodetool_kwargs: dict,
        expected_errors: list[str],
        match_all: bool = False):

    with pytest.raises(subprocess.CalledProcessError) as e:
        nodetool(*nodetool_args, **nodetool_kwargs)

    err_lines = e.value.stderr.rstrip().split('\n')
    out_lines = e.value.stdout.rstrip().split('\n')

    match = 0
    for expected_error in expected_errors:
        if expected_error in err_lines or expected_error in out_lines:
            match += 1

    if match_all:
        assert match == len(expected_errors)
    else:
        assert match > 0


def check_nodetool_fails_with(
        nodetool,
        nodetool_args: tuple,
        nodetool_kwargs: dict,
        expected_errors: list[str]):
    _do_check_nodetool_fails_with(nodetool, nodetool_args, nodetool_kwargs, expected_errors, False)


def check_nodetool_fails_with_all(
        nodetool,
        nodetool_args: tuple,
        nodetool_kwargs: dict,
        expected_errors: list[str]):
    _do_check_nodetool_fails_with(nodetool, nodetool_args, nodetool_kwargs, expected_errors, True)
