#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest
import subprocess
import typing


def check_nodetool_fails_with(
        nodetool,
        nodetool_args: tuple,
        nodetool_kwargs: dict,
        expected_errors: typing.List[str]):

    with pytest.raises(subprocess.CalledProcessError) as e:
        nodetool(*nodetool_args, **nodetool_kwargs)

    err_lines = e.value.stderr.rstrip().split('\n')
    out_lines = e.value.stdout.rstrip().split('\n')

    match = False
    for expected_error in expected_errors:
        if expected_error in err_lines or expected_error in out_lines:
            match = True

    assert match
