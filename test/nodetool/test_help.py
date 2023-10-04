#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import pytest
import subprocess

import utils


# These are simple smoke tests, because automatically testing help is next to impossible.


def test_help(nodetool):
    out = nodetool("help")
    assert out


def test_help_command(nodetool):
    out = nodetool("help", "version")
    assert out


def test_help_nonexistent_command(request, nodetool):
    if request.config.getoption("nodetool") == "scylla":
        utils.check_nodetool_fails_with(
                nodetool,
                ("help", "foo",),
                {},
                ["error processing arguments: unknown command foo"])
    else:
        out = nodetool("help", "foo")
        assert out == "Unknown command foo\n\n"


def test_help_command_too_many_args(nodetool, scylla_only):
    utils.check_nodetool_fails_with(
            nodetool,
            ("help", "compact", "foo", "bar"),
            {},
            ["error: too many positional options have been specified on the command line"])


def test_help_consistent(nodetool, scylla_only):
    for command in ("version", "compact", "settraceprobability"):
        out1 = nodetool("help", command)
        # seastar returns 1 when --help is invoked
        with pytest.raises(subprocess.CalledProcessError) as e:
            nodetool(command, "--help")
        assert e.value.returncode == 1
        out2 = e.value.stdout
        assert out1 == out2
