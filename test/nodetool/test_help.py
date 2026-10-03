#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.nodetool.utils import check_nodetool_fails_with


# These are simple smoke tests, because automatically testing help is next to impossible.


def test_help(nodetool):
    res = nodetool("help")
    assert res.stdout


def test_help_command(nodetool):
    res = nodetool("help", "version")
    assert res.stdout


def test_help_nonexistent_command(nodetool):
    check_nodetool_fails_with(
            nodetool,
            ("help", "foo",),
            {},
            ["error processing arguments: unknown command foo"])


def test_help_command_too_many_args(nodetool):
    check_nodetool_fails_with(
            nodetool,
            ("help", "compact", "foo", "bar"),
            {},
            ["error processing arguments: unknown command compact foo bar"])


def test_help_consistent(nodetool):
    for command in ("version", "compact", "settraceprobability"):
        res1 = nodetool("help", command)
        res2 = nodetool(command, "--help")
        assert res1.stdout == res2.stdout
