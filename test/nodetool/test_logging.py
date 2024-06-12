#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from test.nodetool.rest_api_mock import expected_request
from test.nodetool.utils import check_nodetool_fails_with


def test_getlogginglevels(nodetool):
    res = nodetool("getlogginglevels", expected_requests=[
        expected_request("GET", "/storage_service/logging_level",
                         response=[{"key": "sstable", "value": "info"}, {"key": "cache", "value": "trace"}])])

    assert res.stdout == \
"""
Logger Name                                        Log Level
sstable                                                 info
cache                                                  trace
"""


def test_setlogginglevel(nodetool):
    nodetool("setlogginglevel", "wasm", "trace", expected_requests=[
        expected_request("POST", "/system/logger/wasm", params={"level": "trace"})])


def test_setlogginglevel_reset_logger(nodetool, scylla_only):
    check_nodetool_fails_with(
            nodetool,
            ("setlogginglevel", "wasm"),
            {"expected_requests": []},
            ["error processing arguments: resetting logger(s) is not supported yet, the logger and level parameters are required"])


def test_setlogginglevel_reset_all_loggers(nodetool, scylla_only):
    check_nodetool_fails_with(
            nodetool,
            ("setlogginglevel",),
            {"expected_requests": []},
            ["error processing arguments: resetting logger(s) is not supported yet, the logger and level parameters are required"])
