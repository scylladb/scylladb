#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from rest_api_mock import expected_request


def test_getlogginglevels(nodetool):
    res = nodetool("getlogginglevels", expected_requests=[
        expected_request("GET", "/storage_service/logging_level",
                         response=[{"key": "sstable", "value": "info"}, {"key": "cache", "value": "trace"}])])

    assert res == \
"""
Logger Name                                        Log Level
sstable                                                 info
cache                                                  trace
"""
