#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.nodetool.rest_api_mock import expected_request


def test_version(nodetool):
    res = nodetool("version", expected_requests=[
        expected_request("GET", "/storage_service/release_version", response="1.2.3")])

    assert res.stdout == "ReleaseVersion: 1.2.3\n"
