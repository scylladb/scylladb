#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.nodetool.rest_api_mock import expected_request


def test_drain(nodetool):
    nodetool("drain", expected_requests=[
        expected_request("POST", "/storage_service/drain")
    ])
