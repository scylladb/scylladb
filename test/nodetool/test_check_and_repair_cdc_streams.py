#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.nodetool.rest_api_mock import expected_request


def test_checkandrepaircdcstreams(nodetool):
    nodetool("checkAndRepairCdcStreams", expected_requests=[
        expected_request("POST", "/storage_service/cdc_streams_check_and_repair")
    ])
