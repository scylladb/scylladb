#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.nodetool.rest_api_mock import expected_request


def test_resetlocalschema(nodetool):
    nodetool("resetlocalschema", expected_requests=[
        expected_request("POST", "/storage_service/relocal_schema")])
