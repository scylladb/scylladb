#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from rest_api_mock import expected_request
import pytest
import utils


@pytest.mark.parametrize("key_option", (None, "-hf", "--hex-format"))
def test_getsstables(nodetool, key_option):
    cmd = ["getsstables", "ks", "tbl", "mykey"]
    params = {"key": "mykey"}
    if key_option:
        cmd.append(key_option)
        params["format"] = "hex"
    res = nodetool(*cmd, expected_requests=[
        expected_request("GET", "/column_family/",
                         response=[{"ks": "ks", "cf": "tbl", "type": "ColumnFamilies"}]),
        expected_request(
            "GET",
            "/column_family/sstables/by_key/ks:tbl",
            params=params,
            response=[
                "/var/lib/scylla/data/ks/tbl-3ca78460d61611eea0b49524e39553c0/me-3gec_0mu7_5az0024x96bfm476r6-big-Data.db",
                "/var/lib/scylla/data/ks/tbl-3ca78460d61611eea0b49524e39553c0/me-3gec_0mu7_7bz0024x96bfm476r6-big-Data.db",
                ]),
    ])
    assert res == (
"""/var/lib/scylla/data/ks/tbl-3ca78460d61611eea0b49524e39553c0/me-3gec_0mu7_5az0024x96bfm476r6-big-Data.db
/var/lib/scylla/data/ks/tbl-3ca78460d61611eea0b49524e39553c0/me-3gec_0mu7_7bz0024x96bfm476r6-big-Data.db
""")


def test_getsstables_unknown_ks(nodetool, scylla_only):
    utils.check_nodetool_fails_with(
            nodetool,
            ("getsstables", "unknown_ks", "tbl", "mykey"),
            {"expected_requests": [expected_request("GET", "/column_family/",
                                                    response=[{"ks": "ks", "cf": "tbl", "type": "ColumnFamilies"}])]},
            ["error processing arguments: unknown keyspace: unknown_ks"],
    )


def test_getsstables_unknown_tbl(nodetool):
    utils.check_nodetool_fails_with(
            nodetool,
            ("getsstables", "ks", "unknown_tbl", "mykey"),
            {"expected_requests": [expected_request("GET", "/column_family/",
                                                    response=[{"ks": "ks", "cf": "tbl", "type": "ColumnFamilies"}])]},
            ["error processing arguments: unknown table: unknown_tbl",
             "ColumnFamilyStore for ks/unknown_tbl not found."]
    )
