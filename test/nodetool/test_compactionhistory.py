#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import datetime
import json
import yaml

from test.nodetool.rest_api_mock import expected_request
from test.nodetool.utils import check_nodetool_fails_with


def format_compacted_at(compacted_at: int):
    compacted_at_time = compacted_at / 1000
    milliseconds = compacted_at % 1000
    return "{:%FT%T}.{}".format(
            datetime.datetime.fromtimestamp(compacted_at_time),
            milliseconds)


HISTORY_RESPONSE = [
        {
            "id": "edde9300-5e9c-11ee-a8f6-7d85dcfeb8f4",
            "cf": "peers",
            "ks": "system",
            "compacted_at": 1695973859380,
            "bytes_in": 11714,
            "bytes_out": 11808,
        },
        {
            "id": "edef82f0-5e9c-11ee-a8f6-7d85dcfeb8f4",
            "cf": "functions",
            "ks": "system_schema",
            "compacted_at": 1695973859491,
            "bytes_in": 5790,
            "bytes_out": 5944,
        }]

EXPECTED_REQUEST = expected_request("GET", "/compaction_manager/compaction_history", response=HISTORY_RESPONSE)


def test_text(request, nodetool):
    expected_res_cassandra = \
"""Compaction History: 
id                                   keyspace_name columnfamily_name compacted_at            bytes_in bytes_out rows_merged
edef82f0-5e9c-11ee-a8f6-7d85dcfeb8f4 system_schema functions         {} 5790     5944                 
edde9300-5e9c-11ee-a8f6-7d85dcfeb8f4 system        peers             {} 11714    11808                
""".format(format_compacted_at(1695973859491), format_compacted_at(1695973859380))

    # Scylla aligns number columns to the right.
    expected_res_scylla = \
"""Compaction History:
id                                   keyspace_name columnfamily_name compacted_at            bytes_in bytes_out rows_merged
edef82f0-5e9c-11ee-a8f6-7d85dcfeb8f4 system_schema functions         {}     5790      5944            
edde9300-5e9c-11ee-a8f6-7d85dcfeb8f4 system        peers             {}    11714     11808            
""".format(format_compacted_at(1695973859491), format_compacted_at(1695973859380))

    for cmd in [("compactionhistory",), ("compactionhistory", "--format", "text"), ("compactionhistory", "-F", "text")]:
        if request.config.getoption("nodetool") != "scylla" and len(cmd) > 1:
            # The -F text is a scylla-extension, cassandra-nodetool doesn't support it
            continue

        res = nodetool(*cmd, expected_requests=[EXPECTED_REQUEST])

        if request.config.getoption("nodetool") == "scylla":
            assert res.stdout == expected_res_scylla
        else:
            assert res.stdout == expected_res_cassandra


def test_json(nodetool):
    expected_res = {
            "CompactionHistory": [
                {
                    "id": "edef82f0-5e9c-11ee-a8f6-7d85dcfeb8f4",
                    "columnfamily_name": "functions",
                    "keyspace_name": "system_schema",
                    "compacted_at": format_compacted_at(1695973859491),
                    "bytes_in": 5790,
                    "bytes_out": 5944,
                    "rows_merged": "",
                },
                {
                    "id": "edde9300-5e9c-11ee-a8f6-7d85dcfeb8f4",
                    "columnfamily_name": "peers",
                    "keyspace_name": "system",
                    "compacted_at": format_compacted_at(1695973859380),
                    "bytes_in": 11714,
                    "bytes_out": 11808,
                    "rows_merged": "",
                }
            ]
        }

    for cmd in [("compactionhistory", "--format", "json"), ("compactionhistory", "-F", "json")]:
        res = nodetool(*cmd, expected_requests=[EXPECTED_REQUEST])

        assert json.loads(res.stdout) == expected_res


def test_yaml(nodetool):
    expected_res = {
            "CompactionHistory": [
                {
                    "id": "edef82f0-5e9c-11ee-a8f6-7d85dcfeb8f4",
                    "columnfamily_name": "functions",
                    "keyspace_name": "system_schema",
                    "compacted_at": format_compacted_at(1695973859491),
                    "bytes_in": 5790,
                    "bytes_out": 5944,
                    "rows_merged": "",
                },
                {
                    "id": "edde9300-5e9c-11ee-a8f6-7d85dcfeb8f4",
                    "columnfamily_name": "peers",
                    "keyspace_name": "system",
                    "compacted_at": format_compacted_at(1695973859380),
                    "bytes_in": 11714,
                    "bytes_out": 11808,
                    "rows_merged": "",
                }
            ]
        }

    for cmd in [("compactionhistory", "--format", "yaml"), ("compactionhistory", "-F", "yaml")]:
        res = nodetool(*cmd, expected_requests=[EXPECTED_REQUEST])

        assert yaml.load(res.stdout, Loader=yaml.Loader) == expected_res


def test_invalid_format(nodetool):
    check_nodetool_fails_with(
            nodetool,
            ("compactionhistory", "-F", "foo"),
            {},
            ["error processing arguments: invalid format foo, valid formats are: {text, json, yaml}",
             "error processing arguments: invalid format foo, valid formats are: [\"text\", \"json\", \"yaml\"]",
             "nodetool: arguments for -F are json,yaml only."])
