#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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
            "shard_id": 0,
            "cf": "peers",
            "ks": "system",
            "compaction_type": "Compact",
            "started_at": 1695973259380,
            "compacted_at": 1695973859380,
            "bytes_in": 11714,
            "bytes_out": 11808,
            "rows_merged": [{"key": 1, "value": 12}],
            "sstables_in": [
                {
                    "generation": "3glx_0srx_1lvc01zhjn9048se29",
                    "origin": "memtable",
                    "size": 5466,
                },
                {
                    "generation": "3glx_0sru_3zlr42ksepk902v8dt",
                    "origin": "memtable",
                    "size": 5519,
                }],
            "sstables_out": [
                {
                    "generation": "3glx_0srx_1o0hs1zhjn9048se29",
                    "origin": "compaction",
                    "size": 5457,
                }],
            "total_tombstone_purge_attempt": 14,
            "total_tombstone_purge_failure_due_to_overlapping_with_memtable": 0,
            "total_tombstone_purge_failure_due_to_overlapping_with_uncompacting_sstable": 0,
        },
        {
            "id": "edef82f0-5e9c-11ee-a8f6-7d85dcfeb8f4",
            "shard_id": 0,
            "cf": "functions",
            "ks": "system_schema",
            "compaction_type": "Compact",
            "started_at": 1695973259491,
            "compacted_at": 1695973859491,
            "bytes_in": 5790,
            "bytes_out": 5944,
            "rows_merged": [{"key": 1, "value": 5}, {"key": 2, "value": 1}],
            "sstables_in": [
                {
                    "generation": "3glx_0srx_1lvc02ksepk902v8dt",
                    "origin": "memtable",
                    "size": 5668,
                },
                {
                    "generation": "3glx_0sru_3zlr42ksepk902v8dt",
                    "origin": "memtable",
                    "size": 5669,
                }],
            "sstables_out": [
                {
                    "generation": "3glx_0srx_1pasg2ksepk902v8dt",
                    "origin": "compaction",
                    "size": 5687,
                }],
            "total_tombstone_purge_attempt": 14,
            "total_tombstone_purge_failure_due_to_overlapping_with_memtable": 5,
            "total_tombstone_purge_failure_due_to_overlapping_with_uncompacting_sstable": 5,
        },
        # "rows_merged", "sstables_in", "sstables_out" fields missing on purpose. It happens when dealing with
        # moxed node clusters. In this case, the fields are not present in the response.
        {
            "id": "8b857440-0e35-11f0-9fcc-a895cfb212f2",
            "shard_id": 0,
            "cf": "functions",
            "ks": "system_schema",
            "compaction_type": "Compact",
            "started_at": 1695973859492,
            "compacted_at": 1695973859492,
            "bytes_in": 579,
            "bytes_out": 594,
            "total_tombstone_purge_attempt": 0,
            "total_tombstone_purge_failure_due_to_overlapping_with_memtable": 0,
            "total_tombstone_purge_failure_due_to_overlapping_with_uncompacting_sstable": 0,
        }]

EXPECTED_REQUEST = expected_request("GET", "/compaction_manager/compaction_history", response=HISTORY_RESPONSE)


def _test_text_nodetool_cassandra(nodetool):
    expected_response = \
"""Compaction History:
id                                   keyspace_name columnfamily_name compacted_at            bytes_in bytes_out rows_merged
8b857440-0e35-11f0-9fcc-a895cfb212f2 system_schema functions         {} 579      594   {{}}
edef82f0-5e9c-11ee-a8f6-7d85dcfeb8f4 system_schema functions         {} 5790     5944  {{1: 5, 2: 1}}
edde9300-5e9c-11ee-a8f6-7d85dcfeb8f4 system        peers             {} 11714    11808 {{1: 12}}
""".format(format_compacted_at(1695973859492), format_compacted_at(1695973859491), format_compacted_at(1695973259380))

    VALID_KEYS = ["id", "cf", "ks", "compaction_type", "compacted_at", "bytes_in", "bytes_out"]
    CASSANDRA_HISTORY_RESPONSE = [{key: data[key] for key in VALID_KEYS} for data in HISTORY_RESPONSE]
    CASSANDRA_EXPECTED_REQUEST = expected_request("GET", "/compaction_manager/compaction_history", response=CASSANDRA_HISTORY_RESPONSE)

    for cmd in [("compactionhistory",), ("compactionhistory", "--format", "text")]:
        response = nodetool(*cmd, expected_requests=[CASSANDRA_EXPECTED_REQUEST])
        assert response.stdout == expected_response


def _test_text_nodetool_scylla(nodetool):
    # Scylla aligns number columns to the right.
    expected_response = \
"""Compaction History:
id                                   shard_id keyspace_name columnfamily_name compaction_type started_at              compacted_at            bytes_in bytes_out rows_merged  sstables_in                                                                                                                                                          sstables_out                                                                         total_tombstone_purge_attempt total_tombstone_purge_failure_due_to_overlapping_with_memtable total_tombstone_purge_failure_due_to_overlapping_with_uncompacting_sstable 
8b857440-0e35-11f0-9fcc-a895cfb212f2        0 system_schema functions         Compact         {} {}      579       594 {{}}           []                                                                                                                                                                   []                                                                                   0                             0                                                              0                                                                          
edef82f0-5e9c-11ee-a8f6-7d85dcfeb8f4        0 system_schema functions         Compact         {} {}     5790      5944 {{1: 5, 2: 1}} [{{generation: 5d022760-b617-11ef-a97d-8438c36f0e31, origin: memtable, size: 5668}}, {{generation: 5b756ce0-b617-11ef-a97d-8438c36f0e31, origin: memtable, size: 5669}}] [{{generation: 5d049860-b617-11ef-a97d-8438c36f0e31, origin: compaction, size: 5687}}] 14                            5                                                              5                                                                          
edde9300-5e9c-11ee-a8f6-7d85dcfeb8f4        0 system        peers             Compact         {} {}    11714     11808 {{1: 12}}      [{{generation: 5d022760-b617-11ef-8294-8437c36f0e31, origin: memtable, size: 5466}}, {{generation: 5b756ce0-b617-11ef-a97d-8438c36f0e31, origin: memtable, size: 5519}}] [{{generation: 5d03ae00-b617-11ef-8294-8437c36f0e31, origin: compaction, size: 5457}}] 14                            0                                                              0                                                                          
""".format(format_compacted_at(1695973859492), format_compacted_at(1695973859492), format_compacted_at(1695973259491), format_compacted_at(1695973859491), format_compacted_at(1695973259380), format_compacted_at(1695973859380))

    assert "rows_merged" not in HISTORY_RESPONSE[-1]
    for cmd in [("compactionhistory",), ("compactionhistory", "--format", "text"), ("compactionhistory", "-F", "text")]:
        response = nodetool(*cmd, expected_requests=[EXPECTED_REQUEST])
        assert response.stdout == expected_response


def test_text(request, nodetool):
    if request.config.getoption("nodetool") == "scylla":
        _test_text_nodetool_scylla(nodetool)
    else:
        _test_text_nodetool_cassandra(nodetool)


def test_json(nodetool):
    expected_res = {
            "CompactionHistory": [
                {
                    "id": "8b857440-0e35-11f0-9fcc-a895cfb212f2",
                    "shard_id": 0,
                    "columnfamily_name": "functions",
                    "keyspace_name": "system_schema",
                    "compaction_type": "Compact",
                    "started_at": format_compacted_at(1695973859492),
                    "compacted_at": format_compacted_at(1695973859492),
                    "bytes_in": 579,
                    "bytes_out": 594,
                    "rows_merged": [],
                    "sstables_in": [],
                    "sstables_out": [],
                    "total_tombstone_purge_attempt": 0,
                    "total_tombstone_purge_failure_due_to_overlapping_with_memtable": 0,
                    "total_tombstone_purge_failure_due_to_overlapping_with_uncompacting_sstable": 0,
                },
                {
                    "id": "edef82f0-5e9c-11ee-a8f6-7d85dcfeb8f4",
                    "shard_id": 0,
                    "columnfamily_name": "functions",
                    "keyspace_name": "system_schema",
                    "compaction_type": "Compact",
                    "started_at": format_compacted_at(1695973259491),
                    "compacted_at": format_compacted_at(1695973859491),
                    "bytes_in": 5790,
                    "bytes_out": 5944,
                    "rows_merged": [
                        {
                            "key": 1,
                            "value": 5
                        },
                        {
                            "key": 2,
                            "value": 1
                        }
                    ],
                    "sstables_in": [
                        {
                            "generation": "5d022760-b617-11ef-a97d-8438c36f0e31",
                            "origin": "memtable",
                            "size": 5668,
                        },
                        {
                            "generation": "5b756ce0-b617-11ef-a97d-8438c36f0e31",
                            "origin": "memtable",
                            "size": 5669,
                        }],
                    "sstables_out": [
                        {
                            "generation": "5d049860-b617-11ef-a97d-8438c36f0e31",
                            "origin": "compaction",
                            "size": 5687,
                        }],
                    "total_tombstone_purge_attempt": 14,
                    "total_tombstone_purge_failure_due_to_overlapping_with_memtable": 5,
                    "total_tombstone_purge_failure_due_to_overlapping_with_uncompacting_sstable": 5,
                },
                {
                    "id": "edde9300-5e9c-11ee-a8f6-7d85dcfeb8f4",
                    "shard_id": 0,
                    "columnfamily_name": "peers",
                    "keyspace_name": "system",
                    "compaction_type": "Compact",
                    "started_at": format_compacted_at(1695973259380),
                    "compacted_at": format_compacted_at(1695973859380),
                    "bytes_in": 11714,
                    "bytes_out": 11808,
                    "rows_merged": [
                        {
                            "key": 1,
                            "value": 12
                        }
                    ],
                    "sstables_in": [
                        {
                            "generation": "5d022760-b617-11ef-8294-8437c36f0e31",
                            "origin": "memtable",
                            "size": 5466,
                        },
                        {
                            "generation": "5b756ce0-b617-11ef-a97d-8438c36f0e31",
                            "origin": "memtable",
                            "size": 5519,
                        }],
                    "sstables_out": [
                        {
                            "generation": "5d03ae00-b617-11ef-8294-8437c36f0e31",
                            "origin": "compaction",
                            "size": 5457,
                        }],
                    "total_tombstone_purge_attempt": 14,
                    "total_tombstone_purge_failure_due_to_overlapping_with_memtable": 0,
                    "total_tombstone_purge_failure_due_to_overlapping_with_uncompacting_sstable": 0,
                }
            ]
        }

    assert "rows_merged" not in HISTORY_RESPONSE[-1]
    for cmd in [("compactionhistory", "--format", "json"), ("compactionhistory", "-F", "json")]:
        res = nodetool(*cmd, expected_requests=[EXPECTED_REQUEST])
        assert json.loads(res.stdout) == expected_res


def test_yaml(nodetool):
    expected_res = {
            "CompactionHistory": [
                {
                    "id": "8b857440-0e35-11f0-9fcc-a895cfb212f2",
                    "shard_id": 0,
                    "columnfamily_name": "functions",
                    "keyspace_name": "system_schema",
                    "compaction_type": "Compact",
                    "started_at": format_compacted_at(1695973859492),
                    "compacted_at": format_compacted_at(1695973859492),
                    "bytes_in": 579,
                    "bytes_out": 594,
                    "rows_merged": [],
                    "sstables_in": [],
                    "sstables_out": [],
                    "total_tombstone_purge_attempt": 0,
                    "total_tombstone_purge_failure_due_to_overlapping_with_memtable": 0,
                    "total_tombstone_purge_failure_due_to_overlapping_with_uncompacting_sstable": 0,
                },
                {
                    "id": "edef82f0-5e9c-11ee-a8f6-7d85dcfeb8f4",
                    "shard_id": 0,
                    "columnfamily_name": "functions",
                    "keyspace_name": "system_schema",
                    "compaction_type": "Compact",
                    "started_at": format_compacted_at(1695973259491),
                    "compacted_at": format_compacted_at(1695973859491),
                    "bytes_in": 5790,
                    "bytes_out": 5944,
                    "rows_merged": [
                        {
                            "key": 1,
                            "value": 5
                        },
                        {
                            "key": 2,
                            "value": 1
                        }
                    ],
                    "sstables_in": [
                        {
                            "generation": "5d022760-b617-11ef-a97d-8438c36f0e31",
                            "origin": "memtable",
                            "size": 5668,
                        },
                        {
                            "generation": "5b756ce0-b617-11ef-a97d-8438c36f0e31",
                            "origin": "memtable",
                            "size": 5669,
                        }],
                    "sstables_out": [
                        {
                            "generation": "5d049860-b617-11ef-a97d-8438c36f0e31",
                            "origin": "compaction",
                            "size": 5687,
                        }],
                    "total_tombstone_purge_attempt": 14,
                    "total_tombstone_purge_failure_due_to_overlapping_with_memtable": 5,
                    "total_tombstone_purge_failure_due_to_overlapping_with_uncompacting_sstable": 5,
                },
                {
                    "id": "edde9300-5e9c-11ee-a8f6-7d85dcfeb8f4",
                    "shard_id": 0,
                    "columnfamily_name": "peers",
                    "keyspace_name": "system",
                    "compaction_type": "Compact",
                    "started_at": format_compacted_at(1695973259380),
                    "compacted_at": format_compacted_at(1695973859380),
                    "bytes_in": 11714,
                    "bytes_out": 11808,
                    "rows_merged": [
                        {
                            "key": 1,
                            "value": 12
                        }
                    ],
                    "sstables_in": [
                        {
                            "generation": "5d022760-b617-11ef-8294-8437c36f0e31",
                            "origin": "memtable",
                            "size": 5466,
                        },
                        {
                            "generation": "5b756ce0-b617-11ef-a97d-8438c36f0e31",
                            "origin": "memtable",
                            "size": 5519,
                        }],
                    "sstables_out": [
                        {
                            "generation": "5d03ae00-b617-11ef-8294-8437c36f0e31",
                            "origin": "compaction",
                            "size": 5457,
                        }],
                    "total_tombstone_purge_attempt": 14,
                    "total_tombstone_purge_failure_due_to_overlapping_with_memtable": 0,
                    "total_tombstone_purge_failure_due_to_overlapping_with_uncompacting_sstable": 0,
                }
            ]
        }

    assert "rows_merged" not in HISTORY_RESPONSE[-1]
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
