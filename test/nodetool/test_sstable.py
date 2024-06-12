#
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from test.nodetool.utils import check_nodetool_fails_with
from test.nodetool.rest_api_mock import expected_request
import pytest


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
    assert res.stdout == (
"""/var/lib/scylla/data/ks/tbl-3ca78460d61611eea0b49524e39553c0/me-3gec_0mu7_5az0024x96bfm476r6-big-Data.db
/var/lib/scylla/data/ks/tbl-3ca78460d61611eea0b49524e39553c0/me-3gec_0mu7_7bz0024x96bfm476r6-big-Data.db
""")


def test_getsstables_unknown_ks(nodetool, scylla_only):
    check_nodetool_fails_with(
            nodetool,
            ("getsstables", "unknown_ks", "tbl", "mykey"),
            {"expected_requests": [expected_request("GET", "/column_family/",
                                                    response=[{"ks": "ks", "cf": "tbl", "type": "ColumnFamilies"}])]},
            ["error processing arguments: unknown keyspace: unknown_ks"],
    )


def test_getsstables_unknown_tbl(nodetool):
    check_nodetool_fails_with(
            nodetool,
            ("getsstables", "ks", "unknown_tbl", "mykey"),
            {"expected_requests": [expected_request("GET", "/column_family/",
                                                    response=[{"ks": "ks", "cf": "tbl", "type": "ColumnFamilies"}])]},
            ["error processing arguments: unknown table: unknown_tbl",
             "ColumnFamilyStore for ks/unknown_tbl not found."]
    )


ks_tbl_sstable_info = {
  "keyspace": "ks",
  "table": "tbl",
  "sstables": [
    {
      "size": 5746,
      "data_size": 90,
      "index_size": 24,
      "filter_size": 332,
      "timestamp": "2024-03-11T08:13:19Z",
      "generation": "3gec_0mu7_5az0024x96bfm476r6",
      "level": 0,
      "version": "me",
      "extended_properties": [
        {
          "group": "compression_parameters",
          "attributes": [
            {
              "key": "sstable_compression",
              "value": "org.apache.cassandra.io.compress.LZ4Compressor"
            }
          ]
        }
      ]
    },
    {
      "size": 6746,
      "data_size": 290,
      "index_size": 124,
      "filter_size": 232,
      "timestamp": "2024-03-10T08:13:19Z",
      "generation": "3gec_0mu7_6bz0024x96bfm476r6",
      "level": 0,
      "version": "me",
      "properties": [
        {
          "key": "foo",
          "value": "bar"
        }
      ]
    }
  ]
}


ks_tbl2_sstable_info = {
  "keyspace": "ks",
  "table": "tbl2",
  "sstables": [
    {
      "size": 5481,
      "data_size": 44,
      "index_size": 8,
      "filter_size": 172,
      "timestamp": "2024-03-11T08:13:20Z",
      "generation": "3gec_0mu8_5vrgh24x96bfm476r6",
      "level": 0,
      "version": "me",
      "extended_properties": [
        {
          "group": "compression_parameters",
          "attributes": [
            {
              "key": "sstable_compression",
              "value": "org.apache.cassandra.io.compress.LZ4Compressor"
            }
          ]
        }
      ]
    }
  ]
}


ks2_tbl_sstable_info = {
  "keyspace": "ks2",
  "table": "tbl"
}


def _check_sstableinfo_output(res, info, is_cassandra):
    lines = res.split('\n')
    i = 0

    assert lines[i] == ''
    i += 1

    def split(ln):
        return tuple(part.strip() for part in ln.split(':'))

    for entry in info:
        if "sstables" not in entry and is_cassandra:
            i += 2
        else:
            assert split(lines[i]) == ("keyspace", entry["keyspace"])
            i += 1

            assert split(lines[i]) == ("table", entry["table"])
            i += 1

        if "sstables" in entry:
            assert lines[i] == "sstables :"
            i += 1
        else:
            continue

        for index, sstable in enumerate(entry["sstables"]):
            assert lines[i].lstrip() == f"{index} :"
            i += 1

            for key in ["data_size", "filter_size", "index_size", "level", "size", "generation", "version",
                        "timestamp"]:
                print_key = key.replace("_", " ")
                if print_key == "timestamp":
                    parts = split(lines[i])
                    assert parts[0] == print_key
                    # Java nodetool does a weird reformatting of the date which I see no sense in replicating
                    if not is_cassandra:
                        assert ":".join(parts[1:]) == sstable[key]
                else:
                    assert split(lines[i]) == (print_key, str(sstable[key]))
                i += 1

            if "properties" in sstable:
                assert lines[i].strip() == "properties :"
                i += 1

                for prop in sstable["properties"]:
                    assert split(lines[i]) == (prop["key"], prop["value"])
                    i += 1

            if "extended_properties" in sstable:
                assert lines[i].strip() == "extended properties :"
                i += 1

                for ext_prop in sstable["extended_properties"]:
                    assert lines[i].strip() == f"{ext_prop['group']} :"
                    i += 1

                    for attr in ext_prop["attributes"]:
                        assert split(lines[i]) == (attr["key"], attr["value"])
                        i += 1


def test_sstableinfo(nodetool, request):
    info = [ks_tbl_sstable_info, ks_tbl2_sstable_info, ks2_tbl_sstable_info]
    res = nodetool("sstableinfo", expected_requests=[
        expected_request(
            "GET",
            "/storage_service/sstable_info",
            response=info),
    ])
    _check_sstableinfo_output(res.stdout, info, request.config.getoption("nodetool") == "cassandra")


def test_sstableinfo_keyspace(nodetool, request):
    info = [ks_tbl_sstable_info, ks_tbl2_sstable_info]
    res = nodetool("sstableinfo", "ks", expected_requests=[
        expected_request(
            "GET",
            "/storage_service/sstable_info",
            params={"keyspace": "ks"},
            response=info),
    ])
    _check_sstableinfo_output(res.stdout, info, request.config.getoption("nodetool") == "cassandra")


def test_sstableinfo_keyspace_table(nodetool, request):
    info = [ks_tbl_sstable_info]
    res = nodetool("sstableinfo", "ks", "tbl", expected_requests=[
        expected_request(
            "GET",
            "/storage_service/sstable_info",
            params={"keyspace": "ks", "cf": "tbl"},
            response=info),
    ])
    _check_sstableinfo_output(res.stdout, info, request.config.getoption("nodetool") == "cassandra")


def test_sstableinfo_keyspace_tables(nodetool, request):
    info = [ks_tbl_sstable_info, ks_tbl2_sstable_info]
    res = nodetool("sstableinfo", "ks", "tbl", "tbl2", expected_requests=[
        expected_request(
            "GET",
            "/storage_service/sstable_info",
            params={"keyspace": "ks", "cf": "tbl"},
            response=[ks_tbl_sstable_info]),
        expected_request(
            "GET",
            "/storage_service/sstable_info",
            params={"keyspace": "ks", "cf": "tbl2"},
            response=[ks_tbl2_sstable_info]),
    ])
    _check_sstableinfo_output(res.stdout, info, request.config.getoption("nodetool") == "cassandra")
