# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

def test_system_uptime_ms(rest_api):
    resp = rest_api.send('GET', "system/uptime_ms")
    resp.raise_for_status()


def test_system_highest_sstable_format(rest_api):
    resp = rest_api.send('GET', "system/highest_supported_sstable_version")
    resp.raise_for_status()
    assert resp.json() == "me"
