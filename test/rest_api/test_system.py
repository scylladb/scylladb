# Copyright 2021-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

def test_system_uptime_ms(rest_api):
    resp = rest_api.send('GET', "system/uptime_ms")
    resp.raise_for_status()
