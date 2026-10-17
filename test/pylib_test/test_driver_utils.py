#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.pylib import driver_utils


class _FallbackModes:
    Fallback = object()
    SkipPoolCreation = object()


def test_control_connection_query_fallback_options_unsupported(monkeypatch):
    monkeypatch.delattr(driver_utils.cassandra_cluster, "ControlConnectionQueryFallback", raising=False)

    assert driver_utils.control_connection_query_fallback_options() == {}


def test_control_connection_query_fallback_options(monkeypatch):
    monkeypatch.setattr(driver_utils.cassandra_cluster, "ControlConnectionQueryFallback", _FallbackModes, raising=False)

    assert driver_utils.control_connection_query_fallback_options() == {
        "allow_control_connection_query_fallback": _FallbackModes.Fallback,
    }
    assert driver_utils.control_connection_query_fallback_options(skip_pool_creation=True) == {
        "allow_control_connection_query_fallback": _FallbackModes.SkipPoolCreation,
    }
