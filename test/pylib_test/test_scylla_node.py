#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from types import SimpleNamespace

import pytest

from test.cluster.dtest.ccmlib.scylla_node import ScyllaNode
from test.pylib.internal_types import IPAddress, ServerNum
from test.pylib.manager_client import NoSuchProcess


class FakeManager:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def server_get_returncode(self, server_id: ServerNum) -> int:
        raise NoSuchProcess()

    def server_start(self, **kwargs) -> None:
        self.calls.append(kwargs)


class FakeCluster:
    def __init__(self) -> None:
        self.manager = FakeManager()
        self.scylla_mode = "release"


def test_scylla_node_reports_memory_override_from_scylla_ext_opts(monkeypatch: pytest.MonkeyPatch) -> None:
    # Regression test for SCYLLA_EXT_OPTS memory overrides being visible to the start-time resource check.
    monkeypatch.setenv("SCYLLA_EXT_OPTS", "--memory=2G")
    monkeypatch.setattr(ScyllaNode, "mark_log", lambda self, filename=None: 0)

    cluster = FakeCluster()
    server = SimpleNamespace(
        server_id=ServerNum(1),
        ip_addr=IPAddress("127.0.0.1"),
        rpc_address=IPAddress("127.0.0.1"),
        datacenter="dc1",
        rack="rack1",
        pid=1234,
    )
    node = ScyllaNode(cluster, server, "node1")

    node.start(no_wait=True, wait_for_binary_proto=False, wait_other_notice=False)

    assert cluster.manager.calls
    assert cluster.manager.calls[0]["has_scylla_memory_override"] is True
