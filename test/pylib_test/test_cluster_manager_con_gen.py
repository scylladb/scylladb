#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Tests for the driver clusters created by ScyllaClusterManager."""

import logging
from types import SimpleNamespace

import pytest
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy

from test.pylib import scylla_cluster_manager
from test.pylib.internal_types import IPAddress
from test.pylib.scylla_cluster_manager import ScyllaClusterManager


@pytest.fixture
def manager(monkeypatch) -> ScyllaClusterManager:
    """Exercise the factory without opening driver connections or threads."""
    monkeypatch.setattr(scylla_cluster_manager, "CassandraCluster", lambda **kwargs: SimpleNamespace(**kwargs))
    return object.__new__(ScyllaClusterManager)


@pytest.mark.parametrize("options", [{}, {"load_balancing_policy": None}], ids=["omitted", "none"])
def test_default_policies_keep_independent_hosts(manager: ScyllaClusterManager, options: dict) -> None:
    # Shared policy state sends requests to another Cluster's Host objects:
    # https://github.com/scylladb/python-driver/issues/1161
    first = manager.con_gen([IPAddress("127.0.0.1")], **options)
    second = manager.con_gen([IPAddress("127.0.0.2")], **options)
    first_policy = first.execution_profiles[EXEC_PROFILE_DEFAULT].load_balancing_policy
    second_policy = second.execution_profiles[EXEC_PROFILE_DEFAULT].load_balancing_policy
    assert isinstance(first_policy, RoundRobinPolicy)
    assert isinstance(second_policy, RoundRobinPolicy)

    first_host, second_host = object(), object()
    first_policy.populate(first, [first_host])
    second_policy.populate(second, [second_host])

    assert list(first_policy.make_query_plan()) == [first_host]
    assert list(second_policy.make_query_plan()) == [second_host]


def test_explicit_policy_is_preserved(manager: ScyllaClusterManager) -> None:
    policy = RoundRobinPolicy()
    cluster = manager.con_gen([IPAddress("127.0.0.1")], load_balancing_policy=policy)

    assert cluster.execution_profiles[EXEC_PROFILE_DEFAULT].load_balancing_policy is policy


@pytest.mark.asyncio
async def test_driver_reconnect_uses_fresh_policy(monkeypatch) -> None:
    # Reconnecting must not carry mutable policy state between driver clusters (#32761).
    monkeypatch.setattr(scylla_cluster_manager, "CassandraCluster", lambda **kwargs: SimpleNamespace(
        **kwargs, connect=lambda: object(), shutdown=lambda: None))
    cluster = SimpleNamespace(logger=logging.getLogger(__name__), api=None)
    manager = ScyllaClusterManager("test_driver_reconnect_uses_fresh_policy", cluster, 9042, False)
    server = SimpleNamespace(rpc_address=IPAddress("127.0.0.1"))
    try:
        await manager.driver_connect(server)
        first_policy = manager.ccluster.execution_profiles[EXEC_PROFILE_DEFAULT].load_balancing_policy
        await manager.driver_connect(server)
        second_policy = manager.ccluster.execution_profiles[EXEC_PROFILE_DEFAULT].load_balancing_policy

        first_host, second_host = object(), object()
        first_policy.populate(None, [first_host])
        second_policy.populate(None, [second_host])
        assert list(first_policy.make_query_plan()) == [first_host]
        assert list(second_policy.make_query_plan()) == [second_host]
    finally:
        manager.driver_close()
        manager.thread_pool.shutdown()
