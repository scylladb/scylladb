#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging
import pytest
import time

from cassandra import ConsistencyLevel
from cassandra.protocol import ConfigurationException, InvalidRequest
from cassandra.query import SimpleStatement

from test.pylib.async_cql import _wrap_future
from test.pylib.manager_client import ManagerClient, wait_for_cql_and_get_hosts
from test.pylib.util import unique_name
from test.cluster.util import new_test_keyspace

logger = logging.getLogger(__name__)


async def create_ks_and_assert_warning(cql, query, ks_name, key_warn_msg_words):
    # We have to use `Session::execute_async` here to be able to obtain `warnings`.
    ret = cql.execute_async(query)
    await _wrap_future(ret)
    found = False
    if len(key_warn_msg_words) > 0:
        assert len(ret.warnings) >= 1, "Expected RF guardrail warning"
        for warning in ret.warnings:
            found = found or all(word in warning.lower() for word in key_warn_msg_words)
        assert found, "Didn't match all required keywords"
    await cql.run_async(f"USE {ks_name}")


async def assert_creating_ks_fails(cql, query, ks_name):
    with pytest.raises(ConfigurationException):
        await cql.run_async(query)
    with pytest.raises(InvalidRequest):
        await cql.run_async(f"USE {ks_name}")


@pytest.mark.asyncio
async def test_default_rf(manager: ManagerClient):
    """
    As of now, the only RF guardrail enabled is a soft limit checking that RF >= 3. Not complying to this soft limit
    results in a CQL query being executed, but with a warning. Also, whatever the guardrails' values, RF = 0 is always OK.
    """

    # FIXME: This test verifies that guardrails work. However, if we set `rf_rack_valid_keyspaces` to true,
    # we'll get a different error, so let's disable it for now. For more context, see issues:
    #   scylladb/scylladb#23071 and scylladb/scylla-dtest#5633.
    cfg = {"rf_rack_valid_keyspaces": False}

    await manager.server_add(config=cfg, property_file={"dc": "dc1", "rack": "r1"})
    await manager.server_add(config=cfg, property_file={"dc": "dc2", "rack": "r1"})
    await manager.server_add(config=cfg, property_file={"dc": "dc3", "rack": "r1"})

    cql = manager.get_cql()
    ks_name = unique_name()
    rf = {"dc1": 2, "dc2": 3, "dc3": 0}
    options = ", ".join([f"'{dc}':{rf_val}" for dc, rf_val in rf.items()])
    query = f"CREATE KEYSPACE {ks_name} WITH REPLICATION={{'class':'NetworkTopologyStrategy', {options}}}"
    await create_ks_and_assert_warning(cql, query, ks_name, ["warn", "min", "replication", "factor", "3", "dc1", "2"])


@pytest.mark.asyncio
async def test_all_rf_limits(manager: ManagerClient):
    """
    There are 4 limits for RF: soft/hard min and soft/hard max limits. Breaking soft limits issues a warning,
    breaking the hard limits prevents the query from being executed.
    """
    MIN_FAIL_THRESHOLD = 2
    MIN_WARN_THRESHOLD = 3
    MAX_WARN_THRESHOLD = 4
    MAX_FAIL_THRESHOLD = 5

    # FIXME: This test verifies that guardrails work. However, if we set `rf_rack_valid_keyspaces` to true,
    # we'll get a different error, so let's disable it for now. For more context, see issues:
    #   scylladb/scylladb#23071 and scylladb/scylla-dtest#5633.
    cfg = {
        "rf_rack_valid_keyspaces": False,
        "minimum_replication_factor_fail_threshold": MIN_FAIL_THRESHOLD,
        "minimum_replication_factor_warn_threshold": MIN_WARN_THRESHOLD,
        "maximum_replication_factor_warn_threshold": MAX_WARN_THRESHOLD,
        "maximum_replication_factor_fail_threshold": MAX_FAIL_THRESHOLD,
    }

    dc = "dc1"
    await manager.server_add(config=cfg, property_file={"dc": dc, "rack": "r1"})
    cql = manager.get_cql()

    for rf in range(MIN_FAIL_THRESHOLD - 1, MAX_FAIL_THRESHOLD + 1):
        ks_name = unique_name()
        query = f"CREATE KEYSPACE {ks_name} WITH REPLICATION = {{'class': 'NetworkTopologyStrategy', '{dc}': {rf}}}"
        if rf < MIN_FAIL_THRESHOLD or rf > MAX_FAIL_THRESHOLD:
            await assert_creating_ks_fails(cql, query, ks_name)
        elif rf < MIN_WARN_THRESHOLD:
            await create_ks_and_assert_warning(cql, query, ks_name, ["warn", "min", "replication", "factor", str(MIN_WARN_THRESHOLD), dc, str(rf)])
        elif rf > MAX_WARN_THRESHOLD:
            await create_ks_and_assert_warning(cql, query, ks_name, ["warn", "max", "replication", "factor", str(MAX_WARN_THRESHOLD), dc, str(rf)])
        else:
            await create_ks_and_assert_warning(cql, query, ks_name, [])


@pytest.mark.asyncio
async def test_invalid_write_cl_guardrail_config(manager: ManagerClient):
    """A node configured with invalid values for write_consistency_levels_warned
    and write_consistency_levels_disallowed should still start and respond to
    CQL queries. The invalid values cause yaml-cpp conversion errors logged
    during startup."""
    cfg = {
        "write_consistency_levels_warned": ["INVALID_CL", "BOGUS"],
        "write_consistency_levels_disallowed": ["NOT_A_CL"],
    }
    manager.ignore_log_patterns.append("bad conversion : write_consistency_levels_warned")
    manager.ignore_log_patterns.append("bad conversion : write_consistency_levels_disallowed")

    server = await manager.server_add(config=cfg)
    log = await manager.server_open_log(server.server_id)

    assert len(await log.grep("bad conversion : write_consistency_levels_warned")) > 0
    assert len(await log.grep("bad conversion : write_consistency_levels_disallowed")) > 0

    cql = manager.get_cql()
    rows = await cql.run_async("SELECT * FROM system.local")
    assert len(rows) == 1

@pytest.mark.asyncio
async def test_write_cl_default(manager: ManagerClient):
    """Test checks that the current implementation doesn't cause
    any warning nor failure for the default configuration."""

    servers = await manager.servers_add(3, property_file=[
        {"dc": "dc1", "rack": "r1"},
        {"dc": "dc1", "rack": "r2"},
        {"dc": "dc1", "rack": "r3"},
    ])

    cql = manager.get_cql()
    await wait_for_cql_and_get_hosts(cql, servers, time.time() + 60)

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}") as ks:
        await cql.run_async(f"CREATE TABLE {ks}.t (pk int PRIMARY KEY, v int)")

        all_cls = [ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_ONE,
                ConsistencyLevel.TWO, ConsistencyLevel.THREE, ConsistencyLevel.QUORUM,
                ConsistencyLevel.ALL, ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.EACH_QUORUM]

        for cl in all_cls:
            stmt = SimpleStatement(f"INSERT INTO {ks}.t (pk, v) VALUES (1, 1)", consistency_level=cl)
            ret = cql.execute_async(stmt)
            await _wrap_future(ret)
            assert not ret.warnings, f"Expected no warning for {ConsistencyLevel.value_to_name[cl]}, got {ret.warnings}"
