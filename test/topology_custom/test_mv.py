#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import pytest
from cassandra.protocol import InvalidRequest
from test.topology.util import new_materialized_view, new_test_keyspace, new_test_table
from test.pylib.manager_client import ManagerClient

@pytest.mark.asyncio
async def test_create_mv_with_racks(manager: ManagerClient):
    """
    This test verifies that creating a materialized view in keyspaces with specified racks.
    When tablets are enabled, we only allow for creating a materialized view if RF == #racks or RF == 1.
    When tablets are disabled, there's no restriction.

    For more context, see: scylladb/scylladb#23030.
    """

    # We need to have an isolated environment for the test to be reliable.
    assert len(await manager.running_servers()) == 0

    cmd = ["--experimental-features", "views-with-tablets"]

    s1 = await manager.server_add(cmdline=cmd, property_file={"dc": "dc1", "rack": "r1"})
    _ = await manager.server_add(cmdline=cmd, property_file={"dc": "dc1", "rack": "r2"})
    _ = await manager.server_add(cmdline=cmd, property_file={"dc": "dc2", "rack": "r1"})

    async def try_pass(replication_class: str, replication_details: str, tablets: str):
        async with new_test_keyspace(manager, f"WITH REPLICATION = {{'class': '{replication_class}', {replication_details}}} AND tablets = {{'enabled': {tablets}}}") as ks:
            async with new_test_table(manager, ks, "p int PRIMARY KEY, v int") as table:
                async with new_materialized_view(manager, table, "*", "p, v", "p IS NOT NULL AND v IS NOT NULL"):
                    pass

    async def try_fail(replication_class: str, replication_details: str, tablets: str, regex: str):
        async with new_test_keyspace(manager, f"WITH REPLICATION = {{'class': '{replication_class}', {replication_details}}} AND tablets = {{'enabled': {tablets}}}") as ks:
            async with new_test_table(manager, ks, "p int PRIMARY KEY, v int") as table:
                with pytest.raises(InvalidRequest, match=regex.format(ks=ks)):
                    async with new_materialized_view(manager, table, "*", "p, v", "p IS NOT NULL AND v IS NOT NULL"):
                        pass

    # Below, we test each case twice: with tablets on and off.
    # Note that we only use NetworkTopologyStrategy. That's because of this fragment of our documentation:
    #
    # "When creating a new keyspace with tablets enabled (the default), you can still disable them on a per-keyspace basis.
    #  The recommended NetworkTopologyStrategy for keyspaces remains REQUIRED when using tablets."
    #
    # --- "Data Distribution with Tablets"

    # Part 1: Test the current state of the cluster. Note that every rack currently consists of one node.

    # RF = #racks for every DC.
    await try_pass("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 1", "true")
    await try_pass("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 1", "false")

    # RF != #racks for dc1, but we accept RF = 1.
    await try_pass("NetworkTopologyStrategy", "'dc1': 1, 'dc2': 1", "true")
    await try_pass("NetworkTopologyStrategy", "'dc1': 1, 'dc2': 1", "false")

    # RF != #racks for dc2 and edge case for RF = 0.
    await try_fail("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 0", "true",
                   f"Mismatched replication factor and rack count \(in data center 'dc2'\) for keyspace '{{ks}}': 0 vs. 1")
    await try_pass("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 0", "false")
    # Ditto, just for dc1.
    await try_fail("NetworkTopologyStrategy", "'dc1': 0, 'dc2': 1", "true",
                   f"Mismatched replication factor and rack count \(in data center 'dc1'\) for keyspace '{{ks}}': 0 vs. 2")
    await try_pass("NetworkTopologyStrategy", "'dc1': 0, 'dc2': 1", "false")

    # Note: in case these checks start failing or causing issues, feel free to get rid of them.
    #       We don't care about it (just like we don't care about EverywhereStrategy and LocalStrategy),
    #       so these are more of sanity checks than something we really want to test.
    for rf in [1, 2, 3]:
        await try_pass("SimpleStrategy", f"'replication_factor': {rf}", "false")

    # Part 2: We extend the cluster by one node in dc1/r2. We no longer have a bijection: nodes -> racks.

    _ = await manager.server_add(cmdline=cmd, property_file={"dc": "dc1", "rack": "r2"})

    # RF = #racks for every DC.
    await try_pass("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 1", "true")
    await try_pass("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 1", "false")

    # RF < #racks for dc1, but we accept RF = 1.
    await try_pass("NetworkTopologyStrategy", "'dc1': 1, 'dc2': 1", "true")
    await try_pass("NetworkTopologyStrategy", "'dc1': 1, 'dc2': 1", "false")

    # RF > #racks for dc1.
    await try_fail("NetworkTopologyStrategy", "'dc1': 3, 'dc2': 1", "true",
                   f"Mismatched replication factor and rack count \(in data center 'dc1'\) for keyspace '{{ks}}': 3 vs. 2")
    await try_pass("NetworkTopologyStrategy", "'dc1': 3, 'dc2': 1", "false")

    # RF != #racks for dc2 and edge case for RF = 0.
    await try_fail("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 0", "true",
                   f"Mismatched replication factor and rack count \(in data center 'dc2'\) for keyspace '{{ks}}': 0 vs. 1")
    await try_pass("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 0", "false")
    # Ditto, just for dc1.
    await try_fail("NetworkTopologyStrategy", "'dc1': 0, 'dc2': 1", "true",
                   f"Mismatched replication factor and rack count \(in data center 'dc1'\) for keyspace '{{ks}}': 0 vs. 2")
    await try_pass("NetworkTopologyStrategy", "'dc1': 0, 'dc2': 1", "false")

    # Note: ditto, same as in part 1.
    for rf in [1, 2, 3, 4]:
        await try_pass("SimpleStrategy", f"'replication_factor': {rf}", "false")

    # Part 3: We get rid of dc1/r1. This way, we have two nodes in dc1, but only one rack.

    await manager.decommission_node(s1.server_id)

    # RF = #racks for every DC.
    await try_pass("NetworkTopologyStrategy", "'dc1': 1, 'dc2': 1", "true")
    await try_pass("NetworkTopologyStrategy", "'dc1': 1, 'dc2': 1", "false")

    # RF > #racks for dc1.
    await try_fail("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 1", "true",
                   f"Mismatched replication factor and rack count \(in data center 'dc1'\) for keyspace '{{ks}}': 2 vs. 1")
    await try_pass("NetworkTopologyStrategy", "'dc1': 2, 'dc2': 1", "false")

    # RF != #racks for dc2 and edge case for RF = 0.
    await try_fail("NetworkTopologyStrategy", "'dc1': 1, 'dc2': 0", "true",
                   f"Mismatched replication factor and rack count \(in data center 'dc2'\) for keyspace '{{ks}}': 0 vs. 1")
    await try_pass("NetworkTopologyStrategy", "'dc1': 1, 'dc2': 0", "false")
    # Ditto, just for dc1.
    await try_fail("NetworkTopologyStrategy", "'dc1': 0, 'dc2': 1", "true",
                   f"Mismatched replication factor and rack count \(in data center 'dc1'\) for keyspace '{{ks}}': 0 vs. 1")
    await try_pass("NetworkTopologyStrategy", "'dc1': 0, 'dc2': 1", "false")

    # Note: ditto, same as in part 1.
    for rf in [1, 2, 3]:
        await try_pass("SimpleStrategy", f"'replication_factor': {rf}", "false")
