#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from test.cluster.auth_cluster import extra_scylla_config_options as auth_config
from test.pylib.manager_client import ManagerClient


async def test_tolerating_cycles_in_auth(manager: ManagerClient):
    """
    Verify that the service levels logic in group0 gracefully handles cycles
    in the roles graph (i.e. an edge is created by GRANT role1 TO role2).
    Although auth has an explicit check which should prevent creation of such
    cycles, it might (theoretically, not observed yet in practice) happen that
    some inconsistent data is migrated from the system_auth distributed keyspace
    to group0 tables. For each role, the final service level parameters depend
    on the service levels attached to all roles reachable in the role grants graph.
    This logic needs to be resilient to cycles.
    """
    server = await manager.server_add(config=auth_config)
    cql = manager.get_cql()

    await cql.run_async("CREATE ROLE a")
    await cql.run_async("CREATE ROLE b")

    # Create an invalid state by simulating two cyclic GRANTs.
    await cql.run_async("INSERT INTO system.role_members (role, member) VALUES ('a', 'b')")
    await cql.run_async("INSERT INTO system.role_members (role, member) VALUES ('b', 'a')")
    await cql.run_async("UPDATE system.roles SET member_of = {'b'} where role = 'a'")
    await cql.run_async("UPDATE system.roles SET member_of = {'a'} where role = 'b'")

    # Now that a cycle has been created in group0, the next operation that modifies
    # data in group0 will cause it to be reloaded. Create some service levels
    # to trigger that.
    await cql.run_async("CREATE SERVICE LEVEL sl_a WITH SHARES = 100")
    await cql.run_async("ATTACH SERVICE LEVEL sl_a TO a")
    await cql.run_async("CREATE SERVICE LEVEL sl_b WITH SHARES = 100")
    await cql.run_async("ATTACH SERVICE LEVEL sl_b TO b")

    # There should be a warning about the cycle
    log = await manager.server_open_log(server.server_id)
    await log.wait_for("Cycle detected in the system.role_members table: (a -> b -> a|b -> a -> b)")


async def test_invalid_graph_with_edges_to_non_existing_members(manager: ManagerClient):
    """
    Verify that the service levels logic gracefully handles invalid graphs of roles
    with roles being assigned to roles that no longer exist (e.g. GRANT role1
    TO role2, and role2 no longer exists). Such graphs were observed in the wild,
    and can occur if inconsistent data is migrated from the auth tables
    in system_auth distributed keyspace to group0 tables.
    """
    await manager.server_add(config=auth_config)
    cql = manager.get_cql()

    await cql.run_async("CREATE ROLE a")

    # Create a connection to a role that does not exist.
    await cql.run_async("INSERT INTO system.role_members (role, member) VALUES ('a', 'b')")
    await cql.run_async("UPDATE system.roles SET member_of = {'b'} where role = 'a'")

    # Now that group0 has invalid roles graph, the next operation that modifies
    # data in group0 will cause it to be reloaded. Create some service levels
    # to trigger that.
    await cql.run_async("CREATE SERVICE LEVEL sl_a WITH SHARES = 100")
    await cql.run_async("ATTACH SERVICE LEVEL sl_a TO a")
