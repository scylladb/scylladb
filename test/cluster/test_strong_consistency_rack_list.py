#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest

from cassandra.protocol import ConfigurationException

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.cluster.util import create_new_test_keyspace, disable_schema_agreement_wait, parse_replication_options


async def test_strongly_consistent_keyspace_uses_rack_lists(manager: ScyllaClusterManager):
    """
    Strongly consistent keyspaces always use rack lists, even when neither rf_rack_valid_keyspaces
    nor enforce_rack_list is enabled.
    """
    config = {'rf_rack_valid_keyspaces': False, 'enforce_rack_list': False}
    servers = await manager.servers_add(4, config=config, property_file=[
        {'dc': 'dc1', 'rack': 'r1'}, {'dc': 'dc1', 'rack': 'r2'}, {'dc': 'dc1', 'rack': 'r2'},
        {'dc': 'dc2', 'rack': 'r1'}])
    cql, hosts = await manager.get_ready_cql(servers)
    # All statements go to the same node, so there is no need to wait for schema agreement. A node
    # responds to a schema change after applying it locally, but the other nodes may apply it later.
    host = hosts[0]

    async def create_ks(replication, consistency):
        return await create_new_test_keyspace(cql, f"WITH replication = {{'class': 'NetworkTopologyStrategy', {replication}}}"
                                                   f" AND tablets = {{'initial': 1}} AND consistency = '{consistency}'",
                                              host=host)

    async def rack_lists(ks):
        """Returns the rack list of each DC in which the keyspace uses one."""
        rows = await cql.run_async(f"SELECT replication_v2 FROM system_schema.keyspaces WHERE keyspace_name = '{ks}'",
                                   host=host)
        options = parse_replication_options(rows[0].replication_v2 or {})
        return {dc: sorted(racks) for dc, racks in options.items() if isinstance(racks, list)}

    # Disable the schema agreement wait to speed up the test.
    with disable_schema_agreement_wait(cql):
        # Sanity check: with this configuration, an eventually consistent keyspace keeps its numeric RF
        # and can be RF-rack-invalid (RF=3 with 2 racks).
        ec_ks = await create_ks("'dc1': 3", 'eventual')
        assert await rack_lists(ec_ks) == {}

        # A rack list cannot be built for the same replication factor.
        with pytest.raises(ConfigurationException, match="Replication factor 3 exceeds the number of racks"):
            await create_ks("'dc1': 3", 'global')

        # A numeric RF is expanded to a rack list.
        sc_ks = await create_ks("'dc1': 2", 'global')
        assert await rack_lists(sc_ks) == {'dc1': ['r1', 'r2']}
        await cql.run_async(f"CREATE TABLE {sc_ks}.t (pk int PRIMARY KEY, v int)", host=host)
        await cql.run_async(f"INSERT INTO {sc_ks}.t (pk, v) VALUES (1, 1)", host=host)

        with pytest.raises(ConfigurationException, match="when the old value was a rack list"):
            await cql.run_async(f"ALTER KEYSPACE {sc_ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': 1}}",
                                host=host)

        # An unchanged numeric RF keeps the rack list, and a numeric RF in a new DC is expanded to one.
        await cql.run_async(f"ALTER KEYSPACE {sc_ks} WITH replication = {{'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1}}",
                            host=host)
        assert await rack_lists(sc_ks) == {'dc1': ['r1', 'r2'], 'dc2': ['r1']}

    # Removing the only node of a rack listed by the strongly consistent keyspace is rejected.
    await manager.decommission_node(servers[0].server_id,
                                    expected_error="would make some existing keyspace RF-rack-invalid")

    # The startup checks pass. This node executed all statements, so it knows the latest schema.
    await manager.server_restart(servers[0].server_id)

    # A new rack does not affect a keyspace using rack lists.
    await manager.server_add(config=config, property_file={'dc': 'dc1', 'rack': 'r3'})
