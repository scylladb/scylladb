#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test removenode with node with node no longer member
"""
import logging
from test.pylib.manager_client import ManagerClient
from test.pylib.rest_client import inject_error_one_shot
from test.pylib.util import wait_for_cql_and_get_hosts
from test.topology.util import get_token_ring_host_ids, get_current_group0_config, \
                               check_token_ring_and_group0_consistency, wait_for_token_ring_and_group0_consistency
import time
import pytest


logger = logging.getLogger(__name__)


async def test_remove_garbage_group0_members(manager: ManagerClient):
    """
    Verify that failing to leave group 0 or remove a node from group 0 in removenode/decommission
    can be handled by executing removenode (which should clear the 'garbage' group 0 member),
    even though the node is no longer a token ring member. Does not apply to Raft-topology mode.
    """
    # 4 servers, one dead
    cfg = {'enable_user_defined_functions': False,
           'force_gossip_topology_changes': True}
    servers = [await manager.server_add(config=cfg) for _ in range(4)]

    # Make sure that the driver has connected to all nodes, and they see each other as NORMAL
    # (otherwise the driver may remove connection to some host, even after it manages to connect to it,
    # because the node that it has control connection to considers that host as not NORMAL yet).
    # This ensures that after we stop/remove some nodes in the test, the driver will still
    # be able to connect to the remaining nodes. See scylladb/scylladb#16373
    await wait_for_token_ring_and_group0_consistency(manager, time.time() + 60)
    await wait_for_cql_and_get_hosts(manager.get_cql(), servers, time.time() + 60)

    removed_host_id = await manager.get_host_id(servers[0].server_id)
    await manager.server_stop_gracefully(servers[0].server_id)

    logging.info(f'removenode {servers[0]} using {servers[1]}')
    # removenode will fail after removing the server from the token ring,
    # but before removing it from group 0
    await inject_error_one_shot(manager.api, servers[1].ip_addr,
                                'removenode_fail_before_remove_from_group0')
    try:
        await manager.remove_node(servers[1].server_id, servers[0].server_id)
    except Exception:
        # Note: the exception returned here is only '500 internal server error',
        # need to look in test.py log for the actual message coming from Scylla.
        logging.info(f'expected exception during injection')

    # Query the storage_service/host_id endpoint to calculate a list of known token ring members' Host IDs
    # (internally, this endpoint uses token_metadata)
    token_ring_ids = await get_token_ring_host_ids(manager, servers[1])
    logging.info(f'token ring members: {token_ring_ids}')

    group0_members = await get_current_group0_config(manager, servers[1])
    logging.info(f'group 0 members: {group0_members}')
    group0_ids = {m[0] for m in group0_members}

    # Token ring members should currently be a subset of group 0 members
    assert token_ring_ids <= group0_ids

    garbage_members = group0_ids - token_ring_ids
    logging.info(f'garbage members: {garbage_members}')
    assert len(garbage_members) == 1
    garbage_member = next(iter(garbage_members))

    # The garbage member is the one that we failed to remove
    assert garbage_member == removed_host_id

    # Verify that at least it's a non-voter.
    assert garbage_member in {m[0] for m in group0_members if not m[1]}

    logging.info(f'removenode {servers[0]} using {servers[1]} again')
    # Retry removenode. It should skip the token ring removal step and remove the server from group 0.
    await manager.remove_node(servers[1].server_id, servers[0].server_id)

    group0_members = await get_current_group0_config(manager, servers[1])
    logging.info(f'group 0 members: {group0_members}')
    group0_ids = {m[0] for m in group0_members}

    # Token ring members and group 0 members should now be the same.
    assert token_ring_ids == group0_ids

    # Verify that availability is not reduced.
    # Stop one of the 3 remaining servers and try to remove it. It should succeed with only 2 servers.

    logging.info(f'stop {servers[1]}')
    await manager.server_stop_gracefully(servers[1].server_id)

    logging.info(f'removenode {servers[1]} using {servers[2]}')
    await manager.remove_node(servers[2].server_id, servers[1].server_id)

    # Perform a similar scenario with decommission. One of the node fails to decommission fully,
    # but it manages to leave the token ring. We observe the leftovers using the same APIs as above
    # and remove the leftovers.
    # We can do this with only 2 nodes because during decommission we become a non-voter before
    # leaving the token ring, thus the remaining single node will become a voting majority
    # and will be able to perform removenode alone.

    decommissioned_host_id = await manager.get_host_id(servers[2].server_id)
    await manager.api.enable_injection(
        servers[2].ip_addr, 'decommission_fail_before_leave_group0', one_shot=True)
    logging.info(f'decommission {servers[2]}')
    try:
        await manager.decommission_node(servers[2].server_id)
    except Exception:
        logging.info(f'expected exception during injection')
    logging.info(f'stop {servers[2]}')
    await manager.server_stop_gracefully(servers[2].server_id)

    token_ring_ids = await get_token_ring_host_ids(manager, servers[3])
    logging.info(f'token ring members: {token_ring_ids}')

    group0_members = await get_current_group0_config(manager, servers[3])
    logging.info(f'group 0 members: {group0_members}')
    group0_ids = {m[0] for m in group0_members}

    assert token_ring_ids <= group0_ids

    garbage_members = group0_ids - token_ring_ids
    logging.info(f'garbage members: {garbage_members}')
    assert len(garbage_members) == 1
    garbage_member = next(iter(garbage_members))

    assert garbage_member == decommissioned_host_id
    assert garbage_member in {m[0] for m in group0_members if not m[1]}

    logging.info(f'removenode {servers[2]} using {servers[3]}')
    await manager.remove_node(servers[3].server_id, servers[2].server_id)

    await check_token_ring_and_group0_consistency(manager)

