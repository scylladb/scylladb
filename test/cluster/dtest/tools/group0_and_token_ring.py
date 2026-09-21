import logging

from ccmlib.scylla_node import ScyllaNode

from dtest_class import retry_till_success
from tools.cluster import get_group0_members, get_token_ring_members

logger = logging.getLogger(__name__)


def get_diff_group0_and_token_ring_members(node: ScyllaNode):
    logger.debug("Get group0 members")
    group0_members = get_group0_members(node)

    logger.debug(f"Group0 members: {group0_members}")

    logger.debug("Get token ring members")
    token_ring_members = get_token_ring_members(node)
    logger.debug(f"Token ring members {token_ring_members}")

    token_ring_members_ids = [member["host_id"] for member in token_ring_members]
    group0_members_ids = [member["host_id"] for member in group0_members]
    diff_ids = list(set(group0_members_ids) - set(token_ring_members_ids))
    logger.debug(f"Token ring and group0 member's ids {diff_ids}")

    return diff_ids


def verify_group0_and_token_ring_members(node: ScyllaNode, expected_num_of_members: int):
    """verify consistency of group0 and token ring members

    Get token ring members and group0 members.
    Validate that number of member is equal. Validate that host_ids in group0
    and token_ring are the same. Validate that all hosts in group0 are voters

    """
    logger.debug("Get group0 members")
    group0_members = get_group0_members(node)

    logger.debug(f"Group0 members: {group0_members}")
    assert expected_num_of_members == len(group0_members), f"Number of group0 members is not equal {expected_num_of_members}, got {len(group0_members)}"

    logger.debug("Get token ring members")
    token_ring_members = get_token_ring_members(node)
    logger.debug(f"Token ring members {token_ring_members}")
    assert expected_num_of_members == len(token_ring_members), f"Number of token ring members is not equal {expected_num_of_members}"

    logger.debug("Validate that at least one group0 member is a voter")
    assert any(member["is_voter"] for member in group0_members), "No voters in group0"

    assert not get_diff_group0_and_token_ring_members(node)


def wait_for_token_ring_and_group0_consistency(node: ScyllaNode, expected_num_of_members: int, timeout: int = 120):
    retry_till_success(verify_group0_and_token_ring_members, node, expected_num_of_members, timeout=timeout)


def find_and_clean_garbage_from_group0(verification_node, garbage_host_id, is_removed_from_token_ring, expected_num_of_members=2):
    """Find difference and clean garbage from group0"""
    garbage_host_ids = get_diff_group0_and_token_ring_members(verification_node)
    failed_members = [member for member in get_group0_members(verification_node) if member["host_id"] == garbage_host_id]
    if failed_members:
        for member in failed_members:
            assert not member["is_voter"], f"Node stay as voter {failed_members}"

            if not is_removed_from_token_ring and not garbage_host_ids:
                garbage_host_ids.append(member["host_id"])

    logger.debug(f"Garbage host id in raft group0: {garbage_host_ids}")
    if not garbage_host_ids:
        logger.debug("Node was removed from token ring and group0. Check no garbage left")
        verify_group0_and_token_ring_members(verification_node, expected_num_of_members=expected_num_of_members)
    else:
        logger.debug("Node left in group0")
        assert garbage_host_id in garbage_host_ids, f"Node3 host id {garbage_host_id} is not in group0 {garbage_host_ids}"
        logger.debug("Node3 is not a voter, it could be removed from cluster with removenode")
        retry_till_success(verification_node.nodetool, f"removenode {garbage_host_id}", timeout=120)
