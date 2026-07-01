#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import itertools


def generate_cluster_topology(dc_num: int = 1,
                              rack_num: int = 1,
                              nodes_per_rack: int = 1,
                              dc_name_prefix: str = "datacenter",
                              rack_name_prefix: str = "rack") -> dict[str, dict[str, int]]:
    """Generate a dictionary representing the cluster topology based on the number of datacenters (DCs), racks, and nodes per rack.

    Args:
        dc_num (int): The number of datacenters.
        rack_num (int): The number of racks per datacenter.
        nodes_per_rack (int): The number of nodes per rack.
        dc_name_prefix (str): The prefix for naming the datacenters.
        rack_name_prefix (str): The prefix for naming the racks.

    Returns:
        dict: A dictionary where keys are datacenter names and values are dictionaries representing the rack topology within each datacenter.
    """
    return {f"{dc_name_prefix}{i + 1}": {f"{rack_name_prefix}{j + 1}": nodes_per_rack for j in range(rack_num)} for i in range(dc_num)}


def generate_rack_topology_based_rf(nodes: int = 1, rf: int = 1, rack_name_prefix: str = "rack"):
    """Generate a dictionary representing the rack topology based on the number of nodes and replication factor (RF).

    Args:
        nodes (int): The total number of nodes in a single datacenter.
        rf (int): The replication factor for this datacenter.
        rack_name_prefix (str): The prefix for naming the racks.

    Returns:
        dict: A dictionary where keys are rack names and values are the number of nodes in each rack.
    """
    racks = {}
    racks_num = rf if rf > 0 else nodes
    nodes_per_rack = nodes // racks_num
    extra_nodes = nodes % racks_num
    for i in range(racks_num):
        racks[f"{rack_name_prefix}{i + 1}"] = nodes_per_rack + (1 if i < extra_nodes else 0)
    return racks


def generate_cluster_topology_based_rf(dc_num: int = 1, nodes: int | list = 1, rf: int | list | dict = 1, dc_name_prefix: str = "datacenter", rack_name_prefix: str = "rack") -> dict[str, dict[str, int]]:
    """Generate a dictionary representing the cluster topology based on the number of datacenters (DCs), nodes, and replication factor (RF).

    Args:
        dc_num (int): The number of datacenters.
        nodes (int | list): The total number of nodes or a list specifying the number of nodes per datacenter.
        rf (int | list | dict): The replication factor or a list specifying the replication factor per datacenter.
        dc_name_prefix (str): The prefix for naming the datacenters.
        rack_name_prefix (str): The prefix for naming the racks.

    Returns:
        dict: A dictionary where keys are datacenter names and values are dictionaries representing the rack topology within each datacenter.
    """
    rfs = rf if isinstance(rf, list) else [rf] * dc_num
    nodes = nodes if isinstance(nodes, list) else [nodes] * dc_num
    dcs = {}
    for j, (cur_nodes, curr_rf) in enumerate(itertools.zip_longest(nodes, rfs)):
        if isinstance(rf, dict):
            assert all(dc_name_prefix in k for k in rf.keys()), f"dc names in rf dict should match `dc_name_prefix`={dc_name_prefix}"
            curr_rf = rf.get(f"{dc_name_prefix}{j + 1}", curr_rf)  # noqa: PLW2901
        racks = generate_rack_topology_based_rf(cur_nodes, curr_rf, rack_name_prefix)
        dcs[f"{dc_name_prefix}{j + 1}"] = racks
    return dcs
