#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

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
