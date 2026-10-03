#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import os


def update_properties(nodes: list, properties: dict | None):
    for node in nodes:
        properties.setdefault("dc", node.data_center)
        with open(os.path.join(node.get_conf_dir(), "cassandra-rackdc.properties"), "w") as snitch_file:
            for key, value in properties.items():
                snitch_file.write(f"{key}={value}" + os.linesep)
        # The cluster manager rewrites cassandra-rackdc.properties from its own
        # copy whenever it starts the node, so update that copy as well.
        node.cluster.manager.cluster.servers[node.server_id].property_file = dict(properties)
        node.data_center = properties["dc"]
        node.rack = properties.get("rack", node.rack)
