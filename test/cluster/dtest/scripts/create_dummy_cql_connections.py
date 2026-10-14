#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import argparse

from cassandra.cluster import Cluster, NoHostAvailable

parser = argparse.ArgumentParser(description="Creates multiple dummy connections to Scylla.")
parser.add_argument("address", type=str, help="scylla ip address")
parser.add_argument("connections", type=int, help="numbers of connections to create")

if __name__ == "__main__":
    args = parser.parse_args()
    address, connections = (args.address, args.connections)
    cluster = Cluster([address], connect_timeout=120)
    sessions = []
    connections_created = 0
    for _ in range(connections):
        try:
            sessions.append(cluster.connect())
        except NoHostAvailable:
            break
        connections_created += 1

    input(f"{connections_created} cql connections created. Press any key to close them and quit...\n")
    [session.shutdown() for session in sessions]
