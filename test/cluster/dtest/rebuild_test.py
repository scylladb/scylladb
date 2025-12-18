#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging
from concurrent.futures.thread import ThreadPoolExecutor

import pytest

from dtest_class import Tester


logger = logging.getLogger(__name__)


class TestRebuildStreamingAbortRepro(Tester):
    @pytest.mark.cluster_options(allowed_repair_based_node_ops="")
    def test_rebuild_stream_abort_repro(self):
        """2-DC cluster parallel non-RBNO rebuild failure when expanding RF in DC2.

        Steps to reproduce:
            1. Provision a cluster with 2 datacenters and at least 2 nodes in the second datacenter.
            2. Let’s assume datacenter names are "dc1" and "dc2".
            3. Create a keyspace ("keyspace1") with RF=0 in dc2.
            4. Populate some data into dc1.
            5. Change keyspace1 replication in dc2 to 2.
            6. On 2 nodes in dc2 run the following command in parallel: nodetool rebuild --source-dc dc1
        """
        logger.debug("Creating a 2-DC cluster with 1 and 2 nodes respectively.")
        self.cluster.populate({"dc1": {"rack1": 1}, "dc2": {"rack1": 1, "rack2": 1}}).start(wait_other_notice=True)
        node1, node2, node3 = self.cluster.nodelist()

        logger.debug("Populating data into dc1.")
        node1.stress(["write", "n=100000", "-schema", "replication(strategy=NetworkTopologyStrategy,dc1=1,dc2=0)"])

        logger.debug("Changing keyspace1 replication in dc2 to 2.")
        with self.patient_cql_connection(node1) as session:
            session.execute("""\
                ALTER KEYSPACE keyspace1 
                WITH replication = { 
                    'class': 'NetworkTopologyStrategy', 
                    'dc1': 1, 
                    'dc2': 2 
                }
            """)

        logger.debug("Start rebuild on 2 nodes in dc2 in parallel.")
        with ThreadPoolExecutor(max_workers=2) as executor:
            node2_rebuild = executor.submit(node2.nodetool, "rebuild --source-dc dc1")
            node2.watch_log_for("Executing streaming plan for Rebuild-keyspace1-index-0")
            node3_rebuild = executor.submit(node3.nodetool, "rebuild --source-dc dc1")

        logger.debug("Waiting for rebuild operations to complete.")
        node2_rebuild.result()
        node3_rebuild.result()
