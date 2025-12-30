#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging
import random
import string
from concurrent.futures.thread import ThreadPoolExecutor

import pytest

from dtest_class import Tester


logger = logging.getLogger(__name__)


class TestRebuildStreamingAbortRepro(Tester):
    @pytest.mark.cluster_options(allowed_repair_based_node_ops="")
    def test_rebuild_stream_abort_repro(self):
        """2-DC cluster parallel non-RBNO rebuild failure when expanding RF in DC2 (#27804.)

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

        with self.patient_cql_connection(node1) as session:
            logger.debug("Creating the keyspace and the table.")
            session.execute("""\
                CREATE KEYSPACE keyspace1
                WITH replication = {
                    'class': 'NetworkTopologyStrategy',
                    'dc1': 1,
                    'dc2': 0
                }
            """)
            session.execute(
                "CREATE TABLE keyspace1.standard1 (key blob PRIMARY KEY, C0 blob, C1 blob, C2 blob, C3 blob, C4 blob)"
            )

            logger.debug("Populating data into dc1.")
            insert_query = session.prepare(
                "INSERT INTO keyspace1.standard1 (key, C0, C1, C2, C3, C4) VALUES (?, ?, ?, ?, ?, ?)"
            )
            key_chars = string.ascii_uppercase + string.digits
            for _ in range(100_000):
                session.execute(insert_query, [
                    "".join(random.choices(key_chars, k=10)).encode(),
                    *(random.randbytes(34) for _ in range(5)),
                ])

            logger.debug("Changing keyspace1 replication in dc2 to 2.")
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
