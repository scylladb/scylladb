import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pytest
from cassandra import ConsistencyLevel, Unavailable
from cassandra.query import SimpleStatement
from ccmlib.node import ToolError
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import Tester, create_cf, create_ks
from tools.assertions import assert_one
from tools.cluster_topology import generate_cluster_topology
from tools.data import create_c1c2_table, delete_c1c2, insert_c1c2
from tools.files import get_list_of_sstables
from tools.snapshots import make_snapshot, restore_snapshot_with_refresh

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestCleanup(Tester):
    def prepare(self, nodes, num_keys, timeout=None, consistency=ConsistencyLevel.ALL, amount_of_tables=1, rf=None):  # noqa: PLR0913
        cluster = self.cluster
        if timeout:
            values = {
                "range_request_timeout_in_ms": timeout * 1000,
            }
            logger.info("Setting cluster configuration options: %s", values)
            cluster.set_configuration_options(values=values)
        cluster.populate(nodes).start()
        node1 = self.cluster.nodelist()[0]
        with self.patient_cql_cluster_session(node1) as session:
            if not rf:
                rf = nodes
            create_ks(session=session, name="ks", rf=rf)
            for i in range(amount_of_tables):
                create_cf(session=session, name=f"cf{i}", columns={"c1": "text", "c2": "text"})
            if num_keys:
                logger.info("Inserting %s keys", num_keys)
                for i in range(amount_of_tables):
                    insert_c1c2(session=session, keys=range(num_keys), consistency=consistency, cf=f"cf{i}")

    @pytest.mark.next_gating
    @pytest.mark.single_node
    def test_cleanup(self):
        num_keys = 100000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 10000
        timeout = self.cql_timeout(300)
        self.prepare(1, num_keys, timeout)

        logger.info("Restarting node")
        node1 = self.cluster.nodelist()[0]
        node1.stop()
        node1.start(wait_for_binary_proto=True)

        logger.info("Running cleanup")
        node1.cleanup()

        logger.info("Verifying number of rows")
        session = self.patient_cql_connection(node1)
        rows = session.execute("select count(*) from ks.cf0;", timeout=timeout)

        assert rows.one()[0] == num_keys

    @pytest.mark.next_gating
    def test_cluster_cleanup(self):
        num_keys = 100000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 10000
        timeout = self.cql_timeout(300)
        cluster_topology = {"dc1": {"r1": 1, "r2": 1, "r3": 1}}
        self.prepare(cluster_topology, num_keys, timeout, rf=3)

        cluster = self.cluster
        node1 = cluster.nodelist()[0]

        logger.info("Adding a new node")
        node4 = cluster.new_node(4, data_center="dc1", rack="r1")
        node4.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("Running cleanup")
        cluster.cleanup()

        logger.info("Verifying number of rows")
        session = self.patient_cql_connection(node1)
        rows = session.execute("select count(*) from ks.cf0;", timeout=timeout)
        assert rows.one()[0] == num_keys

    @pytest.mark.required_features("!tablets")
    def test_cleanup_space_amplification(self):
        num_keys = 100000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 10000
        timeout = self.cql_timeout(300)
        self.prepare(1, num_keys, timeout)

        def _get_list_of_sstables(node):
            full_size = 0
            sstables = get_list_of_sstables(node, "ks", "cf0")
            ret = {}
            for file in sstables:
                try:
                    # measure blocks and not logical size since
                    # scylla extends the file size and then write in place
                    # to reduce metadata change overhead for each write
                    # and eventually it truncates the size to the real length
                    blocks = os.stat(file).st_blocks
                    size = blocks * 512
                    ret[file] = size
                    full_size += size
                except FileNotFoundError as ex:
                    logger.info("File %s was not found: %s", file, ex)
            return ret, full_size

        cluster = self.cluster
        node1 = cluster.nodelist()[0]

        logger.info("Adding a new node")
        node2 = cluster.new_node(2)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)
        cluster.flush()
        cluster.stop()
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
        sstables_before, size_before = _get_list_of_sstables(node1)

        def do_run_cleanup(node):
            node.cleanup()

        logger.info("Running cleanup")
        executor = ThreadPoolExecutor(max_workers=1)
        thread1 = executor.submit(do_run_cleanup, node1)

        while not thread1.done():
            sstables_during, size_during = _get_list_of_sstables(node1)
            assert size_during <= size_before * 2, f"Temporary space amplification must be less than 2x during cleanup, before=({sstables_before}, {size_before}) and during=({sstables_during}, {size_during})"
        thread1.result()
        sstables_after, size_after = _get_list_of_sstables(node1)
        assert size_before > size_after, f"Cleanup is supposed to decrease disk utilisation, before=({sstables_before}, {size_before}) and after=({sstables_after}, {size_after})"

    # Reproducer for https://github.com/scylladb/scylladb/issues/1239
    @pytest.mark.next_gating
    def test_cluster_cleanup_no_resurrection(self):
        """
        - Write data to 2-node cluster
        - Add node
        - Run cleanup
        - Delete data
        - Wait for tombstones to expire
        - Run major compaction (this will get rid of both data and tombstones)
        - Remove the added node
        - Verify there are no readable keys.
        - Any original data that wasn't cleaned up properly would get resurrected at this stage.
        """
        num_keys = 1000
        self.prepare(nodes={"dc1": {"r1": 1, "r2": 1}}, num_keys=num_keys, rf=2)
        cluster = self.cluster
        node1, _node2 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        gc_grace_seconds = 0
        session.execute(f"ALTER TABLE ks.cf0 WITH gc_grace_seconds={gc_grace_seconds}")

        logger.info("Adding a new node")
        new_node = cluster.new_node(3, data_center="dc1", rack="r1")
        new_node.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("Running cleanup")
        cluster.nodetool("cleanup ks")

        logger.info("Deleting data")
        delete_c1c2(session, n=num_keys, cf="cf0")

        logger.info("Verifying data")
        query = SimpleStatement("SELECT count(*) FROM ks.cf0", consistency_level=ConsistencyLevel.QUORUM)
        rows = session.execute(query)
        assert rows.one()[0] == 0

        logger.info(f"Sleeping until gc_grace_seconds={gc_grace_seconds} pass")
        time.sleep(gc_grace_seconds + 1)
        logger.info("Running compaction")
        cluster.compact()
        cluster.wait_for_compactions()

        new_node_hostid = new_node.hostid()
        logger.debug(f"Remove node {new_node.name} (host id {new_node_hostid})")
        new_node.stop(wait_other_notice=True)
        node1.removenode(new_node_hostid)

        node1.nodetool("snapshot ks -t after_removenode")

        logger.info("Reverifying data")
        rows = session.execute(query)
        assert rows.one()[0] == 0

    @pytest.mark.required_features("!tablets")
    def test_cluster_restore_no_resurrection(self):  # noqa: PLR0915
        """
        Reproducer for https://github.com/scylladb/scylladb/issues/11933:
        - Write data to 2-node cluster
        - Make a snapshot
        - Add node
        - Run cleanup
        - Delete data
        - Wait for tombstones to expire
        - Run major compaction (this will get rid of both data and tombstones)
        - Restore sstables from snapshot
          ^ Any stale data would get resurrected at this stage (it shouldn't)
        - Verify readable keys (*)

        (*) "Readable" must be the keys that were replicated on nodes 1 and 2 when
            node 3 was in the cluster. To check this redability the "unwanted" node
            is stopped by the time keys are checked.
        """
        num_keys = 1000
        self.prepare(nodes=2, num_keys=num_keys, rf=1)
        cluster = self.cluster
        node1, node2 = cluster.nodelist()
        session = self.patient_cql_connection(node1)
        gc_grace_seconds = 0
        session.execute(f"ALTER TABLE ks.cf0 WITH gc_grace_seconds={gc_grace_seconds}")

        def existing_keys(session):
            res = []
            key_exists_query = session.prepare("SELECT key from ks.cf0 WHERE key = ?")
            key_exists_query.consistency_level = ConsistencyLevel.ONE
            for i in range(num_keys):
                try:
                    if session.execute(key_exists_query, [f"k{i}"]):
                        res.append(i)
                except Unavailable:
                    pass
            return res

        logger.info("Verifying initial dataset")
        assert existing_keys(session) == list(range(num_keys))

        snapshot_name = "pre_bootstrap"
        snapshot_dirs = {}
        for node in cluster.nodelist():
            snapshot_dirs[node.name] = make_snapshot(node, ks="ks", name=snapshot_name)

        logger.info("Adding a new node")
        new_node = cluster.new_node(len(cluster.nodelist()) + 1)
        new_node.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("Running cleanup")
        cluster.nodetool("cleanup ks")

        logger.info(f"Stopping {new_node.name}")
        new_node.stop()

        logger.info(f"Get existing keys")
        existing = existing_keys(session)

        logger.info(f"Starting {new_node.name}")
        new_node.start()

        logger.info("Deleting data")
        delete_c1c2(session, n=num_keys, cf="cf0")

        logger.info("Verifying data")
        query = SimpleStatement("SELECT count(*) FROM ks.cf0", consistency_level=ConsistencyLevel.QUORUM)
        rows = session.execute(query)
        assert rows.one()[0] == 0

        # Note: since fixing #14870 we cannot rely on tombstones going fully away unless
        # commitlogs are also exorcised. Luckily, nodetool flush will help us do that
        for node in [node1, node2]:
            node.flush()

        logger.info(f"Sleeping until gc_grace_seconds={gc_grace_seconds} pass")
        time.sleep(gc_grace_seconds + 1)
        logger.info("Running compaction")
        cluster.compact()
        cluster.wait_for_compactions()

        logger.info("Restoring from snapshot")
        for node in [node1, node2]:
            restore_snapshot_with_refresh(snapshot_dir=snapshot_dirs[node.name], node=node, keyspace="ks", table="cf0", name=snapshot_name)

        logger.info(f"Removing {new_node.name}")
        new_node_hostid = new_node.hostid()
        new_node.stop()
        node1.removenode(new_node_hostid)

        logger.info("Verifying that no data was resurrected")
        restored = existing_keys(session)

        assert restored == existing

    def _num_keys_on_node(self, owner_index, num_nodes, num_keys):
        self.prepare(nodes=num_nodes, num_keys=num_keys, rf=2)
        self.cluster.flush()
        owner_node = None
        for index, node in enumerate(self.cluster.nodelist()):
            if index == owner_index:
                # to ensure the sstables are not being mutated
                node.stop()
                owner_node = node
            else:
                self.cluster.remove(node, remove_node_dir=True)
        assert owner_node is not None, f"node.{owner_index} not found"
        partitions = owner_node.dump_sstables("ks", "cf0")
        self.cluster.remove(owner_node, remove_node_dir=True)
        return len(partitions)

    def test_no_resurrection_by_replaying_stale_commitlog(self):
        # Reproducer for https://github.com/scylladb/scylladb/issues/4734
        # 1. write data to two different tables ("cf0" and "cf1") in a 1-node
        #    cluster, which is composed of node.1
        #    (the data written to "cf1" helps to keep the commitlog pinned,
        #     even after the "cf0" is cleaned)
        # 2. add another node to the cluster, namely node.2. the topology
        #    change in the cluster leads rebalance of the ring, so some keys
        #    previously belonging to node.1 are no long owned by it.
        # 3. run "nodetool cleanup" to cleanup "cf0" on node.1
        #    (this flushes memtables/tables, but without the fix, commitlog
        #    would survive the cleanup)
        # 4. kill -9 node.1
        # 5. calculate the expected number of keys on node.1
        #    num_keys_on_node1 := num_keys - num_keys_on(node2)
        # 6. remove node.2
        # 7. restart node.1
        # 8. verify that the data previous deleted in "cf0" cannot be found
        #    on node.1
        num_keys = 10000
        num_nodes = 1
        # use rf = 1, so that we can tell the exact number of the keys on
        # node1 by calculating the complementary set of keys in $num_keys
        rf = 1
        self.prepare(
            nodes=num_nodes,
            num_keys=num_keys,
            amount_of_tables=2,
            rf=rf,
        )
        node1, *_ = self.cluster.nodelist()
        ks = "ks"
        cf = "cf0"  # table to cleanup

        with self.patient_cql_connection(node1) as session:
            assert_one(session, f"SELECT COUNT(*) FROM {ks}.{cf}", [num_keys])

            # mimic the use case where the tombstone fails to gc the stable
            # data replayed on node.1 after it is restarted
            gc_grace_seconds = 0
            session.execute(f"ALTER TABLE ks.cf0 WITH gc_grace_seconds={gc_grace_seconds}")

        logger.info("Adding a new node")
        num_nodes += 1
        node2 = self.cluster.new_node(num_nodes, is_seed=False)
        node2.start(wait_for_binary_proto=True, wait_other_notice=True)

        logger.info("Running cleanup %s.%s", ks, cf)
        if not "tablets" in self.scylla_features:
            # node1 should not carry any tokens held by node2 now
            node1.nodetool(f"cleanup {ks} {cf}")

        if "tablets" in self.scylla_features:
            node2.watch_log_for([f"Streaming for tablet migration of .* finished table={ks}.cf0", f"Streaming for tablet migration of .* finished table={ks}.cf1"])
            node1.watch_log_for([f"Cleaned up tablet .* of table {ks}.cf0 successfully.", f"Cleaned up tablet .* of table {ks}.cf1 successfully."])
        logger.info("Stopping %s", node1.name)
        node1.stop(gently=False)

        def num_partitions_of(node):
            node.flush(ks, cf)
            node.compact(ks, [cf])
            node.wait_for_compactions(ks, cf)
            return len(node.dump_sstables(ks, cf))

        logger.info("Calculating the number of keys on %s", node2.name)
        expected_num_keys = num_keys - num_partitions_of(node2)
        self.cluster.remove(node2)

        logger.info("Restart %s", node1.name)
        node1.start(wait_for_binary_proto=True)
        actual_num_keys = num_partitions_of(node1)

        assert actual_num_keys == expected_num_keys

    @pytest.mark.single_node
    @pytest.mark.next_gating
    @pytest.mark.required_features("!tablets")  # cleanup skips tablets
    def test_drop_table_during_cleanup(self):
        """
        Reproducer for https://github.com/scylladb/scylladb/issues/12007

        Populate a number of tables (with enough keys to make their cleanup time substantial).
        Drop all tables concurrently during cleanup by dropping the keyspace.
        Expect cleanup to succeed.

        Dropping the keyspace tests 2 cases in parallel, in parctice.
        One is dropping a table that is currently undergoing cleanup,
        where the compaction layer needs to handle that gracefully;
        and the other case drops a table that is pending cleanup but for which
        cleanup hasn't started yet, and the api should handle this case gracefully as well.
        """
        nodes = 1
        num_tables = 3
        # cleanup is performed in each table, sorted by their data size
        # so populate more keys as we go
        factor = 10000 if isinstance(self.cluster, ScyllaCluster) and self.cluster.scylla_mode != "debug" else 1000
        table_keys = [factor * i for i in [3, 4, 5]]

        self.ignore_log_patterns.append("task_manager - Failed force_keyspace_cleanup of ks.cf2: compaction::compaction_stopped_exception[ :]+\\(?Compaction for ks/cf2 was stopped due to: truncate\\)?")

        cluster = self.cluster
        cluster.set_configuration_options({"auto_snapshot": "false"})
        cluster.populate(nodes).start(jvm_args=["--logger-log-level", "task_manager=debug"])
        node1 = self.cluster.nodelist()[0]
        session = self.patient_cql_connection(node1)
        ks = "ks"
        create_ks(session, ks, rf=nodes)
        for i in range(num_tables):
            cf = f"cf{i}"
            num_keys = table_keys[i]
            flush_every = num_keys // 10
            compaction_options = "{'class': 'SizeTieredCompactionStrategy', 'max_threshold': 2, 'min_threshold': 2}"
            create_c1c2_table(session, cf=cf, compaction=compaction_options)
            cluster.nodetool(f"disableautocompaction {ks} {cf}")

            logger.info(f"Inserting {num_keys} keys to ks.{cf}, flushing every {flush_every} keys")
            start_key = 0
            end_key = num_keys
            while start_key < end_key:
                batch_end = end_key if not flush_every else min(start_key + flush_every, end_key)
                insert_c1c2(session=session, keys=range(start_key, batch_end), cf=cf)
                start_key = batch_end
                for node in cluster.nodelist():
                    node.flush("ks", cf)

        def drop_keyspace(session, node, from_mark):
            start_time = datetime.now()
            expected_cleanups = num_tables * node._smp
            logger.info("Waiting for %d cleanups to start", expected_cleanups)
            while True:
                matches = node1.grep_log("Starting force_keyspace_cleanup", from_mark=from_mark)
                if len(matches) >= expected_cleanups:
                    break
                if (datetime.now() - start_time).total_seconds() > 600:
                    pytest.fail("Deadline for starting cleanups has elapsed")

            q = f"DROP KEYSPACE {ks}"
            logger.info(q)
            session.execute(q)

        executor = ThreadPoolExecutor(max_workers=1)

        thread = executor.submit(drop_keyspace, session, node1, node1.mark_log())

        logger.info("Running cleanup")
        try:
            node1.nodetool(f"cleanup {ks}")
        except ToolError as e:
            assert re.search(r"compaction::compaction_stopped_exception[ :]+\(?Compaction for ks/cf2 was stopped due to: truncate\)?\n$", str(e))

        thread.result()
