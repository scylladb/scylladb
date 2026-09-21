import logging
import os.path
import re
from contextlib import contextmanager
from time import sleep, time

import pytest
from cassandra import concurrent

from dtest_class import Tester, create_ks
from tools.cassandra_stess import CassandraStressDocker
from tools.files import get_node_cf_dir, get_sstables_files
from tools.misc import generate_random_text

logger = logging.getLogger(__name__)


class TestTimeWindowCompactionStrategyAdditional(Tester):
    @pytest.mark.next_gating
    @pytest.mark.dtest_full
    @pytest.mark.single_node
    def test_expired_sstables_are_compacted_separately(self):
        """
        verify if expired sstables are compacted (removed) separately even during high load.
        Tests #9533 fixed in 4.6.rc1.
        """
        cluster = self.cluster
        args = ["--smp", "1", "--logger-log-level", "compaction=debug"]
        cluster.populate(1).start(wait_for_binary_proto=True, jvm_args=args)
        ttl = 30
        node = self.cluster.nodelist()[0]
        test_max_duration_minutes = 12 if node.scylla_mode() == "debug" else 4
        expiry_grace_time = 120 if node.scylla_mode() == "debug" else 30
        session = self.patient_cql_connection(node)

        self._prepare_twcs_table(ttl=120, session=session)
        node.nodetool("disableautocompaction")
        sstables = self.create_sstables_with_short_ttl(session, ttl=ttl)
        with self._start_high_load_on_cluster(duration_minutes=test_max_duration_minutes):
            sleep(ttl + expiry_grace_time)  # wait for sstables to be expired with some margin
            mark = self.cluster.nodelist()[0].mark_log()
            node.nodetool("enableautocompaction")
            timeout = (test_max_duration_minutes + 1) * 60
            sstable_exists = self.wait_until_sstables_are_evicted(sstables, timeout)

        assert not sstable_exists, "Expired sstables should be removed soon after expiration time (upon compaction)"
        self.expired_sstables_should_not_be_compacted_along_with_unexpired(mark)

    @staticmethod
    def _prepare_twcs_table(ttl, session):
        create_ks(session=session, name="keyspace1", rf=1)
        cf = f"""CREATE TABLE keyspace1.standard1
                ( key blob PRIMARY KEY, "C0" blob, "C1" blob, "C2" blob, "C3" blob, "C4" blob )
                WITH bloom_filter_fp_chance = 0.01 AND caching = {{'keys': 'ALL', 'rows_per_partition': 'ALL'}}
                AND compaction = {{'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1',
                'compaction_window_unit': 'MINUTES', 'expired_sstable_check_frequency_seconds': '0'}}
                AND crc_check_chance = 1.0
                AND dclocal_read_repair_chance = 0.0
                AND default_time_to_live = {ttl}
                AND gc_grace_seconds = 0 AND max_index_interval = 2048
                AND memtable_flush_period_in_ms = 0 AND min_index_interval = 128
                AND read_repair_chance = 0.0
                AND speculative_retry = '99.0PERCENTILE';"""
        session.execute(cf)
        logger.info("table prepared")

    @contextmanager
    def _start_high_load_on_cluster(self, duration_minutes):
        logger.info("Starting high load on cluster")
        node = self.cluster.nodelist()[0]
        num_threads = 30 if node.scylla_mode() == "debug" else 300
        stress_options = ["write", f"duration={duration_minutes}m", "no-warmup", "-rate", f"threads={num_threads}", "-mode", "native", "cql3", "-pop", "seq=1..1000000000", "-node", node.address()]

        with CassandraStressDocker(node, stress_cmd=f"cassandra-stress {' '.join(stress_options)}", capture_output=False) as cassandra_stress_docker:
            cassandra_stress_docker.run()
            logger.info("started stress")
            yield cassandra_stress_docker
            cassandra_stress_docker.remove_stress_container()

    def create_sstables_with_short_ttl(self, session, duration_minutes=3, flush_period_seconds=30, ttl=30):
        """Simulate a write process across duration minutes. When using TWCS, should create ~duration_minutes number of
        sstables (when time window is 1 minute).
        Returns created sstables names.

        We use `USING TIMESTAMP` to distribute the writes evenly
        across the entire range, simulating a write every second (to
        several partitions).

        Arguments:
            session {Session} -- opened session to node

        Keyword Arguments:
            duration_minutes {number} -- how many minutes to simulate (default: {20})
            flush_period_seconds {number} -- simulates period when data is flushed in seconds (default: {30})

        """
        node = self.cluster.nodelist()[0]
        ks = "keyspace1"
        cf = "standard1"
        insert_statement = session.prepare(f'INSERT INTO {ks}.{cf} (key, "C0", "C1", "C2", "C3", "C4") VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ? AND TTL {ttl}')
        rand_pks = set()

        logger.info(f"creating sstables with ttl={ttl}")
        while len(rand_pks) < 10:
            rand_pks.add(generate_random_text(10).encode())

        for t in range(duration_minutes * 60, 0, -1):
            timestamp = int(time() - t)
            concurrent.execute_concurrent_with_args(session, insert_statement, [(pk, b"test", b"test", b"test", b"test", b"tt", timestamp * 1000 * 1000) for pk in rand_pks])
            # Flush every flush period in seconds on each node
            if t % flush_period_seconds == 0:
                node.flush()
        node.flush()
        logger.info("sstables created")
        cf_dir = get_node_cf_dir(node, ks, cf)
        sstables_file_names = get_sstables_files(cf_dir, f_type="Data")
        logger.debug(f"created sstables: {sstables_file_names}")
        return sstables_file_names

    def wait_until_sstables_are_evicted(self, sstables, timeout):
        """Waits until sstables are removed from disk. Returns list of not removed sstables."""
        logger.info("Waiting for sstables to be removed (due to expiration)")
        node = self.cluster.nodelist()[0]
        cf_dir = get_node_cf_dir(node, "keyspace1", "standard1")
        start_time = time()
        while time() - start_time < timeout:
            sstables = [table for table in sstables if os.path.exists(cf_dir + "/" + table)]
            if sstables:
                logger.debug(f"still not removed: {sstables}")
                node.flush()
                sleep(2)
                continue
            break
        logger.info("sstables has been removed")
        return sstables

    def expired_sstables_should_not_be_compacted_along_with_unexpired(self, from_mark):
        node = self.cluster.nodelist()[0]
        log_file = os.path.join(node.get_path(), "logs", "system.log")

        def get_sstables(line):
            return re.search(r"[\w-]+-Data\.db", line).group(0)

        with open(log_file) as system_log:
            system_log.seek(from_mark)
            lines = system_log.readlines()
            ks_compaction_lines = [line for line in lines if "compaction - [Compact keyspace1.standard1" in line]
            start_compaction_lines = [line for line in ks_compaction_lines if "Compacting [" in line]
            end_compaction_lines = [line for line in ks_compaction_lines if "] Compacted" in line]

            expired_sstables = {get_sstables(line) for line in lines if "Fully expired sstable" in line}
            assert expired_sstables, "no expired sstables found in logs"
            logger.info(f"Expired sstables according to logs: {expired_sstables}")

            compactions = dict()
            for line in start_compaction_lines:
                if expired_sstables_in_compaction := [sstable for sstable in expired_sstables if sstable in line]:
                    compaction_id = line.split("compaction - [Compact keyspace1.standard1 ")[1].split("]")[0]
                    compactions[compaction_id] = expired_sstables_in_compaction
            assert compactions, "no compaction of keyspace1.standard1 found in logs"

            for compaction_id, expired_sstables_in_compaction in compactions.items():
                for line in end_compaction_lines:
                    if compaction_id in line:
                        assert "sstables to []" in line, f"expired sstables {expired_sstables_in_compaction} were compacted along with unexpired ones. id: {compaction_id}. Issue #9533"
