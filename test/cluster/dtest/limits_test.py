#
# Copyright (C) 2015-present The Apache Software Foundation
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging
import math
import os
import pathlib
import re
import resource
import ssl
import sys
from subprocess import PIPE, Popen, check_output

import pytest
import requests
from cassandra import InvalidRequest, WriteFailure
from cassandra.cluster import NoHostAvailable, Session
from ccmlib.cluster import Cluster
from ccmlib.node import Node

from dtest_class import Tester, create_ks, get_ip_from_node
from tools.misc import generate_ssl_stores, is_coverage

logger = logging.getLogger(__name__)
# Those are ideal values according to c* specifications
# they should pass

LIMIT_64_K = 64 * 1024
LIMIT_32K = 32 * 1024
LIMIT_128K = 128 * 1024
LIMIT_2GB = 2 * 1024 * 1024 * 1024

MAX_KEY_SIZE = LIMIT_64_K
MAX_BLOB_SIZE = 8388608  # theoretical limit LIMIT_2GB
MAX_COLUMNS = LIMIT_128K
MAX_TUPLES = LIMIT_32K
MAX_BATCH_SIZE = 50 * 1024
MAX_CELLS_COLUMNS = LIMIT_32K
MAX_CELLS_BATCH_SIZE = 50
MAX_CELLS = 16777216

# Those are values used to validate the tests code
# MAX_KEY_SIZE = 1000
# MAX_BLOB_SIZE = 1000
# MAX_COLUMNS = 1000
# MAX_TUPLES = 1000
# MAX_BATCH_SIZE = 1000
# MAX_CELLS_COLUMNS = 100
# MAX_CELLS_BATCH_SIZE = 100
# MAX_CELLS = 1000


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestLimits(Tester):
    def prepare(self):
        """
        Sets up node to test against.
        """
        cluster = self.cluster
        return cluster

    def _do_test_max_key_length(self, session, node, size, expect_failure=False):
        print("Testing max key length for {}.{}".format(size, " Expected failure..." if expect_failure else ""))
        key_name = "k" * size

        c = f"CREATE TABLE test1 ({key_name} int PRIMARY KEY)"
        if expect_failure:
            expected_error = r"Key size too large: \d+ > 65535"
            self.ignore_log_patterns += [expected_error]
            with pytest.raises(Exception, match=expected_error):
                session.execute(c)
            return

        session.execute(c)

        session.execute("insert into ks.test1  (%s) values (1);" % key_name)
        session.execute("insert into ks.test1  (%s) values (2);" % key_name)

        node.flush()
        # Select
        res = session.execute(
            """
                SELECT * FROM ks.test1
                WHERE %s=1
        """
            % key_name
        )

        assert len(res.current_rows) == 1

        res = session.execute(
            """
                SELECT * FROM ks.test1
                WHERE %s=2
        """
            % key_name
        )

        assert len(res.current_rows) == 1
        session.execute("""DROP TABLE test1""")

    def test_max_key_length(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)

        # biggest that will currently work in scylla
        # key_name = "k" * 65526
        self._do_test_max_key_length(session, node, MAX_KEY_SIZE, expect_failure=True)
        self._do_test_max_key_length(session, node, MAX_KEY_SIZE - 9, expect_failure=True)

        self._do_test_max_key_length(session, node, MAX_KEY_SIZE - 10)

        size = MAX_KEY_SIZE // 2
        while size >= 1:
            self._do_test_max_key_length(session, node, size)
            size >>= 3

    def _do_test_blob_size(self, session, node, size):
        print("Testing blob size %i" % size)

        blob_a = "a" * size
        blob_b = "b" * size

        session.execute(
            """
            CREATE TABLE test1 (
                user ascii PRIMARY KEY,
                payload blob,
            )
        """
        )

        session.execute("insert into ks.test1  (user, payload) values ('tintin', textAsBlob('%s'));" % blob_a)
        session.execute("insert into ks.test1  (user, payload) values ('milou', textAsBlob('%s'));" % blob_b)

        node.flush()
        # Select
        res = session.execute(
            """
                SELECT * FROM ks.test1
                WHERE user='tintin'
        """
        )

        assert len(list(res)) == 1

        res = session.execute(
            """
                SELECT * FROM ks.test1
                WHERE user='milou'
        """
        )

        assert len(list(res)) == 1
        session.execute("""DROP TABLE test1""")

    def test_max_column_value_size(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)

        size = 1
        for i in range(int(math.log(MAX_BLOB_SIZE, 2))):
            size <<= 1
            self._do_test_blob_size(session, node, size - 1)

    def _do_test_max_columns(self, session, count, expect_failure=False):
        print("Testing maximum numbers of columns with count {}.{}".format(count, " Expected failure..." if expect_failure else ""))

        # we must count the primary key
        count -= 1
        count = max(count, 0)

        keys = ""
        keys_create = ""
        for i in range(count):
            keys += "key" + str(i) + ", "
            keys_create += "key" + str(i) + " int, "
        values = "1, " * count

        c = """CREATE TABLE test1 (%s blub int PRIMARY KEY,)""" % keys_create
        if expect_failure:
            expected_error = r"Command size \d+ is greater than the configured limit \d+"
            self.ignore_log_patterns += [expected_error]
            with pytest.raises(Exception, match=expected_error):
                session.execute(c)
            return

        session.execute(c)

        c = f"insert into ks.test1  ({keys} blub) values ({values} 1);"
        session.execute(c)

        session.execute("""DROP TABLE test1""")

    @pytest.mark.next_gating
    @pytest.mark.scylla_mode("!debug")  # client times out in debug mode
    def test_max_columns_and_query_parameters(self):
        cluster = self.prepare()

        if is_coverage(self.cluster.get_install_dir()):
            pytest.skip("Client timeout when coverage is enabled")

        cluster.set_configuration_options(values={"query_tombstone_page_limit": 9999999})
        cluster.populate(1).start(jvm_args=["--memory", "4G"])
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)
        session.execute("UPDATE system.config SET value='100000' WHERE name='query_tombstone_page_limit'")

        count = 1
        for i in range(int(math.log(MAX_COLUMNS, 2))):
            count <<= 1
            self._do_test_max_columns(session, count - 1, expect_failure=count == MAX_COLUMNS)

    def _do_test_max_tuples(self, session, node, count):
        print("Testing max tuples for %i" % count)
        t = ""
        v = ""
        for i in range(count):
            t += "int, "
            v += "1, "
        t = t[:-2]
        v = v[:-2]

        c = (
            """
            CREATE TABLE stuff (
              k int PRIMARY KEY,
              v frozen<tuple<%s>>
            );
            """
            % t
        )
        session.execute(c)

        c = "INSERT INTO stuff (k, v) VALUES(0, (%s));" % v
        session.execute(c)

        c = "SELECT * FROM STUFF;"
        res = session.execute(c)
        assert len(res.current_rows) == 1

        session.execute("""DROP TABLE stuff""")

    def test_max_tuple(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)

        count = 1
        for i in range(int(math.log(MAX_TUPLES, 2))):
            count <<= 1
            self._do_test_max_tuples(session, node, count - 1)

    def _do_test_max_batch_size(self, session, node, size):
        print("Testing max batch size for size=%i" % size)
        c = """
            CREATE TABLE stuff (
              k int PRIMARY KEY,
              v text
            );
            """
        session.execute(c)

        c = "BEGIN UNLOGGED  BATCH\n"
        row_size = 1000
        overhead = 100
        blob = (row_size - overhead) * "x"
        rows = size // row_size
        for i in range(rows):
            c += "INSERT INTO stuff (k, v) VALUES(%i, '%s')\n" % (i, blob)
        c += "APPLY BATCH;\n"

        session.execute(c)

        c = "SELECT * FROM STUFF;"
        res = session.execute(c)

        assert len(list(res)) == rows
        session.execute("""DROP TABLE STUFF""")

    @pytest.mark.dtest_debug
    def test_max_batch_size(self):
        cluster = self.prepare()
        cluster.populate(1).start()
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)

        size = 1
        for i in range(int(math.log(MAX_BATCH_SIZE, 2))):
            size <<= 1
            self._do_test_max_batch_size(session, node, size - 1)

    def _do_test_max_cell_count(self, session, node, cells):
        print("Testing max cells count for %i" % cells)
        keys = ""
        keys_create = ""
        columns = MAX_CELLS_COLUMNS
        for i in range(columns):
            keys += "key" + str(i) + ", "
            keys_create += "key" + str(i) + " int, "
        values = "1, " * columns

        c = """CREATE TABLE test1 (%s blub int PRIMARY KEY,)""" % keys_create
        session.execute(c)

        batch_size = MAX_CELLS_BATCH_SIZE
        rows = cells // columns
        c = "BEGIN UNLOGGED  BATCH\n"
        for i in range(rows):
            c += "insert into ks.test1  (%s blub) values (%s %i);\n" % (keys, values, i)
            if i == rows - 1 or (i + 1) % batch_size == 0:
                c += "APPLY BATCH;\n"
                session.execute(c)
                c = "BEGIN UNLOGGED  BATCH\n"

        session.execute("""DROP TABLE test1""")

    @pytest.mark.scylla_mode("!debug")  # client times out in debug mode
    def test_max_cells(self):
        cluster = self.prepare()
        cluster.set_configuration_options(values={"query_tombstone_page_limit": 9999999, "batch_size_warn_threshold_in_kb": 1024 * 1024, "batch_size_fail_threshold_in_kb": 1024 * 1024, "commitlog_segment_size_in_mb": 64})
        cluster.populate(1).start(jvm_args=["--smp", "1", "--memory", "2G"])
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session, "ks", 1)

        cells = 1
        for i in range(int(math.log(MAX_CELLS, 2))):
            cells <<= 1
            self._do_test_max_cell_count(session, node, cells - 1)

    @pytest.mark.parametrize("mode", ["SSL", "non-SSL"])
    def test_accepted_large_request(self, mode):
        """
        Tests #19472 - we now handle requests
        as long as they can fit into commitlog as whole (including overhead)
        thus the below inserts both should be fine.
        """
        if mode == "SSL":
            cluster_populate = self.populate_cluster_with_ssl_enabled
            create_session = self.create_cql_session_with_ssl
        else:
            cluster_populate = self.prepare().populate
            create_session = self.patient_cql_connection

        logger.debug("Preparing the cluster...")
        cluster = cluster_populate(1)
        cluster.start(jvm_args=["--smp", "2", "--commitlog-total-space-in-mb", "1024", "--memory", "2G"])
        logger.debug("Cluster has been prepared...")
        node = cluster.nodelist()[0]

        session = create_session(node)

        logger.debug("Creating a keyspace...")
        create_ks(session=session, name="test_keyspace", rf=1)

        logger.debug("Creating a table...")
        session.execute("create table test_keyspace.test_table(id int primary key, test_string text);")

        logger.info("Trying to send a large request to database (insert a large string into table)...")
        long_string = "scylla" * 5 * 1024 * 1024
        id_value = 17
        session.execute(query=f"insert into test_keyspace.test_table (id, test_string) values ({id_value}, '{long_string}');")

        session = create_session(node)
        output = session.execute(f"select test_string from test_keyspace.test_table where id = {id_value};")
        assert long_string == output.current_rows[0].test_string, "Expected the table contains very long string"

        logger.info("Trying to send a regular request to database (insert a string of regular size into table)...")
        short_string = "scylla" * 1024 * 1024
        session.execute(query=f"insert into test_keyspace.test_table (id, test_string) values ({id_value}, '{short_string}');")
        output = session.execute(f"select test_string from test_keyspace.test_table where id = {id_value};")
        assert short_string == output.current_rows[0].test_string, "Expected to get the regular string inserted, but did not find it in the table!"

    @pytest.mark.parametrize("mode", ["SSL", "non-SSL"])
    def test_request_too_large(self, mode):
        """
        The request is considered as "too large" if it, and its overhead, cannot fit into a shards commitlog.
        The test scenario is following:
        1. Create node with 2 shards and total size 60mb.
        2. Create a new keyspace and a new table.
        3. Generate a too large request: try to insert a very large string into the table.
        4. Check the exception was raised.
        5. Create a new session and check the table is still empty.
        6. Try to send a regular size request (insert a small string).
        7. Check the string was inserted into the table
        """
        if mode == "SSL":
            cluster_populate = self.populate_cluster_with_ssl_enabled
            create_session = self.create_cql_session_with_ssl
        else:
            cluster_populate = self.prepare().populate
            create_session = self.patient_cql_connection

        logger.debug("Preparing the cluster...")
        cluster = cluster_populate(1)
        cluster.start(jvm_args=["--smp", "2", "--commitlog-total-space-in-mb", "60", "--commitlog-segment-size-in-mb", "6"])
        logger.debug("Cluster has been prepared...")
        node = cluster.nodelist()[0]

        session = create_session(node)

        logger.debug("Creating a keyspace...")
        create_ks(session=session, name="test_keyspace", rf=1)

        logger.debug("Creating a table...")
        session.execute("create table test_keyspace.test_table(id int primary key, test_string text);")

        logger.info("Trying to send a large request to database (insert a large string into table)...")
        long_string = "scylla" * 5 * 1024 * 1024
        id_value = 17
        expected_error = f"Could not write mutation test_keyspace:test_table.*std::invalid_argument.*Mutation.*is too large"
        self.ignore_log_patterns.append(expected_error)
        with pytest.raises((NoHostAvailable, WriteFailure, InvalidRequest)):
            session.execute(query=f"insert into test_keyspace.test_table (id, test_string) values ({id_value}, '{long_string}');")

        session = create_session(node)
        output = session.execute(f"select test_string from test_keyspace.test_table where id = {id_value};")
        assert not output.current_rows, "Expected the table was empty, but id had rows inserted!"

        logger.info("Trying to send a regular request to database (insert a string of regular size into table)...")
        short_string = "scylla" * 1024 * 1024
        session.execute(query=f"insert into test_keyspace.test_table (id, test_string) values ({id_value}, '{short_string}');")
        output = session.execute(f"select test_string from test_keyspace.test_table where id = {id_value};")
        assert short_string == output.current_rows[0].test_string, "Expected to get the regular string inserted, but did not find it in the table!"

    def populate_cluster_with_ssl_enabled(self, nodes_num: int) -> Cluster:
        cluster = self.cluster
        generate_ssl_stores(self.test_path, ip_addresses=[f"{self.cluster.get_ipprefix()}1"])
        options = {"enabled": True, "certificate": os.path.join(self.test_path, "ccm_node.pem"), "keyfile": os.path.join(self.test_path, "ccm_node.key")}
        cluster.set_configuration_options({"client_encryption_options": options})
        cluster.populate(nodes_num)
        return cluster

    def create_cql_session_with_ssl(self, node_to_connect: Node) -> Session:
        # cause of issue https://github.com/scylladb/python-driver/issues/261
        # we are reverting to older TLS version, until we'll figure it out
        # or release a driver with work-around
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ssl_context.load_cert_chain(certfile=os.path.join(self.test_path, "ccm_node.pem"), keyfile=os.path.join(self.test_path, "ccm_node.key"))
        ssl_context.check_hostname = True
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.load_verify_locations(cafile=os.path.join(self.test_path, "ccm_node.cer"))

        return self.patient_cql_connection(node=node_to_connect, ssl_context=ssl_context)


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestMaxCQLConnections(Tester):
    def test_max_cql_connections(self):
        """
        Verifies fix https://github.com/scylladb/scylla/pull/9052 which aimed issue for crashing scylla when
        there was more than 10000 connections per shard. Fix adds possibility to set max no of connections and
        increased default value. But still db crashes when reaching this limit (tracked by #9056).

        Test verifies also if connection pool is properly released after connection shutdown.
        """
        workers = 10  # opening many connections in python gets slower and slower. Spreading to workers helps.
        connections_per_worker = 1050
        total_connections = workers * connections_per_worker
        self._tune_max_open_files_limit(total_connections)
        self.cluster.populate(1).start(jvm_args=["--smp", "1", "--max-networking-io-control-blocks", str(total_connections)])
        address = self.cluster.nodelist()[0].address()
        processes, connections_created = self._create_cql_connections(address, connections_per_worker=connections_per_worker, workers=workers)

        connections_created_metric = self._get_cql_connections_from_metrics(address)
        assert connections_created_metric >= connections_created, f"only {connections_created_metric} connections created from {connections_created} required"
        self._close_connections(processes)

        # repeat to verify scylla closed connections correctly and can create new ones
        processes, connections_created = self._create_cql_connections(address, connections_per_worker=connections_per_worker, workers=workers)
        self._close_connections(processes)
        connections_created_metric = self._get_cql_connections_from_metrics(address)
        assert connections_created_metric >= connections_created, f"only {connections_created_metric} connections created from {connections_created} required"

    def _create_cql_connections(self, address, connections_per_worker, workers):
        logger.info("starting creating connections")
        script_path = pathlib.Path(__file__).parent.absolute() / "scripts" / "create_dummy_cql_connections.py"
        processes = []
        connections_regex = re.compile(r"(\d+) cql connections created.")
        connections_created = 0
        for _ in range(workers):
            process = Popen([sys.executable, script_path, address, str(connections_per_worker)], stdin=PIPE, stdout=PIPE, stderr=PIPE, universal_newlines=True)
            processes.append(process)
            # wait for finish connection creation
            line = process.stdout.readline()
            try:
                connections = int(connections_regex.match(line).group(1))
                connections_created += connections
            except ValueError:
                assert False, f"Dummy connections creation script failed after creating {connections_created} connections. stdout: {line}, stderr: {'\n'.join(process.stderr.readlines())}"
            if not connections == connections_per_worker:
                break
        logger.info(f"Created {connections_created} connections")
        assert connections_created == (workers * connections_per_worker), f"Only {connections_created} connections created from {workers * connections_per_worker} requested"

        logger.info("All connections created successfully")
        return processes, connections_created

    def _close_connections(self, processes):
        """dummy cql connections scripts end after pressing any key."""
        for process in processes:
            stdout, stderr = process.communicate("a", timeout=10)
            assert process.returncode == 0, f"Error in create dummy connections script: {stderr}"

    def _get_cql_connections_from_metrics(self, address):
        resp = requests.get(f"http://{address}:9180/metrics")
        for line in resp.text.splitlines():
            if line.startswith("scylla_transport_cql_connections"):
                return int(line.split('scylla_transport_cql_connections{shard="0"}')[1])

    def _tune_max_open_files_limit(self, total_connections):
        """each connection creates 1 open file per shard.
        Creating many connections requires tuning max open files in system."""
        pid = os.getpid()
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        soft_new_limit = max(soft, 2 * total_connections)
        hard_new_limit = max(hard, 2 * total_connections)
        logger.debug(f"current limits: {soft}, {hard}")
        if soft < soft_new_limit:
            check_output(f"sudo prlimit --pid {pid} --nofile={soft_new_limit}:{hard_new_limit}", shell=True, universal_newlines=True)
        ulimit = int(check_output("ulimit -n", shell=True, universal_newlines=True))
        assert ulimit == soft_new_limit
        logger.info(f"Updated max open files limit to: {ulimit}")

    @pytest.mark.use_cassandra_stress
    def test_max_cql_connections_with_stress(self):
        self.ignore_log_patterns += ["wait queue overload"]
        total_connections = 10000
        smp = 2
        self._tune_max_open_files_limit(total_connections)
        self.cluster.populate(1).start(jvm_args=["--smp", str(smp), "--max-networking-io-control-blocks", str(total_connections)])
        node = self.cluster.nodelist()[0]
        address = node.address()
        node.stress(["write", "n=50000", "no-warmup", "-mode", "native", "cql3", f"connectionsPerHost={smp * total_connections}", "-rate", "threads=1", "throttle=500/s", "-pop", "seq=1..50000"])
        connections_created = self._get_cql_connections_from_metrics(address)
        node.stop()  # release resources gently

        expected_min_number_of_connections = (total_connections - 50) if node.scylla_mode() != "debug" else (total_connections - 50) / 1000
        assert connections_created >= expected_min_number_of_connections, f"only {connections_created} connections created from {expected_min_number_of_connections} required"
