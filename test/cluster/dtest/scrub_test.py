import glob
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
import uuid

import pytest
from ccmlib import common

from dtest_class import Tester, create_cf, create_ks

logger = logging.getLogger(__name__)
KEYSPACE = "ks"


class TestHelper(Tester):
    def get_table_path(self, table):
        """
        Return the path where the table sstables are located
        """
        node1 = self.cluster.nodelist()[0]
        path = ""
        basepath = os.path.join(node1.get_path(), "data", KEYSPACE)
        for x in os.listdir(basepath):
            if x.startswith(table):
                path = os.path.join(basepath, x)
                break
        return path

    def get_index_path(self, index):
        """
        Return the path where the index sstables are located
        """
        node1 = self.cluster.nodelist()[0]
        basepath = os.path.join(node1.get_path(), "data", KEYSPACE)
        index_path = ""
        for x in os.listdir(basepath):
            if x.startswith(index):
                index_path = os.path.join(basepath, x)
                break
        return index_path

    def get_sstable_files(self, path):
        """
        Return the sstable files at a specific location
        """
        ret = []
        logger.debug(f"Checking sstables in {path}")

        for ext in ("*.db", "*.txt", "*.adler32", "*.sha1"):
            for fname in glob.glob(os.path.join(path, ext)):
                bname = os.path.basename(fname)
                if "-scylla." in bname.lower():
                    continue
                ret.append(bname)
        return ret

    def delete_non_essential_sstable_files(self, table):
        """
        Delete all sstable files except for the -Data.db file and the
        -Statistics.db file (only available in >= 3.0)
        """
        # NOTE: TOC file is essential for ScyllaDB, also any component in the
        # TOC is required, so we need to remove deleted components from the TOC too.
        # See https://github.com/scylladb/scylladb/issues/21145

        sstable_re = re.compile(
            r"""(?P<version>la|m[cdes]|n[a-b]|o[a]|d[a])- # the sstable version
                                    (?P<id>[^-]+)-          # sstable identifier
                                    (?P<format>\w+)-        # format: 'big' or 'bti'
                                    (?P<component>.*)       # component: e.g., 'Data'""",
            re.X,
        )
        bti_tocs = list()
        big_tocs = list()
        for fname in self.get_sstable_files(self.get_table_path(table)):
            # Collect removed TOCs, to be restored later.
            fullname = os.path.join(self.get_table_path(table), fname)
            matched = sstable_re.fullmatch(os.path.basename(fname))
            if matched and matched["component"] == "TOC.txt":
                if matched["version"] in ["ms", "da"]:
                    bti_tocs.append(fullname)
                else:
                    big_tocs.append(fullname)
            if not matched or matched["component"] not in ["Data.db", "Index.db", "Statistics.db", "Partitions.db", "Rows.db"]:
                logger.debug(f"Deleting {fullname}")
                os.remove(fullname)
        logger.info(f"TOCS: {bti_tocs + big_tocs}")
        # Restore TOCs
        for toc in big_tocs:
            logger.debug(f"restoring TOC {toc}")
            with open(toc, "w") as f:
                f.write("Data.db\n")
                f.write("Index.db\n")
                f.write("Statistics.db\n")
                f.write("TOC.txt\n")
                f.flush()
        for toc in bti_tocs:
            logger.debug(f"restoring TOC {toc}")
            with open(toc, "w") as f:
                f.write("Data.db\n")
                f.write("Partitions.db\n")
                f.write("Rows.db\n")
                f.write("Statistics.db\n")
                f.write("TOC.txt\n")
                f.flush()

    def get_sstables(self, table, indexes):
        """
        Return the sstables for a table and the specified indexes of this table
        """
        sstables = {}
        table_sstables = self.get_sstable_files(self.get_table_path(table))
        assert len(table_sstables) > 0, f"sstables were not found in {self.get_table_path(table)}"
        sstables[table] = sorted(table_sstables)

        for index in indexes:
            index_sstables = self.get_sstable_files(self.get_index_path(index))
            assert len(index_sstables) > 0, f"No indexes were found by path: {self.get_index_path(index)}"
            sstables[index] = sorted(f"{index}/{sstable}" for sstable in index_sstables)

        return sstables

    def launch_nodetool_cmd(self, cmd):
        """
        Launch a nodetool command and check the result is empty (no error)
        """
        node1 = self.cluster.nodelist()[0]
        response = node1.nodetool(cmd, capture_output=True)[0]
        if not common.is_win():  # nodetool always prints out on windows
            assert len(response) == 0, response  # nodetool does not print anything unless there is an error

    def launch_standalone_scrub(self, ks, cf):
        """
        Launch the standalone scrub
        """
        node1 = self.cluster.nodelist()[0]

        table_path = self.get_table_path(cf)

        with tempfile.TemporaryDirectory() as tmp_dir:
            node1.run_scylla_sstable("scrub", additional_args=["--scrub-mode", "abort", "--output-dir", tmp_dir, "--logger-log-level", "scylla-sstable=debug", "--unsafe-accept-nonempty-output-dir"], keyspace=ks, column_families=[cf])
            # Replace the table's sstables with the scrubbed ones, just like online scrub would do.
            shutil.rmtree(table_path)
            shutil.copytree(tmp_dir, table_path)

    def perform_node_tool_cmd(self, cmd, table, indexes):
        """
        Perform a nodetool command on a table and the indexes specified
        """
        self.launch_nodetool_cmd(f"{cmd} {KEYSPACE} {table}")
        for index in indexes:
            self.launch_nodetool_cmd(f"{cmd} {KEYSPACE} {index}_index")

    def flush(self, table, *indexes):
        """
        Flush table and indexes via nodetool, and then return all sstables
        in a dict keyed by the table or index name.
        """
        self.perform_node_tool_cmd("flush", table, indexes)
        return self.get_sstables(table, indexes)

    def scrub(self, table, *indexes):
        """
        Scrub table and indexes via nodetool, and then return all sstables
        in a dict keyed by the table or index name.
        """
        self.perform_node_tool_cmd("scrub", table, indexes)
        return self.get_sstables(table, indexes)

    def standalonescrub(self, table, *indexes):
        """
        Launch standalone scrub on table and indexes, and then return all sstables
        in a dict keyed by the table or index name.
        """
        self.launch_standalone_scrub(KEYSPACE, table)
        for index in indexes:
            self.launch_standalone_scrub(KEYSPACE, f"{index}_index")
        return self.get_sstables(table, indexes)


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.next_gating
class TestScrubIndexes(TestHelper):
    """
    Test that we scrub indexes as well as their parent tables
    """

    def create_users(self, session):
        columns = {"password": "varchar", "gender": "varchar", "session_token": "varchar", "state": "varchar", "birth_year": "bigint"}
        create_cf(session, "users", columns=columns)
        session.execute("CREATE INDEX gender_idx ON users (gender)")
        session.execute("CREATE INDEX state_idx ON users (state)")
        session.execute("CREATE INDEX birth_year_idx ON users (birth_year)")

    def update_users(self, session):
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1978)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1982)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user3', 'ch@ngem3c', 'f', 'TX', 1978)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user4', 'ch@ngem3d', 'm', 'CA', 1982)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user5', 'ch@ngem3e', 'f', 'TX', 1978)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user6', 'ch@ngem3f', 'm', 'CA', 1982)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user7', 'ch@ngem3g', 'f', 'TX', 1978)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user8', 'ch@ngem3h', 'm', 'CA', 1982)")

        session.execute("DELETE FROM users where KEY = 'user1'")
        session.execute("DELETE FROM users where KEY = 'user5'")
        session.execute("DELETE FROM users where KEY = 'user7'")

    def query_users(self, session):
        ret = list(session.execute("SELECT * FROM users"))
        ret.extend(list(session.execute("SELECT * FROM users WHERE state='TX'")))
        ret.extend(list(session.execute("SELECT * FROM users WHERE gender='f'")))
        ret.extend(list(session.execute("SELECT * FROM users WHERE birth_year=1978")))
        assert len(ret) == 8, "Invalid number of records in table 'users'"
        return ret

    def test_scrub_static_table(self):
        cluster = self.cluster
        cluster.set_configuration_options(
            values={
                "tablets_initial_scale_factor": 1,
            }
        )
        cluster.populate(1).start(jvm_args=["--smp", "1"])
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, KEYSPACE, 1)

        self.create_users(session)
        self.update_users(session)

        initial_users = self.query_users(session)
        initial_sstables = self.flush("users", "gender_idx", "state_idx", "birth_year_idx")
        scrubbed_sstables = self.scrub("users", "gender_idx", "state_idx", "birth_year_idx")

        assert len(initial_sstables) == len(scrubbed_sstables), "Amount of sstables before and after scrub differs"

        users = self.query_users(session)
        assert initial_users == users, "List of users before and after scrub are different"

        # Scrub and check sstables and data again
        scrubbed_sstables = self.scrub("users", "gender_idx", "state_idx", "birth_year_idx")
        assert len(initial_sstables) == len(scrubbed_sstables), "Amount of sstables before and after scrub differs"

        users = self.query_users(session)
        assert initial_users == users, "List of users before and after scrub are different"

        # Restart and check data again
        cluster.stop()
        cluster.start()

        session = self.patient_cql_connection(node1)
        session.execute("USE %s" % (KEYSPACE))

        users = self.query_users(session)
        assert initial_users == users, "List of users before and after scrub are different"

    def test_standalone_scrub(self):
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, KEYSPACE, 1)

        self.create_users(session)
        self.update_users(session)

        initial_users = self.query_users(session)
        initial_sstables = self.flush("users", "gender_idx", "state_idx", "birth_year_idx")

        cluster.stop()

        scrubbed_sstables = self.standalonescrub("users", "gender_idx", "state_idx", "birth_year_idx")
        assert len(initial_sstables) == len(scrubbed_sstables), "Amount of sstables before and after scrub differs"

        cluster.start()
        session = self.patient_cql_connection(node1)
        session.execute("USE %s" % (KEYSPACE))

        users = self.query_users(session)
        assert initial_users == users, "List of users before and after scrub are different"

    def test_scrub_collections_table_without_indexes(self):
        cluster = self.cluster
        cluster.set_configuration_options(
            values={
                "tablets_initial_scale_factor": 1,
            }
        )
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, KEYSPACE, 1)

        session.execute("CREATE TABLE users (user_id uuid PRIMARY KEY, email text, uuids list<uuid>)")

        _id = uuid.uuid4()
        num_users = 100
        for i in range(num_users):
            user_uuid = uuid.uuid4()
            session.execute(f"INSERT INTO users (user_id, email) values ({user_uuid}, 'test@example.com')")
            session.execute(f"UPDATE users set uuids = [{_id}] where user_id = {user_uuid}")

        initial_users = list(session.execute(f"SELECT * from users where uuids contains {_id} ALLOW FILTERING"))
        assert num_users == len(initial_users), "Not all users were added to table"

        initial_sstables = self.flush("users")
        scrubbed_sstables = self.scrub("users")

        assert len(initial_sstables) == len(scrubbed_sstables), "Amount of sstables before and after scrub differs"

        users = list(session.execute(f"SELECT * from users where uuids contains {_id} ALLOW FILTERING"))
        assert initial_users == users, "List of users before and after scrub are different"

        scrubbed_sstables = self.scrub("users")

        assert len(initial_sstables) == len(scrubbed_sstables), "Amount of sstables before and after scrub differs"

        users = list(session.execute(f"SELECT * from users where uuids contains {_id} ALLOW FILTERING"))
        assert initial_users == users, "List of users before and after scrub are different"


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.next_gating
class TestScrub(TestHelper):
    """
    Generic tests for scrubbing
    """

    def create_users(self, session):
        columns = {"password": "varchar", "gender": "varchar", "session_token": "varchar", "state": "varchar", "birth_year": "bigint"}
        create_cf(session, "users", columns=columns)

    def update_users(self, session):
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user1', 'ch@ngem3a', 'f', 'TX', 1978)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user2', 'ch@ngem3b', 'm', 'CA', 1982)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user3', 'ch@ngem3c', 'f', 'TX', 1978)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user4', 'ch@ngem3d', 'm', 'CA', 1982)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user5', 'ch@ngem3e', 'f', 'TX', 1978)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user6', 'ch@ngem3f', 'm', 'CA', 1982)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user7', 'ch@ngem3g', 'f', 'TX', 1978)")
        session.execute("INSERT INTO users (KEY, password, gender, state, birth_year) VALUES ('user8', 'ch@ngem3h', 'm', 'CA', 1982)")

        session.execute("DELETE FROM users where KEY = 'user1'")
        session.execute("DELETE FROM users where KEY = 'user5'")
        session.execute("DELETE FROM users where KEY = 'user7'")

    def query_users(self, session):
        ret = list(session.execute("SELECT * FROM users"))
        assert len(ret) == 5, "Amount of users is different"
        return ret

    def test_nodetool_scrub(self):
        cluster = self.cluster
        cluster.set_configuration_options(
            values={
                "tablets_initial_scale_factor": 1,
            }
        )
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, KEYSPACE, 1)

        self.create_users(session)
        self.update_users(session)

        initial_users = self.query_users(session)
        initial_sstables = self.flush("users")
        scrubbed_sstables = self.scrub("users")

        assert len(initial_sstables) == len(scrubbed_sstables), "Amount of sstables before and after scrub differs"

        users = self.query_users(session)
        assert initial_users == users, "List of users before and after scrub are different"

        # Scrub and check sstables and data again
        scrubbed_sstables = self.scrub("users")
        assert len(initial_sstables) == len(scrubbed_sstables), "Amount of sstables before and after scrub differs"

        users = self.query_users(session)
        assert initial_users == users, "List of users before and after scrub are different"

        # Restart and check data again
        cluster.stop()
        cluster.start()

        session = self.patient_cql_connection(node1)
        session.execute("USE %s" % (KEYSPACE))

        users = self.query_users(session)
        assert initial_users == users, "List of users before and after scrub are different"

    def test_standalone_scrub(self):
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, KEYSPACE, 1)

        self.create_users(session)
        self.update_users(session)

        initial_users = self.query_users(session)
        initial_sstables = self.flush("users")

        cluster.stop()

        scrubbed_sstables = self.standalonescrub("users")
        assert len(initial_sstables) == len(scrubbed_sstables), "Amount of sstables before and after scrub differs"

        cluster.start()
        session = self.patient_cql_connection(node1)
        session.execute("USE %s" % (KEYSPACE))

        users = self.query_users(session)
        assert initial_users == users, "List of users before and after scrub are different"

    def test_standalone_scrub_essential_files_only(self):
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, KEYSPACE, 1)

        self.create_users(session)
        self.update_users(session)

        initial_users = self.query_users(session)
        initial_sstables = self.flush("users")

        cluster.stop()

        self.delete_non_essential_sstable_files("users")

        scrubbed_sstables = self.standalonescrub("users")
        assert len(initial_sstables) == len(scrubbed_sstables), "Amount of sstables before and after scrub differs"

        cluster.start()
        session = self.patient_cql_connection(node1)
        session.execute("USE %s" % (KEYSPACE))

        users = self.query_users(session)
        assert initial_users == users, "List of users before and after scrub are different"

    def test_scrub_with_udt(self):
        """
        @jira_ticket CASSANDRA-7665
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        session.execute("CREATE KEYSPACE test WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1 };")
        session.execute("use test;")
        session.execute("CREATE TYPE point_t (x double, y double);")

        node1.nodetool("scrub")
        time.sleep(2)
        match = node1.grep_log("org.apache.cassandra.serializers.MarshalException: Not enough bytes to read a set")
        assert len(match) == 0, f"{len(match)} is not equal 0"
