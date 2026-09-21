import logging
from concurrent.futures import ThreadPoolExecutor
from random import randint
from threading import Event
from time import sleep, time

import pytest
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement
from psutil import cpu_count

from dtest_class import Tester, create_ks, retry_till_success
from tools.cluster_topology import generate_rack_topology_based_rf
from tools.marks import issue_open, with_feature

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating

KEYSPACE = "lwt_load_ks"

# TODO:
#   - parametrize table and column names
#   - configurable table schema (s)
#   - configurable prelude and epilogue
#

# TODO: fix for case of selecting only 1 column

# ruff: noqa: PLR0913


def listify(item):
    """
    listify a query result consisting of user types

    returns nested arrays representing user type ordering
    """
    decoded = []

    if isinstance(item, tuple | list):
        if len(item) == 1:
            item = item[0]
        nested = []
        for i in item:
            nested.extend(listify(i))
        decoded.append(nested)
    else:
        decoded.append(item)

    return decoded


class InsertRows:
    """Insert rows in table"""

    def __init__(self, name=None, wait_for=None, node_idx=0, nrows=1000, start_value=0):
        self.name = name  # Name of this action
        self.wait_for = wait_for  # Start after dependency finished
        self.node_idx = node_idx  # Node for this action
        self.nrows = nrows  # How many rows to insert
        self.start_value = start_value  # Starting value

    def run(self, session, stop, start_event, end_event, node):
        if start_event:
            start_event.wait()  # Wait for other action to signal done

        session.execute("TRUNCATE TABLE table1")
        insert_cql = "INSERT INTO table1 (pk, ck, v, int_col) VALUES (?, ?, ?, ?)"
        insert_stmt = session.prepare(insert_cql)
        for i in range(self.start_value, self.start_value + self.nrows):
            session.execute(insert_stmt, (i, i, i, i))

        if end_event:
            end_event.set()


class ReadRows:
    """Read a range of rows and see if they changed"""

    def __init__(
        self,
        name=None,
        wait_for=None,
        end=None,
        node_idx=0,
        loop_delay=0,
        row_start=0,
        row_end=10,
    ):
        self.name = name  # Name of this action
        self.wait_for = wait_for  # Start after dependency finished
        self.end = end  # Stop after seconds
        self.node_idx = node_idx  # Node for this action
        self.loop_delay = loop_delay  # Sleep time between runs
        self.row_start = row_start  # Range start for rows selected
        self.row_end = row_end  # Range end   for rows selected

    def run(self, session, stop, start_event, end_event, node):
        if start_event:
            start_event.wait()  # Wait for other action to signal done

        select_cql = "SELECT pk, v FROM table1 WHERE pk > %i AND pk < %i ALLOW FILTERING" % (self.row_start, self.row_end)
        select_stmt = session.prepare(select_cql)
        select_stmt.consistency_level = ConsistencyLevel.ALL
        rows_expected = listify(sorted(session.execute(select_stmt).current_rows))

        end_time = time() + self.end if self.end else None

        while not stop.is_set() and (not end_time or time() < end_time):
            rows = listify(sorted(session.execute(select_stmt).current_rows))
            assert rows == rows_expected
            if self.loop_delay:
                sleep(self.loop_delay)

        if end_event:
            end_event.set()


class LWTLoad:
    def __init__(
        self,
        name=None,
        wait_for=None,
        end=None,
        node_idx=0,
        row_start=0,
        row_end=10,
    ):
        self.name = name  # Name of this action
        self.wait_for = wait_for  # Start after dependency finished
        self.end = end  # Stop after seconds
        self.node_idx = node_idx  # Node for this action
        self.row_start = row_start  # Range start for rows modified
        self.row_end = row_end  # Range end   for rows modified

    def run(self, session, stop, start_event, end_event, node):
        insert_stmt = session.prepare("INSERT INTO table1 (pk, ck, v) VALUES (:pk, :ck, :v) IF NOT EXISTS")
        update_stmt = session.prepare("UPDATE table1 SET v = :v WHERE pk = :pk AND ck = :ck IF v > 50 AND v < 1000")
        insert_stmt.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
        update_stmt.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL

        end_time = time() + self.end if self.end else None

        if start_event:
            start_event.wait()  # Wait for other action to signal done

        val = 0
        while not stop.is_set() and (not end_time or time() < end_time):
            pk = ck = randint(self.row_start, self.row_end)
            session.execute(insert_stmt, (pk, ck, val))
            session.execute(update_stmt, (val, pk, ck))
            val += 1

        if end_event:
            end_event.set()


class LWTLoadCheck:
    """Perform LWT inserts and check results"""

    def __init__(
        self,
        name=None,
        wait_for=None,
        end=None,
        node_idx=0,
        row_start=1000,
        row_end=10000,
    ):
        self.name = name  # Name of this action
        self.wait_for = wait_for  # Start after dependency finished
        self.end = end  # Stop after seconds
        self.node_idx = node_idx  # Node for this action
        self.row_start = row_start  # Range start for rows modified
        self.row_end = row_end  # Range end   for rows modified

    def run(self, session, stop, start_event, end_event, node):
        insert_stmt = session.prepare("INSERT INTO table1 (pk, ck, v) VALUES (:pk, :ck, :v)  IF NOT EXISTS")
        insert_stmt.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
        insert_stmt.consistency_level = ConsistencyLevel.LOCAL_QUORUM

        logger.debug("Producing LWT load on the cluster")
        end_time = time() + self.end if self.end else None

        if start_event:
            start_event.wait()  # Wait for other action to signal done

        i = self.row_start
        fails = 0
        while i <= self.row_end:
            if stop.is_set() or (end_time and time() > end_time):
                logger.debug("LWTLoadCheck premature finish")
                break
            result = session.execute(insert_stmt, (i, i, i))
            if result.current_rows[0].applied:
                i += 1  # success
            else:
                fails += 1  # retry

        logger.debug("LWTLoadCheck done inserting")
        select_cql = """SELECT sum(v) FROM table1
                        WHERE pk >= %i AND pk <= %i
                        ALLOW FILTERING""" % (self.row_start, self.row_end)
        select_stmt = session.prepare(select_cql)
        select_stmt.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        result = session.execute(select_stmt).current_rows[0][0]
        n = i - self.row_start
        expected = (n / 2) * (self.row_start + i - 1)  # i = last written+1
        assert result == expected

        logger.debug("Finished LWT stress workload")

        if end_event:
            end_event.set()


class DropAddColumn:
    """Alter table by removing and adding back again a column
    in a way that doesn"t render the queries incompatible"""

    def __init__(self, name=None, wait_for=None, node_idx=0, inter_delay=0):
        self.name = name  # Name of this action
        self.wait_for = wait_for  # Start after dependency finished
        self.node_idx = node_idx  # Node for this action
        self.inter_delay = inter_delay  # Sleep time between drop and add

    def run(self, session, stop, start_event, end_event, node):
        if start_event:
            start_event.wait()  # Wait for other action to signal done

        if self.inter_delay:
            session.execute("ALTER TABLE table1 DROP int_col")
            sleep(self.inter_delay)
            session.execute("ALTER TABLE table1 ADD int_col bigint")
        else:
            # NOTE: for #6174 doing condition check between statements
            #       doesn't show the bug
            session.execute("ALTER TABLE table1 DROP int_col")
            session.execute("ALTER TABLE table1 ADD  int_col bigint")

        if end_event:
            end_event.set()


class AlterColumnType:
    """Alter column v type while used in another query,
    in a way that doesn"t render the queries incompatible"""

    def __init__(self, name=None, wait_for=None, node_idx=0):
        self.name = name  # Name of this action
        self.wait_for = wait_for  # Start after dependency finished
        self.node_idx = node_idx  # Node for this action

    def run(self, session, stop, start_event, end_event, node):
        if start_event:
            start_event.wait()  # Wait for other action to signal done

        logger.debug("Alter column v to varint")
        session.execute("ALTER TABLE table1 ALTER v TYPE varint")

        if end_event:
            end_event.set()


class DeleteRows:
    """Delete rows from table"""

    def __init__(
        self,
        name=None,
        wait_for=None,
        node_idx=0,
        row_start=11,
        row_end=1000,
        lwt=True,
    ):
        """Params
        row_start: range start
        row_end:   range end
        lwt:       enable LWT
        """
        self.name = name  # Name of this action
        self.wait_for = wait_for  # Start after dependency finished
        self.node_idx = node_idx  # Node for this action
        self.row_start = row_start  # Range start of rows deleted
        self.row_end = row_end  # Range end   of rows deleted
        self.lwt = "IF EXISTS" if lwt else ""  # LWT condition

    def run(self, session, stop, start_event, end_event, node):
        if start_event:
            start_event.wait()  # Wait for other action to signal done

        for i in range(self.row_start, self.row_end):
            # Trigger #6174 race condition with prepare and then execute
            delete_cql = "DELETE FROM table1 WHERE pk = %i %s" % (i, self.lwt)
            delete_stmt = session.prepare(delete_cql)
            delete_stmt.consistency_level = ConsistencyLevel.ALL
            session.execute(delete_stmt)
            if stop:
                break

        if end_event:
            end_event.set()


class Truncate:
    """Truncate table
    NOTE: should be the only operation running
    """

    def __init__(self, name=None, wait_for=None, node_idx=0):
        self.name = name  # Name of this action
        self.wait_for = wait_for  # Start after dependency finished
        self.node_idx = node_idx  # Node for this action

    def run(self, session, stop, start_event, end_event, node):
        if start_event:
            start_event.wait()  # Wait for other action to signal done

        session.execute("TRUNCATE TABLE table1")

        if end_event:
            end_event.set()


class BatchInserts:
    """Batch reads"""

    def __init__(
        self,
        name=None,
        wait_for=None,
        node_idx=0,
        loops=100,
        row_start=0,
        row_end=10,
        lwt=True,
        loop_delay=0.5,
    ):
        self.name = name  # Name of this action
        self.wait_for = wait_for  # Start after dependency finished
        self.node_idx = node_idx  # Node for this action
        self.loops = loops  # How many loops
        self.row_start = row_start  # Range start of rows deleted
        self.row_end = row_end  # Range end   of rows deleted
        self.loop_delay = loop_delay  # Sleep time between loops

    def run(self, session, stop, start_event, end_event, node):
        if start_event:
            start_event.wait()  # Wait for other action to signal done

        for _ in range(self.loops):
            batch = BatchStatement()
            insert_cql = "INSERT INTO table1 (pk, ck, v, int_col) VALUES (?, ?, ?, ?)"
            insert_stmt = session.prepare(insert_cql)
            for i in range(self.row_start, self.row_end):
                session.execute(insert_stmt, (i, i, i, i))
            session.execute(batch)
            sleep(self.loop_delay)
            if stop:
                break

        if end_event:
            end_event.set()


class IndexDropAdd:
    """Drop an existing index and add it again"""

    def __init__(self, name=None, wait_for=None, node_idx=0, inter_delay=0):
        self.name = name  # Name of this action
        self.wait_for = wait_for  # Start after dependency finished
        self.node_idx = node_idx  # Node for this action
        self.inter_delay = inter_delay  # Sleep time between drop and add

    def run(self, session, stop, start_event, end_event, node):
        if start_event:
            start_event.wait()  # Wait for other action to signal done

        session.execute("DROP INDEX table1_v_idx")
        sleep(self.inter_delay)
        session.execute("CREATE INDEX table1_v_idx ON table1 (v)")

        if end_event:
            end_event.set()


class MaterializedView:
    """Drop an existing index and add it again"""

    def __init__(
        self,
        name=None,
        wait_for=None,
        node_idx=0,
        row_max=10,
        loop_delay=None,
    ):
        self.name = name  # Name of this action
        self.wait_for = wait_for  # Start after dependency finished
        self.node_idx = node_idx  # Node for this action
        self.row_max = row_max  # Read x rows from top
        self.loop_delay = loop_delay  # Delay across loops of reads

    def run(self, session, stop, start_event, end_event, node):
        if start_event:
            start_event.wait()  # Wait for other action to signal done

        # Create view *ascending* so top rows are untouched
        session.execute(
            """
                CREATE MATERIALIZED VIEW table1_top_v_view AS
                    SELECT pk, ck, v FROM table1
                    WHERE ck IS NOT NULL
                    PRIMARY KEY (pk, ck)
                    WITH CLUSTERING ORDER BY (ck ASC) AND synchronous_updates = true
                """
        )

        def wait_for_mv_created():
            output = node.nodetool(f"viewbuildstatus {KEYSPACE} table1_top_v_view")
            assert "has finished building" in output[0]

        retry_till_success(wait_for_mv_created)

        select_cql = "SELECT pk, ck, v FROM table1_top_v_view LIMIT %i" % (self.row_max)
        select_stmt = session.prepare(select_cql)
        select_stmt.consistency_level = ConsistencyLevel.ALL

        def wait_for_row_max():
            assert len(session.execute(select_stmt).current_rows) == self.row_max

        retry_till_success(wait_for_row_max)

        rows_expected = listify(sorted(session.execute(select_stmt).current_rows))

        while not stop.is_set():
            rows = listify(sorted(session.execute(select_stmt).current_rows))
            assert rows == rows_expected
            if self.loop_delay:
                sleep(self.loop_delay)

        # NOTE: don't drop materialized view to avoid concurrency issues

        if end_event:
            end_event.set()


@pytest.mark.dtest_full
@pytest.mark.lwt
class TestLWTSchemaModification(Tester):
    """
    Tests LWT schema change under load
    """

    def _setup(self, nodes=3, rf=3, jvm_args=None):
        """Assorted actions in preparation for a test case"""

        # This error might happen on tearDown, ignore it for now
        self.ignore_log_patterns.extend(["exception during mutation write .*schema_mismatch_error", "Error applying view update to .*: data_dictionary::no_such_column_family"])

        cluster = self.cluster

        rack_layout = generate_rack_topology_based_rf(nodes, rf)
        topology_layout = {"dc1": rack_layout}

        cluster.populate(topology_layout).start(wait_for_binary_proto=True, jvm_args=jvm_args)
        node = cluster.nodelist()[0]

        session = self.patient_cql_connection(node)
        create_ks(session=session, name=KEYSPACE, rf=rf)
        return cluster

    def _case_prologue(self, nrows):
        """Prepare for round"""
        logger.debug("Preparing {}.{} with index {}".format(KEYSPACE, "table1", "table1_v_idx"))
        session = self.patient_cql_connection(self.cluster.nodelist()[0])
        session.execute("USE " + KEYSPACE)
        session.execute("CREATE TABLE table1 (pk int, ck int, v int, int_col int, PRIMARY KEY (pk, ck))")
        session.execute("CREATE INDEX table1_v_idx ON table1 (v)")
        insert_cql = "INSERT INTO table1 (pk, ck, v, int_col) VALUES (?, ?, ?, ?)"
        insert_stmt = session.prepare(insert_cql)
        logger.debug("Inserting {} rows into {}.{}".format(nrows, KEYSPACE, "table1"))
        for i in range(nrows):
            session.execute(insert_stmt, (i, i, i, i))

    def _action_thread(self, action, stop, start_event, end_event):
        node = self.cluster.nodelist()[action.node_idx]
        session = self.patient_cql_connection(node, request_timeout=1000)
        session.execute("USE " + KEYSPACE)

        action.run(session, stop, start_event, end_event, node)  # Hand over control to action

    def _test_combine(
        self,
        actions,
        smp=4,
        nodes=4,
        rf=3,
        nrows=1000,
        loops=1,
        run_s=10,
    ):
        """Remove and add a column while table is queried
        actions:          iterable with action objects
        smp:              cores
        nrows:            total rows in table
        run_s:            test run seconds
        """

        actual_smp = min(smp, cpu_count())
        if actual_smp != smp:
            logger.debug(f"smp limited to {actual_smp}")

        cluster = self._setup(nodes=nodes, rf=rf, jvm_args=["--smp", str(actual_smp)])
        stop = Event()

        self._case_prologue(nrows)
        for i in range(loops):
            logger.debug(f"Staring loop {i}/{loops}")
            # For each action create a thread
            threads = []
            action_names = [action.name for action in actions if action.name]
            assert len(action_names) == len(set(action_names))  # No duplicate action names
            action_names = set(action_names)
            # Actions with dependencies
            action_deps = {action.wait_for for action in actions if action.wait_for}
            assert len(action_deps - action_names) == 0  # All dependent actions present and named
            # Each action signals when it's done, other actions can wait for that to start
            action_done = {action.name: Event() for action in actions if action.name in action_deps}

            with ThreadPoolExecutor(max_workers=len(actions)) as pool:
                for action in actions:
                    start_event = action_done.get(action.wait_for, None)  # Action to wait for
                    end_event = action_done[action.name] if action.name and action.name in action_deps else None
                    threads.append(pool.submit(self._action_thread, action, stop, start_event, end_event))

                sleep(run_s)  # Test duration
                stop.set()

                # Wait for worker threads to complete
                for thread in threads:
                    thread.result()

            # Assert that all nodes in the cluster are alive
            for node in cluster.nodelist():
                assert node.is_live()

        cluster.stop()

    @pytest.mark.skip("issue #6151    alter column type vs reads")
    def test_table_alter_col_type(self):
        self._test_combine([ReadRows(row_start=0, row_end=9), AlterColumnType()], run_s=10)

    @pytest.mark.skip("issue #6174  add/remove column changing type vs LWT deletes")
    def test_table_alter_delete(self):
        """Table alter test"""
        self._test_combine([DropAddColumn(), DeleteRows(row_start=1, row_end=1000, lwt=True)], loops=3, run_s=10)

    @pytest.mark.skip("issue #6185 alter columns in parallel bug")
    def test_schema_both(self):
        """Alter two columns of same table.
        change type on one and remove/add on the second one"""
        self._test_combine([DropAddColumn(), AlterColumnType()], run_s=10)

    @pytest.mark.skip("issue #6151    alter column type vs reads")
    def test_all(self):
        self._test_combine(
            [
                ReadRows(row_start=0, row_end=99),  # NOTE: change to 9 for more fun
                LWTLoad(),
                DropAddColumn(inter_delay=0.2),
                AlterColumnType(),
                DeleteRows(row_start=100, row_end=1000, lwt=True),
            ],
            loops=2,
            run_s=10,
        )

    # Test had history of timing out in debug, see: https://github.com/scylladb/scylla-dtest/issues/3275
    @pytest.mark.scylla_mode("!debug")
    def test_lwt_load(self):
        smp = 8 if self.cluster.scylla_mode != "debug" else 4
        nodes = 8 if self.cluster.scylla_mode != "debug" else 4
        loops = 4 if self.cluster.scylla_mode != "debug" else 2
        self._test_combine([ReadRows(row_start=0, row_end=1000), LWTLoad(row_start=1001, row_end=9999)], smp=smp, nodes=nodes, nrows=10000, loops=loops, run_s=30)

    # Test had history of timing out in debug, see: https://github.com/scylladb/scylla-dtest/issues/3275
    @pytest.mark.scylla_mode("!debug")
    def test_lwt_batch_insert(self):
        smp = 8 if self.cluster.scylla_mode != "debug" else 4
        nodes = 8 if self.cluster.scylla_mode != "debug" else 4
        self._test_combine([LWTLoad(end=1), BatchInserts(node_idx=1)], smp=smp, nodes=nodes, loops=1, run_s=10)

    @pytest.mark.next_gating
    def test_index_drop_add(self):
        loops = 4 if self.cluster.scylla_mode != "debug" else 2
        self._test_combine([LWTLoad(row_start=1001, row_end=9999), ReadRows(row_end=1000), IndexDropAdd(inter_delay=0.5)], nrows=10000, loops=loops, run_s=10)

    def test_materialized_view(self):
        self._test_combine([LWTLoad(row_start=1001, row_end=9999), MaterializedView(row_max=1000)], nrows=10000, loops=1, run_s=10)

    # Test had history of timing out in debug, see: https://github.com/scylladb/scylla-dtest/issues/3275
    @pytest.mark.scylla_mode("!debug")
    def test_lwt_load_check(self):
        smp = 8 if self.cluster.scylla_mode != "debug" else 4
        nodes = 8 if self.cluster.scylla_mode != "debug" else 4
        self._test_combine([LWTLoad(row_start=1, row_end=99), LWTLoadCheck(row_start=100, row_end=99999999)], smp=smp, nodes=nodes, nrows=0, loops=1, run_s=30, rf=3)
