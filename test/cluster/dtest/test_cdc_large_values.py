import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from cassandra.cluster import Session, SimpleStatement
from ccmlib.scylla_node import ScyllaNode

from cdc_test import CDCInitializeHelper
from dtest_class import Tester, create_ks
from dtest_setup import DTestSetup
from dtest_setup_overrides import DTestSetupOverrides
from tools.marks import issue_open, unmark
from tools.misc import ImmutableMapping

MB = 1024 * 1024
LOGGER = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


@pytest.mark.dtest_full
@pytest.mark.single_node
@pytest.mark.scylla_cdc
class TestLargeColumnsWithCDC(Tester, CDCInitializeHelper):
    @pytest.fixture(scope="function", autouse=True)
    def fixture_dtest_setup_overrides(self, dtest_config):
        dtest_setup_overrides = DTestSetupOverrides()
        dtest_setup_overrides.cluster_options = ImmutableMapping(
            {"max_memory_for_unlimited_query_soft_limit": 20 * MB, "max_memory_for_unlimited_query": 20 * MB, "compaction_large_row_warning_threshold_mb": 20 * MB, "compaction_large_cell_warning_threshold_mb": 20 * MB}
        )
        return dtest_setup_overrides

    def prepare_cluster(self, n: int, custom_args: list[str] | None = None) -> tuple[ScyllaNode, Session]:
        self.populate_sequentially(n, custom_args=custom_args)
        node: ScyllaNode = self.cluster.nodelist()[0]
        session: Session = self.patient_cql_connection(node, request_timeout=120)
        create_ks(session, "ks", n)
        self.expected_errors = [
            # bad_alloc errors are expected in this test after
            # scylladb/scylladb@aab5954cfb65cc1bee645384a45a844f252f8d7e
            # Ref: https://github.com/scylladb/scylladb/issues/12600
            rf"[Ee]xception when communicating with ({node.address()}|{node.hostid()}), to read from [^\s]+: std::bad_alloc",
            rf"exception during mutation write to ({node.address()}|{node.hostid()}): std::bad_alloc",
            rf"exception during mutation write to ({node.address()}|{node.hostid()}): logalloc::bad_alloc[ :]+\(?failed to refill emergency reserve of [0-9]+ \(have [0-9]+ free segments\)\)?",
            rf"[Ee]xception when communicating with ({node.address()}|{node.hostid()}), to read from [^\s]+: utils::memory_limit_reached[ :]+\(?kill limit triggered on semaphore [^\s]+ by permit .*\)?",
        ]
        self.ignore_log_patterns.extend(self.expected_errors)
        return (node, session)

    @pytest.mark.parametrize("prepare_statements", [True, False], ids=["prepared_statements", "unprepared_statements"])
    def test_single_column_blob_max_size_with_cdc_preimage_full_postimage(self, prepare_statements: bool):
        """Test blob column with max size

        Run mutations with blob size close to limit and validate
        that there is no reactor stalls found
        """
        # This test uses very large blobs and stresses Scylla to the breaking
        # point with the default memory amount of 512M/shard, making this test flaky.
        # Increase memory amount to 1G/shard to make it more stable.
        # See https://github.com/scylladb/scylladb/issues/27539
        node, session = self.prepare_cluster(1, custom_args=["--smp=2", "--memory=2048M"])
        create_table_stmt = "CREATE TABLE ks.cf (pk bigint, ck bigint, v blob, PRIMARY KEY (pk, ck)) \
                             WITH cdc={'enabled': true, 'preimage': 'full', 'postimage': true}"
        session.execute(create_table_stmt)

        insert_value = b"1" * 7 * MB
        if prepare_statements:
            insert_statement = session.prepare("INSERT INTO ks.cf (pk, ck, v) VALUES (?, ?, ?)")
        else:
            insert_statement = SimpleStatement("INSERT INTO ks.cf (pk, ck, v) VALUES (%(pk)s, %(ck)s, %(v)s)")
        insert_parameters = [{"pk": i, "ck": j, "v": insert_value} for i in range(4) for j in range(3)]

        update_value = b"2" * 4 * MB
        if prepare_statements:
            update_statement = session.prepare("UPDATE ks.cf set v = ? where pk = ? and ck = ?")
        else:
            update_statement = SimpleStatement("UPDATE ks.cf set v = %(v)s where pk = %(pk)s and ck=%(ck)s")
        update_parameters = [{"pk": i, "ck": j, "v": update_value} for i in range(4) for j in range(3)]

        self.execute_case(
            node,
            session,
            insert_data={"insert_statement": insert_statement, "insert_parameters": insert_parameters},
            update_data={"update_statement": update_statement, "update_parameters": update_parameters},
            check_stalls=prepare_statements,
        )

    @pytest.mark.parametrize("prepare_statements", [True, False], ids=["prepared_statements", "unprepared_statements"])
    @unmark.next_gating  # https://github.com/scylladb/scylla-dtest/issues/3354
    def test_row_with_several_columns_of_blobs_with_cdc_preimage_full_postimage(self, prepare_statements: bool, fixture_dtest_setup: DTestSetup):
        """test row with several columns of blob type

        Construct row with several columns of blob type and populate
        each column with large blob size, so the total size of mutation
        was close to limit 16MB

        Because cdc feature is enabled the base row size should be ~8MB

        """
        if not prepare_statements:
            fixture_dtest_setup.ignore_log_patterns += ["seastar_memory - oversized allocation"]

        NUM_CELLS = 5
        VALUE = b"1" * 1 * MB
        node, session = self.prepare_cluster(1)

        cells = ", ".join([f"v_{i} blob" for i in range(NUM_CELLS)])
        session.execute(
            f"CREATE TABLE IF NOT EXISTS ks.cf (pk bigint, ck bigint, {cells}, PRIMARY KEY (pk, ck)) \
                WITH cdc={{'enabled': true, 'preimage': 'full', 'postimage': true}}"
        )

        insert_cells_names = ", ".join([f"v_{i}" for i in range(NUM_CELLS)])
        insert_cells_values = ", ".join([f"%(v_{i})s" for i in range(NUM_CELLS)])
        insert_cells_place_holders = ", ".join(["?"] * NUM_CELLS)
        if prepare_statements:
            insert_statement = session.prepare(
                f"INSERT INTO ks.cf (pk, ck, {insert_cells_names}) \
                                               VALUES (?, ?, {insert_cells_place_holders})"
            )
        else:
            insert_statement = SimpleStatement(
                f"INSERT INTO ks.cf (pk, ck, {insert_cells_names}) \
                                               VALUES (%(pk)s, %(ck)s, {insert_cells_values})"
            )
        blob_columns = {f"v_{i}": VALUE for i in range(NUM_CELLS)}
        insert_parameters = [{"pk": i, "ck": j, **blob_columns} for i in range(4) for j in range(3)]

        if prepare_statements:
            update_cells = ", ".join([f"v_{i} = ?" for i in range(NUM_CELLS)])
            update_statement = session.prepare(f"UPDATE ks.cf SET {update_cells} WHERE pk = ? and ck = ?")
        else:
            update_cells = ", ".join([f"v_{i} = %(v_{i})s" for i in range(NUM_CELLS)])
            update_statement = SimpleStatement(f"UPDATE ks.cf SET {update_cells} WHERE pk = %(pk)s and ck = %(ck)s")
        update_parameters = [{"pk": i, "ck": j, **blob_columns} for i in range(4) for j in range(3)]

        self.execute_case(
            node,
            session,
            insert_data={"insert_statement": insert_statement, "insert_parameters": insert_parameters},
            update_data={"update_statement": update_statement, "update_parameters": update_parameters},
            check_stalls=prepare_statements,
            check_oversize_allocation=prepare_statements,
        )

    @pytest.mark.parametrize("prepare_statements", [True, False], ids=["prepared_statements", "unprepared_statements"])
    def test_large_blob_in_map_delta_preimage_full(self, prepare_statements: bool):
        """test map type with large blob

        Test column with map type where one of the field
        is blob. Populate row with blob size close to mutation
        limit 16MB.
        """

        node, session = self.prepare_cluster(1)
        session.execute(
            "CREATE TABLE IF NOT EXISTS ks.cf (pk bigint, ck bigint, v map<text,blob>, PRIMARY KEY (pk, ck)) \
                WITH cdc={'enabled': true, 'preimage': 'full', 'postimage': true}"
        )

        insert_value = b"1" * 1 * MB
        if prepare_statements:
            insert_statement = session.prepare("INSERT INTO ks.cf (pk, ck, v) VALUES (?, ?, ?)")
        else:
            insert_statement = SimpleStatement("INSERT INTO ks.cf (pk, ck, v) VALUES (%(pk)s, %(ck)s, %(v)s)")
        insert_parameters = [{"pk": i, "ck": j, "v": {"key": insert_value}} for i in range(4) for j in range(3)]

        update_value = b"2" * 1 * MB
        if prepare_statements:
            update_statement = session.prepare("UPDATE ks.cf SET v = v + ? WHERE pk=? and ck=?")
        else:
            update_statement = SimpleStatement("UPDATE ks.cf SET v = v + %(v)s WHERE pk=%(pk)s and ck=%(ck)s")
        update_parameters = [{"pk": i, "ck": j, "v": {f"key{k}": update_value}} for i in range(4) for j in range(3) for k in range(5)]

        self.execute_case(
            node,
            session,
            insert_data={"insert_statement": insert_statement, "insert_parameters": insert_parameters},
            update_data={"update_statement": update_statement, "update_parameters": update_parameters},
            check_stalls=prepare_statements,
        )

    def execute_case(  # noqa: PLR0913
        self,
        node: ScyllaNode,
        session: Session,
        insert_data: dict[str, Any],
        update_data: dict[str, Any],
        check_stalls: bool = False,
        check_oversize_allocation: bool = False,
    ):
        select_statement = SimpleStatement("SELECT * FROM ks.cf WHERE pk = %(pk)s and ck = %(ck)s")
        select_parameters = [{"pk": i, "ck": j} for i in range(4) for j in range(3)]

        cdc_select_statement = SimpleStatement("SELECT * FROM ks.cf_scylla_cdc_log LIMIT 10")

        mark = node.mark_log()
        LOGGER.debug(f"Insert {insert_data['insert_statement']}")
        self.execute_query(session, insert_data["insert_statement"], insert_data["insert_parameters"])
        if check_stalls:
            found = node.grep_log("Reactor stall", from_mark=mark)
            assert not found, f"{found}"

        mark = node.mark_log()

        futures = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            future = executor.submit(self.execute_query_during_timeout, session, update_data["update_statement"], update_data["update_parameters"], 60, 0.5)
            futures.append(future)
            future = executor.submit(self.execute_query_during_timeout, session, select_statement, select_parameters, 60, 0.5)
            futures.append(future)
            future = executor.submit(self.execute_query_during_timeout, session=session, statement=cdc_select_statement, duration=60, delay=0.5)
            futures.append(future)

            for future in futures:
                exc = future.exception()
                if exc:
                    print(str(exc))
                    raise exc

        if check_stalls:
            found = node.grep_log("Reactor stall", from_mark=mark)
            assert not found, f"Next Reactor stalls were found: {found}"

        if check_oversize_allocation:
            found = node.grep_log("oversized allocation", from_mark=mark)
            assert not found, f"Next oversized allocation were found: {found}"

        found = self.check_errors(node, exclude_errors=self.expected_errors)
        assert not found, f"Next errors were found: {found}"

    @staticmethod
    def execute_query(session: Session, statement: SimpleStatement, parameters: list[Any], delay: int | float = 0.0, raise_exception: bool = False):
        for param in parameters:
            try:
                session.execute(statement, param)
            except Exception as details:
                LOGGER.error("Error: %s", details)
                if raise_exception:
                    raise details

            time.sleep(delay)

    @staticmethod
    def execute_query_during_timeout(  # noqa: PLR0913
        session: Session,
        statement: SimpleStatement,
        parameters: list[Any] | None = None,
        duration: float | None = None,
        delay: int | float = 0.0,
        raise_exception: bool = False,
    ):
        start_timestampt = current_time = time.time()
        if not parameters:
            parameters = [None]
        while current_time < start_timestampt + duration:
            for param in parameters:
                try:
                    session.execute(statement, param)
                except Exception as details:
                    statement_str = str(statement)
                    if len(statement_str) > 100:
                        # Avoid dumping statements with multi MiB blobs in them into the logs
                        statement_str = statement_str[:100] + "..."
                    LOGGER.warning(f"execute_query_during_timeout(): failed to execute statement {statement_str}: {details}")
                    if raise_exception:
                        raise details
                time.sleep(delay)
            current_time = time.time()
