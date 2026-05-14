#
# Copyright (C) 2014-present The Apache Software Foundation
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import uuid
from collections import namedtuple
from datetime import datetime
from decimal import Decimal

import pytest
from cassandra.util import Date, SortedSet, Time, uuid_from_time

from dtest_class import Tester, create_ks
from tools.assertions import assert_one_prepared
from tools.data import prepare_statement

logger = logging.getLogger(__name__)


@pytest.mark.single_node
class TestCQL(Tester):
    def prepare(self, options=None):
        if options is None:
            options = {}
        cluster = self.cluster

        if options:
            cluster.set_configuration_options(values=options)

        cluster.populate(1).start(wait_other_notice=True, wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        session = self.patient_cql_connection(node1)
        create_ks(session, "ks", 1)
        return session

    def test_batch_preparation(self):
        """Test preparation of batch statement (#4202)"""
        session = self.prepare()

        session.execute(
            """
            CREATE TABLE cf (
                k varchar PRIMARY KEY,
                c int,
            )
        """
        )

        query = "BEGIN BATCH INSERT INTO cf (k, c) VALUES (?, ?); APPLY BATCH"
        pq = session.prepare(query)

        session.execute(pq, ["foo", 4])

    def _lwt_create_table(self, session, column_type, test_data=None, table_name=None):
        """Prepare table for test: create table and populate with test data"""

        if test_data is None:
            test_data = {}
        if not table_name:
            # sanitize name in case we have a collection column type there
            sanitized_column_type = column_type.replace("<", "_").replace(">", "_")
            table_name = sanitized_column_type + "_update_test_table"

        session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                k int PRIMARY KEY,
                value {column_type}
            )
        """
        )

        if test_data:
            insert_stmt = session.prepare(
                f"""
                INSERT INTO {table_name} (k, value) VALUES (?, ?)
            """
            )

            for k, v in test_data:
                session.execute(insert_stmt, [k, v])

        return table_name

    def _lwt_execute_single_type_update_case(self, session, column_type, test_params):
        logger.debug(f"Executing a single LWT Update test for type {column_type}")

        test_cases = test_params["test_cases"]
        default_upd_v = test_params["default_update_value"]
        raw_init_values = []
        args_per_test_case = []

        is_tuple = "collection_type" in test_params and test_params["collection_type"] == "tuple"

        for entry in test_cases:
            init_val = entry["init_val"]
            upd_v = default_upd_v if "update_value" not in entry else entry["update_value"]

            if is_tuple:
                if isinstance(init_val, list | SortedSet):
                    init_val = tuple(init_val)
                if isinstance(upd_v, list):
                    upd_v = tuple(upd_v)

            for pattern in entry["update_patterns"]:
                if isinstance(pattern, dict) and "coll_type_filter" in pattern and "collection_type" in test_params and test_params["collection_type"] not in pattern["coll_type_filter"]:
                    continue
                raw_init_values.append(init_val)
                args_per_test_case.append((init_val, upd_v, pattern))

        table_name = self._lwt_create_table(session, column_type, enumerate(raw_init_values))

        for key, entry in enumerate(args_per_test_case):
            init_val, upd_v, pattern_entry = entry

            query_args = {"upd_v": upd_v}

            if not isinstance(pattern_entry, dict):
                update_pattern = pattern_entry
                query_args.update({"v": init_val})
            else:
                update_pattern = pattern_entry["p"]
                # filter out pattern string and supply the remaining keys as arguments to the query
                #
                # `copy` is needed because `pop` removes the element from the
                # original map, so consequent test runs observe the same `pattern_entry`
                # but without a `p` pattern, which is undesired behavior
                args_from_pattern = pattern_entry.copy()
                args_from_pattern.pop("p")
                query_args.update(args_from_pattern)

            update_query = f"UPDATE {table_name} SET value=:upd_v WHERE k={key} IF {update_pattern}"
            stmt = prepare_statement(session, update_query)

            logger.debug(f"Asserting results from two consecutive identical CAS statements with the following query args: {query_args}")
            assert_one_prepared(session, stmt, [True, init_val], query_args)
            assert_one_prepared(session, stmt, [False, upd_v], query_args)

    def test_lwt_update_prepared(self):
        """
        Test that the most common IF condition patterns with parameter markers work as expected
        for prepared LWT statements.

        This test case is composed of many sub-testcases to handle column types for each primitive type.

        Each nested test case executes a series of tests for each IF pattern with some supplied values,
        usually the following combinations:
        * null values
        * empty values
        * zero values
        * min/max values
        """

        def standard_test_case(init_val):
            update_patterns = ["value=:v", "value in (:v)"]

            # Skip the tests that test 'value IN :v` with `:v = [null]`.
            # There's a bug in the python driver which causes them to fail.
            # Because of the bug the driver sends a list of [empty] instead of [null],
            # and the LWT update isn't applied.
            # https://github.com/scylladb/python-driver/issues/201
            if init_val is not None:
                update_patterns.append({"p": "value in :v", "v": (init_val,)})

            return {"init_val": init_val, "update_patterns": update_patterns}

        PRIMITIVE_TYPES_MAP = {
            "boolean": {"test_cases": [standard_test_case(None), standard_test_case(False)], "default_update_value": True},
            "blob": {"test_cases": [standard_test_case(None), standard_test_case(b""), standard_test_case(b"\x00\x00\x00\x00")], "default_update_value": b"\x00\x00\x00\x01"},
            "ascii": {"test_cases": [standard_test_case(None), standard_test_case(""), standard_test_case("abc")], "default_update_value": "def"},
            "decimal": {"test_cases": [standard_test_case(None), standard_test_case(Decimal("1.2349823094823948209384209348"))], "default_update_value": Decimal("2.3495083459083095483409534534")},
            "double": {
                "test_cases": [standard_test_case(None), standard_test_case(0.0), standard_test_case(2.2250738585072014e-308), standard_test_case(1.7976931348623157e308), standard_test_case(-1.7976931348623157e308)],
                "default_update_value": 1.0,
            },
            "float": {
                "test_cases": [standard_test_case(None), standard_test_case(0.0), standard_test_case(1.1754943508222875e-38), standard_test_case(3.4028234663852886e38), standard_test_case(-3.4028234663852886e38)],
                "default_update_value": 1.0,
            },
            "text": {"test_cases": [standard_test_case(None), standard_test_case(""), standard_test_case("abc")], "default_update_value": "def"},
            "varchar": {"test_cases": [standard_test_case(None), standard_test_case(""), standard_test_case("abc")], "default_update_value": "def"},
            "bigint": {"test_cases": [standard_test_case(None), standard_test_case(0), standard_test_case(2**63 - 1), standard_test_case(-(2**63))], "default_update_value": 1},
            "int": {"test_cases": [standard_test_case(None), standard_test_case(0), standard_test_case(2**31 - 1), standard_test_case(-(2**31))], "default_update_value": 1},
            "smallint": {"test_cases": [standard_test_case(None), standard_test_case(0), standard_test_case(2**15 - 1), standard_test_case(-(2**15))], "default_update_value": 1},
            "tinyint": {"test_cases": [standard_test_case(None), standard_test_case(0), standard_test_case(2**7 - 1), standard_test_case(-(2**7))], "default_update_value": 1},
            "varint": {"test_cases": [standard_test_case(None), standard_test_case(0), standard_test_case(2**128), standard_test_case(-(2**128))], "default_update_value": 1},
            "timestamp": {
                "test_cases": [standard_test_case(None), standard_test_case(datetime(1970, 1, 1, 0, 0)), standard_test_case(datetime.strptime("2020-01-02 14:13:12.001", "%Y-%m-%d %H:%M:%S.%f"))],
                "default_update_value": datetime.strptime("2021-02-03 15:14:13.002", "%Y-%m-%d %H:%M:%S.%f"),
            },
            "date": {"test_cases": [standard_test_case(None), standard_test_case(Date(0)), standard_test_case(Date("2020-1-2"))], "default_update_value": Date("2021-2-3")},
            "time": {"test_cases": [standard_test_case(None), standard_test_case(Time(0)), standard_test_case(Time("12:13:14.001"))], "default_update_value": Time("13:14:15.002")},
            "timeuuid": {"test_cases": [standard_test_case(None), standard_test_case(uuid_from_time(datetime(2020, 1, 2, 3, 4, 5, 0)))], "default_update_value": uuid_from_time(datetime(2021, 2, 3, 4, 5, 6, 1))},
            "uuid": {"test_cases": [standard_test_case(None), standard_test_case(uuid.UUID(bytes=b"\x00" * 16)), standard_test_case(uuid.uuid4())], "default_update_value": uuid.uuid4()},
        }

        session = self.prepare()

        for column_type, test_data in PRIMITIVE_TYPES_MAP.items():
            self._lwt_execute_single_type_update_case(session, column_type, test_data)

    @staticmethod
    def _build_collection_typename(column_type, is_frozen, collection_type):
        ret = f"{collection_type}<{column_type}>"
        if is_frozen:
            ret = "frozen<" + ret + ">"
        return ret

    def test_lwt_update_prepared_listlike_and_tuples(self):
        """
        Test that the most common IF condition patterns with parameter markers work as expected
        for prepared LWT statements.

        This test case is composed of many sub-testcases to handle column types for each primitive type.
        Tested column types are set<T>, list<T>, tuple<T> as well as their frozen<T> companions.

        Each nested test case executes a series of tests for each IF pattern with some supplied values,
        usually the following combinations:
        * null values
        * empty values
        * zero values
        * min/max values
        """

        def standard_test_case(init_val):
            return {
                "init_val": [init_val],
                "update_patterns": [
                    "value=:v",
                    "value in (:v)",
                    {"p": "value in :v", "v": [[init_val]]},
                    # Set and tuple columns don't support indexing by key in C* and Scylla, limit these tests to lists only
                    {"p": "value[:i]=:v", "i": 0, "v": init_val, "coll_type_filter": ["list"]},
                    {"p": "value[:i] in (:v)", "i": 0, "v": init_val, "coll_type_filter": ["list"]},
                    {"p": "value[:i] in :v", "i": 0, "v": [init_val], "coll_type_filter": ["list"]},
                ],
            }

        PRIMITIVE_TYPES_MAP = {
            "boolean": {"test_cases": [standard_test_case(False)], "default_update_value": [True]},
            "blob": {"test_cases": [standard_test_case(b""), standard_test_case(b"\x00\x00\x00\x00")], "default_update_value": [b"\x00\x00\x00\x01"]},
            "ascii": {"test_cases": [standard_test_case(""), standard_test_case("abc")], "default_update_value": ["def"]},
            "decimal": {"test_cases": [standard_test_case(Decimal("1.2349823094823948209384209348"))], "default_update_value": [Decimal("2.3495083459083095483409534534")]},
            "double": {"test_cases": [standard_test_case(0.0), standard_test_case(2.2250738585072014e-308), standard_test_case(1.7976931348623157e308), standard_test_case(-1.7976931348623157e308)], "default_update_value": [1.0]},
            "float": {"test_cases": [standard_test_case(0.0), standard_test_case(1.1754943508222875e-38), standard_test_case(3.4028234663852886e38), standard_test_case(-3.4028234663852886e38)], "default_update_value": [1.0]},
            "text": {"test_cases": [standard_test_case(""), standard_test_case("abc")], "default_update_value": ["def"]},
            "varchar": {"test_cases": [standard_test_case(""), standard_test_case("abc")], "default_update_value": ["def"]},
            "bigint": {"test_cases": [standard_test_case(0), standard_test_case(2**63 - 1), standard_test_case(-(2**63))], "default_update_value": [1]},
            "int": {"test_cases": [standard_test_case(0), standard_test_case(2**31 - 1), standard_test_case(-(2**31))], "default_update_value": [1]},
            "smallint": {"test_cases": [standard_test_case(0), standard_test_case(2**15 - 1), standard_test_case(-(2**15))], "default_update_value": [1]},
            "tinyint": {"test_cases": [standard_test_case(0), standard_test_case(2**7 - 1), standard_test_case(-(2**7))], "default_update_value": [1]},
            "varint": {"test_cases": [standard_test_case(0), standard_test_case(2**128), standard_test_case(-(2**128))], "default_update_value": [1]},
            "timestamp": {
                "test_cases": [standard_test_case(datetime(1970, 1, 1, 0, 0)), standard_test_case(datetime.strptime("2020-01-02 14:13:12.001", "%Y-%m-%d %H:%M:%S.%f"))],
                "default_update_value": [datetime.strptime("2021-02-03 15:14:13.002", "%Y-%m-%d %H:%M:%S.%f")],
            },
            "date": {"test_cases": [standard_test_case(Date(0)), standard_test_case(Date("2020-1-2"))], "default_update_value": [Date("2021-2-3")]},
            "time": {"test_cases": [standard_test_case(Time(0)), standard_test_case(Time("12:13:14.001"))], "default_update_value": [Time("13:14:15.002")]},
            "timeuuid": {"test_cases": [standard_test_case(uuid_from_time(datetime(2020, 1, 2, 3, 4, 5, 0)))], "default_update_value": [uuid_from_time(datetime(2021, 2, 3, 4, 5, 6, 1))]},
            "uuid": {"test_cases": [standard_test_case(uuid.UUID(bytes=b"\x00" * 16)), standard_test_case(uuid.uuid4())], "default_update_value": [uuid.uuid4()]},
        }

        session = self.prepare()

        for is_frozen in (False, True):
            for collection_type in ("list", "set", "tuple"):
                for column_type, test_data in PRIMITIVE_TYPES_MAP.items():
                    additional_test_data = {"collection_type": collection_type}
                    self._lwt_execute_single_type_update_case(session, self._build_collection_typename(column_type, is_frozen, collection_type), {**test_data, **additional_test_data})

    def test_lwt_compare_collection_with_null(self):
        """
        Test that comparing empty collection to null yields correct results.
        Null is passed as an argument to the query as parameter marker.

        Tested situations include the following:
         * empty non-frozen collection ~ null
         * empty frozen collection != null

        Ref: https://github.com/scylladb/scylladb/issues/5855
        """

        session = self.prepare()

        # Create test table and prepare data

        session.execute(
            """
            CREATE TABLE test (
                k INT PRIMARY KEY,
                lvalue list<boolean>,
                flvalue frozen<list<boolean>>,
                svalue set<boolean>,
                fsvalue frozen<set<boolean>>,
                mvalue map<boolean, boolean>,
                fmvalue frozen<map<boolean, boolean>>
            )
        """
        )

        session.execute(
            """
            INSERT INTO test (k) VALUES (0)
        """
        )  # leave collection cells == null

        # Prepare statements

        # list<T>
        TestInfo = namedtuple("TestInfo", ["stmt", "frozen_stmt", "empty", "non_empty"])
        test_infos = []
        # list<T>
        test_infos.append(
            TestInfo(
                stmt=prepare_statement(
                    session,
                    """
                    UPDATE test SET lvalue=:update_val WHERE k=0 IF lvalue=:v
                """,
                ),
                frozen_stmt=prepare_statement(
                    session,
                    """
                    UPDATE test SET flvalue=:update_val WHERE k=0 IF flvalue=:v
                """,
                ),
                empty=[],
                non_empty=[False],
            )
        )
        # set<T>
        test_infos.append(
            TestInfo(
                stmt=prepare_statement(
                    session,
                    """
                    UPDATE test SET svalue=:update_val WHERE k=0 IF svalue=:v
                """,
                ),
                frozen_stmt=prepare_statement(
                    session,
                    """
                    UPDATE test SET fsvalue=:update_val WHERE k=0 IF fsvalue=:v
                """,
                ),
                empty=[],
                non_empty=[False],
            )
        )
        # map<K,V>
        test_infos.append(
            TestInfo(
                stmt=prepare_statement(
                    session,
                    """
                    UPDATE test SET mvalue=:update_val WHERE k=0 IF mvalue=:v
                """,
                ),
                frozen_stmt=prepare_statement(
                    session,
                    """
                    UPDATE test SET fmvalue=:update_val WHERE k=0 IF fmvalue=:v
                """,
                ),
                empty={},
                non_empty={False: True},
            )
        )

        # Execute checks
        for ti in test_infos:
            # Non-frozen empty collection should be equivalent to null
            # The following should also succeed exactly as it does in un-prepared variant.
            # Ref: https://github.com/scylladb/scylla/issues/5855
            # assert_one_prepared(session, ti.stmt, [True, None], {'update_val': ti.empty, 'v': ti.empty})
            assert_one_prepared(session, ti.stmt, [True, None], {"update_val": ti.non_empty, "v": None})
            assert_one_prepared(session, ti.stmt, [False, ti.non_empty], {"update_val": ti.empty, "v": ti.empty})

            # Frozen empty collection should be distinct from null
            assert_one_prepared(session, ti.frozen_stmt, [False, None], {"update_val": ti.empty, "v": ti.empty})
            assert_one_prepared(session, ti.frozen_stmt, [True, None], {"update_val": ti.non_empty, "v": None})
            assert_one_prepared(session, ti.frozen_stmt, [False, ti.non_empty], {"update_val": ti.empty, "v": ti.empty})

    def test_lwt_nested_collections_list_set(self):
        """
        Test that nested collections are working with parameter markers.
        This particular test verifies list<frozen<set<T>>> cases.

        Note: only non_frozen<frozen<T>> combinations are supported.
        """

        session = self.prepare()

        # Create test table and prepare data

        session.execute(
            """
            CREATE TABLE test (
                k INT PRIMARY KEY,
                list_set list<frozen<set<int>>>
            )
        """
        )

        insert_stmt = prepare_statement(
            session,
            """
            INSERT INTO test (k, list_set) VALUES (1, ?)
        """,
        )

        # INSERT INTO test (k, list_set) VALUES (1, [{1,2}, {1,2}])
        session.execute(insert_stmt, [[SortedSet([1, 2]), SortedSet([1, 2])]])

        # UPDATE test SET list_set=[{3,4}, {4,5}] WHERE a = 1 IF list_set = [{1,2}, {1,2}]
        stmt = prepare_statement(
            session,
            """
            UPDATE test SET list_set=:update_val WHERE k=1 IF list_set=:v
        """,
        )
        assert_one_prepared(session, stmt, [True, [SortedSet([1, 2]), SortedSet([1, 2])]], {"update_val": [SortedSet([3, 4]), SortedSet([4, 5])], "v": [SortedSet([1, 2]), SortedSet([1, 2])]})

        # UPDATE test SET list_set=[{3,4,5}, {4,5,6}] WHERE k = 1 IF list_set > [{3,3}, {4,4}]
        stmt = prepare_statement(
            session,
            """
            UPDATE test SET list_set=:update_val WHERE k=1 IF list_set > :v
        """,
        )
        assert_one_prepared(session, stmt, [True, [SortedSet([3, 4]), SortedSet([4, 5])]], {"update_val": [SortedSet([3, 4, 5]), SortedSet([4, 5, 6])], "v": [SortedSet([3, 3]), SortedSet([4, 4])]})

        # UPDATE test SET list_set=[{5,6,7}, {7,8,9}] WHERE k = 1 IF list_set >= [{3,3}, {5,4}]
        stmt = prepare_statement(
            session,
            """
            UPDATE test SET list_set=:update_val WHERE k=1 IF list_set >= :v
        """,
        )
        assert_one_prepared(session, stmt, [True, [SortedSet([3, 4, 5]), SortedSet([4, 5, 6])]], {"update_val": [SortedSet([5, 6, 7]), SortedSet([7, 8, 9])], "v": [SortedSet([3, 3]), SortedSet([5, 4])]})

        # UPDATE test SET list_set=[{5,6,7}, {7,8,9}] WHERE k = 1 IF list_set >= [{3,4}, {4,5}]
        stmt = prepare_statement(
            session,
            """
            UPDATE test SET list_set=:update_val WHERE k=1 IF list_set >= :v
        """,
        )
        assert_one_prepared(session, stmt, [True, [SortedSet([5, 6, 7]), SortedSet([7, 8, 9])]], {"update_val": [SortedSet([5, 6, 7]), SortedSet([7, 8, 9])], "v": [SortedSet([3, 4]), SortedSet([4, 5])]})

    def test_lwt_nested_collections_set_list(self):
        """
        Test that nested collections are working with parameter markers.
        This particular test verifies set<frozen<list<T>>> cases.

        Note: only non_frozen<frozen<T>> combinations are supported.
        """

        session = self.prepare()

        # Create test table and prepare data

        session.execute(
            """
            CREATE TABLE test (
                k INT PRIMARY KEY,
                set_list set<frozen<list<int>>>,
            )
        """
        )

        insert_stmt = prepare_statement(
            session,
            """
            INSERT INTO test (k, set_list) VALUES (1, ?)
        """,
        )

        # INSERT INTO test (k, set_list) VALUES (1, {[1,2], [1,2]})
        session.execute(insert_stmt, [SortedSet([[1, 2], [1, 2]])])

        # UPDATE test SET set_list={[3,3], [4,4]} WHERE k = 1 IF b = {[1,2], [1,2]}
        stmt = prepare_statement(
            session,
            """
            UPDATE test SET set_list=:update_val WHERE k=1 IF set_list=:v
        """,
        )
        assert_one_prepared(session, stmt, [True, SortedSet([[1, 2]])], {"update_val": SortedSet([[3, 3], [4, 4]]), "v": SortedSet([[1, 2], [1, 2]])})

        # UPDATE test SET set_list={[5,5,5], [4,4,4]} WHERE k = 1 if set_list > {[3,3], [4,4]}
        stmt = prepare_statement(
            session,
            """
            UPDATE test SET set_list=:update_val WHERE k=1 IF set_list > :v
        """,
        )
        assert_one_prepared(session, stmt, [False, SortedSet([[3, 3], [4, 4]])], {"update_val": SortedSet([[5, 5, 5], [4, 4, 4]]), "v": SortedSet([[3, 3], [4, 4]])})

        # UPDATE test SET set_list={[5,5,5], [4,4,4]} WHERE k = 1 IF set_list >= {[3,3], [4,4]}
        stmt = prepare_statement(
            session,
            """
            UPDATE test SET set_list=:update_val WHERE k=1 IF set_list >= :v
        """,
        )
        assert_one_prepared(session, stmt, [True, SortedSet([[3, 3], [4, 4]])], {"update_val": SortedSet([[5, 5, 5], [4, 4, 4]]), "v": SortedSet([[3, 3], [4, 4]])})
