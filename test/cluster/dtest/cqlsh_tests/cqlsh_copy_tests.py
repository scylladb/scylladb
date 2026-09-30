#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import csv
import datetime
import json
import logging
import os
import re
import time
from collections import namedtuple
from decimal import Decimal
from itertools import repeat
from tempfile import NamedTemporaryFile
from uuid import uuid1, uuid4

import pytest
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.util import SortedSet
from ccmlib.scylla_cluster import ScyllaCluster

from dtest_class import Tester, create_ks
from tools.assertions import (
    assert_all,
    assert_row_count,
    assert_row_count_in_select_less,
)
from tools.data import rows_to_list
from tools.marks import issue_open, unmark, with_feature
from tools.misc import is_coverage

from .cqlsh_tools import (
    DummyColorMap,
    assert_csvs_items_equal,
    csv_rows,
    monkeypatch_driver,
    random_list,
    strip_timezone_if_time_string,
    unmonkeypatch_driver,
    write_rows_to_csv,
)
from .formatter import DateTimeFormat, _formatters, format_value_default

logger = logging.getLogger(__name__)

DEFAULT_FLOAT_PRECISION = 5  # magic number copied from cqlsh script
DEFAULT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S%z"  # based on cqlsh script

# accept both Python-cqlsh's old wording and cqlsh-rs's new wording for a failed COPY FROM
COPY_FROM_FAILURE_RE = re.compile(r"[1-9]\d* (?:parse|insert) error\(s\) encountered|Failed to (?:import|process)|COPY FROM error")
# accept both Python-cqlsh's old wording and cqlsh-rs's new wording for a failed COPY TO
COPY_TO_FAILURE_RE = re.compile(r"some records might be missing")


class UTC(datetime.tzinfo):
    """
    A utility class to specify a UTC timezone.
    """

    def utcoffset(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return datetime.timedelta(0)


def is_immutable(self):
    raise TypeError("%r objects are immutable" % self.__class__.__name__)


class ImmutableDictMixin:
    """Makes a :class:`dict` immutable."""

    _hash_cache = None

    @classmethod
    def fromkeys(cls, keys, value=None):
        instance = super(cls, cls).__new__(cls)
        instance.__init__(zip(keys, repeat(value)))
        return instance

    def __reduce_ex__(self, protocol):
        return type(self), (dict(self),)

    def _iter_hashitems(self):
        return iter(self.items())

    def __hash__(self):
        if self._hash_cache is not None:
            return self._hash_cache
        rv = self._hash_cache = hash(frozenset(self._iter_hashitems()))
        return rv

    def setdefault(self, key, default=None):
        is_immutable(self)

    def update(self, *args, **kwargs):
        is_immutable(self)

    def pop(self, key, default=None):
        is_immutable(self)

    def popitem(self):
        is_immutable(self)

    def __setitem__(self, key, value):
        is_immutable(self)

    def __delitem__(self, key):
        is_immutable(self)

    def clear(self):
        is_immutable(self)


@pytest.mark.dtest_full
class CqlshPrepare(Tester):
    def prepare(self, nodes=1, configuration_options=None):
        if not self.cluster.nodelist():
            self.cluster.set_partitioner("org.apache.cassandra.dht.Murmur3Partitioner")
            if configuration_options:
                self.cluster.set_configuration_options(values=configuration_options)
            self.cluster.populate(nodes).start(wait_for_binary_proto=True)
        else:
            assert len(self.cluster.nodelist()) == nodes, "Cannot reuse cluster: different number of nodes"
            assert configuration_options is None, f"Unexpected configuration options: {configuration_options}"

        self.node1 = self.cluster.nodelist()[0]
        self.session = self.patient_cql_connection(self.node1)

        self.session.execute("DROP KEYSPACE IF EXISTS ks")
        create_ks(self.session, "ks", 1)

    def all_datatypes_prepare(self, nodes=1):
        self.prepare(nodes)

        self.session.execute("CREATE TYPE name_type (firstname text, lastname text)")
        self.session.execute(
            """
            CREATE TYPE address_type (name frozen<name_type>, number int, street text, phones set<text>)
            """
        )

        self.session.execute(
            """
            CREATE TABLE testdatatype (
                a ascii PRIMARY KEY,
                b bigint,
                c blob,
                d boolean,
                e decimal,
                f double,
                g float,
                h inet,
                i int,
                j text,
                k timestamp,
                l timeuuid,
                m uuid,
                n varchar,
                o varint,
                p list<int>,
                q set<text>,
                r map<timestamp, text>,
                s tuple<int, text, boolean>,
                t frozen<address_type>,
                u frozen<list<list<address_type>>>,
                v frozen<map<map<int,int>,set<text>>>,
                w frozen<set<set<inet>>>,
            )"""
        )

        class Datetime(datetime.datetime):
            def __str__(self):
                return self.strftime(DEFAULT_TIME_FORMAT)

            def __repr__(self):
                return self.strftime(DEFAULT_TIME_FORMAT)

        def maybe_quote(s):
            """
            Return a quoted string representation for strings, unicode and date time parameters,
            otherwise return a string representation of the parameter.
            """
            return f"'{s}'" if isinstance(s, str | Datetime) else str(s)

        class ImmutableDict(ImmutableDictMixin, dict):
            """An immutable :class:`dict`."""

            def __repr__(self):
                return f"{self.__class__.__name__}({dict.__repr__(self)})"

            def copy(self):
                """Return a shallow mutable copy of this object.  Keep in mind that
                the standard library's :func:`copy` function is a no-op for this class
                like for any other python immutable type (eg: :class:`tuple`).
                """
                return dict(self)

            def __copy__(self):
                return self

        class ImmutableSet(SortedSet):
            def __repr__(self):
                return "{{{}}}".format(", ".join([maybe_quote(t) for t in sorted(self._items)]))

            def __hash__(self):
                return hash(tuple([e for e in self]))

        class Name(namedtuple("Name", ("firstname", "lastname"))):
            __slots__ = ()

            def __repr__(self):
                return f"{{firstname: '{self.firstname}', lastname: '{self.lastname}'}}"

        class Address(namedtuple("Address", ("name", "number", "street", "phones"))):
            __slots__ = ()

            def __repr__(self):
                phones_str = "{{{}}}".format(", ".join(maybe_quote(p) for p in sorted(self.phones)))
                return f"{{name: {self.name}, number: {self.number}, street: '{self.street}', phones: {phones_str}}}"

        self.session.cluster.register_user_type("ks", "name_type", Name)
        self.session.cluster.register_user_type("ks", "address_type", Address)

        date1 = Datetime(2005, 7, 14, 12, 30, 0, 0, UTC())
        date2 = Datetime(2005, 7, 14, 13, 30, 0, 0, UTC())

        addr1 = Address(Name("name1", "last1"), 1, "street 1", ImmutableSet(["1111 2222", "3333 4444"]))
        addr2 = Address(Name("name2", "last2"), 2, "street 2", ImmutableSet(["5555 6666", "7777 8888"]))
        addr3 = Address(Name("name3", "last3"), 3, "street 3", ImmutableSet(["1111 2222", "3333 4444"]))
        addr4 = Address(Name("name4", "last4"), 4, "street 4", ImmutableSet(["5555 6666", "7777 8888"]))

        self.data = (
            "ascii",  # a ascii
            2**40,  # b bigint
            bytearray.fromhex("beef"),  # c blob
            True,  # d boolean
            Decimal("3.14"),  # e decimal
            2.444,  # f double
            1.1,  # g float
            "127.0.0.1",  # h inet
            25,  # i int
            "ヽ(`ー`)/",  # j text
            date1,  # k timestamp
            uuid1(),  # l timeuuid
            uuid4(),  # m uuid
            "asdf",  # n varchar
            2**65,  # o varint
            [1, 2, 3],  # p list<int>,
            ImmutableSet(["3", "2", "1"]),  # q set<text>,
            ImmutableDict({date1: "1", date2: "2"}),  # r map<timestamp, text>,
            (1, "1", True),  # s tuple<int, text, boolean>,
            addr1,  # t frozen<address_type>,
            [[addr1, addr2], [addr3, addr4]],  # u frozen<list<list<address_type>>>,
            # v frozen<map<map<int,int>,set<text>>>
            ImmutableDict({ImmutableDict({1: 1, 2: 2}): ImmutableSet(["1", "2", "3"])}),
            # w frozen<set<set<inet>>>, because of the SortedSet.__lt__() implementation, make sure the
            # first set is contained in the second set or else they will not sort consistently
            # and this will cause comparison problems when comparing with csv strings therefore failing
            # some tests
            ImmutableSet([ImmutableSet(["127.0.0.1"]), ImmutableSet(["127.0.0.1", "127.0.0.2"])]),
        )

    def tearDown(self): ...


@pytest.mark.dtest_full
@pytest.mark.next_gating
class TestCqlshCopy(CqlshPrepare):
    """
    Tests the COPY TO and COPY FROM features in cqlsh.
    @jira_ticket CASSANDRA-3906
    """

    @pytest.fixture(scope="class", autouse=True)
    def monkeypatch_driver(self):
        cached_driver_methods = monkeypatch_driver()
        yield
        unmonkeypatch_driver(cached_driver_methods)

    @pytest.fixture(scope="function")
    def clean_temp(self):
        yield
        if hasattr(self, "tempfile"):
            os.unlink(self.tempfile.name)

    def assert_csv_result_equal(self, csv_filename, results):
        result_list = list(self.result_to_csv_rows(results))
        processed_results = [[strip_timezone_if_time_string(v) for v in row] for row in result_list]

        csv_file = list(csv_rows(csv_filename))
        processed_csv = [[strip_timezone_if_time_string(v) for v in row] for row in csv_file]

        self.maxDiff = None
        try:
            assert len(processed_csv) == len(processed_results), f"Expected {len(processed_results)}, got {len(processed_csv)}"
        except AssertionError as e:
            if len(processed_csv) != len(processed_results):
                logger.warning(f"Different # of entries. CSV: {len(processed_csv)!s} vs results: {len(processed_results)!s}")
            elif processed_csv[0] is not None:
                for x in range(len(processed_csv[0])):
                    if processed_csv[0][x] != processed_results[0][x]:
                        logger.warning(f"Mismatch at index:  {x!s}")
                        logger.warning(f"Value in csv: {processed_csv[0][x]!s}")
                        logger.warning(f"Value in result: {processed_results[0][x]!s}")
            raise e

    def format_for_csv(self, val):
        encoding_name = "utf-8"
        date_time_format = DateTimeFormat()

        # this seems gross but if the blob isn't set to type:bytearray is won't compare correctly
        if isinstance(val, str) and hasattr(self, "data") and self.data[2] == val:
            var_type = bytearray
            val = bytearray(val)
        else:
            var_type = type(val)

        formatter = _formatters.get(var_type.__name__.lower(), format_value_default)

        return formatter(val, encoding=encoding_name, date_time_format=date_time_format, time_format=DEFAULT_TIME_FORMAT, float_precision=DEFAULT_FLOAT_PRECISION, colormap=DummyColorMap(), nullval=None)

    def result_to_csv_rows(self, result):
        """
        Given an object returned from a CQL query, returns a string formatted by
        the cqlsh formatting utilities.
        """
        # This has no real dependencies on Tester except that self._cqlshlib has
        # to grab self.cluster's install directory. This should be pulled out
        # into a bare function if cqlshlib is made easier to interact with.
        return [[self.format_for_csv(v) for v in row] for row in result]

    @pytest.mark.skip_bug(link="https://github.com/scylladb/scylladb/issues/2393", reason="scylla has an old version of cqlshlib")
    @pytest.mark.single_node
    def test_list_data(self):
        """
        Tests the COPY TO command with the list datatype by:

        - populating a table with lists of uuids,
        - exporting the table to a CSV file with COPY TO,
        - comparing the CSV file to the SELECTed contents of the table.
        """
        self.prepare()
        self.session.execute(
            """
            CREATE TABLE testlist (
                a int PRIMARY KEY,
                b list<uuid>
            )"""
        )

        insert_statement = self.session.prepare("INSERT INTO testlist (a, b) VALUES (?, ?)")
        args = [(i, random_list(gen=uuid4)) for i in range(1000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = rows_to_list(self.session.execute("SELECT * FROM testlist"))

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        logger.debug(f"Exporting to csv file: {self.tempfile.name}")
        self.node1.run_cqlsh(cmds=f"COPY ks.testlist TO '{self.tempfile.name}'")

        self.assert_csv_result_equal(self.tempfile.name, results)

    @pytest.mark.skip_bug(link="https://github.com/scylladb/scylladb/issues/2393", reason="scylla has an old version of cqlshlib")
    @pytest.mark.single_node
    def test_tuple_data(self):
        """
        Tests the COPY TO command with the tuple datatype by:

        - populating a table with tuples of uuids,
        - exporting the table to a CSV file with COPY TO,
        - comparing the CSV file to the SELECTed contents of the table.
        """
        self.prepare()
        self.session.execute(
            """
            CREATE TABLE testtuple (
                a int primary key,
                b tuple<uuid, uuid, uuid>
            )"""
        )

        insert_statement = self.session.prepare("INSERT INTO testtuple (a, b) VALUES (?, ?)")
        args = [(i, random_list(gen=uuid4, n=3)) for i in range(1000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = rows_to_list(self.session.execute("SELECT * FROM testtuple"))

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        logger.debug(f"Exporting to csv file: {self.tempfile.name}")
        self.node1.run_cqlsh(cmds=f"COPY ks.testtuple TO '{self.tempfile.name}'")

        self.assert_csv_result_equal(self.tempfile.name, results)

    def non_default_delimiter_template(self, delimiter):
        """
        @param delimiter the delimiter to use for the CSV file.

        Test exporting to CSV files using delimiters other than ',' by:

        - populating a table with integers,
        - exporting to a CSV file, specifying a delimiter, then
        - comparing the contents of the csv file to the SELECTed contents of the table.
        """

        self.prepare()
        self.session.execute(
            """
            CREATE TABLE testdelimiter (
                a int primary key
            )"""
        )
        insert_statement = self.session.prepare("INSERT INTO testdelimiter (a) VALUES (?)")
        args = [(i,) for i in range(10000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = rows_to_list(self.session.execute("SELECT * FROM testdelimiter"))

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        logger.debug(f"Exporting to csv file: {self.tempfile.name}")
        cmds = f"COPY ks.testdelimiter TO '{self.tempfile.name}' WITH DELIMITER = '{delimiter}'"
        self.node1.run_cqlsh(cmds=cmds)

        self.assert_csv_result_equal(self.tempfile.name, results)

    @pytest.mark.single_node
    def test_colon_delimiter(self):
        """
        Use non_default_delimiter_template to test COPY with the delimiter ':'.
        """
        self.non_default_delimiter_template(":")

    @pytest.mark.single_node
    def test_letter_delimiter(self):
        """
        Use non_default_delimiter_template to test COPY with the delimiter 'a'.
        """
        self.non_default_delimiter_template("a")

    @pytest.mark.single_node
    def test_number_delimiter(self):
        """
        Use non_default_delimiter_template to test COPY with the delimiter '1'.
        """
        self.non_default_delimiter_template("1")

    def custom_null_indicator_template(self, indicator):
        """
        @param indicator the null indicator to be used in COPY

        A parametrized test that tests COPY with a given null indicator.
        """
        self.all_datatypes_prepare()
        self.session.execute(
            """
            CREATE TABLE testnullindicator (
                a int primary key,
                b text
            )"""
        )
        insert_non_null = self.session.prepare("INSERT INTO testnullindicator (a, b) VALUES (?, ?)")
        execute_concurrent_with_args(self.session, insert_non_null, [(1, "eggs"), (100, "sausage")])
        insert_null = self.session.prepare("INSERT INTO testnullindicator (a) VALUES (?)")
        execute_concurrent_with_args(self.session, insert_null, [(2,), (200,)])

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        logger.debug(f"Exporting to csv file: {self.tempfile.name}")
        cmds = f"COPY ks.testnullindicator TO '{self.tempfile.name}' WITH NULL = '{indicator}'"
        self.node1.run_cqlsh(cmds=cmds)

        results = rows_to_list(self.session.execute("SELECT a, b FROM ks.testnullindicator"))
        results = [[indicator if value is None else value for value in row] for row in results]

        self.assert_csv_result_equal(self.tempfile.name, results)

    @pytest.mark.single_node
    def test_undefined_as_null_indicator(self):
        """
        Use custom_null_indicator_template to test COPY with NULL = undefined.
        """
        self.custom_null_indicator_template("undefined")

    @pytest.mark.single_node
    def test_null_as_null_indicator(self):
        """
        Use custom_null_indicator_template to test COPY with NULL = 'null'.
        """
        self.custom_null_indicator_template("null")

    @pytest.mark.single_node
    def test_writing_use_header(self):
        """
        Test that COPY can write a CSV with a header by:

        - creating and populating a table,
        - exporting the contents of the table to a CSV file using COPY WITH
        HEADER = true
        - checking that the contents of the CSV file are the written values plus
        the header.
        """
        self.prepare()
        self.session.execute(
            """
            CREATE TABLE testheader (
                a int primary key,
                b int
            )"""
        )
        insert_statement = self.session.prepare("INSERT INTO testheader (a, b) VALUES (?, ?)")
        args = [(1, 10), (2, 20), (3, 30)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        logger.debug(f"Exporting to csv file: {self.tempfile.name}")
        cmds = f"COPY ks.testheader TO '{self.tempfile.name}' WITH HEADER = true"
        self.node1.run_cqlsh(cmds=cmds)

        with open(self.tempfile.name) as csvfile:
            csv_values = rows_to_list(csv.reader(csvfile))

        expected = [["a", "b"], ["1", "10"], ["2", "20"], ["3", "30"]]

        assert sorted(csv_values) == sorted(expected), f"Data after table copying not as expected. Expected: {expected}.\nGot: {csv_values}"

    def _test_reading_counter_template(self, copy_options=None):
        """
        Test that COPY can read a csv file of COUNTER values by:

        - creating a table,
        - writing a CSV with COUNTER data with header,
        - importing the contents of the CSV file using COPY with header,
        - checking that the contents of the table are the written values.
        """
        self.prepare()
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS testcounter (
                a int,
                b text,
                c counter,
                PRIMARY KEY (a, b)
            )"""
        )

        tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")

        data = [[1, "1", 20], [2, "2", 40], [3, "3", 60], [4, "4", 80]]

        with open(tempfile.name, "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["a", "b", "c"])
            writer.writeheader()
            for a, b, c in data:
                writer.writerow({"a": a, "b": b, "c": c})

        self.session.execute("TRUNCATE TABLE testcounter")
        cmds = f"COPY ks.testcounter FROM '{tempfile.name}' WITH HEADER = true"
        if copy_options:
            for opt, val in copy_options.items():
                cmds += f" AND {opt} = {val}"

        logger.debug(f"Running {cmds}")
        self.node1.run_cqlsh(cmds=cmds)

        assert_all(session=self.session, query="SELECT * FROM testcounter", expected=data, ignore_order=True)

    @pytest.mark.single_node
    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#18180"))
    def test_reading_counter(self):
        """
        Test that COPY can read a csv file of COUNTER values.

        @jira_ticket CASSANDRA-9043
        """
        self._test_reading_counter_template()

    @pytest.mark.single_node
    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#18180"))
    def test_reading_counter_without_batching(self):
        """
        Test that COPY can read a csv file of COUNTER values with batching disabled,
        that is MAXBATCHSIZE set to 1.

        @jira_ticket CASSANDRA-11474
        """
        self._test_reading_counter_template(copy_options={"MAXBATCHSIZE": "1"})

    @pytest.mark.single_node
    @pytest.mark.skip_if(with_feature("tablets") & issue_open("#18180"))
    def test_reading_counters_with_skip_cols(self):
        """
        Test importing a CSV file for a counter table but skipping some columns:

        - create a table
        - create a csv file with all column values
        - import the csv file with skip_columns
        - check only the columns that were not skipped are in the table

        Because COPY FROM for counters is not idempotent we expect that the values inserted continually increase.

        @jira_ticket CASSANDRA-9303
        """
        self.prepare()
        self.session.execute(
            """
            CREATE TABLE testskipcols (
                a int primary key,
                b counter,
                c counter,
                d counter,
                e counter
            )"""
        )

        tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        data = [[1, 1, 1, 1, 1], [2, 1, 1, 1, 1]]

        with open(tempfile.name, "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["a", "b", "c", "d", "e"])
            for a, b, c, d, e in data:
                writer.writerow({"a": a, "b": b, "c": c, "d": d, "e": e})

        def do_test(skip_cols, expected_results):
            logger.debug(f"Importing csv file {tempfile} with skipcols '{skip_cols}'")
            cmds = f"COPY ks.testskipcols FROM '{tempfile.name}' WITH SKIPCOLS = '{skip_cols}'"
            res = self.node1.run_cqlsh(cmds=cmds, show_output=True)
            logger.debug(res)
            assert_all(session=self.session, query="SELECT * FROM ks.testskipcols", expected=expected_results, ignore_order=True)

        do_test("c, d, e", [[1, 1, None, None, None], [2, 1, None, None, None]])
        do_test("b", [[1, 1, 1, 1, 1], [2, 1, 1, 1, 1]])
        do_test("b", [[1, 1, 2, 2, 2], [2, 1, 2, 2, 2]])
        do_test("e", [[1, 2, 3, 3, 2], [2, 2, 3, 3, 2]])

    @pytest.mark.single_node
    def test_reading_use_header(self):
        """
        Test that COPY can read a CSV with a header by:

        - creating a table,
        - writing a CSV with a header,
        - importing the contents of the CSV file using COPY WITH HEADER = true,
        - checking that the contents of the table are the written values.
        """
        self.prepare()
        self.session.execute(
            """
            CREATE TABLE testheader (
                a int primary key,
                b int
            )"""
        )

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")

        data = [[1, 20], [2, 40], [3, 60], [4, 80]]

        with open(self.tempfile.name, "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["a", "b"])
            writer.writeheader()
            for a, b in data:
                writer.writerow({"a": a, "b": b})
            csvfile.close()

        cmds = f"COPY ks.testheader FROM '{self.tempfile.name}' WITH HEADER = true"
        self.node1.run_cqlsh(cmds=cmds)

        assert_all(session=self.session, query="SELECT * FROM testheader", expected=data, ignore_order=True)

    @pytest.mark.single_node
    def test_writing_with_timeformat(self):
        """
        @jira_ticket CASSANDRA-10633
        Test COPY TO with the time format specified in the WITH option by:

        - creating and populating a table,
        - exporting the contents of the table to a CSV file using COPY TO WITH TIMEFORMAT,
        - checking the time format written to csv.
        """
        self.prepare()
        self.session.execute(
            """
            CREATE TABLE testtimeformat (
                a int primary key,
                b timestamp
            )"""
        )
        insert_statement = self.session.prepare("INSERT INTO testtimeformat (a, b) VALUES (?, ?)")
        args = [(1, datetime.datetime(2015, 1, 1, 7, 00, 0, 0, UTC())), (2, datetime.datetime(2015, 6, 10, 12, 30, 30, 500, UTC())), (3, datetime.datetime(2015, 12, 31, 23, 59, 59, 999, UTC()))]
        execute_concurrent_with_args(self.session, insert_statement, args)

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        logger.debug(f"Exporting to csv file: {self.tempfile.name}")
        cmds = f"COPY ks.testtimeformat TO '{self.tempfile.name}'"
        cmds += " WITH DATETIMEFORMAT = '%Y/%m/%d %H:%M'"
        self.node1.run_cqlsh(cmds=cmds)
        print(cmds)

        with open(self.tempfile.name) as csvfile:
            csv_values = list(csv.reader(csvfile))

        expected = [["1", "2015/01/01 07:00"], ["2", "2015/06/10 12:30"], ["3", "2015/12/31 23:59"]]
        assert sorted(csv_values, key=lambda x: x[0]) == expected, f"Actual value \"{csv_values}\" is not as expected '{expected}'"

    @pytest.mark.single_node
    def test_reading_with_ttl(self):
        """
        @jira_ticket CASSANDRA-9494
        Test COPY FROM with TTL specified in the WITH option by:

        - creating a table,
        - writing a csv,
        - importing the contents of the CSV file using COPY TO WITH TTL,
        - checking the data has been imported,
        - checking again after TTL * 2 seconds that the data has expired.
        """
        self.prepare()
        self.session.execute(
            """
            CREATE TABLE testttl (
                a int primary key,
                b int
            )"""
        )

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")

        data = [[1, 20], [2, 40], [3, 60], [4, 80]]

        with open(self.tempfile.name, "w") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["a", "b"])
            for a, b in data:
                writer.writerow({"a": a, "b": b})
            csvfile.close()

        self.node1.run_cqlsh(cmds=f"COPY ks.testttl FROM '{self.tempfile.name}' WITH TTL = '5'")

        result = rows_to_list(self.session.execute("SELECT * FROM testttl"))
        assert_all(session=self.session, query="SELECT * FROM testttl", expected=data, ignore_order=True)

        time.sleep(10)

        assert_all(session=self.session, query="SELECT * FROM testttl", expected={}, ignore_order=True)

    @pytest.mark.single_node
    def test_explicit_column_order_writing(self):
        """
        Test that COPY can write to a CSV file when the order of columns is
        explicitly specified by:

        - creating a table,
        - COPYing to a CSV file with columns in a different order than they
        appeared in the CREATE TABLE statement,
        - writing a CSV file with the columns in that order, and
        - asserting that the two CSV files contain the same values.
        """
        self.prepare()
        self.session.execute(
            """
            CREATE TABLE testorder (
                a int primary key,
                b int,
                c text
            )"""
        )

        data = [[1, 20, "ham"], [2, 40, "eggs"], [3, 60, "beans"], [4, 80, "toast"]]
        insert_statement = self.session.prepare("INSERT INTO testorder (a, b, c) VALUES (?, ?, ?)")
        execute_concurrent_with_args(self.session, insert_statement, data)

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")

        self.node1.run_cqlsh(f"COPY ks.testorder (a, c, b) TO '{self.tempfile.name}'")

        reference_file = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        with open(reference_file.name, "w") as csvfile:
            writer = csv.writer(csvfile)
            for a, b, c in data:
                writer.writerow([a, c, b])
            csvfile.close()

        assert_csvs_items_equal(self.tempfile.name, reference_file.name)

    @pytest.mark.single_node
    def test_explicit_column_order_reading(self):
        """
        Test that COPY can write to a CSV file when the order of columns is
        explicitly specified by:

        - creating a table,
        - writing a CSV file containing columns with the same types as the
        table, but in a different order,
        - COPYing the contents of that CSV into the table by specifying the
        order of the columns,
        - asserting that the values in the CSV file match those in the table.
        """
        self.prepare()
        self.session.execute(
            """
            CREATE TABLE testorder (
                a int primary key,
                b text,
                c int
            )"""
        )

        data = [[1, 20, "ham"], [2, 40, "eggs"], [3, 60, "beans"], [4, 80, "toast"]]

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        write_rows_to_csv(self.tempfile.name, data)

        self.node1.run_cqlsh(f"COPY ks.testorder (a, c, b) FROM '{self.tempfile.name}'")

        results = rows_to_list(self.session.execute("SELECT * FROM testorder"))
        reference_file = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        with open(reference_file.name, "w") as csvfile:
            writer = csv.writer(csvfile)
            for a, b, c in data:
                writer.writerow([a, c, b])
        csvfile.close()

        self.assert_csv_result_equal(reference_file.name, results)

    def quoted_column_names_reading_template(self, specify_column_names):
        """
        @param specify_column_names if truthy, specify column names in COPY statement
        A parameterized test. Tests that COPY can read from a CSV file into a
        table with quoted column names by:

        - creating a table with quoted column names,
        - writing test data to a CSV file,
        - COPYing that CSV file into the table, explicitly naming columns, and
        - asserting that the CSV file and the table contain the same data.

        If the specify_column_names parameter is truthy, the COPY statement
        explicitly names the columns.
        """
        self.prepare()
        self.session.execute(
            """
            CREATE TABLE testquoted (
                "IdNumber" int PRIMARY KEY,
                "select" text
            )"""
        )

        data = [[1, "no"], [2, "Yes"], [3, "True"], [4, "false"]]

        self.tempfile = NamedTemporaryFile(mode="w", delete=False, encoding="utf-8")
        write_rows_to_csv(self.tempfile.name, data)

        stmt = ("""COPY ks.testquoted ("IdNumber", "select") FROM '{name}'""" if specify_column_names else """COPY ks.testquoted FROM '{name}'""").format(name=self.tempfile.name)

        self.node1.run_cqlsh(stmt)

        results = rows_to_list(self.session.execute("SELECT * FROM ks.testquoted"))
        self.assert_csv_result_equal(self.tempfile.name, results)

    @pytest.mark.single_node
    def test_quoted_column_names_reading_specify_names(self):
        """
        Use quoted_column_names_reading_template to test reading from a CSV file
        into a table with quoted column names, explicitly specifying the column
        names in the COPY statement.
        """
        self.quoted_column_names_reading_template(specify_column_names=True)

    @pytest.mark.single_node
    def test_quoted_column_names_reading_dont_specify_names(self):
        """
        Use quoted_column_names_reading_template to test reading from a CSV file
        into a table with quoted column names, without explicitly specifying the
        column names in the COPY statement.
        """
        self.quoted_column_names_reading_template(specify_column_names=False)

    def quoted_column_names_writing_template(self, specify_column_names):
        """
        @param specify_column_names if truthy, specify column names in COPY statement
        A parameterized test. Test that COPY can write to a table with quoted
        column names by:

        - creating a table with quoted column names,
        - inserting test data into that table,
        - COPYing that table into a CSV file into the table, explicitly naming columns,
        - writing that test data to a CSV file,
        - asserting that the two CSV files contain the same rows.

        If the specify_column_names parameter is truthy, the COPY statement
        explicitly names the columns.
        """
        self.prepare()
        self.session.execute(
            """
            CREATE TABLE testquoted (
                "IdNumber" int PRIMARY KEY,
                "select" text
            )"""
        )

        data = [[1, "no"], [2, "Yes"], [3, "True"], [4, "false"]]
        insert_statement = self.session.prepare("""INSERT INTO testquoted ("IdNumber", "select") VALUES (?, ?)""")
        execute_concurrent_with_args(self.session, insert_statement, data)

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        stmt = ("""COPY ks.testquoted ("IdNumber", "select") TO '{name}'""" if specify_column_names else """COPY ks.testquoted TO '{name}'""").format(name=self.tempfile.name)
        self.node1.run_cqlsh(stmt)

        reference_file = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        write_rows_to_csv(reference_file.name, data)

        assert_csvs_items_equal(self.tempfile.name, reference_file.name)

    @pytest.mark.single_node
    def test_quoted_column_names_writing_specify_names(self):
        self.quoted_column_names_writing_template(specify_column_names=True)

    @pytest.mark.single_node
    def test_quoted_column_names_writing_dont_specify_names(self):
        self.quoted_column_names_writing_template(specify_column_names=False)

    def data_validation_on_read_template(self, load_as_int, expect_invalid):
        """
        @param load_as_int the value that will be loaded into a table as an int value
        @param expect_invalid whether or not to expect the COPY statement to fail

        Test that reading from CSV files fails when there is a type mismatch
        between the value being loaded and the type of the column by:

        - creating a table,
        - writing a CSV file containing the value passed in as load_as_int, then
        - COPYing that csv file into the table, loading load_as_int as an int.

        If expect_invalid, this test will succeed when the COPY command fails.
        If not expect_invalid, this test will succeed when the COPY command prints
        no errors and the table matches the loaded CSV file.

        @jira_ticket CASSANDRA-9302
        """
        self.prepare()
        self.session.execute(
            """
            CREATE TABLE testvalidate (
                a int PRIMARY KEY,
                b int
            )"""
        )

        data = [[1, load_as_int]]

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        write_rows_to_csv(self.tempfile.name, data)

        cmd = f"""COPY ks.testvalidate (a, b) FROM '{self.tempfile.name}'"""
        out, err = self.node1.run_cqlsh(cmd, return_output=True)
        results = rows_to_list(self.session.execute("SELECT * FROM testvalidate"))

        if expect_invalid:
            copy_output = out + err
            assert COPY_FROM_FAILURE_RE.search(copy_output), f"No COPY FROM failure reported by cqlsh: '{copy_output}'"
            assert not results, "Unexpected data found in the 'testvalidate' table"
        else:
            self.assert_csv_result_equal(self.tempfile.name, results)

    @pytest.mark.single_node
    def test_read_valid_data(self):
        """
        Use data_validation_on_read_template to test COPYing an int value from a
        CSV into an int column. This test exists to make sure the parameterized
        test works.
        """
        # make sure the template works properly
        self.data_validation_on_read_template(2, expect_invalid=False)

    @pytest.mark.single_node
    def test_read_invalid_float(self):
        """
        Use data_validation_on_read_template to test COPYing a float value from a
        CSV into an int column.
        """
        self.data_validation_on_read_template(2.14, expect_invalid=True)

    @pytest.mark.single_node
    def test_read_invalid_uuid(self):
        """
        Use data_validation_on_read_template to test COPYing a uuid value from a
        CSV into an int column.
        """
        self.data_validation_on_read_template(uuid4(), expect_invalid=True)

    @pytest.mark.single_node
    def test_read_invalid_text(self):
        """
        Use data_validation_on_read_template to test COPYing a text value from a
        CSV into an int column.
        """
        self.data_validation_on_read_template("test", expect_invalid=True)

    @pytest.mark.skip_bug(link="https://github.com/scylladb/scylladb/issues/2393", reason="scylla has an old version of cqlshlib")
    @pytest.mark.single_node
    def test_all_datatypes_write(self):
        """
        Test that, after COPYing a table containing all CQL datatypes to a CSV
        file, that the table contains the same values as the CSV by:

        - creating and populating a table containing all datatypes,
        - COPYing the contents of that table to a CSV file, and
        - asserting that the CSV file contains the same data as the table.

        @jira_ticket CASSANDRA-9302
        """
        self.all_datatypes_prepare()

        insert_statement = self.session.prepare(
            """INSERT INTO testdatatype (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        )
        self.session.execute(insert_statement, self.data)

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        logger.debug(f"Exporting to csv file: {self.tempfile.name}")
        self.node1.run_cqlsh(cmds=f"COPY ks.testdatatype TO '{self.tempfile.name}'")

        results = rows_to_list(self.session.execute("SELECT * FROM testdatatype"))

        self.assert_csv_result_equal(self.tempfile.name, results)

    @pytest.mark.skip_bug(link="https://github.com/scylladb/scylladb/issues/2393", reason="scylla has an old version of cqlshlib")
    @pytest.mark.single_node
    def test_all_datatypes_read(self):
        """
        Test that, after COPYing a CSV file to a table containing all CQL
        datatypes, that the table contains the same values as the CSV by:

        - creating a table containing all datatypes,
        - writing a corresponding CSV file containing each datatype,
        - COPYing the CSV file into the table, and
        - asserting that the CSV file contains the same data as the table.

        @jira_ticket CASSANDRA-9302
        """
        self.all_datatypes_prepare()

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")

        with open(self.tempfile.name, "w") as csvfile:
            writer = csv.writer(csvfile)
            # serializing blob bytearray in friendly format
            data_set = list(self.data)
            data_set[2] = "0x{}".format("".join("%02x" % c for c in self.data[2]))
            writer.writerow(data_set)
            csvfile.close()

        logger.debug(f"Importing from csv file: {self.tempfile.name}")
        self.node1.run_cqlsh(cmds=f"COPY ks.testdatatype FROM '{self.tempfile.name}'")

        results = rows_to_list(self.session.execute("SELECT * FROM testdatatype"))

        self.assert_csv_result_equal(self.tempfile.name, results)

    @pytest.mark.single_node
    def test_all_datatypes_round_trip(self):
        """
        Test that a table containing all CQL datatypes successfully round-trips
        to and from a CSV file via COPY by:

        - creating and populating a table containing every datatype,
        - COPYing that table to a CSV file,
        - SELECTing the contents of the table,
        - TRUNCATEing the table,
        - COPYing the written CSV file back into the table, and
        - asserting that the previously-SELECTed contents of the table match the
        current contents of the table.

        @jira_ticket CASSANDRA-9302
        """
        self.all_datatypes_prepare()

        insert_statement = self.session.prepare(
            """INSERT INTO testdatatype (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t, u, v, w)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        )
        self.session.execute(insert_statement, self.data)

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        logger.debug(f"Exporting to csv file: {self.tempfile.name}")
        self.node1.run_cqlsh(cmds=f"COPY ks.testdatatype TO '{self.tempfile.name}'")

        exported_results = rows_to_list(self.session.execute("SELECT * FROM testdatatype"))

        self.session.execute("TRUNCATE ks.testdatatype")

        self.node1.run_cqlsh(cmds=f"COPY ks.testdatatype FROM '{self.tempfile.name}'")

        assert_all(session=self.session, query="SELECT * FROM testdatatype", expected=exported_results)

    @pytest.mark.single_node
    def test_wrong_number_of_columns(self):
        """
        Test that a COPY statement will fail when trying to import from a CSV
        file with the wrong number of columns by:

        - creating a table with a single column,
        - writing a CSV file with two columns,
        - attempting to COPY the CSV file into the table, and
        - asserting that the COPY operation failed.

        @jira_ticket CASSANDRA-9302
        """
        self.prepare()
        self.session.execute(
            """
            CREATE TABLE testcolumns (
                a int PRIMARY KEY,
                b int
            )"""
        )

        data = [[1, 2, 3]]
        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        write_rows_to_csv(self.tempfile.name, data)

        logger.debug(f"Importing from csv file: {self.tempfile.name}")
        out, err = self.node1.run_cqlsh(f"COPY ks.testcolumns FROM '{self.tempfile.name}'", return_output=True)

        assert_all(session=self.session, query="SELECT * FROM testcolumns", expected=[])
        copy_output = out + err
        assert COPY_FROM_FAILURE_RE.search(copy_output), f"No COPY FROM failure reported by cqlsh: '{copy_output}'"

    def _test_round_trip(self, nodes, num_records=10000):
        """
        Test a simple round trip of a small CQL table to and from a CSV file via
        COPY.

        - creating and populating a table,
        - COPYing that table to a CSV file,
        - SELECTing the contents of the table,
        - TRUNCATEing the table,
        - COPYing the written CSV file back into the table, and
        - asserting that the previously-SELECTed contents of the table match the
        current contents of the table.
        """
        self.prepare(nodes=nodes)
        self.session.execute(
            """
            CREATE TABLE testcopyto (
                a text PRIMARY KEY,
                b int,
                c float,
                d uuid
            )"""
        )

        insert_statement = self.session.prepare("INSERT INTO testcopyto (a, b, c, d) VALUES (?, ?, ?, ?)")
        args = [(str(i), i, float(i) + 0.5, uuid4()) for i in range(num_records)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = rows_to_list(self.session.execute("SELECT * FROM testcopyto"))

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        logger.debug(f"Exporting to csv file: {self.tempfile.name}")
        out = self.node1.run_cqlsh(cmds=f"COPY ks.testcopyto TO '{self.tempfile.name}'", return_output=True)
        logger.debug(out)

        # check all records were exported
        lines_num = sum(1 for _ in open(self.tempfile.name))
        assert num_records == lines_num, f"Expected exported records: {num_records}, actual exported: {lines_num}"

        # import the CSV file with COPY FROM
        self.session.execute("TRUNCATE ks.testcopyto")
        logger.debug(f"Importing from csv file: {self.tempfile.name}")
        out = self.node1.run_cqlsh(cmds=f"COPY ks.testcopyto FROM '{self.tempfile.name}'", return_output=True)
        logger.debug(out)

        assert_all(session=self.session, query="SELECT * FROM testcopyto", expected=results)

    def test_round_trip_murmur3(self):
        self._test_round_trip(nodes=3)

    @pytest.mark.single_node
    def test_source_copy_round_trip(self):
        """
        Like test_round_trip, but uses the SOURCE command to execute the
        COPY command.  This checks that we don't have unicode-related
        problems when sourcing COPY commands (CASSANDRA-9083).
        """
        self.prepare()
        self.session.execute(
            """
            CREATE TABLE testcopyto (
                a int,
                b text,
                c float,
                d uuid,
                PRIMARY KEY (a, b)
            )"""
        )

        insert_statement = self.session.prepare("INSERT INTO testcopyto (a, b, c, d) VALUES (?, ?, ?, ?)")
        args = [(i, str(i), float(i) + 0.5, uuid4()) for i in range(1000)]
        execute_concurrent_with_args(self.session, insert_statement, args)

        results = rows_to_list(self.session.execute("SELECT * FROM testcopyto"))

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        logger.debug(f"Exporting to csv file: {self.tempfile.name}")

        commandfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        commandfile.file.write("USE ks;\n")
        commandfile.file.write(f"COPY ks.testcopyto TO '{self.tempfile.name}' WITH HEADER=false;")
        commandfile.close()

        self.node1.run_cqlsh(cmds=f"SOURCE '{commandfile.name}'")
        os.unlink(commandfile.name)

        # import the CSV file with COPY FROM
        self.session.execute("TRUNCATE ks.testcopyto")
        logger.debug(f"Importing from csv file: {self.tempfile.name}")

        commandfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        commandfile.file.write("USE ks;\n")
        commandfile.file.write(f"COPY ks.testcopyto FROM '{self.tempfile.name}' WITH HEADER=false;")
        commandfile.close()

        self.node1.run_cqlsh(cmds=f"SOURCE '{commandfile.name}'")

        assert_all(session=self.session, query="SELECT * FROM testcopyto", expected=results)

        os.unlink(commandfile.name)

    def _test_bulk_round_trip(  # noqa: PLR0913, PLR0915
        self,
        nodes,
        partitioner,
        num_operations,
        profile=None,
        stress_table="keyspace1.standard1",
        configuration_options=None,
        skip_count_checks=False,
        copy_to_options=None,
        copy_from_options=None,
    ):
        """
        Test exporting a large number of rows into a csv file.

        If skip_count_checks is True then it means we cannot use "SELECT COUNT(*)" as it may time out but
        it also means that we can be sure that one cassandra-stress operation is one record and hence
        num_records=num_operations.

        Perform the following:
        - create the records with cassandra-stress
        - export the records to a csv file
        - truncate the table and import the csv file
        - export the records to another csv file
        - check that the length of the two csv files is the same

        Therefore, 3 COPY operations are run in total. Return a list of tuples, containing stdout and stderr
        for all 3 copy operations.
        """
        if configuration_options is None:
            configuration_options = {}
        if copy_to_options is None:
            copy_to_options = {}

        # The default truncate timeout of 10 seconds that is set in init_default_config() is not
        # enough for truncating larger tables, see CASSANDRA-11157
        if "truncate_request_timeout_in_ms" not in configuration_options:
            configuration_options["truncate_request_timeout_in_ms"] = 60000

        self.prepare(nodes=nodes, configuration_options=configuration_options)

        ret = []

        def create_records():
            if not profile:
                logger.debug(f"Running stress without any user profile, num_operations={num_operations}")
                self.node1.stress(["write", f"n={num_operations} cl=ALL", "no-warmup", "-rate", "threads=50"])
            else:
                logger.debug(f"Running stress with user profile {profile}, num_operations={num_operations}")
                self.node1.stress(["user", f"profile={profile}", "ops(insert=1)", f"n={num_operations} cl=ALL", "no-warmup", "-rate", "threads=50"])

            if skip_count_checks:
                return num_operations
            else:
                ret = rows_to_list(self.session.execute(f"SELECT COUNT(*) FROM {stress_table}"))[0][0]
                logger.debug(f"Generated {ret} records")
                assert ret >= num_operations, "cassandra-stress did not import enough records"
                return ret

        def run_copy_to(filename):
            logger.debug(f"Exporting to csv file: {filename.name}")
            start = datetime.datetime.now()
            copy_to_cmd = f"CONSISTENCY ALL; COPY {stress_table} TO '{filename.name}'"
            if copy_to_options:
                copy_to_cmd += " WITH " + " AND ".join(f"{k} = {v}" for k, v in copy_to_options.items())
            result = self.node1.run_cqlsh(cmds=copy_to_cmd)
            ret.append(result)
            logger.debug(f"COPY TO took {datetime.datetime.now() - start} to export {num_records} records")

        def run_copy_from(filename):
            logger.debug(f"Importing from csv file: {filename.name}")
            start = datetime.datetime.now()
            copy_from_cmd = f"COPY {stress_table} FROM '{filename.name}'"
            if copy_from_options:
                copy_from_cmd += " WITH " + " AND ".join(f"{k} = {v}" for k, v in copy_from_options.items())
            logger.debug(f"Running {copy_from_cmd}")
            result = self.node1.run_cqlsh(cmds=copy_from_cmd)
            ret.append(result)
            logger.debug(f"COPY FROM took {datetime.datetime.now() - start} to import {num_records} records")

        num_records = create_records()

        # Copy to the first csv files
        tempfile1 = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        run_copy_to(tempfile1)

        # check all records generated were exported
        with open(tempfile1.name, encoding="utf-8", newline="") as csvfile:
            assert num_records == sum(1 for _ in csv.reader(csvfile, quotechar='"', escapechar="\\"))

        # import records from the first csv file
        logger.debug(f"Truncating {stress_table}...")
        self.session.execute(f"TRUNCATE {stress_table}")
        run_copy_from(tempfile1)

        # export again to a second csv file
        tempfile2 = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        run_copy_to(tempfile2)

        # check the length of both files is the same to ensure all exported records were imported
        with open(tempfile1.name, encoding="utf-8") as f1, open(tempfile2.name, encoding="utf-8") as f2:
            f1_lines = sum(1 for _ in f1)
            f2_lines = sum(1 for _ in f2)
        logger.debug(f"f1={f1_lines} f2={f2_lines}")
        assert f1_lines == f2_lines
        return ret

    @pytest.mark.use_cassandra_stress
    def test_bulk_round_trip_default(self):
        """
        Test bulk import with default stress import (one row per operation)

        @jira_ticket CASSANDRA-9302
        """
        self._test_bulk_round_trip(nodes=3, partitioner="murmur3", num_operations=100000)

    @pytest.mark.use_cassandra_stress
    def test_bulk_round_trip_blogposts(self):
        """
        Test bulk import with a user profile that inserts 10 rows per operation

        @jira_ticket CASSANDRA-9302
        """
        self._test_bulk_round_trip(
            nodes=3,
            partitioner="murmur3",
            num_operations=10000,
            configuration_options={"batch_size_warn_threshold_in_kb": "10"},
            profile=os.path.join(os.path.dirname(os.path.realpath(__file__)), "blogposts.yaml"),
            stress_table="stresscql.blogposts",
        )

    @pytest.mark.single_node
    @pytest.mark.use_cassandra_stress
    @pytest.mark.skip_if(issue_open("scylladb/cqlsh-rs#193"))
    def test_copy_to_with_more_failures_than_max_attempts(self):
        """
        Test exporting rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ExportProcess in pylib/copy.py to deviate its behavior from performing normal queries.
        Here we set a token range that will fail more times than the maximum number of attempts, therefore
        we expect this COPY TO job to fail.

        @jira_ticket CASSANDRA-9304
        """
        num_records = 100000
        self.prepare(nodes=1)

        logger.debug("Running stress")
        stress_table = "keyspace1.standard1"
        self.node1.stress(["write", f"n={num_records}", "-rate", "threads=50"])

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        failures = {"failing_range": {"start": 0, "end": 5000000000000000000, "num_failures": 5}}
        os.environ["CQLSH_COPY_TEST_FAILURES"] = json.dumps(failures)

        logger.debug(f"Exporting to csv file: {self.tempfile.name} with {os.environ['CQLSH_COPY_TEST_FAILURES']} and 3 max attempts")
        out, err = self.node1.run_cqlsh(cmds=f"COPY {stress_table} TO '{self.tempfile.name}' WITH MAXATTEMPTS='3'", return_output=True)
        logger.debug(out)
        logger.debug(err)

        copy_output = out + err
        assert COPY_TO_FAILURE_RE.search(copy_output), f"No COPY TO failure reported by cqlsh: '{copy_output}'"

        with open(self.tempfile.name) as file:
            lines_num = len(file.readlines())
        assert lines_num < num_records, f"Expected that lined in the file after copy is less then {num_records}, but got {lines_num}"

    @pytest.mark.single_node
    @pytest.mark.use_cassandra_stress
    @pytest.mark.skip_if(issue_open("scylladb/cqlsh-rs#193"))
    def test_copy_to_with_fewer_failures_than_max_attempts(self):
        """
        Test exporting rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ExportProcess in pylib/copy.py to deviate its behavior from performing normal queries.
        Here we set a token range that will fail fewer times than the maximum number of attempts, therefore
        we expect this COPY TO job to succeed.

        @jira_ticket CASSANDRA-9304
        """
        num_records = 100000
        self.prepare(nodes=1)

        logger.debug("Running stress")
        stress_table = "keyspace1.standard1"
        self.node1.stress(["write", f"n={num_records}", "-rate", "threads=50"])

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        failures = {"failing_range": {"start": 0, "end": 5000000000000000000, "num_failures": 3}}
        os.environ["CQLSH_COPY_TEST_FAILURES"] = json.dumps(failures)
        logger.debug(f"Exporting to csv file: {self.tempfile.name} with {os.environ['CQLSH_COPY_TEST_FAILURES']} and 5 max attempts")
        out, err = self.node1.run_cqlsh(cmds=f"COPY {stress_table} TO '{self.tempfile.name}' WITH MAXATTEMPTS='2'", return_output=True)
        logger.debug(out)
        logger.debug(err)

        copy_output = out + err
        assert COPY_TO_FAILURE_RE.search(copy_output), f"No COPY TO failure reported by cqlsh: '{copy_output}'"

        with open(self.tempfile.name) as file:
            lines_num = len(file.readlines())
        assert lines_num < num_records, f"Expected that lined in the file after copy is less then {num_records}, but got {lines_num}"

    @pytest.mark.single_node
    # Test had history of timing out in debug, see: https://github.com/scylladb/scylla-dtest/issues/3275
    @pytest.mark.skip_mode(mode="debug", reason="test has a history of timing out in debug mode (scylladb/scylla-dtest#3275)")
    @unmark.next_gating  # unmark cause of: https://github.com/scylladb/scylla-cqlsh/issues/37
    @pytest.mark.use_cassandra_stress
    def test_copy_to_with_child_process_crashing(self):
        """
        Test exporting rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ExportProcess in pylib/copy.py to deviate its behavior from performing normal queries.
        Here we set a token range that will cause a child process processing this range to exit, therefore
        we expect this COPY TO job to fail.

        @jira_ticket CASSANDRA-9304
        """
        num_records = 100000
        self.prepare(nodes=1)

        logger.debug("Running stress")
        stress_table = "keyspace1.standard1"
        self.node1.stress(["write", f"n={num_records}", "-rate", "threads=50"])

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        failures = {"exit_range": {"start": 0, "end": 5000000000000000000}}
        os.environ["CQLSH_COPY_TEST_FAILURES"] = json.dumps(failures)

        logger.debug(f"Exporting to csv file: {self.tempfile.name} with {os.environ['CQLSH_COPY_TEST_FAILURES']}")
        out, err = self.node1.run_cqlsh(cmds=f"COPY {stress_table} TO '{self.tempfile.name}'", return_output=True)
        logger.debug(out)
        logger.debug(err)

        assert err.strip(), f"No COPY TO failure reported by cqlsh, stdout: '{out}'"

        with open(self.tempfile.name) as file:
            lines_num = len(file.readlines())
        assert lines_num < num_records, f"Expected that lined in the file after copy is less then {num_records}, but got {lines_num}"

    @pytest.mark.single_node
    @pytest.mark.use_cassandra_stress
    def test_copy_from_with_more_failures_than_max_attempts(self):
        """
        Test importing rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ImportProcess in pylib/copy.py to deviate its behavior from performing normal queries.
        To ensure unique batch ids we must also set the chunk size to one.

        We set a batch id that will cause a batch to fail more times than the maximum number of attempts,
        therefore we expect this COPY TO job to fail.

        @jira_ticket CASSANDRA-9302
        """
        num_records = 1000
        self.prepare(nodes=1)

        logger.debug("Running stress")
        stress_table = "keyspace1.standard1"
        self.node1.stress(["write", f"n={num_records}", "-rate", "threads=50"])

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        logger.debug(f"Exporting to csv file {self.tempfile.name} to generate a file")
        self.node1.run_cqlsh(cmds=f"COPY {stress_table} TO '{self.tempfile.name}'")

        self.session.execute(f"TRUNCATE {stress_table}")

        failures = {"failing_batch": {"id": 30, "failures": 5}}
        os.environ["CQLSH_COPY_TEST_FAILURES"] = json.dumps(failures)
        logger.debug(f"Importing from csv file {self.tempfile.name} with {os.environ['CQLSH_COPY_TEST_FAILURES']}")
        out, err = self.node1.run_cqlsh(cmds=f"COPY {stress_table} FROM '{self.tempfile.name}' WITH CHUNKSIZE='1' AND MAXATTEMPTS='3'", return_output=True)
        logger.debug(out)
        logger.debug(err)

        copy_output = out + err
        assert COPY_FROM_FAILURE_RE.search(copy_output), f"No COPY FROM failure reported by cqlsh: '{copy_output}'"
        assert_row_count_in_select_less(session=self.session, query=f"SELECT COUNT(*) FROM {stress_table}", max_rows_expected=num_records)

    @pytest.mark.single_node
    @pytest.mark.use_cassandra_stress
    @pytest.mark.skip_if(issue_open("scylladb/cqlsh-rs#191"))
    def test_copy_from_with_fewer_failures_than_max_attempts(self):
        """
        Test importing rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ImportProcess in pylib/copy.py to deviate its behavior from performing normal queries.
        To ensure unique batch ids we must also set the chunk size to one.

        We set a batch id that will cause a batch to fail fewer times than the maximum number of attempts,
        therefore we expect this COPY TO job to succeed.

        @jira_ticket CASSANDRA-9302
        """
        num_records = 1000
        self.prepare(nodes=1)

        logger.debug("Running stress")
        stress_table = "keyspace1.standard1"
        self.node1.stress(["write", f"n={num_records}", "-rate", "threads=50"])

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        logger.debug(f"Exporting to csv file {self.tempfile.name} to generate a file")
        self.node1.run_cqlsh(cmds=f"COPY {stress_table} TO '{self.tempfile.name}'")

        self.session.execute(f"TRUNCATE {stress_table}")

        failures = {"failing_batch": {"id": 30, "failures": 3}}
        os.environ["CQLSH_COPY_TEST_FAILURES"] = json.dumps(failures)
        logger.debug(f"Importing from csv file {self.tempfile.name} with {os.environ['CQLSH_COPY_TEST_FAILURES']}")
        out, err = self.node1.run_cqlsh(cmds=f"COPY {stress_table} FROM '{self.tempfile.name}' WITH CHUNKSIZE='1' AND MAXATTEMPTS='5'", return_output=True)
        logger.debug(out)
        logger.debug(err)

        copy_output = out + err
        assert COPY_FROM_FAILURE_RE.search(copy_output), f"No COPY FROM failure reported by cqlsh: '{copy_output}'"

        assert_row_count(session=self.session, table_name=stress_table, expected=num_records)

    @pytest.mark.single_node
    @pytest.mark.use_cassandra_stress
    def test_copy_from_with_child_process_crashing(self):
        """
        Test importing rows with failure injection by setting the environment variable CQLSH_COPY_TEST_FAILURES,
        which is used by ImportProcess in pylib/copy.py to deviate its behavior from performing normal queries.
        To ensure unique batch ids we must also set the chunk size to one.

        We set a batch id that will cause a child process to exit, therefore we expect this COPY TO job to fail.

        @jira_ticket CASSANDRA-9302
        """
        num_records = 1000
        self.prepare(nodes=1)

        logger.debug("Running stress")
        stress_table = "keyspace1.standard1"
        self.node1.stress(["write", f"n={num_records}", "-rate", "threads=50"])

        self.tempfile = NamedTemporaryFile(mode="w+", delete=False, encoding="utf-8")
        logger.debug(f"Exporting to csv file {self.tempfile.name} to generate a file")
        self.node1.run_cqlsh(cmds=f"COPY {stress_table} TO '{self.tempfile.name}'")

        self.session.execute(f"TRUNCATE {stress_table}")

        failures = {"exit_batch": {"id": 30}}
        os.environ["CQLSH_COPY_TEST_FAILURES"] = json.dumps(failures)
        logger.debug(f"Importing from csv file {self.tempfile.name} with {os.environ['CQLSH_COPY_TEST_FAILURES']}")
        out, err = self.node1.run_cqlsh(cmds=f"COPY {stress_table} FROM '{self.tempfile.name}' WITH CHUNKSIZE='1'", return_output=True)
        logger.debug(out)
        logger.debug(err)

        assert err, "no error was found"
        assert_row_count_in_select_less(session=self.session, query=f"SELECT COUNT(*) FROM {stress_table}", max_rows_expected=num_records)
