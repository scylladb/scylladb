import json
import logging
import math
import pprint
import subprocess
import uuid
from decimal import Decimal

import pytest
from dateutil.parser import parse

from cqlsh_tests.cqlsh_copy_tests import CqlshPrepare
from dtest_class import Tester
from tools.data import rows_to_list

logger = logging.getLogger(__name__)


class ScyllaSstable(Tester):
    def _fetch_data_from_json(self, data):
        res = list()
        for partition in data:
            name = partition["key"]["value"]
            values = [int(v["value"]) for k, v in partition["clustering_elements"][0]["columns"].items()]
            values.insert(0, name)
            res.append(values)
        res = [tuple(item) for item in res]
        return res

    def _dump_data(self, table_name):
        logger.debug("Run scylla-sstable dump-data")
        self.node.flush()

        try:
            res = self.node.run_scylla_sstable("dump-data", ["--merge"], keyspace="ks", column_families=[table_name], batch=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"scylla-sstable failed with exit code {e.returncode}, invoked as: {e.cmd}\nstdout: {e.stdout}\nstderr: {e.stderr}")
            raise

        return list(json.loads(res[""][0])["sstables"]["anonymous"])

    def _compare_data(self, src, dst, debug_print=True):
        logger.debug("Compare data")
        if debug_print:
            logger.debug("----------src----------")
            logger.debug(src)
            logger.debug("----------dst----------")
            logger.debug(dst)
        symmetric_diff = set(src) ^ set(dst)
        assert len(symmetric_diff) == 0, f"Destination data set is not same as source. Found difference:\n{symmetric_diff} "


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestScyllaSstableDumpData(ScyllaSstable):
    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_scylla_sstable_basic(self):
        """
        Populate data, run sstabledump, extract data from json
        and compare it with a source
        """
        cluster = self.cluster
        cluster.populate(1).start()
        self.node = cluster.nodelist()[0]

        session = self.patient_cql_connection(self.node)
        session.execute(
            """CREATE KEYSPACE ks
            WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };
        """
        )
        session.execute(
            """CREATE TABLE ks.cf (
            name text PRIMARY KEY,
            value_one int,
            value_two int
            );
        """
        )

        logger.debug("Insert data")
        values_list = [("mary", 1, 12), ("sara", 2, 24), ("mike", 3, 36), ("ted", 4, 48)]
        for values in values_list:
            session.execute(f"INSERT INTO ks.cf (name, value_one, value_two) VALUES {values};")

        data_json = self._dump_data("cf")
        json_values = self._fetch_data_from_json(data_json)
        self._compare_data(values_list, json_values)

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    # Counters are not yet supported with tablets
    @pytest.mark.required_features("!tablets")
    def test_sstabledump_counter_basic(self):
        """
        Populate counter data, run sstabledump, extract data from json
        and compare it with a source
        """
        cluster = self.cluster
        cluster.populate(1).start()
        self.node = cluster.nodelist()[0]

        session = self.patient_cql_connection(self.node)
        session.execute(
            """CREATE KEYSPACE ks
            WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 };
        """
        )
        session.execute(
            """CREATE TABLE ks.cf (
            pk INT PRIMARY KEY,
            cnt COUNTER
            );
        """
        )

        for i in range(1, 10):
            session.execute(f"UPDATE ks.cf SET cnt = cnt + {i} WHERE pk = {i};")
            val = int(math.pow(10, 18))
            session.execute(f"UPDATE ks.cf SET cnt = cnt + {val} WHERE pk = {i};")

        rows = rows_to_list(session.execute("SELECT * FROM ks.cf;"))
        values_list = [tuple(item) for item in rows]

        data_json = self._dump_data("cf")
        json_values = self._fetch_counter_data_from_json(data_json)
        self._compare_data(values_list, json_values)

    @staticmethod
    def _fetch_counter_data_from_json(data):
        res = list()
        for partition in data:
            key = partition["key"]["value"]
            values = []
            for k, v in partition["clustering_elements"][0]["columns"].items():
                counter_value = 0
                for shard in v["value"]:
                    counter_value = counter_value + int(shard["value"])
                values.append(counter_value)
            values.insert(0, int(key))
            res.append(values)
        res = [tuple(item) for item in res]
        return res


class TestScyllaSstableDumpataAllDatatypes(CqlshPrepare, ScyllaSstable):
    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_sstabledump_all_datatypes(self):
        cluster = self.cluster
        cluster.populate(1).start()
        self.all_datatypes_prepare(nodes=1)

        # TODO: apply all the data on scylla-tools-java #24 fix
        data = self.data[:19] + self.data[21:]
        self.session.execute("ALTER TABLE ks.testdatatype DROP t")
        self.session.execute("ALTER TABLE ks.testdatatype DROP u")
        insert_statement = self.session.prepare(
            """INSERT INTO testdatatype (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, v, w)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        )
        self.session.execute(insert_statement, data)

        exp_results = rows_to_list(self.session.execute("SELECT * FROM testdatatype"))
        logger.debug(exp_results)

        self.node = self.node1
        data_json = self._dump_data("testdatatype")
        pp = pprint.PrettyPrinter(indent=2)
        logger.debug(pp.pformat(data_json))

        ex = None
        for add_z in (True, False):
            try:
                json_values = self._fetch_data_from_json(data_json, add_z=add_z)
                self._compare_data(data, json_values)
            except AssertionError as e:
                ex = e
            else:
                break
        else:
            raise ex

    def _fetch_data_from_json(self, data, add_z):
        res = list()
        p = list()
        q = list()
        r = dict()

        def do_parse(v):
            # FIXME: sstabledump converts timestamp to string, appending `Z` at the end to be
            # compliant to ISO 8601, indicating the UTC time.
            # scylla-sstable uses scylla's internal data_type::to_string(), which omits this.
            # This results in dateutil.parser.parse() appending '+000' to the end of the parsed date.
            # We work around this by appending 'z' ourselves here, until this issue is resolved.
            if add_z:
                v = v + "Z"
            return parse(v)

        format_val = {
            "b": lambda v: int(v),
            "c": lambda v: bytearray.fromhex(v),
            "d": lambda v: json.loads(v) if type(v) == str else v,
            "e": lambda v: Decimal(v),
            "f": lambda v: float(v),
            "g": lambda v: float(v),
            "i": lambda v: int(v),
            "j": lambda v: v,
            "k": lambda v: (do_parse(v)),
            "l": lambda v: uuid.UUID(v),
            "m": lambda v: uuid.UUID(v),
            "o": lambda v: int(v),
            "p": lambda v: p.append(int(v["value"]["value"])),
            "q": lambda v: q.append(v["key"]),
            "r": lambda v: r.update({do_parse(v["key"]): v["value"]["value"]}),
            "s": lambda v: tuple([json.loads(val) if i != 1 else val for i, val in enumerate(v.split(":"))]) if type(v) == str else tuple(v),
        }

        for name, item in data[0]["clustering_elements"][0]["columns"].items():
            if name not in ("p", "q", "r"):
                value = item["value"]
                if name in format_val:
                    value = format_val[name](value)
                res.append(value)
            else:
                for cell in item["cells"]:
                    format_val[name](cell)
        res.insert(14, p)
        res.insert(15, set(q))
        res.insert(16, r)
        res.insert(0, data[0]["key"]["value"])
        res = tuple(res)
        return res

    def _compare_data(self, src, dst, debug_print=True):
        logger.debug("Compare data")
        if debug_print:
            logger.debug("----------src----------")
            logger.debug(src)
            logger.debug("----------dst----------")
            logger.debug(dst)

        # TODO: compare all when it will be readable
        for i in range(len(src) - 2):
            if isinstance(dst[i], dict):
                assert dict(src[i]) == dst[i], f"Destination data: {dst[i]} is not same as source: {dict(src[i])}"
            if isinstance(src[i], str) and isinstance(dst[i], bytes):
                assert src[i] == dst[i].decode("utf-8"), f"Destination data: {dst[i].decode('utf-8')} is not same as source: {src[i]}"
            else:
                assert src[i] == dst[i], f"Destination data: {dst[i]} is not same as source: {src[i]}"
