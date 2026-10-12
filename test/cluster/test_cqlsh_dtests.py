#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Scylla's answers to statements sent by cqlsh: non-ASCII map keys and
values, empty values, DESCRIBE of keyspaces, tables, indexes and
materialized views (also re-executed to recreate them), COPY TO and FROM,
int limits, simple statements, comments, a rejected clustering restriction,
and a non-superuser's CREATE USER ... SUPERUSER after LOGIN.

Ported from scylla-dtest cqlsh_tests/cqlsh_tests.py. cqlsh is the client,
as in the dtests; how cqlsh itself parses and prints belongs to
scylla-cqlsh's own tests. The dtests' test methods and their helpers are
kept verbatim in the tester classes below, over a small layer giving them
the dtests' node.run_cqlsh() (commands split at ';' and fed to cqlsh's
standard input), node.nodetool(),
create_ks(), create_cf() and assertions. Like the dtests' nodes, each test
gets a node of its own with vnodes (256 per node), and auto_snapshot off as
the dtests set it; the node is stopped gracefully at the end of the test,
as the dtests' teardown did.
"""

import asyncio
import binascii
import csv
import datetime
import logging
import re
import subprocess
import time
from collections import Counter
from decimal import Decimal
from tempfile import NamedTemporaryFile
from uuid import UUID, uuid4

import pytest
from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement
from packaging.version import Version

from test.cluster.util import stop_servers_gracefully, strip_asan_warnings
from test.cluster.dtest_helpers import start_node_wait_cql
from test import TOP_SRC_DIR
from test.pylib.scylla_cluster_manager import ScyllaClusterManager

logger = logging.getLogger(__name__)

CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256, 'auto_snapshot': False}
# The dtests' cqlsh_options() for cqlsh >= 6.
CREDENTIALS = ["-u", "cassandra", "-p", "cassandra", "--insecure-password-without-warning"]


@pytest.fixture(autouse=True)
async def stop_nodes_gracefully(manager: ScyllaClusterManager):
    """Like the dtests' teardown, stop the nodes gracefully at the end of
    each test: shutdown flushes the memtables and closes the sstables."""
    yield
    await stop_servers_gracefully(manager)


############################################################################
# The dtests' framework, as far as the tests below use it.

class common:
    @staticmethod
    def is_win():
        return False


class Node:
    """A ccm node, as far as the tests use it."""

    def __init__(self, server, exe):
        self.server = server
        self.exe = exe

    def run_cqlsh(self, cmds=None, cqlsh_options=None, return_output=False, timeout=600):
        """ccm's node.run_cqlsh(): the commands split at ';' and written to
        cqlsh's standard input, then quit."""
        options = list(cqlsh_options or [])
        p = subprocess.Popen([str(TOP_SRC_DIR / "bin" / "cqlsh"), *options, self.server.ip_addr, "9042"],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, universal_newlines=True)
        stdin = "".join(cmd.strip() + ";\n" for cmd in (cmds or "").split(";") if cmd.strip()) + "quit;\n"
        output = p.communicate(stdin, timeout=timeout)
        logger.debug("cqlsh %r: %s %s", cmds, output[0], output[1])
        if return_output:
            return output

    def nodetool(self, cmd):
        res = subprocess.run([self.exe, "nodetool", *cmd.split(), "-h", self.server.ip_addr], capture_output=True, text=True)
        assert res.returncode == 0, res.stderr
        return res.stdout, strip_asan_warnings(res.stderr)

    def is_scylla(self):
        return True


class Cluster:
    def __init__(self, nodes):
        self.nodes = nodes

    def nodelist(self):
        return self.nodes

    def version(self):
        return "6.0"


def create_ks(session, name, rf):
    """The dtests' create_ks(): the keyspace, then USE it."""
    session.execute(f"CREATE KEYSPACE {name} WITH replication={{'class':'NetworkTopologyStrategy', 'replication_factor':{rf}}}")
    session.execute(f"USE {name}")


def create_cf(session, name, key_type="varchar", columns=None, compaction_strategy="SizeTieredCompactionStrategy"):
    """The dtests' create_cf() with the options the tests use."""
    additional_columns = "".join(f", {k} {v}" for k, v in (columns or {}).items())
    if not additional_columns:
        query = f"CREATE COLUMNFAMILY {name} (key {key_type}, c varchar, v varchar, PRIMARY KEY(key, c)) WITH comment='test cf'"
    else:
        query = f"CREATE COLUMNFAMILY {name} (key {key_type} PRIMARY KEY{additional_columns}) WITH comment='test cf'"
    query += f" AND compaction = {{'class': '{compaction_strategy}', 'enabled': 'true'}} AND compression = {{}}"
    session.execute(query)
    session.execute(f"SELECT * FROM {name} LIMIT 1")


def rows_to_list(rows):
    return [list(r) for r in rows]


def assert_all(session, query, expected):
    res = rows_to_list(session.execute(query))
    assert res == expected, f"Expected {expected} from {query}, but got {res}"


def assert_none(session, query):
    res = list(session.execute(query))
    assert not res, f"Expected nothing from {query}, but got {res}"


def assert_count_equal(a, b):
    assert Counter(map(tuple, a)) == Counter(map(tuple, b))


def read_barrier(session):
    """The dtests' read_barrier(): dropping a non-existing table (with IF
    EXISTS) issues a read barrier."""
    session.execute("drop table if exists nosuchkeyspace.nosuchtable")


def retry_till_success(fun, timeout=60):
    deadline = time.time() + timeout
    while True:
        try:
            return fun()
        except Exception:  # noqa: BLE001
            if time.time() > deadline:
                raise
            time.sleep(0.25)


def wait_for_view(cluster, session, ks, view, timeout=600):
    """The dtests' wait_for_view(): the view built on every node, per
    system_distributed.view_build_status."""
    query = SimpleStatement(f"SELECT status FROM system_distributed.view_build_status WHERE keyspace_name = '{ks}' AND view_name = '{view}'",
                            consistency_level=ConsistencyLevel.ONE)
    def built():
        statuses = [r.status for r in session.execute(query)]
        assert len(statuses) == len(cluster.nodelist()) and all(s == "SUCCESS" for s in statuses), statuses
    retry_till_success(built, timeout)


class CqlshTester:
    __test__ = False

    def __init__(self, server, exe, session):
        self.node1 = Node(server, exe)
        self.cluster = Cluster([self.node1])
        self.session = session
        self.scylla_features = set()
        self.default_compaction_strategy = r"(IncrementalCompactionStrategy|SizeTieredCompactionStrategy)"
        self.default_compressor = r"(org.apache.cassandra.io.compress.LZ4Compressor|LZ4WithDictsCompressor)"

    def cqlsh_options(self, request_timeout_sec: int | None = None) -> list:
        opts = list(CREDENTIALS)
        if request_timeout_sec:
            opts += ["--request-timeout", str(request_timeout_sec)]
        return opts


############################################################################
# The dtests' test methods and helpers, verbatim.

class CqlshDtest(CqlshTester):
    normalize_numbers_re = re.compile(r"\b(\d+)\.0\b")
    normalize_operators_re = re.compile(r"\s*(\\?[:.,=(){}])\s*")
    normalize_whitespaces_re = re.compile(r"\s+")
    normalize_select_columns_re = re.compile(r"state,\s*username,\s*birth_year,\s*gender,\s*password,\s*session_token")
    normalize_primary_key_re = re.compile(r"(?P<open>\\?\()(?P<key>\w+) (?P<type>\w+) PRIMARY KEY(?P<cols>,[^\\)]+)(?P<close>\\?\))")

    def verify_glass(self, node):
        session = self.session

        def verify_varcharmap(map_name, expected):
            rows = list(session.execute("SELECT %s FROM testks.varcharmaptable WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';" % map_name))

            got = {k: v for k, v in rows[0][0].items()}
            assert got == expected

        verify_varcharmap(
            "varcharasciimap", {"Vitrum edere possum, mihi non nocet.": "Hello", " ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑": "My", "Можам да јадам стакло, а не ме штета.": "Name", "I can eat glass and it does not hurt me": "Is"}
        )

        verify_varcharmap("varcharbigintmap", {"Vitrum edere possum, mihi non nocet.": 5100003, " ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑": -45, "Можам да јадам стакло, а не ме штета.": 12300, "I can eat glass and it does not hurt me": 0})

        verify_varcharmap(
            "varcharblobmap",
            {
                "Vitrum edere possum, mihi non nocet.": binascii.a2b_hex("FEED103A"),
                " ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑": binascii.a2b_hex("DEADBEEF"),
                "Можам да јадам стакло, а не ме штета.": binascii.a2b_hex("BEEFBEEF"),
                "I can eat glass and it does not hurt me": binascii.a2b_hex("FEEB"),
            },
        )

        verify_varcharmap(
            "varcharbooleanmap", {"Vitrum edere possum, mihi non nocet.": True, " ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑": False, "Можам да јадам стакло, а не ме штета.": False, "I can eat glass and it does not hurt me": False}
        )

        verify_varcharmap(
            "varchardecimalmap",
            {
                "Vitrum edere possum, mihi non nocet.": Decimal("50"),
                " ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑": Decimal("-20.4"),
                "Можам да јадам стакло, а не ме штета.": Decimal("11234234.3"),
                "I can eat glass and it does not hurt me": Decimal("10.0"),
            },
        )

        verify_varcharmap(
            "varchardoublemap", {"Vitrum edere possum, mihi non nocet.": 4234243, " ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑": -432.311, "Можам да јадам стакло, а не ме штета.": 3.1415, "I can eat glass and it does not hurt me": 20000.0}
        )

        verify_varcharmap(
            "varcharfloatmap", {"Vitrum edere possum, mihi non nocet.": 10.0, " ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑": -234.3000030517578, "Можам да јадам стакло, а не ме штета.": -234234, "I can eat glass and it does not hurt me": 1000.5}
        )

        verify_varcharmap("varcharintmap", {"Vitrum edere possum, mihi non nocet.": 1, " ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑": 2, "Можам да јадам стакло, а не ме штета.": -3, "I can eat glass and it does not hurt me": -500})

        verify_varcharmap(
            "varcharinetmap",
            {"Vitrum edere possum, mihi non nocet.": "192.168.0.1", " ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑": "127.0.0.1", "Можам да јадам стакло, а не ме штета.": "8.8.8.8", "I can eat glass and it does not hurt me": "8.8.4.4"},
        )

        verify_varcharmap(
            "varchartextmap",
            {"Vitrum edere possum, mihi non nocet.": "Once I went", " ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑": "On a trip", "Можам да јадам стакло, а не ме штета.": "Across", "I can eat glass and it does not hurt me": "The "},
        )

        verify_varcharmap(
            "varchartimestampmap",
            {
                "Vitrum edere possum, mihi non nocet.": datetime.datetime(2013, 6, 19, 3, 21, 1),
                " ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑": datetime.datetime(1985, 8, 3, 4, 21, 1),
                "Можам да јадам стакло, а не ме штета.": datetime.datetime(2000, 1, 1, 0, 20, 1),
                "I can eat glass and it does not hurt me": datetime.datetime(1942, 3, 11, 5, 21, 1),
            },
        )

        verify_varcharmap(
            "varcharuuidmap",
            {
                "Vitrum edere possum, mihi non nocet.": UUID("7787064c-ce54-4324-abdd-05775b89ead7"),
                " ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑": UUID("1df0b6ac-f3d3-456c-8b78-2bc70e585107"),
                "Можам да јадам стакло, а не ме штета.": UUID("e2ed2164-31dc-42cb-8ee9-47376e071210"),
                "I can eat glass and it does not hurt me": UUID("a487fe45-8af5-4454-ac66-2614286d7e89"),
            },
        )

        verify_varcharmap(
            "varchartimeuuidmap",
            {
                "Vitrum edere possum, mihi non nocet.": UUID("4a36c100-d8ec-11e2-a28f-0800200c9a66"),
                " ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑": UUID("670c7f90-d8ec-11e2-a28f-0800200c9a66"),
                "Можам да јадам стакло, а не ме штета.": UUID("750c2d70-d8ec-11e2-a28f-0800200c9a66"),
                "I can eat glass and it does not hurt me": UUID("80d74810-d8ec-11e2-a28f-0800200c9a66"),
            },
        )

        verify_varcharmap(
            "varcharvarcharmap",
            {
                "Vitrum edere possum, mihi non nocet.": "᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜",
                " ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑": " ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑",
                "Можам да јадам стакло, а не ме штета.": "Можам да јадам стакло, а не ме штета.",
                "I can eat glass and it does not hurt me": "I can eat glass and it does not hurt me",
            },
        )

        verify_varcharmap(
            "varcharvarintmap",
            {"Vitrum edere possum, mihi non nocet.": 1010010101020400204143243, " ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑": -40, "Можам да јадам стакло, а не ме штета.": 110230, "I can eat glass and it does not hurt me": 1400},
        )

        output, _err = node.run_cqlsh(return_output=True, cmds="use testks; SELECT * FROM varcharmaptable", cqlsh_options=[*self.cqlsh_options(), "--encoding=utf-8"])

        assert output.count("Можам да јадам стакло, а не ме штета.") == 16
        assert output.count(" ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑") == 16
        assert output.count("᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜") == 2

    def verify_output(self, query, node, expected):
        output, _err = node.run_cqlsh(query, cqlsh_options=self.cqlsh_options(), return_output=True)
        if common.is_win():
            output = output.replace("\r", "")
        logger.debug(output)
        self.check_response(expected_response=expected, response=output)

    def get_keyspace_output(self):
        tablets_enabled_str = str("tablets" in self.scylla_features).lower()
        create_ks_re = (
            rf"CREATE KEYSPACE test WITH replication = {{'class': 'NetworkTopologyStrategy', 'datacenter1': ('1'|\['rack1'\])}} "
            rf"AND durable_writes = true( AND tablets = {{'enabled': {tablets_enabled_str}}})?;"
        )
        return create_ks_re + self.get_test_table_output() + self.get_users_table_output()

    def get_test_table_output(self, has_val=True, has_val_idx=True):
        if has_val:
            ret = r"""
                CREATE TABLE test.test \(
                    id int,
                    col int,
                    val text,
                PRIMARY KEY \(id, col\)\)
                """
        else:
            ret = r"""
                CREATE TABLE test.test \(
                    id int,
                    col int,
                PRIMARY KEY \(id, col\)\)
                """

        if self.node1.is_scylla():
            ret += rf"""
         WITH CLUSTERING ORDER BY \(col ASC\)
            AND bloom_filter_fp_chance = 0.01
            AND caching = {{'keys': 'ALL', 'rows_per_partition': 'ALL'}}
            AND comment = ''
            AND compaction = {{'class': '{self.default_compaction_strategy}'}}
            AND compression = {{'sstable_compression': '{self.default_compressor}'}}
            AND crc_check_chance = 1.0
            (\s*AND dclocal_read_repair_chance=0)?
            AND default_time_to_live = 0
            AND gc_grace_seconds = 864000
            AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0
            AND min_index_interval = 128
            (\s*AND read_repair_chance=0)?
            AND speculative_retry = '99.0PERCENTILE';
        """
        elif Version(self.cluster.version()) >= Version("3.0"):
            ret += r"""
         WITH CLUSTERING ORDER BY \(col ASC\)
            AND bloom_filter_fp_chance = 0.01
            AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
            AND comment = ''
            AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
            AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
            AND crc_check_chance = 1.0
            AND default_time_to_live = 0
            AND gc_grace_seconds = 864000
            AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0
            AND min_index_interval = 128
            AND speculative_retry = '99PERCENTILE';
        """
        else:
            ret += r"""
         WITH CLUSTERING ORDER BY \(col ASC\)
            AND bloom_filter_fp_chance = 0.01
            AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
            AND comment = ''
            AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
            AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
            AND default_time_to_live = 0
            AND gc_grace_seconds = 864000
            AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0
            AND min_index_interval = 128
            AND speculative_retry = '99.0PERCENTILE';
        """

        col_idx_def = self.get_index_output("test_col_idx", "test", "test", "col")

        if has_val_idx:
            val_idx_def = self.get_index_output("test_val_idx", "test", "test", "val")
            if self.node1.is_scylla():
                return ret + "\n" + col_idx_def + "\n" + val_idx_def + "\n"
            elif Version(self.cluster.version()) >= Version("2.2"):
                return ret + "\n" + val_idx_def + "\n" + col_idx_def
            else:
                return ret + "\n" + col_idx_def + "\n" + val_idx_def
        else:
            return ret + "\n" + col_idx_def

    def get_users_table_output(self):
        if self.node1.is_scylla():
            return rf"""
            CREATE TABLE test.users \(
            userid text,
            age int,
            firstname text,
            lastname text,
            PRIMARY KEY\(userid\)
            \) WITH bloom_filter_fp_chance = 0.01
            AND caching = {{'keys': 'ALL', 'rows_per_partition': 'ALL'}}
            AND comment = ''
            AND compaction = {{'class': '{self.default_compaction_strategy}'}}
            AND compression = {{'sstable_compression': '{self.default_compressor}'}}
            AND crc_check_chance = 1.0
            (\s*AND dclocal_read_repair_chance=0)?
            AND default_time_to_live = 0
            AND gc_grace_seconds = 864000
            AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0
            AND min_index_interval = 128
            (\s*AND read_repair_chance=0)?
            AND speculative_retry = '99.0PERCENTILE';
        """ + self.get_index_output("myindex", "test", "users", "age")
        elif Version(self.cluster.version()) >= Version("3.0"):
            return r"""
        CREATE TABLE test.users \(
            userid text PRIMARY KEY,
            age int,
            firstname text,
            lastname text
        \) WITH bloom_filter_fp_chance = 0.01
            AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
            AND comment = ''
            AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
            AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
            AND crc_check_chance = 1.0
            AND default_time_to_live = 0
            AND gc_grace_seconds = 864000
            AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0
            AND min_index_interval = 128
            AND speculative_retry = '99PERCENTILE';
        """ + self.get_index_output("myindex", "test", "users", "age")
        else:
            return r"""
        CREATE TABLE test.users \(
            userid text PRIMARY KEY,
            age int,
            firstname text,
            lastname text
        \) WITH bloom_filter_fp_chance = 0.01
            AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
            AND comment = ''
            AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
            AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
            AND default_time_to_live = 0
            AND gc_grace_seconds = 864000
            AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0
            AND min_index_interval = 128
            AND speculative_retry = '99.0PERCENTILE';
        """ + self.get_index_output("myindex", "test", "users", "age")

    def get_index_output(self, index, ks, table, col):
        return rf"CREATE INDEX {index} ON {ks}.{table} \({col}\);"

    def get_mv_output(self, index, ks, table, col, _id, has_val_idx=False):
        if has_val_idx:
            mv = rf"""
            CREATE MATERIALIZED VIEW {ks}.{index}_index AS
            SELECT {col}, idx_token, {_id}, col
            FROM {ks}.{table}
            WHERE {col} IS NOT NULL
            PRIMARY KEY \({col}, idx_token, {_id}, col\)
            WITH CLUSTERING ORDER BY \(idx_token ASC, {_id} ASC, col ASC\)
            """
        else:
            mv = rf"""
            CREATE MATERIALIZED VIEW {ks}.{index}_index AS
            SELECT {col}, idx_token, {_id}
            FROM {ks}.{table}
            WHERE {col} IS NOT NULL
            PRIMARY KEY \({col}, idx_token, {_id}\)
            WITH CLUSTERING ORDER BY \(idx_token ASC, {_id} ASC\)
            """
        return rf"""{mv}
            AND bloom_filter_fp_chance = 0.01
            AND caching = {{'keys': 'ALL', 'rows_per_partition': 'ALL'}}
            AND comment = ''
            AND compaction = {{'class': '{self.default_compaction_strategy}'}}
            AND compression = {{'sstable_compression': '{self.default_compressor}'}}
            AND crc_check_chance = 1.0
            AND default_time_to_live = 0
            AND gc_grace_seconds = 864000
            AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0
            AND min_index_interval = 128
            AND speculative_retry = '99.0PERCENTILE';
        """

    def get_users_by_state_mv_output(self):
        if self.node1.is_scylla():
            return rf"""
                CREATE MATERIALIZED VIEW test.users_by_state AS
                SELECT \*
                FROM test.users
                WHERE state IS NOT null AND username IS NOT null
                PRIMARY KEY \(state, username\)
                WITH CLUSTERING ORDER BY \(username ASC\)
                AND bloom_filter_fp_chance = 0.01
                AND caching = {{'keys': 'ALL', 'rows_per_partition': 'ALL'}}
                AND comment = ''
                AND compaction = {{'class': '{self.default_compaction_strategy}'}}
                AND compression = {{'sstable_compression': '{self.default_compressor}'}}
                AND crc_check_chance = 1.0
                (\s*AND dclocal_read_repair_chance=0)?
                AND default_time_to_live = 0
                AND gc_grace_seconds = 864000
                AND max_index_interval = 2048
                AND memtable_flush_period_in_ms = 0
                AND min_index_interval = 128
                (\s*AND read_repair_chance=0)?
                AND speculative_retry = '99.0PERCENTILE'
                (\s*AND tombstone_gc={{'mode':'timeout','propagation_delay_in_seconds':'3600'}})?;
            """
        else:
            return r"""
                    CREATE MATERIALIZED VIEW test.users_by_state AS
                    SELECT \*
                    FROM test.users
                    WHERE state IS NOT NULL AND username IS NOT NULL
                    PRIMARY KEY \(state, username\)
                    WITH CLUSTERING ORDER BY \(username ASC\)
                    AND bloom_filter_fp_chance = 0.01
                    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
                    AND comment = ''
                    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
                    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
                    AND crc_check_chance = 1.0
                    AND default_time_to_live = 0
                    AND gc_grace_seconds = 864000
                    AND max_index_interval = 2048
                    AND memtable_flush_period_in_ms = 0
                    AND min_index_interval = 128
                    AND speculative_retry = '99PERCENTILE';
                   """

    def execute(self, cql, expected_output=None, expected_err=None, request_timeout_sec: int | None = None):
        logger.debug(cql)
        (node1,) = self.cluster.nodelist()
        output, _err = node1.run_cqlsh(cql, cqlsh_options=self.cqlsh_options(request_timeout_sec), return_output=True)

        if expected_output:
            self.check_response(output, expected_output)

        return output

    def _normalize_response(self, response, regex=False):
        def normalize_line(line):
            # accept both org.apache.cassandra.locator.NetworkTopologyStrategy and NetworkTopologyStrategy
            line = line.replace("org.apache.cassandra.locator.", "")
            # normalize 0.0, 1.0 etc to 0, 1 etc.
            line = self.normalize_numbers_re.sub(r"\1", line)
            # enforce formatting without whitespace around common operators
            line = self.normalize_operators_re.sub(r"\1", line)
            line = self.normalize_whitespaces_re.sub(" ", line)
            line = self.normalize_select_columns_re.sub(r"\\*" if regex else "*", line)
            line = line.rstrip(";,:")
            m = self.normalize_primary_key_re.search(line)
            if m:
                parens = [r"\(", r"\)"] if regex else ["(", ")"]
                line = rf"{line[: m.span()[0]]}{m.group('open')}{m.group('key')} {m.group('type')}{m.group('cols')},PRIMARY KEY{parens[0]}{m.group('key')}{parens[1]}{m.group('close')}{line[m.span()[1] :]}"
            return line

        return [line for line in [normalize_line(l.strip()) for l in response.split(";")] if line]

    def check_response(self, response, expected_response):
        lines = self._normalize_response(response)
        expected_exprs = [re.compile(ex) for ex in self._normalize_response(expected_response, regex=True)]
        for exp in expected_exprs:
            found = any(exp.search(line) for line in lines)
            assert found, f"Output lines \n {{{lines}}} \n doesn't contain expected line: {exp}"

    def test_eat_glass(self):
        (node1,) = self.cluster.nodelist()

        node1.run_cqlsh(
            cqlsh_options=self.cqlsh_options(),
            cmds="""create KEYSPACE testks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
use testks;

CREATE TABLE varcharmaptable (
        varcharkey varchar ,
        varcharasciimap map<varchar, ascii>,
        varcharbigintmap map<varchar, bigint>,
        varcharblobmap map<varchar, blob>,
        varcharbooleanmap map<varchar, boolean>,
        varchardecimalmap map<varchar, decimal>,
        varchardoublemap map<varchar, double>,
        varcharfloatmap map<varchar, float>,
        varcharintmap map<varchar, int>,
        varcharinetmap map<varchar, inet>,
        varchartextmap map<varchar, text>,
        varchartimestampmap map<varchar, timestamp>,
        varcharuuidmap map<varchar, uuid>,
        varchartimeuuidmap map<varchar, timeuuid>,
        varcharvarcharmap map<varchar, varchar>,
        varcharvarintmap map<varchar, varint>,
        PRIMARY KEY (varcharkey));

INSERT INTO varcharmaptable (varcharkey, varcharasciimap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': 'My','Можам да јадам стакло, а не ме штета.': 'Name','I can eat glass and it does not hurt me': 'Is'} );

UPDATE varcharmaptable SET varcharasciimap = varcharasciimap + {'Vitrum edere possum, mihi non nocet.':'Cassandra'} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharasciimap['Vitrum edere possum, mihi non nocet.'] = 'Hello' WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharbigintmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': -45,'Можам да јадам стакло, а не ме штета.': 12300,'I can eat glass and it does not hurt me': 0} );

UPDATE varcharmaptable SET varcharbigintmap = varcharbigintmap + {'Vitrum edere possum, mihi non nocet.':23} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharbigintmap['Vitrum edere possum, mihi non nocet.'] = 5100003 WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharblobmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': 0xDEADBEEF,'Можам да јадам стакло, а не ме штета.': 0xBEEFBEEF,'I can eat glass and it does not hurt me': 0xFEEB} );

UPDATE varcharmaptable SET varcharblobmap = varcharblobmap + {'Vitrum edere possum, mihi non nocet.':0x10} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharblobmap['Vitrum edere possum, mihi non nocet.'] = 0xFEED103A WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharbooleanmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': FALSE,'Можам да јадам стакло, а не ме штета.': FALSE,'I can eat glass and it does not hurt me': FALSE} );

UPDATE varcharmaptable SET varcharbooleanmap = varcharbooleanmap + {'Vitrum edere possum, mihi non nocet.':TRUE} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharbooleanmap['Vitrum edere possum, mihi non nocet.'] = TRUE WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varchardecimalmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': -20.4,'Можам да јадам стакло, а не ме штета.': 11234234.3,'I can eat glass and it does not hurt me': 10.0} );

UPDATE varcharmaptable SET varchardecimalmap = varchardecimalmap + {'Vitrum edere possum, mihi non nocet.':0.0} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varchardecimalmap['Vitrum edere possum, mihi non nocet.'] = 50.0 WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varchardoublemap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': -432.311,'Можам да јадам стакло, а не ме штета.': 3.1415,'I can eat glass and it does not hurt me': 20000.0} );

UPDATE varcharmaptable SET varchardoublemap = varchardoublemap + {'Vitrum edere possum, mihi non nocet.':11} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varchardoublemap['Vitrum edere possum, mihi non nocet.'] = 4234243 WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharfloatmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': -234.3,'Можам да јадам стакло, а не ме штета.': -234234,'I can eat glass and it does not hurt me': 1000.5} );

UPDATE varcharmaptable SET varcharfloatmap = varcharfloatmap + {'Vitrum edere possum, mihi non nocet.':-3.14} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharfloatmap['Vitrum edere possum, mihi non nocet.'] = 10.0 WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharintmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': 2,'Можам да јадам стакло, а не ме штета.': -3,'I can eat glass and it does not hurt me': -500} );

UPDATE varcharmaptable SET varcharintmap = varcharintmap + {'Vitrum edere possum, mihi non nocet.':20000} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharintmap['Vitrum edere possum, mihi non nocet.'] = 1 WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharinetmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': '127.0.0.1','Можам да јадам стакло, а не ме штета.': '8.8.8.8','I can eat glass and it does not hurt me': '8.8.4.4'} );

UPDATE varcharmaptable SET varcharinetmap = varcharinetmap + {'Vitrum edere possum, mihi non nocet.':'241.30.12.24'} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharinetmap['Vitrum edere possum, mihi non nocet.'] = '192.168.0.1' WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varchartextmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': 'On a trip','Можам да јадам стакло, а не ме штета.': 'Across','I can eat glass and it does not hurt me': 'The '} );

UPDATE varcharmaptable SET varchartextmap = varchartextmap + {'Vitrum edere possum, mihi non nocet.':'Sea'} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varchartextmap['Vitrum edere possum, mihi non nocet.'] = 'Once I went' WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varchartimestampmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': '1985-08-03T04:21:01+0000','Можам да јадам стакло, а не ме штета.': '2000-01-01T00:20:01+0000','I can eat glass and it does not hurt me': '1942-03-11T5:21:01+0000'} );

UPDATE varcharmaptable SET varchartimestampmap = varchartimestampmap + {'Vitrum edere possum, mihi non nocet.':'2043-11-04T11:21:01+0000'} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varchartimestampmap['Vitrum edere possum, mihi non nocet.'] = '2013-06-19T03:21:01+0000' WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharuuidmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': 1df0b6ac-f3d3-456c-8b78-2bc70e585107,'Можам да јадам стакло, а не ме штета.': e2ed2164-31dc-42cb-8ee9-47376e071210,'I can eat glass and it does not hurt me': a487fe45-8af5-4454-ac66-2614286d7e89} );

UPDATE varcharmaptable SET varcharuuidmap = varcharuuidmap + {'Vitrum edere possum, mihi non nocet.':d25bdfc7-eb81-472c-bf5b-b4e6afdf66c2} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharuuidmap['Vitrum edere possum, mihi non nocet.'] = 7787064c-ce54-4324-abdd-05775b89ead7 WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varchartimeuuidmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': 670c7f90-d8ec-11e2-a28f-0800200c9a66,'Можам да јадам стакло, а не ме штета.': 750c2d70-d8ec-11e2-a28f-0800200c9a66,'I can eat glass and it does not hurt me': 80d74810-d8ec-11e2-a28f-0800200c9a66} );

UPDATE varcharmaptable SET varchartimeuuidmap = varchartimeuuidmap + {'Vitrum edere possum, mihi non nocet.':93e276f0-d8ec-11e2-a28f-0800200c9a66} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varchartimeuuidmap['Vitrum edere possum, mihi non nocet.'] = 4a36c100-d8ec-11e2-a28f-0800200c9a66 WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharvarcharmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': ' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑','Можам да јадам стакло, а не ме штета.': 'Можам да јадам стакло, а не ме штета.','I can eat glass and it does not hurt me': 'I can eat glass and it does not hurt me'} );

UPDATE varcharmaptable SET varcharvarcharmap = varcharvarcharmap + {'Vitrum edere possum, mihi non nocet.':'Vitrum edere possum, mihi non nocet.'} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharvarcharmap['Vitrum edere possum, mihi non nocet.'] = '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜' WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

INSERT INTO varcharmaptable (varcharkey, varcharvarintmap ) VALUES      ('᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜',  {' ⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑': -40,'Можам да јадам стакло, а не ме штета.': 110230,'I can eat glass and it does not hurt me': 1400} );

UPDATE varcharmaptable SET varcharvarintmap = varcharvarintmap + {'Vitrum edere possum, mihi non nocet.':20000} WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜';

UPDATE varcharmaptable SET varcharvarintmap['Vitrum edere possum, mihi non nocet.'] = 1010010101020400204143243 WHERE varcharkey= '᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜'
        """,
        )

        self.verify_glass(node1)

    def test_with_empty_values(self):
        """
        CASSANDRA-7196. Make sure the server returns empty values and CQLSH prints them properly
        """
        (node1,) = self.cluster.nodelist()

        node1.run_cqlsh(
            return_output=True,
            cqlsh_options=self.cqlsh_options(),
            cmds="""create keyspace  CASSANDRA_7196 WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} ;

use CASSANDRA_7196;

CREATE TABLE has_all_types (
    num int PRIMARY KEY,
    intcol int,
    asciicol ascii,
    bigintcol bigint,
    blobcol blob,
    booleancol boolean,
    decimalcol decimal,
    doublecol double,
    floatcol float,
    textcol text,
    timestampcol timestamp,
    uuidcol uuid,
    varcharcol varchar,
    varintcol varint
) WITH compression = {'sstable_compression':'LZ4Compressor'};

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, textcol,
                           timestampcol, uuidcol, varcharcol, varintcol)
VALUES (0, -12, 'abcdefg', 1234567890123456789, 0x000102030405fffefd, true,
        19952.11882, 1.0, -2.1, 'Voilá!', '2012-05-14 12:53:20+0000',
        bd1924e1-6af8-44ae-b5e1-f24131dbd460, '"', 10000000000000000000000000);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, textcol,
                           timestampcol, uuidcol, varcharcol, varintcol)
VALUES (1, 2147483647, '__!''$#@!~"', 9223372036854775807, 0xffffffffffffffffff, true,
        0.00000000000001, 9999999.999, 99999.99, '∭Ƕ⑮ฑ➳❏''', '1900-01-01+0000',
        ffffffff-ffff-ffff-ffff-ffffffffffff, 'newline->
<-', 9);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, textcol,
                           timestampcol, uuidcol, varcharcol, varintcol)
VALUES (2, 0, '', 0, 0x, false,
        0.0, 0.0, 0.0, '', 0,
        00000000-0000-0000-0000-000000000000, '', 0);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, textcol,
                           timestampcol, uuidcol, varcharcol, varintcol)
VALUES (3, -2147483648, '''''''', -9223372036854775808, 0x80, false,
        10.0000000000000, -1004.10, 100000000.9, '龍馭鬱', '2038-01-19T03:14-1200',
        ffffffff-ffff-1fff-8fff-ffffffffffff, '''', -10000000000000000000000000);

INSERT INTO has_all_types (num, intcol, asciicol, bigintcol, blobcol, booleancol,
                           decimalcol, doublecol, floatcol, textcol,
                           timestampcol, uuidcol, varcharcol, varintcol)
VALUES (4, blobAsInt(0x), '', blobAsBigint(0x), 0x, blobAsBoolean(0x), blobAsDecimal(0x),
        blobAsDouble(0x), blobAsFloat(0x), '', blobAsTimestamp(0x), blobAsUuid(0x), '',
        blobAsVarint(0x))""",
        )

        output, _err = node1.run_cqlsh(return_output=True, cqlsh_options=self.cqlsh_options(), cmds="select intcol, bigintcol, varintcol from CASSANDRA_7196.has_all_types where num in (0, 1, 2, 3, 4)")
        if common.is_win():
            output = output.replace("\r", "")

        expected = """
 intcol      | bigintcol            | varintcol
-------------+----------------------+-----------------------------
         -12 |  1234567890123456789 |  10000000000000000000000000
  2147483647 |  9223372036854775807 |                           9
           0 |                    0 |                           0
 -2147483648 | -9223372036854775808 | -10000000000000000000000000
             |                      |

(5 rows)"""

        def _rstrip_lines(s):
            return "\n".join(line.rstrip() for line in s.split("\n"))

        assert _rstrip_lines(expected) in _rstrip_lines(output), f"Output \n {{{output}}} \n doesn't contain expected\n {{{expected}}}"

    def test_describe(self):  # noqa: PLR0915
        """
        @jira_ticket CASSANDRA-7814
        """

        self.execute(
            cql="""
                CREATE KEYSPACE test WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1};
                CREATE TABLE test.users ( userid text PRIMARY KEY, firstname text, lastname text, age int);
                CREATE INDEX myindex ON test.users (age);
                CREATE TABLE test.test (id int, col int, val text, PRIMARY KEY(id, col));
                CREATE INDEX ON test.test (col);
                CREATE INDEX ON test.test (val)
                """,
            request_timeout_sec=60,
        )

        # Describe keyspaces
        output = self.execute(cql="DESCRIBE KEYSPACES")
        assert "test" in output
        assert "system" in output

        # Describe keyspace
        self.execute(cql="DESCRIBE KEYSPACE test", expected_output=self.get_keyspace_output())
        self.execute(cql="DESCRIBE test", expected_output=self.get_keyspace_output())
        self.execute(cql="DESCRIBE test2", expected_err="'test2' not found in keyspaces")
        self.execute(cql="USE test; DESCRIBE KEYSPACE", expected_output=self.get_keyspace_output())

        # Describe table
        self.execute(cql="DESCRIBE TABLE test.test", expected_output=self.get_test_table_output())
        self.execute(cql="DESCRIBE TABLE test.users", expected_output=self.get_users_table_output())
        self.execute(cql="DESCRIBE test.test", expected_output=self.get_test_table_output())
        self.execute(cql="DESCRIBE test.users", expected_output=self.get_users_table_output())
        self.execute(cql="DESCRIBE test.users2", expected_err="'users2' not found in keyspace 'test'")
        self.execute(cql="USE test; DESCRIBE TABLE test", expected_output=self.get_test_table_output())
        self.execute(cql="USE test; DESCRIBE TABLE users", expected_output=self.get_users_table_output())
        self.execute(cql="USE test; DESCRIBE test", expected_output=self.get_keyspace_output())
        self.execute(cql="USE test; DESCRIBE users", expected_output=self.get_users_table_output())
        self.execute(cql="USE test; DESCRIBE users2", expected_err="'users2' not found in keyspace 'test'")

        # Describe index
        self.execute(cql="DESCRIBE INDEX test.myindex", expected_output=self.get_index_output("myindex", "test", "users", "age"))
        self.execute(cql="DESCRIBE INDEX test.test_col_idx", expected_output=self.get_index_output("test_col_idx", "test", "test", "col"))
        self.execute(cql="DESCRIBE INDEX test.test_val_idx", expected_output=self.get_index_output("test_val_idx", "test", "test", "val"))
        self.execute(cql="DESCRIBE test.myindex", expected_output=self.get_index_output("myindex", "test", "users", "age"))
        self.execute(cql="DESCRIBE test.test_col_idx", expected_output=self.get_index_output("test_col_idx", "test", "test", "col"))
        self.execute(cql="DESCRIBE test.test_val_idx", expected_output=self.get_index_output("test_val_idx", "test", "test", "val"))
        self.execute(cql="DESCRIBE test.myindex2", expected_err="'myindex2' not found in keyspace 'test'")
        self.execute(cql="USE test; DESCRIBE INDEX myindex", expected_output=self.get_index_output("myindex", "test", "users", "age"))
        self.execute(cql="USE test; DESCRIBE INDEX test_col_idx", expected_output=self.get_index_output("test_col_idx", "test", "test", "col"))
        self.execute(cql="USE test; DESCRIBE INDEX test_val_idx", expected_output=self.get_index_output("test_val_idx", "test", "test", "val"))
        self.execute(cql="USE test; DESCRIBE myindex", expected_output=self.get_index_output("myindex", "test", "users", "age"))
        self.execute(cql="USE test; DESCRIBE test_col_idx", expected_output=self.get_index_output("test_col_idx", "test", "test", "col"))
        self.execute(cql="USE test; DESCRIBE test_val_idx", expected_output=self.get_index_output("test_val_idx", "test", "test", "val"))
        self.execute(cql="USE test; DESCRIBE myindex2", expected_err="'myindex2' not found in keyspace 'test'")

        # Drop table and recreate
        self.execute(cql="DROP TABLE test.users", request_timeout_sec=60)
        self.execute(cql="DESCRIBE test.users", expected_err="'users' not found in keyspace 'test'")
        self.execute(cql="DESCRIBE test.myindex", expected_err="'myindex' not found in keyspace 'test'")
        self.execute(
            cql="""
                CREATE TABLE test.users ( userid text PRIMARY KEY, firstname text, lastname text, age int);
                CREATE INDEX myindex ON test.users (age)
                """,
            request_timeout_sec=60,
        )
        self.execute(cql="DESCRIBE test.users", expected_output=self.get_users_table_output())
        self.execute(cql="DESCRIBE test.myindex", expected_output=self.get_index_output("myindex", "test", "users", "age"))

        # Drop index and recreate
        self.execute(cql="DROP INDEX test.myindex", request_timeout_sec=60)
        self.execute(cql="DESCRIBE test.myindex", expected_err="'myindex' not found in keyspace 'test'")
        self.execute(cql="CREATE INDEX myindex ON test.users (age)", request_timeout_sec=60)
        self.execute(cql="DESCRIBE INDEX test.myindex", expected_output=self.get_index_output("myindex", "test", "users", "age"))

        if not self.node1.is_scylla():  # scylla doesn't support removing columns that are in use
            # Alter table. Renaming indexed columns is not allowed, and since 3.0 neither is dropping them
            # Prior to 3.0 the index would have been automatically dropped, but now we need to explicitly do that.
            self.execute(cql="DROP INDEX test.test_val_idx")
            self.execute(cql="ALTER TABLE test.test DROP val")
            self.execute(cql="DESCRIBE test.test", expected_output=self.get_test_table_output(has_val=False, has_val_idx=False))
            self.execute(cql="DESCRIBE test.test_val_idx", expected_err="'test_val_idx' not found in keyspace 'test'")
            self.execute(cql="ALTER TABLE test.test ADD val text")
            self.execute(cql="DESCRIBE test.test", expected_output=self.get_test_table_output(has_val=True, has_val_idx=False))
            self.execute(cql="DESCRIBE test.test_val_idx", expected_err="'test_val_idx' not found in keyspace 'test'")

    def test_describe_mv(self):
        """
        @jira_ticket CASSANDRA-9961
        """
        (_node1,) = self.cluster.nodelist()

        self.execute(
            cql="""
                CREATE KEYSPACE test WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1};
                CREATE TABLE test.users (username varchar, password varchar, gender varchar,
                session_token varchar, state varchar, birth_year bigint, PRIMARY KEY (username));
                CREATE MATERIALIZED VIEW test.users_by_state AS
                SELECT * FROM users WHERE STATE IS NOT NULL AND username IS NOT NULL PRIMARY KEY (state, username)
                """,
            request_timeout_sec=120,
        )

        output = self.execute(cql="DESCRIBE KEYSPACE test")
        assert "users_by_state" in output

        self.execute(cql="DESCRIBE MATERIALIZED VIEW test.users_by_state", expected_output=self.get_users_by_state_mv_output())
        self.execute(cql="DESCRIBE test.users_by_state", expected_output=self.get_users_by_state_mv_output())
        self.execute(cql="USE test; DESCRIBE MATERIALIZED VIEW test.users_by_state", expected_output=self.get_users_by_state_mv_output())
        self.execute(cql="USE test; DESCRIBE MATERIALIZED VIEW users_by_state", expected_output=self.get_users_by_state_mv_output())
        self.execute(cql="USE test; DESCRIBE users_by_state", expected_output=self.get_users_by_state_mv_output())

        # test quotes
        self.execute(cql='USE test; DESCRIBE MATERIALIZED VIEW "users_by_state"', expected_output=self.get_users_by_state_mv_output())
        self.execute(cql='USE test; DESCRIBE "users_by_state"', expected_output=self.get_users_by_state_mv_output())

    def test_copy_to(self):
        (node1,) = self.cluster.nodelist()

        session = self.session
        create_ks(session, "ks", 1)
        session.execute(
            """
            CREATE TABLE testcopyto (
                a int,
                b text,
                c float,
                d uuid,
                PRIMARY KEY (a, b)
            )"""
        )

        insert_statement = session.prepare("INSERT INTO testcopyto (a, b, c, d) VALUES (?, ?, ?, ?)")
        args = [(i, str(i), float(i) + 0.5, uuid4()) for i in range(10000)]
        execute_concurrent_with_args(session, insert_statement, args)

        results = list(session.execute("SELECT * FROM testcopyto"))

        self.tempfile = NamedTemporaryFile(delete=False)
        logger.debug(f"Exporting to csv file: {self.tempfile.name}")
        node1.run_cqlsh(cqlsh_options=self.cqlsh_options(), cmds=f"COPY ks.testcopyto TO '{self.tempfile.name}'")

        # session
        with open(self.tempfile.name) as csvfile:
            csvreader = csv.reader(csvfile)
            result_list = [list(map(str, cql_row)) for cql_row in results]
            assert_count_equal(result_list, csvreader)

        # import the CSV file with COPY FROM
        session.execute("TRUNCATE ks.testcopyto")
        node1.run_cqlsh(cqlsh_options=self.cqlsh_options(), cmds=f"COPY ks.testcopyto FROM '{self.tempfile.name}'")
        new_results = list(session.execute("SELECT * FROM testcopyto"))
        assert results == new_results

    def test_int_values(self):
        """Tests for CASSANDRA-9399, check tables with int, bigint, smallint and tinyint values"""

        (node1,) = self.cluster.nodelist()

        _stdout, _stderr = node1.run_cqlsh(
            return_output=True,
            cqlsh_options=self.cqlsh_options(),
            cmds="""
            CREATE KEYSPACE int_checks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
            USE int_checks;
            CREATE TABLE values (part text, val1 int, val2 bigint, val3 smallint, val4 tinyint, PRIMARY KEY (part));
            INSERT INTO values (part, val1, val2, val3, val4) VALUES ('1', 1, 1, 1, 1);
            INSERT INTO values (part, val1, val2, val3, val4) VALUES ('0', 0, 0, 0, 0);
            INSERT INTO values (part, val1, val2, val3, val4) VALUES ('min', %d, %d, -32768, -128);
            INSERT INTO values (part, val1, val2, val3, val4) VALUES ('max', %d, %d, 32767, 127)"""
            % (-1 << 31, -1 << 63, (1 << 31) - 1, (1 << 63) - 1),
        )

        self.verify_output(
            "select * from int_checks.values",
            node1,
            """
 part | val1        | val2                 | val3   | val4
------+-------------+----------------------+--------+------
  min | -2147483648 | -9223372036854775808 | -32768 | -128
  max |  2147483647 |  9223372036854775807 |  32767 |  127
    0 |           0 |                    0 |      0 |    0
    1 |           1 |                    1 |      1 |    1
""",
        )

        self.verify_output(
            "DESCRIBE TABLE int_checks.values",
            node1,
            r"""
CREATE TABLE int_checks.values \(
    part text PRIMARY KEY,
    val1 int,
    val2 bigint,
    val3 smallint,
    val4 tinyint\)
""",
        )

    def test_describe_round_trip(self):
        """
        @jira_ticket CASSANDRA-9064

        Tests for the error reported in 9064 by:

        - creating the table described in the bug report, using LCS,
        - DESCRIBE-ing that table via cqlsh, then DROPping it,
        - running the output of the DESCRIBE statement as a CREATE TABLE statement, and
        - inserting a value into the table.

        The final two steps of the test should not fall down. If one does, that
        indicates the output of DESCRIBE is not a correct CREATE TABLE statement.
        """
        (node1,) = self.cluster.nodelist()

        create_ks(self.session, "test_ks", 1)
        self.session.execute("CREATE TABLE lcs_describe (key int PRIMARY KEY) WITH compaction = {'class': 'LeveledCompactionStrategy'}")
        describe_out, _describe_err = node1.run_cqlsh("DESCRIBE TABLE test_ks.lcs_describe", return_output=True, cqlsh_options=self.cqlsh_options())

        self.session.execute("DROP TABLE test_ks.lcs_describe")

        create_statement = "USE test_ks; " + " ".join(describe_out.splitlines())
        _create_out, _create_err = node1.run_cqlsh(create_statement, return_output=True, cqlsh_options=self.cqlsh_options())

        # these statements shouldn't fall down
        reloaded_describe_out, _reloaded_describe_err = node1.run_cqlsh("DESCRIBE TABLE test_ks.lcs_describe", return_output=True, cqlsh_options=self.cqlsh_options())
        self.session.execute("INSERT INTO lcs_describe (key) VALUES (1)")

        # the table created before and after should be the same
        assert reloaded_describe_out == describe_out

    def test_materialized_view(self):
        """
        Test operations on a materialized view: create, describe, select from, drop, create using describe output.
        @jira_ticket CASSANDRA-9961 and CASSANDRA-10348
        """

        def wait_for_mv_created():
            output = node1.nodetool(f"viewbuildstatus test.users_by_state")
            assert "has finished building" in output[0]

        (node1,) = self.cluster.nodelist()
        session = self.session
        create_ks(session, "test", 1)

        session.execute(
            """CREATE TABLE test.users (username varchar, password varchar, gender varchar,
                session_token varchar, state varchar, birth_year bigint, PRIMARY KEY (username))"""
        )

        session.execute(
            """CREATE MATERIALIZED VIEW test.users_by_state AS
                SELECT * FROM users WHERE STATE IS NOT NULL AND username IS NOT NULL PRIMARY KEY (state, username)"""
        )
        retry_till_success(wait_for_mv_created)

        insert_stmt = "INSERT INTO users (username, password, gender, state, birth_year) VALUES "
        session.execute(insert_stmt + "('user1', 'ch@ngem3a', 'f', 'TX', 1968);")
        session.execute(insert_stmt + "('user2', 'ch@ngem3b', 'm', 'CA', 1971);")
        session.execute(insert_stmt + "('user3', 'ch@ngem3c', 'f', 'FL', 1978);")
        session.execute(insert_stmt + "('user4', 'ch@ngem3d', 'm', 'TX', 1974);")

        describe_out, _err = node1.run_cqlsh(return_output=True, cqlsh_options=self.cqlsh_options(), cmds="DESCRIBE MATERIALIZED VIEW test.users_by_state")

        wait_for_view(self.cluster, session, "test", "users_by_state")
        select_out, _err = node1.run_cqlsh(return_output=True, cqlsh_options=self.cqlsh_options(), cmds="SELECT * FROM test.users_by_state")
        logger.debug(select_out)

        # DROP MATERIALIZED VIEW truncates the view table, which on a loaded host can exceed cqlsh's default 10s request timeout.
        out, _err = node1.run_cqlsh(return_output=True, cqlsh_options=self.cqlsh_options(request_timeout_sec=60), cmds="DROP MATERIALIZED VIEW test.users_by_state; DESCRIBE KEYSPACE test; DESCRIBE table test.users")
        assert "CREATE MATERIALIZED VIEW users_by_state" not in out
        out, err = node1.run_cqlsh(return_output=True, cqlsh_options=self.cqlsh_options(), cmds="DESCRIBE MATERIALIZED VIEW test.users_by_state")
        # cqlsh-rs returns the "not found" message on stdout instead of stderr
        combined = out + err
        assert "not found" in combined.lower(), combined

        create_statement = "USE test; " + " ".join(describe_out.splitlines()).strip()[:-1]
        _out, _err = node1.run_cqlsh(return_output=True, cqlsh_options=self.cqlsh_options(), cmds=create_statement)

        retry_till_success(wait_for_mv_created)

        reloaded_describe_out, _err = node1.run_cqlsh(cqlsh_options=self.cqlsh_options(), return_output=True, cmds="DESCRIBE MATERIALIZED VIEW test.users_by_state")
        assert describe_out == reloaded_describe_out

        wait_for_view(self.cluster, session, "test", "users_by_state")
        reloaded_select_out, _err = node1.run_cqlsh(return_output=True, cqlsh_options=self.cqlsh_options(), cmds="SELECT * FROM test.users_by_state")
        logger.info(reloaded_select_out)
        assert select_out == reloaded_select_out


    def test_simple_insert(self):
        (node1,) = self.cluster.nodelist()

        node1.run_cqlsh(
            cmds="""
            CREATE KEYSPACE simple WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
            use simple;
            create TABLE simple (id int PRIMARY KEY , value text ) ;
            insert into simple (id, value) VALUES (1, 'one');
            insert into simple (id, value) VALUES (2, 'two');
            insert into simple (id, value) VALUES (3, 'three');
            insert into simple (id, value) VALUES (4, 'four');
            insert into simple (id, value) VALUES (5, 'five')""",
            cqlsh_options=self.cqlsh_options(),
        )

        session = self.session
        rows = list(session.execute("select id, value from simple.simple"))

        assert {1: "one", 2: "two", 3: "three", 4: "four", 5: "five"} == {k: v for k, v in rows}

    def test_list_queries(self):
        (node1,) = self.cluster.nodelist()
        conn = self.session
        conn.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        conn.execute("CREATE TABLE ks.t1 (k int PRIMARY KEY, v int)")
        conn.execute("CREATE USER user1 WITH PASSWORD 'user1'")
        conn.execute("GRANT ALL ON ks.t1 TO user1")

        # auth reading is eventually consistent so we need to force node update
        read_barrier(conn)

        self.verify_output(
            "LIST USERS",
            node1,
            """
 name      | super
-----------+-------
 cassandra |  True
     user1 | False

(2 rows)
""",
        )

        self.verify_output(
            "LIST ALL PERMISSIONS OF user1",
            node1,
            """
 role  | username | resource      | permission
-------+----------+---------------+------------
 user1 |    user1 | <table ks.t1> |      ALTER
 user1 |    user1 | <table ks.t1> |  AUTHORIZE
 user1 |    user1 | <table ks.t1> |       DROP
 user1 |    user1 | <table ks.t1> |     MODIFY
 user1 |    user1 | <table ks.t1> |     SELECT

(5 rows)
""",
        )

    def test_describe_describes_non_default_compaction_parameters(self):
        (node,) = self.cluster.nodelist()
        create_ks(self.session, "ks", 1)
        self.session.execute("CREATE TABLE tab (key int PRIMARY KEY ) WITH compaction = {'class': 'SizeTieredCompactionStrategy','min_threshold': 10, 'max_threshold': 100 }")
        describe_cmd = "DESCRIBE ks.tab"
        stdout, _ = node.run_cqlsh(describe_cmd, cqlsh_options=self.cqlsh_options(), return_output=True)
        assert "'min_threshold': '10'" in stdout
        assert "'max_threshold': '100'" in stdout

    def test_describe_on_non_reserved_keywords(self):
        """
        @jira_ticket CASSANDRA-9232
        Test that we can describe tables whose name is a non-reserved CQL keyword
        """
        (node,) = self.cluster.nodelist()
        create_ks(self.session, "ks", 1)
        self.session.execute("CREATE TABLE map (key int PRIMARY KEY, val text)")
        describe_cmd = "USE ks; DESCRIBE map"
        out, _err = node.run_cqlsh(describe_cmd, cqlsh_options=self.cqlsh_options(), return_output=True)
        assert "CREATE TABLE ks.map (" in out


class CqlshSmokeDtest(CqlshTester):
    def get_keyspace_names(self):
        self.session.cluster.refresh_schema_metadata()
        return [ks.name for ks in self.session.cluster.metadata.keyspaces.values()]

    def test_uuid(self):
        """
        the `uuid()` function can generate UUIDs from cqlsh.
        """
        create_ks(self.session, "ks", 1)
        create_cf(self.session, "test", key_type="uuid", columns={"i": "int"})

        self.node1.run_cqlsh("INSERT INTO ks.test (key) VALUES (uuid())")

        result = list(self.session.execute("SELECT key FROM ks.test"))
        assert len(result) == 1
        assert len(result[0]) == 1
        assert isinstance(result[0][0], UUID)

        self.node1.run_cqlsh("INSERT INTO ks.test (key) VALUES (uuid())")

        result = list(self.session.execute("SELECT key FROM ks.test"))
        assert len(result) == 2
        assert len(result[0]) == 1
        assert len(result[1]) == 1
        assert isinstance(result[0][0], UUID)
        assert isinstance(result[1][0], UUID)
        assert result[0][0] != result[1][0]

    def test_commented_lines(self):
        create_ks(self.session, "ks", 1)
        create_cf(self.session, "test")

        self.node1.run_cqlsh(
            """
            -- commented line
            // Another comment
            /* multiline
             *
             * comment */
            """
        )
        out, _err = self.node1.run_cqlsh("DESCRIBE KEYSPACE ks; // post-line comment", return_output=True)
        assert out.strip().startswith("CREATE KEYSPACE ks")

    def test_colons_in_string_literals(self):
        create_ks(self.session, "ks", 1)
        create_cf(self.session, "test", columns={"i": "int"})

        self.node1.run_cqlsh(
            """
            INSERT INTO ks.test (key) VALUES ('Cassandra:TheMovie');
            """
        )
        assert_all(self.session, "SELECT key FROM test", [["Cassandra:TheMovie"]])

    def test_select(self):
        create_ks(self.session, "ks", 1)
        create_cf(self.session, "test")

        self.session.execute("INSERT INTO ks.test (key, c, v) VALUES ('a', 'a', 'a')")
        assert_all(self.session, "SELECT key, c, v FROM test", [["a", "a", "a"]])

        out, _err = self.node1.run_cqlsh("SELECT key, c, v FROM ks.test", return_output=True)
        out_lines = [x.strip() for x in out.split("\n")]
        # Normalize internal whitespace so column padding doesn't affect matching
        # e.g. "a   | a | a" (key col padded to header width) -> "a | a | a"
        out_lines_normalized = [" ".join(x.split()) for x in out_lines]

        # there should be only 1 row returned & it should contain the inserted values
        assert "(1 rows)" in out_lines or "(1 row)" in out_lines
        assert "a | a | a" in out_lines_normalized

    def test_insert(self):
        create_ks(self.session, "ks", 1)
        create_cf(self.session, "test")

        self.node1.run_cqlsh("INSERT INTO ks.test (key, c, v) VALUES ('a', 'a', 'a')")
        assert_all(self.session, "SELECT key, c, v FROM test", [["a", "a", "a"]])

    def test_update(self):
        create_ks(self.session, "ks", 1)
        create_cf(self.session, "test")

        self.session.execute("INSERT INTO test (key, c, v) VALUES ('a', 'a', 'a')")
        assert_all(self.session, "SELECT key, c, v FROM test", [["a", "a", "a"]])
        self.node1.run_cqlsh("UPDATE ks.test SET v = 'b' WHERE key = 'a' AND c = 'a'")
        assert_all(self.session, "SELECT key, c, v FROM test", [["a", "a", "b"]])

    def test_delete(self):
        create_ks(self.session, "ks", 1)
        create_cf(self.session, "test", columns={"i": "int"})

        self.session.execute("INSERT INTO test (key) VALUES ('a')")
        self.session.execute("INSERT INTO test (key) VALUES ('b')")
        self.session.execute("INSERT INTO test (key) VALUES ('c')")
        self.session.execute("INSERT INTO test (key) VALUES ('d')")
        self.session.execute("INSERT INTO test (key) VALUES ('e')")
        assert_all(self.session, "SELECT key from test", [["a"], ["c"], ["e"], ["d"], ["b"]])

        self.node1.run_cqlsh("DELETE FROM ks.test WHERE key = 'c'")

        assert_all(self.session, "SELECT key from test", [["a"], ["e"], ["d"], ["b"]])

    def test_create_keyspace(self):
        assert "created" not in self.get_keyspace_names()

        self.node1.run_cqlsh("CREATE KEYSPACE created WITH replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }")
        assert "created" in self.get_keyspace_names()

    def test_truncate_with_limit(self):
        """
        Create keyspace RF=1 and table, populate the table with data
        Truncate the table, run select with limit 1
        Result: no error, no rows to return
        Issue: https://github.com/scylladb/scylla/issues/1694
        """
        create_ks(self.session, "ks", 1)
        self.node1.run_cqlsh("CREATE TABLE ks.test (test_id int, partition_key text, time timestamp, value double, PRIMARY KEY ((test_id, partition_key), time));")

        for i in range(10):
            query = f"INSERT INTO ks.test (test_id, partition_key, time, value) VALUES ({i}, '{uuid4()!s}', '{datetime.datetime.now().replace(microsecond=0).isoformat()}', {float(i)});"
            self.session.execute(query)
        assert [10] == rows_to_list(self.session.execute("SELECT count(*) from test"))[0]

        _out, err = self.node1.run_cqlsh("TRUNCATE ks.test;", cqlsh_options=["--request-timeout", "60"], return_output=True)
        assert not err
        assert [] == rows_to_list(self.session.execute("SELECT * from test limit 1"))

    def test_select_all_cl_quorum(self):
        """The part after the cluster grew to four nodes (see the test)."""
        ks1_stdout, _ks1_stderr = self.node1.run_cqlsh("select * from keyspace1.standard1 LIMIT 10;", return_output=True)
        assert 10 == len([x for x in ks1_stdout.split("\n") if x and x.startswith(" 0x")])

        ks1_stdout, _ks1_stderr = self.node1.run_cqlsh("alter KEYSPACE keyspace1 WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '3'};", return_output=True)
        ks1_stdout, _ks1_stderr = self.node1.run_cqlsh("select * from keyspace1.standard1 LIMIT 10;", return_output=True)
        assert 10 == len([x for x in ks1_stdout.split("\n") if x and x.startswith(" 0x")])
        ks1_stdout, _ks1_stderr = self.node1.run_cqlsh("consistency quorum;", return_output=True)
        assert ks1_stdout == "Consistency level set to QUORUM.\n"
        ks1_stdout, _ks1_stderr = self.node1.run_cqlsh("consistency quorum; select * from keyspace1.standard1 LIMIT 10;", return_output=True)
        assert 10 == len([x for x in ks1_stdout.split("\n") if x and x.startswith(" 0x")])

    def test_truncate(self):
        create_ks(self.session, "ks", 1)
        create_cf(self.session, "test", columns={"i": "int"})

        self.session.execute("INSERT INTO test (key) VALUES ('a')")
        self.session.execute("INSERT INTO test (key) VALUES ('b')")
        self.session.execute("INSERT INTO test (key) VALUES ('c')")
        self.session.execute("INSERT INTO test (key) VALUES ('d')")
        self.session.execute("INSERT INTO test (key) VALUES ('e')")
        assert_all(self.session, "SELECT key from test", [["a"], ["c"], ["e"], ["d"], ["b"]])

        _out, err = self.node1.run_cqlsh("TRUNCATE ks.test;", cqlsh_options=["--request-timeout", "60"], return_output=True)
        assert not err
        assert [] == rows_to_list(self.session.execute("SELECT * from test"))

    def test_use_keyspace(self):
        # ks1 contains ks1table, ks2 contains ks2table
        create_ks(self.session, "ks1", 1)
        create_cf(self.session, "ks1table")
        create_ks(self.session, "ks2", 1)
        create_cf(self.session, "ks2table")

        ks1_stdout, _ks1_stderr = self.node1.run_cqlsh(
            """
            USE ks1;
            DESCRIBE TABLES;
            """,
            return_output=True,
        )
        assert [x for x in ks1_stdout.split() if x] == ["ks1table"]

        ks2_stdout, _ks2_stderr = self.node1.run_cqlsh(
            """
            USE ks2;
            DESCRIBE TABLES;
            """,
            return_output=True,
        )
        assert [x for x in ks2_stdout.split() if x] == ["ks2table"]

    def test_create_index(self):
        create_ks(self.session, "ks", 1)
        create_cf(self.session, "test", columns={"i": "int"})

        # create a statement that will only work if there's an index on i
        requires_index = "SELECT * from test WHERE i = 5;"

        # make sure it fails as expected
        with pytest.raises(InvalidRequest):
            self.session.execute(requires_index)

        # make sure index exists after creating via cqlsh
        self.node1.run_cqlsh("CREATE INDEX index_to_drop ON ks.test (i);")
        assert_none(self.session, requires_index)

        # drop the index, then make sure it fails again
        self.session.execute("DROP INDEX ks.index_to_drop;")
        with pytest.raises(InvalidRequest):
            self.session.execute(requires_index)

    def test_incorrect_clustering_restrictions(self):
        # https://github.com/scylladb/scylla/issues/2421
        create_ks(self.session, "ks", 1)  # self.create_cf(self.session, 'ks1table')
        self.session.execute(
            """CREATE TABLE foo2 (id text,
                                a text,
                                b text,
                                at timestamp,
                                c text,
                                PRIMARY KEY (id,at,a,b,c));"""
        )
        self.node1.run_cqlsh("insert into ks.foo2 (id, a,b,at,c) values ('id2', 'a1', 'b1', '2017-01-01T00:00:01.000', 'c2');")
        self.node1.run_cqlsh("insert into ks.foo2 (id, a,b,at,c) values ('id2', 'a1', 'b1', '2017-01-01T00:00:00.000', 'c1');")

        cqlsh_stderr = self.node1.run_cqlsh("SELECT id FROM ks.foo2 WHERE id = 't' AND a = 'y' AND b = 'z' AND at <= '2017-01-01T00:00:00.000' AND at >= '2016-01-01T00:00:00.000';", return_output=True)

        error_msg = 'PRIMARY KEY column "b" cannot be restricted (preceding column "at" is restricted by a non-EQ relation)'
        assert error_msg in cqlsh_stderr[1], f"Expected error message not found in stderr: {cqlsh_stderr[1]!r}"


class CqlLoginDtest(CqlshTester):
    def test_login_authenticates_correct_user(self):
        create_ks(self.session, "ks1", 1)
        create_cf(self.session, "ks1table")
        self.session.execute("CREATE USER user1 WITH PASSWORD 'changeme';")

        if Version(self.cluster.version()) >= Version("3.0"):
            query = """
                    LOGIN user1 'changeme';
                    CREATE USER user2 WITH PASSWORD 'fail' SUPERUSER;
                    """
            expected_error = "Only superusers can create a role with superuser status"
        else:
            query = """
                    LOGIN user1 'changeme';
                    CREATE USER user2 WITH PASSWORD 'fail';
                    """
            expected_error = "Only superusers are allowed to perform CREATE USER queries"

        _cqlsh_stdout, cqlsh_stderr = self.node1.run_cqlsh(query, return_output=True, cqlsh_options=self.cqlsh_options())

        err_lines = cqlsh_stderr.splitlines()
        for err_line in err_lines:
            if expected_error in err_line:
                break
        else:
            pytest.fail("Did not find expected error '{}' in cqlsh stderr output: {}".format(expected_error, "\n".join(err_lines)))


############################################################################
# The tests: each on a node of its own, its dtest method run in a thread.

async def run_dtest(manager: ScyllaClusterManager, tester_class, method: str, config: dict | None = None, **kwargs):
    [server] = await manager.servers_add(1, config={**CONFIG, **(config or {})}, **kwargs)
    exe = await manager.server_get_exe(server.server_id)
    tester = tester_class(server, exe, manager.get_cql())
    await asyncio.to_thread(getattr(tester, method))


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("method", ["test_eat_glass", "test_with_empty_values", "test_describe", "test_describe_mv", "test_copy_to", "test_int_values", "test_describe_round_trip", "test_materialized_view",
                                    "test_simple_insert", "test_describe_describes_non_default_compaction_parameters", "test_describe_on_non_reserved_keywords"])
async def test_cqlsh(manager: ScyllaClusterManager, method):
    await run_dtest(manager, CqlshDtest, method)


@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
@pytest.mark.parametrize("method", ["test_uuid", "test_commented_lines", "test_colons_in_string_literals", "test_select", "test_insert", "test_update", "test_delete", "test_create_keyspace", "test_truncate", "test_use_keyspace", "test_create_index", "test_incorrect_clustering_restrictions",
                                    "test_truncate_with_limit"])
async def test_cqlsh_smoke(manager: ScyllaClusterManager, method):
    await run_dtest(manager, CqlshSmokeDtest, method)


# As in the dtest, the node uses PasswordAuthenticator, and the session is
# the default superuser's, once it was created.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_login_authenticates_correct_user(manager: ScyllaClusterManager):
    from cassandra.auth import PlainTextAuthProvider
    await run_dtest(manager, CqlLoginDtest, "test_login_authenticates_correct_user",
                    config={"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator"},
                    driver_connect_opts={"auth_provider": PlainTextAuthProvider(username="cassandra", password="cassandra")})


# Ported from scylla-dtest cqlsh_tests/cqlsh_tests.py::TestCqlsh::test_list_queries.
# The node is restarted with PasswordAuthenticator and CassandraAuthorizer
# (no permission caching); once the default superuser exists, a table and a
# user with ALL on it are created, and cqlsh's LIST USERS and LIST ALL
# PERMISSIONS OF user1 print the dtest's tables.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_list_queries(manager: ScyllaClusterManager):
    from cassandra.auth import PlainTextAuthProvider
    [server] = await manager.servers_add(1, config=CONFIG)
    log = await manager.server_open_log(server.server_id)
    mark = await log.mark()
    await manager.server_update_config(server.server_id, config_options={
        "authenticator": "org.apache.cassandra.auth.PasswordAuthenticator",
        "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer", "permissions_validity_in_ms": "0"}, reload=False)
    await manager.server_stop_gracefully(server.server_id)
    await start_node_wait_cql(manager, server)
    await log.wait_for("Created default superuser", from_mark=mark, timeout=60)
    await manager.driver_connect(auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"))
    exe = await manager.server_get_exe(server.server_id)
    tester = CqlshDtest(server, exe, manager.get_cql())
    await asyncio.to_thread(tester.test_list_queries)


# Ported from scylla-dtest cqlsh_tests/cqlsh_tests.py::TestCqlshSmoke::test_select_all_cl_quorum.
# The smoke node (datacenter1/rack1) gets three more nodes in rack2-rack4,
# started without bootstrap as ccm's new_node(bootstrap=False) does;
# cassandra-stress writes 1K rows with RF=4; cqlsh then reads 10 of them,
# after an ALTER to RF 3 in datacenter1, and at CL=QUORUM (scylladb#2593).
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_select_all_cl_quorum(manager: ScyllaClusterManager):
    from test.cluster.util import cassandra_stress
    # The dtest's cluster_options(rf_rack_valid_keyspaces=False).
    config = {**CONFIG, "rf_rack_valid_keyspaces": False}
    [server] = await manager.servers_add(1, config=config, property_file={"dc": "datacenter1", "rack": "rack1"})
    for i in range(3):
        await manager.server_add(config={**config, "auto_bootstrap": False}, property_file={"dc": "datacenter1", "rack": f"rack{i + 2}"})
    await cassandra_stress(manager, server, "write", "n=1K", "-rate", "threads=8", "-schema", "replication(factor=4)", create_keyspace=False)
    exe = await manager.server_get_exe(server.server_id)
    tester = CqlshSmokeDtest(server, exe, manager.get_cql())
    await asyncio.to_thread(tester.test_select_all_cl_quorum)

