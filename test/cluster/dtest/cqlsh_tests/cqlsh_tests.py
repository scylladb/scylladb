import binascii
import csv
import datetime
import logging
import os
import re
import ssl
import subprocess
from decimal import Decimal
from functools import cached_property
from pathlib import Path
from tempfile import NamedTemporaryFile
from textwrap import dedent
from uuid import UUID, uuid4

import pytest
from cassandra import InvalidRequest
from cassandra.concurrent import execute_concurrent_with_args
from ccmlib import common
from packaging.version import Version

from dtest_class import Tester, create_cf, create_ks, read_barrier, retry_till_success
from tools.assertions import assert_all, assert_count_equal, assert_none
from tools.cluster import new_node
from tools.cluster_topology import generate_cluster_topology
from tools.data import create_c1c2_table, insert_c1c2, rows_to_list
from tools.misc import generate_ssl_stores
from tools.tables_view_manager import wait_for_view

from .cqlsh_tools import monkeypatch_driver, unmonkeypatch_driver

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.next_gating


class CqlshVersionMixing(Tester):
    ssl = False

    @cached_property
    def cqlsh_version(self) -> Version:
        node, *_ = self.cluster.nodelist()
        output, _err = node.run_cqlsh(cmds="", cqlsh_options=["--version"], return_output=True)
        return Version(output.strip().split(" ")[1])

    def cqlsh_options(self, request_timeout_sec: int | None = None) -> list:
        opts = ["-u", "cassandra", "-p", "cassandra"]
        if self.cqlsh_version >= Version("6.0.0"):
            opts += ["--insecure-password-without-warning"]
        if self.ssl:
            opts += ["--ssl", "--cqlshrc", self.cqlshrc_file]
        if request_timeout_sec:
            opts += ["--request-timeout", str(request_timeout_sec)]
        return opts


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestCqlsh(CqlshVersionMixing):
    normalize_numbers_re = re.compile(r"\b(\d+)\.0\b")
    normalize_operators_re = re.compile(r"\s*(\\?[:.,=(){}])\s*")
    normalize_whitespaces_re = re.compile(r"\s+")
    normalize_select_columns_re = re.compile(r"state,\s*username,\s*birth_year,\s*gender,\s*password,\s*session_token")
    normalize_primary_key_re = re.compile(r"(?P<open>\\?\()(?P<key>\w+) (?P<type>\w+) PRIMARY KEY(?P<cols>,[^\\)]+)(?P<close>\\?\))")

    @pytest.fixture(scope="function", autouse=True)
    def setup(self):
        # No cqlsh test checks snapshots; skip them so DROP TABLE doesn't pay for snapshot I/O.
        self.cluster.set_configuration_options({"auto_snapshot": False})
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        self.node1, *_ = self.cluster.nodelist()
        self.session = self.create_session()

    @pytest.fixture(scope="function", autouse=True)
    def get_default_compaction_strategy(self, dtest_config):
        self.default_compaction_strategy = r"(IncrementalCompactionStrategy|SizeTieredCompactionStrategy)"

    @pytest.fixture(scope="function", autouse=True)
    def get_default_compressor(self):
        self.default_compressor = r"(org.apache.cassandra.io.compress.LZ4Compressor|LZ4WithDictsCompressor)"

    def create_session(self, username: str | None = None, password: str | None = None):
        return self.patient_cql_connection(self.node1, user=username, password=password)

    @pytest.fixture(scope="class", autouse=True)
    def monkeypatch_driver(self):
        cached_driver_methods = monkeypatch_driver()
        yield
        unmonkeypatch_driver(cached_driver_methods)

    @pytest.fixture(scope="function")
    def clean_temp(self):
        yield
        if hasattr(self, "tempfile") and not common.is_win():
            os.unlink(self.tempfile.name)

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

    def test_past_and_future_dates(self):
        (node1,) = self.cluster.nodelist()

        node1.run_cqlsh(
            cqlsh_options=self.cqlsh_options(),
            cmds="""
            CREATE KEYSPACE simple WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
            use simple;
            create TABLE simpledate (id int PRIMARY KEY , value timestamp ) ;
            insert into simpledate (id, value) VALUES (1, '2143-04-19 11:21:01+0000');
            insert into simpledate (id, value) VALUES (2, '1943-04-19 11:21:01+0000')""",
        )

        rows = list(self.session.execute("select id, value from simple.simpledate"))

        output, _err = node1.run_cqlsh(return_output=True, cqlsh_options=self.cqlsh_options(), cmds="use simple; SELECT * FROM simpledate")

        assert "2143-04-19 11:21:01.000000+0000" in output
        assert "1943-04-19 11:21:01.000000+0000" in output

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

    def test_source_glass(self):
        (node1,) = self.cluster.nodelist()

        node1.run_cqlsh(cqlsh_options=self.cqlsh_options(), cmds="SOURCE 'cqlsh_tests/glass.cql'")

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

    def test_tracing_from_system_traces(self):
        (node1,) = self.cluster.nodelist()

        session = self.session

        create_ks(session, "ks", 1)
        create_c1c2_table(session)

        insert_c1c2(session, n=100)

        out, _err = node1.run_cqlsh(return_output=True, cqlsh_options=self.cqlsh_options(), cmds="TRACING ON; SELECT * FROM ks.cf")
        assert "Tracing session: " in out

        out, _err = node1.run_cqlsh(return_output=True, cqlsh_options=self.cqlsh_options(), cmds="TRACING ON; SELECT * FROM system_traces.events")
        assert "Tracing session: " not in out

        out, _err = node1.run_cqlsh(return_output=True, cqlsh_options=self.cqlsh_options(), cmds="TRACING ON; SELECT * FROM system_traces.sessions")
        assert "Tracing session: " not in out

    def test_select_element_inside_udt(self):
        (node1,) = self.cluster.nodelist()
        session = self.session

        create_ks(session, "ks", 1)
        session.execute(
            """
            CREATE TYPE address (
            street text,
            city text,
            zip_code int,
            phones set<text>
             );"""
        )

        session.execute(
            """CREATE TYPE fullname (
            firstname text,
            lastname text
            );"""
        )

        session.execute(
            """CREATE TABLE users (
            id uuid PRIMARY KEY,
            name FROZEN <fullname>,
            addresses map<text, FROZEN <address>>
            );"""
        )

        session.execute(
            """INSERT INTO users (id, name)
            VALUES (62c36092-82a1-3a00-93d1-46196ee77204, {firstname: 'Marie-Claude', lastname: 'Josset'});
            """
        )

        _out, err = node1.run_cqlsh(return_output=True, cmds="SELECT name.lastname FROM ks.users WHERE id=62c36092-82a1-3a00-93d1-46196ee77204")
        assert "list index out of range" not in err
        # If this assertion fails check CASSANDRA-7891

    def verify_output(self, query, node, expected):
        output, _err = node.run_cqlsh(query, cqlsh_options=self.cqlsh_options(), return_output=True)
        if common.is_win():
            output = output.replace("\r", "")
        logger.debug(output)
        self.check_response(expected_response=expected, response=output)

    def test_list_queries(self):
        config = {"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator", "authorizer": "org.apache.cassandra.auth.CassandraAuthorizer", "permissions_validity_in_ms": "0"}
        self.cluster.set_configuration_options(values=config)
        self.cluster.stop()
        self.cluster.start(wait_for_binary_proto=True)

        (node1,) = self.cluster.nodelist()
        node1.watch_log_for("Created default superuser")

        conn = self.create_session(username="cassandra", password="cassandra")
        conn.execute("CREATE KEYSPACE ks WITH replication = {'class':'NetworkTopologyStrategy', 'replication_factor':1}")
        conn.execute("CREATE TABLE ks.t1 (k int PRIMARY KEY, v int)")
        conn.execute("CREATE USER user1 WITH PASSWORD 'user1'")
        conn.execute("GRANT ALL ON ks.t1 TO user1")

        # auth reading is eventually consistent so we need to force node update
        read_barrier(conn)

        if Version(self.cluster.version()) >= Version("3.0"):
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
        else:
            self.verify_output(
                "LIST USERS",
                node1,
                """
 name      | super
-----------+-------
     user1 | False
 cassandra |  True

(2 rows)
""",
            )

        if Version(self.cluster.version()) >= Version("3.0"):
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
        else:
            self.verify_output(
                "LIST ALL PERMISSIONS OF user1",
                node1,
                """
 username | resource      | permission
----------+---------------+------------
    user1 | <table ks.t1> |      ALTER
    user1 | <table ks.t1> |  AUTHORIZE
    user1 | <table ks.t1> |       DROP
    user1 | <table ks.t1> |     MODIFY
    user1 | <table ks.t1> |     SELECT

(5 rows)
""",
            )

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

    def test_float_formatting(self):
        """Tests for CASSANDRA-9224, check format of float and double values"""

        (node1,) = self.cluster.nodelist()

        _stdout, _stderr = node1.run_cqlsh(
            return_output=True,
            cqlsh_options=self.cqlsh_options(),
            cmds="""
            CREATE KEYSPACE formatting WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
            use formatting;
            create TABLE values ( part text, id int, val1 double, val2 float, PRIMARY KEY (part, id) );
            insert into values (part, id, val1, val2) VALUES ('+', 1, 0.00000006, 0.00000006);
            insert into values (part, id, val1, val2) VALUES ('+', 2, 0.0000006, 0.0000006);
            insert into values (part, id, val1, val2) VALUES ('+', 3, 0.000006, 0.000006);
            insert into values (part, id, val1, val2) VALUES ('+', 4, 0.00006, 0.00006);
            insert into values (part, id, val1, val2) VALUES ('+', 5, 0.0006, 0.0006);
            insert into values (part, id, val1, val2) VALUES ('+', 6, 0.006, 0.006);
            insert into values (part, id, val1, val2) VALUES ('+', 7, 0.06, 0.06);
            insert into values (part, id, val1, val2) VALUES ('+', 8, 0.6, 0.6);
            insert into values (part, id, val1, val2) VALUES ('+', 9, 6, 6);
            insert into values (part, id, val1, val2) VALUES ('+', 10, 6.0, 6.0);
            insert into values (part, id, val1, val2) VALUES ('+', 11, 6.00, 6.00);
            insert into values (part, id, val1, val2) VALUES ('+', 12, 6.000, 6.000);
            insert into values (part, id, val1, val2) VALUES ('+', 13, 6.00000, 6.00000);
            insert into values (part, id, val1, val2) VALUES ('+', 14, 6.000000, 6.000000);
            insert into values (part, id, val1, val2) VALUES ('+', 15, 6.1, 6.1);
            insert into values (part, id, val1, val2) VALUES ('+', 16, 6.12, 6.12);
            insert into values (part, id, val1, val2) VALUES ('+', 17, 6.123, 6.123);
            insert into values (part, id, val1, val2) VALUES ('+', 18, 6.1234, 6.1234);
            insert into values (part, id, val1, val2) VALUES ('+', 19, 6.12345, 6.12345);
            insert into values (part, id, val1, val2) VALUES ('+', 20, 6.123454, 6.123454);
            insert into values (part, id, val1, val2) VALUES ('+', 21, 6.123455, 6.123455);
            insert into values (part, id, val1, val2) VALUES ('+', 22, 6.123456, 6.123456);
            insert into values (part, id, val1, val2) VALUES ('+', 23, 6.1234565, 6.1234565);
            insert into values (part, id, val1, val2) VALUES ('+', 24, 6.1234555, 6.1234555);
            insert into values (part, id, val1, val2) VALUES ('+', 25, 6.12345555, 6.12345555);
            insert into values (part, id, val1, val2) VALUES ('+', 26, 6.12345555555555, 6.12345555555555);
            insert into values (part, id, val1, val2) VALUES ('+', 27, 16.12345, 16.12345);
            insert into values (part, id, val1, val2) VALUES ('+', 28, 116.12345, 116.12345);
            insert into values (part, id, val1, val2) VALUES ('+', 29, 1116.12345, 1116.12345);
            insert into values (part, id, val1, val2) VALUES ('+', 30, 11116.12345, 11116.12345);
            insert into values (part, id, val1, val2) VALUES ('+', 31, 111116.12345, 111116.12345);
            insert into values (part, id, val1, val2) VALUES ('+', 32, 1111116.12345, 1111116.12345);
            insert into values (part, id, val1, val2) VALUES ('+', 33, 11111116.12345, 11111116.12345)""",
        )

        self.verify_output(
            "select * from formatting.values where part = '+'",
            node1,
            """
 part | id | val1        | val2
------+----+-------------+-------------
    + |  1 |       6e-08 |       6e-08
    + |  2 |       6e-07 |       6e-07
    + |  3 |       6e-06 |       6e-06
    + |  4 |       6e-05 |       6e-05
    + |  5 |      0.0006 |      0.0006
    + |  6 |       0.006 |       0.006
    + |  7 |        0.06 |        0.06
    + |  8 |         0.6 |         0.6
    + |  9 |           6 |           6
    + | 10 |           6 |           6
    + | 11 |           6 |           6
    + | 12 |           6 |           6
    + | 13 |           6 |           6
    + | 14 |           6 |           6
    + | 15 |         6.1 |         6.1
    + | 16 |        6.12 |        6.12
    + | 17 |       6.123 |       6.123
    + | 18 |      6.1234 |      6.1234
    + | 19 |     6.12345 |     6.12345
    + | 20 |     6.12345 |     6.12345
    + | 21 |     6.12345 |     6.12346
    + | 22 |     6.12346 |     6.12346
    + | 23 |     6.12346 |     6.12346
    + | 24 |     6.12346 |     6.12346
    + | 25 |     6.12346 |     6.12346
    + | 26 |     6.12346 |     6.12346
    + | 27 |    16.12345 |    16.12345
    + | 28 |   116.12345 |   116.12345
    + | 29 |  1116.12345 |  1116.12341
    + | 30 | 11116.12345 | 11116.12305
    + | 31 |  1.1112e+05 |  1.1112e+05
    + | 32 |  1.1111e+06 |  1.1111e+06
    + | 33 |  1.1111e+07 |  1.1111e+07
""",
        )

        _stdout, _stderr = node1.run_cqlsh(
            return_output=True,
            cqlsh_options=self.cqlsh_options(),
            cmds="""
            use formatting;
            insert into values (part, id, val1, val2) VALUES ('-', 1, -0.00000006, -0.00000006);
            insert into values (part, id, val1, val2) VALUES ('-', 2, -0.0000006, -0.0000006);
            insert into values (part, id, val1, val2) VALUES ('-', 3, -0.000006, -0.000006);
            insert into values (part, id, val1, val2) VALUES ('-', 4, -0.00006, -0.00006);
            insert into values (part, id, val1, val2) VALUES ('-', 5, -0.0006, -0.0006);
            insert into values (part, id, val1, val2) VALUES ('-', 6, -0.006, -0.006);
            insert into values (part, id, val1, val2) VALUES ('-', 7, -0.06, -0.06);
            insert into values (part, id, val1, val2) VALUES ('-', 8, -0.6, -0.6);
            insert into values (part, id, val1, val2) VALUES ('-', 9, -6, -6);
            insert into values (part, id, val1, val2) VALUES ('-', 10, -6.0, -6.0);
            insert into values (part, id, val1, val2) VALUES ('-', 11, -6.00, -6.00);
            insert into values (part, id, val1, val2) VALUES ('-', 12, -6.000, -6.000);
            insert into values (part, id, val1, val2) VALUES ('-', 13, -6.00000, -6.00000);
            insert into values (part, id, val1, val2) VALUES ('-', 14, -6.000000, -6.000000);
            insert into values (part, id, val1, val2) VALUES ('-', 15, -6.1, -6.1);
            insert into values (part, id, val1, val2) VALUES ('-', 16, -6.12, -6.12);
            insert into values (part, id, val1, val2) VALUES ('-', 17, -6.123, -6.123);
            insert into values (part, id, val1, val2) VALUES ('-', 18, -6.1234, -6.1234);
            insert into values (part, id, val1, val2) VALUES ('-', 19, -6.12345, -6.12345);
            insert into values (part, id, val1, val2) VALUES ('-', 20, -6.123454, -6.123454);
            insert into values (part, id, val1, val2) VALUES ('-', 21, -6.123455, -6.123455);
            insert into values (part, id, val1, val2) VALUES ('-', 22, -6.123456, -6.123456);
            insert into values (part, id, val1, val2) VALUES ('-', 23, -6.1234565, -6.1234565);
            insert into values (part, id, val1, val2) VALUES ('-', 24, -6.1234555, -6.1234555);
            insert into values (part, id, val1, val2) VALUES ('-', 25, -6.12345555, -6.12345555);
            insert into values (part, id, val1, val2) VALUES ('-', 26, -6.12345555555555, -6.12345555555555);
            insert into values (part, id, val1, val2) VALUES ('-', 27, -16.12345, -16.12345);
            insert into values (part, id, val1, val2) VALUES ('-', 28, -116.12345, -116.12345);
            insert into values (part, id, val1, val2) VALUES ('-', 29, -1116.12345, -1116.12345);
            insert into values (part, id, val1, val2) VALUES ('-', 30, -11116.12345, -11116.12345);
            insert into values (part, id, val1, val2) VALUES ('-', 31, -111116.12345, -111116.12345);
            insert into values (part, id, val1, val2) VALUES ('-', 32, -1111116.12345, -1111116.12345);
            insert into values (part, id, val1, val2) VALUES ('-', 33, -11111116.12345, -11111116.12345)""",
        )

        self.verify_output(
            "select * from formatting.values where part = '-'",
            node1,
            """
 part | id | val1         | val2
------+----+--------------+--------------
    - |  1 |       -6e-08 |       -6e-08
    - |  2 |       -6e-07 |       -6e-07
    - |  3 |       -6e-06 |       -6e-06
    - |  4 |       -6e-05 |       -6e-05
    - |  5 |      -0.0006 |      -0.0006
    - |  6 |       -0.006 |       -0.006
    - |  7 |        -0.06 |        -0.06
    - |  8 |         -0.6 |         -0.6
    - |  9 |           -6 |           -6
    - | 10 |           -6 |           -6
    - | 11 |           -6 |           -6
    - | 12 |           -6 |           -6
    - | 13 |           -6 |           -6
    - | 14 |           -6 |           -6
    - | 15 |         -6.1 |         -6.1
    - | 16 |        -6.12 |        -6.12
    - | 17 |       -6.123 |       -6.123
    - | 18 |      -6.1234 |      -6.1234
    - | 19 |     -6.12345 |     -6.12345
    - | 20 |     -6.12345 |     -6.12345
    - | 21 |     -6.12345 |     -6.12346
    - | 22 |     -6.12346 |     -6.12346
    - | 23 |     -6.12346 |     -6.12346
    - | 24 |     -6.12346 |     -6.12346
    - | 25 |     -6.12346 |     -6.12346
    - | 26 |     -6.12346 |     -6.12346
    - | 27 |    -16.12345 |    -16.12345
    - | 28 |   -116.12345 |   -116.12345
    - | 29 |  -1116.12345 |  -1116.12341
    - | 30 | -11116.12345 | -11116.12305
    - | 31 |  -1.1112e+05 |  -1.1112e+05
    - | 32 |  -1.1111e+06 |  -1.1111e+06
    - | 33 |  -1.1111e+07 |  -1.1111e+07
""",
        )

        _stdout, _stderr = node1.run_cqlsh(
            return_output=True,
            cqlsh_options=self.cqlsh_options(),
            cmds="""
            use formatting;
            insert into values (part, id, val1, val2) VALUES ('0', 1, 0, 0);
            insert into values (part, id, val1, val2) VALUES ('0', 2, 0.000000000001, 0.000000000001);
            insert into values (part, id, val1, val2) VALUES ('0', 3, 0.0000000000001, 0.0000000000001);
            insert into values (part, id, val1, val2) VALUES ('0', 4, 0.00000000000001, 0.00000000000001);
            insert into values (part, id, val1, val2) VALUES ('0', 5, 0.000000000000001, 0.000000000000001);
            insert into values (part, id, val1, val2) VALUES ('0', 6, 0.0000000000000001, 0.0000000000000001)""",
        )

        self.verify_output(
            "select * from formatting.values where part = '0'",
            node1,
            """
 part | id | val1  | val2
------+----+-------+-------
    0 |  1 |     0 |     0
    0 |  2 | 1e-12 | 1e-12
    0 |  3 | 1e-13 | 1e-13
    0 |  4 | 1e-14 | 1e-14
    0 |  5 | 1e-15 | 1e-15
    0 |  6 | 1e-16 | 1e-16
""",
        )

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

    def test_datetime_values(self):
        """Tests for CASSANDRA-9399, check tables with date and time values"""

        (node1,) = self.cluster.nodelist()

        _stdout, _stderr = node1.run_cqlsh(
            return_output=True,
            cqlsh_options=self.cqlsh_options(),
            cmds="""
            CREATE KEYSPACE datetime_checks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
            USE datetime_checks;
            CREATE TABLE values (d date, t time, PRIMARY KEY (d, t));
            INSERT INTO values (d, t) VALUES ('9800-12-31', '23:59:59.999999999');
            INSERT INTO values (d, t) VALUES ('2015-05-14', '16:30:00.555555555');
            INSERT INTO values (d, t) VALUES ('1582-1-1', '00:00:00.000000000');
            INSERT INTO values (d, t) VALUES ('%d-1-1', '00:00:00.000000000');
            INSERT INTO values (d, t) VALUES ('%d-1-1', '01:00:00.000000000');
            INSERT INTO values (d, t) VALUES ('%d-1-1', '02:00:00.000000000');
            INSERT INTO values (d, t) VALUES ('%d-1-1', '03:00:00.000000000')"""
            % (datetime.MINYEAR - 1, datetime.MINYEAR, datetime.MAXYEAR, datetime.MAXYEAR + 1),
        )
        # outside the MIN and MAX range it should print the number of days from the epoch

        self.verify_output(
            "select * from datetime_checks.values",
            node1,
            """
 d          | t
------------+--------------------
    -719528 | 00:00:00.000000000
 9800-12-31 | 23:59:59.999999999
 0001-01-01 | 01:00:00.000000000
 1582-01-01 | 00:00:00.000000000
    2932897 | 03:00:00.000000000
 9999-01-01 | 02:00:00.000000000
 2015-05-14 | 16:30:00.555555555
""",
        )

        self.verify_output(
            "DESCRIBE TABLE datetime_checks.values",
            node1,
            r"""
CREATE TABLE datetime_checks.values \(
    d date,
    t time,
    PRIMARY KEY \(d, t\)\)
""",
        )

    def test_tracing(self):
        """
        Tests for CASSANDRA-9399, check tracing works.
        We care mostly that we do not crash, not so much on the tracing content, which may change and would
        therefore make this test too brittle.
        """

        (node1,) = self.cluster.nodelist()

        _stdout, _stderr = node1.run_cqlsh(
            return_output=True,
            cqlsh_options=self.cqlsh_options(),
            cmds="""
            CREATE KEYSPACE tracing_checks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
            USE tracing_checks;
            CREATE TABLE test (id int, val text, PRIMARY KEY (id));
            INSERT INTO test (id, val) VALUES (1, 'adfad');
            INSERT INTO test (id, val) VALUES (2, 'lkjlk');
            INSERT INTO test (id, val) VALUES (3, 'iuiou')""",
        )

        self.verify_output(
            "use tracing_checks; tracing on; select * from test",
            node1,
            """Now Tracing is enabled

 id | val
----+-------
  1 | adfad
  2 | lkjlk
  3 | iuiou

(3 rows)

Tracing session:""",
        )

    def test_connect_timeout(self):
        """
        @jira_ticket CASSANDRA-9601
        """

        (node1,) = self.cluster.nodelist()

        _stdout, _stderr = node1.run_cqlsh(cmds="USE system", cqlsh_options=[*self.cqlsh_options(), "--debug", "--connect-timeout=10"], return_output=True)
        assert "Using connect timeout: 10 seconds" in _stderr

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

        out, _err = node1.run_cqlsh(return_output=True, cqlsh_options=self.cqlsh_options(), cmds="DROP MATERIALIZED VIEW test.users_by_state; DESCRIBE KEYSPACE test; DESCRIBE table test.users")
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

    def test_clear(self):
        """
        Test the CLEAR command
        @jira_ticket CASSANDRA-10086
        """
        self._test_clear_screen("CLEAR")

    def test_cls(self):
        """
        Test the CLS command
        @jira_ticket CASSANDRA-10086
        """
        self._test_clear_screen("CLS")

    def _test_clear_screen(self, cmd):
        """
        We use ANSI escape sequences to check the output:
        http://ascii-table.com/ansi-escape-sequences-vt-100.php

        Possible clear screen sequences:
        Esc[J or Esc[0J or Esc[1J or Esc[2J

        In addition we can move the cursor upper left:
        Esc[H or Esc[;H

        The escape character code is 27.

        We don't check for moving the cursor though, we only check that
        there is no error and that at least one of the possible clear
        screen sequences is contained in the output, via a regular
        expression.
        """
        (node1,) = self.cluster.nodelist()

        out, _err = node1.run_cqlsh(cmd, extra_env={"TERM": "xterm"}, cqlsh_options=self.cqlsh_options(), return_output=True)

        # Can't check escape sequence on cmd prompt. Assume no errors is good enough metric.
        if not common.is_win():
            assert re.search(chr(27) + r"\[[0,1,2]?J", out)

    def test_batch(self):
        """
        Test the BATCH command
        @jira_ticket CASSANDRA-10272
        """
        (node1,) = self.cluster.nodelist()

        node1.run_cqlsh(
            """
            CREATE KEYSPACE Excelsior  WITH REPLICATION={'class':'NetworkTopologyStrategy','replication_factor':1};
            CREATE TABLE excelsior.data (id int primary key);
            BEGIN BATCH INSERT INTO excelsior.data (id) VALUES (0); APPLY BATCH""",
            cqlsh_options=self.cqlsh_options(),
        )

        rows = list(self.session.execute("SELECT id FROM excelsior.data"))
        assert len(rows) == 1, f"Expected 1 row, got {len(rows)}: {rows}"
        assert rows[0].id == 0


@pytest.mark.dtest_full
class TestCqlshCluster(CqlshVersionMixing):
    def test_refresh_schema_on_timeout_error(self):
        """
        @jira_ticket CASSANDRA-9689
        """
        self.cluster.populate(3)
        self.cluster.start(wait_for_binary_proto=True)

        node1, node2, _node3 = self.cluster.nodelist()
        node2.stop(wait_other_notice=True)

        stdout, stderr = node1.run_cqlsh(
            return_output=True,
            cmds="""
              CREATE KEYSPACE training WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1};
              DESCRIBE KEYSPACES""",
        )
        assert "training" in stdout
        # cqlsh-rs may not emit schema mismatch warnings; accept both behaviors
        if "Warning: schema version mismatch detected" in stderr:
            assert "check the schema versions of your nodes in system.local and system.peers." in stderr

        stdout, stderr = node1.run_cqlsh(
            return_output=True,
            cmds="""USE training;
                                                  CREATE TABLE mytable (id int, val text, PRIMARY KEY (id));
                                                  describe tables""",
        )
        assert "mytable" in stdout
        if "Warning: schema version mismatch detected" in stderr:
            assert "check the schema versions of your nodes in system.local and system.peers." in stderr


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestCqlshSmoke(Tester):
    """
    Tests simple use cases for clqsh.
    """

    @pytest.fixture(scope="function", autouse=True)
    def setup(self):
        self.cluster.populate(generate_cluster_topology(rack_num=1)).start(wait_for_binary_proto=True)

        [self.node1] = self.cluster.nodelist()
        self.session = self.patient_cql_connection(self.node1)

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

    def test_batch(self):
        create_ks(self.session, "ks", 1)
        create_cf(self.session, "test", columns={"i": "int"})
        # run batch statement (inserts are fine)
        self.node1.run_cqlsh(
            """
            BEGIN BATCH
                INSERT INTO ks.test (key) VALUES ('eggs')
                INSERT INTO ks.test (key) VALUES ('sausage')
                INSERT INTO ks.test (key) VALUES ('spam')
            APPLY BATCH;
            """
        )
        # make sure everything inserted is actually there
        assert_all(self.session, "SELECT key FROM ks.test", [["eggs"], ["spam"], ["sausage"]])

    def test_create_keyspace(self):
        assert "created" not in self.get_keyspace_names()

        self.node1.run_cqlsh("CREATE KEYSPACE created WITH replication = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }")
        assert "created" in self.get_keyspace_names()

    def test_drop_keyspace(self):
        create_ks(self.session, "ks", 1)
        assert "ks" in self.get_keyspace_names()

        self.node1.run_cqlsh("DROP KEYSPACE ks")

        assert "ks" not in self.get_keyspace_names()

    def test_create_table(self):
        create_ks(self.session, "ks", 1)

        self.node1.run_cqlsh("CREATE TABLE ks.test (i int PRIMARY KEY);")
        assert self.get_tables_in_keyspace("ks") == ["test"]

    def test_drop_table(self):
        create_ks(self.session, "ks", 1)
        create_cf(self.session, "test")

        assert_none(self.session, "SELECT key FROM test")

        self.node1.run_cqlsh("DROP TABLE ks.test;")
        self.session.cluster.refresh_schema_metadata()

        assert 0 == len(self.session.cluster.metadata.keyspaces["ks"].tables)

    def test_truncate(self):
        create_ks(self.session, "ks", 1)
        create_cf(self.session, "test", columns={"i": "int"})

        self.session.execute("INSERT INTO test (key) VALUES ('a')")
        self.session.execute("INSERT INTO test (key) VALUES ('b')")
        self.session.execute("INSERT INTO test (key) VALUES ('c')")
        self.session.execute("INSERT INTO test (key) VALUES ('d')")
        self.session.execute("INSERT INTO test (key) VALUES ('e')")
        assert_all(self.session, "SELECT key from test", [["a"], ["c"], ["e"], ["d"], ["b"]])

        self.node1.run_cqlsh("TRUNCATE ks.test;")
        assert [] == rows_to_list(self.session.execute("SELECT * from test"))

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

        self.node1.run_cqlsh("TRUNCATE ks.test;")
        assert [] == rows_to_list(self.session.execute("SELECT * from test limit 1"))

    def test_alter_table(self):
        create_ks(self.session, "ks", 1)
        create_cf(self.session, "test", columns={"i": "ascii"})

        def get_ks_columns():
            table = self.session.cluster.metadata.keyspaces["ks"].tables["test"]

            return [[table.name, column.name, column.cql_type] for column in table.columns.values()]

        old_column_spec = ["test", "i", "ascii"]
        assert old_column_spec in get_ks_columns()

        self.node1.run_cqlsh("ALTER TABLE ks.test ALTER i TYPE text;")
        self.session.cluster.refresh_table_metadata("ks", "test")

        new_columns = get_ks_columns()
        assert old_column_spec not in new_columns
        assert ["test", "i", "text"] in new_columns

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

    # DROP INDEX statement fails in 2.0 (see CASSANDRA-9247)
    def test_drop_index(self):
        create_ks(self.session, "ks", 1)
        create_cf(self.session, "test", columns={"i": "int"})

        # create a statement that will only work if there's an index on i
        requires_index = "SELECT * from test WHERE i = 5"

        # make sure it fails as expected
        with pytest.raises(InvalidRequest):
            return self.session.execute(requires_index)

        # make sure it doesn't fail when an index exists
        self.session.execute("CREATE INDEX index_to_drop ON test (i);")
        assert_none(self.session, requires_index)

        # drop the index via cqlsh, then make sure it fails
        self.node1.run_cqlsh("DROP INDEX ks.index_to_drop;")
        with pytest.raises(InvalidRequest):
            return self.session.execute(requires_index)

    # DROP INDEX statement fails in 2.0 (see CASSANDRA-9247)
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

    @pytest.mark.use_cassandra_stress
    @pytest.mark.cluster_options(rf_rack_valid_keyspaces=False)  # test need to clear a rack, and reduce RF, not possible yet with rf_rack_valid_keyspaces=True
    def test_select_all_cl_quorum(self):
        """
        https://github.com/scylladb/scylla/issues/2593
        ccm create scylla-4 --scylla --vnodes -n 4 --install-dir=/home//scylla
        ccm start
        ccm node1 stress write n=1000
        ccm node1 cqlsh
            select * from keyspace1.standard1;
            alter KEYSPACE keyspace1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : '4'};
            consistency quorum;
            select * from keyspace1.standard1;
        """
        for i in range(3):
            new_node(self.cluster, bootstrap=False, data_center="datacenter1", rack=f"rack{i + 2}")
        self.cluster.start()
        self.cluster.nodelist()[0].stress(["write", "n=1K", "-rate", "threads=8", "-schema", f"replication(factor=4)"])

        ks1_stdout, _ks1_stderr = self.node1.run_cqlsh("select * from keyspace1.standard1 LIMIT 10;", return_output=True)
        assert 10 == len([x for x in ks1_stdout.split("\n") if x and x.startswith(" 0x")])

        ks1_stdout, _ks1_stderr = self.node1.run_cqlsh("alter KEYSPACE keyspace1 WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : '3'};", return_output=True)
        ks1_stdout, _ks1_stderr = self.node1.run_cqlsh("select * from keyspace1.standard1 LIMIT 10;", return_output=True)
        assert 10 == len([x for x in ks1_stdout.split("\n") if x and x.startswith(" 0x")])
        ks1_stdout, _ks1_stderr = self.node1.run_cqlsh("consistency quorum;", return_output=True)
        assert ks1_stdout == "Consistency level set to QUORUM.\n"
        ks1_stdout, _ks1_stderr = self.node1.run_cqlsh("consistency quorum; select * from keyspace1.standard1 LIMIT 10;", return_output=True)
        assert 10 == len([x for x in ks1_stdout.split("\n") if x and x.startswith(" 0x")])

    def get_keyspace_names(self):
        self.session.cluster.refresh_schema_metadata()
        return [ks.name for ks in self.session.cluster.metadata.keyspaces.values()]

    def get_tables_in_keyspace(self, keyspace):
        self.session.cluster.refresh_schema_metadata()
        return [table.name for table in self.session.cluster.metadata.keyspaces[keyspace].tables.values()]


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestCqlLogin(CqlshVersionMixing):
    """
    Tests login which requires password authenticator
    """

    @pytest.fixture(scope="function", autouse=True)
    def setup(self):
        config = {"authenticator": "org.apache.cassandra.auth.PasswordAuthenticator"}
        self.cluster.set_configuration_options(values=config)
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        [self.node1] = self.cluster.nodelist()
        self.node1.watch_log_for("Created default superuser")
        self.session = self.patient_cql_connection(self.node1, user="cassandra", password="cassandra")

    def test_login_keeps_keyspace(self):
        create_ks(self.session, "ks1", 1)
        create_cf(self.session, "ks1table")
        self.session.execute("CREATE USER user1 WITH PASSWORD 'changeme';")

        cqlsh_stdout, _cqlsh_stderr = self.node1.run_cqlsh(
            """
            USE ks1;
            DESCRIBE TABLES;
            LOGIN user1 'changeme';
            DESCRIBE TABLES;
            """,
            return_output=True,
            cqlsh_options=self.cqlsh_options(),
        )
        assert [x for x in cqlsh_stdout.split() if x] == ["ks1table", "ks1table"]

    def test_login_rejects_bad_pass(self):
        create_ks(self.session, "ks1", 1)
        create_cf(self.session, "ks1table")
        self.session.execute("CREATE USER user1 WITH PASSWORD 'changeme';")

        _out, err = self.node1.run_cqlsh(
            """
            LOGIN user1 'badpass';
            """,
            return_output=True,
            cqlsh_options=self.cqlsh_options(),
        )
        assert "Username and/or password are incorrect" in err

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

    def test_login_allows_bad_pass_and_continued_use(self):
        create_ks(self.session, "ks1", 1)
        create_cf(self.session, "ks1table")
        self.session.execute("CREATE USER user1 WITH PASSWORD 'changeme';")

        cqlsh_stdout, cqlsh_stderr = self.node1.run_cqlsh(
            """
            LOGIN user1 'badpass';
            USE ks1;
            DESCRIBE TABLES;
            """,
            return_output=True,
            cqlsh_options=self.cqlsh_options(),
        )
        assert [x for x in cqlsh_stdout.split() if x] == ["ks1table"]
        assert "Username and/or password are incorrect" in cqlsh_stderr


@pytest.mark.dtest_full
@pytest.mark.single_node
class TestCqlshWithSSL(TestCqlsh):
    ssl = True

    def create_session(self, username: str | None = None, password: str | None = None):
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        if self.require_client_auth:
            ssl_context.load_cert_chain(certfile=os.path.join(self.test_path, "ccm_node.pem"), keyfile=os.path.join(self.test_path, "ccm_node.key"))
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.load_verify_locations(cafile=os.path.join(self.test_path, "ccm_node.cer"))

        return self.patient_cql_connection(self.node1, ssl_context=ssl_context, user=username, password=password)

    @pytest.fixture(scope="function", autouse=True, params=[True, False], ids=["require_client_auth=true", "require_client_auth=false"])
    def setup(self, tmp_path: Path, request: pytest.FixtureRequest):
        self.require_client_auth = request.param
        generate_ssl_stores(self.test_path, ip_addresses=[f"{self.cluster.get_ipprefix()}1"])
        options = {"enabled": True}
        options.update({"certificate": os.path.join(self.test_path, "ccm_node.pem"), "keyfile": os.path.join(self.test_path, "ccm_node.key")})
        options.update({"truststore": os.path.join(self.test_path, "ccm_node.cer"), "require_client_auth": self.require_client_auth})
        self.cluster.set_configuration_options({"client_encryption_options": options, "auto_snapshot": False})
        self.cluster.populate(1).start(wait_for_binary_proto=True)
        self.node1, *_ = self.cluster.nodelist()

        self.cqlshrc_file = tmp_path / "cqlshrc"
        cqlshrc_content = dedent(
            f"""
            [ssl]
            certfile = {Path(self.test_path) / "ccm_node.cer"}
            validate = true
            """
        )
        if self.require_client_auth:
            cqlshrc_content += dedent(
                f"""
                userkey = {Path(self.test_path) / "ccm_node.key"}
                usercert = {Path(self.test_path) / "ccm_node.pem"}
            """
            )
        self.cqlshrc_file.write_text(cqlshrc_content)
        self.session = self.create_session()
