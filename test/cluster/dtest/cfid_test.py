#
# Copyright (C) 2014-present The Apache Software Foundation
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import os
from pathlib import Path

import pytest

from dtest_class import Tester, create_cf, create_ks


# pylint:disable=too-few-public-methods
@pytest.mark.dtest_full
@pytest.mark.single_node
class TestCFID(Tester):
    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    def test_cfid(self):
        """
        Test through adding/dropping cf's that the path to sstables for each cf are unique and formatted correctly
        """
        cluster = self.cluster
        loop_size = 5
        cluster.populate(1).start(wait_other_notice=True)
        [node1] = cluster.nodelist()

        session = self.patient_cql_connection(node1)
        create_ks(session=session, name="ks", rf=1)

        for _ in range(loop_size):
            create_cf(session=session, name="cf", gc_grace=0, key_type="int", columns={"c1": "int"})
            session.execute("insert into cf (key, c1) values (1,1)")
            session.execute("insert into cf (key, c1) values (2,1)")
            node1.flush()
            session.execute("drop table ks.cf;")

        # get a list of cf directories
        try:
            cfs = os.listdir(str(node1.get_path() / Path("data/ks")))
        except OSError as err:
            raise OSError("Path to sstables not valid.") from err

        # check that there are 5 unique directories
        assert len(cfs) == loop_size

        # check that these are in fact column family directories
        for dire in cfs:
            assert dire[0:2] == "cf"
