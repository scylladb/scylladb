#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from dtest_class import Tester, create_ks


MY_NODE = 0


class TestExample(Tester):
    def test_some(self, nodes=3, rf=3, jvm_args=None):
        cluster = self.cluster
        cluster.populate(nodes).start(wait_for_binary_proto=True)
        node = cluster.nodelist()[MY_NODE]
        session = self.patient_cql_connection(node)
        create_ks(session=session, name="ks", rf=rf)

        self.enable_error("error1", MY_NODE)
        self.enable_error("error2", MY_NODE, one_shot=True)
        assert sorted(self.list_errors(MY_NODE)) == sorted(["error1", "error2"])
        self.disable_error("error1", MY_NODE)
        assert self.list_errors(MY_NODE) == ["error2"]
        self.disable_errors(MY_NODE)
        assert self.list_errors(MY_NODE) == []
