#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.protocol import ConfigurationException, InvalidRequest
from cassandra.query import SimpleStatement

from dtest_class import Tester, create_ks, get_ip_from_node
from tools.metrics import get_node_metrics

logger = logging.getLogger(__name__)

class TestRfGuardrails(Tester):

    @staticmethod
    def create_ks_and_assert_warning(session, query, ks_name, key_warn_msg_words):
        ret = session.execute_async(query)
        _ = ret.result()
        found = False
        if len(key_warn_msg_words) > 0:
            assert len(ret.warnings) >= 1, "Expected RF guardrail warning"
            for warning in ret.warnings:
                found = found or all(word in warning.lower() for word in key_warn_msg_words)
            assert found, "Didn't match all required keywords"
        session.execute(f"USE {ks_name}")

    @staticmethod
    def assert_creating_ks_fails(session, query, ks_name):
        with pytest.raises(ConfigurationException):
            session.execute(query)
        with pytest.raises(InvalidRequest):
            session.execute(f"USE {ks_name}")

    def test_default_rf(self):
        """
        As of now, the only RF guardrail enabled is a soft limit checking that RF >= 3. Not complying to this soft limit
        results in a CQL being executed, but with a warning. Also, whatever the guardrails' values, RF = 0 is always OK.
        """
        cluster = self.cluster

        # FIXME: This test verifies that guardrails work. However, if we set `rf_rack_valid_keyspaces` to true,
        # we'll get a different error, so let's disable it for now. For more context, see issues:
        #   scylladb/scylladb#23071 and scylladb/scylla-dtest#5633.
        cluster.set_configuration_options(values={"rf_rack_valid_keyspaces": False})

        cluster.populate([1, 1, 1]).start(wait_other_notice=True)
        session_dc1: Session = self.patient_cql_connection(cluster.nodelist()[0])

        ks_name = "ks"
        rf = {"dc1": 2, "dc2": 3, "dc3": 0}
        query = "CREATE KEYSPACE %s WITH REPLICATION={%s}"
        options = ", ".join(["'%s':%d" % (dc_value, rf_value) for dc_value, rf_value in rf.items()])
        query = query % (ks_name, "'class':'NetworkTopologyStrategy', %s" % options)
        self.create_ks_and_assert_warning(session_dc1, query, ks_name, ["warn", "min", "replication", "factor", "3", "dc1", "2"])

    def test_all_rf_limits(self):
        """
        There're 4 limits for RF: soft/hard min and soft/hard max limits. Breaking soft limits issues a warning,
        breaking the hard limits prevents the query from being executed.
        """
        cluster = self.cluster

        MIN_FAIL_THRESHOLD = 2
        MIN_WARN_THRESHOLD = 3
        MAX_WARN_THRESHOLD = 4
        MAX_FAIL_THRESHOLD = 5

        # FIXME: This test verifies that guardrails work. However, if we set `rf_rack_valid_keyspaces` to true,
        # we'll get a different error, so let's disable it for now. For more context, see issues:
        #   scylladb/scylladb#23071 and scylladb/scylla-dtest#5633.
        cluster.set_configuration_options(values={"rf_rack_valid_keyspaces": False})

        cluster.set_configuration_options(
            values={
                "minimum_replication_factor_fail_threshold": MIN_FAIL_THRESHOLD, "minimum_replication_factor_warn_threshold": MIN_WARN_THRESHOLD, "maximum_replication_factor_warn_threshold": MAX_WARN_THRESHOLD,
                "maximum_replication_factor_fail_threshold": MAX_FAIL_THRESHOLD
            }
        )

        query = "CREATE KEYSPACE %s WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'dc1': %s}"
        cluster.populate([1]).start()
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        def test_rf(rf):
            ks_name = f"ks_{rf}"
            if rf < MIN_FAIL_THRESHOLD or rf > MAX_FAIL_THRESHOLD:
                self.assert_creating_ks_fails(session, query % (ks_name, rf), ks_name)
            elif rf < MIN_WARN_THRESHOLD:
                self.create_ks_and_assert_warning(session, query % (ks_name, rf), ks_name, ["warn", "min", "replication", "factor", str(MIN_WARN_THRESHOLD), "dc1", "2"])
            elif rf > MAX_WARN_THRESHOLD:
                self.create_ks_and_assert_warning(session, query % (ks_name, rf), ks_name, ["warn", "max", "replication", "factor", str(MAX_WARN_THRESHOLD), "dc1", "5"])
            else:
                self.create_ks_and_assert_warning(session, query % (ks_name, rf), ks_name, [])

        for rf in range(MIN_FAIL_THRESHOLD - 1, MAX_FAIL_THRESHOLD + 1):
            test_rf(rf)


class TestWriteConsistencyLevelGuardrails(Tester):

    WARNED_METRIC = "scylla_cql_write_consistency_levels_warned_violations"
    DISALLOWED_METRIC = "scylla_cql_write_consistency_levels_disallowed_violations"

    def metric_for_cl(self, name, cl):
        cl_name = ConsistencyLevel.value_to_name[cl]
        return f'{name}{{consistency_level="{cl_name}"'

    def get_metric(self, node, name, cl):
        pattern = self.metric_for_cl(name, cl)
        return get_node_metrics(get_ip_from_node(node), [pattern])[pattern]

    def assert_metric_increased(self, node, name, cl, before):
        after = self.get_metric(node, name, cl)
        cl_name = ConsistencyLevel.value_to_name[cl]
        assert after > before, f"Expected {name} for {cl_name} to increase, but got {before} -> {after}"

    def assert_metric_unchanged(self, node, name, cl, before):
        after = self.get_metric(node, name, cl)
        cl_name = ConsistencyLevel.value_to_name[cl]
        assert after == before, f"Expected {name} for {cl_name} unchanged, but got {before} -> {after}"

    def change_config(self, session, param, value):
        session.execute(f"UPDATE system.config SET value = '{value}' WHERE name = '{param}'")

    def test_write_cl_live_update(self):
        """
        Test that write_consistency_levels_disallowed and write_consistency_levels_warned
        support live updates via CQL config changes.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={
            "write_consistency_levels_warned": "",
            "write_consistency_levels_disallowed": "",
        })
        cluster.populate([1]).start(wait_for_binary_proto=True)

        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        create_ks(session, "ks", 1)
        session.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

        # ANY should work initially with no metric bumps
        before_warned = self.get_metric(node, self.WARNED_METRIC, ConsistencyLevel.ANY)
        stmt = SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (1, 1)", consistency_level=ConsistencyLevel.ANY)
        session.execute(stmt)
        self.assert_metric_unchanged(node, self.WARNED_METRIC, ConsistencyLevel.ANY, before_warned)

        # Update config via CQL to disallow ANY
        self.change_config(session, "write_consistency_levels_disallowed", "ANY")

        # ANY should now be rejected and disallowed metric should increase
        before_disallowed = self.get_metric(node, self.DISALLOWED_METRIC, ConsistencyLevel.ANY)
        stmt = SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (2, 2)", consistency_level=ConsistencyLevel.ANY)
        with pytest.raises(InvalidRequest):
            session.execute(stmt)
        self.assert_metric_increased(node, self.DISALLOWED_METRIC, ConsistencyLevel.ANY, before_disallowed)

        # Now update to warned instead of disallowed
        self.change_config(session, "write_consistency_levels_disallowed", "")
        self.change_config(session, "write_consistency_levels_warned", "ANY")

        # ANY should now work but warned metric should increase
        before_warned = self.get_metric(node, self.WARNED_METRIC, ConsistencyLevel.ANY)
        stmt = SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (3, 3)", consistency_level=ConsistencyLevel.ANY)
        session.execute(stmt)
        self.assert_metric_increased(node, self.WARNED_METRIC, ConsistencyLevel.ANY, before_warned)
