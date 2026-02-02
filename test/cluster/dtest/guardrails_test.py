#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging

import pytest
from cassandra.cluster import Session
from cassandra.protocol import ConfigurationException, InvalidRequest

from dtest_class import Tester

logger = logging.getLogger(__name__)


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


def assert_creating_ks_fails(session, query, ks_name):
    with pytest.raises(ConfigurationException):
        session.execute(query)
    with pytest.raises(InvalidRequest):
        session.execute(f"USE {ks_name}")


@pytest.mark.next_gating
class TestGuardrails(Tester):
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
        create_ks_and_assert_warning(session_dc1, query, ks_name, ["warn", "min", "replication", "factor", "3", "dc1", "2"])

    def test_all_rf_limits(self):
        """
        There're 4 limits for RF: soft/hard min and soft/hard max limits. Breaking soft limits issues a warning,
        breaking the hard limits prevents the query from being executed.
        """
        cluster = self.cluster

        # FIXME: This test verifies that guardrails work. However, if we set `rf_rack_valid_keyspaces` to true,
        # we'll get a different error, so let's disable it for now. For more context, see issues:
        #   scylladb/scylladb#23071 and scylladb/scylla-dtest#5633.
        cluster.set_configuration_options(values={"rf_rack_valid_keyspaces": False})

        cluster.set_configuration_options(
            values={"minimum_replication_factor_fail_threshold": 2, "minimum_replication_factor_warn_threshold": 3, "maximum_replication_factor_warn_threshold": 4, "maximum_replication_factor_fail_threshold": 5}
        )

        query = "CREATE KEYSPACE %s WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'dc1': %s}"
        cluster.populate([1]).start()
        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        rf = 1
        ks_name = "ks_a"
        assert_creating_ks_fails(session, query % (ks_name, rf), ks_name)

        rf = 2
        ks_name = "ks_b"
        create_ks_and_assert_warning(session, query % (ks_name, rf), ks_name, ["warn", "min", "replication", "factor", "3", "dc1", "2"])

        rf = 3
        ks_name = "ks_c"
        create_ks_and_assert_warning(session, query % (ks_name, rf), ks_name, [])

        rf = 4
        ks_name = "ks_d"
        create_ks_and_assert_warning(session, query % (ks_name, rf), ks_name, [])

        rf = 5
        ks_name = "ks_e"
        create_ks_and_assert_warning(session, query % (ks_name, rf), ks_name, ["warn", "max", "replication", "factor", "4", "dc1", "5"])

        rf = 6
        ks_name = "ks_f"
        assert_creating_ks_fails(session, query % (ks_name, rf), ks_name)
