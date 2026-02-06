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

from dtest_class import Tester, create_ks

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
    """Tests for write_consistency_levels_warned and write_consistency_levels_disallowed config options."""

    def test_write_cl_guardrails(self):
        """
        Test write_consistency_levels_disallowed and write_consistency_levels_warned config options.
        """
        cluster = self.cluster
        cluster.populate([1]).start(wait_for_binary_proto=True)

        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        create_ks(session, "ks", 1)
        session.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

        # Set disallowed and warned CLs via live config update
        self.change_config(session, "write_consistency_levels_disallowed", "ANY, ALL")
        self.change_config(session, "write_consistency_levels_warned", "QUORUM")

        # Disallowed CLs should be rejected
        stmt = SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (1, 1)", consistency_level=ConsistencyLevel.ANY)
        with pytest.raises(InvalidRequest) as exc_info:
            session.execute(stmt)
        assert "not allowed" in str(exc_info.value).lower()

        stmt = SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (2, 2)", consistency_level=ConsistencyLevel.ALL)
        with pytest.raises(InvalidRequest):
            session.execute(stmt)

        # Warned CL should succeed with warning
        stmt = SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (3, 3)", consistency_level=ConsistencyLevel.QUORUM)
        result = session.execute_async(stmt)
        result.result()
        assert result.warnings is not None and len(result.warnings) >= 1

        # Non-guardrailed CL should work without warning
        stmt = SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (4, 4)", consistency_level=ConsistencyLevel.ONE)
        result = session.execute_async(stmt)
        result.result()
        assert result.warnings is None or len(result.warnings) == 0

    def test_write_cl_invalid_level_ignored(self):
        """
        Test that invalid consistency level names in the config are ignored.
        The node should start and valid operations should work.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={
            "write_consistency_levels_disallowed": "INVALID_CL, ANY"
        })
        cluster.populate([1]).start(wait_for_binary_proto=True)

        node = cluster.nodelist()[0]

        # Verify that the invalid CL was logged
        node.watch_log_for(r"Ignoring unknown consistency level 'INVALID_CL'")

        session = self.patient_cql_connection(node)

        create_ks(session, "ks", 1)
        session.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

        # ONE should work (INVALID_CL ignored)
        stmt = SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (1, 1)", consistency_level=ConsistencyLevel.ONE)
        session.execute(stmt)

        # ANY should still be rejected
        stmt = SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (2, 2)", consistency_level=ConsistencyLevel.ANY)
        with pytest.raises(InvalidRequest):
            session.execute(stmt)

    def test_write_cl_case_insensitive(self):
        """
        Test that consistency level names in config are case-insensitive.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={
            "write_consistency_levels_disallowed": "any, Any, ANY"
        })
        cluster.populate([1]).start(wait_for_binary_proto=True)

        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        create_ks(session, "ks", 1)
        session.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

        # ANY should be rejected (config values should be deduplicated)
        stmt = SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (1, 1)", consistency_level=ConsistencyLevel.ANY)
        with pytest.raises(InvalidRequest):
            session.execute(stmt)

    def change_config(self, session, param, value):
        """Helper to change a live-updatable config parameter via CQL."""
        session.execute(f"UPDATE system.config SET value = '{value}' WHERE name = '{param}'")

    def test_write_cl_live_update(self):
        """
        Test that write_consistency_levels_disallowed and write_consistency_levels_warned
        support live updates via CQL config changes.
        """
        cluster = self.cluster
        # Start with no guardrails
        cluster.populate([1]).start(wait_for_binary_proto=True)

        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        create_ks(session, "ks", 1)
        session.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

        # ANY should work initially
        stmt = SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (1, 1)", consistency_level=ConsistencyLevel.ANY)
        session.execute(stmt)

        # Update config via CQL to disallow ANY
        self.change_config(session, "write_consistency_levels_disallowed", "ANY")

        # ANY should now be rejected
        stmt = SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (2, 2)", consistency_level=ConsistencyLevel.ANY)
        with pytest.raises(InvalidRequest):
            session.execute(stmt)

        # Now update to warned instead of disallowed
        self.change_config(session, "write_consistency_levels_disallowed", "")
        self.change_config(session, "write_consistency_levels_warned", "ANY")

        # ANY should now work but with warning
        stmt = SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (3, 3)", consistency_level=ConsistencyLevel.ANY)
        result = session.execute_async(stmt)
        result.result()
        assert result.warnings is not None and len(result.warnings) >= 1

    def test_write_cl_empty_config(self):
        """
        Test that empty config values work (no CL is blocked or warned).
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={
            "write_consistency_levels_disallowed": "",
            "write_consistency_levels_warned": ""
        })
        cluster.populate([1]).start(wait_for_binary_proto=True)

        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        create_ks(session, "ks", 1)
        session.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

        # All CLs should work without warnings
        for cl in [ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.QUORUM]:
            stmt = SimpleStatement(f"INSERT INTO ks.t (pk, v) VALUES (1, 2)", consistency_level=cl)
            result = session.execute_async(stmt)
            result.result()
            assert result.warnings is None or len(result.warnings) == 0

    def test_write_cl_only_affects_writes(self):
        """
        Test that write CL guardrails only affect writes (INSERT, UPDATE), not reads (SELECT).
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={
            "write_consistency_levels_warned": "ONE"
        })
        cluster.populate([1]).start(wait_for_binary_proto=True)

        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        create_ks(session, "ks", 1)
        session.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

        # INSERT with ONE should produce warning (default config)
        stmt = SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (1, 1)", consistency_level=ConsistencyLevel.ONE)
        result = session.execute_async(stmt)
        result.result()
        assert result.warnings is not None and len(result.warnings) >= 1, "INSERT should produce warning"

        # UPDATE with ONE should also produce warning
        stmt = SimpleStatement("UPDATE ks.t SET v = 2 WHERE pk = 1", consistency_level=ConsistencyLevel.ONE)
        result = session.execute_async(stmt)
        result.result()
        assert result.warnings is not None and len(result.warnings) >= 1, "UPDATE should produce warning"

        # SELECT with ONE should NOT produce warning
        stmt = SimpleStatement("SELECT * FROM ks.t WHERE pk = 1", consistency_level=ConsistencyLevel.ONE)
        result = session.execute_async(stmt)
        result.result()
        assert result.warnings is None or len(result.warnings) == 0, "SELECT should not produce warning"

    def test_write_cl_delete(self):
        """
        Test that DELETE statements are subject to write CL guardrails (warning and rejection).
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={
            "write_consistency_levels_warned": "ONE",
            "write_consistency_levels_disallowed": "ANY"
        })
        cluster.populate([1]).start(wait_for_binary_proto=True)

        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        create_ks(session, "ks", 1)
        session.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

        # DELETE - Warning
        stmt = SimpleStatement("DELETE FROM ks.t WHERE pk = 1", consistency_level=ConsistencyLevel.ONE)
        result = session.execute_async(stmt)
        result.result()
        assert result.warnings is not None and len(result.warnings) >= 1, "DELETE should produce warning with CL=ONE"

        # DELETE - Disallowed
        stmt = SimpleStatement("DELETE FROM ks.t WHERE pk = 1", consistency_level=ConsistencyLevel.ANY)
        with pytest.raises(InvalidRequest) as exc_info:
            session.execute(stmt)
        assert "not allowed" in str(exc_info.value).lower()

    def test_write_cl_batch_simple(self):
        """
        Test that simple BATCH statements (LOGGED/UNLOGGED) are subject to write CL guardrails.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={
            "write_consistency_levels_warned": "ONE",
            "write_consistency_levels_disallowed": "ANY"
        })
        cluster.populate([1]).start(wait_for_binary_proto=True)

        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        create_ks(session, "ks", 1)
        session.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

        # LOGGED BATCH (Default)
        logged_batch = """
        BEGIN BATCH
            INSERT INTO ks.t (pk, v) VALUES (2, 2);
            UPDATE ks.t SET v = 3 WHERE pk = 3;
        APPLY BATCH;
        """
        # Warning
        stmt = SimpleStatement(logged_batch, consistency_level=ConsistencyLevel.ONE)
        result = session.execute_async(stmt)
        result.result()
        assert result.warnings is not None and len(result.warnings) >= 1, "LOGGED BATCH should produce warning with CL=ONE"
        
        # Disallowed
        stmt = SimpleStatement(logged_batch, consistency_level=ConsistencyLevel.ANY)
        with pytest.raises(InvalidRequest) as exc_info:
            session.execute(stmt)
        assert "not allowed" in str(exc_info.value).lower()

        # UNLOGGED BATCH
        unlogged_batch = """
        BEGIN UNLOGGED BATCH
            INSERT INTO ks.t (pk, v) VALUES (4, 4);
            UPDATE ks.t SET v = 5 WHERE pk = 5;
        APPLY BATCH;
        """
        # Warning
        stmt = SimpleStatement(unlogged_batch, consistency_level=ConsistencyLevel.ONE)
        result = session.execute_async(stmt)
        result.result()
        assert result.warnings is not None and len(result.warnings) >= 1, "UNLOGGED BATCH should produce warning with CL=ONE"
        
        # Disallowed
        stmt = SimpleStatement(unlogged_batch, consistency_level=ConsistencyLevel.ANY)
        with pytest.raises(InvalidRequest) as exc_info:
            session.execute(stmt)
        assert "not allowed" in str(exc_info.value).lower()

    def test_write_cl_conditional_batch(self):
        """
        Test that conditional BATCH statements are subject to write CL guardrails.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={
            "write_consistency_levels_warned": "ONE",
            "write_consistency_levels_disallowed": "ANY"
        })
        cluster.populate([1]).start(wait_for_binary_proto=True)

        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        create_ks(session, "ks", 1)
        session.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

        batch_query = """
        BEGIN BATCH
            INSERT INTO ks.t (pk, v) VALUES (4, 4) IF NOT EXISTS;
        APPLY BATCH;
        """

        # Conditional BATCH - Warning
        stmt = SimpleStatement(batch_query, consistency_level=ConsistencyLevel.ONE)
        result = session.execute_async(stmt)
        result.result()
        assert result.warnings is not None and len(result.warnings) >= 1, "Conditional BATCH should produce warning with CL=ONE"

        # Conditional BATCH - Disallowed
        stmt = SimpleStatement(batch_query, consistency_level=ConsistencyLevel.ANY)
        with pytest.raises(InvalidRequest) as exc_info:
            session.execute(stmt)
        assert "not allowed" in str(exc_info.value).lower()

    def test_write_cl_counter(self):
        """
        Test that counter updates are subject to write CL guardrails.
        """
        cluster = self.cluster
        cluster.set_configuration_options(values={
            "write_consistency_levels_warned": "ONE",
            "write_consistency_levels_disallowed": "ANY"
        })
        cluster.populate([1]).start(wait_for_binary_proto=True)

        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        create_ks(session, "ks", 1)
        session.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, c counter)")

        # Counter Update - Warning
        stmt = SimpleStatement("UPDATE ks.t SET c = c + 1 WHERE pk = 1", consistency_level=ConsistencyLevel.ONE)
        result = session.execute_async(stmt)
        result.result()
        assert result.warnings is not None and len(result.warnings) >= 1, "Counter update should produce warning with CL=ONE"

        # Counter Update - Disallowed
        stmt = SimpleStatement("UPDATE ks.t SET c = c + 1 WHERE pk = 1", consistency_level=ConsistencyLevel.ANY)
        with pytest.raises(InvalidRequest) as exc_info:
            session.execute(stmt)
        assert "not allowed" in str(exc_info.value).lower()

    def test_write_cl_default_warned(self):
        """
        Test that by default ANY, ONE, and LOCAL_ONE produce warnings for writes.
        """
        cluster = self.cluster
        cluster.populate([1]).start(wait_for_binary_proto=True)

        node = cluster.nodelist()[0]
        session = self.patient_cql_connection(node)

        create_ks(session, "ks", 1)
        session.execute("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")

        # ANY, ONE, LOCAL_ONE should produce warnings by default
        for cl in [ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_ONE]:
            stmt = SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (1, 1)", consistency_level=cl)
            result = session.execute_async(stmt)
            result.result()
            assert result.warnings is not None and len(result.warnings) >= 1, f"Expected warning for {cl}"

        # QUORUM should not produce warnings
        stmt = SimpleStatement("INSERT INTO ks.t (pk, v) VALUES (2, 2)", consistency_level=ConsistencyLevel.QUORUM)
        result = session.execute_async(stmt)
        result.result()
        assert result.warnings is None or len(result.warnings) == 0
