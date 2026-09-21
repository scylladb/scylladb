import logging

import pytest

from dtest_class import Tester

logger = logging.getLogger(__name__)


@pytest.mark.dtest_full
class TestLargeColumn(Tester):
    """
    Check that inserting and reading large columns to the database doesn't cause off heap memory usage
    that is proportional to the size of the memory read/written.
    """

    def stress_with_col_size(self, cluster, node, size):
        size = str(size)
        node.stress(["write", "n=5", "no-warmup", "cl=ALL", "-pop", "seq=1...5", "-schema", "replication(factor=2)", "-col", "n=fixed(1)", "size=fixed(" + size + ")", "-rate", "threads=1"])
        node.stress(["read", "n=5", "no-warmup", "cl=ALL", "-pop", "seq=1...5", "-schema", "replication(factor=2)", "-col", "n=fixed(1)", "size=fixed(" + size + ")", "-rate", "threads=1"])

    def directbytes(self, node):
        output = node.nodetool("gcstats", capture_output=True)
        output = output[0].split("\n")
        assert output[0].strip().startswith("Interval"), "Expected output from nodetool gcstats starts with a header line with first column Interval"
        fields = output[1].split()
        assert len(fields) >= 6, "Expected output from nodetool gcstats has at least six fields"
        for field in fields:
            assert field.strip().isdigit() or field == "NaN", "Expected numeric from fields from nodetool gcstats"
        return fields[6]

    @pytest.mark.next_gating
    @pytest.mark.dtest_debug
    @pytest.mark.single_node
    @pytest.mark.use_cassandra_stress
    def test_large_columns_mixed_workload_stress(self):
        """
        See https://github.com/scylladb/scylla/issues/1574
        """
        cluster = self.cluster
        cluster.populate(1).start()
        node1 = cluster.nodelist()[0]
        node1.stress(["write", "n=10000", "no-warmup", "-col", "n=fixed(1)", "size=fixed(40000)", "-rate", "threads=2"])
        node1.stress(["mixed", "no-warmup", "duration=15s", "-pop", "seq=1..10000", "-col", "n=fixed(1)", "size=fixed(40000)", "-rate", "threads=8"])
