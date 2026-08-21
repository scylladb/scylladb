#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging

import pytest

from dtest_class import Tester

logger = logging.getLogger(__file__)


@pytest.mark.single_node
class TestSetSmp(Tester):
    """Test that node.set_smp() properly persists across restarts."""

    def _get_smp_from_log(self, node, from_mark=None):
        """Extract smp value from the node's log by looking at the SHARD_COUNT gossip value."""
        matches = node.grep_log(r"SHARD_COUNT : Value\((\d+),\d+\)", from_mark=from_mark)
        assert matches, "Could not find SHARD_COUNT in node log"
        # Return the last match (most recent start)
        return int(matches[-1][1].group(1))

    def test_set_smp(self):
        """Verify that set_smp() takes effect on the next start."""
        cluster = self.cluster
        cluster.populate(1).start(wait_for_binary_proto=True)
        node1 = cluster.nodelist()[0]

        default_smp = self._get_smp_from_log(node1)

        cluster.stop()

        # set_smp to a different value and restart without jvm_args
        target_smp = 1 if default_smp != 1 else 2
        node1.set_smp(target_smp)
        mark = node1.mark_log()
        cluster.start(wait_for_binary_proto=True)

        node1 = cluster.nodelist()[0]
        actual_smp = self._get_smp_from_log(node1, from_mark=mark)
        assert actual_smp == target_smp, \
            f"Expected smp={target_smp} after set_smp({target_smp}), got {actual_smp}"
