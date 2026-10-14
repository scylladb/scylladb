#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Thin ccm-compatible shim for ccmlib.node.

Not-yet-adapted dtest/unported test modules import `Node`, `Status`, and the
exception types from here (as ccm's ccmlib.node does), while the in-tree port
implements the equivalent functionality in ccmlib.scylla_node. Re-export what
already exists there, and add back the couple of names ccm's ccmlib.node
provides that scylla_node.py has no equivalent for.
"""

from __future__ import annotations

from test.cluster.dtest.ccmlib.scylla_node import NodeError, NodetoolError, ScyllaNode, Status, ToolError

# ccm's ccmlib.node.Node is the base class ScyllaNode used to extend; in the
# in-tree port ScyllaNode is the concrete node class.
Node = ScyllaNode

__all__ = ["Node", "NodeError", "NodetoolError", "Status", "TimeoutError", "ToolError"]


# ccm's ccmlib.node defined its own TimeoutError and raised that same class.
# Here the timeouts are raised by ccmlib.scylla_node and by the tools modules,
# and they all raise the builtin, so this name has to be the builtin too --
# otherwise `from ccmlib.node import TimeoutError` gives tests a class that
# nothing raises and their `except TimeoutError:` never fires.
TimeoutError = TimeoutError  # noqa: PLW0127
