#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Thin ccm-compatible shim for ccmlib.cluster.

Not-yet-adapted dtest/unported test modules import `Cluster` from here (as
ccm's ccmlib.cluster does); the in-tree port implements the equivalent
functionality in ccmlib.scylla_cluster.
"""

from __future__ import annotations

from test.cluster.dtest.ccmlib.scylla_cluster import ScyllaCluster

Cluster = ScyllaCluster

__all__ = ["Cluster"]
