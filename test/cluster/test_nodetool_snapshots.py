#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""
Snapshots taken, listed, cleared and restored through "scylla nodetool", on
a node of their own, and checked against the snapshot directories on disk.

Ported from scylla-dtest. Like the dtests, the nodes create keyspaces with
vnodes by default.
"""


# Like the dtests' nodes, new keyspaces use vnodes by default, 256 per node, and dropped or
# truncated tables are snapshotted (test.py turns auto_snapshot off).
CONFIG = {'tablets_mode_for_new_keyspaces': 'disabled', 'num_tokens': 256, 'tablets_initial_scale_factor': 1, 'auto_snapshot': True}


def ks_opts(rf: int = 1) -> str:
    return f"WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': {rf}}}"

