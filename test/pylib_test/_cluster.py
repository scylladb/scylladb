#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import pathlib

from test.pylib.scylla_cluster import ScyllaCluster


def make_cluster(vardir: pathlib.Path) -> ScyllaCluster:
    """Create a cluster for tests that never start Scylla."""

    return ScyllaCluster(
        test_name="pylib_test",
        logger=logging.getLogger(__name__),
        vardir=vardir,
        mode="dev",
        cmdline_options=[],
        cmdline_options_override=[],
        config_options={},
        append_env={},
        scylla_exe="/nonexistent/scylla",
    )
