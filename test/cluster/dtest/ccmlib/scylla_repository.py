#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Stand-in for ccm's ccmlib/scylla_repository.py.

The real module downloads/builds relocatable Scylla packages, which the in-tree
dtest port has no use for (Scylla is provided by test.pylib.scylla_cluster_manager
instead). This stub exists only so `from ccmlib import scylla_repository` resolves
at import time for not-yet-adapted dtest/unported test modules; `setup()` is not
implemented and is not expected to be called.
"""

from __future__ import annotations


def setup(version, verbose=True, skip_downloads=False):
    raise NotImplementedError(
        "ccmlib.scylla_repository.setup is not available in the in-tree dtest port; "
        "Scylla installs are managed via test.pylib.scylla_cluster_manager instead."
    )
