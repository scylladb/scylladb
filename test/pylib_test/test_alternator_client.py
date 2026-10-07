#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import subprocess
import sys
from importlib.metadata import PackageNotFoundError, version

from test import TOP_SRC_DIR
from test.pylib.skip_types import skip_env


def test_dtest_does_not_shadow_alternator_client():
    """Reproduce the combined-collection import failure from PR #31970."""
    try:
        version('alternator-client')
    except PackageNotFoundError:
        skip_env('alternator-client is not installed')
    # pytest prepends this directory when collecting dtest's conftest. Use a
    # fresh interpreter so a previously imported client cannot hide the clash.
    subprocess.run([
        sys.executable, '-c',
        'import sys; sys.path.insert(0, sys.argv[1]); from alternator import Auth; assert callable(Auth.disabled)',
        str(TOP_SRC_DIR / 'test/cluster/dtest'),
    ], check=True, capture_output=True, text=True)
