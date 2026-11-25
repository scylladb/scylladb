# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Smoke tests for tools/scyllatop, a top-like tool showing Scylla's metrics.
# They check that scyllatop can still read and parse the metrics that the
# Scylla node under test exports in the Prometheus format.
#############################################################################

from test import DEBUG_MODES
import os
import subprocess
import sys

import pytest

SCYLLATOP = os.path.join(os.path.dirname(__file__), '..', '..', 'tools', 'scyllatop', 'scyllatop.py')

# A metric which every Scylla node exports.
KNOWN_METRIC = 'scylla_reactor_utilization'

@pytest.fixture(scope="module")
def scyllatop(cql):
    # scyllatop needs the urwid module, which may not be installed.
    pytest.importorskip('urwid')
    return [sys.executable, SCYLLATOP, '-p', f'http://{cql.cluster.contact_points[0]}:9180/metrics']

@pytest.fixture
def logfile(tmp_path):
    return ['-L', str(tmp_path / 'scyllatop.log'), '-v', 'DEBUG']

@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_scyllatop_list(scylla_only, scyllatop, logfile):
    out = subprocess.run(scyllatop + logfile + ['--list'], capture_output=True, text=True, check=True, timeout=60).stdout
    assert KNOWN_METRIC in out
