# Copyright 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

#############################################################################
# Tests for Scylla's command line help ("scylla --help")
#############################################################################

from test import DEBUG_MODES
import collections
import re
import subprocess
import pytest

# "scylla --help" must not list any option twice.
@pytest.mark.skip_mode(mode=["dev", *DEBUG_MODES], reason="scylla-dtest gates on it in release mode only")
def test_scylla_help_has_no_duplicate_options(scylla_path):
    help_text = subprocess.run([scylla_path, '--help'], capture_output=True, text=True).stdout
    assert 'Scylla options:' in help_text
    # Options are listed on lines like:
    #   -h [ --help ]                         show help message
    #   --options-file arg                    configuration file (i.e.
    options = []
    for match in re.findall(r'^  (?:(-\w) \[ (--[\w-]+) \]|(--[\w-]+))', help_text, re.M):
        options += [o for o in match if o]
    # Make sure the options were parsed at all.
    assert len(options) > 100
    assert [o for o, n in collections.Counter(options).items() if n > 1] == []
