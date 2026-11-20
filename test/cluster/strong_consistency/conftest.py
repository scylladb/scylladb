#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest


@pytest.fixture
def slow(build_mode: str) -> int:
    """How many times longer every duration of a scenario is: debug boots, restarts and
    elects several times slower.

    Not scale_timeout: that one stretches timeouts in every mode, while these are the
    lengths of the scenario's steps, which only debug needs longer.
    """
    return 4 if build_mode == "debug" else 1
