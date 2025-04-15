#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import os
import random
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pytest import Session


def pytest_sessionstart(session: Session) -> None:
    if not session.config.getoption("collectonly") and "xdist" in sys.modules:
        if sys.modules["xdist"].is_xdist_controller(request_or_session=session):
            os.environ["TOPOLOGY_RANDOM_FAILURES_TEST_SHUFFLE_SEED"] = str(random.randrange(sys.maxsize))
