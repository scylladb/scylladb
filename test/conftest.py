#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import logging
import pytest

from test import TEST_RUNNER
from test.pylib.report_plugin import ReportPlugin


pytest_plugins = []
logger = logging.getLogger(__name__)


if TEST_RUNNER == "runpy":
    @pytest.fixture(scope="session")
    def testpy_test() -> None:
        return None
else:
    pytest_plugins.append("test.pylib.runner")
    pytest_plugins.append("test.pylib.argus_report")


def pytest_configure(config: pytest.Config) -> None:
    config.pluginmanager.register(ReportPlugin())
