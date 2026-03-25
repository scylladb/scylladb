#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import allure
import pytest

from test import TEST_RUNNER
from test.pylib.report_plugin import ReportPlugin
from test.pylib.skip_reason_plugin import SkipReasonPlugin
from test.pylib.skip_types import SkipType


pytest_plugins = []

if TEST_RUNNER == "runpy":
    @pytest.fixture(scope="session")
    def testpy_test() -> None:
        return None
else:
    pytest_plugins.append("test.pylib.runner")


def pytest_configure(config: pytest.Config) -> None:
    config.pluginmanager.register(ReportPlugin())

    def _allure_report(skip_type: str, reason: str) -> None:
        """Enrich Allure reports with skip type metadata."""
        allure.dynamic.tag(f"skip_type:{skip_type}")
        allure.dynamic.label("skip_type", skip_type)

    config.pluginmanager.register(SkipReasonPlugin(SkipType, report_callback=_allure_report))
