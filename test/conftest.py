#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import _pytest
import allure
import pytest
from test import TEST_RUNNER
from test.pylib.report_plugin import ReportPlugin
from test.pylib.skip_reason_plugin import SkipReasonPlugin
from test.pylib.skip_types import SkipType


pytest_plugins = []

def dynamic_scope() -> _pytest.scope._ScopeName:
    """Dynamic scope for fixtures which rely on a current test.py suite/test.

    Even though test.py not running tests anymore, there is some logic still there that requires module scope.
    When using runpy runner, all custom logic should be disabled and scope should be session.
    """
    if TEST_RUNNER == "runpy":
        return "session"
    return "module"


if TEST_RUNNER == "runpy":
    @pytest.fixture(scope="session")
    def scylla_cluster() -> None:
        return None
else:
    pytest_plugins.append("test.pylib.runner")


def pytest_addoption(parser: pytest.Parser) -> None:
    # In the root conftest so it is registered in every pytest session (incl. runpy).
    parser.addoption('--artifacts_dir_url', action='store', type=str, default=None, dest='artifacts_dir_url',
                     help='URL to the artifacts directory, used to generate links to failed tests log folders')


def pytest_configure(config: pytest.Config) -> None:
    config.pluginmanager.register(ReportPlugin())

    def _allure_report(skip_type: str, reason: str) -> None:
        """Enrich Allure reports with skip type metadata."""
        allure.dynamic.tag(f"skip_type:{skip_type}")
        allure.dynamic.label("skip_type", skip_type)

    config.pluginmanager.register(SkipReasonPlugin(SkipType, report_callback=_allure_report))
