#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import json
import logging
import os
from typing import Any
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


def pytest_configure(config: pytest.Config) -> None:
    config.pluginmanager.register(ReportPlugin())


def pytest_sessionstart(session: pytest.Session):
    argus_reporter = session.config.pluginmanager.get_plugin("argus-reporter-runtime")
    if argus_reporter and session.config.getoption("--argus-post-reports"):
        try:
            argus_reporter.base_url = os.environ["ARGUS_BASE_URL"]
            argus_reporter.api_key = os.environ["ARGUS_AUTH_TOKEN"]
            argus_reporter.extra_headers = os.environ.get("ARGUS_EXTRA_HEADERS", "{}")
            argus_reporter.run_id = os.environ["ARGUS_RUN_ID"]
            argus_reporter.test_type = "test.py"
            logger.warning("Initialized argus with following settings: BaseUrl: %s, Token Length: %s, Headers: %s, Run Id: %s", argus_reporter.base_url, len(argus_reporter.api_key), bool(argus_reporter.extra_headers), argus_reporter.run_id)
        except KeyError as exc:
            logger.warning("Unable to configure Argus Reporting, missing: %s", exc.args[0], exc_info=True)


def pytest_collection_modifyitems(items, config: pytest.Config):
    if argus_reporter := config.pluginmanager.get_plugin("argus-reporter-runtime"):
        query_fields: dict[str, Any] = dict(status="passed")
        query_fields["SCYLLA_MODE"] = config.getoption("mode", "NoMode")
        argus_reporter.slices_query_fields = query_fields


@pytest.fixture(scope="session", autouse=True)
def configure_argus_reporter(request: pytest.FixtureRequest):
    argus_reporter = None
    try:
        argus_reporter = request.getfixturevalue("argus_reporter")
    except pytest.FixtureLookupError:
        pass

    if argus_reporter:
        extra_data = {
            "SCYLLA_ARCH": os.environ.get("SCYLLA_ARCH") or os.uname().machine,
            **{k.lower(): v for k, v in os.environ.items() if k.startswith("BUILD_")},
        }
        argus_reporter.session_data.update(**extra_data)
