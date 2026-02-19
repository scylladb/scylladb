#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import sys
import logging
import os
import json
from typing import Any

import pytest

LOGGER = logging.getLogger(__name__)


def pytest_sessionstart(session: pytest.Session):
    LOGGER.debug("Session start for %s", os.environ.get("PYTEST_XDIST_WORKER", "master"))
    argus_reporter = session.config.pluginmanager.get_plugin("argus-reporter-runtime")
    reporter_enabled = (
        session.config.getoption("--argus-post-reports")
        or "--pytest-arg=--argus-post-reports" in sys.argv
        or bool(int(os.environ.get("ARGUS_XDIST_ENABLE_REPORTING", "0")))
    )
    LOGGER.debug("Reporting will be %s, Argv: %s", reporter_enabled, sys.argv)
    if argus_reporter and reporter_enabled:
        try:
            argus_reporter.base_url = os.environ["ARGUS_BASE_URL"]
            argus_reporter.api_key = os.environ["ARGUS_AUTH_TOKEN"]
            argus_reporter.extra_headers = json.loads(os.environ.get("ARGUS_EXTRA_HEADERS", "{}"))
            argus_reporter.run_id = os.environ["ARGUS_RUN_ID"]
            argus_reporter.test_type = "test.py"
            argus_reporter.post_reports = reporter_enabled
            if not os.environ.get("PYTEST_XDIST_WORKER"):
                os.environ["ARGUS_XDIST_ENABLE_REPORTING"] = "1"
            LOGGER.debug("Initialized argus with following settings: BaseUrl: %s, Token Length: %s, Headers: %s, Run Id: %s", argus_reporter.base_url, len(argus_reporter.api_key), bool(argus_reporter.extra_headers), argus_reporter.run_id)
        except KeyError as exc:
            LOGGER.warning("Unable to configure Argus Reporting, missing: %s", exc.args[0], exc_info=True)
        except Exception: 
            LOGGER.warning("General error setting up argus reporting..", exc_info=True)


@pytest.fixture(scope="session", autouse=True)
def configure_argus_reporter(request: pytest.FixtureRequest, config: pytest.Config):
    argus_reporter = None
    try:
        argus_reporter = request.getfixturevalue("argus_reporter")
    except pytest.FixtureLookupError:
        pass

    if argus_reporter:
        extra_data = {
            "SCYLLA_ARCH": os.environ.get("SCYLLA_ARCH") or os.uname().machine,
            "SCYLLA_MODE": config.getoption("mode"),
            **{k.lower(): v for k, v in os.environ.items() if k.startswith("BUILD_")},
        }
        argus_reporter.session_data.update(**extra_data)
