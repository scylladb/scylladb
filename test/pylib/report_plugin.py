#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import platform

import allure
import pytest
from allure_commons.model2 import Status
from allure_pytest.utils import get_pytest_report_status


class ReportPlugin:
    config = None

    # Pytest hook to attach logs to the Allure report only when the test fails.
    def pytest_configure(self, config):
        self.config = config

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_makereport(self):
        outcome = yield
        report = outcome.get_result()
        status =  get_pytest_report_status(report)
        # skip attaching logs for passed tests
        # attach_capture is a destination for "--allure-no-capture" option from allure-plugin
        if status != Status.PASSED and not self.config.option.attach_capture:
            if report.caplog:
                allure.attach(report.caplog, "log", allure.attachment_type.TEXT, None)
            if report.capstdout:
                allure.attach(report.capstdout, "stdout", allure.attachment_type.TEXT, None)
            if report.capstderr:
                allure.attach(report.capstderr, "stderr", allure.attachment_type.TEXT, None)

    @pytest.fixture(autouse=True)
    def allure_run_parameters(self, request):
        """
        Make Allure tell architectures, modes and repeats apart.

        Allure groups results by test function name and parameters into one result with
        retries; the .mode.run_id suffix is only in the node name. The mode is named
        build_mode: Allure prefers a test's own parameter of the same name.
        """
        # Imported here: test.pylib.runner is a pytest plugin, importing it from conftest
        # would keep pytest from rewriting its asserts. It is loaded by the time tests run.
        from test.pylib.runner import BUILD_MODE, RUN_ID

        allure.dynamic.parameter("arch", platform.machine())
        if (mode := request.node.stash.get(BUILD_MODE, None)) is not None:
            allure.dynamic.tag(mode)
            allure.dynamic.parameter("build_mode", mode)
            allure.dynamic.parameter("run_id", request.node.stash[RUN_ID])
