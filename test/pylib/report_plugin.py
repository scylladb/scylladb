#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

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
