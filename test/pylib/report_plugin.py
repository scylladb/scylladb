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
    build_mode = None
    run_id = None

    # Pytest hook to modify test name to include mode and run_id
    def pytest_configure(self, config):
        self.build_mode = config.getoption('mode')
        self.config = config
        self.run_id = config.getoption("run_id")

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_makereport(self):
        outcome = yield
        report = outcome.get_result()
        if self.build_mode is not None or self.run_id is not None:
            report.nodeid = f"{report.nodeid}.{self.build_mode}.{self.run_id}"
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

    @pytest.fixture(scope="function", autouse=True)
    def allure_set_mode(self, request):
        """
        Add mode tag to be able to search by it.
        Add parameters to make allure distinguish them and not put them to retries.
        """
        request.node.name = f"{request.node.name}.{self.build_mode}.{self.run_id}"
        allure.dynamic.tag(self.build_mode)
        allure.dynamic.parameter('mode', self.build_mode)
        allure.dynamic.parameter('run_id', self.run_id)
