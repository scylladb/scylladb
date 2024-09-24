#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import allure
import pytest
from allure_commons.model2 import Status
from allure_pytest.utils import get_pytest_report_status


class ReportPlugin:
    config = None
    mode = None
    run_id = None

    # Pytest hook to modify test name to include mode and run_id
    def pytest_configure(self, config):
        self.config = config
        self.mode = config.getoption("mode")
        self.run_id = config.getoption("run_id")

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_makereport(self):
        outcome = yield
        report = outcome.get_result()
        if self.mode is not None or self.run_id is not None:
            report.nodeid = f"{report.nodeid}.{self.mode}.{self.run_id}"
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
        run_id = request.config.getoption('run_id')
        mode = request.config.getoption('mode')
        request.node.name = f"{request.node.name}.{mode}.{run_id}"
        allure.dynamic.tag(mode)
        allure.dynamic.parameter('mode', mode)
        allure.dynamic.parameter('run_id', run_id)
