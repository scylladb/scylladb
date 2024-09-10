#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import allure
import pytest



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
