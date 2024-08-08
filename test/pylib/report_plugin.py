import importlib
import logging

import pytest

logger = logging.getLogger(name=__name__)
_allure_module = None


def _get_allure_module():
    global _allure_module
    if _allure_module is None:
        _allure_module = importlib.import_module('allure')
    return _allure_module


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
        report.nodeid = f"{report.nodeid}.{self.mode}.{self.run_id}"

    @pytest.fixture(autouse=True, scope="session")
    def is_allure_installed(self):
        try:
            _get_allure_module()
        except ImportError:
            logger.warning('No allure plugin installed, skipping allure report')
            return False
        else:
            return True

    @pytest.fixture(scope="function", autouse=True)
    def allure_set_mode(self, request, is_allure_installed):
        """
        Add mode tag to be able to search by it.
        Add parameters to make allure distinguish them and not put them to retries.
        """
        if is_allure_installed:
            allure = _get_allure_module()
            run_id = request.config.getoption('run_id')
            mode = request.config.getoption('mode')
            request.node.name = f"{request.node.name}.{mode}.{run_id}"
            allure.dynamic.tag(mode)
            allure.dynamic.parameter('mode', mode)
            allure.dynamic.parameter('run_id', run_id)
