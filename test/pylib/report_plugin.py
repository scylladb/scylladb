import pytest


class ReportPlugin:
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
