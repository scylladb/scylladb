import pytest

from test.pylib.report_plugin import ReportPlugin

ALL_MODES = {'debug': 'Debug',
             'release': 'RelWithDebInfo',
             'dev': 'Dev',
             'sanitize': 'Sanitize',
             'coverage': 'Coverage'}

def pytest_addoption(parser):
    parser.addoption('--mode', choices=ALL_MODES.keys(), dest="mode",
                     help="Run only tests for given build mode(s)")
    parser.addoption('--tmpdir', action='store', default='testlog', help='''Path to temporary test data and log files. The data is
            further segregated per build mode. Default: ./testlog.''', )
    parser.addoption('--run_id', action='store', default=None, help='Run id for the test run')


@pytest.fixture(scope="session")
def build_mode(request):
    """
    This fixture returns current build mode.
    """
    return request.config.getoption('mode')

def pytest_configure(config):
    config.pluginmanager.register(ReportPlugin())