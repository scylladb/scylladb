#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import os
import random
import subprocess
import sys
import time
from typing import NamedTuple

import pytest
import requests.exceptions

from test.nodetool.rest_api_mock import set_expected_requests, expected_request, get_expected_requests, \
    get_unexpected_requests, expected_requests_manager
from test.pylib.report_plugin import ReportPlugin


def pytest_addoption(parser):
    parser.addoption('--mode', action='store', default='dev',
                     help='Scylla build mode to use (default: %(default)s)')
    parser.addoption('--nodetool', action='store', choices=["scylla", "cassandra"], default="scylla",
                     help="Which nodetool implementation to run the tests against (default: %(default)s)")
    parser.addoption('--nodetool-path', action='store', default=None,
                     help="Path to the nodetool binary,"
                     " with --nodetool=scylla, this should be the scylla binary,"
                     " with --nodetool=cassandra, this should be the nodetool binary")
    parser.addoption('--jmx-path', action='store', default=None,
                     help="Path to the jmx binary, only used with --nodetool=cassandra")
    parser.addoption('--run-within-unshare', action='store_true',
                     help="Setup the 'lo' network if launched with unshare(1)")
    parser.addoption('--run_id', action='store', default=1,
                     help='Run id for the test run (default: %(default)d)')


class ServerAddress(NamedTuple):
    ip: str
    port: int


@pytest.fixture(scope="session")
def server_address(request):
    # unshare(1) -rn drops us in a new network namespace in which the "lo" is
    # not up yet, so let's set it up first.
    if request.config.getoption('--run-within-unshare'):
        try:
            args = "ip link set lo up".split()
            subprocess.run(args, check=True)
        except FileNotFoundError:
            args = "/sbin/ifconfig lo up".split()
            subprocess.run(args, check=True)
        # we use a fixed ip and port, because the network namespace is not shared
        ip = '127.0.0.1'
        port = 12345
    else:
        ip = f"127.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
        port = random.randint(10000, 65535)
    yield ServerAddress(ip, port)


@pytest.fixture(scope="session")
def rest_api_mock_server(request, server_address):
    server_process = subprocess.Popen([sys.executable,
                                       os.path.join(os.path.dirname(__file__), "rest_api_mock.py"),
                                       server_address.ip,
                                       str(server_address.port)])
    # wait 5 seconds for the expected requests
    timeout = 5
    interval = 0.1
    for _ in range(int(timeout / interval)):
        returncode = server_process.poll()
        if returncode is not None:
            # process terminated
            raise subprocess.CalledProcessError(returncode, server_process.args)
        try:
            get_expected_requests(server_address)
            break
        except requests.exceptions.ConnectionError:
            time.sleep(interval)
    else:
        server_process.terminate()
        server_process.wait()
        raise subprocess.TimeoutExpired(server_process.args, timeout)

    try:
        yield server_address
    finally:
        server_process.terminate()
        server_process.wait()


def _path_from_top_srcdir(*p):
    top_srcdir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    return os.path.join(top_srcdir, *p)


@pytest.fixture(scope="session")
def jmx(request, rest_api_mock_server):
    if request.config.getoption("nodetool") == "scylla":
        yield
        return

    jmx_path = request.config.getoption("jmx_path")
    if jmx_path is None:
        jmx_path = _path_from_top_srcdir("tools", "jmx", "scripts", "scylla-jmx")
    else:
        jmx_path = os.path.abspath(jmx_path)

    workdir = os.path.join(os.path.dirname(jmx_path), "..")
    ip, api_port = rest_api_mock_server
    expected_requests = [
            expected_request(
                "GET",
                "/column_family/",
                response=[{"ks": "system_schema",
                           "cf": "columns",
                           "type": "ColumnFamilies"},
                          {"ks": "system_schema",
                           "cf": "computed_columns",
                           "type": "ColumnFamilies"}]),
            expected_request(
                "GET",
                "/stream_manager/",
                response=[])]
    set_expected_requests(rest_api_mock_server, expected_requests)

    # Our nodetool launcher script ignores the host param, so this has to be 127.0.0.1, matching the internal default.
    jmx_ip = "127.0.0.1"
    jmx_port = random.randint(10000, 65535)
    while jmx_port == api_port:
        jmx_port = random.randint(10000, 65535)

    jmx_process = subprocess.Popen(
            [
                jmx_path,
                "-a", ip,
                "-p", str(api_port),
                "-ja", jmx_ip,
                "-jp", str(jmx_port),
            ],
            cwd=workdir, text=True)

    # Wait until jmx starts up
    # We rely on the expected requests being consumed for this
    i = 0
    while len(get_expected_requests(rest_api_mock_server)) > 0:
        if i == 50:  # 5 seconds
            raise RuntimeError("timed out waiting for JMX to start")
        time.sleep(0.1)
        i += 1

    yield jmx_ip, jmx_port

    jmx_process.terminate()
    jmx_process.wait()


all_modes = {'debug': 'Debug',
             'release': 'RelWithDebInfo',
             'dev': 'Dev',
             'sanitize': 'Sanitize',
             'coverage': 'Coverage'}


def _path_to_scylla(mode):
    build_dir = _path_from_top_srcdir("build")
    if os.path.exists(os.path.join(build_dir, 'build.ninja')):
        return os.path.join(build_dir, all_modes[mode], "scylla")
    return os.path.join(build_dir, mode, "scylla")


@pytest.fixture(scope="session")
def nodetool_path(request):
    if request.config.getoption("nodetool") == "scylla":
        mode = request.config.getoption("mode")
        return _path_to_scylla(mode)

    path = request.config.getoption("nodetool_path")
    if path is not None:
        return os.path.abspath(path)

    return _path_from_top_srcdir("java", "bin", "nodetool")


@pytest.fixture(scope="function")
def scylla_only(request):
    if request.config.getoption("nodetool") != "scylla":
        pytest.skip('Scylla-only test skipped')


@pytest.fixture(scope="function")
def cassandra_only(request):
    if request.config.getoption("nodetool") != "cassandra":
        pytest.skip('Cassandra-only test skipped')

def split_list(l, delim):
    before = []
    after = []
    for elem in l:
        (after if after or elem == delim else before).append(elem)
    return (before, after)

@pytest.fixture(scope="module")
def nodetool(request, jmx, nodetool_path, rest_api_mock_server):
    def invoker(method, *args, expected_requests=None, check_return_code=True):
        with expected_requests_manager(rest_api_mock_server, expected_requests or []):
            before, after = split_list(list(args), "--")
            if request.config.getoption("nodetool") == "scylla":
                api_ip, api_port = rest_api_mock_server
                cmd = [nodetool_path, "nodetool", method] + before + ["--logger-log-level",
                       "scylla-nodetool=trace",
                       "-h", api_ip,
                       "-p", str(api_port)] + after
            else:
                jmx_ip, jmx_port = jmx
                cmd = [nodetool_path, "-h", jmx_ip, "-p", str(jmx_port), method]
                cmd += list(args)
            suppressions_path = _path_from_top_srcdir("ubsan-suppressions.supp")
            env = {'UBSAN_OPTIONS': f'halt_on_error=1:abort_on_error=1:suppressions={suppressions_path}',
                   'ASAN_OPTIONS': f'disable_coredump=0:abort_on_error=1:detect_stack_use_after_return=1'}
            res = subprocess.run(cmd, capture_output=True, text=True, env=env)
            sys.stdout.write(res.stdout)
            sys.stderr.write(res.stderr)

            expected_requests = [r for r in get_expected_requests(rest_api_mock_server)
                                 if not r.exhausted()]

            unexpected_requests = get_unexpected_requests(rest_api_mock_server)

            # Check the return-code first, if the command failed probably not all requests were consumed
            if check_return_code:
                res.check_returncode()
            assert len(expected_requests) == 0, ''.join(str(r) for r in expected_requests)
            assert unexpected_requests == 0

            return res

    return invoker


def pytest_configure(config):
    config.pluginmanager.register(ReportPlugin())
