#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import os
import subprocess
import sys
import time
from typing import NamedTuple

import pytest
import requests.exceptions

from test import asan_options, path_to, ubsan_options
from test.nodetool.rest_api_mock import get_expected_requests, get_unexpected_requests, expected_requests_manager
from test.pylib.host_registry import HostRegistry


def pytest_addoption(parser):
    parser.addoption('--run-within-unshare', action='store_true',
                     help="Setup the 'lo' network if launched with unshare(1)")


class ServerAddress(NamedTuple):
    ip: str
    port: int


@pytest.fixture(scope="module")
async def server_address(request):
    # Each test module gets a unique IP, so a fixed port suffices and
    # avoids any port-collision or TOCTOU concerns. This mirrors the
    # approach used in test/cqlpy/run.py.
    port = 12345
    # unshare(1) -rn drops us in a new network namespace in which the "lo" is
    # not up yet, so let's set it up first.
    if request.config.getoption('--run-within-unshare', default=False):
        try:
            args = "ip link set lo up".split()
            subprocess.run(args, check=True)
        except FileNotFoundError:
            args = "/sbin/ifconfig lo up".split()
            subprocess.run(args, check=True)
        # the network namespace isn't shared, so any IP works
        ip = "127.0.0.1"
    else:
        hosts = HostRegistry()
        ip = await hosts.lease_host()

    yield ServerAddress(ip, port)

    if ip != "127.0.0.1":
        await hosts.release_host(ip)


@pytest.fixture(scope="module")
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
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # The server is up but the endpoint is not ready yet, keep waiting
                time.sleep(interval)
            else:
                raise
    else:
        server_process.terminate()
        server_process.wait()
        raise subprocess.TimeoutExpired(server_process.args, timeout)

    try:
        yield server_address
    finally:
        server_process.terminate()
        server_process.wait()


@pytest.fixture(scope="module")
def nodetool_path(build_mode):
    return path_to(build_mode, "scylla")


def split_list(l, delim):
    before = []
    after = []
    for elem in l:
        (after if after or elem == delim else before).append(elem)
    return (before, after)

@pytest.fixture(scope="module")
def nodetool(nodetool_path, rest_api_mock_server):
    def invoker(method, *args, expected_requests=None, check_return_code=True):
        with expected_requests_manager(rest_api_mock_server, expected_requests or []):
            before, after = split_list(list(args), "--")
            api_ip, api_port = rest_api_mock_server
            cmd = [nodetool_path, "nodetool", method] + before + ["--logger-log-level",
                   "scylla-nodetool=trace",
                   "-h", api_ip,
                   "-p", str(api_port)] + after
            env = {'UBSAN_OPTIONS': ubsan_options(),
                   'ASAN_OPTIONS': asan_options()}
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
