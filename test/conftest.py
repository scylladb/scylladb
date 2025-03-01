#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from __future__ import annotations

import asyncio
import glob
import logging
import os
import pathlib
import shutil
import sys
import time
from typing import TYPE_CHECKING

import pytest
import universalasync

from test.pylib.host_registry import HostRegistry
from test.pylib.minio_server import MinioServer
from test.pylib.report_plugin import ReportPlugin
from test.pylib.s3_proxy import S3ProxyServer
from test.pylib.s3_server_mock import MockS3Server
from test.pylib.suite.base import TestSuite, init_testsuite_globals
from test.pylib.util import LogPrefixAdapter, get_configured_modes

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop


TEST_RUNNER = os.environ.get("SCYLLA_TEST_RUNNER", "pytest")

ALL_MODES = {'debug': 'Debug',
             'release': 'RelWithDebInfo',
             'dev': 'Dev',
             'sanitize': 'Sanitize',
             'coverage': 'Coverage'}

def pytest_addoption(parser):
    parser.addoption('--mode', choices=ALL_MODES.keys(), action="append", dest="modes",
                     help="Run only tests for given build mode(s)")
    parser.addoption('--tmpdir', action='store', default='testlog', help='''Path to temporary test data and log files. The data is
            further segregated per build mode. Default: ./testlog.''', )
    parser.addoption('--run_id', action='store', default=None, help='Run id for the test run')

    # Following option is to use with bare pytest command.
    #
    # For compatibility with reasons need to run bare pytest with  --test-py-init option
    # to run a test.py-compatible pytest session.
    #
    # TODO: remove this when we'll completely switch to bare pytest runner.
    parser.addoption('--test-py-init', action='store_true', default=False,
                     help='Run pytest session in test.py-compatible mode.  I.e., start all required services, etc.')


@pytest.fixture(scope="session")
def build_mode(request):
    """
    This fixture returns current build mode.
    This is for running tests through the test.py script, where only one mode is passed to the test
    """
    # to avoid issues when there's no provided mode parameter, do it in two steps: get the parameter and if it's not
    # None, get the first value from the list
    mode = request.config.getoption("modes")
    if mode:
        return mode[0]
    return mode

def pytest_configure(config):
    config.pluginmanager.register(ReportPlugin())

def pytest_collection_modifyitems(config, items):
    """
    This is a standard pytest method.
    This is needed to modify the test names with dev mode and run id to differ them one from another
    """
    run_id = config.getoption('run_id', None)

    for item in items:
        # check if this is custom cpp tests that have additional attributes for name modification
        if hasattr(item, 'mode'):
            # modify name with mode that is always present in cpp tests
            item.nodeid = f'{item.nodeid}.{item.mode}'
            item.name = f'{item.name}.{item.mode}'
            if item.run_id:
                item.nodeid = f'{item.nodeid}.{item.run_id}'
                item.name = f'{item.name}.{item.run_id}'
        else:
            # here go python tests that are executed through test.py
            # since test.py is responsible for creating several tests with the required mode,
            # a list with modes contains only one value,
            # that's why in name modification the first element is used
            modes = config.getoption('modes')
            if modes:
                item._nodeid = f'{item._nodeid}.{modes[0]}'
                item.name = f'{item.name}.{modes[0]}'
            if run_id:
                item._nodeid = f'{item._nodeid}.{run_id}'
                item.name = f'{item.name}.{run_id}'


def prepare_dir(dirname: str, pattern: str) -> None:
    # Ensure the dir exists
    pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
    # Remove old artifacts
    for p in glob.glob(os.path.join(dirname, pattern), recursive=True):
        pathlib.Path(p).unlink()


def prepare_dirs(tempdir_base: str, modes: list[str]) -> None:
    prepare_dir(tempdir_base, "*.log")
    for mode in modes:
        prepare_dir(os.path.join(tempdir_base, mode), "*.log")
        prepare_dir(os.path.join(tempdir_base, mode), "*.reject")
        prepare_dir(os.path.join(tempdir_base, mode, "xml"), "*.xml")
        shutil.rmtree(os.path.join(tempdir_base, mode, "failed_test"), ignore_errors=True)
        prepare_dir(os.path.join(tempdir_base, mode, "failed_test"), "*")
        prepare_dir(os.path.join(tempdir_base, mode, "allure"), "*.xml")
        if TEST_RUNNER == "pytest":
            shutil.rmtree(os.path.join(tempdir_base, mode, "pytest"), ignore_errors=True)
            prepare_dir(os.path.join(tempdir_base, mode, "pytest"), "*")


@universalasync.async_to_sync_wraps
async def start_s3_mock_services(minio_tempdir_base: str) -> None:
    ms = MinioServer(
        tempdir_base=minio_tempdir_base,
        address="127.0.0.1",
        logger=LogPrefixAdapter(logger=logging.getLogger("minio"), extra={"prefix": "minio"}),
    )
    await ms.start()
    TestSuite.artifacts.add_exit_artifact(None, ms.stop)

    hosts = HostRegistry()
    TestSuite.artifacts.add_exit_artifact(None, hosts.cleanup)

    mock_s3_server = MockS3Server(
        host=await hosts.lease_host(),
        port=2012,
        logger=LogPrefixAdapter(logger=logging.getLogger("s3_mock"), extra={"prefix": "s3_mock"}),
    )
    await mock_s3_server.start()
    TestSuite.artifacts.add_exit_artifact(None, mock_s3_server.stop)

    minio_uri = f"http://{os.environ[ms.ENV_ADDRESS]}:{os.environ[ms.ENV_PORT]}"
    proxy_s3_server = S3ProxyServer(
        host=await hosts.lease_host(),
        port=9002,
        minio_uri=minio_uri,
        max_retries=3,
        seed=int(time.time()),
        logger=LogPrefixAdapter(logger=logging.getLogger("s3_proxy"), extra={"prefix": "s3_proxy"}),
    )
    await proxy_s3_server.start()
    TestSuite.artifacts.add_exit_artifact(None, proxy_s3_server.stop)


def pytest_sessionstart(session: pytest.Session) -> None:
    # test.py starts S3 mock and create/cleanup testlog by itself. Also, if we run with --collect-only option,
    # we don't need this stuff.
    if TEST_RUNNER != "pytest" or session.config.getoption("--collect-only"):
        return

    if not session.config.getoption("--test-py-init"):
        return

    init_testsuite_globals()
    TestSuite.artifacts.add_exit_artifact(None, TestSuite.hosts.cleanup)

    # Run stuff just once for the pytest session even running under xdist.
    if "xdist" not in sys.modules or not sys.modules["xdist"].is_xdist_worker(request_or_session=session):
        temp_dir = os.path.join(session.config.rootpath, "..", session.config.getoption("--tmpdir"))
        prepare_dirs(tempdir_base=temp_dir, modes=session.config.getoption("--mode") or get_configured_modes())
        start_s3_mock_services(minio_tempdir_base=temp_dir)


def pytest_sessionfinish() -> None:
    if getattr(TestSuite, "artifacts", None) is not None:
        asyncio.get_event_loop().run_until_complete(TestSuite.artifacts.cleanup_before_exit())
