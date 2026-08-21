#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#


from contextlib import asynccontextmanager

import pytest

from test.pylib.connect_options import add_s3_options
from test.pylib.object_storage import (
    format_tuples,
    keyspace_options,
    create_s3_server,
    create_gs_server,
    GSFront,
    GSServer,
    S3Server,
    S3_Server,
    MinioWrapper,
    s3_server,
)


def pytest_addoption(parser):
    add_s3_options(parser)


@asynccontextmanager
async def make_object_storage(kind, pytestconfig, tmpdir, log_dir, test_name):
    """Start an object-storage backend for a test.

    `tmpdir` holds the server's scratch data (per-test, discarded by CI), while
    `log_dir` must be a CI-archived directory (testlog) so that container logs
    survive for post-mortem analysis.
    """
    if kind == 'gs':
        server = create_gs_server(log_dir)
    else:
        server = create_s3_server(pytestconfig, tmpdir, log_dir)

    bucket_created = False
    try:
        await server.start()
        server.create_test_bucket(test_name)
        bucket_created = True
        yield server
    finally:
        if bucket_created:
            server.destroy_test_bucket()
        await server.stop()


@pytest.fixture(scope="function", params=['s3', 'gs'])
async def object_storage(request, pytestconfig, tmpdir, suite_log_dir):
    async with make_object_storage(request.param, pytestconfig, tmpdir, suite_log_dir, request.node.name) as server:
        yield server


@pytest.fixture(scope="function")
async def s3_storage(request, pytestconfig, tmpdir, suite_log_dir):
    async with make_object_storage('s3', pytestconfig, tmpdir, suite_log_dir, request.node.name) as server:
        yield server


@pytest.fixture(scope="function")
async def gs_storage(request, pytestconfig, tmpdir, suite_log_dir):
    async with make_object_storage('gs', pytestconfig, tmpdir, suite_log_dir, request.node.name) as server:
        yield server
