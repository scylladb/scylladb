#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#


from contextlib import asynccontextmanager

import pytest

from test.pylib.connect_options import add_s3_options
from test.pylib.manager_client import ManagerClient
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
async def make_object_storage(kind, pytestconfig, tmpdir, log_dir, test_name, manager: ManagerClient):
    """Start an object-storage backend for a test.

    `tmpdir` holds the server's scratch data (per-test, discarded by CI), while
    `log_dir` must be a CI-archived directory (testlog) so that container logs
    survive for post-mortem analysis.

    The bucket is destroyed and the server stopped from teardown callbacks,
    that is after the harness has stopped the cluster, not on exit from this
    context manager.
    """
    if kind == 'gs':
        server = create_gs_server(log_dir)
    else:
        server = create_s3_server(pytestconfig, tmpdir, log_dir)

    await server.start()
    # Registered first so that it fires last, since the deletes below need the
    # server.  Nothing else frees the server until it is registered, hence the
    # eager stop.
    try:
        await manager.add_teardown_callback(server.stop, 'stop object storage server')
    except BaseException:
        await server.stop()
        raise

    server.create_test_bucket(test_name)
    # Emptying the bucket while a node is still running can abort an in-flight
    # compaction, tablet migration or streaming operation (SCYLLADB-2471), so
    # destroy it from a callback instead: it fires once the cluster is down.
    try:
        await manager.add_teardown_callback(server.destroy_test_bucket, 'destroy test bucket')
    except BaseException:
        # A failed registration does not say whether the manager got as far as
        # recording the callback -- _call() shields the operation, so a
        # timed-out or cancelled caller leaves it running.  Destroy the bucket
        # here regardless: no node has been told about it yet, so this races
        # with nothing, and destroy_test_bucket() is a no-op the second time if
        # the callback did register and fires later.
        server.destroy_test_bucket()
        raise

    yield server


@pytest.fixture(scope="function", params=['s3', 'gs'])
async def object_storage(request, pytestconfig, tmpdir, suite_log_dir, manager: ManagerClient):
    async with make_object_storage(request.param, pytestconfig, tmpdir, suite_log_dir, request.node.name, manager) as server:
        yield server


@pytest.fixture(scope="function")
async def s3_storage(request, pytestconfig, tmpdir, suite_log_dir, manager: ManagerClient):
    async with make_object_storage('s3', pytestconfig, tmpdir, suite_log_dir, request.node.name, manager) as server:
        yield server


@pytest.fixture(scope="function")
async def gs_storage(request, pytestconfig, tmpdir, suite_log_dir, manager: ManagerClient):
    async with make_object_storage('gs', pytestconfig, tmpdir, suite_log_dir, request.node.name, manager) as server:
        yield server
