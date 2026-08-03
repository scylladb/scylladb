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
async def make_object_storage(kind, pytestconfig, tmpdir, test_name, manager: ManagerClient):
    """Yield a started object-storage server with a fresh per-test bucket.

    The bucket is destroyed and the server stopped from on_teardown callbacks,
    that is after the harness has stopped the cluster, not on exit from this
    context manager.
    """
    if kind == 'gs':
        server = create_gs_server(tmpdir)
    else:
        server = create_s3_server(pytestconfig, tmpdir)

    await server.start()
    try:
        server.create_test_bucket(test_name)
    except BaseException:
        # Nothing is registered yet, free the server eagerly.
        await server.stop()
        raise

    # Emptying the bucket while a node is still running can abort an in-flight
    # compaction, tablet migration or streaming operation (SCYLLADB-2471), so
    # defer the cleanup.  Callbacks run in LIFO order, hence the bucket is
    # destroyed while the storage server is still up to serve the deletes.
    manager.on_teardown(server.stop)
    manager.on_teardown(server.destroy_test_bucket)

    yield server


@pytest.fixture(scope="function", params=['s3', 'gs'])
async def object_storage(request, pytestconfig, tmpdir, manager: ManagerClient):
    async with make_object_storage(request.param, pytestconfig, tmpdir, request.node.name, manager) as server:
        yield server


@pytest.fixture(scope="function")
async def s3_storage(request, pytestconfig, tmpdir, manager: ManagerClient):
    async with make_object_storage('s3', pytestconfig, tmpdir, request.node.name, manager) as server:
        yield server
