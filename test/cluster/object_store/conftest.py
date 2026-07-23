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
async def make_object_storage(kind, pytestconfig, tmpdir, test_name):
    """Yield a started object-storage server with a fresh per-test bucket.

    The context manager DOES NOT stop the server or destroy the bucket on
    exit.  The caller is responsible for scheduling those steps via
    ``manager.on_teardown()`` in the correct order relative to Scylla
    cluster shutdown (see ``register_object_storage_teardown`` below).
    """
    if kind == 'gs':
        server = create_gs_server(tmpdir)
    else:
        server = create_s3_server(pytestconfig, tmpdir)

    await server.start()
    try:
        server.create_test_bucket(test_name)
    except BaseException:
        # Bucket creation failed before the caller could register the
        # deferred cleanup; free the server eagerly.
        await server.stop()
        raise

    yield server


def register_object_storage_teardown(manager: ManagerClient, server) -> None:
    """Register on_teardown callbacks so that the bucket is destroyed only
    after the Scylla cluster has been drained.

    Required execution order (SCYLLADB-2471):
        1. Stop every running Scylla server -- so no in-flight compaction,
           tablet migration or streaming can read from the bucket while it
           is being emptied.
        2. Destroy the test bucket -- the storage server is still live at
           this point so the boto3 delete calls succeed.
        3. Stop the storage server.

    on_teardown fires callbacks in LIFO order, so callbacks are registered
    in reverse of the execution order above.
    """
    async def stop_scylla_servers():
        for srv in await manager.running_servers():
            await manager.server_stop(srv.server_id, convict=False)

    manager.on_teardown(server.stop)                    # step 3
    manager.on_teardown(server.destroy_test_bucket)     # step 2
    manager.on_teardown(stop_scylla_servers)            # step 1


@pytest.fixture(scope="function", params=['s3', 'gs'])
async def object_storage(request, pytestconfig, tmpdir, manager: ManagerClient):
    async with make_object_storage(request.param, pytestconfig, tmpdir, request.node.name) as server:
        register_object_storage_teardown(manager, server)
        yield server


@pytest.fixture(scope="function")
async def s3_storage(request, pytestconfig, tmpdir, manager: ManagerClient):
    async with make_object_storage('s3', pytestconfig, tmpdir, request.node.name) as server:
        register_object_storage_teardown(manager, server)
        yield server
