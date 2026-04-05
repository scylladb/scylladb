#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#


from contextlib import asynccontextmanager

import pytest

from test.pylib.manager_client import ManagerClient
from test.pylib.suite.python import add_s3_options
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
async def make_object_storage(kind, pytestconfig, tmpdir, test_name, manager: ManagerClient | None = None):
    if kind == 'gs':
        server = create_gs_server(tmpdir)
    else:
        server = create_s3_server(pytestconfig, tmpdir)

    bucket_created = False
    try:
        await server.start()
        server.create_test_bucket(test_name)
        bucket_created = True
        yield server
    finally:
        # Stop all running Scylla servers before destroying the bucket.
        # Without this, in-flight operations (compaction, tablet migration) may
        # still reference objects in the bucket, causing S3 404s that abort the
        # node.  See SCYLLADB-2471.
        if manager is not None:
            try:
                for srv in await manager.running_servers():
                    await manager.server_stop(srv.server_id, convict=False)
            except Exception:
                pass  # Best effort — servers may already be stopped
        if bucket_created:
            server.destroy_test_bucket()
        await server.stop()


@pytest.fixture(scope="function", params=['s3', 'gs'])
async def object_storage(request, pytestconfig, tmpdir, manager: ManagerClient):
    """Object storage fixture. Depends on manager to stop servers before
    bucket teardown (see SCYLLADB-2471)."""
    async with make_object_storage(request.param, pytestconfig, tmpdir, request.node.name, manager) as server:
        yield server


@pytest.fixture(scope="function")
async def s3_storage(request, pytestconfig, tmpdir, manager: ManagerClient):
    """S3 storage fixture. Depends on manager to stop servers before
    bucket teardown (see SCYLLADB-2471)."""
    async with make_object_storage('s3', pytestconfig, tmpdir, request.node.name, manager) as server:
        yield server
