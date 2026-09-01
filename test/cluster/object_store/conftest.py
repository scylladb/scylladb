#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import pytest

from test.pylib.connect_options import add_s3_options
from test.pylib.object_storage import Storage, StorageFactory


def pytest_addoption(parser):
    add_s3_options(parser)


@pytest.fixture(params=["s3", "gs"])
async def object_storage(request: pytest.FixtureRequest, object_storage_factory: StorageFactory) -> Storage:
    return await object_storage_factory(request.param)


@pytest.fixture
async def s3_storage(object_storage_factory: StorageFactory) -> Storage:
    return await object_storage_factory("s3")


@pytest.fixture
async def gs_storage(object_storage_factory: StorageFactory) -> Storage:
    return await object_storage_factory("gs")
