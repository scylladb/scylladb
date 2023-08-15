#
# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import os
import pytest
import shutil
import tempfile


def pytest_addoption(parser):
    parser.addoption('--mode', action='store', default='dev',
        help='Scylla build mode to use')


@pytest.fixture(scope="module")
def scylla_mode(request):
    return request.config.getoption("mode")


@pytest.fixture(scope="module")
def dummy_scylla_repo_path():
    # Real Scylla takes a long time to start up, use a dummy instead to cut down
    # on test runtime.
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "dummy-scylla-repo"))


@pytest.fixture(scope="module")
def real_scylla_repo_path():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))


@pytest.fixture(scope="module")
def root_dir():
    """Create a common temporary workdir for all tests in this module"""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


@pytest.fixture
def cluster_dir(root_dir):
    test_dir = os.path.join(root_dir, "test_cluster")
    os.mkdir(test_dir)
    yield test_dir
    shutil.rmtree(test_dir)
