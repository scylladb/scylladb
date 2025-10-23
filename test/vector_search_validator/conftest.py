#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from os import path
from pathlib import Path
import pytest
from test import path_to
from test import BUILD_DIR
from test import TEST_DIR

def pytest_addoption(parser):
    parser.addoption(
        "--filters",
        action="store",
        default="",
        help="vector-search-validator test filter",
    )

@pytest.fixture
def logdir_path(request, build_mode):
    suffix = 1
    while True:
        try:
            logdir = Path(request.config.getoption("--tmpdir")).joinpath(build_mode).joinpath(f"vector-search-validator-{suffix}").absolute()
            logdir.mkdir(parents=True, exist_ok=False)
            break
        except FileExistsError:
            suffix += 1
    return logdir

@pytest.fixture(scope="package")
def scylla_path(build_mode):
    return path_to(build_mode, "scylla")

@pytest.fixture(scope="package")
def validator_path():
    return path.join(BUILD_DIR, f"vector-search-validator/bin/vector-search-validator")

@pytest.fixture(scope="package")
def vector_store_path(build_mode):
    return path.join(BUILD_DIR, f"vector-search-validator/bin/vector-store")

@pytest.fixture
def filters(request):
    return request.config.getoption("--filters").replace(",", " ")

@pytest.fixture(scope="package")
def dir_path():
    return f"{TEST_DIR}/vector_search_validator"
