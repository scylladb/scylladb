#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from pathlib import Path
import pytest
from test import path_to, BUILD_DIR, TEST_DIR
from test.pylib.runner import RUN_ID


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--filters",
        action="store",
        default="",
        help="vector-search-validator test filter",
    )


@pytest.fixture
def logdir_path(request: pytest.FixtureRequest, build_mode: str) -> Path:
    return Path(request.config.getoption("--tmpdir")).absolute() / build_mode / "vector-search-validator"

@pytest.fixture(scope="module")
def scylla_path(build_mode: str) -> str:
    return path_to(build_mode, "scylla")

@pytest.fixture
def filters(request: pytest.FixtureRequest) -> str:
    return request.config.getoption("--filters").replace(",", " ")


@pytest.fixture
def run_id(request: pytest.FixtureRequest) -> int:
    return request.node.stash[RUN_ID]
