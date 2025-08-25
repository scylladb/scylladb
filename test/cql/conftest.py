#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import pytest

from test.cqlpy.conftest import host, cql, this_dc  # add required fixtures
from test.pylib.cql_repl import CQL_TEST_SUFFIX, CqlFile
from test.pylib.runner import testpy_test_fixture_scope, add_host_option, add_cql_connection_options
from test.pylib.suite.base import get_testpy_test

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from _pytest.nodes import Collector
    from cassandra.cluster import Session


def pytest_addoption(parser: pytest.Parser) -> None:
    """Set up command line parameters.

    By default, tests run against a CQL server (ScyllaDB or Cassandra) listening on `localhost:9042`.
    Add the `--host` and `--port` options to allow overriding these defaults.
    """
    add_host_option(parser)
    add_cql_connection_options(parser)


def pytest_collect_file(file_path: Path, parent: Collector) -> Collector | None:
    if file_path.name.endswith(CQL_TEST_SUFFIX):
        return CqlFile.from_parent(parent=parent, path=file_path)
    return None


# Test files are not Python files, so, we need to define all fixtures with `autouse=True` to have
# access to them in `CqlFile` instance via the `self.funcargs` attribute.
#
# Fixture `cql` will be used automatically too as a dependency.


@pytest.fixture(autouse=True)
def cql_test_connection(cql: Session) -> Generator[None]:
    """Check if a DB node is still up after test finished."""

    yield

    try:
        # We want to run a do-nothing CQL command.
        # "use system" is the closest to do-nothing I could find...
        cql.execute("use system")
    except Exception as exc:
        pytest.fail(f"Scylla appears to have crashed: {exc}")


@pytest.fixture(scope=testpy_test_fixture_scope, autouse=True)
def keyspace(cql: Session, this_dc: str) -> Generator[str]:
    """Create a random keyspace for this pytest session.

    The keyspace is created with RF=1, and automatically deleted at the end.
    """
    keyspace_name = f"test_{uuid.uuid4().hex}"
    cql.execute(f"CREATE KEYSPACE {keyspace_name} "
                f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }}")
    yield keyspace_name
    cql.execute(f"DROP KEYSPACE {keyspace_name}")


@pytest.fixture(scope="module", autouse=True)
async def output_path(request: pytest.FixtureRequest, build_mode: str) -> Path:
    """A file to collect real output of test's CQL queries to compare with .result file."""

    testpy_test = await get_testpy_test(path=request.path, options=request.config.option, mode=build_mode)
    return testpy_test.suite.log_dir / f"{testpy_test.uname}.reject"
