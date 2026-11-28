# Copyright 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1

"""Fixtures shared by the cqlsh-rs suites.

Each suite under this directory wraps one cqlsh-rs test category and only has
to declare which category it is (via the CATEGORY attribute of its test
module) plus whatever connection details differ.  Everything else lives here.
"""

import pytest

from test.pylib.connect_options import add_host_option, add_cql_connection_options
from test.pylib.cqlsh_rs_helpers import cargo_available, category_modules
from test.pylib.scylla_cluster import ScyllaCluster
from test.pylib.skip_types import skip_env
from test.conftest import dynamic_scope


# Tracked upstream; until it is fixed the auth category has no tests to run.
AUTH_TESTS_ISSUE = "https://github.com/scylladb/cqlsh-rs/issues/190"


def pytest_addoption(parser):
    add_host_option(parser)
    add_cql_connection_options(parser)
    parser.addoption("--ssl-port", default="9142",
                     help="encrypted CQL port to connect to (native_transport_port_ssl)")


def pytest_collection_modifyitems(config, items):
    """Drop the items of categories that have no tests upstream.

    Deselecting rather than skipping matters for more than tidiness: a
    deselected item never sets up its fixtures, so the module-scoped
    scylla_cluster fixture is never requested and no Scylla is started for a
    suite that has nothing to run.

    This hook sees every collected item in the session, including ones from
    other suites that are not plain Python functions and have no module, hence
    the defensive getattr.
    """
    kept, deselected, categories = [], [], set()
    for item in items:
        module = getattr(item, "module", None)
        category = getattr(module, "CATEGORY", None)
        if category is not None and not category_modules(category):
            deselected.append(item)
            categories.add(category)
        else:
            kept.append(item)

    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = kept
        # Warn from every process that collects.  Under xdist that is each
        # worker rather than the controller, and pytest folds the copies into a
        # single line in the warnings summary -- which is what keeps an empty
        # category visible instead of quietly reporting success.
        config.issue_config_time_warning(
            pytest.PytestCollectionWarning(
                f"cqlsh-rs has no tests for {', '.join(sorted(categories))} yet, "
                f"skipping those suites entirely (see {AUTH_TESTS_ISSUE})"
            ),
            stacklevel=1,
        )


@pytest.fixture(scope="session")
def require_cargo():
    """cargo is what builds the tests, so without it there is nothing to run.

    This is the one genuine environment skip here: a missing toolchain is not
    a cqlsh-rs defect, whereas a cqlsh-rs that fails to build is, and that
    fails the suite instead.
    """
    if not cargo_available():
        skip_env("cargo not found in PATH, cannot build the cqlsh-rs integration tests")


@pytest.fixture(scope=dynamic_scope())
async def host(request, scylla_cluster: ScyllaCluster | None) -> str:
    """Address of the Scylla to test against.

    No readiness polling: the test framework only hands over a cluster once
    the server has reported ServerUpState.SERVING, so CQL is already up.
    """
    if scylla_cluster is None:
        return request.config.getoption("--host")
    return scylla_cluster.endpoint()


@pytest.fixture(scope=dynamic_scope())
def port(request) -> int:
    return int(request.config.getoption("--port"))
