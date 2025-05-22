#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import argparse
import logging
from typing import TYPE_CHECKING

import pytest

from test.cluster.dtest.dtest_config import DTestConfig
from test.cluster.dtest.dtest_setup import DTestSetup
from test.cluster.dtest.dtest_setup_overrides import DTestSetupOverrides

if TYPE_CHECKING:
    from collections.abc import Generator

    from pytest import Config, Parser, FixtureRequest

    from test.pylib.manager_client import ManagerClient


logger = logging.getLogger(__name__)


def pytest_addoption(parser: Parser) -> None:
    parser.addoption("--use-vnodes", action="store_true", default=True, help="Determines wither or not to setup clusters using vnodes for tests")
    parser.addoption("--num-tokens", action="store", default=256, help="Number of tokens to set num_tokens yaml setting to when creating instances with vnodes enabled")
    parser.addoption("--experimental-features", type=lambda s: s.split(","), action="store", help="Pass experimental features <feature>,<feature> to enable")
    parser.addoption("--tablets", action=argparse.BooleanOptionalAction, default=False, help="Whether to enable tablets support (default: %(default)s)")
    parser.addoption("--force-gossip-topology-changes", action="store_true", default=False, help="force gossip topology changes in a fresh cluster")


def pytest_configure(config: Config) -> None:
    logging.getLogger("cassandra").setLevel(logging.INFO)
    logging.getLogger("boto3").setLevel(logging.INFO)
    logging.getLogger("botocore").setLevel(logging.INFO)
    logging.getLogger("s3transfer").setLevel(logging.INFO)

    features = {"cdc", "raft", "consistent-cluster-management", "consistent-topology-changes"}
    if experimental_features := config.getoption("--experimental-features"):
        features.update(experimental_features)
    if config.getoption("--force-gossip-topology-changes") and config.getoption("--tablets"):
        raise Exception("--force-gossip-topology-changes and --tablets cannot be used together")
    if config.getoption("--force-gossip-topology-changes"):
        features.remove("consistent-topology-changes")
    if config.getoption("--tablets"):
        features.add("tablets")
    config.scylla_features = features


@pytest.fixture(scope="function", autouse=True)
def fixture_dtest_setup_overrides(dtest_config: DTestConfig) -> DTestSetupOverrides:
    """
    no-op default implementation of fixture_dtest_setup_overrides.
    we run this when a test class hasn't implemented their own
    fixture_dtest_setup_overrides
    """
    return DTestSetupOverrides()


@pytest.fixture(scope="function", autouse=False)
def fixture_dtest_setup(request: FixtureRequest,
                        dtest_config: DTestConfig,
                        fixture_dtest_setup_overrides: DTestSetupOverrides,
                        manager: ManagerClient,
                        build_mode: str) -> Generator[DTestSetup]:
    dtest_setup = DTestSetup(
        dtest_config=dtest_config,
        setup_overrides=fixture_dtest_setup_overrides,
        manager=manager,
        scylla_mode=build_mode,
    )

    if request.node.get_closest_marker("single_node") or not request.node.get_closest_marker("no_boot_speedups"):
        dtest_setup.cluster_options.setdefault("skip_wait_for_gossip_to_settle", 0)

    # Reduce waiting time for the nodes to hear from others before joining the ring.
    # Since all test cases run on localhost and there are no large test clusters
    # it's safe to reduce the value to save a lot of time while testing.
    # (Default value for the option is 30s)
    dtest_setup.cluster_options.setdefault("ring_delay_ms", 10000)

    cluster_options = request.node.get_closest_marker("cluster_options")
    if cluster_options:
        for name, value in cluster_options.kwargs.items():
            dtest_setup.cluster_options.setdefault(name, value)

    dtest_setup.init_default_config()

    # at this point we're done with our setup operations in this fixture
    # yield to allow the actual test to run
    yield dtest_setup

    # phew! we're back after executing the test, now we need to do
    # all of our teardown and cleanup operations

    dtest_setup.jvm_args = []

    for con in dtest_setup.connections:
        con.cluster.shutdown()
    dtest_setup.connections = []

    try:
        dtest_setup.cluster.stop(gently=True)
    except Exception as e:  # noqa: BLE001
        logger.error("Error stopping cluster: %s", str(e))

    try:
        if not dtest_setup.allow_log_errors:
            exclude_errors = []
            if marker := request.node.get_closest_marker("exclude_errors"):
                exclude_errors = list(marker.args)
            dtest_setup.check_errors_all_nodes(exclude_errors=exclude_errors)
    finally:
        pass


@pytest.fixture(scope="session", autouse=True)
def install_debugging_signal_handler() -> None:
    import faulthandler

    faulthandler.enable()


@pytest.fixture(scope="session")
def dtest_config(request: FixtureRequest) -> Generator[DTestConfig]:
    dtest_config = DTestConfig()
    dtest_config.setup(request)

    yield dtest_config
