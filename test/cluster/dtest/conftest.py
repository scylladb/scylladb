#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import argparse
import contextlib
import importlib
import logging
import os
import pkgutil
import sys
from typing import TYPE_CHECKING

import pytest

import test.cluster.dtest.ccmlib
from test.cluster.dtest.dtest_config import DTestConfig
from test.cluster.dtest.dtest_setup import DTestSetup
from test.cluster.dtest.dtest_setup_overrides import DTestSetupOverrides
from test.cluster.dtest.tools.marks import check_issue_closed, enable_with_features
from test.pylib.driver_utils import safe_driver_shutdown
from test.pylib.runner import TEST_SUITE, get_params_stash
from test.pylib.scylla_cluster import ScyllaCluster as PylibScyllaCluster
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.skip_types import skip_env

if TYPE_CHECKING:
    import pathlib
    from collections.abc import AsyncGenerator, Callable, Generator

    from pytest import Config, Parser, FixtureRequest




logger = logging.getLogger(__name__)
def _alias_ccmlib() -> None:
    """Make `ccmlib.X` the very same module as `test.cluster.dtest.ccmlib.X`.

    The test modules import the shim as scylla-dtest did (`from
    ccmlib.scylla_cluster import ScyllaCluster`), which pytest resolves through
    the test/cluster/dtest entry it puts on sys.path, while the harness builds
    the cluster and nodes from `test.cluster.dtest.ccmlib`.  Left alone, Python
    loads each shim file twice, as two unrelated modules, so every
    `isinstance(self.cluster, ScyllaCluster)` in the tests is False and they
    take their Cassandra branches (e.g. repair_test.py's check_repair_logs()).
    """
    package = test.cluster.dtest.ccmlib
    sys.modules["ccmlib"] = package
    for info in pkgutil.walk_packages(package.__path__, prefix=f"{package.__name__}."):
        sys.modules[f"ccmlib.{info.name.removeprefix(f'{package.__name__}.')}"] = importlib.import_module(info.name)


_alias_ccmlib()


def pytest_addoption(parser: Parser) -> None:
    parser.addoption("--use-vnodes", action="store_true", default=True, help="Determines wither or not to setup clusters using vnodes for tests")
    parser.addoption("--num-tokens", action="store", default=256, help="Number of tokens to set num_tokens yaml setting to when creating instances with vnodes enabled")
    parser.addoption("--experimental-features", type=lambda s: s.split(","), action="store", help="Pass experimental features <feature>,<feature> to enable", default=None)
    parser.addoption("--tablets", action=argparse.BooleanOptionalAction, default=False, help="Whether to enable tablets support (default: %(default)s)")
    parser.addoption("--force-gossip-topology-changes", action="store_true", default=False, help="force gossip topology changes in a fresh cluster")
    parser.addoption("--scylla-manager-package", action="store", default=None,
                     help="Scylla Manager relocatable to test against: a URL, a local .tar.gz, or a directory holding the unpacked "
                          "binaries. Defaults to the newest master build on downloads.scylladb.com.")


def pytest_configure(config: Config) -> None:
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


def pytest_collection_modifyitems(config: Config, items: list[pytest.Item]) -> None:
    """Honour @pytest.mark.required_features and skip_if, as scylla-dtest's conftest did.

    scylla-dtest deselected a test whose required features were not enabled for
    the run.  Here the test is skipped instead of deselected, so it still shows
    up in the collected total and says why it did not run.  Without this the
    markers are inert and, for instance, the tablets-only tests run against a
    cluster with tablets off and fail on the server's own error.
    """

    features = config.scylla_features
    for item in items:
        marker = item.get_closest_marker("required_features")
        if marker and not enable_with_features(marker.args, features):
            item.add_marker(pytest.mark.skip_env(
                reason=f"requires scylla features {list(marker.args)}, "
                       f"but this run enables {sorted(features)}"))
        # scylla-dtest deselected a test whose @pytest.mark.skip_if condition
        # (a tools.marks predicate, e.g. ~with_feature("tablets")) held.
        if (marker := item.get_closest_marker("skip_if")) and marker.args:
            condition = marker.args[0]
            condition.apply(enabled_features=features)
            if condition:
                item.add_marker(pytest.mark.skip_env(
                    reason=marker.kwargs.get("reason")
                           or f"skip_if condition holds for scylla features {sorted(features)}"))


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
                        manager: ScyllaClusterManager,
                        build_mode: str) -> Generator[DTestSetup]:
    # Tests marked scylla_manager drive a real Scylla Manager server through
    # sctool, with a manager agent beside every node. The manager is not built
    # from this tree, so its binaries are downloaded (once, then cached) and
    # only these tests pay for that.
    manager_install_dir = None
    if request.node.get_closest_marker("scylla_manager"):
        if request.config.getoption("skip_internet_dependent_tests") and not dtest_config.manager_package:
            skip_env(reason="Scylla Manager is not built from this tree and skip_internet_dependent_tests is set; "
                            "pass --scylla-manager-package to point at a local relocatable")
        manager_install_dir = DTestSetup.prepare_scylla_manager(dtest_config.manager_package)

    dtest_setup = DTestSetup(
        dtest_config=dtest_config,
        setup_overrides=fixture_dtest_setup_overrides,
        manager=manager,
        scylla_mode=build_mode,
        manager_install_dir=manager_install_dir,
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
        safe_driver_shutdown(con.cluster)
    dtest_setup.connections = []

    try:
        dtest_setup.cluster.stop(gently=True)
    except Exception as e:  # noqa: BLE001
        logger.error("Error stopping cluster: %s", str(e))

    manager.ignore_log_patterns.extend(dtest_setup.ignore_log_patterns)
    manager.ignore_cores_log_patterns.extend(dtest_setup.ignore_cores_log_patterns)

    try:
        if not dtest_setup.allow_log_errors:
            exclude_errors = []
            if marker := request.node.get_closest_marker("exclude_errors"):
                exclude_errors = list(marker.args)
            dtest_setup.check_errors_all_nodes(exclude_errors=exclude_errors)
    finally:
        pass


@pytest.fixture(scope="function")
async def secondary_cluster_manager(request: FixtureRequest,  # noqa: PLR0913
                                    build_mode: str,
                                    suite_log_dir: pathlib.Path,
                                    scylla_binary: str,
                                    testpy_logger: logging.Logger,
                                    testpy_test_name: str) -> AsyncGenerator[ScyllaClusterManager]:
    """A second, independent Scylla cluster for the test, with its own manager.

    The `manager` fixture gives a test one cluster, and every server added to
    it joins that one ring.  A handful of Scylla Manager tests -- restoring
    into a different cluster, one manager server driving two clusters -- need a
    second ring, so build one the same way test.pylib.runner builds the first.
    Server ids and IP leases are handed out globally, so the two clusters do
    not collide.
    """

    suite_config = get_params_stash(node=request.node)[TEST_SUITE]
    options = request.config.option

    cluster = PylibScyllaCluster(
        logger=testpy_logger,
        vardir=suite_log_dir,
        mode=build_mode,
        cmdline_options=suite_config.cfg.get("extra_scylla_cmdline_options", []),
        cmdline_options_override=options.extra_scylla_cmdline_options.split(),
        config_options=suite_config.cfg.get("extra_scylla_config_options", {}),
        append_env={},
        scylla_exe=scylla_binary,
        save_log_on_success=options.save_log_on_success,
    )
    testpy_logger.info("Created secondary Scylla cluster %s for test %s", cluster, testpy_test_name)
    try:
        async with ScyllaClusterManager(
                test_name=f"{testpy_test_name}::secondary",
                cluster=cluster,
                port=int(request.config.getoption("--port")),
                use_ssl=bool(request.config.getoption("--ssl")),
                auth_username=request.config.getoption("--auth_username", default=None),
                auth_password=request.config.getoption("--auth_password", default=None),
        ).run_in_thread() as secondary_manager:
            yield secondary_manager
    finally:
        await cluster.recycle()


class SubTests:
    """The part of pytest-subtests' `subtests` fixture the dtests use.

    scylla-dtest depends on the pytest-subtests plugin, which is not in the
    toolchain, and the toolchain's pytest predates the built-in `subtests`
    fixture of pytest 9 (which, being a plugin fixture, this one overrides).
    Unlike the plugin, the first failing subtest fails the whole test instead
    of being reported on its own; the failure is annotated with the subtest's
    description, so it still says which one it was.
    """

    @contextlib.contextmanager
    def test(self, msg: str | None = None, **kwargs: object) -> Generator[None]:
        try:
            yield
        except Exception as exc:
            params = ", ".join(f"{key}={value!r}" for key, value in kwargs.items())
            exc.add_note(f"in subtest {' '.join(filter(None, (msg and repr(msg), params and f'({params})')))}")
            raise


@pytest.fixture(scope="function")
def subtests() -> SubTests:
    return SubTests()


@pytest.fixture(scope="function")
def is_issue_open() -> Callable[[str], bool]:
    """Report whether a referenced issue is still open.

    Used by tests that work around a known bug and want to stop working around
    it once the bug is fixed.  This port has no GitHub or Jira client and must
    not touch the network, so tools.marks.check_issue_closed is an offline
    stand-in that reports every valid reference as closed -- the same answer
    @pytest.mark.require already gets here.
    """

    def _is_open(ref: str, *, verbose: bool = False) -> bool:  # noqa: ARG001
        return not check_issue_closed(ref, scylla_version=None)

    return _is_open


@pytest.fixture(scope="session", autouse=True)
def install_debugging_signal_handler() -> None:
    import faulthandler

    faulthandler.enable()


@pytest.fixture(scope="session")
def dtest_config(request: FixtureRequest) -> Generator[DTestConfig]:
    dtest_config = DTestConfig()
    dtest_config.setup(request)

    yield dtest_config

# Verbatim copies from the scylla-dtest repository, not adapted yet.  A file
# leaves unported.txt in the commit that ports it.
_unported_list = os.path.join(os.path.dirname(__file__), "unported.txt")
if os.path.exists(_unported_list):
    with open(_unported_list, encoding="utf-8") as _f:
        collect_ignore = _f.read().split()
else:
    collect_ignore = []
