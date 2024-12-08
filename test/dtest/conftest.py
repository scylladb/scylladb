#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import argparse
import logging
import re
import warnings
from collections import defaultdict

import pytest
from packaging.version import Version

from test.dtest.dtest_config import DTestConfig
from test.dtest.dtest_setup import DTestSetup
from test.dtest.dtest_setup_overrides import DTestSetupOverrides
from test.dtest.tools.marks import IssueClosed, MarkedLocals, RequirePredicate, enable_with_features, get_version
from test.dtest.tools.misc import colored_text


pytest_plugins = [
    "test.conftest",
    "test.topology.conftest",
]

logger = logging.getLogger(__name__)


def pytest_addoption(parser) -> None:
    parser.addoption("--use-vnodes", action="store_true", default=True, help="Determines wither or not to setup clusters using vnodes for tests")
    parser.addoption("--num-tokens", action="store", default=256, help="Number of tokens to set num_tokens yaml setting to when creating instances " "with vnodes enabled")
    parser.addoption("--experimental-features", type=lambda s: s.split(","), action="store", help="Pass experimental features <feature>,<feature> to enable")
    parser.addoption("--tablets", action=argparse.BooleanOptionalAction, default=False, help="Whether to enable tablets support (default: %(default)s)")
    parser.addoption("--force-gossip-topology-changes", action="store_true", default=False, help="force gossip topology changes in a fresh cluster")


def pytest_configure() -> None:
    logging.getLogger("cassandra").setLevel(logging.INFO)
    logging.getLogger("boto3").setLevel(logging.INFO)
    logging.getLogger("botocore").setLevel(logging.INFO)
    logging.getLogger("s3transfer").setLevel(logging.INFO)


@pytest.fixture(scope="function", autouse=True)
def fixture_dtest_setup_overrides(dtest_config):
    """
    no-op default implementation of fixture_dtest_setup_overrides.
    we run this when a test class hasn't implemented their own
    fixture_dtest_setup_overrides
    """
    return DTestSetupOverrides()


@pytest.fixture(scope="function", autouse=False)
def fixture_dtest_setup(request, dtest_config, fixture_dtest_setup_overrides, manager, build_mode):
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
def install_debugging_signal_handler():
    import faulthandler

    faulthandler.enable()


@pytest.fixture(scope="session")
def dtest_config(request):
    dtest_config = DTestConfig()
    dtest_config.setup(request)

    yield dtest_config


def pytest_collection_modifyitems(items, config):  # noqa: PLR0912, PLR0915
    """
    This function is called upon during the pytest test collection phase and allows for modification
    of the test items within the list
    """
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

    selected_items = []
    deselected_items = []

    matchexpr = config.option.markexpr

    for item in items:
        deselect_test = False
        if re.search(r"""[$!#&"()|<>`\;'\s+"']""", item.nodeid):
            pytest.fail(f"'{item.nodeid}' has illegal characters in it's name, it's gonna break our CI")

        # temporarily deselect tests in cqlsh_copy_tests that depend on cqlshlib,
        # until cqlshlib is Python 3 compatibile
        if item.get_closest_marker("depends_cqlshlib"):
            deselect_test = True

        condition_context = {
            "enabled_features": features,
            "verbose": False,
            "nodeid": item.nodeid,
            "is_enterprise": False,
        }
        require_mark = item.get_closest_marker("require")
        if require_mark:
            if "condition" in require_mark.kwargs:
                condition = require_mark.kwargs["condition"]
                assert isinstance(condition, RequirePredicate)
                condition.apply(**condition_context)
            else:
                condition = IssueClosed(*require_mark.args, **require_mark.kwargs)
                condition.apply(**condition_context)
            if not condition:
                deselect_test = True

        if marker := item.get_closest_marker("skip_if"):
            condition = marker.args[0]
            condition.apply(**condition_context)
            if condition:
                deselect_test = True

        if matchexpr:
            unmarks = set()
            if marker := item.get_closest_marker("unmark_if"):
                condition = marker.kwargs.get("condition")
                if condition is not None:
                    condition.apply(**condition_context)
                    if condition:
                        unmarks.update(marker.args)
                else:
                    warnings.warn(f"Incorrect usage of unmark_if, condition parameter is missing for {item.name}")
            marks = [m.name for m in list(item.iter_markers()) if m.name not in unmarks]
            if not eval(matchexpr, {}, MarkedLocals(marks)):
                text = f"Deselecting {item}: mark removed by @unmark: matchexpr='{matchexpr}' marks={marks} unmarks={unmarks}"
                print(colored_text(text, "yellow"))
                deselect_test = True

            if require_mark and "next_gating" in matchexpr and eval(matchexpr, {}, defaultdict(lambda: False, **item.keywords)):
                text = f"Deselecting {item.nodeid} (cause it's marked both require and next_gating)"
                print(colored_text(text, "yellow"))
                deselect_test = True

        if marker := item.get_closest_marker("required_features"):
            if not enable_with_features(marker.args, features):
                text = f"Deselecting <{item.nodeid}>: the test required_features={marker.args}, but scylla features={features}"
                print(colored_text(text, "yellow"))
                deselect_test = True

        if deselect_test:
            deselected_items.append(item)
        else:
            selected_items.append(item)

    config.hook.pytest_deselected(items=deselected_items)
    items[:] = selected_items


def pytest_markeval_namespace():
    return dict(
        Version=Version,
        get_version=get_version,
    )
