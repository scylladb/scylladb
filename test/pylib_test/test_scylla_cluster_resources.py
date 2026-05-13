#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging

import pytest

from test.pylib.scylla_cluster import (
    ScyllaCluster,
    ScyllaResourceLimit,
    ScyllaResourceUsage,
    parse_scylla_memory,
    scylla_cmdline_has_memory_override,
    scylla_resource_usage_from_cmdline,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("512M", 512 * 1024 ** 2),
        ("1G", 1024 ** 3),
        ("2GiB", 2 * 1024 ** 3),
        (1024, 1024),
    ],
)
def test_parse_scylla_memory(value, expected):
    assert parse_scylla_memory(value) == expected


@pytest.mark.parametrize(
    "cmdline",
    [
        ["-m", "2G"],
        ["-m2G"],
        ["--memory", "2G"],
        ["--memory=2G"],
    ],
)
def test_scylla_cmdline_has_memory_override(cmdline):
    assert scylla_cmdline_has_memory_override(cmdline)


def test_scylla_cmdline_has_memory_override_ignores_other_options():
    assert not scylla_cmdline_has_memory_override(["--max-task-backlog", "200", "--smp", "4"])


@pytest.mark.parametrize(
    ("cmdline", "usage"),
    [
        ([], ScyllaResourceUsage(cores=2, memory_bytes=1024 ** 3)),
        (["--smp", "1"], ScyllaResourceUsage(cores=1, memory_bytes=1024 ** 3)),
        (["--smp=4", "-m", "2G"], ScyllaResourceUsage(cores=4, memory_bytes=2 * 1024 ** 3)),
        (["--memory=512M"], ScyllaResourceUsage(cores=2, memory_bytes=512 * 1024 ** 2)),
    ],
)
def test_scylla_resource_usage_from_cmdline(cmdline, usage):
    assert scylla_resource_usage_from_cmdline(cmdline) == usage


def test_scylla_resource_limit_rejects_excess_cores():
    cluster = ScyllaCluster(logging.getLogger(__name__), None, 0, lambda params: None)
    cluster.set_resource_limit(ScyllaResourceLimit(cores=2))

    with pytest.raises(RuntimeError, match="Scylla core limit exceeded"):
        cluster._check_resource_limit(ScyllaResourceUsage(cores=3), has_memory_override=False)


def test_unbounded_scylla_resource_limit_does_not_reject_usage():
    cluster = ScyllaCluster(logging.getLogger(__name__), None, 0, lambda params: None)
    cluster.set_resource_limit(ScyllaResourceLimit(cores=2, memory_bytes=1024 ** 3, enforce_usage_limits=False))

    cluster._check_resource_limit(ScyllaResourceUsage(cores=100, memory_bytes=100 * 1024 ** 3), has_memory_override=False)


def test_unbounded_scylla_resource_limit_still_controls_memory_override():
    cluster = ScyllaCluster(logging.getLogger(__name__), None, 0, lambda params: None)
    cluster.set_resource_limit(ScyllaResourceLimit(cores=2, memory_bytes=1024 ** 3, enforce_usage_limits=False))

    with pytest.raises(RuntimeError, match="Scylla memory overrides"):
        cluster._check_resource_limit(ScyllaResourceUsage(), has_memory_override=True)


def test_scylla_resource_limit_rejects_memory_override_without_resources_marker():
    cluster = ScyllaCluster(logging.getLogger(__name__), None, 0, lambda params: None)

    with pytest.raises(RuntimeError, match="Scylla memory overrides"):
        cluster._check_resource_limit(ScyllaResourceUsage(), has_memory_override=True)


def test_scylla_resource_limit_detects_global_memory_override():
    cluster = ScyllaCluster(
        logging.getLogger(__name__),
        None,
        0,
        lambda params: None,
        build_cmdline_options=lambda cmdline, version: ["--smp", "1", "-m", "1G", "--memory", "1G"],
        has_memory_override=lambda cmdline, version: True,
    )

    with pytest.raises(RuntimeError, match="Scylla memory overrides"):
        cluster._check_resource_limit(
            cluster._resource_usage_from_test_cmdline([], None),
            cluster._has_memory_override_from_test_cmdline([], None),
        )
