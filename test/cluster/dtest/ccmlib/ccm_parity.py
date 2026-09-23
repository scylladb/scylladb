#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Run the nodes of ported dtests exactly as upstream scylla-dtest and ccm did.

The first port of scylla-dtest has to exercise the same Scylla code as the
original, so its nodes get ccm's scylla.yaml, command line, vnodes and snitch,
even where test.py's defaults are better for CI.  The dtests that were in the
tree before the port (pre_port_dtests.txt) keep test.py's defaults.  Meant to go
away once the port is merged.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pytest

PRE_PORT_DTESTS = Path(__file__).resolve().parent.parent / "pre_port_dtests.txt"

# scylla-dtest's --num-tokens default: ccm wrote it into every node's scylla.yaml.
UPSTREAM_NUM_TOKENS = 256

# ccm gave every node these (ccmlib/scylla_node.py update_yaml(); network_interfaces()).
CCM_STORAGE_PORT = 7000
CCM_NATIVE_TRANSPORT_PORT = 9042

# What test.py itself reads back from a server's config, or needs to run it: the
# node's addresses and seeds, and where it keeps its files.  ccm wrote the same
# things with node-specific values (data directories under the node's own path);
# these keep test.py's.
TESTPY_KEEPS = ("cluster_name", "listen_address", "rpc_address", "api_address", "seed_provider")


@cache
def pre_port_dtests() -> frozenset[str]:
    return frozenset(line.strip() for line in PRE_PORT_DTESTS.read_text().splitlines()
                     if line.strip() and not line.startswith("#"))


def runs_as_upstream(item: pytest.Item) -> bool:
    """Whether a test is one of the ported ones, which run their nodes as ccm did."""

    name = Path(str(item.fspath)).name
    test = getattr(item, "originalname", None) or item.name
    classes = [c.__name__ for c in item.cls.__mro__] if getattr(item, "cls", None) else []
    keys = [f"{name}::{c}::{test}" for c in classes] or [f"{name}::{test}"]
    return not any(k in pre_port_dtests() for k in keys)


def merge_options(data: dict[str, Any], options: dict[str, Any]) -> dict[str, Any]:
    """ccm's way of applying cluster and node options to a node's scylla.yaml.

    A None value deletes the option; a dict value is merged into the existing dict
    (ccmlib/scylla_node.py update_yaml()).
    """

    for name, value in options.items():
        if value is None:
            data.pop(name, None)
        elif isinstance(data.get(name), dict) and isinstance(value, dict):
            data[name] = data[name] | value
        else:
            data[name] = value
    return data


def ccm_scylla_yaml(shipped: dict[str, Any],
                    current: dict[str, Any],
                    options: dict[str, Any],
                    auto_bootstrap: bool,
                    initial_token: str | None,
                    use_vnodes: bool) -> dict[str, Any]:
    """The scylla.yaml ccm would have written for a node.

    `shipped` is the tree's conf/scylla.yaml, `current` the config test.py made
    for the server (only TESTPY_KEEPS is taken from it), `options` the cluster's
    options followed by the node's own.  As in ccm, the node's own attributes
    (auto_bootstrap, initial_token, one token without vnodes) go in first and the
    options after them, so an option set to None -- dtest's initial_token with
    vnodes on -- deletes the node's value too.
    """

    data = dict(shipped)
    data |= {k: current[k] for k in TESTPY_KEEPS if k in current}
    # ccm spelled the working directory "workdir,W", the option's full name in
    # Scylla (db/config.cc); test.py's "workdir" is the same option.
    if "workdir" in current:
        data["workdir,W"] = current["workdir"]
        # ccm named these two explicitly (so Scylla does not derive them from the
        # workdir); the commitlog keeps test.py's directory name, "commitlog" where
        # ccm had "commitlogs", which the in-tree tests and the shim look for.
        data["data_file_directories"] = [f"{current['workdir']}/data"]
        data["commitlog_directory"] = f"{current['workdir']}/commitlog"
    data |= {
        "auto_bootstrap": auto_bootstrap,
        "initial_token": initial_token,
        "storage_port": CCM_STORAGE_PORT,
        "native_transport_port": CCM_NATIVE_TRANSPORT_PORT,
    }
    if not use_vnodes:
        data["num_tokens"] = 1
    data = merge_options(data, options)
    if data.get("initial_token") is None:
        data.pop("initial_token", None)
    if "alternator_port" in data or "alternator_https_port" in data:
        data["alternator_address"] = data["listen_address"]
    return data
