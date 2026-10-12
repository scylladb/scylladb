#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

# Tests of the system.config virtual table which need control over how the
# node was started (its scylla.yaml), so they can't run in test/cqlpy.
# More system.config tests are in test/cqlpy/test_virtual_tables.py.

import json

import pytest
from cassandra import WriteFailure

from test.pylib.scylla_cluster_manager import ScyllaClusterManager


def parse_value(value: str):
    """system.config shows values as JSON, except for some enum options."""
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def same_value(config_value, yaml_value) -> bool:
    if config_value == yaml_value:
        return True
    if isinstance(yaml_value, bool):
        # Enum options like "restriction mode" accept true/false in the yaml
        # but are shown as 1/0.
        return str(config_value).lower() in (("1", "true") if yaml_value else ("0", "false"))
    return str(config_value) == str(yaml_value)


# Every option system.config reports with source 'config' must have the value
# written in the node's scylla.yaml.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_system_config_source_config(manager: ScyllaClusterManager):
    server = await manager.server_add()
    yaml = await manager.server_get_config(server.server_id)
    rows = [row for row in await manager.get_cql().run_async("SELECT name, type, value, source FROM system.config")
            if row.source == "config"]
    assert rows
    mismatches = []
    for row in rows:
        if row.name == "seed_provider":
            # system.config doesn't print the seed provider's value.
            continue
        # An option with a short alias is named by both, e.g. 'workdir,W'.
        name = row.name if row.name in yaml else row.name.split(",")[0]
        if name not in yaml or not same_value(parse_value(row.value), yaml[name]):
            mismatches.append((row, yaml.get(name)))
    assert not mismatches


# Every option system.config reports with source 'cli' must have the value
# given on the node's command line.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_system_config_source_cli(manager: ScyllaClusterManager):
    server = await manager.server_add(config={"tablets_mode_for_new_keyspaces": "disabled"})
    with open(f"/proc/{server.pid}/cmdline", "rb") as f:
        args = f.read().decode().split("\0")
    rows = [row for row in await manager.get_cql().run_async("SELECT name, value, source FROM system.config")
            if row.source == "cli"]
    assert rows
    mismatches = []
    for row in rows:
        arg = "--" + row.name.replace("_", "-")
        values = [args[i + 1] for i, a in enumerate(args) if a == arg]
        if len(values) != 1 or not same_value(parse_value(row.value), parse_value(values[0])):
            mismatches.append((row, values))
    assert not mismatches


# Once enable_cql_config_updates is turned off through system.config, no
# further updates of system.config are accepted, including turning it back
# on. This can't be undone through CQL, so it needs its own node.
@pytest.mark.skip_mode(mode=["dev", "debug"], reason="scylla-dtest gates on it in release mode only")
async def test_system_config_disable_cql_updates(manager: ScyllaClusterManager):
    await manager.server_add(config={"tablets_mode_for_new_keyspaces": "disabled"})
    cql = manager.get_cql()
    await cql.run_async("update system.config set value = '0' where name = 'enable_cql_config_updates';")
    with pytest.raises(WriteFailure, match="this virtual table doesn't allow updates"):
        await cql.run_async("update system.config set value = '1' where name = 'enable_cql_config_updates';")
