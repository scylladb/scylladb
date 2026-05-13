#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import TYPE_CHECKING, Any, TypeAlias

import pytest

if TYPE_CHECKING:
    from _pytest.nodes import Node


JsonValue: TypeAlias = Any


@dataclass(frozen=True)
class ScyllaClusterNodeProfile:
    config: dict[str, JsonValue]
    cmdline: list[str]
    property_file: dict[str, JsonValue] | None
    server_encryption: str

    def to_json(self) -> dict[str, object]:
        return {
            "config": self.config,
            "cmdline": self.cmdline,
            "property_file": self.property_file,
            "server_encryption": self.server_encryption,
        }


@dataclass(frozen=True)
class ScyllaClusterProfile:
    key: str
    name: str
    reuse: str
    nodes: tuple[ScyllaClusterNodeProfile, ...]

    def as_manager_payload(self) -> dict[str, object]:
        return {
            "key": self.key,
            "name": self.name,
            "reuse": self.reuse,
            "nodes": [node.to_json() for node in self.nodes],
        }


def _jsonable(value: object, marker_name: str, field_name: str) -> JsonValue:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (tuple, list)):
        return [_jsonable(item, marker_name, field_name) for item in value]
    if isinstance(value, dict):
        result: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise pytest.UsageError(f"@pytest.mark.{marker_name} {field_name} keys must be strings")
            result[key] = _jsonable(item, marker_name, f"{field_name}.{key}")
        return result
    raise pytest.UsageError(
        f"@pytest.mark.{marker_name} {field_name} must contain only literal JSON-compatible values, got {type(value).__name__}"
    )


def _dict(value: object | None, marker_name: str, field_name: str) -> dict[str, JsonValue]:
    if value is None:
        return {}
    normalized = _jsonable(value, marker_name, field_name)
    if not isinstance(normalized, dict):
        raise pytest.UsageError(f"@pytest.mark.{marker_name} {field_name} must be a dictionary")
    return normalized


def _optional_dict(value: object | None, marker_name: str, field_name: str) -> dict[str, JsonValue] | None:
    if value is None:
        return None
    return _dict(value, marker_name, field_name)


def _cmdline(value: object | None, marker_name: str, field_name: str) -> list[str]:
    if value is None:
        return []
    normalized = _jsonable(value, marker_name, field_name)
    if not isinstance(normalized, list) or not all(isinstance(item, str) for item in normalized):
        raise pytest.UsageError(f"@pytest.mark.{marker_name} {field_name} must be a list of strings")
    return normalized


def _positive_int(value: object, marker_name: str, field_name: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise pytest.UsageError(f"@pytest.mark.{marker_name} {field_name} must be a positive integer") from exc
    if result <= 0:
        raise pytest.UsageError(f"@pytest.mark.{marker_name} {field_name} must be a positive integer")
    return result


def _suite_cfg(suite_config: object | None) -> dict[str, Any]:
    if suite_config is None:
        return {}
    cfg = getattr(suite_config, "cfg", {})
    return cfg if isinstance(cfg, dict) else {}


def _suite_name(suite_config: object | None) -> str | None:
    if suite_config is None:
        return None
    name = getattr(suite_config, "name", None)
    return str(name) if name is not None else None


def _build_mode(suite_config: object | None, build_mode: str | None) -> str | None:
    if build_mode is not None:
        return build_mode
    if suite_config is None:
        return None
    mode = getattr(suite_config, "mode", None)
    return str(mode) if mode is not None else None


def _suite_cmdline_options(suite_config: object | None) -> list[str]:
    cmdline_options = _suite_cfg(suite_config).get("extra_scylla_cmdline_options", [])
    if isinstance(cmdline_options, str):
        return [cmdline_options]
    if cmdline_options is None:
        return []
    normalized = _jsonable(cmdline_options, "scylla_cluster", "suite extra_scylla_cmdline_options")
    if not isinstance(normalized, list) or not all(isinstance(item, str) for item in normalized):
        raise pytest.UsageError("suite extra_scylla_cmdline_options must be a list of strings")
    return normalized


def _suite_config_options(suite_config: object | None) -> dict[str, JsonValue]:
    return _dict(_suite_cfg(suite_config).get("extra_scylla_config_options", {}), "scylla_cluster", "suite extra_scylla_config_options")


def _profile_from_kwargs(kwargs: dict[str, object], suite_config: object | None, marker_name: str,
                         build_mode: str | None = None) -> ScyllaClusterProfile:
    unknown_kwargs = set(kwargs) - {"nodes", "config", "cmdline", "property_file", "auto_rack_dc", "server_encryption", "reuse"}
    if unknown_kwargs:
        raise pytest.UsageError(f"Unknown @pytest.mark.{marker_name} arguments: {', '.join(sorted(unknown_kwargs))}")

    reuse = kwargs.get("reuse", "sequential")
    if reuse != "sequential":
        raise pytest.UsageError(f"@pytest.mark.{marker_name} reuse must be 'sequential'")

    config = _dict(kwargs.get("config"), marker_name, "config")
    cmdline = _cmdline(kwargs.get("cmdline"), marker_name, "cmdline")
    server_encryption = kwargs.get("server_encryption", "none")
    if not isinstance(server_encryption, str):
        raise pytest.UsageError(f"@pytest.mark.{marker_name} server_encryption must be a string")

    property_file_arg = kwargs.get("property_file")
    auto_rack_dc = kwargs.get("auto_rack_dc")
    if property_file_arg is not None and auto_rack_dc is not None:
        raise pytest.UsageError(f"@pytest.mark.{marker_name} accepts either property_file or auto_rack_dc, not both")
    if auto_rack_dc is not None and not isinstance(auto_rack_dc, str):
        raise pytest.UsageError(f"@pytest.mark.{marker_name} auto_rack_dc must be a string")

    nodes_arg = kwargs.get("nodes")
    if nodes_arg is None:
        raise pytest.UsageError(f"@pytest.mark.{marker_name} requires nodes")

    nodes: list[ScyllaClusterNodeProfile]
    if isinstance(nodes_arg, int) and not isinstance(nodes_arg, bool):
        node_count = _positive_int(nodes_arg, marker_name, "nodes")
        property_files: list[dict[str, JsonValue] | None]
        if auto_rack_dc is not None:
            property_files = [{"dc": auto_rack_dc, "rack": f"rack{i + 1}"} for i in range(node_count)]
        elif isinstance(property_file_arg, (list, tuple)):
            normalized_property_files = _jsonable(property_file_arg, marker_name, "property_file")
            if not isinstance(normalized_property_files, list) or len(normalized_property_files) != node_count:
                raise pytest.UsageError(f"@pytest.mark.{marker_name} property_file list length must match nodes")
            property_files = []
            for index, item in enumerate(normalized_property_files):
                if not isinstance(item, dict):
                    raise pytest.UsageError(f"@pytest.mark.{marker_name} property_file[{index}] must be a dictionary")
                property_files.append(item)
        else:
            property_files = [_optional_dict(property_file_arg, marker_name, "property_file") for _ in range(node_count)]
        nodes = [ScyllaClusterNodeProfile(config, cmdline, property_files[i], server_encryption) for i in range(node_count)]
    elif isinstance(nodes_arg, (list, tuple)):
        if property_file_arg is not None:
            raise pytest.UsageError(f"@pytest.mark.{marker_name} property_file must be specified per node when nodes is a list")
        if auto_rack_dc is not None:
            raise pytest.UsageError(f"@pytest.mark.{marker_name} auto_rack_dc requires integer nodes")
        nodes = []
        for index, raw_node in enumerate(nodes_arg):
            if not isinstance(raw_node, dict):
                raise pytest.UsageError(f"@pytest.mark.{marker_name} nodes[{index}] must be a dictionary")
            unknown_node_kwargs = set(raw_node) - {"config", "cmdline", "property_file", "server_encryption"}
            if unknown_node_kwargs:
                raise pytest.UsageError(
                    f"Unknown @pytest.mark.{marker_name} nodes[{index}] arguments: {', '.join(sorted(unknown_node_kwargs))}"
                )
            node_config = config | _dict(raw_node.get("config"), marker_name, f"nodes[{index}].config")
            node_cmdline = cmdline + _cmdline(raw_node.get("cmdline"), marker_name, f"nodes[{index}].cmdline")
            node_server_encryption = raw_node.get("server_encryption", server_encryption)
            if not isinstance(node_server_encryption, str):
                raise pytest.UsageError(f"@pytest.mark.{marker_name} nodes[{index}].server_encryption must be a string")
            nodes.append(
                ScyllaClusterNodeProfile(
                    config=node_config,
                    cmdline=node_cmdline,
                    property_file=_optional_dict(raw_node.get("property_file"), marker_name, f"nodes[{index}].property_file"),
                    server_encryption=node_server_encryption,
                )
            )
        if not nodes:
            raise pytest.UsageError(f"@pytest.mark.{marker_name} nodes must not be empty")
    else:
        raise pytest.UsageError(f"@pytest.mark.{marker_name} nodes must be a positive integer or a list of dictionaries")

    suite_config_options = _suite_config_options(suite_config)
    suite_cmdline_options = _suite_cmdline_options(suite_config)
    key_payload = {
        "suite": {
            "name": _suite_name(suite_config),
            "build_mode": _build_mode(suite_config, build_mode),
            "extra_scylla_config_options": suite_config_options,
            "extra_scylla_cmdline_options": suite_cmdline_options,
        },
        "reuse": reuse,
        "nodes": [
            {
                "config": suite_config_options | node.config,
                "cmdline": suite_cmdline_options + node.cmdline,
                "property_file": node.property_file,
                "server_encryption": node.server_encryption,
            }
            for node in nodes
        ],
    }
    key = json.dumps(key_payload, sort_keys=True, separators=(",", ":"))
    name = f"scylla_cluster:{hashlib.sha1(key.encode('utf-8')).hexdigest()[:12]}"
    return ScyllaClusterProfile(key=key, name=name, reuse=str(reuse), nodes=tuple(nodes))


def scylla_cluster_profile_from_node(node: Node, suite_config: object | None = None,
                                     build_mode: str | None = None) -> ScyllaClusterProfile | None:
    """Return the reusable Scylla cluster profile declared on a pytest node."""
    marker = node.get_closest_marker("scylla_cluster")
    if marker is not None:
        if marker.args:
            if len(marker.args) > 1 or "nodes" in marker.kwargs:
                raise pytest.UsageError("Use @pytest.mark.scylla_cluster(nodes=...) or a single positional nodes argument")
            kwargs = {**marker.kwargs, "nodes": marker.args[0]}
        else:
            kwargs = dict(marker.kwargs)
        return _profile_from_kwargs(kwargs, suite_config, "scylla_cluster", build_mode)

    if node.get_closest_marker("prepare_3_nodes_cluster") is not None:
        return _profile_from_kwargs({"nodes": 3}, suite_config, "prepare_3_nodes_cluster", build_mode)

    if node.get_closest_marker("prepare_3_racks_cluster") is not None:
        return _profile_from_kwargs({"nodes": 3, "auto_rack_dc": "dc1"}, suite_config, "prepare_3_racks_cluster", build_mode)

    return None
