#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from cassandra.auth import PlainTextAuthProvider

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.dtest.ccmlib.common import logger
from test.dtest.ccmlib.scylla_node import ScyllaNode

if TYPE_CHECKING:
    from collections.abc import Iterable


SCYLLA_VERSION_FILE = Path(__file__).parent.parent.parent.parent / "build" / "SCYLLA-VERSION-FILE"


class ScyllaCluster:
    def __init__(self, manager: ManagerClient, scylla_mode: str):
        self.manager = manager
        self.scylla_mode = scylla_mode
        self._config_options = {}

    @staticmethod
    def _sorted_nodes(servers: Iterable[ServerInfo]) -> list[ServerInfo]:
        return sorted(servers, key=lambda s: s.server_id)

    @property
    def nodes(self) -> dict[str, ScyllaNode]:
        return {node.name: node for node in self.nodelist()}

    def nodelist(self) -> list[ScyllaNode]:
        return [ScyllaNode(cluster=self, server=server) for server in self._sorted_nodes(self.manager.all_servers())]

    def populate(self, nodes: int | list[int]) -> ScyllaCluster:
        if self._config_options.get("alternator_enforce_authorization"):
            self.manager.auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
        match nodes:
            case int():
                self.manager.servers_add(servers_num=nodes, config=self._config_options, start=False)
            case list():
                for dc, n_nodes in enumerate(nodes, start=1):
                    self.manager.servers_add(
                        servers_num=n_nodes,
                        config=self._config_options,
                        property_file={
                            "dc": f"dc{dc}",
                            "rack": "RAC1",
                        },
                        start=False,
                    )
            case _:
                raise RuntimeError(f"Unsupported topology specification: {nodes}")

        return self

    def new_node(self, i, auto_bootstrap=False, debug=False, initial_token=None, add_node=True, is_seed=True, data_center=None, rack=None) -> ScyllaNode:
        s_info = self.manager.server_add(config=self._config_options, start=False)
        return ScyllaNode(cluster=self, server=s_info)

    def start(self, wait_for_binary_proto: bool | None = None, wait_other_notice: bool | None = None) -> None:
        for server in self._sorted_nodes(self.manager.all_servers()):
            self.manager.server_start(server_id=server.server_id)

    def stop(self, wait=True, gently=True, wait_other_notice=False, other_nodes=None, wait_seconds=127):
        for server in self._sorted_nodes(self.manager.running_servers()):
            self.manager.server_stop(server_id=server.server_id)

    @staticmethod
    def version() -> str:
        return SCYLLA_VERSION_FILE.read_text().strip()

    def set_configuration_options(self,
                                  values: dict | None = None,
                                  batch_commitlog: bool | None = None) -> ScyllaCluster:
        values = {} if values is None else values.copy()
        if batch_commitlog is not None:
            if batch_commitlog:
                values["commitlog_sync"] = "batch"
                values["commitlog_sync_batch_window_in_ms"] = 5
                values["commitlog_sync_period_in_ms"] = None
            else:
                values["commitlog_sync"] = "periodic"
                values["commitlog_sync_period_in_ms"] = 10000
                values["commitlog_sync_batch_window_in_ms"] = None
        if values:
            self._config_options.update(values)
            for server in self._sorted_nodes(self.manager.all_servers()):
                for k, v in values.items():
                    self.manager.server_update_config(server_id=server.server_id, key=k, value=v)
        return self

    @staticmethod
    def debug(message: str) -> None:
        logger.debug(message)

    @staticmethod
    def info(message: str) -> None:
        logger.info(message)

    @staticmethod
    def warning(message: str) -> None:
        logger.warning(message)

    @staticmethod
    def error(message: str) -> None:
        logger.error(message)
