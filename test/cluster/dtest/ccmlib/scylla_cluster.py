#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from cassandra.auth import PlainTextAuthProvider

from test.pylib.internal_types import ServerInfo
from test.pylib.manager_client import ManagerClient
from test.cluster.dtest.ccmlib.common import logger
from test.cluster.dtest.ccmlib.scylla_node import ScyllaNode

if TYPE_CHECKING:
    from collections.abc import Iterable


SCYLLA_VERSION_FILE = Path(__file__).parent.parent.parent.parent / "build" / "SCYLLA-VERSION-FILE"


class ScyllaCluster:
    def __init__(self, manager: ManagerClient, scylla_mode: str, force_wait_for_cluster_start: bool = False):
        self.manager = manager
        self.scylla_mode = scylla_mode
        self._config_options = {}

        if self.scylla_mode == "debug":
            self.default_wait_other_notice_timeout = 600
            self.default_wait_for_binary_proto = 900
        else:
            self.default_wait_other_notice_timeout = 120
            self.default_wait_for_binary_proto = 420

        self.force_wait_for_cluster_start = force_wait_for_cluster_start

    @staticmethod
    def _sorted_nodes(servers: Iterable[ServerInfo]) -> list[ServerInfo]:
        return sorted(servers, key=lambda s: s.server_id)

    @property
    def nodes(self) -> dict[str, ScyllaNode]:
        return {node.name: node for node in self.nodelist()}

    def nodelist(self) -> list[ScyllaNode]:
        return [
            ScyllaNode(cluster=self, server=server, name=f"node{n}")
            for n, server in enumerate(self._sorted_nodes(self.manager.all_servers()), start=1)
        ]

    def populate(self, nodes: int | list[int]) -> ScyllaCluster:
        if self._config_options.get("alternator_enforce_authorization"):
            self.manager.auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
        match nodes:
            case int():
                self.manager.servers_add(servers_num=nodes, config=self._config_options, start=False, auto_rack_dc="dc1")
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

    def start_nodes(self,
                    nodes: list[ScyllaNode] | None = None,
                    no_wait: bool = False,
                    verbose: bool | None = None,  # not used in scylla-dtest
                    wait_for_binary_proto: bool | None = None,
                    wait_other_notice: bool | None = None,
                    wait_normal_token_owner: bool | None = None,
                    jvm_args: list[str] | None = None,
                    profile_options: dict[str, str] | None = None,  # not used in scylla-dtest
                    quiet_start: bool | None = None) -> list[ScyllaNode]:  # not used in scylla-dtest
        assert verbose is None, "argument `verbose` is not supported"
        assert profile_options is None, "argument `profile_options` is not supported"
        assert quiet_start is None, "argument `quiet_start` is not supported"

        self.debug(
            f"start_nodes: no_wait={no_wait} wait_for_binary_proto={wait_for_binary_proto}"
            f" wait_other_notice={wait_other_notice} wait_normal_token_owner={wait_normal_token_owner}"
            f" force_wait_for_cluster_start={self.force_wait_for_cluster_start}"
        )

        if nodes is None:
            nodes = self.nodelist()
        elif isinstance(nodes, ScyllaNode):
            nodes = [nodes]
        started = []

        for node in nodes:
            if not node.is_running():
                node.start(
                    no_wait=no_wait,
                    wait_other_notice=wait_other_notice,
                    wait_normal_token_owner=wait_normal_token_owner,
                    wait_for_binary_proto=wait_for_binary_proto,
                    jvm_args=jvm_args,
                )
                started.append(node)

        return started

    def start(self,
              no_wait: bool = False,
              verbose: bool | None = None,  # not used in scylla-dtest
              wait_for_binary_proto: bool | None = None,
              wait_other_notice: bool | None = None,
              wait_normal_token_owner: bool | None = None,
              jvm_args: list[str] | None = None,
              profile_options: dict[str, str] | None = None,  # not used in scylla-dtest
              quiet_start: bool | None = None) -> list[ScyllaNode]:  # not used in scylla-dtest
        assert verbose is None, "argument `verbose` is not supported"
        assert profile_options is None, "argument `profile_options` is not supported"
        assert quiet_start is None, "argument `quiet_start` is not supported"

        return self.start_nodes(
            no_wait=no_wait,
            wait_for_binary_proto=wait_for_binary_proto,
            wait_other_notice=wait_other_notice,
            wait_normal_token_owner=wait_normal_token_owner,
            jvm_args=jvm_args,
        )

    def stop_nodes(self,
                   nodes: list[ScyllaNode] | None = None,
                   wait: bool = True,
                   gently: bool = True,
                   wait_other_notice: bool = False,
                   other_nodes: list[ScyllaNode] | None = None,
                   wait_seconds: int | None = None) -> list[ScyllaNode]:
        if nodes is None:
            nodes = self.nodelist()
        elif isinstance(nodes, ScyllaNode):
            nodes = [nodes]

        for node in nodes:
            node.stop(
                wait=wait,
                wait_other_notice=wait_other_notice,
                other_nodes=other_nodes,
                gently=gently,
                wait_seconds=wait_seconds,
            )

        return [node for node in nodes if not node.is_running()]

    def stop(self,
             wait: bool = True,
             gently: bool = True,
             wait_other_notice: bool = False,
             other_nodes: list[ScyllaNode] | None = None,
             wait_seconds: int | None = None) -> list[ScyllaNode]:
        return self.stop_nodes(
            wait=wait,
            gently=gently,
            wait_other_notice=wait_other_notice,
            other_nodes=other_nodes,
            wait_seconds=wait_seconds,
        )

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
