#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from cassandra.auth import PlainTextAuthProvider

from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.scylla_server import ScyllaVersionDescription, get_current_version_description
from test.cluster.dtest.ccmlib import scylla_repository
from test.cluster.dtest.ccmlib.common import logger
from test.cluster.dtest.ccmlib.scylla_node import ScyllaNode

if TYPE_CHECKING:
    from typing import Any


def _racks_of(dc_nodes: dict | list, topology: dict) -> dict:
    """{rack: node count} for one datacenter of a populate() topology."""

    if isinstance(dc_nodes, list):
        return {f"RAC{i}": n for i, n in enumerate(dc_nodes, start=1)}
    if isinstance(dc_nodes, dict):
        return dc_nodes
    raise RuntimeError(f"Unsupported topology specification: {topology}")


class ScyllaCluster:
    def __init__(self,
                 manager: ScyllaClusterManager,
                 scylla_mode: str,
                 force_wait_for_cluster_start: bool = False,
                 scylla_version: str | None = None):
        self.manager = manager
        self.scylla_mode = scylla_mode
        self._config_options = {}
        # Cached ScyllaNode instances, keyed by name and in the order
        # _add_nodes() creates them from servers_add().
        self._nodes: dict[str, ScyllaNode] = {}
        self._next_node_num: int = 1

        # How many vnode tokens a node starts with (ScyllaNode._num_tokens()).
        # scylla-dtest ran with dtest_config.num_tokens (256); 9280a039ee
        # lowered it to 16 for the whole suite.  With 16 random tokens a node
        # can own twice what another does, so a test that asserts on how evenly
        # vnodes spread data sets this back to what upstream ran with.
        self.num_tokens: int = 16

        # Which Scylla new nodes are started on.  Upgrade tests move this around
        # with set_install_dir(); everything else stays on the build under test.
        scylla_repository.set_build_mode(scylla_mode)
        self._install = scylla_repository.install(scylla_version or scylla_repository.current_version())

        if self.scylla_mode == "debug":
            self.default_wait_other_notice_timeout = 600
            self.default_wait_for_binary_proto = 900
        else:
            self.default_wait_other_notice_timeout = 120
            self.default_wait_for_binary_proto = 420

        self.force_wait_for_cluster_start = force_wait_for_cluster_start

    @property
    def current_scylla_exe(self) -> str:
        """The executable of the build under test, as the test runner resolved it.

        The path depends on --exe-path and the build mode, so the cluster manager
        is the only place that knows it; scylla_repository leaves it unset.
        """
        return str(self.manager.cluster.scylla_exe)

    def exe_for(self, install: scylla_repository.ScyllaInstall) -> str:
        return self.current_scylla_exe if install.is_current else str(install.exe)

    def version_description(self,
                            install: scylla_repository.ScyllaInstall | None = None) -> ScyllaVersionDescription:
        """How the cluster manager should start a node on this version.

        A released package gets the plain command line: SCYLLA_CMDLINE_OPTIONS is
        the 2025.1 baseline (see test/pylib/scylla_server.py), so it is what every
        version taking part in an upgrade understands.  Only the build under test
        adds options of its own, which older binaries would refuse to boot with.
        """
        install = self._install if install is None else install
        if install.is_current:
            return get_current_version_description(self.current_scylla_exe)
        return ScyllaVersionDescription(path=str(install.exe), config={}, argv=[])

    def get_install_dir(self) -> str:
        """The install dir of the Scylla this cluster's nodes run on.

        ccm's Cluster.get_install_dir().  Ported tests hand it to
        ccmlib.common.isScylla() and tools.misc.is_coverage(); for the build
        under test it is the source tree root, which is the layout both of
        those probe.
        """
        return str(self._install.install_dir)

    def set_install_dir(self, install_dir: str) -> ScyllaCluster:
        """Run nodes added from now on on the Scylla in this install dir.

        ccm's Cluster.set_install_dir(); upgrade tests call it (through
        UpgradeTester._change_cluster_version) to add a node on an older version
        than the rest of the cluster, or the other way round.
        """
        self._install = scylla_repository.install_for_dir(install_dir)
        self.debug(f"Cluster install dir is now {install_dir} (Scylla {self._install.version})")
        return self

    def upgrade_cluster(self, upgrade_version: str) -> None:
        """Upgrade every node to `upgrade_version`, one at a time."""
        for node in self.nodelist():
            node.upgrade(upgrade_to_version=upgrade_version)
        self._install = scylla_repository.install(upgrade_version)

    def _add_nodes(self, servers: list) -> None:
        """Create ScyllaNode instances for the given servers and cache them."""
        for server in servers:
            name = f"node{self._next_node_num}"
            self._next_node_num += 1
            self._nodes[name] = ScyllaNode(cluster=self, server=server, name=name)

    @property
    def nodes(self) -> dict[str, ScyllaNode]:
        """The nodes by name.  Live, as ccm's is: a test that removes a node from
        the cluster drops it from here (see e.g. the topology-during-upgrade
        tests, which removenode() an old-version node and then forget it)."""
        return self._nodes

    def nodelist(self) -> list[ScyllaNode]:
        return list(self._nodes.values())

    def get_node_ip(self, nodeid: int) -> str:
        return self.nodelist()[nodeid-1].address()

    def populate(self, nodes: int | list[int]) -> ScyllaCluster:
        if self._config_options.get("alternator_enforce_authorization"):
            self.manager.auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
        version = self.version_description()
        match nodes:
            case int():
                self._add_nodes(self.manager.servers_add(servers_num=nodes, config=self._new_node_config, version=version, start=False, auto_rack_dc="dc1"))
            case list():
                for dc, n_nodes in enumerate(nodes, start=1):
                    dc_name = f"dc{dc}"
                    if n_nodes == 0:
                        continue
                    self._add_nodes(self.manager.servers_add(
                        servers_num=n_nodes,
                        config=self._config_options,
                        version=version,
                        start=False,
                        auto_rack_dc=dc_name
                    ))
            case dict():
                # {"dc1": {"rack1": 3, "rack2": 2}}, {"dc1": [3, 2]} (racks RAC1, RAC2)
                # or {"dc1": 3}.  A bare count gets one rack per node, as the int
                # and list forms do: ccm put them in one rack, but tablets
                # keyspaces here need as many racks as their replication factor.
                for dc, dc_nodes in nodes.items():
                    if isinstance(dc_nodes, int):
                        if dc_nodes == 0:  # ccm creates no node for an empty datacenter
                            continue
                        self._add_nodes(self.manager.servers_add(
                            servers_num=dc_nodes,
                            config=self._new_node_config,
                            version=version,
                            start=False,
                            auto_rack_dc=dc,
                        ))
                        continue
                    dc_nodes = _racks_of(dc_nodes, nodes)
                    for rack, rack_nodes in dc_nodes.items():
                        if not isinstance(rack_nodes, int):
                            raise RuntimeError(f"Unsupported topology specification: {nodes}")
                        if rack_nodes == 0:
                            continue
                        self._add_nodes(self.manager.servers_add(
                            servers_num=rack_nodes,
                            config=self._config_options,
                            version=version,
                            property_file={
                                "dc": dc,
                                "rack": rack,
                            },
                            start=False,
                        ))
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

    def nodetool(self, nodetool_cmd: str) -> ScyllaCluster:
        for node in self.nodelist():
            if node.is_running():
                node.nodetool(nodetool_cmd)
        return self

    def version(self) -> str:
        """The Scylla version nodes added right now would run."""
        return self._install.version

    def cassandra_version(self) -> str:
        """ccm's Cluster.cassandra_version(), an alias of version()."""
        return self.version()

    def show(self, verbose: bool) -> str:
        """ccm's Cluster.show(): the cluster's nodes, one status line each unless verbose."""
        header = f"Cluster: '{self.manager.cluster.name}'"
        lines = [header, "-" * len(header)]
        nodes = self.nodelist()
        if not nodes:
            lines.append("No node in this cluster yet")
        for node in nodes:
            lines.append(node.show(show_cluster=False) + "\n" if verbose else node.show(only_status=True))
        return "\n".join(lines)

    def set_configuration_options(self,
                                  values: dict[str, Any] | None = None,
                                  batch_commitlog: bool | None = None,
                                  nodes: ScyllaNode | list[ScyllaNode] | None = None) -> ScyllaCluster:
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
            if nodes is None:
                self._config_options.update(values)  # keep values as a cluster config for new nodes
                nodes = self.nodelist()
            elif isinstance(nodes, ScyllaNode):
                nodes = [nodes]
            for node in nodes:
                self.manager.server_update_config(server_id=node.server_id, config_options=values)
        return self

    def enable_internode_ssl(self, node_ssl_path: str, internode_encryption: str = "all") -> ScyllaCluster:
        """Configure server_encryption_options from certs generated by tools.misc.generate_ssl_stores().

        Mirrors scylla-ccm's Cluster.enable_internode_ssl() (ccmlib/cluster.py), adapted to
        Scylla's server_encryption_options schema (certificate/keyfile/truststore, see
        conf/scylla.yaml) rather than the Cassandra-only jks keystore options.
        """
        node_ssl_options = {
            "internode_encryption": internode_encryption,
            "certificate": os.path.join(node_ssl_path, "ccm_node.pem"),
            "keyfile": os.path.join(node_ssl_path, "ccm_node.key"),
            "truststore": os.path.join(node_ssl_path, "ccm_node.cer"),
        }
        return self.set_configuration_options(values={"server_encryption_options": node_ssl_options})

    def flush(self) -> None:
        for node in self.nodelist():
            if node.is_running():
                node.flush()

    @staticmethod
    def remove_dir_with_retry(path: str) -> None:
        """Remove a file or directory."""
        import shutil
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)

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
