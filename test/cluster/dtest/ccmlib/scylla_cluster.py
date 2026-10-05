#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import os
import shutil
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from cassandra.auth import PlainTextAuthProvider

from test import TOP_SRC_DIR
from test.pylib.scylla_cluster_manager import ScyllaClusterManager
from test.pylib.scylla_server import ScyllaVersionDescription, get_current_version_description
from test.cluster.dtest.ccmlib import ccm_parity as parity, scylla_repository
from test.cluster.dtest.ccmlib.common import logger
from test.cluster.dtest.ccmlib.scylla_manager import ScyllaManager
from test.cluster.dtest.ccmlib.scylla_node import NodeError, NodetoolError, ScyllaNode

if TYPE_CHECKING:
    from typing import Any


# Keys of conf/scylla.yaml the harness decides for itself: node identity and
# addresses, where the node keeps its files, and what the dtest setup or the
# cluster suite sets (timeouts, tablets, rack validity) -- upstream's dtest
# overrode the same ones on top of ccm's copy of the file.
# ccm's snitch for a cluster whose populate() named datacenters (ccmlib/scylla_cluster.py SNITCH).
CCM_SNITCH = "org.apache.cassandra.locator.GossipingPropertyFileSnitch"

_NOT_FROM_SHIPPED_YAML = {
    "cluster_name", "listen_address", "rpc_address", "api_address", "api_port", "prometheus_address",
    "native_transport_port", "native_shard_aware_transport_port", "storage_port", "ssl_storage_port",
    "seed_provider", "endpoint_snitch", "num_tokens", "initial_token",
    "data_file_directories", "commitlog_directory", "hints_directory", "view_hints_directory",
    "saved_caches_directory", "api_ui_dir", "api_doc_dir", "maintenance_socket",
    "read_request_timeout_in_ms", "write_request_timeout_in_ms", "cas_contention_timeout_in_ms",
    "tablets_mode_for_new_keyspaces", "rf_rack_valid_keyspaces",
}


# Linux capability bits (linux/capability.h).
_CAP_DAC_OVERRIDE = 1
_CAP_DAC_READ_SEARCH = 2


@cache
def _node_launch_prefix() -> list[str]:
    """How to start a node so that file permissions bind it, as they did under ccm.

    Under ccm the nodes ran as an ordinary user, and some tests count on that: they
    take a directory's permissions away and expect Scylla to fail on it.  Where the
    tests run as root (CI's test.py stage runs in a rootless podman container, as uid 0
    of its user namespace), root's capabilities to ignore file permissions would let
    Scylla read anything; start the nodes without them.  Nothing changes for an
    ordinary user.
    """
    with open("/proc/self/status") as status:
        effective = int(next(line.split()[1] for line in status if line.startswith("CapEff:")), 16)
    if not effective & (1 << _CAP_DAC_OVERRIDE | 1 << _CAP_DAC_READ_SEARCH):
        return []
    caps = "-dac_override,-dac_read_search"
    return [shutil.which("setpriv") or "setpriv", f"--inh-caps={caps}", f"--bounding-set={caps}", "--"]


@cache
def _shipped_scylla_yaml() -> dict[str, Any]:
    """The Scylla tree's conf/scylla.yaml, as ccm started every node from it.

    ccm copied the installation's conf/scylla.yaml into each node and rewrote only
    the node-specific keys, so scylla-dtest ran with the packaged settings --
    audit to table, column_index_size_in_kb 1, auto_snapshot, the large-data
    thresholds and so on.  test.py builds a node's scylla.yaml from scratch, so
    without this those options would be at Scylla's built-in defaults and the
    tests would exercise different code than upstream did.
    """
    return {k: v for k, v in _full_shipped_scylla_yaml().items() if k not in _NOT_FROM_SHIPPED_YAML}


@cache
def _full_shipped_scylla_yaml(install_dir: Path = TOP_SRC_DIR) -> dict[str, Any]:
    """conf/scylla.yaml of a Scylla install as is: what ccm copied into a node before rewriting its keys.

    ccm copied it from the install the cluster was created on, so the nodes of an upgrade
    test that starts on an older release get that release's conf/scylla.yaml -- kept
    through the upgrade, and given to nodes added afterwards too.
    """
    with open(Path(install_dir) / "conf" / "scylla.yaml") as f:
        return yaml.safe_load(f) or {}


def _racks_of(dc_nodes: dict | list, topology: dict) -> dict:
    """{rack: node count} for one datacenter of a populate() topology."""

    if isinstance(dc_nodes, list):
        return {f"RAC{i}": n for i, n in enumerate(dc_nodes, start=1)}
    if isinstance(dc_nodes, dict):
        return dc_nodes
    raise RuntimeError(f"Unsupported topology specification: {topology}")


class ScyllaCluster:
    def __init__(self,  # noqa: PLR0913
                 manager: ScyllaClusterManager,
                 scylla_mode: str,
                 force_wait_for_cluster_start: bool = False,
                 scylla_version: str | None = None,
                 manager_install_dir: str | Path | None = None,
                 skip_manager_server: bool = False,
                 ccm_parity: bool = False):
        self.manager = manager
        # Run the nodes exactly as scylla-dtest/ccm did (scylla.yaml, command line,
        # vnodes, snitch): the ported tests do, see ccm_parity.py.
        self.ccm_parity = ccm_parity
        self._ccm_use_vnodes = False
        self._path: Path | None = None
        self.scylla_mode = scylla_mode
        self._config_options = {}
        # Cached ScyllaNode instances, keyed by name and in the order
        # _add_nodes() creates them from servers_add().
        self._nodes: dict[str, ScyllaNode] = {}
        # Nodes made by new_node(add_node=False) and not yet add()ed: as in ccm, the
        # cluster does not list them, so cluster-wide operations (stop(), nodelist(),
        # wait_other_notice) leave them alone.  The harness still owns their servers.
        self._detached: dict[str, ScyllaNode] = {}

        # ccm's Cluster.seeds.  Outside ccm parity the in-tree manager decides
        # which nodes a starting server actually seeds from, so this list is
        # bookkeeping for the tests that maintain it (cluster_replacement_test,
        # the repair-based node operations).  Under parity it is ccm's list, and
        # nodes start with the seeds ccm would have written for them
        # (_save_ccm_seeds()).
        self.seeds: list = []
        # (level, logger or None for the default level), applied to new nodes too.
        self._log_levels: list[tuple[str, str | None]] = []
        self._next_node_num: int = 1

        # How many vnode tokens a node starts with (ScyllaNode._num_tokens()).
        # scylla-dtest ran with dtest_config.num_tokens (256); 9280a039ee
        # lowered it to 16 for the whole suite.  With 16 random tokens a node
        # can own twice what another does, so a test that asserts on how evenly
        # vnodes spread data sets this back to what upstream ran with.  The ported
        # tests run with upstream's 256 (ccm_parity).
        self.num_tokens: int = parity.UPSTREAM_NUM_TOKENS if self.ccm_parity else 16

        # Which Scylla new nodes are started on.  Upgrade tests move this around
        # with set_install_dir(); everything else stays on the build under test.
        scylla_repository.set_build_mode(scylla_mode)
        self._install = scylla_repository.install(scylla_version or scylla_repository.current_version())
        # The install whose conf/scylla.yaml ccm gave every node of the cluster, nodes added
        # after an upgrade test moved the cluster to a newer version included.
        self._created_install = self._install

        if self.scylla_mode == "debug":
            self.default_wait_other_notice_timeout = 600
            self.default_wait_for_binary_proto = 900
        else:
            self.default_wait_other_notice_timeout = 120
            self.default_wait_for_binary_proto = 420

        self.force_wait_for_cluster_start = force_wait_for_cluster_start

        # Scylla Manager, when the test asked for one.  skip_manager_server
        # keeps the per-node agents but leaves the server to another cluster's
        # manager (see the secondary_cluster fixture).
        self.skip_manager_server = skip_manager_server
        self._scylla_manager = ScyllaManager(self, manager_install_dir) if manager_install_dir else None

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

    @property
    def ccm_scylla_mode(self) -> str:
        """The build mode ccm's ScyllaCluster reported.

        ccm took it from the install's path (build/<mode>); a cluster made from a repository
        version (--scylla-version, an upgrade test's older release) is a relocatable package,
        which it called "release" unless the package was a debug one.
        """
        if self._created_install.is_current:
            return self.scylla_mode
        return "debug" if self.scylla_mode in scylla_repository.DEBUG_BUILD_MODES else "release"

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

    def get_path(self) -> str:
        """Return this cluster's own directory, beside the servers' work dirs.

        The in-tree cluster shim keeps nothing here itself -- every server has
        its own work dir under the suite log dir -- but Scylla Manager needs a
        per-cluster place for its config, binaries and log.
        """

        # One directory per ScyllaCluster, i.e. per test: the manager's cluster (and
        # its name) is reused by the next test on the same worker, and what tests
        # keep here must not leak into it -- generate_ssl_stores() is a no-op when
        # a keystore is already there, so a later TLS test got the certificate
        # issued for the previous test's node addresses.  ccm gave every test a
        # fresh cluster directory too.
        if self._path is None:
            self._path = Path(self.manager.cluster.vardir) / f"dtest-cluster-{self.manager.cluster.name}-{uuid.uuid4().hex[:8]}"
            self._path.mkdir(parents=True, exist_ok=True)
        return str(self._path)

    def _add_nodes(self, servers: list, auto_bootstrap: bool = False) -> None:
        """Create ScyllaNode instances for the given servers and cache them."""
        for server in servers:
            name = f"node{self._next_node_num}"
            self._next_node_num += 1
            node = self._nodes[name] = ScyllaNode(cluster=self, server=server, name=name)
            self.manager.cluster.servers[node.server_id].launch_prefix = _node_launch_prefix()
            for level, class_name in self._log_levels:
                node.set_log_level(level, class_name)
            if self.ccm_parity:
                self._write_ccm_scylla_yaml(node, auto_bootstrap=auto_bootstrap)

    def _write_ccm_scylla_yaml(self, node: ScyllaNode, auto_bootstrap: bool) -> None:
        """Give a new, still stopped node the scylla.yaml ccm would have written.

        test.py builds a server's config from its own defaults (make_scylla_conf():
        raised timeouts, authentication, shutdown_announce_in_ms 0, ...); ccm
        started from the tree's conf/scylla.yaml and changed only what it had to.
        Every key ccm would not have written is removed, and ccm's are set.
        """
        current = self.manager.server_get_config(server_id=node.server_id)
        target = parity.ccm_scylla_yaml(shipped=_full_shipped_scylla_yaml(self._created_install.install_dir),
                                            current=current,
                                            options=self._config_options,
                                            auto_bootstrap=auto_bootstrap,
                                            initial_token=current.get("initial_token"),
                                            use_vnodes=self._ccm_use_vnodes)
        for key in current.keys() - target.keys():
            self.manager.server_remove_config_option(server_id=node.server_id, key=key)
        self.manager.server_update_config(server_id=node.server_id, config_options=target)

    def set_log_level(self, new_level: str, class_names: list[str] | None = None) -> ScyllaCluster:
        """Set the log level of every node, including nodes added later, as ccm's Cluster did."""

        for class_name in class_names or [None]:
            self._log_levels.append((new_level, class_name))
            for node in self.nodelist():
                node.set_log_level(new_level, class_name)
        return self

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

    def get_binary_interface(self, nodeid: int) -> tuple[str, int]:
        """(address, default CQL port) of the nodeid-th node, as ccm's Cluster.get_binary_interface()."""
        return (self.nodelist()[nodeid-1].network_interfaces["binary"][0], 9042)

    @property
    def _new_node_config(self) -> dict[str, Any]:
        """The scylla.yaml options a node is created with.

        ccm's update_yaml() always wrote native_transport_port (the port of
        node.network_interfaces["binary"], 9042) before the cluster's options,
        and Scylla only listens on the unencrypted port when that option is set
        or native_transport_port_ssl is not (transport/controller.cc): a test
        that sets just native_transport_port_ssl expects both ports.

        Underneath it all, like ccm, the Scylla tree's shipped conf/scylla.yaml
        (see _shipped_scylla_yaml()).
        """
        return _shipped_scylla_yaml() | {"native_transport_port": 9042} | self._config_options

    def repair(self, options: list[str] | None = None) -> None:
        """Repair every live node in the cluster (see ScyllaNode.repair)."""

        for node in self.nodelist():
            node.repair(options)

    def cleanup(self) -> None:
        """Clean up every live node in the cluster (see ScyllaNode.cleanup)."""

        for node in self.nodelist():
            if node.is_running():
                node.cleanup()

    def compact(self, keyspace: str = "", tables: tuple | list = ()) -> None:
        """Compact every live node in the cluster (see ScyllaNode.compact)."""

        for node in self.nodelist():
            if node.is_running():
                node.compact(keyspace, tables)

    def wait_for_compactions(self,
                             keyspace: str = "",
                             column_family: str = "",
                             timeout: float = 300,
                             quiesce_time: float = 0.5) -> None:
        """Wait out the compactions on every live node (see ScyllaNode.wait_for_compactions)."""

        for node in self.nodelist():
            if node.is_running():
                node.wait_for_compactions(keyspace=keyspace,
                                          column_family=column_family,
                                          timeout=timeout,
                                          quiesce_time=quiesce_time)

    def balanced_tokens(self, node_count: int) -> list[str]:
        """Return `node_count` initial tokens, evenly spaced across Scylla's murmur3 ring."""

        return [str(((2**64 // node_count) * i) - 2**63) for i in range(node_count)]

    def add_seed(self, node: ScyllaNode | str) -> None:
        """Record a node (or address) as a seed, as ccm's Cluster.add_seed did."""

        # ccm's add_seed() turned only a plain Node into its address; a ScyllaNode
        # went into the list as it was, and so counted as a seed in get_seeds(node).
        address = node if self.ccm_parity or not isinstance(node, ScyllaNode) else node.address()
        if address not in self.seeds:
            self.seeds.append(address)

    def _ccm_seeds_of(self, node: ScyllaNode) -> list[str]:
        """ccm's Cluster.get_seeds(node): all seeds, or for a seed, those up to itself."""

        seeds = self.seeds[:self.seeds.index(node) + 1] if node in self.seeds else self.seeds
        return [s.address() if isinstance(s, ScyllaNode) else s for s in seeds]

    def _save_ccm_seeds(self, nodes: list[ScyllaNode]) -> None:
        """Note the seeds ccm's ScyllaNode.update_yaml() would write for these nodes now.

        ccm wrote a node's seeds into its scylla.yaml when the node was added and
        whenever its configuration was set, and never at start: a node keeps
        seeds that are down, or no longer seeds, until then.  The node starts
        with them (ScyllaNode.start()).
        """
        for node in nodes:
            node.ccm_seeds = self._ccm_seeds_of(node)

    def get_seeds(self) -> list[str]:
        """The recorded seed addresses (ccm's Cluster.get_seeds)."""

        return [s.address() if isinstance(s, ScyllaNode) else s for s in self.seeds]

    def set_partitioner(self, partitioner: str) -> ScyllaCluster:
        """Accept ccm's partitioner choice; there is nothing to configure here.

        ccm wrote a `partitioner:` key into scylla.yaml.  In this tree the
        option already defaults to Murmur3Partitioner (db/config.cc) and
        enable_deprecated_partitioners defaults to false, so Murmur3 is the
        only partitioner a node accepts.  Assert rather than silently ignore,
        so a caller asking for anything else fails loudly.
        """

        assert partitioner == "org.apache.cassandra.dht.Murmur3Partitioner", \
            f"scylla only supports Murmur3Partitioner, not {partitioner!r}"
        return self

    def _node_placements(self, nodes: int | list[int] | dict) -> list[dict[str, str]]:
        """Flatten a populate() topology into one {"dc", "rack"} per node.

        The order matches the order populate()'s batched path creates nodes in,
        which is the order ccm handed out initial tokens in.
        """

        match nodes:
            case int():
                return [{"dc": "dc1", "rack": f"rack{i + 1}"} for i in range(nodes)]
            case list():
                return [{"dc": f"dc{dc}", "rack": f"rack{i + 1}"}
                        for dc, n_nodes in enumerate(nodes, start=1)
                        for i in range(n_nodes)]
            case dict():
                placements = []
                for dc, dc_nodes in nodes.items():
                    if isinstance(dc_nodes, int):
                        placements += [{"dc": dc, "rack": f"rack{i + 1}"} for i in range(dc_nodes)]
                        continue
                    dc_nodes = _racks_of(dc_nodes, nodes)
                    for rack, rack_nodes in dc_nodes.items():
                        if not isinstance(rack_nodes, int):
                            raise RuntimeError(f"Unsupported topology specification: {nodes}")
                        placements += [{"dc": dc, "rack": rack} for _ in range(rack_nodes)]
                return placements
            case _:
                raise RuntimeError(f"Unsupported topology specification: {nodes}")

    def new_node(self,  # noqa: PLR0913
                 i: int,  # not used here: node names follow the order nodes are created
                 auto_bootstrap: bool = False,
                 debug: bool | None = None,  # not used in scylla-dtest
                 initial_token: str | None = None,
                 add_node: bool = True,
                 is_seed: bool = True,
                 data_center: str | None = None,
                 rack: str | None = None) -> ScyllaNode:
        """Add one more node to the cluster, stopped, and return it.

        `debug` opened a Java remote-debug port in ccm and means nothing to Scylla.
        With add_node=False ccm left the node out of the cluster until the test
        called add(): the node could run and join Scylla's cluster, but ccm's
        cluster did not list it and so did not stop or wait for it.  The harness
        registers every server it creates, so here the node is only kept off the
        cluster's node list until add().
        """

        assert initial_token is None, "argument `initial_token` is not supported"

        if self.ccm_parity:
            # ccm's Cluster.add(): a node named no datacenter takes the first
            # existing node's datacenter (and its rack, if it names none either).
            if data_center is None:
                for existing in self.nodelist():
                    if existing.data_center is not None:
                        data_center = existing.data_center
                        rack = rack if rack is not None else existing.rack
                        break
            node = self._add_ccm_node(data_center, rack, config=self._new_node_config, auto_bootstrap=auto_bootstrap,
                                      is_seed=is_seed and add_node)
            if not add_node:
                self._detached[node.name] = self._nodes.pop(node.name)
            return node

        if data_center and rack:
            placement = {"property_file": {"dc": data_center, "rack": rack}}
        else:
            # Without an explicit rack, let the harness pick one, in the given
            # datacenter or in the one the cluster already lives in.
            placement = {"auto_rack_dc": data_center or (self.nodelist()[0].data_center if self._nodes else "dc1")}

        self._add_nodes(self.manager.servers_add(
            servers_num=1,
            config=self._new_node_config,
            start=False,
            **placement,
        ))
        node = self.nodelist()[-1]
        node.placement_explicit = bool(data_center and rack)
        if not add_node:
            self._detached[node.name] = self._nodes.pop(node.name)
        return node

    def add(self,
            node: ScyllaNode,
            is_seed: bool,  # the manager picks seeds, see get_seeds()
            data_center: str | None = None,
            rack: str | None = None) -> ScyllaCluster:
        """Finish adding a node created with new_node(add_node=False): list it, place it."""

        if self._detached.pop(node.name, None) is not None:
            self._nodes[node.name] = node
        if data_center or rack:
            node.move_to(data_center=data_center or node.data_center, rack=rack or node.rack)
            node.placement_explicit = True
        if self.ccm_parity:
            if is_seed and node not in self.seeds:
                self.seeds.append(node)
            self._save_ccm_seeds([node])
        return self

    def clear(self) -> None:
        """Stop every node and wipe its data, as ccm's Cluster.clear() did."""

        self.stop()
        for node in self.nodelist():
            node.clear()

    def populate(self,
                 nodes: int | list[int] | dict,
                 tokens: list[str] | None = None,
                 use_vnodes: bool | None = None) -> ScyllaCluster:
        """Create the nodes of the given topology, stopped.

        use_vnodes=False is ccm's single-token layout: each node gets one
        balanced initial_token (ccmlib/cluster.py populate()).  Left out, nodes
        keep the harness's vnodes.
        """
        if self._config_options.get("alternator_enforce_authorization"):
            self.manager.auth_provider = PlainTextAuthProvider(username="cassandra", password="cassandra")
        version = self.version_description()

        if self.ccm_parity:
            return self._populate_as_ccm(nodes, tokens=tokens, use_vnodes=use_vnodes, version=version)

        if tokens is None and use_vnodes is False:
            tokens = self.balanced_tokens(len(self._node_placements(nodes)))

        if tokens is not None:
            # ccm gave node i the token tokens[i-1] as its initial_token
            # (ccmlib/cluster.py populate()).  servers_add() applies one config
            # to a whole batch, so a token per node means adding them one by one.
            placements = self._node_placements(nodes)
            assert len(tokens) == len(placements), \
                f"populate() got {len(tokens)} tokens for {len(placements)} nodes"
            for placement, token in zip(placements, tokens, strict=True):
                self._add_nodes([self.manager.server_add(
                    config=self._new_node_config | {"initial_token": token},
                    version=version,
                    property_file=placement,
                    start=False,
                )])
            return self

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
                        config=self._new_node_config,
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
                            config=self._new_node_config,
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

    def _populate_as_ccm(self, nodes: int | list[int] | dict, tokens: list[str] | None,
                         use_vnodes: bool | None, version: ScyllaVersionDescription) -> ScyllaCluster:
        """populate() as ccm's Cluster.populate() placed the nodes (ccmlib/cluster.py).

        A bare count means no datacenter at all: no cassandra-rackdc.properties and
        the shipped SimpleSnitch, so Scylla calls it datacenter1/rack1.  A list or
        dict names datacenters: GossipingPropertyFileSnitch for the whole cluster,
        and a rack that is not named is RAC1.
        """
        match nodes:
            case int():
                locations = [(None, None)] * nodes
            case list():
                locations = [(f"dc{i}", None) for i, n in enumerate(nodes, start=1) for _ in range(n)]
            case dict():
                locations = []
                for dc, x in nodes.items():
                    if isinstance(x, int):
                        locations += [(dc, None)] * x
                    elif isinstance(x, list):
                        locations += [(dc, f"RAC{i}") for i, n in enumerate(x, start=1) for _ in range(n)]
                    elif isinstance(x, dict):
                        locations += [(dc, rack) for rack, n in x.items() for _ in range(n)]
                    else:
                        raise RuntimeError(f"Unsupported topology specification: {nodes}")
            case _:
                raise RuntimeError(f"Unsupported topology specification: {nodes}")
        if not locations:
            raise RuntimeError(f"invalid topology {nodes}")
        if any(dc is not None for dc, _ in locations):
            self.set_configuration_options(values={"endpoint_snitch": CCM_SNITCH})
        # ccm's populate(use_vnodes=False) -- its default -- balanced one token per node;
        # with use_vnodes=True it left num_tokens alone.  dtest's options override both.
        self._ccm_use_vnodes = bool(use_vnodes)
        if tokens is None and not use_vnodes:
            tokens = self.balanced_tokens(len(locations))
        for i, (dc, rack) in enumerate(locations):
            config = self._new_node_config
            if tokens is not None and i < len(tokens):
                config = config | {"initial_token": tokens[i]}
            self._add_ccm_node(dc, rack, config=config, version=version)
        self._ccm_cluster_cleanup()
        return self

    def _ccm_cluster_cleanup(self) -> None:
        """ccm's Cluster.cluster_cleanup(), which its populate() ended with.

        If any node is already running (the test is growing a live cluster), run
        `nodetool cluster cleanup` on the first one, or, where that command does
        not exist, `nodetool cleanup` on every running node but the newest.
        """
        nodes = [node for node in self.nodelist() if node.is_running()]
        if not nodes:
            return
        try:
            nodes[0].nodetool("cluster cleanup")
        except NodetoolError:
            for node in nodes[:-1]:
                node.nodetool("cleanup")

    def _add_ccm_node(self, dc: str | None, rack: str | None, config: dict[str, Any],
                      version: ScyllaVersionDescription | None = None, auto_bootstrap: bool = False,
                      is_seed: bool = True) -> ScyllaNode:
        """Create one stopped node where ccm would have put it (see _populate_as_ccm())."""
        placement = {} if dc is None else {"property_file": {"dc": dc, "rack": rack or "RAC1"}}
        self._add_nodes([self.manager.server_add(config=config, version=version, start=False, **placement)],
                        auto_bootstrap=auto_bootstrap)
        node = self.nodelist()[-1]
        # ccm's Node.data_center/rack: what the test asked for, None when it named nothing.
        node.data_center, node.rack = dc, rack
        node.placement_explicit = dc is not None and rack is not None
        # ccm's new_node() -- populate() made every node that way -- added the node
        # to the seeds unless told otherwise.
        if is_seed:
            self.seeds.append(node)
        self._save_ccm_seeds([node])
        return node

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

        if self.ccm_parity:
            return self._start_nodes_as_ccm(nodes, no_wait=no_wait, wait_for_binary_proto=wait_for_binary_proto,
                                            wait_other_notice=wait_other_notice,
                                            wait_normal_token_owner=wait_normal_token_owner, jvm_args=jvm_args)

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

    def _start_nodes_as_ccm(self, nodes: list[ScyllaNode], no_wait: bool, wait_for_binary_proto: bool | None,
                            wait_other_notice: bool | None, wait_normal_token_owner: bool | None,
                            jvm_args: list[str] | None) -> list[ScyllaNode]:
        """ccm's ScyllaCluster.start_nodes() (ccmlib/scylla_cluster.py).

        A node is started only once the one before it logged that it is normal or
        serves CQL; each start waits for the nodes already up, but not for token
        ownership; and at the end every node -- those that were running too --
        waits in its log and over REST until it sees every started node alive,
        so a test's first session finds the cluster settled.
        """
        if wait_for_binary_proto is None:
            wait_for_binary_proto = self.force_wait_for_cluster_start
        if wait_other_notice is None:
            wait_other_notice = self.force_wait_for_cluster_start
        if wait_normal_token_owner is None and wait_other_notice:
            wait_normal_token_owner = True

        marks = [(node, node.mark_log()) for node in self.nodelist() if node.is_running()] if wait_other_notice else []
        started: list[tuple[ScyllaNode, int]] = []
        for node in nodes:
            if node.is_running():
                continue
            if started:
                last_node, last_mark = started[-1]
                last_node.watch_log_for("node is now in normal status|Starting listening for CQL clients",
                                        from_mark=last_mark)
            mark = node.mark_log() if os.path.exists(node.logfilename()) else 0
            node.start(no_wait=no_wait, wait_for_binary_proto=wait_for_binary_proto,
                       wait_other_notice=wait_other_notice, wait_normal_token_owner=False, jvm_args=jvm_args)
            started.append((node, mark))
            marks.append((node, mark))

        for node, _ in started:
            if not node.is_running():
                raise NodeError(f"Error starting {node.name}.")
        if wait_for_binary_proto:
            for node, mark in started:
                node.watch_log_for("Starting listening for CQL clients", from_mark=mark)
        if wait_other_notice:
            timeout = self.default_wait_other_notice_timeout
            for old_node, mark in marks:
                for node, _ in started:
                    if old_node is not node:
                        old_node.watch_log_for_alive(node, from_mark=mark, timeout=timeout)
                        old_node.watch_rest_for_alive(node, timeout=timeout, wait_normal_token_owner=wait_normal_token_owner)
        return [node for node, _ in started]

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

        started = self.start_nodes(
            no_wait=no_wait,
            wait_for_binary_proto=wait_for_binary_proto,
            wait_other_notice=wait_other_notice,
            wait_normal_token_owner=wait_normal_token_owner,
            jvm_args=jvm_args,
        )

        # The manager keeps its metadata in this cluster, so it can only start
        # once the nodes serve CQL.
        self.start_scylla_manager()

        return started

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
        if wait_other_notice and not other_nodes:
            # As ccm's stop_nodes(): the nodes that must notice are the ones not being stopped.
            # Left to each node, a node stopped alongside it would be waited on, and a node that
            # is shutting down never logs another as down.
            other_nodes = [node for node in self.nodelist() if node not in nodes]

        def stop(node: ScyllaNode) -> None:
            node.stop(
                wait=wait,
                wait_other_notice=wait_other_notice,
                other_nodes=other_nodes,
                gently=gently,
                wait_seconds=wait_seconds,
            )

        if self.ccm_parity and len(nodes) > 1:
            # ccm's ScyllaCluster.stop_nodes() signalled every node before waiting
            # for any ("stop all nodes in parallel"), so none of them watched the
            # others go down one by one.  A node's stop here blocks until the
            # process is gone, so the nodes are stopped from threads of their own.
            with ThreadPoolExecutor(max_workers=len(nodes)) as pool:
                for future in [pool.submit(stop, node) for node in nodes]:
                    future.result()
        else:
            for node in nodes:
                stop(node)

        return [node for node in nodes if not node.is_running()]

    def stop(self,
             wait: bool = True,
             gently: bool = True,
             wait_other_notice: bool = False,
             other_nodes: list[ScyllaNode] | None = None,
             wait_seconds: int | None = None) -> list[ScyllaNode]:
        self.stop_scylla_manager(gently=gently)
        return self.stop_nodes(
            wait=wait,
            gently=gently,
            wait_other_notice=wait_other_notice,
            other_nodes=other_nodes,
            wait_seconds=wait_seconds,
        )

    def remove(self,
               node: ScyllaNode | None = None,
               wait_other_notice: bool = False,
               other_nodes: list[ScyllaNode] | None = None,
               remove_node_dir: bool = True) -> None:
        """Kill a node and drop it from the cluster, or kill the whole cluster.

        Mirrors scylla-ccm's Cluster.remove(): the node is killed, not
        decommissioned, so the rest of the ring still knows about it and
        reports it as down.  `remove_node_dir` also deletes its work
        directory, which is what leaves a node looking wiped to a later
        replace; the replace tests pass False to keep it.
        """

        if node is None:
            node_path = self.get_path()
            self.stop(gently=False, wait_other_notice=wait_other_notice, other_nodes=other_nodes)
        else:
            # _nodes is keyed by node name, as ccm's Cluster.nodes was.
            if node.name not in self._nodes:
                return
            node_path = node.get_path()
            del self._nodes[node.name]
            if node in self.seeds:
                self.seeds.remove(node)
            node.stop(gently=False, wait_other_notice=wait_other_notice, other_nodes=other_nodes)

        if remove_node_dir:
            self.remove_dir_with_retry(node_path)

    def sctool(self, cmd: list[str]) -> tuple[str, str]:
        if self._scylla_manager is None:
            raise RuntimeError("scylla manager not enabled - sctool command cannot be executed")
        return self._scylla_manager.sctool(cmd)

    def start_scylla_manager(self) -> None:
        if not self._scylla_manager or self.skip_manager_server:
            return
        self._scylla_manager.start()

    def stop_scylla_manager(self, gently: bool = True) -> None:
        if not self._scylla_manager or self.skip_manager_server:
            return
        self._scylla_manager.stop(gently)

    def stress(self, stress_options: list[str], **kwargs):
        """Run `cassandra-stress` against the first running node."""

        for node in self.nodelist():
            if node.is_running():
                return node.stress(stress_options, **kwargs)
        raise RuntimeError("no running node to run cassandra-stress against")

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
                if self.ccm_parity:
                    # ccm deleted an option set to None (ccmlib/scylla_node.py update_yaml()),
                    # and only wrote the file: a running node reads it when a test sends it
                    # SIGHUP, or at its next start.
                    for key in [k for k, v in values.items() if v is None]:
                        self.manager.server_remove_config_option(server_id=node.server_id, key=key, reload=False)
                    set_values = {k: v for k, v in values.items() if v is not None}
                    if set_values:
                        self.manager.server_update_config(server_id=node.server_id, config_options=set_values,
                                                          reload=False)
                else:
                    self.manager.server_update_config(server_id=node.server_id, config_options=values)
        if self.ccm_parity:
            # ccm rewrote the scylla.yaml of every node it configured, seeds included.
            self._save_ccm_seeds(self.nodelist() if nodes is None else [nodes] if isinstance(nodes, ScyllaNode) else nodes)
        return self

    def enable_internode_ssl(self, node_ssl_path: str, internode_encryption: str = "all") -> ScyllaCluster:
        """Configure server_encryption_options from certs generated by tools.misc.generate_ssl_stores().

        Mirrors scylla-ccm's Cluster.enable_internode_ssl() (ccmlib/cluster.py), adapted to
        Scylla's server_encryption_options schema (certificate/keyfile/truststore, see
        conf/scylla.yaml) rather than the Cassandra-only jks keystore options.
        """
        # Like ccm, copy the key pair in as internode-*: tests wait for Scylla
        # to report reloading those files after they regenerate the certs.
        for name in ("ccm_node.pem", "ccm_node.key"):
            shutil.copyfile(os.path.join(node_ssl_path, name), os.path.join(self.get_path(), f"internode-{name}"))
        node_ssl_options = {
            "internode_encryption": internode_encryption,
            "certificate": os.path.join(self.get_path(), "internode-ccm_node.pem"),
            "keyfile": os.path.join(self.get_path(), "internode-ccm_node.key"),
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
