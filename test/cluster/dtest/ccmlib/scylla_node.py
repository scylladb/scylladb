#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

from __future__ import annotations

import argparse
import glob
import json
import locale
import logging
import os
import re
import shutil
import signal
import socket
import subprocess
import time
import uuid
from enum import Enum
from functools import cached_property
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiohttp
import requests
import yaml
from ruamel.yaml import YAML

from test import TOP_SRC_DIR
from test.cluster.dtest.ccmlib import ccm_parity as parity, scylla_repository
from test.cluster.dtest.ccmlib.common import (
    ArgumentError,
    wait_for,
    BIN_DIR,
    SCYLLAMANAGER_AGENT_CONF,
    SCYLLA_CONF_DIR,
    check_socket_listening,
    parse_interface,
)
from test.cluster.dtest.ccmlib.scylla_manager import (
    AGENT_API_PORT,
    AGENT_DEBUG_PORT,
    AGENT_PROMETHEUS_PORT,
)
from test.pylib.internal_types import ServerUpState
from test.pylib.rest_client import HTTPError

if TYPE_CHECKING:
    from test.pylib.internal_types import ServerInfo
    from test.pylib.log_browsing import ScyllaLogFile
    from test.cluster.dtest.ccmlib.scylla_cluster import ScyllaCluster


logger = logging.getLogger("scylla_node")


NODETOOL_STDERR_IGNORED_PATTERNS = (
    re.compile(r"WARNING: debug mode. Not for benchmarking or production"),
    re.compile(
        r"==[0-9]+==WARNING: ASan doesn't fully support makecontext/swapcontext"
        r" functions and may produce false positives in some cases!"
    ),
)

# An sstable file name, as ccm's ccmlib.node parses it, used to map a data file
# back to the sstable it belongs to.
_sstable_regexp = re.compile(
    r"((?P<keyspace>[^\s-]+)-(?P<cf>[^\s-]+)-)?(?P<tmp>tmp(link)?-)?(?P<version>[^\s-]+)"
    r"-(?P<identifier>[^-]+)-(?P<big>big-)?(?P<suffix>[a-zA-Z]+)\.[a-zA-Z0-9]+$"
)

CASSANDRA_OPTIONS_MAPPING = {
    "-Dcassandra.replace_address_first_boot": "--replace-address-first-boot",
}

DEFAULT_SMP = 2
DEFAULT_MEMORY_PER_CPU = 512 * 1024 * 1024  # bytes
DEFAULT_SCYLLA_LOG_LEVEL = "info"

# Scylla's REST API port; the manager agent reads the node's configuration through it.
SCYLLA_API_PORT = 10000
AGENT_START_TIMEOUT = 180

# The real cqlsh binary, as shipped by this repo (a thin wrapper around
# tools/cqlsh/bin/cqlsh.py). Used by ScyllaNode.run_cqlsh() below.
CQLSH_BIN = TOP_SRC_DIR / BIN_DIR / "cqlsh"

# scylla.yaml options that belong to one node rather than to the cluster: the
# cluster manager derives them from the node's address and work directory, so
# update_yaml() must not take them from another node's copy of the file.
NODE_SPECIFIC_CONFIG_OPTIONS = frozenset({
    "workdir",
    "workdir,W",  # ccm's spelling of it, which ccm parity writes (ccm_parity.py)
    "maintenance_socket",
    "api_doc_dir",
    "cluster_name",
    "listen_address",
    "rpc_address",
    "api_address",
    "prometheus_address",
    "alternator_address",
    "broadcast_address",
    "broadcast_rpc_address",
    "seed_provider",
    "data_file_directories",
    "commitlog_directory",
    "hints_directory",
    "view_hints_directory",
    "saved_caches_directory",
    "replace_address_first_boot",
    "replace_node_first_boot",
    "ignore_dead_nodes_for_replace",
    # Per-node in ccm too: its update_yaml() writes the node's own flag and token, so a
    # node added to a running cluster keeps auto_bootstrap: true over a copied file's false.
    "auto_bootstrap",
    "initial_token",
})

KNOWN_LOG_LEVELS = {
    "TRACE": "trace",
    "DEBUG": "debug",
    "INFO": "info",
    "WARN": "warn",
    "ERROR": "error",
    "OFF": "info",
}


class Status:
    """ccm's ccmlib.node.Status.  Kept here, next to the node, because
    ccmlib.node imports from this module and cannot be imported back."""

    UNINITIALIZED = "UNINITIALIZED"
    UP = "UP"
    DOWN = "DOWN"
    DECOMMISSIONED = "DECOMMISSIONED"


class NodeError(Exception):
    def __init__(self, msg: str, process: int | None = None):
        super().__init__(msg)
        self.process = process


class NodeUpgradeError(Exception):
    ...


class ToolError(Exception):
    def __init__(self, command: str | list[str], exit_status: int, stdout: Any = None, stderr: Any = None):
        self.command = command
        self.exit_status = exit_status
        self.stdout = stdout
        self.stderr = stderr

        message = [f"Subprocess {command} exited with non-zero status; exit status: {exit_status}"]
        if stdout:
            message.append(f"stdout: {self._decode(stdout)}")
        if stderr:
            message.append(f"stderr: {self._decode(stderr)}")

        Exception.__init__(self, "; \n".join(message))

    @staticmethod
    def _decode(value: str | bytes) -> str:
        if isinstance(value, bytes):
            return bytes.decode(value, locale.getpreferredencoding(do_setlocale=False))
        return value


NodetoolError = ToolError

# `nodetool compactionstats` lines ccm's Node._parse_tasks() counted (ccmlib/node.py).
_CCM_PENDING_TASKS = re.compile(r"- (?P<ks>\w+)\.(?P<cf>\w+): (?P<tasks>\d+)")
_CCM_ACTIVE_TASKS = re.compile(r"\s*([\w-]+)\s+\w+\s+(?P<ks>\w+)\s+(?P<cf>\w+)\s+\d+\s+\d+\s+\w+\s+\d+\.\d+%")


# Restored verbatim (imports aside) from ccm's ccmlib/scylla_node.py, since
# not-yet-adapted dtest/unported test modules (via tools.data) import it.
class ScyllaType:
    """
    Helper class for defining Scylla type definitions using type strings.

    Refer to the Cassandra class names found at
    https://github.com/scylladb/scylladb/blob/master/docs/dev/cql3-type-mapping.md for valid type strings.

    Examples :
        type1 = ScyllaType.make_partition_key("Int32Type")
        type2 = ScyllaType.make_clustering_key("Int32Type", "FloatType")
    """
    class TypeKind(Enum):
        REGULAR = 1
        CLUSTERING_KEY = 2
        PARTITION_KEY = 3

    kind: TypeKind
    types: Iterable[str]

    def __init__(self, kind: TypeKind, types: Iterable[str]):
        if not types:
            raise ArgumentError("Please pass at least one type to create ScyllaType")
        self.kind = kind
        self.types = types

    @classmethod
    def make_regular(cls, keytype: str):
        return cls(ScyllaType.TypeKind.REGULAR, (keytype,))

    @classmethod
    def make_clustering_key(cls, *keytypes):
        return cls(ScyllaType.TypeKind.CLUSTERING_KEY, keytypes)

    @classmethod
    def make_partition_key(cls, *keytypes):
        return cls(ScyllaType.TypeKind.PARTITION_KEY, keytypes)

    def as_types_args(self) -> list[str]:
        """Return the type definition as arguments to scylla types command"""
        args = []
        if self.kind == ScyllaType.TypeKind.CLUSTERING_KEY:
            args.append("--prefix-compound")
        elif self.kind == ScyllaType.TypeKind.PARTITION_KEY:
            args.append("--full-compound")
        for keytype in self.types:
            args.extend(["-t", keytype])
        return args


# The name ccm gave a Scylla node's log; the in-tree node has that one log only,
# so tests that ask for it by name get it.
SCYLLA_LOG_FILENAME = "system.log"


def _set_stress_val(key: str, val: str, res: dict[str, float]) -> None:
    """Parse one "key : value" line of cassandra-stress's summary (ccm's Node._set_stress_val())."""

    def parse_num(s: str) -> float:
        return float(s.replace(",", ""))

    if "[" in val:
        if m := re.match(r"^\s*([\d\.\,]+\d?)\s*\[.*", val):
            res[key] = parse_num(m.group(1))
        if m := re.match(r"^.*READ:\s*([\d\.\,]+\d?)[^\d].*", val):
            res[key + ":read"] = parse_num(m.group(1))
        if m := re.match(r".*WRITE:\s*([\d\.\,]+\d?)[^\d].*", val):
            res[key + ":write"] = parse_num(m.group(1))
    else:
        try:
            res[key] = parse_num(val)
        except ValueError:
            res[key] = val


class ScyllaNode:
    def __init__(self, cluster: ScyllaCluster, server: ServerInfo, name: str):
        self.cluster = cluster
        self.server_id = server.server_id
        self.name = name
        self.network_interfaces = {
            "storage": (str(server.rpc_address), 7000),
            "binary": (str(server.rpc_address), 9042),
        }
        self.data_center = server.datacenter
        self.rack = server.rack
        # Whether the test chose this node's dc and rack; see _follow_replaced_node().
        self.placement_explicit = False

        # Every pid this node has run under, as ccm's Node.all_pids accumulated
        # them; copy_logs() matches core files against it.
        self._all_pids: list[int] = []

        self._smp_set_during_test = None
        # ccm's ScyllaNode started at a real smp (2); tests read the attribute
        # directly (cleanup_test's expected_cleanups), so it must not be None.
        self._smp = DEFAULT_SMP
        self._memory = None

        # ccm's Node kept a DECOMMISSIONED status; the manager only knows
        # whether a process is up, so the shim remembers it here.
        self._decommissioned = False

        self.__global_log_level = "info"
        self.__classes_log_level = {}

        self.bootstrap = True

        self._hostid = None
        # Under ccm parity, the seeds ccm would have written into this node's
        # scylla.yaml (ScyllaCluster._save_ccm_seeds()).
        self.ccm_seeds: list[str] | None = None
        # Exit status of the process stop() ended, which the manager does not keep.
        self._stop_returncode: int | None = None

        # Scylla's REST API port; every node in this tree uses the default.
        self.api_port = 10000

        # Version switching.  `_node_scylla_version` caches what `scylla
        # --version` said about the executable this node currently runs; the
        # upgrader clears it when it points the node at another one.
        self._node_scylla_version = None
        self.upgraded = False
        self.upgrader = NodeUpgrader(node=self)

        # The Scylla Manager agent that runs beside this node, when the cluster
        # was given a manager. It is started and stopped together with the node.
        self.scylla_manager = cluster._scylla_manager
        self._process_agent = None

    def set_configuration_options(self,
                                  values: dict | None = None,
                                  batch_commitlog: bool | None = None) -> None:
        """Set DB node configuration options.

        Example:
            node.set_configuration_options(values={
                'hinted_handoff_enabled' : True,
                'concurrent_writes' : 64,
            })

        The batch_commitlog option gives an easier way to switch to batch
        commitlog (since it requires setting 2 options and unsetting one).
        """
        self.cluster.set_configuration_options(values=values, batch_commitlog=batch_commitlog, nodes=self)

    def set_log_level(self, new_level: str, class_name: str | None = None) -> ScyllaNode:
        if new_log_level := KNOWN_LOG_LEVELS.get(new_level):
            if class_name is None:
                self.__global_log_level = new_log_level
            else:
                self.__classes_log_level[class_name] = new_log_level
            return self
        raise ArgumentError(f"Unknown log level {new_level} (use one of {' '.join(KNOWN_LOG_LEVELS)})")

    def scylla_mode(self) -> str:
        return self.cluster.scylla_mode

    @property
    def pid(self) -> int | None:
        """OS pid of this node's scylla process, or None when it is not running.

        ccm kept the pid from the node's pid file and cleared it on a confirmed
        stop; here it comes from the manager's ServerInfo.  It must not be a
        plain attribute: tests hand it to psutil.Process(), and psutil.Process(None)
        silently wraps the *test runner's own* process instead of the node's.
        """

        for server in self.cluster.manager.running_servers():
            if server.server_id == self.server_id:
                return server.pid
        return None

    @property
    def all_pids(self) -> list[int]:
        """Every pid this node has run under, newest last (ccm's Node.all_pids)."""

        return list(self._all_pids)

    def set_smp(self, smp: int) -> None:
        logger.debug(f"Setting smp: {self=} {smp=}")
        self._smp_set_during_test = smp

    def smp(self) -> int:
        logger.debug(f"Getting smp: {self=} _smp_set_during_test={self._smp_set_during_test} _smp={self._smp} {DEFAULT_SMP=}")
        return self._smp_set_during_test or self._smp or DEFAULT_SMP

    def memory(self) -> int:
        return self._memory or self.smp() * DEFAULT_MEMORY_PER_CPU

    def _adjust_smp_and_memory(self, smp: int | None = None, memory: int | None = None) -> None:
        if not memory and not smp:
            return
        logger.debug(f"Adjusting smp={smp} memory={memory} current_smp={self._smp} current_memory={self._memory}")
        if memory:
            self._memory = memory // (smp or self.smp()) * self.smp()
        if smp:
            memory_per_cpu = self.memory() // self.smp()
            self._smp = smp
            self._memory = memory_per_cpu * self.smp()

    def set_mem_mb_per_cpu(self, mem: int) -> None:  # not used in scylla-dtest
        raise NotImplementedError("setting memory per CPU during a test is not supported")

    def address(self) -> str:
        """Return the IP use by this node for internal communication."""

        return self.network_interfaces["storage"][0]

    def change_ip(self) -> str:
        """Give this node a new IP address. The node must be stopped.

        scylla-ccm lets a test pick the address itself, by assigning
        node.network_interfaces and rewriting listen_address. Here the cluster
        manager owns the address pool, so it leases the next free address and
        rewrites the node's scylla.yaml (listen_address, rpc_address,
        api_address, prometheus_address, alternator_address); the caller gets
        back the address it was given. The old address is held to the end of
        the test so it is not recycled inside the same cluster.
        """
        if self.is_running():
            raise NodeError(f"Can't change the IP of a running node {self.name}; stop it first")
        new_ip = str(self.cluster.manager.server_change_ip(server_id=self.server_id))
        if self.cluster.ccm_parity:
            # The test's set_configuration_options(listen_address=...) made ccm rewrite the
            # node's seeds, before it reassigned network_interfaces: its own old address,
            # and the new ones of the nodes moved before it.
            self.cluster._save_ccm_seeds([self])  # noqa: SLF001
        # server_change_ip() rewrites rpc_address too, so both interfaces move.
        self.network_interfaces = {name: (new_ip, port) for name, (_, port) in self.network_interfaces.items()}
        logger.debug(f"Changed IP of {self.name} to {new_ip}")
        return new_ip

    def change_rpc_address(self) -> str:
        """Give this node a new CQL (rpc_address) address. The node must be stopped."""
        if self.is_running():
            raise NodeError(f"Can't change the RPC address of a running node {self.name}; stop it first")
        new_ip = str(self.cluster.manager.server_change_rpc_address(server_id=self.server_id))
        _, port = self.network_interfaces["binary"]
        self.network_interfaces["binary"] = (new_ip, port)
        logger.debug(f"Changed RPC address of {self.name} to {new_ip}")
        return new_ip

    def is_running(self) -> bool:
        return self.cluster.manager.server_is_alive(server_id=self.server_id)

    def show(self, only_status: bool = False, show_cluster: bool = True) -> str:
        """This node's configuration, which ccm printed; returned here, as tests log it."""
        pid = self.pid
        lines = [f"{self.name}: {'UP' if pid else 'DOWN'}"]
        if not only_status:
            if show_cluster:
                lines.append(f"  cluster={self.cluster.manager.cluster.name}")
            lines.append(f"  auto_bootstrap={self.bootstrap}")
            lines += [f"  {name}={interface}" for name, interface in self.network_interfaces.items()]
            if pid:
                lines.append(f"  pid={pid}")
        return "\n".join(lines)

    def is_live(self) -> bool:
        """ccm's Node.is_live(): running and not decommissioned.

        A decommissioned node that is still running has left the cluster, so the
        other nodes never see a new node join; start(wait_other_notice=True) must
        not wait for it.
        """
        return self.is_running() and not self._decommissioned

    @cached_property
    def scylla_log_file(self) -> ScyllaLogFile:
        return self.cluster.manager.server_open_log(server_id=self.server_id)

    def grep_log(self,
                 expr: str,
                 filter_expr: str | None = None,
                 filename: str | None = None,  # not used in scylla-dtest
                 from_mark: int | None = None) -> list[tuple[str, re.Match[str]]]:
        assert filename in (None, SCYLLA_LOG_FILENAME), "only ScyllaDB's log is supported"

        return self.scylla_log_file.grep(expr=expr, filter_expr=filter_expr, from_mark=from_mark)

    def grep_log_for_errors(self,
                            filename: str | None = None,  # not used in scylla-dtest
                            distinct_errors: bool = False,
                            search_str: str | None = None,  # not used in scylla-dtest
                            case_sensitive: bool = True,  # not used in scylla-dtest
                            from_mark: int | None = None) -> list[str] | list[list[str]]:
        assert filename in (None, SCYLLA_LOG_FILENAME), "only ScyllaDB's log is supported"
        assert search_str is None, "argument `search_str` is not supported"
        assert case_sensitive, "only case sensitive search is supported"

        from_mark = getattr(self, "error_mark", None) if from_mark is None else from_mark

        return self.scylla_log_file.grep_for_errors(distinct_errors=distinct_errors, from_mark=from_mark)

    def mark_log_for_errors(self, filename: str | None = None) -> None:
        assert filename in (None, SCYLLA_LOG_FILENAME), "only ScyllaDB's log is supported"

        self.error_mark = self.mark_log()

    def mark_log(self, filename: str | None = None) -> int:
        assert filename in (None, SCYLLA_LOG_FILENAME), "only ScyllaDB's log is supported"

        return self.scylla_log_file.mark()

    def watch_log_for(self,
                      exprs: str | list[str],
                      from_mark: int | None = None,
                      timeout: float = 600,
                      process: subprocess.Popen | None = None,  # don't use it here
                      verbose: bool | None = None,  # not used in scylla-dtest
                      filename: str | None = None,  # not used in scylla-dtest
                      polling_interval: float | None = None) -> tuple[str, re.Match[str]] | list[tuple[str, re.Match[str]]]:  # not used in scylla-dtest
        assert process is None, "argument `process` is not supported"
        assert verbose is None, "argument `verbose` is not supported"
        assert filename in (None, SCYLLA_LOG_FILENAME), "only ScyllaDB's log is supported"
        assert polling_interval is None, "argument `polling_interval` is not supported"

        if isinstance(exprs, str):
            exprs = [exprs]

        # ccm reads the log up to its current end before it looks at the deadline, so lines which are already
        # logged match even with `timeout=0` (tests use that to assert that something has been logged).
        # ScyllaLogFile.wait_for() arms its timeout before the first read, so scan the existing lines here and
        # wait only for the patterns still missing.
        patterns = [re.compile(expr) for expr in exprs]
        matches = []
        with open(self.scylla_log_file.file, "rb") as log_file:
            if from_mark is not None:
                log_file.seek(from_mark)
            while patterns and (raw_line := log_file.readline()).endswith(b"\n"):
                line = raw_line.decode("utf-8", errors="replace")
                # As in ccm, a line satisfies one copy of a repeated pattern
                # (callers pass e.g. [pattern] * smp to want one line per shard).
                matched_here = set()
                for pattern in patterns.copy():
                    if pattern.pattern in matched_here:
                        continue
                    if match := pattern.search(line):
                        matches.append((line, match))
                        patterns.remove(pattern)
                        matched_here.add(pattern.pattern)
                from_mark = log_file.tell()
        if patterns:
            try:
                _, more_matches = self.scylla_log_file.wait_for(*patterns, from_mark=from_mark, timeout=timeout)
            except TimeoutError:
                # ccm's message: tests check which patterns it reports missing.
                missing = [p.pattern for p in patterns]
                raise TimeoutError(f"{time.strftime('%d %b %Y %H:%M:%S', time.gmtime())} [{self.name}] Missing: {missing} "
                                   f"not found in {self.logfilename()}") from None
            matches.extend(more_matches)

        return matches[0] if len(matches) == 1 else matches

    def watch_log_for_death(self,
                            nodes: ScyllaNode | list[ScyllaNode],
                            from_mark: int | None = None,
                            timeout: float = 600,
                            filename: str | None = None) -> None:
        """Watch the log of this node until it detects that the provided other nodes are marked dead.

        This method returns nothing but throw a TimeoutError if all the requested node have not been found
        to be marked dead before timeout sec.

        A mark as returned by mark_log() can be used as the `from_mark` parameter to start watching the log
        from a given position. Otherwise, the log is watched from the beginning.
        """
        assert filename in (None, SCYLLA_LOG_FILENAME), "only ScyllaDB's log is supported"

        if not isinstance(nodes, list):
            nodes = [nodes]

        self.watch_log_for(
            [f"({_node_id_alternatives(node)}).* now (dead|DOWN)" for node in nodes],
            from_mark=from_mark,
            timeout=timeout,
        )

    def watch_log_for_alive(self,
                            nodes: ScyllaNode | list[ScyllaNode],
                            from_mark: int | None = None,
                            timeout: float = 120,
                            filename: str | None = None) -> None:
        """Watch the log of this node until it detects that the provided other nodes are marked UP.

        This method works similarly to watch_log_for_death().
        """
        assert filename in (None, SCYLLA_LOG_FILENAME), "only ScyllaDB's log is supported"

        if not isinstance(nodes, list):
            nodes = [nodes]

        self.watch_log_for(
            [f"({_node_id_alternatives(node)}).* now UP" for node in nodes],
            from_mark=from_mark,
            timeout=timeout,
        )

    def watch_rest_for_alive(self,
                             nodes: ScyllaNode | list[ScyllaNode],
                             timeout: float = 120,
                             wait_normal_token_owner: bool = True) -> None:
        nodes = nodes if isinstance(nodes, list) else [nodes]
        tofind_host_id_map = {node.address(): node.hostid() for node in nodes}
        tofind = {node.address() for node in nodes}
        node_ip = self.address()

        found = set()
        found_host_id_map = {}

        api = self.cluster.manager.api

        deadline = time.perf_counter() + timeout
        while time.perf_counter() < deadline:
            if tofind <= set(api.get_alive_endpoints(node_ip=node_ip)) - set(api.get_joining_nodes(node_ip=node_ip)):
                if not any(node for node in tofind if not api.get_tokens(node_ip=node_ip, endpoint=node)):
                    if not wait_normal_token_owner:
                        return

                    # Verify other nodes are considered normal token owners on this node and their host_ids
                    # match the host_ids the client knows about.
                    host_id_map = {x["key"]: x["value"] for x in api.get_host_id_map(dst_server_ip=node_ip)}
                    found_host_id_map.update(host_id_map)
                    for addr, host_id in host_id_map.items():
                        if addr not in tofind_host_id_map:
                            continue
                        if host_id == tofind_host_id_map[addr] or not tofind_host_id_map[addr]:
                            tofind.discard(addr)
                            found.add(addr)

                    if not tofind:
                        return
            time.sleep(0.1)

        self.debug(f"watch_rest_for_alive: {tofind=} {found=}: {tofind_host_id_map=} {found_host_id_map=}")
        raise TimeoutError(f"watch_rest_for_alive() timeout after {timeout} seconds")

    def _check_binary_socket_listening(self, timeout: float = 10) -> None:
        """ccm's second step of wait_for_binary_interface(): connect to the CQL port.

        ccm (ccmlib/node.py, common.check_socket_listening()) opened and closed a
        plain TCP connection every 0.2 s until one succeeded, and only warned if
        none did in 10 s.  The server sees a connection that sends no frame.
        """
        itf = (self.address(), parity.CCM_NATIVE_TRANSPORT_PORT)
        deadline = time.perf_counter() + timeout
        while time.perf_counter() < deadline:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                try:
                    sock.connect(itf)
                    return
                except OSError:
                    time.sleep(0.2)
        logger.warning(f"Binary interface {itf[0]}:{itf[1]} is not listening after {timeout} seconds, "
                       "node may have failed to start.")

    def wait_for_binary_interface(self,
                                  from_mark: int | None = None,
                                  timeout: float | None = None,
                                  process: Any = None) -> None:  # ccm's process handle; the process is watched below
        """Waits for the binary CQL interface to be listening."""

        if timeout is None:
            timeout = self.cluster.default_wait_for_binary_proto

        # As ccm's wait_for_starting() does, give up as soon as the process exits
        # (e.g. a replace Scylla rejects at startup) instead of waiting out the
        # whole timeout, and report it with ccm's message, which tests match on.
        deadline = time.perf_counter() + timeout
        while True:
            try:
                self.watch_log_for(exprs="Starting listening for CQL clients", from_mark=from_mark,
                                   timeout=max(0.1, min(2.0, deadline - time.perf_counter())))
                if self.cluster.ccm_parity:
                    self._check_binary_socket_listening()
                return
            except TimeoutError:
                if not self.is_running():
                    cmd = self.cluster.manager.cluster.servers[self.server_id].cmd
                    # stop() drops the process handle; it recorded what it did instead.
                    returncode = cmd.returncode if cmd is not None else self._stop_returncode
                    if returncode == 0:
                        # ccm's watch_log_for(process=) returns quietly on a clean
                        # exit, e.g. a banned node's _exit(0).
                        return
                    raise RuntimeError(f"The process is dead, returncode={returncode}") from None
                if time.perf_counter() >= deadline:
                    raise

    def wait_until_stopped(self,
                           wait_seconds: int | None = None,
                           marks: list[tuple[ScyllaNode, int]] | None = None,
                           dump_core: bool = True) -> None:  # not implemented
        if wait_seconds is None:
            wait_seconds = 127 if self.scylla_mode() != "debug" else 600
        if not wait_for(func=lambda: not self.is_running(), timeout=wait_seconds):
            raise NodeError(f"Problem stopping node {self.name}")

        for node, mark in marks or []:
            if node.server_id != self.server_id:
                node.watch_log_for_death(nodes=self, from_mark=mark)

    def _process_scylla_args(self, *args: str) -> list[str]:
        # Parse default overrides in SCYLLA_EXT_OPTS
        scylla_args = _parse_scylla_args(os.environ.get("SCYLLA_EXT_OPTS", "").split())

        if smp := scylla_args.pop("--smp", None):
            smp = int(smp[0])
        if memory := scylla_args.pop("--memory", None):
            memory = _parse_size(memory[0])
        self._adjust_smp_and_memory(smp=smp, memory=memory)

        if args:
            parsed_args = []
            for arg in args:
                option, *value = arg.split("=")
                if not value or option not in CASSANDRA_OPTIONS_MAPPING:
                    parsed_args.append(arg)
                elif len(value) == 1:
                    scylla_args[CASSANDRA_OPTIONS_MAPPING[option]] = value
                else:
                    raise RuntimeError(f"Option {arg} not in form '-Dcassandra.foo=bar'. Please check your test")
            scylla_args.update(_parse_scylla_args(parsed_args))
            if smp := scylla_args.pop("--smp", None):
                self._adjust_smp_and_memory(smp=int(smp[0]))
            if memory := scylla_args.pop("--memory", None):
                self._memory = _parse_size(memory[0])

        default_scylla_args = {
            "--smp": [str(self.smp())],
            "--memory": [f"{self.memory() // 1024 ** 2}M"],
            "--developer-mode": ["true"],
            "--default-log-level": [self.__global_log_level],
            "--kernel-page-cache": ["1"],
            "--commitlog-use-o-dsync": ["0"],
            "--max-networking-io-control-blocks": ["1000"],
            "--unsafe-bypass-fsync": ["1"],
        }
        if self.cluster.ccm_parity:
            # ccm's command line (ccmlib/scylla_node.py _start_scylla()): the vnode
            # count comes from scylla.yaml, which dtest's options set; a test that
            # changes cluster.num_tokens still gets it.
            default_scylla_args["--log-to-stdout"] = ["1"]
            # ccm named the config file and gave the addresses on the command line;
            # test.py leaves Scylla to find conf/scylla.yaml from its working directory.
            default_scylla_args["--options-file"] = [os.path.join(self.get_conf_dir(), "scylla.yaml")]
            default_scylla_args["--api-address"] = [self.address()]
            default_scylla_args["--prometheus-address"] = [self.address()]
            if self.cluster.num_tokens != parity.UPSTREAM_NUM_TOKENS:
                default_scylla_args["--num-tokens"] = [self._num_tokens()]
        else:
            default_scylla_args["--num-tokens"] = [self._num_tokens()]

        if self.scylla_mode() == "debug":
            default_scylla_args["--blocked-reactor-notify-ms"] = ["5000"]

        scylla_args = default_scylla_args | scylla_args

        if "--cpuset" not in scylla_args:
            scylla_args["--overprovisioned"] = [""]

        return list(chain.from_iterable(
            (arg, value) if values and all(values) else (arg, )
            for arg, values in scylla_args.items()
            for value in values
        ))

    def _num_tokens(self) -> str:
        """The cluster's vnode count (ScyllaCluster.num_tokens), unless scylla.yaml pins the node's tokens.

        The command line beats scylla.yaml, and Scylla refuses to start when
        initial_token lists a different number of tokens than num_tokens asks for.
        """
        if initial_token := self.get_configuration_options().get("initial_token"):
            return str(len(str(initial_token).split(",")))
        return str(self.cluster.num_tokens)

    @staticmethod
    def _process_scylla_env() -> dict[str, str]:
        scylla_env = {}

        for var in os.environ.get("SCYLLA_EXT_ENV", "").replace(";" , " ").split():
            k, v = var.split(sep="=", maxsplit=1)
            if not v:
                raise RuntimeError(f"SCYLLA_EXT_ENV: unable to parse {var!r} as an env variable")
            scylla_env[k] = v

        return scylla_env

    def start(self,
              join_ring: bool | None = None,  # not used in scylla-dtest
              no_wait: bool = False,
              verbose: bool | None = None,  # not used in scylla-dtest
              update_pid: bool = True,  # not used here
              wait_other_notice: bool | None = None,
              wait_normal_token_owner: bool | None = None,
              replace_token: str | None = None,  # not used in scylla-dtest
              replace_address: str | None = None,
              replace_node_host_id: str | None = None,
              jvm_args: list[str] | None = None,
              wait_for_binary_proto: bool | None = None,
              profile_options: dict[str, str] | None = None,  # not used in scylla-dtest
              use_jna: bool | None = None,  # not used in scylla-dtest
              quiet_start: bool | None = None,  # not used in scylla-dtest
              expected_error: str | None = None) -> None:
        del join_ring  # ccm's ScyllaNode.start() ignores it as well
        assert verbose is None, "argument `verbose` is not supported"
        assert replace_token is None, "argument `replace_token` is not supported"
        assert profile_options is None, "argument `profile_options` is not supported"
        assert use_jna is None, "argument `use_jna` is not supported"
        assert quiet_start is None, "argument `quiet_start` is not supported"

        assert replace_address is None or replace_node_host_id is None, \
            "replace_address and replace_node_host_id cannot be specified together"

        if self.is_running():
            raise NodeError(f"{self.name} is already running")

        if replace_address or replace_node_host_id:
            self._follow_replaced_node(host_id=replace_node_host_id, address=replace_address)

        scylla_args = self._process_scylla_args(
            *(jvm_args or []),
            *(["--replace-address", replace_address] if replace_address else []),
            *(["--replace-node-first-boot", replace_node_host_id] if replace_node_host_id else []),
        )
        scylla_env = self._process_scylla_env()
        if self.cluster.ccm_parity:
            # ccm ran every node with SCYLLA_HOME set to the node's directory.
            scylla_env.setdefault("SCYLLA_HOME", self.get_path())

        # The cluster-wide default has to be applied before the marks are taken:
        # left to after them, a start() that names no wait_other_notice took no
        # marks and then waited for nobody.
        if wait_other_notice is None:
            wait_other_notice = self.cluster.force_wait_for_cluster_start and not no_wait
        marks = []
        if wait_other_notice:
            marks = [(node, node.mark_log()) for node in self.cluster.nodelist() if node.is_live()]

        self.mark = self.mark_log()

        logger.debug(f"Starting server: server_id={self.server_id} {scylla_args=} {scylla_env=}")

        # The manager caches a server's host id for good, but a node that was
        # wiped comes back with a new one.
        vars(self.cluster.manager.cluster.servers[self.server_id]).pop("_host_id", None)
        self._stop_returncode = None

        # A node the test expects to be rejected has to be started through the
        # manager's expected_error path: it watches for the message, files the
        # server as stopped, and so a later stop() is a no-op instead of
        # raising over the non-zero exit the test asked for.  PROCESS_STARTED
        # would return before any of that happens, and so would any lower
        # state than SERVING: the REST API answers the host id query before
        # the join is rejected.  connect_driver=False would cap the state at
        # HOST_ID_QUERIED; with an expected error the manager never connects.
        self.cluster.manager.server_start(
            server_id=self.server_id,
            # ccm made a node added with auto_bootstrap=False a seed, but its seed
            # list still held the seeds added before it (ccm Cluster.get_seeds()),
            # so it joined the existing cluster.  Seeding it with only itself
            # would make it form a cluster of its own.  Let the manager pick the
            # running nodes, as for any other node -- unless the test took the
            # seeds out of the node's scylla.yaml, see _seeds_left_on_disk().
            # Under ccm parity the node starts with the seeds ccm wrote for it,
            # down ones included.
            seeds=self._seeds_left_on_disk() or (self.ccm_seeds if self.cluster.ccm_parity else None),
            expected_error=expected_error,
            expected_server_up_state=(ServerUpState.SERVING if expected_error
                                      else ServerUpState.PROCESS_STARTED),
            cmdline_options_override=scylla_args,
            append_env_override=scylla_env,
            connect_driver=expected_error is not None,
        )

        # The host id is fetched once and cached, but a node that was wiped and
        # restarted comes back with a new one, and watch_rest_for_alive() then
        # waits for an id that will never appear.  ccm reset it on every start.
        self._hostid = None
        # A started node is not decommissioned any more (ccm's start() set its
        # status to UP): tests wipe a decommissioned node and bootstrap it again.
        self._decommissioned = False

        if (pid := self.pid) is not None and pid not in self._all_pids:
            self._all_pids.append(pid)

        if self.scylla_manager and self.scylla_manager.is_agent_available:
            self.start_scylla_manager_agent()

        if wait_for_binary_proto is None:
            wait_for_binary_proto = self.cluster.force_wait_for_cluster_start and not no_wait
        if wait_normal_token_owner is None and wait_other_notice:
            wait_normal_token_owner = True

        if wait_for_binary_proto:
            self.wait_for_binary_interface(from_mark=self.mark)

        if wait_other_notice:
            timeout = self.cluster.default_wait_other_notice_timeout
            for node, mark in marks:
                node.watch_log_for_alive(nodes=self, from_mark=mark, timeout=timeout)
                node.watch_rest_for_alive(nodes=self, timeout=timeout, wait_normal_token_owner=wait_normal_token_owner)
                self.watch_rest_for_alive(nodes=node, timeout=timeout, wait_normal_token_owner=wait_normal_token_owner)

    def stop(self,
             wait: bool = True,
             wait_other_notice: bool = False,
             other_nodes: list[ScyllaNode] | None = None,
             gently: bool = True,
             wait_seconds: int = 127,
             marks: list[int] | None = None) -> bool:
        self.stop_scylla_manager_agent(gently=gently)

        if not self.is_running():
            return False

        if wait_other_notice:
            # Scylla names the node by host id in its "is now DOWN" line, and the
            # host id can only be read from a running node, so read it now.
            self.hostid()

        if marks is None:
            marks = [
                (node, node.mark_log())
                for node in (self.cluster.nodelist() if other_nodes is None else other_nodes)
                if node.server_id != self.server_id and node.is_live()
            ] if wait_other_notice else []

        if gently:
            self._stop_returncode = 0
            try:
                self.cluster.manager.server_stop_gracefully(server_id=self.server_id)
            except RuntimeError as exc:
                # ccm's stop() never looked at the exit status, and a node
                # stopped before it finished starting exits non-zero ("Startup
                # failed", or -15 before Scylla handles SIGTERM).  Let the
                # manager file the server as stopped.
                if "exited with non-zero exit code" not in str(exc):
                    raise
                self._stop_returncode = None
                self.cluster.manager.server_stop(server_id=self.server_id, convict=False)
        else:
            self._stop_returncode = -signal.SIGKILL
            self.cluster.manager.server_stop(server_id=self.server_id, convict=False)

        if wait or wait_other_notice:
            self.wait_until_stopped(wait_seconds=wait_seconds, marks=marks, dump_core=gently)

        return True

    def nodetool(self,
                 cmd: str,
                 capture_output: bool = True,
                 wait: bool = True,
                 timeout: int | float | None = None,
                 verbose: bool = True) -> tuple[str, str]:
        if capture_output and not wait:
            raise ArgumentError("Cannot set capture_output while wait is False.")

        exe = self.cluster.manager.server_get_exe(server_id=self.server_id)
        nodetool_cmd = [exe, "nodetool", "-h", str(self.cluster.manager.get_host_ip(server_id=self.server_id))]
        env = None
        if self.cluster.ccm_parity:
            # ccm's ScyllaNode.nodetool(): the node's launch environment, a
            # `<command> --help` run first to check the command exists, and the
            # REST API port named explicitly.
            env = os.environ | self._process_scylla_env() | {"SCYLLA_HOME": self.get_path()}
            command = next(arg for arg in cmd.split() if not arg.startswith("-"))
            subprocess.run([exe, "nodetool", command, "--help"], env=env, check=False,
                           stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
            nodetool_cmd += ["-p", str(self.api_port)]
        nodetool_cmd += cmd.split()

        if verbose:
            self.debug(f"nodetool cmd={nodetool_cmd} wait={wait} timeout={timeout}")

        if capture_output:
            p = subprocess.Popen(nodetool_cmd, universal_newlines=True, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate(timeout=timeout)
        else:
            p = subprocess.Popen(nodetool_cmd, universal_newlines=True, env=env)
            stdout, stderr = None, None

        if wait and p.wait(timeout=timeout):
            raise NodetoolError(" ".join(nodetool_cmd), p.returncode, stdout, stderr)

        if stderr is not None:
            stderr = "\n".join(
                line for line in stderr.splitlines()
                if self.debug(f"checking {line}") or not any(p.fullmatch(line) for p in NODETOOL_STDERR_IGNORED_PATTERNS)
            )

        return stdout, stderr

    def get_path(self) -> str:
        """Return the path to this node top level directory (where config/data is stored.)"""

        return self.cluster.manager.server_get_workdir(server_id=self.server_id)

    def get_conf_dir(self) -> str:
        """Return the path to this node's configuration directory."""

        return os.path.join(self.get_path(), SCYLLA_CONF_DIR)

    def logfilename(self) -> str:
        """Return the path to this node's Scylla log."""

        return str(Path(self.get_path()).with_suffix(".log"))

    def get_datacenter_name(self) -> str:
        """The datacenter the node reports, from `nodetool info`, as ccm's Node.get_datacenter_name().

        Not self.data_center: that is what the test asked for, and None for a node placed in no
        datacenter at all, which Scylla then calls datacenter1.
        """
        info = self.nodetool("info")[0]
        lines = [line for line in info.splitlines() if line.startswith("Data Center")]
        if len(lines) != 1:
            raise RuntimeError(f"Expected output from `nodetool info` to contain exactly 1 line starting with "
                               f"\"Data Center\". Found:\n{info}")
        return lines[0].split(":", 1)[1].strip()

    # ------------------------------------------------- Scylla Manager agent

    def _create_agent_config(self) -> str:
        """Write the agent's config file and return its path.

        Every port here is bound to this node's own address, so agents of
        clusters running in parallel never collide.
        """

        conf_file = os.path.join(self.get_conf_dir(), SCYLLAMANAGER_AGENT_CONF)
        data = {
            "https": f"{self.address()}:{AGENT_API_PORT}",
            "auth_token": self.scylla_manager.auth_token,
            "tls_cert_file": self.scylla_manager.agent_tls_cert_file,
            "tls_key_file": self.scylla_manager.agent_tls_key_file,
            "logger": {"level": "debug"},
            "debug": f"{self.address()}:{AGENT_DEBUG_PORT}",
            "scylla": {"api_address": self.address(), "api_port": SCYLLA_API_PORT},
            "prometheus": f"{self.address()}:{AGENT_PROMETHEUS_PORT}",
        }
        with open(conf_file, "w") as f:
            YAML().dump(data, f)
        return conf_file

    def update_agent_config(self, new_settings: dict, restart_agent_after_change: bool = True) -> None:
        conf_file = os.path.join(self.get_conf_dir(), SCYLLAMANAGER_AGENT_CONF)
        yaml = YAML()
        with open(conf_file) as f:
            current_config = yaml.load(f)

        current_config.update(new_settings)

        with open(conf_file, "w") as f:
            yaml.dump(current_config, f)

        if restart_agent_after_change:
            self.restart_scylla_manager_agent(gently=True, recreate_config=False)

    def _follow_replaced_node(self, host_id: str | None, address: str | None) -> None:
        """Move this node into the dc and rack of the node it is about to replace.

        Scylla refuses a replacement from another dc or rack.  ccm put every node
        of a datacenter in one rack, so upstream tests never name one for the
        replacing node; here populate() spreads nodes over racks, and a new node
        lands wherever the harness puts it.  Unless the test placed this node
        itself, give it the replaced node's placement before its first start.
        """
        if self.placement_explicit:
            return
        for node in self.cluster.nodelist():
            if node is self:
                continue
            if (host_id and node._hostid == host_id) or (address and node.address() == address):
                replaced = node
                break
        else:
            return
        if (replaced.data_center, replaced.rack) == (self.data_center, self.rack):
            return
        self.debug(f"moving to {replaced.data_center}/{replaced.rack} to replace {replaced.name}")
        self.move_to(data_center=replaced.data_center, rack=replaced.rack)

    def move_to(self, data_center: str, rack: str) -> None:
        """Place this stopped, never started node in another dc and rack."""

        assert not self.is_running(), f"{self.name} must be stopped to move"
        server = self.cluster.manager.cluster.servers[self.server_id]
        server.property_file = {"dc": data_center, "rack": rack}
        # Rewrites cassandra-rackdc.properties along with scylla.yaml.
        self.cluster.manager.server_update_config(server_id=self.server_id, config_options={})
        self.data_center, self.rack = data_center, rack

    def kill(self, sig: int = signal.SIGKILL) -> None:
        """Send a signal to this node's scylla process, as ccm's Node.kill() did."""

        os.kill(self.pid, sig)

    def import_config_files(self) -> None:
        """Write this node's native transport interface back into its scylla.yaml.

        ccm regenerated a node's whole config here; the cluster manager owns
        everything but what a test can change on the node object, and that is
        network_interfaces["binary"].
        """
        host, port = self.network_interfaces["binary"]
        self.cluster.manager.server_update_config(server_id=self.server_id,
                                                  config_options={"rpc_address": host, "native_transport_port": port})
        if self.cluster.ccm_parity:
            # ccm's update_yaml() wrote the seeds as well: what the cluster's seeds are now
            # (a test that narrows cluster.seeds before a replace relies on it).
            self.cluster._save_ccm_seeds([self])  # noqa: SLF001

    def start_scylla_manager_agent(self, create_config: bool = True) -> None:
        agent_bin = self.scylla_manager._get_bin("scylla-manager-agent")
        config_file = self._create_agent_config() if create_config else os.path.join(self.get_conf_dir(), SCYLLAMANAGER_AGENT_CONF)
        log_file = self.logfilename() + ".manager_agent"

        args = [agent_bin, "--config-file", config_file]
        self.debug(f"Starting Scylla Manager agent: {args}")
        with open(log_file, "a") as agent_log:
            self._process_agent = subprocess.Popen(args, stdout=agent_log, stderr=agent_log, close_fds=True)

        with open(config_file) as f:
            listening_port = int(YAML().load(f)["https"].split(":")[1])

        api_interface = parse_interface(self.address(), listening_port)
        if not check_socket_listening(api_interface, timeout=AGENT_START_TIMEOUT):
            raise NodeError(
                f"scylla-manager-agent API {api_interface[0]}:{api_interface[1]} is not listening after "
                f"{AGENT_START_TIMEOUT}s; see {log_file}"
            )

    def stop_scylla_manager_agent(self, gently: bool = True) -> None:
        if not self._process_agent:
            return
        try:
            if gently:
                self._process_agent.terminate()
            else:
                self._process_agent.kill()
            self._process_agent.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self._process_agent.kill()
            self._process_agent.wait(timeout=30)
        except OSError:
            pass
        self._process_agent = None

    def restart_scylla_manager_agent(self, gently: bool = True, recreate_config: bool = True) -> None:
        self.stop_scylla_manager_agent(gently=gently)
        self.start_scylla_manager_agent(create_config=recreate_config)

    def stress(self, stress_options: list[str], **kwargs):
        """Run `cassandra-stress` against this node, without parsing its output.

        As scylla-dtest's conftest did to ccm's ScyllaNode.stress: the tool runs
        in a container of the image pinned in tools/values_docker_versions.yaml
        (tools.stress.run_stress), not whatever cassandra-stress the host has.
        `kwargs` go to CassandraStressDocker (env, volumes, timeout, ...).

        :return: `stdout`, `stderr` and `rc` of the run; a non-zero exit raises ToolError.
        """
        from tools.stress import run_stress  # noqa: PLC0415 -- the dtest tools package, as tests import it
        return run_stress(self, stress_options, **kwargs)

    def stress_object(self, stress_options: list[str], ignore_errors: bool | None = None, **kwargs) -> dict[str, float]:
        """Run `cassandra-stress` and return its "Results:" section as a dict.

        Same parsing as ccm's Node.stress_object(): keys are lower-cased, numbers
        are floats, and a "READ: x, WRITE: y" breakdown adds "<key>:read" and
        "<key>:write" entries.
        """
        del ignore_errors  # deprecated in ccm as well
        ret = self.stress(stress_options, **kwargs)
        res = {}
        started = False
        for line in (line.strip() for line in ret.stdout.splitlines()):
            if not started:
                started = line == "Results:"
            elif m := re.match(r"^\s*([^:]+)\s*:\s*(\S.*)\s*$", line):
                _set_stress_val(m.group(1).strip().lower(), m.group(2).strip(), res)
        return res

    def run_cqlsh(self,
                  cmds: str | None = None,
                  show_output: bool = False,
                  cqlsh_options: list[str] | None = None,
                  return_output: bool = False,
                  timeout: int | float = 600,
                  extra_env: dict | None = None) -> tuple[str, str] | None:
        """Run the real `cqlsh` binary shipped by this repo (./bin/cqlsh) against this node.

        Mirrors scylla-ccm's Node.run_cqlsh() (ccmlib/node.py): `cmds` is a
        string of `;`-separated statements piped to cqlsh's stdin, `cqlsh_options`
        are extra argv options inserted before the host/port positionals, and
        with `return_output` the (stdout, stderr) pair is returned -- so ported
        dtest bodies that call node.run_cqlsh(...) need no changes.

        Unlike the ccm version, there is no interactive (`cmds=None`) mode and no
        Windows branch: this in-tree port only needs to feed cqlsh a fixed set of
        commands and read back its output.
        """
        cqlsh_options = list(cqlsh_options or [])

        env = os.environ.copy()
        if extra_env:
            env.update(extra_env)

        host, port = self.network_interfaces["binary"]
        args = cqlsh_options if "--cloudconf" in cqlsh_options else [*cqlsh_options, host, str(port)]

        self.debug(f"run_cqlsh cmd={[CQLSH_BIN, *args]}")
        # cqlsh leaves files in its working directory (COPY FROM's import_<ks>_<table>.err),
        # so run it from the node's directory, as ccm's cqlsh ran from the test directory.
        p = subprocess.Popen([CQLSH_BIN, *args], env=env, cwd=self.get_path(), stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        try:
            if cmds is not None:
                for cmd in cmds.split(";"):
                    cmd = cmd.strip()
                    if cmd:
                        p.stdin.write(cmd + ";\n")
                p.stdin.write("quit;\n")
        except BrokenPipeError:
            # cqlsh already exited, e.g. it was only asked to print --version.
            pass

        try:
            stdout, stderr = p.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            p.kill()
            p.communicate()
            raise

        for line in stderr.splitlines():
            if line.strip():
                self.warning(f"(cqlsh stderr) {line}")

        if show_output:
            self.debug(stdout)

        if return_output:
            return stdout, stderr
        return None

    def flush(self, ks: str | None = None, table: str | None = None, **kwargs) -> None:
        """Flush memtables to sstables via the REST API.

        Uses ScyllaRESTAPIClient (same as test/pylib/nodetool.py) which is
        much faster than spawning a nodetool subprocess.  Under ccm parity it
        is ccm's Node.flush(): `nodetool flush [ks [table]]`.
        """
        if self.cluster.ccm_parity:
            self.nodetool(" ".join(["flush", *([ks] if ks else []), *([table] if table else [])]), **kwargs)
            return
        if ks:
            self.cluster.manager.api.keyspace_flush(node_ip=self.address(), keyspace=ks, table=table)
        else:
            self.cluster.manager.api.flush_all_keyspaces(node_ip=self.address())

    def compact(self, keyspace: str = "", tables: tuple | list = ()) -> None:
        if self.cluster.ccm_parity:
            # ccm's Node.compact(): `nodetool compact [keyspace [tables...]]`.
            self.nodetool(" ".join(["compact", *([keyspace] if keyspace else []), *tables]))
            return
        node_ip = self.address()
        if keyspace:
            self.cluster.manager.api.keyspace_compaction(
                node_ip=node_ip,
                keyspace=keyspace,
                table=",".join(tables) if tables else None,
            )
        else:
            self.cluster.manager.api.compact(node_ip=node_ip)

    def wait_for_compactions(self,
                             keyspace: str = "",
                             column_family: str = "",
                             timeout: float = 300,
                             quiesce_time: float = 0.5) -> None:
        """Wait until this node has no compaction left to do.

        ccm polled `nodetool compactionstats`; the same state comes from the
        task manager here.  Routine compactions register as internal tasks
        (compaction::regular_compaction_task_impl), so they are only listed
        with internal=True.  A new compaction can start just after another
        finishes, so the node has to look idle for `quiesce_time` before this
        returns -- the guard ccm's wait_for_compactions had.
        """

        if column_family and not keyspace:
            raise ArgumentError("Cannot wait on a column family without naming its keyspace")

        if self.cluster.ccm_parity:
            self._wait_for_compactions_as_ccm(keyspace, column_family, timeout, quiesce_time)
            return

        pending_states = {"created", "running", "suspended"}
        deadline = time.perf_counter() + timeout
        idle_since = None
        while True:
            tasks = self.cluster.manager.api.get_tasks(
                node_ip=self.address(),
                module="compaction",
                keyspace=keyspace,
                table=column_family,
                internal=True,
            )
            active = [t for t in tasks if t.get("state") in pending_states]
            now = time.perf_counter()
            if active:
                idle_since = None
            elif idle_since is None:
                idle_since = now
            elif now - idle_since >= quiesce_time:
                return
            if now >= deadline:
                raise TimeoutError(
                    f"compactions on {self.name} did not finish within {timeout}s; still active: {active}")
            time.sleep(0.1)

    def _wait_for_compactions_as_ccm(self, keyspace: str, column_family: str, timeout: float, quiesce_time: float) -> None:
        """ccm's Node.wait_for_compactions(): poll `nodetool compactionstats`.

        Counts the pending and active tasks it prints (for the keyspace/table, if
        given); done once there have been none for `quiesce_time`.  Times out only
        after `timeout` seconds without the count changing, as ccm did.
        """
        pending_tasks = -1
        last_change = None
        idle_since = None
        output = ""
        while not last_change or time.time() - last_change < timeout:
            output, _ = self.nodetool("compactionstats")
            tasks: dict[tuple[str, str], int] = {}
            for line in output.strip().splitlines():
                line = line.strip()
                if m := _CCM_PENDING_TASKS.match(line):
                    key = (m.group("ks"), m.group("cf"))
                    tasks[key] = tasks.get(key, 0) + int(m.group("tasks"))
                elif m := _CCM_ACTIVE_TASKS.match(line):
                    key = (m.group("ks"), m.group("cf"))
                    tasks[key] = tasks.get(key, 0) + 1
            n = sum(v for (ks, cf), v in tasks.items()
                    if (not keyspace or ks == keyspace) and (not column_family or cf == column_family))
            if n == 0:
                if idle_since is None:
                    idle_since = time.time()
                elif time.time() - idle_since >= quiesce_time:
                    return
            else:
                idle_since = None
                if n != pending_tasks:
                    last_change = time.time()
                    pending_tasks = n
            time.sleep(0.1)
        raise TimeoutError(f"Waiting for compactions timed out after {timeout} seconds with pending tasks remaining: {output}.")

    def cleanup(self) -> None:
        """Clean up this node, as ccm's Node.cleanup() did: `nodetool cleanup`.

        That posts /storage_service/cleanup_all/ with global=false, a local cleanup
        through the compaction manager.  The endpoint's default is global=true, a
        cluster-wide vnodes cleanup driven by the topology coordinator, which is
        what calling it without the parameter used to do here.
        """
        self.nodetool("cleanup")

    def drain(self, block_on_log: bool = False) -> None:
        """Drain the node via the REST API (under ccm parity, ccm's `nodetool drain`)."""
        mark = self.mark_log()
        if self.cluster.ccm_parity:
            self.nodetool("drain")
        else:
            self.cluster.manager.api.drain(node_ip=self.address())
        if block_on_log:
            self.watch_log_for("DRAINED", from_mark=mark)

    def repair(self,  # noqa: PLR0913
               options: list[str] | None = None,
               *,
               keyspace: str = "",
               tables: list[str] | None = None,
               dcs: list[str] | None = None,
               hosts: list[str] | None = None,
               local: bool = False,
               partitioner_range: bool = False,
               timeout: float | None = None) -> tuple[str, str]:
        """
        Repair via the REST API and wait for completion.

        Supports two calling conventions: the CLI-style `options=[keyspace, table]`
        (with `-dc`/`--source-dc`), used by a couple of tests, and the keyword form
        (`keyspace=`, `tables=`, `dcs=`, `hosts=`, `local=`, `partitioner_range=`)
        used by most.  `timeout` is not used in scylla-dtest: the REST call already
        waits for completion.  Returns a ("", "") pair, only for source compatibility
        with nodetool()'s (stdout, stderr).
        """
        if self.cluster.ccm_parity:
            return self._repair_as_ccm(options, keyspace=keyspace, tables=tables, dcs=dcs, hosts=hosts,
                                       local=local, partitioner_range=partitioner_range, timeout=timeout)
        if options is not None:
            opt_keyspace, opt_table, opt_dc = self._parse_repair_options(options)
            keyspace = keyspace or opt_keyspace
            if opt_table:
                tables = tables or [opt_table]
            if opt_dc:
                dcs = dcs or [opt_dc]

        command = f"repair {keyspace}".strip()

        # nodetool refuses -pr with -dc/-hosts before calling Scylla (see
        # repair_operation() in tools/scylla-nodetool.cc).  Scylla itself lets
        # "-pr -dc <local dc>" through, which is what `local` is sent as, so
        # check the explicit options here, before `local` is folded into `dcs`.
        if partitioner_range and (dcs or hosts):
            raise NodetoolError(command, 1, stderr="primary range repair should be performed on all nodes in the cluster")

        if local and not dcs:
            dcs = [self.data_center]

        # A failed or rejected repair makes nodetool exit non-zero, and the tests
        # expect the NodetoolError ccm raises for it, not the REST client's error.
        try:
            self.cluster.manager.api.repair_and_wait(
                node_ip=self.address(),
                keyspace=keyspace,
                table=",".join(tables) if tables else "",
                data_centers=",".join(dcs) if dcs else "",
                hosts=",".join(hosts) if hosts else "",
                primary_range=partitioner_range,
            )
        except (RuntimeError, HTTPError) as exc:
            raise NodetoolError(command, 1, stderr=str(exc)) from exc
        return ("", "")

    def _repair_as_ccm(self,  # noqa: PLR0913
                       options: list[str] | None,
                       keyspace: str,
                       tables: list[str] | None,
                       dcs: list[str] | None,
                       hosts: list[str] | None,
                       local: bool,
                       partitioner_range: bool,
                       timeout: float | None) -> tuple:
        """ccm's repair, run through nodetool.

        The CLI-style options went to ccm's Node.repair(): `nodetool repair
        <options>`.  The keyword form is ccm's ScyllaNode.repair(): `nodetool
        repair` for a vnodes keyspace, `nodetool cluster repair` for a tablets
        one (asking the REST API which it is), and both when no keyspace is
        named; the (stdout, stderr) pairs come back zipped, as ccm returned them.
        """
        if options is not None:
            return self.nodetool(" ".join(["repair", *options]), timeout=timeout)
        common = [*([keyspace, " ".join(tables)] if keyspace and tables else [keyspace] if keyspace else []),
                  *(["--in-hosts", ",".join(hosts)] if hosts else []),
                  *(["--in-dc", ",".join(dcs)] if dcs else [])]
        vnode = ["repair", *common, *(["--in-local-dc"] if local else []),
                 *(["--partitioner-range"] if partitioner_range else [])]
        tablet = ["cluster", "repair", *common]
        if keyspace:
            response = requests.get(url=f"http://{self.address()}:{self.api_port}/storage_service/keyspaces",
                                    params={"replication": "vnodes"})
            response.raise_for_status()
            runs = [vnode if keyspace in response.json() else tablet]
        else:
            runs = [vnode, tablet]
        outs, errs = zip(*(self.nodetool(" ".join(run), timeout=timeout) for run in runs))
        return outs, errs

    @staticmethod
    def _parse_repair_options(options: list[str]) -> tuple[str, str, str]:
        parser = argparse.ArgumentParser(description="Parse repair options")
        parser.add_argument("-dc", "--source-dc", type=str, default="", help="data center name")
        parser.add_argument("keyspace", nargs="?", default="", help="keyspace to repair")
        parser.add_argument("table", nargs="?", default="", help="table to repair")
        args = parser.parse_args(options)
        return args.keyspace, args.table, args.source_dc

    def decommission(self) -> None:
        if self.cluster.ccm_parity:
            # ccm's Node.decommission(): `nodetool decommission`, and the node
            # keeps running until the test stops it.
            self.nodetool("decommission")
            self._decommissioned = True
            return
        # ccm ran `nodetool decommission`, so tests expect a NodetoolError when
        # Scylla refuses, or when the node dies under the request.
        try:
            self.cluster.manager.decommission_node(server_id=self.server_id)
        except (RuntimeError, HTTPError, aiohttp.ClientError) as exc:
            raise NodetoolError("decommission", 1, stdout="", stderr=str(exc)) from exc
        self._decommissioned = True

    def take_snapshot(self, keyspace: str, tag: str, tables: list[str] | None = None) -> None:
        self.cluster.manager.api.take_snapshot(node_ip=self.address(), ks=keyspace, tag=tag, tables=tables)

    def clear_snapshot(self, tag: str, keyspace: str = "") -> None:
        self.cluster.manager.api.delete_snapshot(node_ip=self.address(), tag=tag, keyspace=keyspace)

    def load_new_sstables(self, keyspace: str, table: str, load_and_stream: bool = False) -> None:
        self.cluster.manager.api.load_new_sstables(
            node_ip=self.address(), keyspace=keyspace, table=table,
            load_and_stream=load_and_stream)

    def rebuild(self, source_dc: str | None = None, timeout: float = 1000) -> None:
        self.cluster.manager.api.rebuild_node(host_ip=self.address(), timeout=timeout, source_dc=source_dc)

    def scrub(self, keyspace: str = "", table: str = "", scrub_mode: str = "ABORT") -> None:
        self.cluster.manager.api.keyspace_scrub_sstables(
            node_ip=self.address(), ks=keyspace, scrub_mode=scrub_mode, table=table)

    def get_endpoints(self, keyspace: str, table: str, key: str) -> list:
        return self.cluster.manager.api.natural_endpoints(node_ip=self.address(), keyspace=keyspace, table=table, key=key)

    def is_scylla(self) -> bool:
        return True

    def scylla_exe(self) -> str:
        """Return the path of the scylla binary this node runs."""

        return str(self.cluster.manager.server_get_exe(server_id=self.server_id))

    def get_sstablespath(self, keyspace: str, tables: list[str] | None = None, **kwargs) -> list[str]:
        """Return the -Data.db paths of the given tables, as ccm's Node.get_sstablespath() does."""

        del kwargs
        files = []
        for table in tables or [""]:
            files += self.get_sstables(keyspace, table)
        return files

    def get_sstables(self, keyspace, column_family, ignore_unsealed=True, cleanup_unsealed=False):
        keyspace_dir = os.path.join(self.get_path(), 'data', keyspace)
        cf_glob = '*'
        if column_family:
            cf_glob = column_family + '-*'
        if not os.path.exists(keyspace_dir):
            raise ArgumentError(f"Unknown keyspace {keyspace}")

        files = glob.glob(os.path.join(keyspace_dir, cf_glob, "*big-Data.db"))
        for f in files:
            if os.path.exists(f.replace('Data.db', 'Compacted')):
                files.remove(f)
        if ignore_unsealed or cleanup_unsealed:
            # sstablelevelreset tries to open all sstables under the configured
            # `data_file_directories` in cassandra.yaml. if any of them does not
            # have missing component, it complains in its stderr. and some tests
            # using this tool checks the stderr for unexpected error message, so
            # we need to remove all unsealed sstable files in this case.
            for toc_tmp in glob.glob(os.path.join(keyspace_dir, cf_glob, '*TOC.txt.tmp')):
                if cleanup_unsealed:
                    self.info(f"get_sstables: Cleaning up unsealed SSTable: {toc_tmp}")
                    for unsealed in glob.glob(toc_tmp.replace('TOC.txt.tmp', '*')):
                        os.remove(unsealed)
                else:
                    self.info(f"get_sstables: Ignoring unsealed SSTable: {toc_tmp}")
                data_sst = toc_tmp.replace('TOC.txt.tmp', 'Data.db')
                try:
                    files.remove(data_sst)
                except ValueError:
                    pass
        return files

    def __gather_sstables(self, datafiles=None, keyspace=None, columnfamilies=None):
        files = []
        if keyspace is None:
            for k in self.list_keyspaces():
                files = files + self.get_sstables(k, "")
        elif datafiles is None:
            if columnfamilies is None:
                files = files + self.get_sstables(keyspace, "")
            else:
                for cf in columnfamilies:
                    files = files + self.get_sstables(keyspace, cf)
        else:
            if not columnfamilies or len(columnfamilies) > 1:
                raise ArgumentError("Exactly one column family must be specified with datafiles")

            cf_dir = os.path.join(os.path.realpath(self.get_path()), 'data', keyspace, columnfamilies[0])

            sstables = set()
            for datafile in datafiles:
                if not os.path.isabs(datafile):
                    datafile = os.path.join(os.getcwd(), datafile)

                if not datafile.startswith(cf_dir + '-') and not datafile.startswith(cf_dir + os.sep):
                    raise NodeError("File doesn't appear to belong to the specified keyspace and column family: " + datafile)

                sstable = _sstable_regexp.match(os.path.basename(datafile))
                if not sstable:
                    raise NodeError("File doesn't seem to be a valid sstable filename: " + datafile)

                sstable = sstable.groupdict()
                if not sstable['tmp'] and sstable['identifier'] not in sstables:
                    if not os.path.exists(datafile):
                        raise IOError("File doesn't exist: " + datafile)
                    sstables.add(sstable['identifier'])
                    files.append(datafile)

        return files

    def run_scylla_sstable(self, command, additional_args=None, keyspace=None, datafiles=None, column_families=None, batch=False, text=True, env=None):
        """Invoke scylla-sstable, with the specified command (operation) and additional_args.

        For more information about scylla-sstable, see https://docs.scylladb.com/stable/operating-scylla/admin-tools/scylla-sstable.html.

        Params:
        * command - The scylla-sstable command (operation) to run.
        * additional_args - Additional command-line arguments to pass to scylla-sstable, this should be a list of strings.
        * keyspace - Restrict the operation to sstables of this keyspace.
        * datafiles - Restrict the operation to the specified sstables (Data components).
        * column_families - Restrict the operation to sstables of these column_families. Must contain exactly one column family when datafiles is used.
        * batch - If True, all sstables will be passed in a single batch. If False, sstables will be passed one at a time.
            Batch-mode can be only used if column_families contains a single item.
        * text - If True, output of the command is treated as text, if not as bytes

        If datafiles is provided, the caller is responsible for making sure these
        files are not removed by ScyllaDB while the tool is running.
        If datafiles = None, this function will create a snapshot and dump
        sstables from the snapshot to ensure that the sstables are not removed
        while the tools is running.
        The snapshot is removed after the dump completed, but it is left there in
        case of error, for post-mortem analysis.

        Returns: map: {sstable: (stdout, stderr)} of all invokations. When batch == True, a single entry will be present, with empty key.

        Raises: subprocess.CalledProcessError if scylla-sstable returns a non-zero exit code.
        """
        if additional_args is None:
            additional_args = []

        scylla_path = self.cluster.manager.server_get_exe(server_id=self.server_id)
        ret = {}

        if datafiles is None and keyspace is not None and self.is_running():
            tag = "sstable-dump-{}".format(uuid.uuid1())
            self.debug(f"run_scylla_sstable(): creating snapshot with tag {tag} to be used for sstable dumping")
            if self.cluster.ccm_parity:
                # ccm: `nodetool snapshot -t <tag> <ks>.<cf>,...`
                self.nodetool(f"snapshot -t {tag} " + ",".join(f"{keyspace}.{cf}" for cf in column_families))
            else:
                self.take_snapshot(keyspace=keyspace, tag=tag, tables=list(column_families))
            sstables = []
            for column_family in column_families:
                sstables.extend(glob.glob(os.path.join(self.get_path(), 'data', keyspace, f"{column_family}-*/snapshots/{tag}/*-Data.db")))
        else:
            sstables = self.__gather_sstables(datafiles, keyspace, column_families)
            tag = None

        self.debug(f"run_scylla_sstable(): preparing to dump sstables {sstables}")

        def do_invoke(sstables):
            # there are chances that the table is not replicated on this node,
            # in that case, scylla tool will fail to dump the sstables and error
            # out. let's just return an empty list for the partitions, so that
            # dump_sstables() can still parse it in the same way.
            if not sstables:
                empty_dump = {'sstables': {'anonymous': []}}
                stdout, stderr = json.dumps(empty_dump), ''
                if text:
                    return stdout, stderr
                else:
                    return stdout.encode('utf-8'), stderr.encode('utf-8')
            common_args = [scylla_path, "sstable", command] + additional_args
            # Under ccm parity, from a directory with no conf/: the tool falls back to
            # ./conf/scylla.yaml, which the repository's root has and scylla-dtest's checkout
            # has not.
            res = subprocess.run(common_args + sstables, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=text, check=False, env=env,
                                 cwd=self.cluster.get_path() if self.cluster.ccm_parity else None)
            if res.returncode:
                raise ToolError(command=' '.join(common_args + sstables), exit_status=res.returncode, stdout=res.stdout, stderr=res.stderr)
            return (res.stdout, res.stderr)

        if batch:
            if column_families is None or len(column_families) > 1:
                raise NodeError("run_scylla_sstable(): batch mode can only be used in conjunction with a single column_family")
            ret[""] = do_invoke(sstables)
        else:
            for sst in sstables:
                ret[sst] = do_invoke([sst])

        # Deliberately not putting this in a `finally` block, if the command
        # above failed, leave the snapshot with the sstables around for
        # post-mortem analysis.
        if tag is not None:
            if self.cluster.ccm_parity:
                self.nodetool(f"clearsnapshot -t {tag} {keyspace}")
            else:
                self.clear_snapshot(tag=tag, keyspace=keyspace)

        return ret

    def run_scylla_types(self, action: str, scylla_type: ScyllaType, *values: Any, extra_args: list[Any] | None = None) -> str:
        """Run `scylla types <action>` on the values and return its stdout, as ccm did."""

        cmd = [self.scylla_exe(), "types", action, *(extra_args or []), *scylla_type.as_types_args(), "--", *values]
        cmd = [str(arg) for arg in cmd]
        res = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if res.returncode:
            raise ToolError(cmd, res.returncode, res.stdout, res.stderr)
        return res.stdout.strip()

    def dump_sstable_stats(self,
                           keyspace: str,
                           column_family: str,
                           datafiles: list[str] | None = None) -> dict[str, Any]:
        """`scylla sstable dump-statistics` for this table, keyed by sstable, as in ccm."""

        sstable_stats = self.run_scylla_sstable(
            "dump-statistics",
            keyspace=keyspace,
            column_families=[column_family],
            datafiles=datafiles,
            batch=True,
            text=False,
        )
        assert "" in sstable_stats
        stdout, _ = sstable_stats[""]
        return json.loads(stdout.decode("utf-8", "ignore"))["sstables"]

    def dump_sstables(self,
                      keyspace: str,
                      column_family: str,
                      datafiles: list[str] | None = None) -> list[dict[str, Any]]:
        """The partitions in this table, via `scylla sstable dump-data`.

        Same as ccm's ScyllaNode.dump_sstables(): the sstables are merged into
        one dump, so the result is the list of partitions the node holds.
        """
        sstable_dumps = self.run_scylla_sstable(
            "dump-data",
            ["--merge"],
            keyspace=keyspace,
            column_families=[column_family],
            datafiles=datafiles,
            batch=True,
        )
        assert "" in sstable_dumps
        stdout, _ = sstable_dumps[""]
        return json.loads(stdout)["sstables"]["anonymous"]

    @property
    def status(self) -> str:
        """ccm's Node.status, as far as this shim tracks it.

        The manager only knows whether the process is up, so a node that is
        not running reads as DOWN -- except a decommissioned one, which tests
        single out to leave alone (its REST API is gone).  ccm's UNINITIALIZED
        is still not distinguished.
        """
        if self._decommissioned:
            return Status.DECOMMISSIONED
        return Status.UP if self.is_running() else Status.DOWN

    def clear(self, clear_all: bool = False, only_data: bool = False, saved_caches: bool = False) -> None:
        """Delete this node's on-disk state, as ccm's Node.clear() does.

        `only_data` keeps the system keyspaces and empties every other table's
        directory, which is how a test makes a node forget its user data without
        making it forget it is a member of the cluster.

        The directory names are the ones Scylla derives from the workdir the
        cluster manager gives it (`commitlog`, where ccm has `commitlogs`), and a
        directory the node has not created yet is skipped.
        """
        path = Path(self.get_path())

        if only_data:
            data_dir = path / "data"
            for keyspace_dir in sorted(data_dir.iterdir()) if data_dir.is_dir() else []:
                if not keyspace_dir.is_dir() or keyspace_dir.name.startswith("system"):
                    continue
                for table_dir in sorted(keyspace_dir.iterdir()):
                    if table_dir.is_dir():
                        shutil.rmtree(table_dir)
                        table_dir.mkdir()
            return

        dirs = ["data", "commitlog"]
        if clear_all:
            dirs.append("logs")
            if saved_caches:
                dirs.append("saved_caches")
        elif saved_caches:
            dirs.append("saved_caches")
        for name in dirs:
            full_dir = path / name
            if full_dir.is_dir():
                self.rmtree(full_dir)

    def _seeds_left_on_disk(self) -> list[str] | None:
        """The seeds to start with when a test broke the seeds entry of scylla.yaml.

        ccm started Scylla on whatever the test left in the node's scylla.yaml; the
        manager rewrites the seeds on every start, which would undo a test that
        deliberately misspells the key (bootstrap_test's misspelled-seeds case).
        Scylla without a "seeds" parameter falls back to 127.0.0.1 (init.cc,
        get_seeds_from_db_config()), so start the node with exactly that.  None,
        for the usual intact entry, lets the manager pick the running nodes.
        """
        conf_file = os.path.join(self.get_conf_dir(), "scylla.yaml")
        try:
            with open(conf_file) as f:
                seed_provider = (yaml.safe_load(f) or {}).get("seed_provider")
        except FileNotFoundError:
            return None
        if not seed_provider:
            return None
        parameters = (seed_provider[0].get("parameters") or [{}])[0]
        return None if "seeds" in parameters else ["127.0.0.1"]

    def get_configuration_options(self) -> dict:
        """This node's scylla.yaml, as a dict."""
        return self.cluster.manager.server_get_config(server_id=self.server_id)

    def get_conf_dir(self) -> str:
        """The directory holding this node's scylla.yaml."""
        return os.path.join(self.get_path(), "conf")

    def update_yaml(self) -> None:
        """Re-apply this node's own settings to its scylla.yaml on disk.

        ccm's Node.update_yaml() rewrites the file from the cluster's options plus
        the node's own (addresses, ports, work directories).  Here the manager owns
        the node-specific half, so this reads back whatever is in the file, keeps
        only the cluster-wide part of it, and hands that to the manager, which
        merges it into the config it maintains and rewrites the file.  That is what
        a test wants after copying another node's scylla.yaml over this one's: the
        cluster-wide options come across, the addresses stay this node's.
        """
        conf_file = os.path.join(self.get_conf_dir(), "scylla.yaml")
        with open(conf_file) as f:
            on_disk = yaml.safe_load(f) or {}
        cluster_wide = {k: v for k, v in on_disk.items() if k not in NODE_SPECIFIC_CONFIG_OPTIONS}
        self.cluster.manager.server_update_config(server_id=self.server_id, config_options=cluster_wide)

    def get_node_scylla_version(self, scylla_exec_path: str | None = None) -> str:
        """`scylla --version` for this node's executable, or for the given one."""
        exe = scylla_exec_path or self.cluster.manager.server_get_exe(server_id=self.server_id)
        run_output = subprocess.run([str(exe), "--version"], capture_output=True, text=True, check=False)
        if run_output.returncode:
            raise NodeError(f"Failed to run {exe} --version. Error:\n{run_output.stderr}")
        return run_output.stdout.strip()

    @property
    def scylla_build_id(self) -> str:
        """`scylla --build-id` for this node's executable (ccm's ScyllaNode.scylla_build_id)."""
        exe = self.cluster.manager.server_get_exe(server_id=self.server_id)
        run_output = subprocess.run([str(exe), "--build-id"], capture_output=True, text=True, check=False)
        if run_output.returncode:
            raise NodeError(f"Failed to run {exe} --build-id. Error:\n{run_output.stderr}")
        return run_output.stdout.strip()

    @property
    def node_scylla_version(self) -> str:
        if not self._node_scylla_version:
            self._node_scylla_version = self.get_node_scylla_version()
        return self._node_scylla_version

    @node_scylla_version.setter
    def node_scylla_version(self, scylla_exec_path: str | None) -> None:
        self._node_scylla_version = self.get_node_scylla_version(scylla_exec_path)

    def upgrade(self, upgrade_to_version: str) -> None:
        """Restart this node on another Scylla version."""
        self.upgrader.upgrade(upgrade_version=upgrade_to_version)

    def rollback(self, upgrade_to_version: str) -> None:
        """Restart this node on an older Scylla version, restoring system tables."""
        self.upgrader.upgrade(upgrade_version=upgrade_to_version, recover_system_tables=True)

    def removenode(self, hid: str) -> None:
        """Remove the node with this host id from the cluster, from this node."""
        # Find the node by the shim's host ids, not by asking every server over
        # REST (manager.all_servers_by_host_id()): start() drops the manager's
        # cached id, and a node that is down cannot tell it again -- e.g. a node
        # banned by an aborted removenode, which exits as soon as it is started,
        # and which the test then removes for good.  hostid() reads the log then.
        nodes = self.cluster.nodelist()
        removed = (next((node for node in nodes if node._hostid == hid), None)
                   or next((node for node in nodes if node.hostid() == hid), None))
        if removed and self.cluster.ccm_parity:
            # ccm's Node.removenode(): `nodetool removenode <host id>`.
            self.nodetool(f"removenode {hid}")
            self.cluster.manager.cluster.server_mark_removed(removed.server_id)
        elif removed:
            # The manager sends the id it holds for the server, which it cannot
            # fetch from a node that is down.
            server = self.cluster.manager.cluster.servers[removed.server_id]
            if getattr(server, "_host_id", None) is None:
                server._host_id = hid
            # Like `nodetool removenode`, do not wait for the other nodes to see
            # the node as dead first: tests call this on a node that is still
            # alive (or just killed) and expect Scylla to reject it with a
            # NodetoolError; the tests that want it to succeed stop the node
            # with wait_other_notice=True beforehand.
            try:
                self.cluster.manager.remove_node(initiator_id=self.server_id, server_id=removed.server_id, wait_dead=False)
            except (RuntimeError, HTTPError) as exc:
                raise NodetoolError(f"removenode {hid}", 1, stderr=str(exc)) from exc
        else:
            # A host id the manager does not know (e.g. one already forgotten by
            # the cluster object); fall back to plain nodetool, as ccm does.
            self.nodetool(f"removenode {hid}")

    def upgradesstables_if_command_available(self) -> bool:
        stdout, _ = self.nodetool("help")
        return "upgradesstables" in stdout

    def get_node_supported_sstable_versions(self) -> list[str]:
        match = self.grep_log(r"Feature (.*)_SSTABLE_FORMAT is enabled")
        return [m[1].group(1).lower() for m in match] if match else []

    def check_node_sstables_format(self, timeout: int = 10) -> set[str]:
        """The set of sstable format versions this node's system tables are in."""
        node_system_folder = os.path.join(self.get_path(), "data", "system")
        find_cmd = f"find {node_system_folder} -type f ! -path *snapshots* -printf %f\\n".split()
        result = subprocess.run(find_cmd, capture_output=True, timeout=timeout, text=True, check=False)
        assert not result.stderr, result.stderr
        assert result.stdout, f"Empty output from '{find_cmd}'"

        sstable_version_regex = re.compile(r"(\w+)-[^-]+-(.+)\.(db|txt|sha1|crc32)")
        return {
            match.group(1)
            for f in result.stdout.splitlines()
            if (match := sstable_version_regex.search(f))
        }

    def hostid(self, timeout: float | None = None, force_refresh: bool | None = None) -> str | None:
        assert timeout is None, "argument `timeout` is not supported"  # not used in scylla-dtest
        assert force_refresh is None, "argument `force_refresh` is not supported"  # not used in scylla-dtest

        # Cached, as ccm's Node.hostid() is: a host id belongs to the node for
        # its whole life, and it can only be read over the REST API while the
        # node is up -- which is exactly when a stopped node's callers
        # (watch_log_for_death(), removenode()) cannot ask for it any more.
        if self._hostid is None and self.cluster.ccm_parity:
            # ccm's ScyllaNode.hostid() read the whole log first -- the last id any
            # run of the node logged -- and asked the REST API only when the log
            # had none.
            try:
                m = self.grep_log(r"init - Setting local host id to ([0-9a-f-]{36})")
            except Exception:  # noqa: BLE001 -- e.g. a node that never started has no log
                m = []
            if m:
                self._hostid = m[-1][1].group(1)
        if self._hostid is None:
            try:
                self._hostid = self.cluster.manager.get_host_id(server_id=self.server_id)
            except Exception as exc:
                # As ccm's ScyllaNode.hostid() does, read it from the log, which
                # also works for a node that is down.  A running node may still
                # be on its way to a new id (wiped), so only its current run counts.
                from_mark = getattr(self, "mark", None) if self.is_running() else None
                try:
                    m = self.grep_log(r"init - Setting local host id to ([0-9a-f-]{36})", from_mark=from_mark)
                except Exception:  # noqa: BLE001 -- e.g. a node that never started has no log
                    m = []
                if m:
                    self._hostid = m[-1][1].group(1)
                else:
                    self.error(f"Failed to get hostid: {exc}")
        return self._hostid

    def rmtree(self, path: str | Path) -> None:
        """Delete a directory content without removing the directory.

        Copied this code from Python's documentation for Path.walk() method.
        """
        for root, dirs, files in Path(path).walk(top_down=False):
            for name in files:
                (root / name).unlink()
            for name in dirs:
                (root / name).rmdir()

    def _log_message(self, message: str) -> str:
        return f"{self.name}: {message}"

    def debug(self, message: str) -> None:
        self.cluster.debug(self._log_message(message))

    def info(self, message: str) -> None:
        self.cluster.info(self._log_message(message))

    def warning(self, message: str) -> None:
        self.cluster.warning(self._log_message(message))

    def error(self, message: str) -> None:
        self.cluster.error(self._log_message(message))

    def __repr__(self) -> str:
        return f"<ScyllaNode name={self.server_id} dc={self.data_center} rack={self.rack}>"


def _node_id_alternatives(node: ScyllaNode) -> str:
    """A regex alternation of the names Scylla's log may call this node by."""
    return "|".join(re.escape(str(i)) for i in (node.address(), node.hostid()) if i)


def _parse_scylla_args(args: list[str]) -> dict[str, list[str]]:
    parsed_args = {}

    args = iter(args)
    arg = next(args, None)

    while arg is not None:
        assert arg.startswith("-")

        key, *value = arg.split(sep="=", maxsplit=1)

        if value:  # handle argument in `--foo=bar` form
            arg = next(args, None)
        else:
            for arg in args:  # handle argument in `--foo bar` form
                if arg.startswith("-"):
                    break
                value.append(arg)
            else:  # no more arguments
                arg = None

        if key.startswith("--scylla-manager"):  # skip Scylla Manager arguments
            continue

        parsed_args.setdefault(key, []).append(" ".join(value))

    return parsed_args


def _parse_size(s: str) -> int:
    try:
        factor = 1024 ** abs("k KMGT".index(s[-1]) - 1)
    except ValueError:
        return int(s)
    return int(s[:-1]) * factor


class NodeUpgrader:
    """Restart a node on a different Scylla version.

    ccm's NodeUpgrader replaces the executables under the node's own install dir.
    Here the cluster manager owns the executable path, so the same three steps --
    stop the node, point it at the other binary, start it again -- go through
    `server_switch_executable()`, the primitive the manager's own
    `server_change_version()` is built out of.  Driving the node's `stop()`/
    `start()` rather than calling `server_change_version()` keeps the per-node
    command line this shim assembles (smp, memory, log levels) across the upgrade.

    The direction does not matter: "upgrading" to an older version is a rollback,
    and `recover_system_tables` then restores the system tables from the snapshot
    taken just before the switch (scylladb/scylla-enterprise#1950).
    """

    def __init__(self, node: ScyllaNode):
        self.node = node
        self._scylla_version_for_upgrade = None
        self.install_dir_for_upgrade = None

    @property
    def scylla_version_for_upgrade(self) -> str | None:
        return self._scylla_version_for_upgrade

    @scylla_version_for_upgrade.setter
    def scylla_version_for_upgrade(self, scylla_version_for_upgrade: str) -> None:
        self._scylla_version_for_upgrade = scylla_version_for_upgrade

    def _recover_system_tables(self) -> None:
        """Restore the system tables from the snapshot taken before the switch."""
        node_data_directory = Path(self.node.get_path()) / "data"
        if not node_data_directory.exists():
            raise NodeError(f"Data directory {node_data_directory} is not found")

        snapshot_folder_name = None
        for system_folder in ("system", "system_schema"):
            node_system_ks_directory = node_data_directory / system_folder
            if not snapshot_folder_name:
                # Some tables may not exist in the older version; "peers" is in
                # all of them, so use its snapshot directory to name the others.
                system_peers = [p for p in node_system_ks_directory.iterdir() if p.name.startswith("peers-")]
                snapshots = sorted((system_peers[0] / "snapshots").iterdir(), key=os.path.getmtime)
                if not snapshots:
                    raise NodeError(f"Unable to recover {system_folder} sstables: snapshot is not found")
                snapshot_folder_name = snapshots[0].name

            for recover_table in node_system_ks_directory.iterdir():
                if not recover_table.is_dir():
                    continue
                recover_keyspace_snapshot = recover_table / "snapshots" / snapshot_folder_name
                if not recover_keyspace_snapshot.exists():
                    continue
                for the_file in recover_table.iterdir():
                    if the_file.is_file():
                        the_file.unlink()
                for the_file in recover_keyspace_snapshot.iterdir():
                    if the_file.is_file():
                        shutil.copy2(the_file, recover_table / the_file.name)

    def upgrade(self, upgrade_version: str, recover_system_tables: bool = False) -> None:
        node = self.node
        install = scylla_repository.install(upgrade_version)
        exe = node.cluster.exe_for(install)

        self.scylla_version_for_upgrade = upgrade_version
        version_before = node.node_scylla_version
        version_after = node.get_node_scylla_version(exe)
        node.info(f"Upgrading from Scylla {version_before} to {version_after} ({upgrade_version})")

        if node.is_running():
            # ccm snapshots before every switch so that a later rollback has
            # system tables to restore from.
            node.nodetool("snapshot")
            node.stop(wait_other_notice=True)
            if node.is_running():
                raise NodeUpgradeError(f"Node {node.name} failed to stop before upgrade")

        node.cluster.manager.server_switch_executable(server_id=node.server_id, path=exe)
        node._node_scylla_version = None  # noqa: SLF001  -- the executable changed under it

        if recover_system_tables:
            self._recover_system_tables()

        try:
            node.start(wait_other_notice=True, wait_for_binary_proto=True)
        except Exception as exc:
            raise NodeUpgradeError(f"Node {node.name} failed to start after upgrade. Error: {exc}") from exc

        self.install_dir_for_upgrade = str(install.install_dir)
        if node.node_scylla_version != version_after:
            raise NodeUpgradeError(
                f"Node {node.name} hasn't been upgraded. Expected version after upgrade:"
                f" {version_after}, got: {node.node_scylla_version}"
            )
        node.info(f"Upgraded from Scylla {version_before} to {node.node_scylla_version}")
        node.upgraded = True
