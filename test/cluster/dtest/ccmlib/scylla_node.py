#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from __future__ import annotations

import os
import re
import time
import locale
import subprocess
from itertools import chain
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

from test.cluster.dtest.ccmlib.common import ArgumentError, wait_for
from test.pylib.internal_types import ServerUpState
from test.pylib.manager_client import NoSuchProcess

if TYPE_CHECKING:
    from test.pylib.internal_types import ServerInfo
    from test.pylib.log_browsing import ScyllaLogFile
    from test.cluster.dtest.ccmlib.scylla_cluster import ScyllaCluster


NODETOOL_STDERR_IGNORED_PATTERNS = (
    re.compile(r"WARNING: debug mode. Not for benchmarking or production"),
    re.compile(
        r"==[0-9]+==WARNING: ASan doesn't fully support makecontext/swapcontext"
        r" functions and may produce false positives in some cases!"
    ),
)

CASSANDRA_OPTIONS_MAPPING = {
    "-Dcassandra.replace_address_first_boot": "--replace-address-first-boot",
}

DEFAULT_SMP = 2
DEFAULT_MEMORY_PER_CPU = 512 * 1024 * 1024  # bytes
DEFAULT_SCYLLA_LOG_LEVEL = "info"

KNOWN_LOG_LEVELS = {
    "TRACE": "trace",
    "DEBUG": "debug",
    "INFO": "info",
    "WARN": "warn",
    "ERROR": "error",
    "OFF": "info",
}


class NodeError(Exception):
    def __init__(self, msg: str, process: int | None = None):
        super().__init__(msg)
        self.process = process


class ToolError(Exception):
    def __init__(self, command: str, exit_status: int, stdout: Any = None, stderr: Any = None):
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


class ScyllaNode:
    def __init__(self, cluster: ScyllaCluster, server: ServerInfo, name: str):
        self.cluster = cluster
        self.server_id = server.server_id
        self.name = name
        self.pid = None
        self.all_pids = []
        self.network_interfaces = {
            "storage": (str(server.rpc_address), 7000),
            "binary": (str(server.rpc_address), 9042),
        }
        self.data_center = server.datacenter
        self.rack = server.rack

        self._smp_set_during_test = None
        self._smp = None
        self._memory = None

        self.__global_log_level = "info"
        self.__classes_log_level = {}

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

    def set_smp(self, smp: int) -> None:
        self._smp_set_during_test = smp

    def smp(self) -> int:
        return self._smp_set_during_test or self._smp or DEFAULT_SMP

    def memory(self) -> int:
        return self._memory or self.smp() * DEFAULT_MEMORY_PER_CPU

    def _adjust_smp_and_memory(self, smp: int | None = None, memory: int | None = None) -> None:
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

    def is_running(self) -> bool:
        try:
            return self.cluster.manager.server_get_returncode(server_id=self.server_id) is None
        except NoSuchProcess:
            return False

    is_live = is_running

    @cached_property
    def scylla_log_file(self) -> ScyllaLogFile:
        return self.cluster.manager.server_open_log(server_id=self.server_id)

    def grep_log(self,
                 expr: str,
                 filter_expr: str | None = None,
                 filename: str | None = None,  # not used in scylla-dtest
                 from_mark: int | None = None) -> list[tuple[str, re.Match[str]]]:
        assert filename is None, "only ScyllaDB's log is supported"

        return self.scylla_log_file.grep(expr=expr, filter_expr=filter_expr, from_mark=from_mark)

    def grep_log_for_errors(self,
                            filename: str | None = None,  # not used in scylla-dtest
                            distinct_errors: bool = False,
                            search_str: str | None = None,  # not used in scylla-dtest
                            case_sensitive: bool = True,  # not used in scylla-dtest
                            from_mark: int | None = None) -> list[str] | list[list[str]]:
        assert filename is None, "only ScyllaDB's log is supported"
        assert search_str is None, "argument `search_str` is not supported"
        assert case_sensitive, "only case sensitive search is supported"

        from_mark = getattr(self, "error_mark", None) if from_mark is None else from_mark

        return self.scylla_log_file.grep_for_errors(distinct_errors=distinct_errors, from_mark=from_mark)

    def mark_log_for_errors(self, filename: str | None = None) -> None:
        assert filename is None, "only ScyllaDB's log is supported"

        self.error_mark = self.mark_log()

    def mark_log(self, filename: str | None = None) -> int:
        assert filename is None, "only ScyllaDB's log is supported"

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
        assert filename is None, "only ScyllaDB's log is supported"
        assert polling_interval is None, "argument `polling_interval` is not supported"

        if isinstance(exprs, str):
            exprs = [exprs]

        _, matches = self.scylla_log_file.wait_for(*exprs, from_mark=from_mark, timeout=timeout)

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
        assert filename is None, "only ScyllaDB's log is supported"

        if not isinstance(nodes, list):
            nodes = [nodes]

        self.watch_log_for(
            [f"({node.address()}|{node.hostid()}).* now (dead|DOWN)" for node in nodes],
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
        assert filename is None, "only ScyllaDB's log is supported"

        if not isinstance(nodes, list):
            nodes = [nodes]

        self.watch_log_for(
            [f"({node.address()}|{node.hostid()}).* now UP" for node in nodes],
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

    def wait_for_binary_interface(self, from_mark: int | None = None, timeout: float | None = None) -> None:
        """Waits for the binary CQL interface to be listening."""

        if timeout is None:
            timeout = self.cluster.default_wait_for_binary_proto
        self.watch_log_for(exprs="Starting listening for CQL clients", from_mark=from_mark, timeout=timeout)

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
              quiet_start: bool | None = None) -> None:  # not used in scylla-dtest
        assert join_ring is None, "argument `join_ring` is not supported"
        assert verbose is None, "argument `verbose` is not supported"
        assert replace_token is None, "argument `replace_token` is not supported"
        assert profile_options is None, "argument `profile_options` is not supported"
        assert use_jna is None, "argument `use_jna` is not supported"
        assert quiet_start is None, "argument `quiet_start` is not supported"

        assert replace_address is None or replace_node_host_id is None, \
            "replace_address and replace_node_host_id cannot be specified together"

        if self.is_running():
            raise NodeError(f"{self.name} is already running")

        scylla_args = self._process_scylla_args(
            *(jvm_args or []),
            *(["--replace-address", replace_address] if replace_address else []),
            *(["--replace-node-first-boot", replace_node_host_id] if replace_node_host_id else []),
        )
        scylla_env = self._process_scylla_env()

        marks = []
        if wait_other_notice:
            marks = [(node, node.mark_log()) for node in self.cluster.nodelist() if node.is_live()]

        self.mark = self.mark_log()

        self.cluster.manager.server_start(
            server_id=self.server_id,
            expected_server_up_state=ServerUpState.PROCESS_STARTED,
            cmdline_options_override=scylla_args,
            append_env_override=scylla_env,
            connect_driver=False,
        )

        if wait_for_binary_proto is None:
            wait_for_binary_proto = self.cluster.force_wait_for_cluster_start and not no_wait
        if wait_other_notice is None:
            wait_other_notice = self.cluster.force_wait_for_cluster_start and not no_wait
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
        if not self.is_running():
            return False

        if marks is None:
            marks = [
                (node, node.mark_log())
                for node in other_nodes or self.cluster.nodelist()
                if node.server_id != self.server_id and node.is_live()
            ] if wait_other_notice else []

        if gently:
            self.cluster.manager.server_stop_gracefully(server_id=self.server_id)
        else:
            self.cluster.manager.server_stop(server_id=self.server_id)

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

        nodetool_cmd = [
            self.cluster.manager.server_get_exe(server_id=self.server_id),
            "nodetool",
            "-h",
            str(self.cluster.manager.get_host_ip(server_id=self.server_id)),
            *cmd.split(),
        ]

        if verbose:
            self.debug(f"nodetool cmd={nodetool_cmd} wait={wait} timeout={timeout}")

        if capture_output:
            p = subprocess.Popen(nodetool_cmd, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = p.communicate(timeout=timeout)
        else:
            p = subprocess.Popen(nodetool_cmd, universal_newlines=True)
            stdout, stderr = None, None

        if wait and p.wait(timeout=timeout):
            raise NodetoolError(" ".join(nodetool_cmd), p.returncode, stdout, stderr)

        stderr = "\n".join(
            line for line in stderr.splitlines()
            if self.debug(f"checking {line}") or not any(p.fullmatch(line) for p in NODETOOL_STDERR_IGNORED_PATTERNS)
        )

        return stdout, stderr

    def get_path(self) -> str:
        """Return the path to this node top level directory (where config/data is stored.)"""

        return self.cluster.manager.server_get_workdir(server_id=self.server_id)

    def flush(self, ks: str | None = None, table: str | None = None, **kwargs) -> None:
        cmd = ["flush"]
        if ks:
            cmd.append(ks)
        if table:
            cmd.append(table)
        self.nodetool(" ".join(cmd), **kwargs)

    def drain(self, block_on_log: bool = False) -> None:
        mark = self.mark_log()
        self.nodetool("drain")
        if block_on_log:
            self.watch_log_for("DRAINED", from_mark=mark)

    def repair(self, options: list[str] | None = None, **kwargs) -> tuple[str, str]:
        cmd = ["repair"]
        if options:
            cmd.extend(options)
        return self.nodetool(" ".join(cmd), **kwargs)

    def decommission(self) -> None:
        self.cluster.manager.decommission_node(server_id=self.server_id)

    def hostid(self, timeout: float | None = None, force_refresh: bool | None = None) -> str | None:
        assert timeout is None, "argument `timeout` is not supported"  # not used in scylla-dtest
        assert force_refresh is None, "argument `force_refresh` is not supported"  # not used in scylla-dtest

        try:
            return self.cluster.manager.get_host_id(server_id=self.server_id)
        except Exception as exc:
            self.error(f"Failed to get hostid: {exc}")

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
