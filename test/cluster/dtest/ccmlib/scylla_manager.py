#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""A Scylla Manager server for the in-tree dtest cluster shim.

This is the in-tree counterpart of ccm's `ccmlib.scylla_cluster.ScyllaManager`.
It runs the real `scylla-manager` binary from a relocatable package (see
ccmlib.scylla_repository.setup_scylla_manager) against the test cluster, and
provides the `sctool` entry point the manager tests drive it through.

Layout, all under `<cluster path>/scylla-manager/`:

    bin/{sctool,scylla-manager,scylla-manager-agent}  symlinks into the package
    scylla-manager.yaml                               server config
    scylla-manager-agent.crt/.key                     TLS pair shared by the agents
    scylla-manager.log                                server log
    scylla-manager.pid

The server binds to node1's IP so that parallel clusters, which each get their
own loopback addresses, never fight over a port.  It keeps its metadata in the
test cluster itself (`database.hosts` points at node1), exactly as ccm does.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

from ruamel.yaml import YAML

from test.cluster.dtest.ccmlib.common import (
    BIN_DIR,
    SCYLLAMANAGER_AGENT_CONF,
    SCYLLAMANAGER_CONF,
    SCYLLAMANAGER_DIR,
    CCMError,
    check_socket_listening,
    parse_interface,
)
from test.cluster.dtest.ccmlib.utils.version import ComparableScyllaVersion

if TYPE_CHECKING:
    from test.cluster.dtest.ccmlib.scylla_cluster import ScyllaCluster
    from test.cluster.dtest.ccmlib.scylla_node import ScyllaNode


logger = logging.getLogger(__name__)

# Ports, all bound to a node's own IP address.  The server shares node1's
# address with node1's own agent, so it may not reuse the agent's ports.
MANAGER_API_PORT = 5080
MANAGER_PROMETHEUS_PORT = 56091
MANAGER_DEBUG_PORT = 5611
AGENT_API_PORT = 10001
AGENT_PROMETHEUS_PORT = 56090
AGENT_DEBUG_PORT = 56112

MANAGER_START_TIMEOUT = 300

_BINARIES = ("sctool", "scylla-manager", "scylla-manager-agent")


class ScyllaManagerError(CCMError):
    ...


class ScyllaManager:
    def __init__(self, scylla_cluster: ScyllaCluster, install_dir: str | Path):
        self.scylla_cluster = scylla_cluster
        self.install_dir = Path(install_dir)
        self.auth_token = str(uuid.uuid4())
        self._process = None
        self._install()

    # ---------------------------------------------------------------- paths

    def _get_path(self) -> str:
        return os.path.join(self.scylla_cluster.get_path(), SCYLLAMANAGER_DIR)

    def _get_bin(self, name: str) -> str:
        return os.path.join(self._get_path(), BIN_DIR, name)

    def _get_conf_file(self) -> str:
        return os.path.join(self._get_path(), SCYLLAMANAGER_CONF)

    def _get_pid_file(self) -> str:
        return os.path.join(self._get_path(), "scylla-manager.pid")

    def _get_log_file(self) -> str:
        return os.path.join(self._get_path(), "scylla-manager.log")

    @property
    def agent_tls_cert_file(self) -> str:
        return os.path.join(self._get_path(), "scylla-manager-agent.crt")

    @property
    def agent_tls_key_file(self) -> str:
        return os.path.join(self._get_path(), "scylla-manager-agent.key")

    @property
    def is_agent_available(self) -> bool:
        return os.path.exists(self._get_bin("scylla-manager-agent"))

    # ------------------------------------------------------------- install

    def _install(self) -> None:
        """Lay out bin/ and the TLS pair the agents share.

        The binaries are symlinked rather than copied: the three of them are
        over 100 MB together and every test would otherwise pay for a copy.
        """

        bin_dir = Path(self._get_path()) / BIN_DIR
        bin_dir.mkdir(parents=True, exist_ok=True)
        for name in _BINARIES:
            src = self.install_dir / name
            if not src.is_file():
                raise ScyllaManagerError(f"{src} not found in the Scylla Manager install dir")
            dst = bin_dir / name
            if not dst.exists():
                dst.symlink_to(src)

        # The agents serve their API over HTTPS. The manager does not validate
        # the certificate, so a self-signed one generated per cluster is enough
        # (ccm ships a fixed, long-expired pair for the same reason).
        if not os.path.exists(self.agent_tls_cert_file):
            from test.cluster.dtest.tools.sslkeygen import create_self_signed_x509_certificate

            create_self_signed_x509_certificate(
                test_path=self._get_path(),
                cert_file="scylla-manager-agent.crt",
                key_file="scylla-manager-agent.key",
                cname="scylla-manager-agent",
            )

    # -------------------------------------------------------------- config

    def _get_api_address(self) -> str:
        return f"{self.scylla_cluster.get_node_ip(1)}:{MANAGER_API_PORT}"

    def _update_config(self) -> None:
        """(Re)write the server config against the cluster as it is right now.

        Called on every start(), because the addresses it needs only exist once
        the cluster has been populated.
        """

        node_ip = self.scylla_cluster.get_node_ip(1)
        yaml = YAML()
        conf_file = self._get_conf_file()

        data = {}
        if os.path.exists(conf_file):
            with open(conf_file) as f:
                data = yaml.load(f) or {}

        data["http"] = f"{node_ip}:{MANAGER_API_PORT}"
        data["prometheus"] = f"{node_ip}:{MANAGER_PROMETHEUS_PORT}"
        data["debug"] = f"{node_ip}:{MANAGER_DEBUG_PORT}"
        data["logger"] = {"mode": "stderr", "level": "info"}
        database = data.get("database") or {}
        database["hosts"] = [node_ip]
        # As ccm does.  With the manager's default of 1 each piece of its
        # metadata lives on a single node (and is read at ONE), so a test that
        # takes a node down -- drain, stop, decommission -- takes whatever
        # tasks and runs that node held with it.
        database["replication_factor"] = 3
        data["database"] = database

        with open(conf_file, "w") as f:
            yaml.dump(data, f)

    # ------------------------------------------------------------ lifecycle

    def start(self) -> None:
        self._update_config()

        if self._process and self._process.poll() is None:
            return

        Path(self._get_pid_file()).unlink(missing_ok=True)

        args = [self._get_bin("scylla-manager"), "--config-file", self._get_conf_file()]
        logger.debug(f"Starting Scylla Manager: {args}")
        with open(self._get_log_file(), "a") as log:
            self._process = subprocess.Popen(args, stdout=log, stderr=log, close_fds=True)
        with open(self._get_pid_file(), "w") as pid_file:
            pid_file.write(str(self._process.pid))

        api_interface = parse_interface(self._get_api_address(), MANAGER_API_PORT)
        if not check_socket_listening(api_interface, timeout=MANAGER_START_TIMEOUT):
            raise ScyllaManagerError(
                f"scylla-manager API {api_interface[0]}:{api_interface[1]} is not listening after "
                f"{MANAGER_START_TIMEOUT}s; see {self._get_log_file()}"
            )

    def stop(self, gently: bool = True) -> None:
        if not self._process:
            return
        try:
            self._process.send_signal(signal.SIGTERM if gently else signal.SIGKILL)
            self._process.wait(timeout=60)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait(timeout=60)
        except OSError:
            pass
        self._process = None

    # ---------------------------------------------------------------- tools

    def _run(self, args: list[str], ignore_exit_status: bool = False, timeout: float = 300) -> tuple[str, str]:
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        try:
            stdout, stderr = p.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            p.kill()
            stdout, stderr = p.communicate()
            raise ScyllaManagerError(
                " ".join(args), -1, stdout, f"timed out after {timeout}s and was killed: {stderr}"
            ) from None
        if p.returncode != 0 and not ignore_exit_status:
            raise ScyllaManagerError(" ".join(args), p.returncode, stdout, stderr)
        return stdout, stderr

    @property
    def version(self) -> ComparableScyllaVersion:
        """The sctool client's version.

        `sctool version` prints "Client version: X" and, when the server
        answers, "Server version: Y" below it; the two match here because both
        come out of the same relocatable.
        """

        stdout, _ = self.sctool(["version"], ignore_exit_status=True)
        for line in stdout.splitlines():
            if ":" in line:
                return ComparableScyllaVersion(line.split(":", 1)[1].strip())
        raise ScyllaManagerError(f"could not read a version out of `sctool version`: {stdout!r}")

    def sctool(self, cmd: list[str], ignore_exit_status: bool = False) -> tuple[str, str]:
        args = [self._get_bin("sctool"), "--api-url", f"http://{self._get_api_address()}/api/v1", *cmd]
        return self._run(args, ignore_exit_status=ignore_exit_status)

    def agent_check_location(self, location_list: list[str], extra_config_file_list: list[str] | None = None) -> tuple[str, str]:
        args = [self._get_bin("scylla-manager-agent"), "check-location", "-L", ",".join(location_list)]
        for config_file in extra_config_file_list or ():
            args.extend(["-c", config_file])
        return self._run(args)

    def agent_download_files(  # noqa: PLR0913
        self,
        node: ScyllaNode,
        location_list: list[str],
        snapshot_tag: str,
        keyspace_filter_list: list[str] | None = None,
        dry_run: bool = False,
    ) -> tuple[str, str]:
        """Run `scylla-manager-agent download-files` on `node`'s behalf.

        Downloads a backed-up snapshot into the node's upload directory.
        `keyspace_filter_list` takes glob patterns; `dry_run` only lists what
        would be downloaded, with the size of each table's snapshot.
        """

        agent_config_file = os.path.join(node.get_conf_dir(), SCYLLAMANAGER_AGENT_CONF)
        args = [
            self._get_bin("scylla-manager-agent"),
            "download-files",
            "-c", agent_config_file,
            "-L", ",".join(location_list),
            "-T", snapshot_tag,
            "-n", node.hostid(),
            "-d", os.path.join(node.get_path(), "data"),
            "--mode", "upload",
        ]
        if keyspace_filter_list:
            args.extend(["-K", ",".join(keyspace_filter_list)])
        if dry_run:
            args.append("--dry-run")
        return self._run(args)
