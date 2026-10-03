#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""A small Apache Cassandra cluster, run in Docker containers.

A handful of dtests migrate sstables between Scylla and a real Apache
Cassandra: they write data with one and read it back with the other, which is
the only way to prove the on-disk formats really interoperate. scylla-dtest
gets that Cassandra from ccm, which unpacks a Cassandra tarball and runs it
from a work directory, and needs a matching JDK on the host. There is no ccm
in this tree, so the cluster comes from the official `cassandra` image
instead; the JVM and its JDK live in the container.

Only the slice of the ccm cluster/node API the migration tests use is
implemented: populate/start/stop, nodelist, nodetool, flush, and a node work
directory laid out like ccm's, so `tools.files.get_node_cf_dir()` finds the
table directories. Each node's data directory is a bind mount, so the test
can copy sstables into it from the host and then `nodetool refresh`.

The containers run as the calling user (the image's /var/lib/cassandra,
/var/log/cassandra and /etc/cassandra are all world-writable, and the image's
entrypoint only drops privileges when it starts as root), so the sstables
Cassandra writes are owned by the test and the sstables the test copies in
are readable by Cassandra.
"""

from __future__ import annotations

import functools
import glob
import logging
import os
import re
import shutil
import time
from typing import TYPE_CHECKING

import yaml
from docker.errors import DockerException, ImageNotFound, NotFound

from ccmlib.scylla_node import NodetoolError, ToolError
from tools.docker_utils import (
    container_exec_run,
    container_remove,
    dump_container_logs,
    get_docker_client,
    get_ip_address_of_container,
)
from tools.docker_versions import get_docker_version

if TYPE_CHECKING:
    from typing import Any

logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=1)
def cassandra_docker_available() -> bool:
    """Whether the Docker daemon this harness runs Cassandra in is reachable.

    The in-tree counterpart of scylla-dtest's java_version_exist(8): ccm ran
    Cassandra 3.x from a tarball on the host's JDK 8, this harness runs its
    official image instead.
    """
    import docker  # noqa: PLC0415 -- only the tests that need Cassandra pay for it

    try:
        docker.from_env(timeout=10).ping()
    except DockerException:
        return False
    return True

# The JVM sizes itself off the host's memory, which on a test machine means a
# multi-GB heap per node for a cluster holding a few thousand rows. Four nodes
# of that would not fit alongside the Scylla cluster the test also runs.
DEFAULT_MAX_HEAP_SIZE = "1G"
DEFAULT_HEAP_NEWSIZE = "256M"
DEFAULT_MEM_LIMIT = "2g"

# One rack is enough: the migrated keyspaces replicate per datacenter, and with
# as many Cassandra nodes as Scylla nodes every node is a replica anyway.
DEFAULT_RACK = "rack1"

# Cassandra takes tens of seconds to open the native transport on a cold JVM,
# and gossip between nodes settles after that.
BINARY_PROTO_TIMEOUT = 600
OTHER_NOTICE_TIMEOUT = 600


class CassandraDockerNode:
    """One Cassandra node: a container plus a ccm-shaped work directory."""

    def __init__(self, cluster: CassandraDockerCluster, name: str):
        self.cluster = cluster
        self.name = name
        self.container = None
        self._ip = None

        self.workdir = os.path.join(cluster.get_path(), name)
        for subdir in ("data", "logs", "conf"):
            os.makedirs(os.path.join(self.workdir, subdir), exist_ok=True)

        self.network_interfaces = {"storage": (None, 7000), "binary": (None, 9042)}

    # -- ccm node API -----------------------------------------------------

    def get_path(self) -> str:
        """Return the path to this node's top level directory (where config/data is stored)."""

        return self.workdir

    def address(self) -> str:
        if self._ip is None:
            raise RuntimeError(f"{self.name} is not started, it has no address yet")
        return self._ip

    def is_running(self) -> bool:
        if self.container is None:
            return False
        try:
            self.container.reload()
        except NotFound:
            return False
        return self.container.status == "running"

    is_live = is_running

    def nodetool(self, cmd: str, capture_output: bool = True, timeout: int | float | None = None, **kwargs) -> tuple[str, str]:
        """Run nodetool inside the container. Signature follows ScyllaNode.nodetool()."""

        del capture_output, timeout, kwargs  # the container runs it synchronously
        exit_code, output = self._exec(["nodetool", *cmd.split()])
        if exit_code:
            raise NodetoolError(f"nodetool {cmd}", exit_code, output, "")
        return output, ""

    def flush(self, ks: str | None = None, table: str | None = None, **kwargs) -> None:
        del kwargs
        cmd = "flush"
        if ks:
            cmd += f" {ks}"
            if table:
                cmd += f" {table}"
        self.nodetool(cmd)

    def is_scylla(self) -> bool:
        return False

    def get_cassandra_version(self) -> str:
        return self.cluster.version

    def get_sstablespath(self, keyspace: str, tables: list[str] | None = None, **kwargs) -> list[str]:
        """Return the -Data.db paths of the given tables, as ccm's Node.get_sstablespath() does.

        The data directory is a bind mount, so these are host paths and can be
        handed to a tool running outside the container.
        """
        del kwargs
        ks_dir = os.path.join(self.workdir, "data", keyspace.replace('"', ""))
        files = []
        for table in tables or ["*"]:
            pattern = os.path.join(ks_dir, f"{table.replace(chr(34), '')}-*", "*-Data.db")
            files += sorted(glob.glob(pattern))
        return files

    def _log_path(self, filename: str | None = None) -> str:
        return os.path.join(self.workdir, "logs", filename or "system.log")

    def mark_log(self, filename: str | None = None) -> int:
        path = self._log_path(filename)
        if not os.path.exists(path):
            return 0
        return os.path.getsize(path)

    def watch_log_for(self, exprs: str | list[str], from_mark: int | None = None, timeout: float = 600, filename: str | None = None, **kwargs) -> list[str]:
        """Wait until every expression has matched a line of the log after `from_mark`."""

        del kwargs
        if isinstance(exprs, str):
            exprs = [exprs]
        patterns = [re.compile(e) for e in exprs]
        path = self._log_path(filename)
        deadline = time.time() + timeout
        matches: list[str] = []
        while time.time() < deadline:
            if os.path.exists(path):
                with open(path, encoding="utf-8", errors="replace") as f:
                    f.seek(from_mark or 0)
                    for line in f:
                        for pattern in list(patterns):
                            if pattern.search(line):
                                matches.append(line)
                                patterns.remove(pattern)
                if not patterns:
                    return matches
            time.sleep(1)
        raise TimeoutError(f"{self.name}: {[p.pattern for p in patterns]} not found in {path} within {timeout}s")

    def grep_log(self, expr: str) -> list[str]:
        log = os.path.join(self.workdir, "logs", "system.log")
        if not os.path.exists(log):
            return []
        pattern = re.compile(expr)
        with open(log, encoding="utf-8", errors="replace") as f:
            return [line for line in f if pattern.search(line)]

    def stop(self, gently: bool = True, **kwargs) -> None:
        del kwargs
        if self.container is None:
            return
        try:
            if gently:
                self.container.stop(timeout=120)
            else:
                self.container.kill()
        except NotFound:
            pass

    # -- container plumbing -----------------------------------------------

    def _exec(self, cmd: list[str]) -> tuple[int, str]:
        if not self.is_running():
            dump_container_logs(self.container)
            raise ToolError(" ".join(cmd), 1, f"container for {self.name} is not running", "")
        result = container_exec_run(self.container, cmd, demux=False)
        output = result.output.decode("utf-8", errors="replace") if result.output else ""
        return result.exit_code, output

    def start_container(self, seeds: list[str]) -> None:
        client = self.cluster.docker_client
        image = self.cluster.image
        self._write_config()

        environment = {
            "MAX_HEAP_SIZE": DEFAULT_MAX_HEAP_SIZE,
            "HEAP_NEWSIZE": DEFAULT_HEAP_NEWSIZE,
            "CASSANDRA_CLUSTER_NAME": self.cluster.name,
        }
        if self.cluster.datacenter:
            environment["CASSANDRA_ENDPOINT_SNITCH"] = "GossipingPropertyFileSnitch"
            environment["CASSANDRA_DC"] = self.cluster.datacenter
            environment["CASSANDRA_RACK"] = DEFAULT_RACK
        if seeds:
            environment["CASSANDRA_SEEDS"] = ",".join(seeds)

        logger.debug(f"Starting Cassandra container for {self.name} from {image} (seeds={seeds})")
        self.container = client.containers.run(
            image=image,
            name=f"{self.cluster.container_prefix}-{self.name}",
            detach=True,
            labels=["dtest"],
            user=f"{os.getuid()}:{os.getgid()}",
            environment=environment,
            mem_limit=DEFAULT_MEM_LIMIT,
            volumes={
                os.path.join(self.workdir, "data"): {"bind": "/var/lib/cassandra/data", "mode": "rw"},
                os.path.join(self.workdir, "logs"): {"bind": "/var/log/cassandra", "mode": "rw"},
                os.path.join(self.workdir, "conf", "cassandra.yaml"): {"bind": "/etc/cassandra/cassandra.yaml", "mode": "rw"},
            },
        )
        self.container.reload()
        self._ip = get_ip_address_of_container(self.container)
        if not self._ip:
            dump_container_logs(self.container)
            raise RuntimeError(f"Cassandra container for {self.name} has no IP address")
        self.network_interfaces = {name: (self._ip, port) for name, (_, port) in self.network_interfaces.items()}
        logger.debug(f"Cassandra container for {self.name} is at {self._ip}")

    def _write_config(self) -> None:
        """Write this node's cassandra.yaml: the image's own, plus the test's options.

        The image's entrypoint only understands a fixed set of CASSANDRA_*
        environment variables, so anything else a test asks for -- the
        `hinted_handoff_enabled: false` the migration tests set, say -- has to
        go into the file. The entrypoint then edits the addresses into this
        same file, in place, after the bind mount is in effect.

        The options are edited in line by line rather than through a
        load/dump round trip, because the entrypoint finds the addresses by
        `^(# )?<key>:` and several of them -- broadcast_rpc_address above all --
        ship commented out. A round trip drops those comments, the entrypoint's
        edit then matches nothing, and Cassandra refuses to start with a
        wildcard rpc_address and no broadcast_rpc_address.
        """
        lines = self.cluster.default_config_yaml.splitlines(keepends=True)
        for key, value in self.cluster.config_options.items():
            rendered = yaml.safe_dump({key: value}, default_flow_style=False)
            pattern = re.compile(rf"^(# )?{re.escape(key)}:")
            for i, line in enumerate(lines):
                if pattern.match(line):
                    lines[i] = rendered
                    break
            else:
                lines.append(rendered)
        path = os.path.join(self.workdir, "conf", "cassandra.yaml")
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(lines)

    def wait_for_binary_proto(self, timeout: float = BINARY_PROTO_TIMEOUT) -> None:
        deadline = time.time() + timeout
        last = ""
        while time.time() < deadline:
            if not self.is_running():
                dump_container_logs(self.container)
                raise RuntimeError(f"Cassandra container for {self.name} died while starting")
            exit_code, output = self._exec(["nodetool", "statusbinary"])
            # "not running" also contains "running", so match the whole word:
            # a node that is still bootstrapping reports exactly "not running".
            last = output.strip()
            if exit_code == 0 and last.splitlines()[-1:] == ["running"]:
                logger.debug(f"{self.name} native transport is up")
                return
            time.sleep(2)
        dump_container_logs(self.container)
        raise TimeoutError(f"{self.name} did not open its native transport within {timeout}s (last: {last!r})")

    def wait_for_nodes_up(self, count: int, timeout: float = OTHER_NOTICE_TIMEOUT) -> None:
        deadline = time.time() + timeout
        seen = 0
        while time.time() < deadline:
            _, output = self._exec(["nodetool", "status"])
            seen = sum(1 for line in output.splitlines() if line.startswith("UN"))
            if seen >= count:
                return
            time.sleep(2)
        raise TimeoutError(f"{self.name} sees only {seen} of {count} nodes up after {timeout}s")

    def remove(self) -> None:
        if self.container is None:
            return
        try:
            container_remove(self.container, force=True, v=True)
        except (DockerException, NotFound) as e:  # noqa: BLE001
            logger.warning(f"Failed to remove the Cassandra container for {self.name}: {e}")
        self.container = None


class CassandraDockerCluster:
    """A Cassandra cluster of `cassandra` containers, with the bits of ccm's Cluster the migration tests use."""

    def __init__(self, version: str, workdir: str, name: str = "test", datacenter: str | None = None):
        self.version = version
        self.name = name
        self.workdir = workdir
        # The keyspaces migrated from Scylla name Scylla's datacenter in their
        # NetworkTopologyStrategy, so Cassandra has to answer to the same name
        # or nothing is a replica of anything.
        self.datacenter = datacenter
        self.config_options: dict[str, Any] = {}
        self._nodes: list[CassandraDockerNode] = []
        self.container_prefix = f"dtest-cassandra-{os.getpid()}-{id(self):x}"
        self.image = self._resolve_image(version)
        self._default_config_yaml: str | None = None

        os.makedirs(self.workdir, exist_ok=True)

    @staticmethod
    def _resolve_image(version: str) -> str:
        """Pick the image for `version`.

        values_docker_versions.yaml pins an exact tag, which is what dependabot
        bumps; a test asking for the series it belongs to ("3.11") gets that
        pin, and anything else gets the tag it asked for.
        """
        pinned = get_docker_version("cassandra")
        pinned_tag = pinned.rsplit(":", maxsplit=1)[-1]
        if pinned_tag == version or pinned_tag.startswith(f"{version}."):
            return pinned
        return f"cassandra:{version}"

    @property
    def docker_client(self):
        return get_docker_client()

    @property
    def default_config_yaml(self) -> str:
        """The image's own cassandra.yaml, read once per cluster."""

        if self._default_config_yaml is None:
            self._pull_image()
            # The image's entrypoint execs anything that is not `cassandra`.
            self._default_config_yaml = self.docker_client.containers.run(
                image=self.image,
                command=["cat", "/etc/cassandra/cassandra.yaml"],
                remove=True,
                labels=["dtest"],
            ).decode("utf-8")
        return self._default_config_yaml

    def _pull_image(self) -> None:
        if not self.docker_client.images.list(filters={"reference": self.image}):
            logger.info(f"Pulling {self.image}")
            try:
                self.docker_client.images.pull(self.image)
            except ImageNotFound as e:
                raise RuntimeError(f"Cassandra image {self.image} is not available") from e

    # -- ccm cluster API ---------------------------------------------------

    def get_path(self) -> str:
        return self.workdir

    def nodelist(self) -> list[CassandraDockerNode]:
        return list(self._nodes)

    @property
    def nodes(self) -> dict[str, CassandraDockerNode]:
        return {node.name: node for node in self._nodes}

    def set_configuration_options(self, values: dict[str, Any] | None = None, **kwargs) -> CassandraDockerCluster:
        del kwargs  # batch_commitlog and friends are not used against Cassandra here
        if values:
            self.config_options.update(values)
        return self

    def populate(self, nodes: int) -> CassandraDockerCluster:
        for i in range(1, nodes + 1):
            self._nodes.append(CassandraDockerNode(cluster=self, name=f"node{i}"))
        return self

    def start(self, wait_for_binary_proto: bool = True, wait_other_notice: bool = True, **kwargs) -> CassandraDockerCluster:
        """Start every node.

        The first node has to come up before the rest, because it is the seed
        and its address is only known once its container exists.
        """
        del kwargs
        self._pull_image()
        seeds: list[str] = []
        for node in self._nodes:
            node.start_container(seeds=seeds)
            if not seeds:
                # The seed has to be serving before the others gossip to it,
                # whatever the caller asked for.
                node.wait_for_binary_proto()
                seeds = [node.address()]
            elif wait_for_binary_proto:
                node.wait_for_binary_proto()
        if wait_other_notice and len(self._nodes) > 1:
            for node in self._nodes:
                node.wait_for_nodes_up(len(self._nodes))
        return self

    def stop(self, gently: bool = True, **kwargs) -> None:
        del kwargs
        for node in self._nodes:
            node.stop(gently=gently)

    def flush(self) -> None:
        for node in self._nodes:
            if node.is_running():
                node.flush()

    def remove(self) -> None:
        """Remove the containers and the work directory. Safe to call twice."""

        for node in self._nodes:
            node.remove()
        if os.path.exists(self.workdir):
            shutil.rmtree(self.workdir, ignore_errors=True)

    def check_errors(self) -> list[str]:
        """Return the ERROR lines in every node's system.log."""

        errors = []
        for node in self._nodes:
            errors += [f"{node.name}: {line.rstrip()}" for line in node.grep_log(r"\bERROR\b")]
        return errors
