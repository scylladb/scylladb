#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import logging
import os
import shutil
import subprocess
import time
from contextlib import suppress
from functools import cache

from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError as AwsClientError
from docker.errors import APIError, DockerException, NotFound
from filelock import FileLock

import docker
from tools.keystore import KeyStore
from tools.retrying import retrying

LOGGER = logging.getLogger(__name__)

# Store Docker client per (process ID, timeout) to handle pytest-xdist forking
_docker_clients: dict[tuple[int, int], docker.DockerClient] = {}


def running_in_docker():
    path = "/proc/self/cgroup"
    with open(path) as cgroup:
        return os.path.exists("/.dockerenv") or (os.path.isfile(path) and any("docker" in line for line in cgroup))


@cache
def running_in_podman():
    return os.getenv("container") == "podman"


def _podman_docker_host() -> str | None:
    """The Docker API served by rootless podman, started here when it is not running yet.

    A host without a Docker daemon (CI's test.py stage runs in a podman container) still
    has podman, which test.pylib's DockerizedService uses through its command line.
    `podman system service` serves the Docker API on a socket, so the docker client below
    works unchanged; the service is shared by all test processes of the user and exits
    after ten idle minutes.

    Its containers run without SELinux labels, as a Docker daemon's usually do: the tests
    bind-mount files from the source tree (cassandra-stress profiles, the local-kms seed),
    which a labelled container cannot read on an enforcing host, and relabelling them
    (`:z`) would change the tree's own files.
    """
    podman = shutil.which("podman")
    if podman is None:
        return None
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR") or "/tmp"
    socket_path = os.path.join(runtime_dir, f"scylla-dtest-podman-{os.getuid()}.sock")
    url = f"unix://{socket_path}"
    conf_path = os.path.join(runtime_dir, f"scylla-dtest-podman-{os.getuid()}.conf")

    def serving() -> bool:
        try:
            docker.DockerClient(base_url=url, timeout=10).ping()
        except DockerException:
            return False
        return True

    with FileLock(f"{socket_path}.lock"):
        if not serving():
            LOGGER.info("no Docker daemon reachable; starting the podman Docker API service on %s", url)
            with open(conf_path, "w") as conf:
                conf.write("[containers]\nlabel = false\n")
            subprocess.Popen([podman, "system", "service", "--time=600", url],
                             env={**os.environ, "CONTAINERS_CONF_OVERRIDE": conf_path},
                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             start_new_session=True)
            deadline = time.monotonic() + 60
            while not serving():
                if time.monotonic() > deadline:
                    raise DockerException(f"podman Docker API service did not come up on {url}")
                time.sleep(0.5)
    return url


@retrying(num_attempts=10, sleep_time=1, allowed_exceptions=DockerException)
def get_docker_client(timeout=360):
    """
    Get a Docker client that is cached per-process.
    This is necessary because pytest-xdist forks processes and a cached
    Docker client from the parent process may not work correctly in child processes.

    Without a reachable Docker daemon the client talks to podman's Docker API instead,
    and DOCKER_HOST is set so that any later docker.from_env() in this process does too.

    :param timeout: timeout in seconds for each Docker API call made with this client;
        the default is larger than docker-py's 60s, which is too short on some builders
    """
    key = (os.getpid(), timeout)
    if key in _docker_clients:
        return _docker_clients[key]

    try:
        client = docker.from_env(timeout=timeout)
    except DockerException:
        if (host := _podman_docker_host()) is None:
            raise
        os.environ["DOCKER_HOST"] = host
        client = docker.DockerClient(base_url=host, timeout=timeout)
    LOGGER.info("docker client version: %s (pid=%d, timeout=%ds)", client.version(), key[0], timeout)
    try:
        creds = KeyStore().get_docker_hub_credentials()
        client.login(username=creds["username"], password=creds["password"], registry="https://index.docker.io/v1/")
    except (BotoCoreError, AwsClientError) as ex:
        LOGGER.warning("using docker client without login %s", ex)

    _docker_clients[key] = client
    return client


def get_ip_address_of_container(container):
    """
    Get the IP address of a Docker container.
    take into account https://docs.docker.com/engine/deprecated/#top-level-network-properties-in-networksettings

    :param container: Docker container object
    """
    return container.attrs["NetworkSettings"].get("IPAddress") or next(iter(container.attrs["NetworkSettings"]["Networks"].values())).get("IPAddress")


class ContainerNotRunningError(Exception):
    """Raised when a container is not running and cannot execute commands."""


def dump_container_logs(container, level=logging.ERROR, tail=200):
    """Dump container logs for post-mortem debugging when a container dies unexpectedly."""
    try:
        logs = container.logs(tail=tail).decode("utf-8", errors="replace")
        LOGGER.log(level, "Container %s logs:\n%s", container.name, logs)
    except Exception as e:  # noqa: BLE001
        LOGGER.warning("Failed to collect logs from container %s: %s", container.name, e)


def _container_is_running(container):
    """Reload and check the container's real status; transient API errors propagate to the caller's retry."""
    try:
        container.reload()
    except NotFound:
        return False
    return container.status == "running"


@retrying(num_attempts=5, sleep_time=1, allowed_exceptions=APIError)
def container_exec_run(container, *args, **kwargs):
    """Run a command in a container with retries on transient Docker API errors (e.g. 409 Conflict)."""
    try:
        return container.exec_run(*args, **kwargs)
    except APIError as e:
        # Docker/podman phrase the "container is not running" 409 differently across
        # versions, so check actual container state instead of matching error text.
        if e.status_code == 409 and not _container_is_running(container):
            dump_container_logs(container)
            raise ContainerNotRunningError(f"Container {container.name} is not running. Logs dumped above.") from e
        raise


@retrying(num_attempts=5, sleep_time=1, allowed_exceptions=APIError)
def container_reload(container):
    """Reload container state with retries on transient Docker API errors."""
    return container.reload()


@retrying(num_attempts=5, sleep_time=1, allowed_exceptions=APIError)
def container_remove(container, **kwargs):
    """Remove a container with retries on transient Docker API errors; an already-gone container counts as removed."""
    with suppress(NotFound):
        container.remove(**kwargs)


def cleanup_dtest_containers(label="dtest"):
    """
    Clean up all Docker containers created by dtest.
    This function removes all containers with the specified label (default: "dtest").
    Useful for cleaning up orphaned containers when tests are interrupted or fail.

    :param label: Docker label to filter containers (default: "dtest")
    :return: Number of containers removed
    """
    try:
        client = get_docker_client()
        # Find all containers with the dtest label
        containers = client.containers.list(all=True, filters={"label": label})
        removed_count = 0

        for container in containers:
            try:
                LOGGER.info("Removing dtest container: %s (id: %s)", container.name, container.short_id)
                container_remove(container, force=True)
                removed_count += 1
            except DockerException as e:
                LOGGER.warning("Failed to remove container %s: %s", container.name, e)

        if removed_count > 0:
            LOGGER.info("Cleaned up %d dtest container(s)", removed_count)

        return removed_count
    except DockerException as e:
        LOGGER.error("Failed to cleanup dtest containers: %s", e)
        return 0
