#!/usr/bin/python3
#
# Copyright (C) 2024-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#
"""Helpers for running nested containers (podman-in-podman) in the toolchain."""

import asyncio
import logging
import shutil
import subprocess
from typing import Optional

logger = logging.getLogger(__name__)

CONTAINER_RUNTIME = 'podman'

# Image references for nested containers
CASSANDRA_STRESS_IMAGE = 'ghcr.io/scylladb/cassandra-stress:latest'
MINIO_IMAGE = 'ghcr.io/scylladb/minio:latest'
ANTLR3_IMAGE = 'ghcr.io/scylladb/antlr3:latest'


def _runtime() -> str:
    """Return the container runtime binary path."""
    runtime = shutil.which(CONTAINER_RUNTIME)
    if runtime is None:
        raise RuntimeError(f"{CONTAINER_RUNTIME} not found in PATH")
    return runtime


def build_image(containerfile_dir: str, tag: str) -> None:
    """Build a container image from a Containerfile directory."""
    subprocess.check_call([
        _runtime(), 'build', '-t', tag, '-f', 'Containerfile', '.'
    ], cwd=containerfile_dir)


def ensure_image(image: str, containerfile_dir: Optional[str] = None) -> None:
    """Ensure image is available locally; build from Containerfile if not pullable."""
    try:
        subprocess.check_call(
            [_runtime(), 'image', 'exists', image],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        if containerfile_dir:
            logger.info(f"Building image {image} from {containerfile_dir}")
            build_image(containerfile_dir, image)
        else:
            logger.info(f"Pulling image {image}")
            subprocess.check_call([_runtime(), 'pull', image])


async def run_container(image: str, args: list[str],
                        name: Optional[str] = None,
                        network: str = 'host',
                        env: Optional[dict[str, str]] = None,
                        volumes: Optional[list[str]] = None,
                        detach: bool = False,
                        remove: bool = True,
                        stdout=None, stderr=None) -> asyncio.subprocess.Process:
    """Run a container with the given arguments.

    Args:
        image: Container image to run.
        args: Arguments to pass to the container entrypoint.
        name: Optional container name.
        network: Network mode (default: host).
        env: Environment variables to set.
        volumes: Volume mounts in format 'host:container'.
        detach: Run in background.
        remove: Remove container after exit.
        stdout: stdout redirect for the process.
        stderr: stderr redirect for the process.

    Returns:
        The subprocess.Process handle.
    """
    cmd = [_runtime(), 'run']
    if name:
        cmd.extend(['--name', name])
    if network:
        cmd.extend(['--network', network])
    if remove:
        cmd.append('--rm')
    if detach:
        cmd.append('-d')
    if env:
        for k, v in env.items():
            cmd.extend(['-e', f'{k}={v}'])
    if volumes:
        for vol in volumes:
            cmd.extend(['-v', vol])
    cmd.append(image)
    cmd.extend(args)

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=stdout,
        stderr=stderr,
    )
    return proc


def run_container_sync(image: str, args: list[str],
                       network: str = 'host',
                       env: Optional[dict[str, str]] = None,
                       volumes: Optional[list[str]] = None,
                       remove: bool = True,
                       **kwargs) -> subprocess.CompletedProcess:
    """Run a container synchronously.

    Returns:
        subprocess.CompletedProcess
    """
    cmd = [_runtime(), 'run']
    if network:
        cmd.extend(['--network', network])
    if remove:
        cmd.append('--rm')
    if env:
        for k, v in env.items():
            cmd.extend(['-e', f'{k}={v}'])
    if volumes:
        for vol in volumes:
            cmd.extend(['-v', vol])
    cmd.append(image)
    cmd.extend(args)

    return subprocess.run(cmd, **kwargs)


def stop_container(name: str) -> None:
    """Stop a running container by name."""
    subprocess.run([_runtime(), 'stop', name],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def kill_container(name: str) -> None:
    """Kill a running container by name."""
    subprocess.run([_runtime(), 'kill', name],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def rm_container(name: str) -> None:
    """Remove a container by name."""
    subprocess.run([_runtime(), 'rm', '-f', name],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def mc_in_container(minio_image: str, args: list[str],
                    config_dir: Optional[str] = None,
                    container_name: Optional[str] = None,
                    stdout=None, stderr=None) -> None:
    """Run the minio client (mc) command.

    If a container_name is provided, uses 'podman exec' to run mc inside the
    existing minio container (avoids starting a second nested container).
    Otherwise starts a new container.
    """
    if container_name:
        cmd = [_runtime(), 'exec', container_name, 'mc']
        cmd.extend(args)
        subprocess.check_call(cmd, stdout=stdout, stderr=stderr)
        return

    cmd = [_runtime(), 'run', '--rm', '--network', 'host']
    if config_dir:
        cmd.extend(['-v', f'{config_dir}:/root/.mc'])
    cmd.extend(['--entrypoint', 'mc', minio_image])
    cmd.extend(args)
    subprocess.check_call(cmd, stdout=stdout, stderr=stderr)
