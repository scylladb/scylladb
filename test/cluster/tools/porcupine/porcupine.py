#
# Copyright (C) 2026-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""Invoke the Porcupine-based linearizability checker.

The checker reads a JSON-lines history from stdin and outputs a JSON result to
stdout.  Its source code and container image are maintained at
https://github.com/scylladb/porcupine_validator -- it is not part of this
repository, only the image tag is, in the ``porcupine_image`` file next to this
module.

There are two ways to run it:

* **Container (the default, and what CI uses).**  The image tag is pinned in
  ``porcupine_image``, so every run checks the history with the same checker
  build, and nothing beyond a container runtime has to be installed.

* **Locally built Go binary (``PORCUPINE_USE_CONTAINER=0``).**  For working on
  the checker itself: with a checkout of porcupine_validator in
  ``porcupine_checker/`` next to this module, the binary is rebuilt whenever
  its sources are newer, so a change to the Go code takes effect on the next
  test run without rebuilding and republishing the image.  Also the way out
  when there is no container runtime at hand.  Needs a Go toolchain and that
  checkout -- the sources are not vendored here, so without them this mode
  raises FileNotFoundError.

Environment variables:

``PORCUPINE_USE_CONTAINER``
    ``0`` selects the locally built binary; anything else (default) the
    container.
``PORCUPINE_CONTAINER_RUNTIME``
    Container runtime to invoke, ``podman`` by default.
``PORCUPINE_CHECKER_IMAGE``
    Overrides the image tag from ``porcupine_image``, e.g. to try a checker
    build that has not been pinned yet.

Typical usage from a test::

    from test.cluster.tools.porcupine import run_porcupine_checker

    result = await run_porcupine_checker(history_data)
    assert result["valid"], f"Linearizability violation: {result}"
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import uuid
from pathlib import Path
from typing import Any

from test import TEST_DIR

logger = logging.getLogger(__name__)

_PORCUPINE_DIR = TEST_DIR / "cluster" / "tools" / "porcupine"
CHECKER_SRC_DIR = _PORCUPINE_DIR / "porcupine_checker"
CHECKER_BINARY = CHECKER_SRC_DIR / "porcupine_checker"


CONTAINER_RUNTIME = os.environ.get("PORCUPINE_CONTAINER_RUNTIME", "podman")
USE_CONTAINER_CHECKER = os.environ.get("PORCUPINE_USE_CONTAINER", "1") != "0"

_IMAGE_FILE = _PORCUPINE_DIR / "porcupine_image"


def _checker_image() -> str:
    """Read the checker image name from the 'image' file, with env override."""
    override = os.environ.get("PORCUPINE_CHECKER_IMAGE")
    if override:
        return override
    return _IMAGE_FILE.read_text().strip()


def _needs_rebuild() -> bool:
    """Return True if the checker binary needs to be (re)built."""
    if not CHECKER_BINARY.exists():
        return True
    binary_mtime = CHECKER_BINARY.stat().st_mtime
    for p in CHECKER_SRC_DIR.iterdir():
        if p.suffix in (".go", ".mod", ".sum") and p.stat().st_mtime > binary_mtime:
            return True
    return False


def _build_checker() -> None:
    """Build the Go checker binary if the source is newer or binary is missing."""
    if not CHECKER_SRC_DIR.exists():
        raise FileNotFoundError(
            f"Porcupine checker source not found at {CHECKER_SRC_DIR}"
        )

    if not _needs_rebuild():
        logger.debug("Porcupine checker binary is up to date")
        return

    logger.info("Building porcupine checker in %s", CHECKER_SRC_DIR)
    result = subprocess.run(
        ["go", "build", "-o", str(CHECKER_BINARY), "."],
        cwd=str(CHECKER_SRC_DIR),
        capture_output=True,
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"go build failed (exit {result.returncode}):\n"
            f"{result.stderr.decode(errors='replace')}"
        )

    logger.info("Porcupine checker built successfully")


async def _remove_container(name: str) -> None:
    """Force-remove the checker container, ignoring a container already gone.

    Killing the ``podman run`` process does not stop the container it started:
    podman is daemonless, so the container and its monitor outlive the client.
    With ``--rm`` it is still removed, but only once it exits by itself -- and
    the reason we get here is that it was not going to exit any time soon.
    """
    proc = await asyncio.create_subprocess_exec(
        CONTAINER_RUNTIME, "rm", "--force", "--time", "0", name,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        logger.warning("Failed to remove checker container %s: %s",
                       name, stderr.decode(errors="replace").strip())


async def run_porcupine_checker(
    history_data: str | bytes,
    *,
    timeout_seconds: float = 120,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    """Build (if needed) and run the Porcupine linearizability checker.

    The history is piped to the checker's stdin as JSON-lines.

    Args:
        history_data: JSON-lines history content (str or bytes).
        timeout_seconds: Maximum wall-clock time for the checker subprocess.
        output_dir: Directory for debug artifacts (written only on failure).

    Returns:
        Parsed JSON result dict from the checker.  On success::

            {"valid": True, "keys_checked": 100, "total_ops": 5000}

        On linearizability violation::

            {"valid": False, "error": "...",
             "visualization": "/path/to/viz.html"}

    Raises:
        FileNotFoundError: If the checker source is missing (native mode).
        RuntimeError: If the build fails, the checker exits non-zero, times
            out, or produces unparseable output.
    """
    if not USE_CONTAINER_CHECKER:
        _build_checker()

    if isinstance(history_data, str):
        history_data = history_data.encode()

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)

    container_name = f"porcupine-checker-{uuid.uuid4().hex[:12]}"

    if USE_CONTAINER_CHECKER:
        cmd = [CONTAINER_RUNTIME, "run", "--rm", "-i", "--name", container_name]

        if output_dir is not None:
            cmd += ["-v", f"{output_dir}:/output:Z"]

        cmd += [_checker_image()]

        if output_dir is not None:
            cmd += ["--output-dir", "/output"]
    else:
        cmd = [str(CHECKER_BINARY)]
        if output_dir is not None:
            cmd += ["--output-dir", str(output_dir)]

    logger.info("Running porcupine checker (%d bytes): %s", len(history_data), " ".join(cmd))

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(input=history_data), timeout=timeout_seconds,
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        if USE_CONTAINER_CHECKER:
            await _remove_container(container_name)
        raise RuntimeError(
            f"Porcupine checker timed out after {timeout_seconds}s"
        )

    stderr_text = stderr.decode(errors="replace").strip()
    if stderr_text:
        logger.info("Porcupine checker stderr:\n%s", stderr_text)

    # Exit 0 = linearizable, exit 1 = violation (both produce JSON).
    # Exit 2 = input/usage error, anything else is unexpected.
    if proc.returncode not in (0, 1):
        raise RuntimeError(
            f"Porcupine checker exited with code {proc.returncode}\n"
            f"Artifacts: {output_dir}\n"
            f"stderr:\n{stderr_text}"
        )

    stdout_text = stdout.decode().strip()
    if not stdout_text:
        raise RuntimeError(
            f"Porcupine checker produced no output on stdout\n"
            f"Artifacts: {output_dir}\n"
            f"stderr:\n{stderr_text}"
        )

    try:
        result: dict[str, Any] = json.loads(stdout_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Porcupine checker output is not valid JSON:\n{stdout_text}"
        ) from exc

    if output_dir is not None:
        result["artifacts_dir"] = str(output_dir)

    logger.info(
        "Porcupine result: valid=%s keys_checked=%s total_ops=%s",
        result.get("valid"),
        result.get("keys_checked"),
        result.get("total_ops"),
    )

    return result
