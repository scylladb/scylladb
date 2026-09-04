#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import fcntl
import hashlib
import importlib
import shutil
import socket
import subprocess
import sys
import sysconfig
import time
import os
from pathlib import Path

__all__ = ["ALL_MODES", "BUILD_DIR", "DEBUG_MODES", "HOST_ID", "TEST_DIR", "TEST_RUNNER", "TOP_SRC_DIR", "path_to"]


TEST_RUNNER = os.environ.get("SCYLLA_TEST_RUNNER", "pytest")

TOP_SRC_DIR = Path(__file__).parent.parent  # ScyllaDB's source code root directory
TEST_DIR = TOP_SRC_DIR / "test"
BUILD_DIR = TOP_SRC_DIR / "build"


def _pinned_scylla_driver_requirement() -> str:
    """Return the scylla-driver requirement line pinned in test/test-requirements.txt."""
    requirements_file = TEST_DIR / "test-requirements.txt"
    for line in requirements_file.read_text().splitlines():
        line = line.strip()
        if line.startswith("scylla-driver"):
            return line
    raise RuntimeError(f"scylla-driver pin not found in {requirements_file}")


def _ensure_scylla_driver() -> None:
    """Make scylla-driver importable, installing the pinned version on demand.

    scylla-driver isn't baked into the frozen toolchain image, so it's
    installed here into a cache dir and added to sys.path, instead of
    every test.py run depending on an image rebuild to pick up a new
    pinned driver version. The cache dir is keyed by both the interpreter
    ABI and the pinned version: this lets a populated cache from a
    previous run be reused (no re-invoking pip every time) while
    ensuring two test.py runs that pin different scylla-driver versions
    -- e.g. concurrent runs from different checkouts sharing the same
    XDG_CACHE_HOME -- install into separate directories instead of
    racing to overwrite one another or one silently observing the
    other's (wrong) version.

    A marker file is written only after a full, successful install, and
    its presence (rather than merely the target dir's, or an import
    probe) is what's used to decide whether to skip installing: a
    directory left half-populated by a crashed or concurrently-running
    install must not be mistaken for a ready one. A per-target-dir file
    lock serializes concurrent installs into the same directory (e.g.
    two test.py runs against the same interpreter and driver version
    started at the same time).
    """
    requirement = _pinned_scylla_driver_requirement()
    version = requirement.rsplit("==", 1)[1]

    cache_home = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
    cache_root = cache_home / "scylla-test-pip-packages"
    target_dir = cache_root / f"{sysconfig.get_config_var('SOABI')}-{version}"
    marker = target_dir / ".install-complete"

    sys.path.insert(0, str(target_dir))
    if marker.exists():
        return

    cache_root.mkdir(parents=True, exist_ok=True)
    with open(cache_root / f"{target_dir.name}.lock", "w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            if marker.exists():  # another process installed while we were waiting for the lock
                return
            # A directory left behind by a crashed install is missing the marker, so it
            # would reach this point again -- but pip's --target only reliably behaves
            # against an empty directory, so a stale partial install must be cleared first.
            shutil.rmtree(target_dir, ignore_errors=True)
            target_dir.mkdir(parents=True, exist_ok=True)
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "--quiet",
                 "--target", str(target_dir), requirement],
                check=True,
            )
            # target_dir didn't exist yet when it was inserted into sys.path above; if anything
            # probed an import through it before now, Python's import system would have cached
            # its absence in sys.path_importer_cache, and the just-installed package would stay
            # unimportable without invalidating that cache.
            importlib.invalidate_caches()
            marker.touch()
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


_ensure_scylla_driver()

ALL_MODES = {
    "debug": "Debug",
    'release': "RelWithDebInfo",
    "dev": "Dev",
    "sanitize": "Sanitize",
    "coverage": "Coverage",
}

DEBUG_MODES = {"debug", "sanitize"}
MODES_TIMEOUT_FACTOR = {"release": 1, "sanitize": 3, "debug": 3, "dev": 2, "coverage": 1}

HOST_ID = os.environ.get("SCYLLA_TEST_HOST_ID")
if HOST_ID is None:
    HOST_ID = hashlib.sha3_224((socket.gethostname() + str(time.time())).encode("utf-8")).hexdigest()[:5]
    os.environ["SCYLLA_TEST_HOST_ID"] = HOST_ID


def path_to(mode: str, *components: str) -> str:
    """Resolve path to built executable."""

    # cmake places build.ninja in build/, traditional is in ./.
    # We choose to test for traditional, not cmake, because IDEs may
    # invoke cmake to learn the configuration and generate false positives
    if not TOP_SRC_DIR.joinpath("build.ninja").exists():
        *dir_components, basename = components
        return str(BUILD_DIR.joinpath(*dir_components, ALL_MODES[mode], basename))
    return str(BUILD_DIR.joinpath(mode, *components))
