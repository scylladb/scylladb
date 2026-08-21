#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import hashlib
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


def _ensure_scylla_driver() -> None:
    """Make scylla-driver importable, installing it on demand.

    scylla-driver isn't baked into the frozen toolchain image, so it's
    installed here into a cache dir keyed by Python ABI (since that cache
    dir can outlive a toolchain/interpreter upgrade) and added to
    sys.path, instead of every test.py run depending on an image rebuild
    to pick up a new driver version. The cache dir is added to sys.path
    before the importability check, so a populated cache from a previous
    run is reused instead of re-invoking uv every time.
    """
    cache_home = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
    target_dir = cache_home / "scylla-test-uv-packages" / sysconfig.get_config_var("SOABI")
    sys.path.insert(0, str(target_dir))

    try:
        import cassandra  # noqa: F401
    except ImportError:
        pass
    else:
        return

    target_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["uv", "pip", "install", "--quiet", "--target", str(target_dir), "-r", str(TEST_DIR / "uv-requirements.txt")],
        check=True,
    )


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
