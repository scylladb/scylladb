#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import hashlib
import socket
import time
import os
from pathlib import Path

__all__ = ["ALL_MODES", "BUILD_DIR", "DEBUG_MODES", "HOST_ID", "TEST_DIR", "TEST_RUNNER", "TOP_SRC_DIR", "path_to"]


TEST_RUNNER = os.environ.get("SCYLLA_TEST_RUNNER", "pytest")

TOP_SRC_DIR = Path(__file__).parent.parent  # ScyllaDB's source code root directory
TEST_DIR = TOP_SRC_DIR / "test"
BUILD_DIR = TOP_SRC_DIR / "build"

ALL_MODES = {
    "debug": "Debug",
    'release': "RelWithDebInfo",
    "dev": "Dev",
    "sanitize": "Sanitize",
    "coverage": "Coverage",
}
DEBUG_MODES = {"debug", "sanitize"}

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
