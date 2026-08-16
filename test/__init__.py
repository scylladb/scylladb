#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import hashlib
import socket
import time
import os
from pathlib import Path

__all__ = ["ALL_MODES", "BUILD_DIR", "DEBUG_MODES", "HOST_ID", "TEST_DIR", "TEST_RUNNER", "TOP_SRC_DIR",
           "asan_options", "path_to", "ubsan_options"]


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
MODES_TIMEOUT_FACTOR = {"release": 1, "sanitize": 3, "debug": 3, "dev": 2, "coverage": 1}

HOST_ID = os.environ.get("SCYLLA_TEST_HOST_ID")
if HOST_ID is None:
    HOST_ID = hashlib.sha3_224((socket.gethostname() + str(time.time())).encode("utf-8")).hexdigest()[:5]
    os.environ["SCYLLA_TEST_HOST_ID"] = HOST_ID


def ubsan_options(inherit: bool = False) -> str:
    """UBSAN_OPTIONS for running a sanitizer-enabled Scylla or Scylla-based tool.

    With inherit=True, a UBSAN_OPTIONS already present in the environment is
    appended; sanitizer options are last-wins, so it overrides what is set here.
    """

    opts = [
        "halt_on_error=1",
        "abort_on_error=1",
        f"suppressions={TOP_SRC_DIR / 'ubsan-suppressions.supp'}",
    ]
    if inherit:
        opts.append(os.getenv("UBSAN_OPTIONS"))
    return ":".join(filter(None, opts))


def asan_options(inherit: bool = False) -> str:
    """ASAN_OPTIONS for running a sanitizer-enabled Scylla or Scylla-based tool.

    See ubsan_options() for inherit.

    abort_on_error + disable_coredump make a sanitizer failure dump a core, which
    is the only way to inspect corrupted allocator state after the fact.
    detect_stack_use_after_return makes ASAN relocate frames onto its own "fake
    stack" so that a frame touched after it returns is caught; keep in mind that
    it also means the address of a local no longer identifies the stack the code
    is running on (see cql3/util.cc).
    """

    opts = [
        "disable_coredump=0",
        "abort_on_error=1",
        "detect_stack_use_after_return=1",
    ]
    if inherit:
        opts.append(os.getenv("ASAN_OPTIONS"))
    return ":".join(filter(None, opts))


def path_to(mode: str, *components: str) -> str:
    """Resolve path to built executable."""

    # cmake places build.ninja in build/, traditional is in ./.
    # We choose to test for traditional, not cmake, because IDEs may
    # invoke cmake to learn the configuration and generate false positives
    if not TOP_SRC_DIR.joinpath("build.ninja").exists():
        *dir_components, basename = components
        return str(BUILD_DIR.joinpath(*dir_components, ALL_MODES[mode], basename))
    return str(BUILD_DIR.joinpath(mode, *components))
