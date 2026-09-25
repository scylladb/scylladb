#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

import hashlib
import socket
import sys
import time
import os
from pathlib import Path

# The Cassandra driver imports numpy if it can find it, to offer a numpy protocol
# handler that nothing in this suite asks for (no test imports numpy).  That costs
# about 22 MB in every pytest-xdist worker, and there is one worker per half core.
# There, numpy looks absent: importlib.util.find_spec("numpy") is None and
# importing it raises ModuleNotFoundError ("import of numpy halted; None in
# sys.modules"), so the driver takes its ordinary path.  SCYLLA_TEST_ALLOW_NUMPY=1
# lets workers import it; other processes (test.py itself, tools) always can.
if os.environ.get("PYTEST_XDIST_WORKER") and os.environ.get("SCYLLA_TEST_ALLOW_NUMPY") != "1":
    sys.modules.setdefault("numpy", None)

__all__ = ["ALL_MODES", "BUILD_DIR", "DEBUG_AND_SANITIZE_MODES", "DEBUG_MODES", "HOST_ID", "SANITIZE_MODE", "TEST_DIR", "TEST_RUNNER",
           "TOP_SRC_DIR", "asan_options", "path_to", "ubsan_options", "use_traditional_build"]


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

# Unoptimized builds: tests need much longer timeouts and smaller workloads there.
DEBUG_MODES = {"debug", "coverage"}
# Sanitize is built with -O2, so it runs almost as fast as dev and is not a debug
# mode: it gets dev-sized workloads and runs the tests that the debug modes skip.
# It still carries the sanitizers' overhead and the DEBUG define, so use
# DEBUG_AND_SANITIZE_MODES for things that depend on those:
# - Deadlines: how long to wait for a node to start or stop, for a log line, for
#   a test executable to finish. The wait ends as soon as the awaited thing
#   happens, so a passing test never spends the deadline and the debug value
#   costs sanitize nothing. Only a failing test pays for it, by being reported
#   later. A deadline that is too short costs a spurious failure, and the
#   sanitizers' overhead is uneven, so sanitize keeps the debug value rather
#   than a tuned one.
# - Things the binary decides with #ifdef DEBUG, such as default timeouts.
# Do not use it for a time that a test waits out in full, or for a workload
# size: those make every run slower, and sanitize gets the dev value there.
SANITIZE_MODE = "sanitize"
DEBUG_AND_SANITIZE_MODES = DEBUG_MODES | {SANITIZE_MODE}
MODES_TIMEOUT_FACTOR = {"release": 1, "sanitize": 2.5, "debug": 3, "dev": 2, "coverage": 3}

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


def use_traditional_build() -> bool:
    """Is this a configure.py build tree, rather than a cmake one?

    cmake places build.ninja in build/, traditional is in ./.  We choose to
    test for traditional, not cmake, because IDEs may invoke cmake to learn
    the configuration and generate false positives.
    """

    return TOP_SRC_DIR.joinpath("build.ninja").exists()


def path_to(mode: str, *components: str) -> str:
    """Resolve path to built executable."""

    if not use_traditional_build():
        *dir_components, basename = components
        return str(BUILD_DIR.joinpath(*dir_components, ALL_MODES[mode], basename))
    return str(BUILD_DIR.joinpath(mode, *components))
