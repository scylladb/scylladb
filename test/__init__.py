#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import os
from pathlib import Path

__all__ = ["ALL_MODES", "BUILD_DIR", "DEBUG_MODES", "TEST_DIR", "TEST_RUNNER", "TOP_SRC_DIR", "path_to"]


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


def path_to(mode: str, *components: str) -> str:
    """Resolve path to built executable."""

    if BUILD_DIR.joinpath("build.ninja").exists():
        *dir_components, basename = components
        return str(BUILD_DIR.joinpath(*dir_components, ALL_MODES[mode], basename))
    return str(BUILD_DIR.joinpath(mode, *components))
