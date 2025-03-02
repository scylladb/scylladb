#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from pathlib import Path

__all__ = ["ALL_MODES", "BUILDDIR", "DEBUG_MODES", "TESTDIR", "TOP_SRCDIR", "path_to"]


TOP_SRCDIR = Path(__file__).parent.parent  # ScyllaDB's source code root directory
TESTDIR = TOP_SRCDIR / "test"
BUILDDIR = TOP_SRCDIR / "build"

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

    if BUILDDIR.joinpath("build.ninja").exists():
        *dir_components, basename = components
        return str(BUILDDIR.joinpath(*dir_components, ALL_MODES[mode], basename))
    return str(BUILDDIR.joinpath(mode, *components))
