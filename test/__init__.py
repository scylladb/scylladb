#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#
from __future__ import annotations

import os
from pathlib import Path

TEST_RUNNER = os.environ.get("SCYLLA_TEST_RUNNER", "pytest")
TOP_SRC_DIR = Path(__file__).parent.parent  # ScyllaDB's source code root directory
BUILD_DIR = TOP_SRC_DIR / "build"
TEST_DIR = TOP_SRC_DIR / "test"
COMBINED_TESTS = Path('test/boost/combined_tests')
