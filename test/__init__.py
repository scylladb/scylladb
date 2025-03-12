#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from pathlib import Path


TOP_SRC_DIR = Path(__file__).parent.parent  # ScyllaDB's source code root directory
TEST_DIR = TOP_SRC_DIR / "test"
BUILD_DIR = TOP_SRC_DIR / "build"